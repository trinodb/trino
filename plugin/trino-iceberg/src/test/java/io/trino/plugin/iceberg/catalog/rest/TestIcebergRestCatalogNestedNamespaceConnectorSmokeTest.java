/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.iceberg.catalog.rest;

import io.airlift.http.server.testing.TestingHttpServer;
import io.trino.filesystem.Location;
import io.trino.plugin.iceberg.BaseIcebergConnectorSmokeTest;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.SchemaInitializer;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.rest.DelegatingRestSessionCatalog;
import org.assertj.core.util.Files;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkOrcFileSorting;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkParquetFileSorting;
import static io.trino.plugin.iceberg.catalog.rest.RestCatalogTestUtils.backendCatalog;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.apache.iceberg.FileFormat.PARQUET;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestIcebergRestCatalogNestedNamespaceConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    private File warehouseLocation;
    private JdbcCatalog backend;

    public TestIcebergRestCatalogNestedNamespaceConnectorSmokeTest()
    {
        super(new IcebergConfig().getFileFormat().toIceberg());
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_CREATE_MATERIALIZED_VIEW -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        warehouseLocation = Files.newTemporaryFolder();
        closeAfterClass(() -> deleteRecursively(warehouseLocation.toPath(), ALLOW_INSECURE));

        backend = closeAfterClass((JdbcCatalog) backendCatalog(warehouseLocation));

        DelegatingRestSessionCatalog delegatingCatalog = DelegatingRestSessionCatalog.builder()
                .delegate(backend)
                .build();

        TestingHttpServer testServer = delegatingCatalog.testServer();
        testServer.start();
        closeAfterClass(testServer::stop);

        return IcebergQueryRunner.builder("level_1.level_2")
                .setBaseDataDir(Optional.of(warehouseLocation.toPath()))
                .addIcebergProperty("iceberg.file-format", format.name())
                .addIcebergProperty("iceberg.catalog.type", "rest")
                .addIcebergProperty("iceberg.rest-catalog.uri", testServer.getBaseUrl().toString())
                .addIcebergProperty("iceberg.register-table-procedure.enabled", "true")
                .addIcebergProperty("iceberg.writer-sort-buffer-size", "1MB")
                .setSchemaInitializer(SchemaInitializer.builder()
                        .withSchemaName("level_1.level_2")
                        .withClonedTpchTables(REQUIRED_TPCH_TABLES)
                        .build())
                .build();
    }

    @Test
    void testDropNestedSchemaCascade()
    {
        String rootSchemaName = "test_root" + randomNameSuffix();
        assertUpdate("CREATE SCHEMA " + rootSchemaName);
        assertUpdate("CREATE SCHEMA \"" + rootSchemaName + ".test_nested\"");

        assertQueryFails("DROP SCHEMA " + rootSchemaName + " CASCADE", "Cannot drop non-empty schema: .*");

        assertUpdate("DROP SCHEMA \"" + rootSchemaName + ".test_nested\" CASCADE");
        assertUpdate("DROP SCHEMA " + rootSchemaName);
    }

    @Test
    @Override // Override because the schema name requires double quotes
    public void testShowCreateTable()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .matches("" +
                        "CREATE TABLE iceberg.\"" + schemaName + "\".region \\(\n" +
                        "   regionkey bigint,\n" +
                        "   name varchar,\n" +
                        "   comment varchar\n" +
                        "\\)\n" +
                        "WITH \\(\n" +
                        "   format = '" + format.name() + "',\n" +
                        "   format_version = 2,\n" +
                        format("   location = '.*/" + schemaName + "/region.*'\n") +
                        "\\)");
    }

    @Test
    @Override // Override because the schema name requires double quotes
    public void testView()
    {
        String viewName = "test_view_" + randomNameSuffix();
        assertUpdate("CREATE VIEW " + viewName + " AS SELECT * FROM nation");

        assertThat(query("SELECT * FROM " + viewName))
                .skippingTypesCheck()
                .matches("SELECT * FROM nation");

        assertThat((String) computeScalar("SHOW CREATE VIEW " + viewName))
                .isEqualTo("""
                        CREATE VIEW iceberg."level_1.level_2".%s SECURITY DEFINER AS
                        SELECT *
                        FROM
                          nation""".formatted(viewName));

        assertUpdate("DROP  VIEW " + viewName);
    }

    @Test
    @Override
    public void testRenameSchema()
    {
        assertThatThrownBy(super::testRenameSchema)
                .hasMessageContaining("renameNamespace is not supported for Iceberg REST catalog");
    }

    @Test
    @Override
    public void testMaterializedView()
    {
        assertThatThrownBy(super::testMaterializedView)
                .hasMessageContaining("createMaterializedView is not supported for Iceberg REST catalog");
    }

    @Test
    @Override
    public void testDropTableWithMissingMetadataFile()
    {
        assertThatThrownBy(super::testDropTableWithMissingMetadataFile)
                .hasMessageMatching("Failed to load table: (.*)");
    }

    @Test
    @Override
    public void testDropTableWithMissingSnapshotFile()
    {
        assertThatThrownBy(super::testDropTableWithMissingSnapshotFile)
                .hasMessageMatching("Server error: NotFoundException: Failed to open input stream for file: (.*)");
    }

    @Test
    @Override
    public void testDropTableWithMissingManifestListFile()
    {
        assertThatThrownBy(super::testDropTableWithMissingManifestListFile)
                .hasMessageContaining("Table location should not exist");
    }

    @Test
    @Override
    public void testDropTableWithNonExistentTableLocation()
    {
        assertThatThrownBy(super::testDropTableWithNonExistentTableLocation)
                .hasMessageMatching("Failed to load table: (.*)");
    }

    @Override
    protected boolean isFileSorted(Location path, String sortColumnName)
    {
        if (format == PARQUET) {
            return checkParquetFileSorting(fileSystem.newInputFile(path), sortColumnName);
        }
        return checkOrcFileSorting(fileSystem, path, sortColumnName);
    }

    @Override
    protected void deleteDirectory(String location)
    {
        try {
            deleteRecursively(Path.of(location), ALLOW_INSECURE);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected void dropTableFromMetastore(String tableName)
    {
        backend.dropTable(toIdentifier(tableName), false);
    }

    @Override
    protected String getMetadataLocation(String tableName)
    {
        BaseTable table = (BaseTable) backend.loadTable(toIdentifier(tableName));
        return table.operations().current().metadataFileLocation();
    }

    @Override
    protected String schemaPath()
    {
        return format("%s/%s", warehouseLocation, getSession().getSchema());
    }

    @Override
    protected boolean locationExists(String location)
    {
        return java.nio.file.Files.exists(Path.of(location));
    }

    private TableIdentifier toIdentifier(String tableName)
    {
        return TableIdentifier.of(getSession().getSchema().orElseThrow(), tableName);
    }
}
