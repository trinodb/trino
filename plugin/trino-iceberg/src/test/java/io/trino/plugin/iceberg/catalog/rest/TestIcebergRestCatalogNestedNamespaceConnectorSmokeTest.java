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

import com.google.common.collect.ImmutableMap;
import io.airlift.http.server.testing.TestingHttpServer;
import io.trino.filesystem.Location;
import io.trino.plugin.iceberg.BaseIcebergConnectorSmokeTest;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.SchemaInitializer;
import io.trino.plugin.iceberg.TestingIcebergPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryFailedException;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.rest.DelegatingRestSessionCatalog;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkOrcFileSorting;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkParquetFileSorting;
import static io.trino.plugin.iceberg.catalog.rest.RestCatalogTestUtils.backendCatalog;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.apache.iceberg.FileFormat.PARQUET;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestIcebergRestCatalogNestedNamespaceConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    private Path warehouseLocation;
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
        warehouseLocation = Files.createTempDirectory(null);
        closeAfterClass(() -> deleteRecursively(warehouseLocation, ALLOW_INSECURE));

        backend = closeAfterClass((JdbcCatalog) backendCatalog(warehouseLocation));

        DelegatingRestSessionCatalog delegatingCatalog = DelegatingRestSessionCatalog.builder()
                .delegate(backend)
                .build();

        TestingHttpServer testServer = delegatingCatalog.testServer();
        testServer.start();
        closeAfterClass(testServer::stop);

        String nestedSchema = "level_1.level_2";
        QueryRunner queryRunner = DistributedQueryRunner.builder(testSessionBuilder()
                        .setCatalog(ICEBERG_CATALOG)
                        .setSchema(nestedSchema)
                        .build())
                .setBaseDataDir(Optional.of(warehouseLocation))
                .build();

        Map<String, String> nestedNamespaceDisabled = ImmutableMap.<String, String>builder()
                .put("fs.hadoop.enabled", "true")
                .put("iceberg.file-format", format.name())
                .put("iceberg.catalog.type", "rest")
                .put("iceberg.rest-catalog.uri", testServer.getBaseUrl().toString())
                .put("iceberg.register-table-procedure.enabled", "true")
                .put("iceberg.writer-sort-buffer-size", "1MB")
                .put("iceberg.allowed-extra-properties", "write.metadata.delete-after-commit.enabled,write.metadata.previous-versions-max")
                .buildOrThrow();

        Map<String, String> nestedNamespaceEnabled = ImmutableMap.<String, String>builder()
                .putAll(nestedNamespaceDisabled)
                .put("iceberg.rest-catalog.nested-namespace-enabled", "true")
                .buildOrThrow();
        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            Path dataDir = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data");
            queryRunner.installPlugin(new TestingIcebergPlugin(dataDir));

            queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg", nestedNamespaceEnabled);
            queryRunner.createCatalog("nested_namespace_disabled", "iceberg", nestedNamespaceDisabled);

            SchemaInitializer.builder()
                    .withSchemaName(nestedSchema)
                    .withClonedTpchTables(REQUIRED_TPCH_TABLES)
                    .build()
                    .accept(queryRunner);

            return queryRunner;
        }
        catch (Exception e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    @Test
    void testNestedNamespaceDisabled()
    {
        // SHOW SCHEMAS returns only the top-level schema
        assertThat(query("SHOW SCHEMAS FROM nested_namespace_disabled"))
                .skippingTypesCheck()
                .containsAll("VALUES 'level_1'");

        // Other statements should fail
        assertQueryFails("SHOW TABLES IN nested_namespace_disabled.\"level_1.level_2\"", "Nested namespace is not enabled for this catalog");
        assertQueryFails("CREATE SCHEMA nested_namespace_disabled.\"level_1.level_2.level_3\"", "Nested namespace is not enabled for this catalog");
        assertQueryFails("CREATE TABLE nested_namespace_disabled.\"level_1.level_2\".test_nested(x int)", "Nested namespace is not enabled for this catalog");
        assertQueryFails("SELECT * FROM nested_namespace_disabled.\"level_1.level_2\".region", "Nested namespace is not enabled for this catalog");
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
                        format("   location = '.*/" + schemaName + "/region.*',\n" +
                        "   max_commit_retry = 4\n") +
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
                .isEqualTo(
                        """
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
                .isInstanceOf(QueryFailedException.class)
                .cause()
                .hasMessageContaining("Failed to drop table")
                .cause()
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
