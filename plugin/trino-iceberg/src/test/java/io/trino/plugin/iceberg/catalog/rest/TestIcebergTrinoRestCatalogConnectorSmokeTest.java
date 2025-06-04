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
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.QueryFailedException;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.rest.DelegatingRestSessionCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
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
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestIcebergTrinoRestCatalogConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    private Path warehouseLocation;

    private JdbcCatalog backend;

    public TestIcebergTrinoRestCatalogConnectorSmokeTest()
    {
        super(new IcebergConfig().getFileFormat().toIceberg());
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_CREATE_MATERIALIZED_VIEW,
                SUPPORTS_RENAME_MATERIALIZED_VIEW,
                SUPPORTS_RENAME_SCHEMA -> false;
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

        return IcebergQueryRunner.builder()
                .setBaseDataDir(Optional.of(warehouseLocation))
                .setIcebergProperties(
                        ImmutableMap.<String, String>builder()
                                .put("iceberg.file-format", format.name())
                                .put("iceberg.catalog.type", "rest")
                                .put("iceberg.rest-catalog.uri", testServer.getBaseUrl().toString())
                                .put("iceberg.register-table-procedure.enabled", "true")
                                .put("iceberg.writer-sort-buffer-size", "1MB")
                                .put("iceberg.allowed-extra-properties", "write.metadata.delete-after-commit.enabled,write.metadata.previous-versions-max")
                                .buildOrThrow())
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @AfterAll
    public void teardown()
    {
        backend = null; // closed by closeAfterClass
    }

    @Test
    void testDropSchemaCascadeWithViews()
    {
        String schemaName = "test_drop_schema_cascade" + randomNameSuffix();

        assertUpdate("CREATE SCHEMA " + schemaName);

        TableIdentifier sparkViewIdentifier = TableIdentifier.of(schemaName, "test_spark_views" + randomNameSuffix());
        backend.buildView(sparkViewIdentifier)
                .withDefaultNamespace(Namespace.of(schemaName))
                .withDefaultCatalog("iceberg")
                .withQuery("spark", "SELECT 1 x")
                .withSchema(new Schema(required(1, "x", Types.LongType.get())))
                .create();

        TableIdentifier trinoViewIdentifier = TableIdentifier.of(schemaName, "test_trino_views" + randomNameSuffix());
        backend.buildView(trinoViewIdentifier)
                .withDefaultNamespace(Namespace.of(schemaName))
                .withDefaultCatalog("iceberg")
                .withQuery("trino", "SELECT 1 x")
                .withSchema(new Schema(required(1, "x", Types.LongType.get())))
                .create();

        assertThat(backend.viewExists(sparkViewIdentifier)).isTrue();
        assertThat(backend.viewExists(trinoViewIdentifier)).isTrue();

        assertUpdate("DROP SCHEMA " + schemaName + " CASCADE");

        assertThat(backend.viewExists(sparkViewIdentifier)).isFalse();
        assertThat(backend.viewExists(trinoViewIdentifier)).isFalse();
    }

    @Test
    void testUnsupportedViewDialect()
    {
        String viewName = "test_unsupported_dialect" + randomNameSuffix();
        TableIdentifier identifier = TableIdentifier.of("tpch", viewName);

        backend.buildView(identifier)
                .withDefaultNamespace(Namespace.of("tpch"))
                .withDefaultCatalog("iceberg")
                .withQuery("spark", "SELECT 1 x")
                .withSchema(new Schema(required(1, "x", Types.LongType.get())))
                .create();

        assertThat(computeActual("SHOW TABLES FROM iceberg.tpch").getOnlyColumnAsSet())
                .contains(viewName);

        assertThat(computeActual("SELECT table_name FROM information_schema.views WHERE table_schema = 'tpch'").getOnlyColumnAsSet())
                .doesNotContain(viewName);

        assertThat(computeActual("SELECT table_name FROM information_schema.columns WHERE table_schema = 'tpch'").getOnlyColumnAsSet())
                .doesNotContain(viewName);

        assertQueryReturnsEmptyResult("SELECT * FROM information_schema.columns WHERE table_schema = 'tpch' AND table_name = '" + viewName + "'");

        assertQueryFails("SELECT * FROM " + viewName, "Cannot read unsupported dialect 'spark' for view '.*'");

        backend.dropView(identifier);
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
    public void testRenameSchema()
    {
        assertThatThrownBy(super::testRenameSchema)
                .hasMessageContaining("renameNamespace is not supported for Iceberg REST catalog");
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

    private TableIdentifier toIdentifier(String tableName)
    {
        return TableIdentifier.of(getSession().getSchema().orElseThrow(), tableName);
    }
}
