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
package io.trino.plugin.iceberg.catalog.jdbc;

import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.Location;
import io.trino.plugin.iceberg.BaseIcebergConnectorSmokeTest;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.catalog.jdbc.IcebergJdbcCatalogConfig.SchemaVersion;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkOrcFileSorting;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkParquetFileSorting;
import static io.trino.plugin.iceberg.catalog.jdbc.TestingIcebergJdbcServer.PASSWORD;
import static io.trino.plugin.iceberg.catalog.jdbc.TestingIcebergJdbcServer.USER;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.apache.iceberg.CatalogProperties.CATALOG_IMPL;
import static org.apache.iceberg.CatalogProperties.URI;
import static org.apache.iceberg.CatalogProperties.WAREHOUSE_LOCATION;
import static org.apache.iceberg.CatalogUtil.buildIcebergCatalog;
import static org.apache.iceberg.FileFormat.PARQUET;
import static org.apache.iceberg.jdbc.JdbcCatalog.PROPERTY_PREFIX;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestIcebergJdbcCatalogConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    private JdbcCatalog jdbcCatalog;
    private File warehouseLocation;

    public TestIcebergJdbcCatalogConnectorSmokeTest()
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
        warehouseLocation = Files.createTempDirectory("test_iceberg_jdbc_catalog_smoke_test").toFile();
        closeAfterClass(() -> deleteRecursively(warehouseLocation.toPath(), ALLOW_INSECURE));
        TestingIcebergJdbcServer server = closeAfterClass(new TestingIcebergJdbcServer());
        jdbcCatalog = (JdbcCatalog) buildIcebergCatalog("tpch", ImmutableMap.<String, String>builder()
                        .put(CATALOG_IMPL, JdbcCatalog.class.getName())
                        .put(URI, server.getJdbcUrl())
                        .put(PROPERTY_PREFIX + "user", USER)
                        .put(PROPERTY_PREFIX + "password", PASSWORD)
                        .put(PROPERTY_PREFIX + "schema-version", SchemaVersion.V1.toString())
                        .put(WAREHOUSE_LOCATION, warehouseLocation.getAbsolutePath())
                        .buildOrThrow(),
                new Configuration(false));
        return IcebergQueryRunner.builder()
                .setIcebergProperties(
                        ImmutableMap.<String, String>builder()
                                .put("iceberg.file-format", format.name())
                                .put("iceberg.catalog.type", "jdbc")
                                .put("iceberg.jdbc-catalog.driver-class", "org.postgresql.Driver")
                                .put("iceberg.jdbc-catalog.connection-url", server.getJdbcUrl())
                                .put("iceberg.jdbc-catalog.connection-user", USER)
                                .put("iceberg.jdbc-catalog.connection-password", PASSWORD)
                                .put("iceberg.jdbc-catalog.catalog-name", "tpch")
                                .put("iceberg.register-table-procedure.enabled", "true")
                                .put("iceberg.writer-sort-buffer-size", "1MB")
                                .put("iceberg.jdbc-catalog.default-warehouse-dir", warehouseLocation.getAbsolutePath())
                                .buildOrThrow())
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @AfterAll
    public final void destroy()
    {
        jdbcCatalog.close();
        jdbcCatalog = null;
    }

    @Test
    void testDropSchemaCascadeWithViews()
    {
        String schemaName = "test_drop_schema_cascade" + randomNameSuffix();

        assertUpdate("CREATE SCHEMA " + schemaName);

        TableIdentifier sparkViewIdentifier = TableIdentifier.of(schemaName, "test_spark_views" + randomNameSuffix());
        jdbcCatalog.buildView(sparkViewIdentifier)
                .withDefaultNamespace(Namespace.of(schemaName))
                .withDefaultCatalog("iceberg")
                .withQuery("spark", "SELECT 1 x")
                .withSchema(new Schema(required(1, "x", Types.LongType.get())))
                .create();

        TableIdentifier trinoViewIdentifier = TableIdentifier.of(schemaName, "test_trino_views" + randomNameSuffix());
        jdbcCatalog.buildView(trinoViewIdentifier)
                .withDefaultNamespace(Namespace.of(schemaName))
                .withDefaultCatalog("iceberg")
                .withQuery("trino", "SELECT 1 x")
                .withSchema(new Schema(required(1, "x", Types.LongType.get())))
                .create();

        assertThat(jdbcCatalog.viewExists(sparkViewIdentifier)).isTrue();
        assertThat(jdbcCatalog.viewExists(trinoViewIdentifier)).isTrue();

        assertUpdate("DROP SCHEMA " + schemaName + " CASCADE");

        assertThat(jdbcCatalog.viewExists(sparkViewIdentifier)).isFalse();
        assertThat(jdbcCatalog.viewExists(trinoViewIdentifier)).isFalse();
    }

    @Test
    void testUnsupportedViewDialect()
    {
        String viewName = "test_unsupported_dialect" + randomNameSuffix();
        TableIdentifier identifier = TableIdentifier.of("tpch", viewName);

        jdbcCatalog.buildView(identifier)
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

        jdbcCatalog.dropView(identifier);
    }

    @Test
    @Override
    public void testMaterializedView()
    {
        assertThatThrownBy(super::testMaterializedView)
                .hasMessageContaining("createMaterializedView is not supported for Iceberg JDBC catalogs");
    }

    @Test
    @Override
    public void testRenameSchema()
    {
        assertThatThrownBy(super::testRenameSchema)
                .hasMessageContaining("renameNamespace is not supported for Iceberg JDBC catalogs");
    }

    @Test
    @Override
    public void testDeleteRowsConcurrently()
    {
        // TODO https://github.com/trinodb/trino/issues/21862 Fix flaky test
    }

    @Override
    protected void dropTableFromMetastore(String tableName)
    {
        jdbcCatalog.dropTable(toIdentifier(tableName), false);
    }

    @Override
    protected String getMetadataLocation(String tableName)
    {
        BaseTable table = (BaseTable) jdbcCatalog.loadTable(toIdentifier(tableName));
        return table.operations().current().metadataFileLocation();
    }

    @Override
    protected String schemaPath()
    {
        return format("%s/%s", warehouseLocation, getSession().getSchema().orElseThrow());
    }

    @Override
    protected boolean locationExists(String location)
    {
        return Files.exists(Path.of(location));
    }

    private TableIdentifier toIdentifier(String tableName)
    {
        return TableIdentifier.of(getSession().getSchema().orElseThrow(), tableName);
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
    protected boolean isFileSorted(Location path, String sortColumnName)
    {
        if (format == PARQUET) {
            return checkParquetFileSorting(fileSystem.newInputFile(path), sortColumnName);
        }
        return checkOrcFileSorting(fileSystem, path, sortColumnName);
    }
}
