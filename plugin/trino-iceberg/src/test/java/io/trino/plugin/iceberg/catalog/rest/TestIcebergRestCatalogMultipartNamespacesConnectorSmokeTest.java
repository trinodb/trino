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
import com.google.common.collect.ImmutableSet;
import io.airlift.http.server.testing.TestingHttpServer;
import io.trino.filesystem.Location;
import io.trino.plugin.iceberg.BaseIcebergConnectorSmokeTest;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.rest.DelegatingRestSessionCatalog;
import org.assertj.core.util.Files;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Optional;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
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

final class TestIcebergRestCatalogMultipartNamespacesConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    private static final String LEVEL_1_NAMESPACE = "level_1_%s".formatted(randomNameSuffix());
    private static final String LEVEL_2_NAMESPACE = "level_2_%s".formatted(randomNameSuffix());

    private File warehouseLocation;

    private JdbcCatalog backend;

    public TestIcebergRestCatalogMultipartNamespacesConnectorSmokeTest()
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
        warehouseLocation = Files.newTemporaryFolder();
        closeAfterClass(() -> deleteRecursively(warehouseLocation.toPath(), ALLOW_INSECURE));

        backend = closeAfterClass((JdbcCatalog) backendCatalog(warehouseLocation));

        DelegatingRestSessionCatalog delegatingCatalog = DelegatingRestSessionCatalog.builder()
                .delegate(backend)
                .build();

        TestingHttpServer testServer = delegatingCatalog.testServer();
        testServer.start();
        closeAfterClass(testServer::stop);

        return IcebergQueryRunner.builder()
                .setBaseDataDir(Optional.of(warehouseLocation.toPath()))
                .setIcebergProperties(
                        ImmutableMap.<String, String>builder()
                                .put("iceberg.file-format", format.name())
                                .put("iceberg.catalog.type", "rest")
                                .put("iceberg.rest-catalog.namespace-separator", ".")
                                .put("iceberg.rest-catalog.uri", testServer.getBaseUrl().toString())
                                .put("iceberg.register-table-procedure.enabled", "true")
                                .put("iceberg.writer-sort-buffer-size", "1MB")
                                .buildOrThrow())
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @BeforeAll
    public void setUp()
    {
        assertUpdate("CREATE SCHEMA " + LEVEL_1_NAMESPACE);
        assertUpdate("CREATE SCHEMA \"%s\"".formatted(buildNamespace(LEVEL_2_NAMESPACE, NamespaceLevel.SECOND)));
    }

    @AfterAll
    public void teardown()
    {
        assertUpdate("DROP SCHEMA \"%s\"".formatted(buildNamespace(LEVEL_2_NAMESPACE, NamespaceLevel.SECOND)));
        assertUpdate("DROP SCHEMA " + LEVEL_1_NAMESPACE);
        backend = null; // closed by closeAfterClass
    }

    @Test
    public void testCreateMultipartNamespace()
    {
        String nestedNamespace = buildNamespace("level_3_%s".formatted(randomNameSuffix()), NamespaceLevel.THIRD);
        ImmutableSet<String> expectedSchemas = ImmutableSet.of(
                LEVEL_1_NAMESPACE,
                buildNamespace(LEVEL_2_NAMESPACE, NamespaceLevel.SECOND),
                nestedNamespace);

        assertUpdate("CREATE SCHEMA \"%s\"".formatted(nestedNamespace));

        ImmutableSet<String> actualSchemas = computeActual("show schemas")
                .getOnlyColumnAsSet()
                .stream()
                .map(String.class::cast)
                .collect(toImmutableSet());

        assertThat(actualSchemas).containsAll(expectedSchemas);

        assertUpdate("DROP SCHEMA \"%s\"".formatted(nestedNamespace));
    }

    @Test
    public void testCreateTableWithMultipartNamespace()
    {
        String nestedNamespace = buildNamespace("level_3_%s".formatted(randomNameSuffix()), NamespaceLevel.THIRD);
        String tableFullPath = "\"%s\".%s".formatted(nestedNamespace, "test_table_%s".formatted(randomNameSuffix()));

        assertUpdate("CREATE SCHEMA \"%s\"".formatted(nestedNamespace));
        assertUpdate("CREATE TABLE %s (field_1 int, field_2 varchar)".formatted(tableFullPath));
        assertUpdate("INSERT INTO %s (field_1, field_2) VALUES (1, 'first_value')".formatted(tableFullPath), 1);
        assertUpdate("INSERT INTO %s (field_1, field_2) VALUES (2, 'second_value')".formatted(tableFullPath), 1);

        int rowsCount = computeActual("SELECT * FROM %s".formatted(tableFullPath)).getRowCount();

        assertThat(rowsCount).isEqualTo(2);
        assertUpdate("DROP TABLE %s".formatted(tableFullPath));
        assertUpdate("DROP SCHEMA \"%s\"".formatted(nestedNamespace));
    }

    @Test
    public void testCreateViewWithMultipartNamespace()
    {
        String nestedNamespace = buildNamespace("level_3_%s".formatted(randomNameSuffix()), NamespaceLevel.THIRD);
        String tableFullPath = "\"%s\".%s".formatted(nestedNamespace, "test_table_%s".formatted(randomNameSuffix()));
        String viewFullPath = "\"%s\".%s".formatted(nestedNamespace, "test_table_view_%s".formatted(randomNameSuffix()));

        assertUpdate("CREATE SCHEMA \"%s\"".formatted(nestedNamespace));
        assertUpdate("CREATE TABLE %s (field_1 int, field_2 varchar)".formatted(tableFullPath));
        assertUpdate("INSERT INTO %s (field_1, field_2) VALUES (1, '%s')".formatted(tableFullPath, randomNameSuffix()), 1);
        assertUpdate("INSERT INTO %s (field_1, field_2) VALUES (2, '%s')".formatted(tableFullPath, randomNameSuffix()), 1);
        assertUpdate("CREATE VIEW %s AS SELECT * FROM %s".formatted(viewFullPath, tableFullPath));

        int rowsCount = computeActual("SELECT * FROM %s".formatted(viewFullPath)).getRowCount();

        assertThat(rowsCount).isEqualTo(2);
        assertUpdate("DROP TABLE %s".formatted(tableFullPath));
        assertUpdate("DROP VIEW %s".formatted(viewFullPath));
        assertUpdate("DROP SCHEMA \"%s\"".formatted(nestedNamespace));
    }

    @Test
    public void testCascadeDeleteSchemaHavingNestedSchemas()
    {
        assertQueryFails(
                "DROP SCHEMA %s CASCADE".formatted(LEVEL_1_NAMESPACE),
                ".* schema: %s, contains %s .*".formatted(LEVEL_1_NAMESPACE, buildNamespace(LEVEL_2_NAMESPACE, NamespaceLevel.SECOND)));
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

    private String buildNamespace(String namespace, NamespaceLevel level)
    {
        return switch (level) {
            case FIRST -> namespace;
            case SECOND -> "%s.%s".formatted(LEVEL_1_NAMESPACE, namespace);
            case THIRD -> "%s.%s.%s".formatted(LEVEL_1_NAMESPACE, LEVEL_2_NAMESPACE, namespace);
        };
    }

    private enum NamespaceLevel
    {
        FIRST, SECOND, THIRD
    }
}
