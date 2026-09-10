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
import io.trino.plugin.iceberg.BaseIcebergConnectorSmokeTest;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.QueryFailedException;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.rest.DelegatingRestSessionCatalog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.iceberg.catalog.rest.RestCatalogTestUtils.backendCatalog;
import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ofPattern;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestIcebergRestCatalogRefsSnapshotLoadingModeConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    @TempDir
    private static Path warehouseLocation;
    private JdbcCatalog backend;

    public TestIcebergRestCatalogRefsSnapshotLoadingModeConnectorSmokeTest()
    {
        super(new IcebergConfig().getFileFormat().toIceberg());
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_CREATE_MATERIALIZED_VIEW,
                 SUPPORTS_RENAME_SCHEMA -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
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
                                .put("iceberg.rest-catalog.snapshot-loading-mode", "REFS")
                                .put("iceberg.register-table-procedure.enabled", "true")
                                .put("iceberg.writer-sort-buffer-size", "1MB")
                                // REST servers built on Iceberg's CatalogHandlers fail to load tables with statistics files
                                // on historical snapshots in REFS mode, see https://github.com/apache/iceberg/issues/17538
                                .put("iceberg.extended-statistics.collect-on-write", "false")
                                .buildOrThrow())
                .addIcebergProperty("fs.hadoop.enabled", "true")
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Test
    void testSnapshotHistoryQueries()
    {
        // The rows are inserted one by one to create a separate snapshot per row
        try (TestTable table = newTrinoTable("refs_snapshot_loading_", "(a BIGINT)")) {
            String tableName = table.getName();
            assertUpdate("INSERT INTO " + tableName + " VALUES (1)", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2)", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (3)", 1);

            assertThat(query("SELECT * FROM " + tableName))
                    .matches("VALUES BIGINT '1', BIGINT '2', BIGINT '3'");

            assertThat(query("SELECT count(*) FROM \"" + tableName + "$snapshots\""))
                    .matches("VALUES BIGINT '4'");
            assertThat(query("SELECT count(*) FROM \"" + tableName + "$history\""))
                    .matches("VALUES BIGINT '4'");
            assertThat(computeActual("SELECT * FROM \"" + tableName + "$refs\"").getRowCount()).isEqualTo(1);

            long firstInsertSnapshotId = (long) computeScalar(
                    "SELECT snapshot_id FROM \"" + tableName + "$snapshots\" ORDER BY committed_at OFFSET 1 LIMIT 1");
            assertThat(query(format("SELECT * FROM %s FOR VERSION AS OF %d", tableName, firstInsertSnapshotId)))
                    .matches("VALUES BIGINT '1'");

            ZonedDateTime secondInsertTime = (ZonedDateTime) computeScalar(
                    "SELECT committed_at FROM \"" + tableName + "$snapshots\" ORDER BY committed_at OFFSET 2 LIMIT 1");
            assertThat(query(format("SELECT * FROM %s FOR TIMESTAMP AS OF TIMESTAMP '%s'", tableName, secondInsertTime.format(ofPattern("yyyy-MM-dd HH:mm:ss.SSS VV")))))
                    .matches("VALUES BIGINT '1', BIGINT '2'");
        }
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
                .hasMessageMatching("Failed to open input stream for file: .*avro")
                .hasNoCause();
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
    protected void dropTableFromCatalog(String tableName)
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
        return Files.exists(Path.of(location));
    }

    private TableIdentifier toIdentifier(String tableName)
    {
        return TableIdentifier.of(getSession().getSchema().orElseThrow(), tableName);
    }
}
