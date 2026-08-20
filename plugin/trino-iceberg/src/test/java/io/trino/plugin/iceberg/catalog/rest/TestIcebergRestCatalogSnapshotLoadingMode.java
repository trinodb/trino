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
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.rest.DelegatingRestSessionCatalog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.iceberg.catalog.rest.RestCatalogTestUtils.backendCatalog;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ofPattern;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * Verifies that the REST catalog works end to end with {@code iceberg.rest-catalog.snapshot-loading-mode=REFS}.
 * The testing REST server honors the {@code snapshots=refs} query parameter and prunes historical snapshots
 * from responses, so this exercises real lazy snapshot loading for queries that need the full snapshot
 * history, such as time travel and the snapshot metadata tables.
 */
@TestInstance(PER_CLASS)
final class TestIcebergRestCatalogSnapshotLoadingMode
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Path warehouseLocation = Files.createTempDirectory(null);
        closeAfterClass(() -> deleteRecursively(warehouseLocation, ALLOW_INSECURE));

        JdbcCatalog backend = closeAfterClass((JdbcCatalog) backendCatalog(warehouseLocation));

        DelegatingRestSessionCatalog delegatingCatalog = DelegatingRestSessionCatalog.builder()
                .delegate(backend)
                .build();

        TestingHttpServer testServer = delegatingCatalog.testServer();
        testServer.start();
        closeAfterClass(testServer::stop);

        return IcebergQueryRunner.builder()
                .setBaseDataDir(Optional.of(warehouseLocation))
                .addIcebergProperty("iceberg.catalog.type", "rest")
                .addIcebergProperty("iceberg.rest-catalog.uri", testServer.getBaseUrl().toString())
                .addIcebergProperty("iceberg.rest-catalog.snapshot-loading-mode", "REFS")
                .addIcebergProperty("fs.hadoop.enabled", "true")
                // The testing REST server fails to serve snapshots=refs responses for tables with
                // statistics files on historical snapshots, see https://github.com/apache/iceberg/issues/17538
                .addIcebergProperty("iceberg.extended-statistics.collect-on-write", "false")
                .build();
    }

    @Test
    void testSnapshotHistoryQueries()
    {
        String tableName = "test_snapshot_loading_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (a BIGINT)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (2)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (3)", 1);

        assertQuery("SELECT * FROM " + tableName, "VALUES (1), (2), (3)");

        assertQuery("SELECT count(*) FROM \"" + tableName + "$snapshots\"", "VALUES 4");
        assertQuery("SELECT count(*) FROM \"" + tableName + "$history\"", "VALUES 4");
        assertThat(computeActual("SELECT * FROM \"" + tableName + "$refs\"").getRowCount()).isEqualTo(1);

        long firstInsertSnapshotId = (long) computeScalar(
                "SELECT snapshot_id FROM \"" + tableName + "$snapshots\" ORDER BY committed_at OFFSET 1 LIMIT 1");
        assertQuery(format("SELECT * FROM %s FOR VERSION AS OF %d", tableName, firstInsertSnapshotId), "VALUES (1)");

        ZonedDateTime secondInsertTime = (ZonedDateTime) computeScalar(
                "SELECT committed_at FROM \"" + tableName + "$snapshots\" ORDER BY committed_at OFFSET 2 LIMIT 1");
        assertQuery(
                format("SELECT * FROM %s FOR TIMESTAMP AS OF TIMESTAMP '%s'", tableName, secondInsertTime.format(ofPattern("yyyy-MM-dd HH:mm:ss.SSS VV"))),
                "VALUES (1), (2)");

        assertUpdate("DROP TABLE " + tableName);
    }
}
