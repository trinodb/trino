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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.execution.QueryStats;
import io.trino.spi.QueryId;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for Copy-on-Write DELETE operations in Iceberg.
 */
@TestInstance(Lifecycle.PER_CLASS)
public class TestIcebergCopyOnWriteIntegration
        extends AbstractTestQueryFramework
{
    @TempDir
    public Path tempDir;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .addIcebergProperty("iceberg.format-version", "2")
                .build();
    }

    @Test
    public void testCowDeleteDataConsistency()
            throws Exception
    {
        String tableName = "test_cow_delete_consistency_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INT, name VARCHAR, value INT) " +
                "WITH (format_version = 2, write_delete_mode = 'COPY_ON_WRITE')");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a', 100), (2, 'b', 200), (3, 'c', 300), (4, 'd', 400)", 4);

        assertUpdate("DELETE FROM " + tableName + " WHERE id = 2", 1);

        assertQuery(
                "SELECT * FROM " + tableName + " ORDER BY id",
                "VALUES (1, 'a', 100), (3, 'c', 300), (4, 'd', 400)");

        // CoW invariant: no delete files (content != 0) in $files
        assertQuery(
                "SELECT count(*) FROM \"" + tableName + "$files\" WHERE content != 0",
                "VALUES BIGINT '0'");

        // Exactly one rewritten data file
        assertQuery(
                "SELECT count(*) FROM \"" + tableName + "$files\"",
                "VALUES BIGINT '1'");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testConcurrentDeleteOperations()
            throws Exception
    {
        String tableName = "test_concurrent_deletes_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INT, region VARCHAR, value INT) " +
                "WITH (format_version = 2, write_delete_mode = 'COPY_ON_WRITE', partitioning = ARRAY['region'])");

        assertUpdate(
                "INSERT INTO " + tableName + " VALUES " +
                        "(1, 'US', 100), (2, 'US', 200), (3, 'US', 300)," +
                        "(4, 'EU', 400), (5, 'EU', 500), (6, 'EU', 600)," +
                        "(7, 'ASIA', 700), (8, 'ASIA', 800), (9, 'ASIA', 900)",
                9);

        ExecutorService executor = Executors.newFixedThreadPool(3);
        try {
            QueryRunner testQueryRunner = getQueryRunner();
            List<Callable<MaterializedResult>> tasks = ImmutableList.of(
                    () -> testQueryRunner.execute("DELETE FROM " + tableName + " WHERE region = 'US' AND id = 2"),
                    () -> testQueryRunner.execute("DELETE FROM " + tableName + " WHERE region = 'EU' AND id = 5"),
                    () -> testQueryRunner.execute("DELETE FROM " + tableName + " WHERE region = 'ASIA' AND id = 8"));

            List<Future<MaterializedResult>> futures = executor.invokeAll(tasks, 30, SECONDS);

            for (Future<MaterializedResult> future : futures) {
                MaterializedResult result = future.get();
                assertThat(result.getUpdateCount()).hasValue(1);
            }
        }
        finally {
            executor.shutdown();
        }

        assertQuery("SELECT id FROM " + tableName + " WHERE region = 'US' ORDER BY id", "VALUES (1), (3)");
        assertQuery("SELECT id FROM " + tableName + " WHERE region = 'EU' ORDER BY id", "VALUES (4), (6)");
        assertQuery("SELECT id FROM " + tableName + " WHERE region = 'ASIA' ORDER BY id", "VALUES (7), (9)");

        // CoW invariant: no delete files across all partitions
        assertQuery(
                "SELECT count(*) FROM \"" + tableName + "$files\" WHERE content != 0",
                "VALUES BIGINT '0'");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testSnapshotIsolation()
            throws Exception
    {
        String tableName = "test_snapshot_isolation_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INT, name VARCHAR, value INT) " +
                "WITH (format_version = 2, write_delete_mode = 'COPY_ON_WRITE')");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a', 100), (2, 'b', 200), (3, 'c', 300)", 3);

        // Record the snapshot ID before the delete
        long snapshotId = (Long) computeScalar(
                "SELECT snapshot_id FROM \"" + tableName + "$snapshots\" ORDER BY committed_at DESC LIMIT 1");

        assertUpdate("DELETE FROM " + tableName + " WHERE id = 2", 1);

        // Current table reflects the delete
        assertQuery(
                "SELECT * FROM " + tableName + " ORDER BY id",
                "VALUES (1, 'a', 100), (3, 'c', 300)");

        // Historical snapshot still shows all 3 original rows
        assertQuery(
                "SELECT * FROM " + tableName + " FOR VERSION AS OF " + snapshotId + " ORDER BY id",
                "VALUES (1, 'a', 100), (2, 'b', 200), (3, 'c', 300)");

        // CoW invariant: no delete files
        assertQuery(
                "SELECT count(*) FROM \"" + tableName + "$files\" WHERE content != 0",
                "VALUES BIGINT '0'");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCowSnapshotHistory()
            throws Exception
    {
        String tableName = "test_cow_snapshot_history_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INT, name VARCHAR, value INT) " +
                "WITH (format_version = 2, write_delete_mode = 'COPY_ON_WRITE')");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a', 100), (2, 'b', 200), (3, 'c', 300)", 3);

        long snapshotCountAfterInsert = (Long) computeScalar(
                "SELECT count(*) FROM \"" + tableName + "$snapshots\"");

        assertUpdate("DELETE FROM " + tableName + " WHERE id = 1", 1);

        long snapshotCountAfterFirstDelete = (Long) computeScalar(
                "SELECT count(*) FROM \"" + tableName + "$snapshots\"");
        assertThat(snapshotCountAfterFirstDelete).isGreaterThan(snapshotCountAfterInsert);

        assertUpdate("DELETE FROM " + tableName + " WHERE id = 3", 1);

        long snapshotCountAfterSecondDelete = (Long) computeScalar(
                "SELECT count(*) FROM \"" + tableName + "$snapshots\"");
        assertThat(snapshotCountAfterSecondDelete).isGreaterThan(snapshotCountAfterFirstDelete);

        // At least 3 snapshots: 1 insert + 2 deletes
        assertThat(snapshotCountAfterSecondDelete).isGreaterThanOrEqualTo(3);

        // CoW invariant: no delete files across entire history
        assertQuery(
                "SELECT count(*) FROM \"" + tableName + "$files\" WHERE content != 0",
                "VALUES BIGINT '0'");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testVerifyLogging()
            throws Exception
    {
        String tableName = "test_verify_logging_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INT, name VARCHAR, value INT) " +
                "WITH (format_version = 2, write_delete_mode = 'COPY_ON_WRITE')");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a', 100), (2, 'b', 200), (3, 'c', 300), (4, 'd', 400), (5, 'e', 500)", 5);

        MaterializedResultWithPlan result = executeWithPlan("DELETE FROM " + tableName + " WHERE id IN (1, 3, 5)");
        assertThat(result.result().getUpdateCount()).hasValue(3);

        QueryStats queryStats = getQueryStats(result.queryId());
        assertThat(queryStats.getPhysicalInputDataSize()).isGreaterThan(DataSize.ofBytes(0));
        assertThat(queryStats.getPhysicalWrittenDataSize()).isGreaterThan(DataSize.ofBytes(0));

        // CoW invariant: no delete files
        assertQuery(
                "SELECT count(*) FROM \"" + tableName + "$files\" WHERE content != 0",
                "VALUES BIGINT '0'");

        assertUpdate("DROP TABLE " + tableName);
    }

    private QueryStats getQueryStats(QueryId queryId)
    {
        DistributedQueryRunner runner = (DistributedQueryRunner) getQueryRunner();
        return runner.getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(queryId)
                .getQueryStats();
    }

    private MaterializedResultWithPlan executeWithPlan(String sql)
    {
        DistributedQueryRunner runner = (DistributedQueryRunner) getQueryRunner();
        return runner.executeWithPlan(getSession(), sql);
    }

    private static String randomNameSuffix()
    {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    }
}
