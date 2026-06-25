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
package io.trino.plugin.deltalake;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.deltalake.TestingDeltaLakeUtils.getConnectorService;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@code cleanupFailedWrite()} preserves original source parquet
 * files when a DV-enabled UPDATE/MERGE fails due to transaction conflict.
 */
@Isolated
final class TestDeltaLakeDeletionVectorWriteCleanup
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DeltaLakeQueryRunner.builder()
                .addDeltaProperty("delta.enable-non-concurrent-writes", "true")
                .addDeltaProperty("delta.unique-table-location", "true")
                .build();
    }

    @Test
    void testConcurrentUpdatePreservesSourceFiles()
            throws Exception
    {
        String tableName = "test_dv_cleanup_" + randomNameSuffix();

        assertUpdate(
                "CREATE TABLE " + tableName + " (id INTEGER, value VARCHAR, category VARCHAR) " +
                        "WITH (deletion_vectors_enabled = true)");
        assertUpdate(
                "INSERT INTO " + tableName + " VALUES (1, 'a', 'X'), (2, 'b', 'X'), (3, 'c', 'Y')", 3);

        Set<String> originalFiles = getDataFilePaths(tableName);
        assertThat(originalFiles)
                .as("table should have data files after insert")
                .isNotEmpty();

        // Two concurrent UPDATEs on overlapping rows force a TransactionConflictException
        // in one writer. The failing writer calls cleanupFailedWrite().
        ExecutorService executor = newFixedThreadPool(2);
        CyclicBarrier barrier = new CyclicBarrier(2);
        try {
            Future<?> update1 = executor.submit(() -> {
                try {
                    barrier.await(30, SECONDS);
                    getQueryRunner().execute(
                            getSession(),
                            "UPDATE " + tableName + " SET value = 'thread1' WHERE category = 'X'");
                }
                catch (Exception _) {
                    // Expected: one thread gets TransactionConflictException
                }
            });

            Future<?> update2 = executor.submit(() -> {
                try {
                    barrier.await(30, SECONDS);
                    getQueryRunner().execute(
                            getSession(),
                            "UPDATE " + tableName + " SET value = 'thread2' WHERE category = 'X'");
                }
                catch (Exception _) {
                    // Expected: one thread gets TransactionConflictException
                }
            });

            update1.get(60, SECONDS);
            update2.get(60, SECONDS);
        }
        finally {
            executor.shutdownNow();
            executor.awaitTermination(10, SECONDS);
        }

        // Verify original data files still exist on the filesystem
        TrinoFileSystem fileSystem = getConnectorService(getQueryRunner(), TrinoFileSystemFactory.class)
                .create(ConnectorIdentity.ofUser("test"));

        for (String path : originalFiles) {
            assertThat(fileSystem.newInputFile(Location.of(path)).exists())
                    .as("Original data file must not be deleted by failed write cleanup: %s", path)
                    .isTrue();
        }

        // The table must remain fully readable
        assertThat(query("SELECT count(*) FROM " + tableName))
                .skippingTypesCheck()
                .matches("VALUES (BIGINT '3')");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testConcurrentMergePreservesSourceFiles()
            throws Exception
    {
        String targetTable = "test_dv_merge_" + randomNameSuffix();
        String sourceTable = "test_dv_merge_src_" + randomNameSuffix();

        assertUpdate(
                "CREATE TABLE " + targetTable + " (id INTEGER, value VARCHAR) " +
                        "WITH (deletion_vectors_enabled = true)");
        assertUpdate(
                "INSERT INTO " + targetTable + " VALUES (1, 'a'), (2, 'b'), (3, 'c')", 3);
        assertUpdate(
                "CREATE TABLE " + sourceTable + " (id INTEGER, value VARCHAR)");
        assertUpdate(
                "INSERT INTO " + sourceTable + " VALUES (1, 'merged'), (2, 'merged')", 2);

        Set<String> originalFiles = getDataFilePaths(targetTable);
        assertThat(originalFiles).isNotEmpty();

        ExecutorService executor = newFixedThreadPool(2);
        CyclicBarrier barrier = new CyclicBarrier(2);
        try {
            Future<?> merge1 = executor.submit(() -> {
                try {
                    barrier.await(30, SECONDS);
                    getQueryRunner().execute(getSession(),
                            "MERGE INTO " + targetTable + " t USING " + sourceTable + " s " +
                                    "ON t.id = s.id " +
                                    "WHEN MATCHED THEN UPDATE SET value = s.value");
                }
                catch (Exception _) {
                }
            });

            Future<?> merge2 = executor.submit(() -> {
                try {
                    barrier.await(30, SECONDS);
                    getQueryRunner().execute(getSession(),
                            "MERGE INTO " + targetTable + " t USING " + sourceTable + " s " +
                                    "ON t.id = s.id " +
                                    "WHEN MATCHED THEN UPDATE SET value = concat(s.value, '_v2')");
                }
                catch (Exception _) {
                }
            });

            merge1.get(60, SECONDS);
            merge2.get(60, SECONDS);
        }
        finally {
            executor.shutdownNow();
            executor.awaitTermination(10, SECONDS);
        }

        TrinoFileSystem fileSystem = getConnectorService(getQueryRunner(), TrinoFileSystemFactory.class)
                .create(ConnectorIdentity.ofUser("test"));

        for (String path : originalFiles) {
            assertThat(fileSystem.newInputFile(Location.of(path)).exists())
                    .as("Original data file must not be deleted by failed MERGE cleanup: %s", path)
                    .isTrue();
        }

        assertThat(query("SELECT count(*) FROM " + targetTable))
                .skippingTypesCheck()
                .matches("VALUES (BIGINT '3')");

        assertUpdate("DROP TABLE " + sourceTable);
        assertUpdate("DROP TABLE " + targetTable);
    }

    private Set<String> getDataFilePaths(String tableName)
    {
        MaterializedResult result = computeActual(
                "SELECT DISTINCT \"$path\" FROM " + tableName);
        return result.getMaterializedRows().stream()
                .map(row -> (String) row.getField(0))
                .collect(toImmutableSet());
    }
}
