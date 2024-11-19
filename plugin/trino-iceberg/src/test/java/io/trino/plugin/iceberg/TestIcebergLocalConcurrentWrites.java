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
import io.airlift.concurrent.MoreFutures;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static io.trino.testing.QueryAssertions.getTrinoExceptionCause;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
final class TestIcebergLocalConcurrentWrites
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder().build();
    }

    @Test
    void testConcurrentInserts()
            throws Exception
    {
        testConcurrentInserts(false);
        testConcurrentInserts(true);
    }

    private void testConcurrentInserts(boolean partitioned)
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_inserts_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a INT, part INT) " +
                (partitioned ? " WITH (partitioning = ARRAY['part'])" : ""));

        try {
            // insert data concurrently
            executor.invokeAll(ImmutableList.<Callable<Void>>builder()
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("INSERT INTO " + tableName + " VALUES (1, 10)");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("INSERT INTO " + tableName + " VALUES (11, 20)");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("INSERT INTO " + tableName + " VALUES (21, 30)");
                                return null;
                            })
                            .build())
                    .forEach(MoreFutures::getDone);

            assertThat(query("SELECT * FROM " + tableName)).matches("VALUES (1, 10), (11, 20), (21, 30)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @Test
    void testConcurrentInsertsSelectingFromTheSameTable()
            throws Exception
    {
        testConcurrentInsertsSelectingFromTheSameTable(true);
        testConcurrentInsertsSelectingFromTheSameTable(false);
    }

    private void testConcurrentInsertsSelectingFromTheSameTable(boolean partitioned)
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_inserts_select_from_same_table_" + randomNameSuffix();

        assertUpdate(
                "CREATE TABLE " + tableName + " (a, part) " + (partitioned ? " WITH (partitioning = ARRAY['part'])" : "") + "  AS VALUES (0, 10)",
                1);

        try {
            List<Future<Boolean>> futures = IntStream.range(0, threads)
                    .mapToObj(_ -> executor.submit(() -> {
                        barrier.await(10, SECONDS);
                        getQueryRunner().execute("INSERT INTO " + tableName + " SELECT COUNT(*), 10 AS part FROM " + tableName);
                        return true;
                    }))
                    .collect(toImmutableList());

            long successfulInsertsCount = futures.stream()
                    .map(MoreFutures::getFutureValue)
                    .filter(success -> success)
                    .count();

            assertThat(successfulInsertsCount).isEqualTo(3);
            // Queries in Iceberg have snapshot isolation, so all writes are done with data available at beginning of transaction
            assertQuery(
                    "SELECT * FROM " + tableName,
                    "VALUES (0, 10), (1, 10), (1, 10), (1, 10)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @Test
    void testConcurrentInsertsSelectingFromTheSameVersionedTable()
            throws Exception
    {
        testConcurrentInsertsSelectingFromTheSameVersionedTable(true);
        testConcurrentInsertsSelectingFromTheSameVersionedTable(false);
    }

    private void testConcurrentInsertsSelectingFromTheSameVersionedTable(boolean partitioned)
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_inserts_select_from_same_versioned_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a, part) " + (partitioned ? " WITH (partitioning = ARRAY['part'])" : "") + "  AS VALUES (0, 'a')", 1);

        long currentSnapshotId = getCurrentSnapshotId(tableName);

        try {
            executor.invokeAll(ImmutableList.<Callable<Void>>builder()
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("INSERT INTO " + tableName + " SELECT 1, 'b' AS part FROM " + tableName + " FOR VERSION AS OF " + currentSnapshotId);
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("INSERT INTO " + tableName + " SELECT 2, 'c' AS part FROM " + tableName + " FOR VERSION AS OF " + currentSnapshotId);
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("INSERT INTO " + tableName + " SELECT 3, 'd' AS part FROM " + tableName + " FOR VERSION AS OF " + currentSnapshotId);
                                return null;
                            })
                            .build())
                    .forEach(MoreFutures::getDone);

            assertQuery("SELECT * FROM " + tableName, "VALUES (0, 'a'), (1, 'b'), (2, 'c'), (3, 'd')");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @Test
    void testConcurrentDelete()
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_deletes_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a, part)  WITH (partitioning = ARRAY['part']) AS VALUES (1, 10), (11, 20), (21, 30), (31, 40)", 4);

        try {
            // delete data concurrently by using non-overlapping partition predicate
            executor.invokeAll(ImmutableList.<Callable<Void>>builder()
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("DELETE FROM " + tableName + " WHERE part = 10");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("DELETE FROM " + tableName + " WHERE part = 20");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("DELETE FROM " + tableName + " WHERE part = 30");
                                return null;
                            })
                            .build())
                    .forEach(MoreFutures::getDone);

            assertThat(query("SELECT * FROM " + tableName)).matches("VALUES (31, 40)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @Test
    void testConcurrentDeleteFromTheSamePartition()
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_delete_from_same_partition_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a, part) WITH (partitioning = ARRAY['part']) AS VALUES (0, 10), (11, 20), (22, 30)", 3);

        try {
            List<Future<Boolean>> futures = IntStream.range(0, threads)
                    .mapToObj(threadNumber -> executor.submit(() -> {
                        barrier.await(10, SECONDS);
                        getQueryRunner().execute("DELETE FROM " + tableName + "  WHERE part = 10");
                        return true;
                    }))
                    .collect(toImmutableList());

            long successfulDeletesCount = futures.stream()
                    .map(MoreFutures::getFutureValue)
                    .filter(success -> success)
                    .count();

            assertThat(successfulDeletesCount).isEqualTo(3);
            assertQuery("SELECT * FROM " + tableName, "VALUES (11, 20), (22, 30)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @Test
    void testConcurrentTruncate()
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_truncate_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a, part) WITH (partitioning = ARRAY['part']) AS VALUES (0, 10), (11, 20), (22, 30)", 3);

        try {
            List<Future<Boolean>> futures = IntStream.range(0, threads)
                    .mapToObj(_ -> executor.submit(() -> {
                        barrier.await(10, SECONDS);
                        getQueryRunner().execute("TRUNCATE TABLE " + tableName);
                        return true;
                    }))
                    .collect(toImmutableList());

            long successfulTruncatesCount = futures.stream()
                    .map(MoreFutures::getFutureValue)
                    .filter(success -> success)
                    .count();

            assertThat(successfulTruncatesCount).isEqualTo(3);
            assertThat(query("SELECT * FROM " + tableName)).returnsEmptyResult();
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @Test
    void testConcurrentTruncateAndInserts()
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_truncate_and_inserts_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a, part)  WITH (partitioning = ARRAY['part']) AS VALUES (1, 10), (11, 20)", 2);

        try {
            // truncate data while concurrently adding new inserts
            executor.invokeAll(ImmutableList.<Callable<Void>>builder()
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("TRUNCATE TABLE " + tableName);
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("INSERT INTO " + tableName + " VALUES (21, 30)");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("INSERT INTO " + tableName + " VALUES (31, 40)");
                                return null;
                            })
                            .build())
                    .forEach(MoreFutures::getDone);

            assertQuery("SELECT * FROM " + tableName, "VALUES (21, 30), (31, 40)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @Test
    void testConcurrentNonOverlappingUpdate()
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_non_overlapping_updates_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a, part)  WITH (partitioning = ARRAY['part']) AS VALUES (1, 10), (11, 20), (21, 30), (31, 40)", 4);

        try {
            // update data concurrently by using non-overlapping partition predicate
            executor.invokeAll(ImmutableList.<Callable<Void>>builder()
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("UPDATE " + tableName + " SET a = a + 1 WHERE part = 10");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("UPDATE " + tableName + " SET a = a + 1  WHERE part = 20");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("UPDATE " + tableName + " SET a = a + 1  WHERE part = 30");
                                return null;
                            })
                            .build())
                    .forEach(MoreFutures::getDone);

            assertThat(query("SELECT * FROM " + tableName)).matches("VALUES (2, 10), (12, 20), (22, 30), (31, 40)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @Test
    void testConcurrentOverlappingUpdate()
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_overlapping_updates_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a, part)  WITH (partitioning = ARRAY['part']) AS VALUES (1, 10), (11, 20), (21, 30), (31, 40)", 4);

        try {
            List<Future<Boolean>> futures = IntStream.range(0, threads)
                    .mapToObj(_ -> executor.submit(() -> {
                        barrier.await(10, SECONDS);
                        try {
                            getQueryRunner().execute("UPDATE " + tableName + " SET a = a + 1 WHERE a > 11");
                            return true;
                        }
                        catch (Exception e) {
                            RuntimeException trinoException = getTrinoExceptionCause(e);
                            try {
                                assertThat(trinoException).hasMessageMatching("Failed to commit the transaction during write.*|" +
                                        "Failed to commit during write.*");
                            }
                            catch (Throwable verifyFailure) {
                                if (verifyFailure != e) {
                                    verifyFailure.addSuppressed(e);
                                }
                                throw verifyFailure;
                            }
                            return false;
                        }
                    }))
                    .collect(toImmutableList());

            long successes = futures.stream()
                    .map(future -> tryGetFutureValue(future, 10, SECONDS).orElseThrow(() -> new RuntimeException("Wait timed out")))
                    .filter(success -> success)
                    .count();

            assertThat(successes).isGreaterThanOrEqualTo(1);
            //There can be different possible results depending on query order execution.
            switch ((int) successes) {
                case 1 -> assertThat(query("SELECT * FROM " + tableName)).matches("VALUES (1, 10), (11, 20), (22, 30), (32, 40)");
                case 2 -> assertThat(query("SELECT * FROM " + tableName)).matches("VALUES (1, 10), (11, 20), (23, 30), (33, 40)");
                case 3 -> assertThat(query("SELECT * FROM " + tableName)).matches("VALUES (1, 10), (11, 20), (24, 30), (34, 40)");
            }
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @Test
    void testConcurrentNonOverlappingUpdateOnNestedPartition()
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_non_overlapping_updates_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a int, parent ROW(child int))  WITH (partitioning = ARRAY['\"parent.child\"'])");
        assertUpdate(
                "INSERT INTO " + tableName + " VALUES " +
                        "(1, ROW(10)), " +
                        "(11, ROW(20)), " +
                        "(21, ROW(30)), " +
                        "(31, ROW(40))",
                4);
        try {
            // update data concurrently by using non-overlapping partition predicate
            executor.invokeAll(ImmutableList.<Callable<Void>>builder()
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("UPDATE " + tableName + " SET a = a + 1 WHERE parent.child = 10");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("UPDATE " + tableName + " SET a = a + 1  WHERE parent.child = 20");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("UPDATE " + tableName + " SET a = a + 1  WHERE parent.child = 30");
                                return null;
                            })
                            .build())
                    .forEach(MoreFutures::getDone);

            assertThat(query("SELECT a, parent.child FROM " + tableName)).matches("VALUES (2, 10), (12, 20), (22, 30), (31, 40)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @Test
    void testConcurrentDeleteAndInserts()
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_delete_and_inserts_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a, part)  WITH (partitioning = ARRAY['part']) AS VALUES (1, 10), (11, 20)", 2);

        try {
            // Use a WHERE predicate in the DELETE statement which involves scanning the whole table while concurrently adding new inserts
            List<Future<Boolean>> futures = executor.invokeAll(ImmutableList.<Callable<Boolean>>builder()
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                // DELETE will in most cases conflict with (21, 30) insert
                                try {
                                    getQueryRunner().execute("DELETE FROM " + tableName + " WHERE a > 10");
                                    return true;
                                }
                                catch (Exception e) {
                                    RuntimeException trinoException = getTrinoExceptionCause(e);
                                    try {
                                        assertThat(trinoException).hasMessageMatching("Failed to commit the transaction during write.*|" +
                                                "Failed to commit during write.*");
                                    }
                                    catch (Throwable verifyFailure) {
                                        if (verifyFailure != e) {
                                            verifyFailure.addSuppressed(e);
                                        }
                                        throw verifyFailure;
                                    }
                                    return false;
                                }
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("INSERT INTO " + tableName + " VALUES (8, 10)");
                                return true;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("INSERT INTO " + tableName + " VALUES (21, 30)");
                                return true;
                            })
                            .build())
                    .stream().collect(toImmutableList());

            long successfulWrites = futures.stream()
                    .map(future -> tryGetFutureValue(future, 10, SECONDS).orElseThrow(() -> new RuntimeException("Wait timed out")))
                    .filter(success -> success)
                    .count();

            assertThat(successfulWrites).isGreaterThanOrEqualTo(2);

            //There can be different possible results depending on query order execution.
            if (successfulWrites == 2) {
                // If all queries starts at the same time DELETE will fail and results are:
                assertThat(query("SELECT * FROM " + tableName)).matches("VALUES (1, 10), (8, 10), (11, 20), (21, 30)");
            }
            else {
                // If DELETE is executed after INSERTS:
                MaterializedResult expected1 = computeActual("VALUES (1, 10), (8, 10)");
                // If DELETE is executed before INSERTS:
                MaterializedResult expected2 = computeActual("VALUES (1, 10), (8, 10), (21, 30)");
                assertThat(computeActual("SELECT * FROM " + tableName + " ORDER BY a"))
                        .isIn(expected1, expected2);
            }
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @Test
    void testConcurrentUpdateAndInserts()
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_update_and_inserts_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a, part)  WITH (partitioning = ARRAY['part']) AS VALUES (1, 10), (11, 20)", 2);

        try {
            // Use a WHERE predicate in the UPDATE statement which involves scanning the whole table while concurrently adding new inserts
            List<Future<Boolean>> futures = executor.invokeAll(ImmutableList.<Callable<Boolean>>builder()
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                try {
                                    getQueryRunner().execute("UPDATE " + tableName + " SET a = a + 1");
                                    return true;
                                }
                                catch (Exception e) {
                                    RuntimeException trinoException = getTrinoExceptionCause(e);
                                    try {
                                        assertThat(trinoException).hasMessageMatching("Failed to commit the transaction during write.*|" +
                                                "Failed to commit during write.*");
                                    }
                                    catch (Throwable verifyFailure) {
                                        if (verifyFailure != e) {
                                            verifyFailure.addSuppressed(e);
                                        }
                                        throw verifyFailure;
                                    }
                                    return false;
                                }
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                Thread.sleep(1000);
                                getQueryRunner().execute("INSERT INTO " + tableName + " VALUES (13, 20)");
                                return true;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                Thread.sleep(1000);
                                getQueryRunner().execute("INSERT INTO " + tableName + " VALUES (21, 30)");
                                return true;
                            })
                            .build())
                    .stream().collect(toImmutableList());

            long successfulWrites = futures.stream()
                    .map(future -> tryGetFutureValue(future, 10, SECONDS).orElseThrow(() -> new RuntimeException("Wait timed out")))
                    .filter(success -> success)
                    .count();

            assertThat(successfulWrites).isGreaterThanOrEqualTo(2);

            //There can be different possible results depending on query order execution.
            if (successfulWrites == 2) {
                // If all queries starts at the same time UPDATE will fail and results are:
                assertThat(query("SELECT * FROM " + tableName)).matches("VALUES (1, 10), (11, 20), (13, 20), (21, 30)");
            }
            else {
                // If UPDATE is executed after INSERTS:
                MaterializedResult expected1 = computeActual("VALUES (2, 10), (12, 20), (13, 20), (21, 30)");
                // If UPDATE is executed before INSERTS:
                MaterializedResult expected2 = computeActual("VALUES (2, 10), (12, 20), (14, 20), (22, 30)");
                assertThat(computeActual("SELECT * FROM " + tableName + " ORDER BY a"))
                        .isIn(expected1, expected2);
            }
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @Test
    void testConcurrentMergeAndInserts()
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_merge_and_inserts_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a, part)  WITH (partitioning = ARRAY['part']) AS VALUES (1, 10), (11, 20)", 2);

        try {
            List<Future<Boolean>> futures = executor.invokeAll(ImmutableList.<Callable<Boolean>>builder()
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                try {
                                    getQueryRunner().execute(
                                            """
                                                    MERGE INTO %s t USING (VALUES (11, 20), (8, 10), (21, 30)) AS s(a, part)
                                                      ON (t.a = s.a AND t.part = s.part)
                                                        WHEN MATCHED THEN DELETE
                                                    """.formatted(tableName));
                                    return true;
                                }
                                catch (Exception e) {
                                    RuntimeException trinoException = getTrinoExceptionCause(e);
                                    try {
                                        assertThat(trinoException).hasMessageMatching("Failed to commit the transaction during write.*|" +
                                                "Failed to commit during write.*");
                                    }
                                    catch (Throwable verifyFailure) {
                                        if (verifyFailure != e) {
                                            verifyFailure.addSuppressed(e);
                                        }
                                        throw verifyFailure;
                                    }
                                    return false;
                                }
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("INSERT INTO " + tableName + " VALUES (8, 10)");
                                return true;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("INSERT INTO " + tableName + " VALUES (21, 30)");
                                return true;
                            })
                            .build())
                    .stream().collect(toImmutableList());

            long successfulWrites = futures.stream()
                    .map(future -> tryGetFutureValue(future, 10, SECONDS).orElseThrow(() -> new RuntimeException("Wait timed out")))
                    .filter(success -> success)
                    .count();

            assertThat(successfulWrites).isGreaterThanOrEqualTo(2);

            //There can be different possible results depending on query order execution.
            if (successfulWrites == 2) {
                // If all queries starts at the same time MERGE will fail and results are:
                assertThat(query("SELECT * FROM " + tableName)).matches("VALUES (1, 10), (11, 20), (8, 10), (21, 30)");
            }
            else {
                // If MERGE is executed after INSERTS:
                MaterializedResult expected1 = computeActual("VALUES (1, 10)");
                // If MERGE is executed before INSERTS:
                MaterializedResult expected2 = computeActual("VALUES (1, 10), (8, 10), (21, 30)");
                assertThat(computeActual("SELECT * FROM " + tableName + " ORDER BY a"))
                        .isIn(expected1, expected2);
            }
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @Test
    void testConcurrentDeleteAndDeletePushdownAndInsert()
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_delete_and_inserts_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a, part)  WITH (partitioning = ARRAY['part']) AS VALUES (1, 10), (11, 20), (21, 30)", 3);
        // Add more files in the partition 10
        assertUpdate("INSERT INTO " + tableName + " VALUES (2, 10)", 1);

        try {
            // The DELETE and INSERT operation operate on non-overlapping partitions
            executor.invokeAll(ImmutableList.<Callable<Void>>builder()
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                // Use a non-partition filter as well to ensure the DELETE operation is not being pushed down
                                getQueryRunner().execute("DELETE FROM " + tableName + " WHERE part = 10 AND a IN (1, 2)");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("INSERT INTO " + tableName + " SELECT a + 1, part FROM " + tableName + " WHERE part = 20");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("DELETE FROM " + tableName + " WHERE part = 30");
                                return null;
                            })
                            .build())
                    .forEach(MoreFutures::getDone);

            assertThat(query("SELECT * FROM " + tableName)).matches("VALUES (11, 20), (12, 20)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    private long getCurrentSnapshotId(String tableName)
    {
        return (long) computeScalar("SELECT snapshot_id FROM \"" + tableName + "$snapshots\" ORDER BY committed_at DESC FETCH FIRST 1 ROW WITH TIES");
    }
}
