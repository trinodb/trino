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

import com.google.common.collect.ImmutableList;
import com.google.common.reflect.ClassPath;
import io.airlift.concurrent.MoreFutures;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.deltalake.TestingDeltaLakeUtils.getConnectorService;
import static io.trino.testing.QueryAssertions.getTrinoExceptionCause;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.regex.Matcher.quoteReplacement;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestDeltaLakeLocalConcurrentWritesTest
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DeltaLakeQueryRunner.builder()
                .addDeltaProperty("delta.unique-table-location", "true")
                .addDeltaProperty("delta.register-table-procedure.enabled", "true")
                .build();
    }

    // Copied from BaseDeltaLakeConnectorSmokeTest
    @Test
    public void testConcurrentInsertsReconciliationForBlindInserts()
            throws Exception
    {
        testConcurrentInsertsReconciliationForBlindInserts(false);
        testConcurrentInsertsReconciliationForBlindInserts(true);
    }

    private void testConcurrentInsertsReconciliationForBlindInserts(boolean partitioned)
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_inserts_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a INT, part INT) " +
                (partitioned ? " WITH (partitioned_by = ARRAY['part'])" : ""));

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
            assertQuery(
                    "SELECT version, operation, isolation_level, read_version, is_blind_append FROM \"" + tableName + "$history\"",
                    """
                    VALUES
                        (0, 'CREATE TABLE', 'WriteSerializable', 0, true),
                        (1, 'WRITE', 'WriteSerializable', 0, true),
                        (2, 'WRITE', 'WriteSerializable', 1, true),
                        (3, 'WRITE', 'WriteSerializable', 2, true)
                    """);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    // Copied from BaseDeltaLakeConnectorSmokeTest
    @Test
    public void testConcurrentInsertsSelectingFromTheSameTable()
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
                "CREATE TABLE " + tableName + " (a, part) " + (partitioned ? " WITH (partitioned_by = ARRAY['part'])" : "") + "  AS VALUES (0, 10)",
                1);

        try {
            // Considering T1, T2, T3 being the order of completion of the concurrent INSERT operations,
            // if all the operations would eventually succeed, the entries inserted per thread would look like this:
            // T1: (1, 10)
            // T2: (2, 10)
            // T3: (3, 10)
            List<Future<Boolean>> futures = IntStream.range(0, threads)
                    .mapToObj(threadNumber -> executor.submit(() -> {
                        barrier.await(10, SECONDS);
                        try {
                            getQueryRunner().execute("INSERT INTO " + tableName + " SELECT COUNT(*), 10 AS part FROM " + tableName);
                            return true;
                        }
                        catch (Exception e) {
                            RuntimeException trinoException = getTrinoExceptionCause(e);
                            try {
                                assertThat(trinoException).hasMessage("Failed to write Delta Lake transaction log entry");
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

            long successfulInsertsCount = futures.stream()
                    .map(MoreFutures::getFutureValue)
                    .filter(success -> success)
                    .count();

            assertThat(successfulInsertsCount).isGreaterThanOrEqualTo(1);
            assertQuery(
                    "SELECT * FROM " + tableName,
                    "VALUES (0, 10)" +
                            LongStream.rangeClosed(1, successfulInsertsCount)
                                    .boxed()
                                    .map("(%d, 10)"::formatted)
                                    .collect(joining(", ", ", ", "")));
            assertQuery(
                    "SELECT version, operation, isolation_level, read_version, is_blind_append FROM \"" + tableName + "$history\"",
                    "VALUES (0, 'CREATE TABLE AS SELECT', 'WriteSerializable', 0, true)" +
                            LongStream.rangeClosed(1, successfulInsertsCount)
                                    .boxed()
                                    .map(version -> "(%s, 'WRITE', 'WriteSerializable', %s, false)".formatted(version, version - 1))
                                    .collect(joining(", ", ", ", "")));
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @Test
    public void testConcurrentInsertsSelectingFromTheSameVersionedTable()
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

        assertUpdate("CREATE TABLE " + tableName + " (a, part) " + (partitioned ? " WITH (partitioned_by = ARRAY['part'])" : "") + "  AS VALUES (0, 'a')", 1);

        try {
            executor.invokeAll(ImmutableList.<Callable<Void>>builder()
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("INSERT INTO " + tableName + " SELECT 1, 'b' AS part FROM " + tableName + " FOR VERSION AS OF 0");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("INSERT INTO " + tableName + " SELECT 2, 'c' AS part FROM " + tableName + " FOR VERSION AS OF 0");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("INSERT INTO " + tableName + " SELECT 3, 'd' AS part FROM " + tableName + " FOR VERSION AS OF 0");
                                return null;
                            })
                            .build())
                    .forEach(MoreFutures::getDone);

            assertQuery("SELECT * FROM " + tableName, "VALUES (0, 'a'), (1, 'b'), (2, 'c'), (3, 'd')");
            assertQuery(
                    "SELECT version, operation, isolation_level, read_version, is_blind_append FROM \"" + tableName + "$history\"",
                    """
                    VALUES
                        (0, 'CREATE TABLE AS SELECT', 'WriteSerializable', 0, true),
                        (1, 'WRITE', 'WriteSerializable', 0, true),
                        (2, 'WRITE', 'WriteSerializable', 1, true),
                        (3, 'WRITE', 'WriteSerializable', 2, true)
                    """);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @Test
    public void testConcurrentInsertsSelectingFromTheSamePartition()
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_inserts_select_from_same_partition_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a, part) WITH (partitioned_by = ARRAY['part']) AS VALUES (0, 10), (11, 20), (22, 30)", 3);

        try {
            // Considering T1, T2, T3 being the order of completion of the concurrent INSERT operations,
            // if all the operations would eventually succeed, the entries inserted per thread would look like this:
            // T1: (1, 10)
            // T2: (2, 10)
            // T3: (3, 10)
            // The state of the table after the successful INSERT operations would be:
            // (0,10), (1, 10), (2, 10), (3, 10), (11, 20), (22, 30)
            List<Future<Boolean>> futures = IntStream.range(0, threads)
                    .mapToObj(threadNumber -> executor.submit(() -> {
                        barrier.await(10, SECONDS);
                        try {
                            getQueryRunner().execute("INSERT INTO " + tableName + " SELECT COUNT(*) as a, 10 as part FROM " + tableName + " WHERE part = 10");
                            return true;
                        }
                        catch (Exception e) {
                            RuntimeException trinoException = getTrinoExceptionCause(e);
                            try {
                                assertThat(trinoException).hasMessage("Failed to write Delta Lake transaction log entry");
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

            long successfulInsertsCount = futures.stream()
                    .map(MoreFutures::getFutureValue)
                    .filter(success -> success)
                    .count();

            assertThat(successfulInsertsCount).isGreaterThanOrEqualTo(1);
            assertQuery(
                    "SELECT * FROM " + tableName,
                    "VALUES (0, 10), (11, 20), (22, 30)" +
                            LongStream.rangeClosed(1, successfulInsertsCount)
                                    .boxed()
                                    .map("(%d, 10)"::formatted)
                                    .collect(joining(", ", ", ", "")));
            assertQuery(
                    "SELECT version, operation, isolation_level, read_version, is_blind_append FROM \"" + tableName + "$history\"",
                    "VALUES (0, 'CREATE TABLE AS SELECT', 'WriteSerializable', 0, true)" +
                            LongStream.rangeClosed(1, successfulInsertsCount)
                                    .boxed()
                                    .map(version -> "(%s, 'WRITE', 'WriteSerializable', %s, false)".formatted(version, version - 1))
                                    .collect(joining(", ", ", ", "")));
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    // Copied from BaseDeltaLakeConnectorSmokeTest
    @Test
    public void testConcurrentInsertsReconciliationForMixedInserts()
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_mixed_inserts_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a, part) WITH (partitioned_by = ARRAY['part']) AS VALUES (0, 10), (11, 20)", 2);

        try {
            // insert data concurrently
            executor.invokeAll(ImmutableList.<Callable<Void>>builder()
                            .add(() -> {
                                // Read from the partition `10` of the same table to avoid reconciliation failures
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("INSERT INTO " + tableName + " SELECT COUNT(*) AS a, 10 AS part FROM " + tableName + " WHERE part = 10");
                                return null;
                            })
                            .add(() -> {
                                // Read from the partition `20` of the same table to avoid reconciliation failures
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("INSERT INTO " + tableName + " SELECT COUNT(*) AS a, 20 AS part FROM " + tableName + " WHERE part = 20");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("INSERT INTO " + tableName + " VALUES (22, 30)");
                                return null;
                            })
                            .build())
                    .forEach(MoreFutures::getDone);

            assertQuery(
                    "SELECT * FROM " + tableName,
                    "VALUES (0, 10), (1, 10), (11, 20), (1, 20), (22, 30)");
            assertQuery(
                    "SELECT operation, isolation_level, is_blind_append FROM \"" + tableName + "$history\"",
                    """
                    VALUES
                        ('CREATE TABLE AS SELECT', 'WriteSerializable', true),
                        ('WRITE', 'WriteSerializable', false),
                        ('WRITE', 'WriteSerializable', false),
                        ('WRITE', 'WriteSerializable', true)
                    """);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @Test
    public void testConcurrentInsertsSelectingFromDifferentPartitionsOfSameTable()
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_mixed_inserts_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a, part) WITH (partitioned_by = ARRAY['part']) AS VALUES (0, 10), (11, 20), (22, 30)", 3);

        try {
            // perform non-overlapping non-blind insert operations concurrently which read from different source partitions
            // and write within the same target partition of the table.
            executor.invokeAll(ImmutableList.<Callable<Void>>builder()
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("INSERT INTO " + tableName + " SELECT a + 1,  40 as part FROM " + tableName + " WHERE part = 10");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("INSERT INTO " + tableName + " SELECT a + 2,  40 as part FROM " + tableName + " WHERE part = 20");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("INSERT INTO " + tableName + " SELECT a + 3,  40 as part FROM " + tableName + " WHERE part = 30");
                                return null;
                            })
                            .build())
                    .forEach(MoreFutures::getDone);

            assertQuery(
                    "SELECT * FROM " + tableName,
                    "VALUES (0, 10), (11, 20), (22, 30), (1, 40), (13, 40), (25, 40)");
            assertQuery(
                    "SELECT operation, isolation_level, is_blind_append FROM \"" + tableName + "$history\"",
                    """
                    VALUES
                        ('CREATE TABLE AS SELECT', 'WriteSerializable', true),
                        ('WRITE', 'WriteSerializable', false),
                        ('WRITE', 'WriteSerializable', false),
                        ('WRITE', 'WriteSerializable', false)
                    """);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @Test
    public void testConcurrentInsertsSelectingFromMultipleNonoverlappingPartitionsOfSameTable()
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_mixed_inserts_table_" + randomNameSuffix();

        // Create table with multiple partitions and multiple files per partition
        assertUpdate("CREATE TABLE " + tableName + " (a, part) WITH (partitioned_by = ARRAY['part']) AS VALUES (0, 10), (11, 20), (22, 30), (33, 40), (44, 50), (55, 60)", 6);
        assertUpdate("INSERT INTO " + tableName + " VALUES (2, 10), (13, 20), (24, 30), (35, 40), (46, 50), (57, 60)", 6);

        try {
            // perform non-overlapping non-blind insert operations concurrently which read from different source partitions
            // and write within the same target partition of the table.
            executor.invokeAll(ImmutableList.<Callable<Void>>builder()
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("INSERT INTO " + tableName + " SELECT a + 1, part FROM " + tableName + " WHERE part IN (10, 20)");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("INSERT INTO " + tableName + " SELECT a + 1, part FROM " + tableName + " WHERE part IN (30, 40)");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("INSERT INTO " + tableName + " SELECT a + 1, part FROM " + tableName + " WHERE part IN (50, 60)");
                                return null;
                            })
                            .build())
                    .forEach(MoreFutures::getDone);

            assertQuery(
                    "SELECT * FROM " + tableName,
                    """
                    VALUES
                        (0, 10), (1, 10), (2, 10), (3, 10),
                        (11, 20), (12, 20), (13, 20), (14, 20),
                        (22, 30), (23, 30),(24, 30), (25, 30),
                        (33, 40), (34, 40), (35, 40), (36, 40),
                        (44, 50), (45, 50), (46, 50), (47, 50),
                        (55, 60), (56,60), (57, 60), (58,60)
                    """);
            assertQuery(
                    "SELECT version, operation, isolation_level, read_version, is_blind_append FROM \"" + tableName + "$history\"",
                    """
                    VALUES
                        (0, 'CREATE TABLE AS SELECT', 'WriteSerializable', 0, true),
                        (1, 'WRITE', 'WriteSerializable', 0, true),
                        (2, 'WRITE', 'WriteSerializable', 1, false),
                        (3, 'WRITE', 'WriteSerializable', 2, false),
                        (4, 'WRITE', 'WriteSerializable', 3, false)
                    """);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @Test
    public void testConcurrentSerializableBlindInsertsReconciliationFailure()
            throws Exception
    {
        int threads = 5;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_serializable_blind_inserts_table_reconciliation" + randomNameSuffix();

        // TODO create the table through Trino when `isolation_level` table property can be set
        registerTableFromResources(tableName, "deltalake/serializable_partitioned_table", getQueryRunner());

        assertThat(query("SELECT * FROM " + tableName)).matches("VALUES (0, 10), (33, 40)");

        try {
            List<Future<Boolean>> futures = IntStream.range(0, threads)
                    .mapToObj(threadNumber -> executor.submit(() -> {
                        barrier.await(10, SECONDS);
                        try {
                            // Writing concurrently on the same partition even when doing blind inserts is not permitted
                            // in Serializable isolation level
                            getQueryRunner().execute("INSERT INTO " + tableName + " VALUES (1, 10)");
                            return true;
                        }
                        catch (Exception e) {
                            RuntimeException trinoException = getTrinoExceptionCause(e);
                            try {
                                assertThat(trinoException).hasMessage("Failed to write Delta Lake transaction log entry");
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

            long successfulInsertsCount = futures.stream()
                    .map(MoreFutures::getFutureValue)
                    .filter(success -> success)
                    .count();
            assertThat(successfulInsertsCount).isGreaterThanOrEqualTo(1);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @Test
    public void testConcurrentInsertsReconciliationFailure()
            throws Exception
    {
        testConcurrentInsertsReconciliationFailure(false);
        testConcurrentInsertsReconciliationFailure(true);
    }

    private void testConcurrentInsertsReconciliationFailure(boolean partitioned)
            throws Exception
    {
        int threads = 5;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_inserts_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a, part)" +
                (partitioned ? " WITH (partitioned_by = ARRAY['part'])" : "") + " AS VALUES (1, 10)", 1);

        try {
            List<Future<Boolean>> futures = IntStream.range(0, threads)
                    .mapToObj(threadNumber -> executor.submit(() -> {
                        barrier.await(10, SECONDS);
                        try {
                            // Writing concurrently on the same partition of the table as from which we are reading from should fail
                            getQueryRunner().execute("INSERT INTO " + tableName + " SELECT * FROM " + tableName + " WHERE part = 10");
                            return true;
                        }
                        catch (Exception e) {
                            RuntimeException trinoException = getTrinoExceptionCause(e);
                            try {
                                assertThat(trinoException).hasMessage("Failed to write Delta Lake transaction log entry");
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

            long successfulInsertsCount = futures.stream()
                    .map(MoreFutures::getFutureValue)
                    .filter(success -> success)
                    .count();
            assertThat(successfulInsertsCount).isGreaterThanOrEqualTo(1);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    // Copied from BaseDeltaLakeConnectorSmokeTest
    @Test
    public void testConcurrentDeletePushdownReconciliation()
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_inserts_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a, part)  WITH (partitioned_by = ARRAY['part']) AS VALUES (1, 10), (11, 20), (21, 30), (31, 40)", 4);

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
            assertQuery(
                    "SELECT version, operation, isolation_level, is_blind_append FROM \"" + tableName + "$history\"",
                    """
                    VALUES
                        (0, 'CREATE TABLE AS SELECT', 'WriteSerializable', true),
                        (1, 'DELETE', 'WriteSerializable', false),
                        (2, 'DELETE', 'WriteSerializable', false),
                        (3, 'DELETE', 'WriteSerializable', false)
                    """);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @Test
    public void testConcurrentDeletePushdownFromTheSamePartition()
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_delete_from_same_partition_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a, part) WITH (partitioned_by = ARRAY['part']) AS VALUES (0, 10), (11, 20), (22, 30)", 3);

        try {
            List<Future<Boolean>> futures = IntStream.range(0, threads)
                    .mapToObj(threadNumber -> executor.submit(() -> {
                        barrier.await(10, SECONDS);
                        try {
                            getQueryRunner().execute("DELETE FROM " + tableName + "  WHERE part = 10");
                            return true;
                        }
                        catch (Exception e) {
                            RuntimeException trinoException = getTrinoExceptionCause(e);
                            try {
                                assertThat(trinoException).hasMessage("Failed to write Delta Lake transaction log entry");
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

            long successfulDeletesCount = futures.stream()
                    .map(MoreFutures::getFutureValue)
                    .filter(success -> success)
                    .count();

            assertThat(successfulDeletesCount).isGreaterThanOrEqualTo(1);
            assertQuery("SELECT * FROM " + tableName, "VALUES (11, 20), (22, 30)");
            assertQuery(
                    "SELECT version, operation, isolation_level, is_blind_append FROM \"" + tableName + "$history\"",
                    "VALUES (0, 'CREATE TABLE AS SELECT', 'WriteSerializable', true)" +
                            LongStream.rangeClosed(1, successfulDeletesCount)
                                    .boxed()
                                    .map("(%s, 'DELETE', 'WriteSerializable', false)"::formatted)
                                    .collect(joining(", ", ", ", "")));
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @Test
    public void testConcurrentTruncateReconciliationFailure()
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_delete_from_same_partition_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a, part) WITH (partitioned_by = ARRAY['part']) AS VALUES (0, 10), (11, 20), (22, 30)", 3);

        try {
            List<Future<Boolean>> futures = IntStream.range(0, threads)
                    .mapToObj(threadNumber -> executor.submit(() -> {
                        barrier.await(10, SECONDS);
                        try {
                            getQueryRunner().execute("TRUNCATE TABLE " + tableName);
                            return true;
                        }
                        catch (Exception e) {
                            RuntimeException trinoException = getTrinoExceptionCause(e);
                            try {
                                assertThat(trinoException).hasMessage("Failed to write Delta Lake transaction log entry");
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

            long successfulTruncatesCount = futures.stream()
                    .map(MoreFutures::getFutureValue)
                    .filter(success -> success)
                    .count();

            assertThat(successfulTruncatesCount).isGreaterThanOrEqualTo(1);
            assertThat(query("SELECT * FROM " + tableName)).returnsEmptyResult();
            assertQuery(
                    "SELECT version, operation, isolation_level, is_blind_append FROM \"" + tableName + "$history\"",
                    "VALUES (0, 'CREATE TABLE AS SELECT', 'WriteSerializable', true)" +
                            LongStream.rangeClosed(1, successfulTruncatesCount)
                                    .boxed()
                                    .map("(%s, 'TRUNCATE', 'WriteSerializable', false)"::formatted)
                                    .collect(joining(", ", ", ", "")));
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @Test
    public void testConcurrentDeletePushdownAndBlindInsertsReconciliation()
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_delete_and_inserts_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a, part)  WITH (partitioned_by = ARRAY['part']) AS VALUES (1, 10), (11, 20)", 2);

        try {
            // delete whole data from the table while concurrently adding new blind inserts
            executor.invokeAll(ImmutableList.<Callable<Void>>builder()
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("DELETE FROM " + tableName);
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

            // Depending on the read version of the operation we may have different results for the following query
            // although it will most likely be 52 (sum of 21 and 31).
            assertThat((long) computeActual("SELECT sum(a) FROM " + tableName).getOnlyValue()).isIn(0L, 21L, 31L, (long) (21 + 31));
            assertQuery(
                    "SELECT operation, isolation_level FROM \"" + tableName + "$history\"",
                    """
                    VALUES
                        ('CREATE TABLE AS SELECT', 'WriteSerializable'),
                        ('DELETE', 'WriteSerializable'),
                        ('WRITE', 'WriteSerializable'),
                        ('WRITE', 'WriteSerializable')
                    """);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @Test
    public void testConcurrentTruncateAndBlindInsertsReconciliation()
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_truncate_and_inserts_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a, part)  WITH (partitioned_by = ARRAY['part']) AS VALUES (1, 10), (11, 20)", 2);

        try {
            // truncate data while concurrently adding new blind inserts
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

            // Depending on the read version of the operation we may have different results for the following query
            // although it will most likely be 52 (sum of 21 and 31).
            assertThat((long) computeActual("SELECT sum(a) FROM " + tableName).getOnlyValue()).isIn(0L, 21L, 31L, (long) (21 + 31), (long) (1 + 11 + 21 + 31));
            assertQuery(
                    "SELECT operation, isolation_level, is_blind_append FROM \"" + tableName + "$history\"",
                    """
                    VALUES
                        ('CREATE TABLE AS SELECT', 'WriteSerializable', true),
                        ('TRUNCATE', 'WriteSerializable', false),
                        ('WRITE', 'WriteSerializable', true),
                        ('WRITE', 'WriteSerializable', true)
                    """);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @Test
    public void testConcurrentDeletePushdownAndNonBlindInsertsReconciliation()
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_delete_and_inserts_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a, part)  WITH (partitioned_by = ARRAY['part']) AS VALUES (1, 10), (11, 20), (21, 30)", 3);
        // Add more files in the partition 10
        assertUpdate("INSERT INTO " + tableName + " VALUES (2, 10)", 1);

        try {
            // The DELETE and non-blind INSERT operations operate on non-overlapping partitions
            executor.invokeAll(ImmutableList.<Callable<Void>>builder()
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("DELETE FROM " + tableName + " WHERE part = 10");
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
            assertQuery(
                    "SELECT operation, isolation_level, is_blind_append FROM \"" + tableName + "$history\"",
                    """
                    VALUES
                        ('CREATE TABLE AS SELECT', 'WriteSerializable', true),
                        ('WRITE', 'WriteSerializable', true),
                        ('DELETE', 'WriteSerializable', false),
                        ('DELETE', 'WriteSerializable', false),
                        ('WRITE', 'WriteSerializable', false)
                    """);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @Test
    public void testConcurrentSerializableDeletesPushdownReconciliationFailure()
            throws Exception
    {
        int threads = 5;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_serializable_delete_reconciliation" + randomNameSuffix();

        // TODO create the table through Trino when `isolation_level` table property can be set
        registerTableFromResources(tableName, "deltalake/serializable_partitioned_table", getQueryRunner());

        assertThat(query("SELECT * FROM " + tableName)).matches("VALUES (0, 10), (33, 40)");

        try {
            List<Future<Boolean>> futures = IntStream.range(0, threads)
                    .mapToObj(threadNumber -> executor.submit(() -> {
                        barrier.await(10, SECONDS);
                        try {
                            // Deleting concurrently from the same partition even when doing blind inserts is not permitted
                            // in Serializable isolation level
                            getQueryRunner().execute("DELETE FROM " + tableName + " WHERE part = 10");
                            return true;
                        }
                        catch (Exception e) {
                            RuntimeException trinoException = getTrinoExceptionCause(e);
                            try {
                                assertThat(trinoException).hasMessage("Failed to write Delta Lake transaction log entry");
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

            long successfulDeletesCount = futures.stream()
                    .map(MoreFutures::getFutureValue)
                    .filter(success -> success)
                    .count();
            assertThat(successfulDeletesCount).isGreaterThanOrEqualTo(1);
            assertThat(query("SELECT * FROM " + tableName)).matches("VALUES (33, 40)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @Test
    public void testConcurrentSerializableTruncateReconciliationFailure()
            throws Exception
    {
        int threads = 5;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_serializable_delete_reconciliation" + randomNameSuffix();

        // TODO create the table through Trino when `isolation_level` table property can be set
        registerTableFromResources(tableName, "deltalake/serializable_partitioned_table", getQueryRunner());

        assertThat(query("SELECT * FROM " + tableName)).matches("VALUES (0, 10), (33, 40)");

        try {
            List<Future<Boolean>> futures = IntStream.range(0, threads)
                    .mapToObj(threadNumber -> executor.submit(() -> {
                        barrier.await(10, SECONDS);
                        try {
                            // Truncating concurrently is not permitted in Serializable or WriteSerializable isolation level
                            getQueryRunner().execute("TRUNCATE TABLE " + tableName);
                            return true;
                        }
                        catch (Exception e) {
                            RuntimeException trinoException = getTrinoExceptionCause(e);
                            try {
                                assertThat(trinoException).hasMessage("Failed to write Delta Lake transaction log entry");
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

            long successfulTruncatesCount = futures.stream()
                    .map(MoreFutures::getFutureValue)
                    .filter(success -> success)
                    .count();
            assertThat(successfulTruncatesCount).isGreaterThanOrEqualTo(1);
            assertThat(query("SELECT * FROM " + tableName)).returnsEmptyResult();
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @Test
    public void testConcurrentUpdateReconciliation()
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_updates_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a, part)  WITH (partitioned_by = ARRAY['part']) AS VALUES (1, 10), (11, 20), (21, 30), (31, 40)", 4);

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
            assertQuery(
                    "SELECT version, operation, isolation_level, is_blind_append FROM \"" + tableName + "$history\"",
                    """
                    VALUES
                        (0, 'CREATE TABLE AS SELECT', 'WriteSerializable', true),
                        (1, 'MERGE', 'WriteSerializable', false),
                        (2, 'MERGE', 'WriteSerializable', false),
                        (3, 'MERGE', 'WriteSerializable', false)
                    """);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @Test
    public void testConcurrentDeleteReconciliation()
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_deletes_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a, part)  WITH (partitioned_by = ARRAY['part']) AS VALUES (1, 10), (11, 20), (21, 30), (31, 40)", 4);
        // Add more files in the partition 10
        assertUpdate("INSERT INTO " + tableName + " VALUES (2, 10)", 1);

        try {
            // delete data concurrently by using non-overlapping partition predicate
            // use as well a non-partition predicate to ensure that the delete operation is not being pushed down
            executor.invokeAll(ImmutableList.<Callable<Void>>builder()
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("DELETE FROM " + tableName + " WHERE part = 10 AND a IN (1, 2)");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("DELETE FROM " + tableName + " WHERE part = 20 AND a = 11");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("DELETE FROM " + tableName + " WHERE part = 30 AND a = 21");
                                return null;
                            })
                            .build())
                    .forEach(MoreFutures::getDone);

            assertThat(query("SELECT * FROM " + tableName)).matches("VALUES (31, 40)");
            assertQuery(
                    "SELECT version, operation, isolation_level, is_blind_append FROM \"" + tableName + "$history\"",
                    """
                    VALUES
                        (0, 'CREATE TABLE AS SELECT', 'WriteSerializable', true),
                        (1, 'WRITE', 'WriteSerializable', true),
                        (2, 'MERGE', 'WriteSerializable', false),
                        (3, 'MERGE', 'WriteSerializable', false),
                        (4, 'MERGE', 'WriteSerializable', false)
                    """);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    // Copied from BaseDeltaLakeSmokeConnectorTest
    @Test
    public void testConcurrentMergeReconciliation()
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_merges_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a, part)  WITH (partitioned_by = ARRAY['part']) AS VALUES (1, 10), (11, 20), (21, 30), (31, 40)", 4);
        // Add more files in the partition 30
        assertUpdate("INSERT INTO " + tableName + " VALUES (22, 30)", 1);

        try {
            // merge data concurrently by using non-overlapping partition predicate
            executor.invokeAll(ImmutableList.<Callable<Void>>builder()
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                // No source table handles are employed for this MERGE statement, which causes a blind insert
                                getQueryRunner().execute(
                                        """
                                        MERGE INTO %s t USING (VALUES (12, 20)) AS s(a, part)
                                          ON (FALSE)
                                            WHEN NOT MATCHED THEN INSERT (a, part) VALUES(s.a, s.part)
                                        """.formatted(tableName));
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute(
                                        """
                                        MERGE INTO %s t USING (VALUES (21, 30)) AS s(a, part)
                                          ON (t.part = s.part)
                                            WHEN MATCHED THEN DELETE
                                        """.formatted(tableName));
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute(
                                        """
                                        MERGE INTO %s t USING (VALUES (32, 40)) AS s(a, part)
                                          ON (t.part = s.part)
                                            WHEN MATCHED THEN UPDATE SET a = s.a
                                        """.formatted(tableName));
                                return null;
                            })
                            .build())
                    .forEach(MoreFutures::getDone);

            assertThat(query("SELECT * FROM " + tableName)).matches("VALUES (1, 10), (11, 20), (12, 20), (32, 40)");
            assertQuery(
                    "SELECT operation, isolation_level, is_blind_append FROM \"" + tableName + "$history\"",
                    """
                    VALUES
                        ('CREATE TABLE AS SELECT', 'WriteSerializable', true),
                        ('WRITE', 'WriteSerializable', true),
                        ('MERGE', 'WriteSerializable', true),
                        ('MERGE', 'WriteSerializable', false),
                        ('MERGE', 'WriteSerializable', false)
                    """);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @Test
    public void testConcurrentSerializableMergeReconciliationFailure()
            throws Exception
    {
        int threads = 5;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_serializable_merge_reconciliation" + randomNameSuffix();

        // TODO create the table through Trino when `isolation_level` table property can be set
        registerTableFromResources(tableName, "deltalake/serializable_partitioned_table", getQueryRunner());

        assertThat(query("SELECT * FROM " + tableName)).matches("VALUES (0, 10), (33, 40)");

        try {
            List<Future<Boolean>> futures = IntStream.range(0, threads)
                    .mapToObj(threadNumber -> executor.submit(() -> {
                        barrier.await(10, SECONDS);
                        try {
                            // Writing concurrently on the same partition even when doing blind inserts is not permitted
                            // in Serializable isolation level
                            getQueryRunner().execute(
                                    """
                                    MERGE INTO %s t USING (VALUES (12, 20)) AS s(a, part)
                                      ON (FALSE)
                                        WHEN NOT MATCHED THEN INSERT (a, part) VALUES(s.a, s.part)
                                    """.formatted(tableName));
                            return true;
                        }
                        catch (Exception e) {
                            RuntimeException trinoException = getTrinoExceptionCause(e);
                            try {
                                assertThat(trinoException).hasMessage("Failed to write Delta Lake transaction log entry");
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

            long successfulMergeOperationsCount = futures.stream()
                    .map(MoreFutures::getFutureValue)
                    .filter(success -> success)
                    .count();
            assertThat(successfulMergeOperationsCount).isGreaterThanOrEqualTo(1);
            assertThat((long) computeScalar("SELECT count(*) FROM " + tableName + " WHERE part = 20")).isGreaterThanOrEqualTo(1L);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @Test
    public void testConcurrentDeleteAndNonBlindInsertsReconciliation()
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_delete_and_inserts_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a, part)  WITH (partitioned_by = ARRAY['part']) AS VALUES (1, 10), (11, 20), (21, 30)", 3);
        // Add more files in the partition 10
        assertUpdate("INSERT INTO " + tableName + " VALUES (2, 10)", 1);

        try {
            // The DELETE and non-blind INSERT operations operate on non-overlapping partitions
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
                                // Use a non-partition filter as well to ensure the DELETE operation is not being pushed down
                                getQueryRunner().execute("DELETE FROM " + tableName + " WHERE part = 30 AND a BETWEEN 20 AND 30");
                                return null;
                            })
                            .build())
                    .forEach(MoreFutures::getDone);

            assertThat(query("SELECT * FROM " + tableName)).matches("VALUES (11, 20), (12, 20)");
            assertQuery(
                    "SELECT operation, isolation_level, is_blind_append FROM \"" + tableName + "$history\"",
                    """
                    VALUES
                        ('CREATE TABLE AS SELECT', 'WriteSerializable', true),
                        ('WRITE', 'WriteSerializable', true),
                        ('MERGE', 'WriteSerializable', false),
                        ('MERGE', 'WriteSerializable', false),
                        ('WRITE', 'WriteSerializable', false)
                    """);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @Test
    public void testConcurrentDeleteAndBlindInsertsReconciliation()
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_delete_and_inserts_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a, part)  WITH (partitioned_by = ARRAY['part']) AS VALUES (1, 10), (11, 20)", 2);

        try {
            // Use a WHERE predicate in the DELETE statement which involves scanning the whole table while concurrently adding new blind inserts
            executor.invokeAll(ImmutableList.<Callable<Void>>builder()
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("DELETE FROM " + tableName + " WHERE a > 10");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("INSERT INTO " + tableName + " VALUES (8, 10)");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("INSERT INTO " + tableName + " VALUES (21, 30)");
                                return null;
                            })
                            .build())
                    .forEach(MoreFutures::getDone);

            // Depending on the read version of the operation we may have different results for the following query
            // although it will most likely be 30 (sum of 1, 8 and 21).
            assertThat((long) computeActual("SELECT sum(a) FROM " + tableName).getOnlyValue()).isIn(
                    1L,
                    (long) (1 + 8),
                    (long) (1 + 21),
                    (long) (1 + 8 + 21));
            assertQuery(
                    "SELECT operation, isolation_level FROM \"" + tableName + "$history\"",
                    """
                    VALUES
                        ('CREATE TABLE AS SELECT', 'WriteSerializable'),
                        ('MERGE', 'WriteSerializable'),
                        ('WRITE', 'WriteSerializable'),
                        ('WRITE', 'WriteSerializable')
                    """);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @Test
    public void testConcurrentUpdateAndBlindInsertsReconciliation()
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_update_and_inserts_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a, part)  WITH (partitioned_by = ARRAY['part']) AS VALUES (1, 10), (11, 20)", 2);

        try {
            // Use a WHERE predicate in the UPDATE statement which involves scanning the whole table while concurrently adding new blind inserts
            executor.invokeAll(ImmutableList.<Callable<Void>>builder()
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("UPDATE " + tableName + " SET a = a + 1");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("INSERT INTO " + tableName + " VALUES (13, 20)");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("INSERT INTO " + tableName + " VALUES (21, 30)");
                                return null;
                            })
                            .build())
                    .forEach(MoreFutures::getDone);

            // Depending on the read version of the operation we may have different results for the following query
            // although it will most likely be 48 (sum of 2, 12, 13 and 21).
            assertThat((long) computeActual("SELECT sum(a) FROM " + tableName).getOnlyValue()).isIn(
                    (long) (2 + 12 + 13 + 21),
                    (long) (2 + 12 + 14 + 21),
                    (long) (2 + 12 + 13 + 22),
                    (long) (2 + 12 + 14 + 22));
            assertQuery(
                    "SELECT operation, isolation_level FROM \"" + tableName + "$history\"",
                    """
                    VALUES
                        ('CREATE TABLE AS SELECT', 'WriteSerializable'),
                        ('MERGE', 'WriteSerializable'),
                        ('WRITE', 'WriteSerializable'),
                        ('WRITE', 'WriteSerializable')
                    """);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @Test
    public void testConcurrentMergeAndBlindInsertsReconciliation()
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_merge_and_inserts_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a, part)  WITH (partitioned_by = ARRAY['part']) AS VALUES (1, 10), (11, 20)", 2);

        try {
            executor.invokeAll(ImmutableList.<Callable<Void>>builder()
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute(
                                        """
                                        MERGE INTO %s t USING (VALUES (11, 20), (8, 10), (21, 30)) AS s(a, part)
                                          ON (t.a = s.a AND t.part = s.part)
                                            WHEN MATCHED THEN DELETE
                                        """.formatted(tableName));
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("INSERT INTO " + tableName + " VALUES (8, 10)");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("INSERT INTO " + tableName + " VALUES (21, 30)");
                                return null;
                            })
                            .build())
                    .forEach(MoreFutures::getDone);

            // Depending on the read version of the operation we may have different results for the following query
            // although it will most likely be 30 (sum of 1, 8 and 21).
            assertThat((long) computeActual("SELECT sum(a) FROM " + tableName).getOnlyValue()).isIn(
                    1L,
                    (long) (1 + 8),
                    (long) (1 + 21),
                    (long) (1 + 8 + 21));
            assertQuery(
                    "SELECT operation, isolation_level FROM \"" + tableName + "$history\"",
                    """
                    VALUES
                        ('CREATE TABLE AS SELECT', 'WriteSerializable'),
                        ('MERGE', 'WriteSerializable'),
                        ('WRITE', 'WriteSerializable'),
                        ('WRITE', 'WriteSerializable')
                    """);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @Test
    public void testConcurrentDeleteAndDeletePushdownAndNonBlindInsertsReconciliation()
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_delete_and_inserts_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a, part)  WITH (partitioned_by = ARRAY['part']) AS VALUES (1, 10), (11, 20), (21, 30)", 3);
        // Add more files in the partition 10
        assertUpdate("INSERT INTO " + tableName + " VALUES (2, 10)", 1);

        try {
            // The DELETE and non-blind INSERT operations operate on non-overlapping partitions
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
            assertQuery(
                    "SELECT operation, isolation_level, is_blind_append FROM \"" + tableName + "$history\"",
                    """
                    VALUES
                        ('CREATE TABLE AS SELECT', 'WriteSerializable', true),
                        ('WRITE', 'WriteSerializable', true),
                        ('MERGE', 'WriteSerializable', false),
                        ('DELETE', 'WriteSerializable', false),
                        ('WRITE', 'WriteSerializable', false)
                    """);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @Test
    public void testConcurrentOptimizeReconciliation()
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_optimize_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a, part)  WITH (partitioned_by = ARRAY['part']) AS VALUES (1, 10), (11, 20), (21, 30)", 3);
        // Add more files on each partition
        assertUpdate("INSERT INTO " + tableName + " VALUES (2, 10), (12, 20), (22, 30)", 3);
        Set<String> beforeOptimizeActiveFiles = getActiveFiles(tableName);
        try {
            // The OPTIMIZE operations operate on non-overlapping partitions
            executor.invokeAll(ImmutableList.<Callable<Void>>builder()
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE WHERE part = 10");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE WHERE part = 20");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE WHERE part = 30");
                                return null;
                            })
                            .build())
                    .forEach(MoreFutures::getDone);

            // Verify OPTIMIZE happened, but table data didn't change
            assertThat(beforeOptimizeActiveFiles).isNotEqualTo(getActiveFiles(tableName));
            assertThat(query("SELECT * FROM " + tableName)).matches("VALUES (1, 10), (2, 10), (11, 20), (12, 20), (21, 30), (22, 30)");
            assertQuery(
                    "SELECT operation, isolation_level, is_blind_append FROM \"" + tableName + "$history\"",
                    """
                            VALUES
                                ('CREATE TABLE AS SELECT', 'WriteSerializable', true),
                                ('WRITE', 'WriteSerializable', true),
                                ('OPTIMIZE', 'WriteSerializable', false),
                                ('OPTIMIZE', 'WriteSerializable', false),
                                ('OPTIMIZE', 'WriteSerializable', false)
                            """);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @Test
    public void testConcurrentSerializableOptimizeReconciliationFailure()
            throws Exception
    {
        int threads = 5;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_serializable_optimize_reconciliation" + randomNameSuffix();

        // TODO create the table through Trino when `isolation_level` table property can be set
        registerTableFromResources(tableName, "deltalake/serializable_partitioned_table", getQueryRunner());

        assertThat(query("SELECT * FROM " + tableName)).matches("VALUES (0, 10), (33, 40)");

        try {
            List<Future<Boolean>> futures = IntStream.range(0, threads)
                    .mapToObj(threadNumber -> executor.submit(() -> {
                        barrier.await(10, SECONDS);
                        try {
                            // Optimizing concurrently is not permitted on the same partition on Serializable of WriteSerializable isolation level
                            getQueryRunner().execute("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE WHERE part = 10");
                            return true;
                        }
                        catch (Exception e) {
                            RuntimeException trinoException = getTrinoExceptionCause(e);
                            try {
                                assertThat(trinoException).hasMessage("Failed to write Delta Lake transaction log entry");
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

            long successfulOptimizeOperationsCount = futures.stream()
                    .map(MoreFutures::getFutureValue)
                    .filter(success -> success)
                    .count();
            assertThat(successfulOptimizeOperationsCount).isGreaterThanOrEqualTo(1);
            assertThat(query("SELECT * FROM " + tableName)).matches("VALUES  (0, 10), (33, 40)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @Test
    public void testConcurrentOptimizeAndBlindInsertsReconciliation()
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_optimize_and_inserts_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a, part)  WITH (partitioned_by = ARRAY['part']) AS VALUES (1, 10), (21, 30)", 2);
        assertUpdate("INSERT INTO " + tableName + " VALUES (22, 30)", 1);
        Set<String> beforeOptimizeActiveFilesOnPartition30 = computeActual("SELECT DISTINCT \"$path\" FROM " + tableName + " WHERE part = 30").getOnlyColumnAsSet().stream()
                .map(String.class::cast)
                .collect(toImmutableSet());

        try {
            executor.invokeAll(ImmutableList.<Callable<Void>>builder()
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE WHERE part > 20");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("INSERT INTO " + tableName + " VALUES (8, 10)");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("INSERT INTO " + tableName + " VALUES (11, 20)");
                                return null;
                            })
                            .build())
                    .forEach(MoreFutures::getDone);

            // Verify OPTIMIZE happened, but table data didn't change
            Set<String> afterOptimizeActiveFilesOnPartition30 = computeActual("SELECT DISTINCT \"$path\" FROM " + tableName + " WHERE part = 30").getOnlyColumnAsSet().stream()
                    .map(String.class::cast)
                    .collect(toImmutableSet());
            assertThat(beforeOptimizeActiveFilesOnPartition30).isNotEqualTo(afterOptimizeActiveFilesOnPartition30);

            assertThat(query("SELECT * FROM " + tableName)).matches("VALUES  (1, 10), (8, 10), (11, 20), (21, 30), (22, 30)");
            assertQuery(
                    "SELECT operation, isolation_level FROM \"" + tableName + "$history\"",
                    """
                            VALUES
                                ('CREATE TABLE AS SELECT', 'WriteSerializable'),
                                ('OPTIMIZE', 'WriteSerializable'),
                                ('WRITE', 'WriteSerializable'),
                                ('WRITE', 'WriteSerializable'),
                                ('WRITE', 'WriteSerializable')
                            """);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @Test
    public void testConcurrentOptimizeAndNonBlindInsertsReconciliation()
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_optimize_and_inserts_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a, part)  WITH (partitioned_by = ARRAY['part']) AS VALUES (1, 10), (11, 20), (21, 30)", 3);
        // Add more files in the partition 10
        assertUpdate("INSERT INTO " + tableName + " VALUES (2, 10)", 1);
        Set<String> beforeOptimizeActiveFilesOnPartition10 = computeActual("SELECT DISTINCT \"$path\" FROM " + tableName + " WHERE part = 10").getOnlyColumnAsSet().stream()
                .map(String.class::cast)
                .collect(toImmutableSet());

        try {
            // The OPTIMIZE, DELETE and non-blind INSERT operations operate on non-overlapping partitions
            executor.invokeAll(ImmutableList.<Callable<Void>>builder()
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE WHERE part = 10");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("INSERT INTO " + tableName + " SELECT a + 1, part FROM " + tableName + " WHERE part = 20");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                // Use a non-partition filter as well to ensure the DELETE operation is not being pushed down
                                getQueryRunner().execute("DELETE FROM " + tableName + " WHERE part = 30 AND a BETWEEN 20 AND 30");
                                return null;
                            })
                            .build())
                    .forEach(MoreFutures::getDone);

            // Verify OPTIMIZE happened, but table data didn't change
            Set<String> afterOptimizeActiveFilesOnPartition10 = computeActual("SELECT DISTINCT \"$path\" FROM " + tableName + " WHERE part = 10").getOnlyColumnAsSet().stream()
                    .map(String.class::cast)
                    .collect(toImmutableSet());
            assertThat(beforeOptimizeActiveFilesOnPartition10).isNotEqualTo(afterOptimizeActiveFilesOnPartition10);
            assertThat(query("SELECT * FROM " + tableName)).matches("VALUES (1, 10), (2, 10), (11, 20), (12, 20)");
            assertQuery(
                    "SELECT operation, isolation_level, is_blind_append FROM \"" + tableName + "$history\"",
                    """
                            VALUES
                                ('CREATE TABLE AS SELECT', 'WriteSerializable', true),
                                ('WRITE', 'WriteSerializable', true),
                                ('OPTIMIZE', 'WriteSerializable', false),
                                ('MERGE', 'WriteSerializable', false),
                                ('WRITE', 'WriteSerializable', false)
                            """);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    protected void registerTableFromResources(String table, String resourcePath, QueryRunner queryRunner)
            throws IOException
    {
        TrinoFileSystem fileSystem = getConnectorService(queryRunner, TrinoFileSystemFactory.class)
                .create(ConnectorIdentity.ofUser("test"));

        String tableLocation = "local:///" + table;
        fileSystem.createDirectory(Location.of(tableLocation));

        try {
            List<ClassPath.ResourceInfo> resources = ClassPath.from(getClass().getClassLoader())
                    .getResources()
                    .stream()
                    .filter(resourceInfo -> resourceInfo.getResourceName().startsWith(resourcePath + "/"))
                    .collect(toImmutableList());
            for (ClassPath.ResourceInfo resourceInfo : resources) {
                String fileName = resourceInfo.getResourceName().replaceFirst("^" + Pattern.quote(resourcePath), quoteReplacement(tableLocation));
                byte[] bytes = resourceInfo.asByteSource().read();
                TrinoOutputFile trinoOutputFile = fileSystem.newOutputFile(Location.of(fileName));
                trinoOutputFile.createOrOverwrite(bytes);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        queryRunner.execute(format("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')", table, tableLocation));
    }

    private Set<String> getActiveFiles(String tableName)
    {
        return computeActual("SELECT DISTINCT \"$path\" FROM " + tableName).getOnlyColumnAsSet().stream()
                .map(String.class::cast)
                .collect(toImmutableSet());
    }
}
