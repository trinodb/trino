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
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.ClassPath;
import io.airlift.concurrent.MoreFutures;
import io.trino.Session;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.plugin.deltalake.metastore.TestingDeltaLakeMetastoreModule;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.deltalake.DeltaLakeConnectorFactory.CONNECTOR_NAME;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.plugin.deltalake.TestingDeltaLakeUtils.getConnectorService;
import static io.trino.plugin.hive.metastore.file.TestingFileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.testing.QueryAssertions.getTrinoExceptionCause;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.regex.Matcher.quoteReplacement;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestDeltaLakeLocalConcurrentWritesTest
        extends AbstractTestQueryFramework
{
    protected static final String SCHEMA = "test_delta_concurrent_writes_" + randomNameSuffix();

    private Path dataDirectory;
    private HiveMetastore metastore;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(DELTA_CATALOG)
                .setSchema(SCHEMA)
                .build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).build();

        this.dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("delta_lake_data");
        this.metastore = createTestingFileHiveMetastore(dataDirectory.toFile());

        queryRunner.installPlugin(new TestingDeltaLakePlugin(dataDirectory, Optional.of(new TestingDeltaLakeMetastoreModule(metastore))));

        Map<String, String> connectorProperties = ImmutableMap.<String, String>builder()
                .put("delta.unique-table-location", "true")
                .put("delta.register-table-procedure.enabled", "true")
                .buildOrThrow();

        queryRunner.createCatalog(DELTA_CATALOG, CONNECTOR_NAME, connectorProperties);
        queryRunner.execute("CREATE SCHEMA " + SCHEMA);

        return queryRunner;
    }

    @AfterAll
    public void tearDown()
            throws IOException
    {
        if (metastore != null) {
            metastore.dropDatabase(SCHEMA, false);
            deleteRecursively(dataDirectory, ALLOW_INSECURE);
        }
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
            assertQuery("SELECT version, operation, isolation_level, read_version, is_blind_append FROM \"" + tableName + "$history\"",
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
            assertTrue(executor.awaitTermination(10, SECONDS));
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
            assertTrue(executor.awaitTermination(10, SECONDS));
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
            assertQuery("SELECT version, operation, isolation_level, read_version, is_blind_append FROM \"" + tableName + "$history\"",
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
            assertTrue(executor.awaitTermination(10, SECONDS));
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
            assertTrue(executor.awaitTermination(10, SECONDS));
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
            assertTrue(executor.awaitTermination(10, SECONDS));
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
            assertTrue(executor.awaitTermination(10, SECONDS));
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
            assertTrue(executor.awaitTermination(10, SECONDS));
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
            assertThat(successfulInsertsCount).isLessThan(threads);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, SECONDS));
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
            assertThat(successfulInsertsCount).isLessThan(threads);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, SECONDS));
        }
    }

    protected void registerTableFromResources(String table, String resourcePath, QueryRunner queryRunner)
            throws IOException
    {
        TrinoFileSystem fileSystem = getConnectorService(queryRunner, TrinoFileSystemFactory.class)
                .create(ConnectorIdentity.ofUser("test"));

        String tableLocation = "local:///" + SCHEMA + "/" + table;
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

        queryRunner.execute(format("CALL system.register_table('%s', '%s', '%s')", SCHEMA, table, tableLocation));
    }
}
