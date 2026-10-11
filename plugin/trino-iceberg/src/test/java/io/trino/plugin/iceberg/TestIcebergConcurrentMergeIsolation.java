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

import io.trino.Session;
import io.trino.spi.Plugin;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_COMMIT_ERROR;
import static io.trino.spi.type.StandardTypes.BIGINT;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertThatTrinoException;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.iceberg.TableProperties.DELETE_ISOLATION_LEVEL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD) // The blocking function coordinates one pair of queries at a time.
final class TestIcebergConcurrentMergeIsolation
        extends AbstractTestQueryFramework
{
    private static final AtomicReference<MergeBarrier> MERGE_BARRIER = new AtomicReference<>();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = IcebergQueryRunner.builder()
                .addIcebergProperty("iceberg.allowed-extra-properties", DELETE_ISOLATION_LEVEL)
                .build();
        queryRunner.installPlugin(new Plugin()
        {
            @Override
            public Set<Class<?>> getFunctions()
            {
                return Set.of(BlockingMergeSource.class);
            }
        });
        return queryRunner;
    }

    @ParameterizedTest
    @MethodSource("formatVersionsAndPartitions")
    void testConcurrentMergeMatchingKeyInDifferentPartitions(int formatVersion, String latePartition)
            throws Exception
    {
        try (TestTable table = newTrinoTable(
                "test_merge_matching_key",
                "(k bigint, part integer, v integer) WITH (format_version = %s, partitioning = ARRAY['part'])".formatted(formatVersion))) {
            // Establish a snapshot without a matching key. Both MERGEs must choose their INSERT branch.
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (0, 0, 0)", 1);
            String lateMerge = merge(table.getName(), "merge_source_barrier(1)", latePartition, "100", "t.k = s.k", false);
            String firstMerge = merge(table.getName(), "1", "20", "200", "t.k = s.k", false);

            runConcurrentMerges(table.getName(), lateMerge, firstMerge, true);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (BIGINT '0', 0, 0), (BIGINT '1', 20, 200)");

            // A retry sees the first commit and updates its row instead of inserting a duplicate key.
            assertUpdate(merge(table.getName(), "1", latePartition, "100", "t.k = s.k", false), 1);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (BIGINT '0', 0, 0), (BIGINT '1', 20, 100)");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testConcurrentMergeMatchingKeyIntoEmptyTable(boolean filterTarget)
            throws Exception
    {
        try (TestTable table = newTrinoTable("test_merge_empty_table", "(k bigint, part integer, v integer) WITH (partitioning = ARRAY['part'])")) {
            String predicate = "t.k = s.k" + (filterTarget ? " AND t.part IN (10, 20)" : "");
            String lateMerge = merge(table.getName(), "merge_source_barrier(1)", "10", "100", predicate, false);
            String firstMerge = merge(table.getName(), "1", "20", "200", predicate, false);

            runConcurrentMerges(table.getName(), lateMerge, firstMerge, true);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (BIGINT '1', 20, 200)");

            assertUpdate(merge(table.getName(), "1", "10", "100", predicate, false), 1);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (BIGINT '1', 20, 100)");
        }
    }

    @Test
    void testConcurrentMergeWithSnapshotIsolation()
            throws Exception
    {
        try (TestTable table = newTrinoTable(
                "test_merge_snapshot_isolation",
                "(k bigint, part integer, v integer) WITH (partitioning = ARRAY['part'], extra_properties = MAP(ARRAY['" + DELETE_ISOLATION_LEVEL + "'], ARRAY['snapshot']))")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (0, 0, 0)", 1);
            String lateMerge = merge(table.getName(), "merge_source_barrier(1)", "10", "100", "t.k = s.k", false);
            String firstMerge = merge(table.getName(), "1", "20", "200", "t.k = s.k", false);

            runConcurrentMerges(table.getName(), lateMerge, firstMerge, false);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (BIGINT '0', 0, 0), (BIGINT '1', 10, 100), (BIGINT '1', 20, 200)");
        }
    }

    @Test
    void testConcurrentInsertOnlyMergeWithoutTargetRead()
            throws Exception
    {
        try (TestTable table = newTrinoTable("test_merge_without_target_read", "(k bigint, part integer, v integer) WITH (partitioning = ARRAY['part'])")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (0, 0, 0)", 1);
            String lateMerge = merge(table.getName(), "merge_source_barrier(1)", "10", "100", "false", false);
            String firstMerge = merge(table.getName(), "1", "20", "200", "false", false);

            runConcurrentMerges(table.getName(), lateMerge, firstMerge, false);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (BIGINT '0', 0, 0), (BIGINT '1', 10, 100), (BIGINT '1', 20, 200)");
        }
    }

    @ParameterizedTest
    @MethodSource("formatVersionsAndPartitions")
    void testConcurrentMergeWithDisjointReadPartitions(int formatVersion, String latePartition)
            throws Exception
    {
        try (TestTable table = newTrinoTable(
                "test_merge_disjoint_partitions",
                "(k bigint, part integer, v integer) WITH (format_version = %s, partitioning = ARRAY['part'])".formatted(formatVersion))) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, " + latePartition + ", 0), (2, 20, 0)", 2);
            String latePredicate = "t.k = s.k AND " + (latePartition.equals("NULL") ? "t.part IS NULL" : "t.part = " + latePartition);
            String lateMerge = merge(table.getName(), "merge_source_barrier(1)", latePartition, "100", latePredicate, false);
            String firstMerge = merge(table.getName(), "2", "20", "200", "t.k = s.k AND t.part = 20", false);

            runConcurrentMerges(table.getName(), lateMerge, firstMerge, false);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (BIGINT '1', " + latePartition + ", 100), (BIGINT '2', 20, 200)");
        }
    }

    private static Stream<Arguments> formatVersionsAndPartitions()
    {
        return Stream.of(
                arguments(2, "10"),
                arguments(2, "NULL"),
                arguments(3, "10"),
                arguments(3, "NULL"));
    }

    @Test
    void testConcurrentMergeMovingPartition()
            throws Exception
    {
        try (TestTable table = newTrinoTable("test_merge_moving_partition", "(k bigint, part integer, v integer) WITH (partitioning = ARRAY['part'])")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 10, 0), (2, 30, 0)", 2);
            String lateMerge = merge(table.getName(), "merge_source_barrier(1)", "20", "100", "t.k = s.k AND t.part = 10", true);
            String firstMerge = merge(table.getName(), "2", "30", "200", "t.k = s.k AND t.part = 30", false);

            runConcurrentMerges(table.getName(), lateMerge, firstMerge, false);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (BIGINT '1', 20, 100), (BIGINT '2', 30, 200)");
        }
    }

    @Test
    void testConcurrentMergeReadingAnotherTargetPartition()
            throws Exception
    {
        try (TestTable table = newTrinoTable("test_merge_source_partition", "(k bigint, part integer, v integer) WITH (partitioning = ARRAY['part'])")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 20, 0)", 1);
            String lateMerge =
                    """
                    MERGE INTO %1$s t
                    USING (SELECT merge_source_barrier(k) AS k, v FROM %1$s WHERE part = 20) s
                    ON t.k = s.k AND t.part = 10
                    WHEN MATCHED THEN UPDATE SET v = s.v
                    WHEN NOT MATCHED THEN INSERT (k, part, v) VALUES (s.k, 10, s.v)
                    """.formatted(table.getName());
            String firstMerge = merge(table.getName(), "1", "20", "200", "t.k = s.k AND t.part = 20", false);

            runConcurrentMerges(table.getName(), lateMerge, firstMerge, true);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (BIGINT '1', 20, 200)");
        }
    }

    @Test
    void testConcurrentMergeReadingHistoricalSnapshot()
            throws Exception
    {
        try (TestTable table = newTrinoTable("test_merge_historical_source", "(k bigint, part integer, v integer) WITH (partitioning = ARRAY['part'])")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 20, 0)", 1);
            long historicalSnapshot = currentSnapshot(table.getName()).orElseThrow();

            String lateMerge =
                    """
                    MERGE INTO %1$s t
                    USING (SELECT merge_source_barrier(k) AS k, v FROM %1$s FOR VERSION AS OF %2$s WHERE part = 20) s
                    ON false
                    WHEN NOT MATCHED THEN INSERT (k, part, v) VALUES (s.k, 10, s.v)
                    """.formatted(table.getName(), historicalSnapshot);
            String firstMerge =
                    """
                    MERGE INTO %s t
                    USING (VALUES (2, 20, 200)) s(k, part, v)
                    ON false
                    WHEN NOT MATCHED THEN INSERT (k, part, v) VALUES (s.k, s.part, s.v)
                    """.formatted(table.getName());

            runConcurrentMerges(table.getName(), lateMerge, firstMerge, false);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (BIGINT '1', 20, 0), (BIGINT '1', 10, 0), (BIGINT '2', 20, 200)");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testConcurrentMergeReadingPartitionMetadata(boolean optimizeMetadataQueries)
            throws Exception
    {
        Session session = Session.builder(getSession())
                .setSystemProperty("optimize_metadata_queries", Boolean.toString(optimizeMetadataQueries))
                .build();
        try (TestTable table = newTrinoTable("test_merge_partition_metadata", "(k bigint, part integer, v integer) WITH (partitioning = ARRAY['part'])")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (0, 0, 0)", 1);
            String mergeSql =
                    """
                    MERGE INTO %1$s t
                    USING (SELECT %2$s AS k, max(part) + 1 AS part, %3$s AS v FROM %1$s) s
                    ON false
                    WHEN NOT MATCHED THEN INSERT (k, part, v) VALUES (s.k, s.part, s.v)
                    """;
            String lateMerge = mergeSql.formatted(table.getName(), "merge_source_barrier(CAST(max(part) + 1 AS bigint))", "100");
            String firstMerge = mergeSql.formatted(table.getName(), "CAST(max(part) + 1 AS bigint)", "200");

            // Folding the source scan into metadata-derived VALUES must not discard its conflict detection.
            runConcurrentMerges(session, table.getName(), lateMerge, firstMerge, true);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (BIGINT '0', 0, 0), (BIGINT '1', 1, 200)");

            assertUpdate(session, mergeSql.formatted(table.getName(), "CAST(max(part) + 1 AS bigint)", "100"), 1);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (BIGINT '0', 0, 0), (BIGINT '1', 1, 200), (BIGINT '2', 2, 100)");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"files", "partitions"})
    void testConcurrentMergeReadingTargetMetadataTable(String metadataTableType)
            throws Exception
    {
        try (TestTable table = newTrinoTable("test_merge_target_metadata", "(k bigint, part integer, v integer) WITH (partitioning = ARRAY['part'])")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (0, 0, 0)", 1);
            String lateMerge = mergeFromMetadata(table.getName(), table.getName(), metadataTableType, true, 100);
            String firstMerge = mergeFromMetadata(table.getName(), table.getName(), metadataTableType, false, 200);

            runConcurrentMerges(table.getName(), lateMerge, firstMerge, true);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (BIGINT '0', 0, 0), (BIGINT '1', 1, 200)");

            assertUpdate(mergeFromMetadata(table.getName(), table.getName(), metadataTableType, false, 100), 1);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (BIGINT '0', 0, 0), (BIGINT '1', 1, 200), (BIGINT '2', 2, 100)");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"files", "partitions"})
    void testConcurrentMergeReadingUnrelatedMetadataTable(String metadataTableType)
            throws Exception
    {
        try (TestTable target = newTrinoTable("test_merge_unrelated_metadata_target", "(k bigint, part integer, v integer) WITH (partitioning = ARRAY['part'])");
                TestTable source = newTrinoTable("test_merge_unrelated_metadata_source", "(k bigint, part integer, v integer) WITH (partitioning = ARRAY['part'])")) {
            assertUpdate("INSERT INTO " + target.getName() + " VALUES (0, 0, 0)", 1);
            assertUpdate("INSERT INTO " + source.getName() + " VALUES (0, 0, 0)", 1);
            String lateMerge = mergeFromMetadata(target.getName(), source.getName(), metadataTableType, true, 100);
            String firstMerge = mergeFromMetadata(target.getName(), source.getName(), metadataTableType, false, 200);

            runConcurrentMerges(target.getName(), lateMerge, firstMerge, false);
            assertThat(query("SELECT * FROM " + target.getName()))
                    .matches("VALUES (BIGINT '0', 0, 0), (BIGINT '1', 1, 100), (BIGINT '1', 1, 200)");
            assertThat(query("SELECT * FROM " + source.getName()))
                    .matches("VALUES (BIGINT '0', 0, 0)");
        }
    }

    private void runConcurrentMerges(String tableName, String lateMergeSql, String firstMergeSql, boolean expectConflict)
            throws Exception
    {
        runConcurrentMerges(getSession(), tableName, lateMergeSql, firstMergeSql, expectConflict);
    }

    private void runConcurrentMerges(Session session, String tableName, String lateMergeSql, String firstMergeSql, boolean expectConflict)
            throws Exception
    {
        MergeBarrier barrier = new MergeBarrier();
        checkState(MERGE_BARRIER.compareAndSet(null, barrier), "Another MERGE is already blocked");
        try (ExecutorService executor = newSingleThreadExecutor()) {
            OptionalLong initialSnapshot = currentSnapshot(tableName);
            Future<Optional<RuntimeException>> lateMerge = executor.submit(() -> {
                try {
                    getQueryRunner().execute(session, lateMergeSql);
                    return Optional.empty();
                }
                catch (RuntimeException e) {
                    return Optional.of(e);
                }
            });
            try {
                // The nondeterministic function executes after planning has captured the target snapshot.
                boolean started = barrier.started.await(30, SECONDS);
                if (!started && lateMerge.isDone()) {
                    Optional<RuntimeException> failure = lateMerge.get(1, SECONDS);
                    if (failure.isPresent()) {
                        throw failure.orElseThrow();
                    }
                }
                assertThat(started).as("The blocked MERGE reached execution").isTrue();
                assertThat(currentSnapshot(tableName)).isEqualTo(initialSnapshot);
                assertUpdate(session, firstMergeSql, 1);
                assertThat(currentSnapshot(tableName)).isNotEqualTo(initialSnapshot);
                barrier.release.countDown();

                Optional<RuntimeException> failure = lateMerge.get(30, SECONDS);
                if (expectConflict) {
                    assertThat(failure)
                            .as("A stale MERGE must conflict; table contents: %s", computeActual("SELECT * FROM " + tableName))
                            .hasValueSatisfying(error -> assertThatTrinoException(error)
                                    .hasErrorCode(ICEBERG_COMMIT_ERROR)
                                    .hasMessageContaining("Found conflicting files"));
                }
                else {
                    assertThat(failure).isEmpty();
                }
            }
            finally {
                barrier.release.countDown();
            }
        }
        finally {
            MERGE_BARRIER.set(null);
        }
    }

    private OptionalLong currentSnapshot(String tableName)
    {
        Object snapshotId = computeScalar("SELECT max_by(snapshot_id, committed_at) FROM \"" + tableName + "$snapshots\"");
        return snapshotId == null ? OptionalLong.empty() : OptionalLong.of((long) snapshotId);
    }

    private static String merge(String tableName, String key, String partition, String value, String predicate, boolean updatePartition)
    {
        return """
               MERGE INTO %s t
               USING (SELECT %s AS k, CAST(%s AS integer) AS part, %s AS v) s
               ON %s
               WHEN MATCHED THEN UPDATE SET v = s.v%s
               WHEN NOT MATCHED THEN INSERT (k, part, v) VALUES (s.k, s.part, s.v)
               """.formatted(tableName, key, partition, value, predicate, updatePartition ? ", part = s.part" : "");
    }

    private static String mergeFromMetadata(String targetTable, String sourceTable, String metadataTableType, boolean block, int value)
    {
        String key = "CAST(max(partition.part) + 1 AS bigint)";
        if (block) {
            key = "merge_source_barrier(" + key + ")";
        }
        return """
               MERGE INTO %1$s t
               USING (SELECT %4$s AS k, max(partition.part) + 1 AS part, %5$s AS v FROM "%2$s$%3$s") s
               ON false
               WHEN NOT MATCHED THEN INSERT (k, part, v) VALUES (s.k, s.part, s.v)
               """.formatted(targetTable, sourceTable, metadataTableType, key, value);
    }

    public static final class BlockingMergeSource
    {
        private BlockingMergeSource() {}

        @ScalarFunction(value = "merge_source_barrier", deterministic = false)
        @SqlType(BIGINT)
        public static long mergeSourceBarrier(@SqlType(BIGINT) long key)
                throws InterruptedException
        {
            MergeBarrier barrier = MERGE_BARRIER.get();
            checkState(barrier != null, "No MERGE barrier is registered");
            barrier.started.countDown();
            checkState(barrier.release.await(60, SECONDS), "Timed out waiting to release MERGE");
            return key;
        }
    }

    private static final class MergeBarrier
    {
        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
    }
}
