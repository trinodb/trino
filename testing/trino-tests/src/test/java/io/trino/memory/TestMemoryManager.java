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
package io.trino.memory;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.plugin.blackhole.BlackHolePlugin;
import io.trino.server.BasicQueryInfo;
import io.trino.server.BasicQueryStats;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.tests.tpch.TpchQueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.SystemSessionProperties.RESOURCE_OVERCOMMIT;
import static io.trino.execution.QueryState.FAILED;
import static io.trino.execution.QueryState.FINISHED;
import static io.trino.operator.BlockedReason.WAITING_FOR_MEMORY;
import static io.trino.spi.StandardErrorCode.CLUSTER_OUT_OF_MEMORY;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD) // run single threaded to avoid creating multiple query runners at once
public class TestMemoryManager
{
    private static final Session SESSION = testSessionBuilder()
            .setCatalog("tpch")
            // Use sf1000 to make sure this takes at least one second, so that the memory manager will fail the query
            .setSchema("sf1000")
            .build();

    private static final Session TINY_SESSION = testSessionBuilder()
            .setCatalog("tpch")
            .setSchema("tiny")
            .build();

    private ExecutorService executor;

    @BeforeAll
    public void setUp()
    {
        executor = newCachedThreadPool();
    }

    @AfterAll
    public void shutdown()
    {
        executor.shutdownNow();
        executor = null;
    }

    @Test
    @Timeout(240)
    public void testResourceOverCommit()
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("query.max-memory-per-node", "1kB")
                .put("query.max-memory", "1kB")
                .buildOrThrow();

        try (QueryRunner queryRunner = createQueryRunner(TINY_SESSION, properties)) {
            assertThatThrownBy(() -> queryRunner.execute("SELECT COUNT(*), clerk FROM orders GROUP BY clerk"))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageStartingWith("Query exceeded per-node memory limit of ");
            Session session = testSessionBuilder()
                    .setCatalog("tpch")
                    .setSchema("tiny")
                    .setSystemProperty(RESOURCE_OVERCOMMIT, "true")
                    .build();
            queryRunner.execute(session, "SELECT COUNT(*), clerk FROM orders GROUP BY clerk");
        }
    }

    @Test
    @Timeout(240)
    public void testOutOfMemoryKiller()
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("query.low-memory-killer.delay", "5s")
                .put("query.low-memory-killer.policy", "total-reservation")
                .buildOrThrow();

        try (DistributedQueryRunner queryRunner = createQueryRunner(TINY_SESSION, properties)) {
            queryRunner.installPlugin(new BlackHolePlugin());
            queryRunner.createCatalog("blackhole", "blackhole");
            queryRunner.execute("" +
                    "CREATE TABLE blackhole.default.take_30s(dummy varchar(10)) " +
                    "WITH (split_count=1, pages_per_split=30, rows_per_page=1, page_processing_delay='1s')");

            // Reserve all the memory
            TaskId fakeTaskId = new TaskId(new StageId("fake", 0), 0, 0);
            for (TestingTrinoServer server : queryRunner.getServers()) {
                MemoryPool memoryPool = server.getLocalMemoryManager().getMemoryPool();
                assertThat(memoryPool.tryReserve(fakeTaskId, "test", memoryPool.getMaxBytes())).isTrue();
            }

            int queries = 2;
            CompletionService<MaterializedResult> completionService = new ExecutorCompletionService<>(executor);
            for (int i = 0; i < queries; i++) {
                completionService.submit(() -> queryRunner.execute("" +
                        "SELECT COUNT(*), clerk " +
                        "FROM (SELECT clerk FROM orders UNION ALL SELECT dummy FROM blackhole.default.take_30s)" +
                        "GROUP BY clerk"));
            }

            // Wait for queries to start
            assertEventually(() -> assertThat(queryRunner.getCoordinator().getQueryManager().getQueries()).hasSize(1 + queries));

            // Wait for one of the queries to die
            waitForQueryToBeKilled(queryRunner);

            for (TestingTrinoServer server : queryRunner.getServers()) {
                MemoryPool pool = server.getLocalMemoryManager().getMemoryPool();
                assertThat(pool.getReservedBytes() > 0).isTrue();
                // Free up the entire pool
                pool.free(fakeTaskId, "test", pool.getMaxBytes());
                assertThat(pool.getFreeBytes() > 0).isTrue();
            }

            assertThatThrownBy(() -> {
                for (int i = 0; i < queries; i++) {
                    completionService.take().get();
                }
            })
                    .isInstanceOf(ExecutionException.class)
                    .hasMessageMatching(".*Query killed because the cluster is out of memory. Please try again in a few minutes.");
        }
    }

    private void waitForQueryToBeKilled(QueryRunner queryRunner)
            throws InterruptedException
    {
        while (true) {
            boolean hasRunningQuery = false;
            for (BasicQueryInfo info : queryRunner.getCoordinator().getQueryManager().getQueries()) {
                if (info.getState() == FAILED) {
                    assertThat(info.getErrorCode()).isEqualTo(CLUSTER_OUT_OF_MEMORY.toErrorCode());
                    return;
                }
                assertThat(info.getErrorCode())
                        .describedAs("errorCode unexpectedly present for " + info)
                        .isNull();
                if (!info.getState().isDone()) {
                    hasRunningQuery = true;
                }
            }
            checkState(hasRunningQuery, "All queries already completed without failure");
            MILLISECONDS.sleep(10);
        }
    }

    @Test
    @Timeout(240)
    public void testNoLeak()
            throws Exception
    {
        testNoLeak("SELECT clerk FROM orders"); // TableScan operator
        testNoLeak("SELECT COUNT(*), clerk FROM orders WHERE orderstatus='O' GROUP BY clerk"); // ScanFilterProjectOperator, AggregationOperator
    }

    private void testNoLeak(@Language("SQL") String query)
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.of("task.per-operator-cpu-timer-enabled", "true");

        try (DistributedQueryRunner queryRunner = createQueryRunner(TINY_SESSION, properties)) {
            executor.submit(() -> queryRunner.execute(query)).get();

            for (BasicQueryInfo info : queryRunner.getCoordinator().getQueryManager().getQueries()) {
                assertThat(info.getState()).isEqualTo(FINISHED);
            }

            // Make sure we didn't leak any memory on the workers
            for (TestingTrinoServer worker : queryRunner.getServers()) {
                MemoryPool pool = worker.getLocalMemoryManager().getMemoryPool();
                assertThat(pool.getMaxBytes()).isEqualTo(pool.getFreeBytes());
            }
        }
    }

    @Test
    @Timeout(240)
    public void testClusterPools()
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.of("task.per-operator-cpu-timer-enabled", "true");

        try (DistributedQueryRunner queryRunner = createQueryRunner(TINY_SESSION, properties)) {
            // Reserve all the memory
            TaskId fakeTaskId = new TaskId(new StageId("fake", 0), 0, 0);
            for (TestingTrinoServer server : queryRunner.getServers()) {
                MemoryPool pool = server.getLocalMemoryManager().getMemoryPool();
                assertThat(pool.tryReserve(fakeTaskId, "test", pool.getMaxBytes())).isTrue();
            }

            List<Future<?>> queryFutures = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                // for this test to work, the query has to have enough groups for HashAggregationOperator to go over QueryContext.GUARANTEED_MEMORY
                queryFutures.add(executor.submit(() -> queryRunner.execute("SELECT COUNT(*), cast(orderkey as varchar), partkey FROM lineitem GROUP BY cast(orderkey as varchar), partkey")));
            }

            ClusterMemoryManager memoryManager = queryRunner.getCoordinator().getClusterMemoryManager();

            ClusterMemoryPool clusterPool = memoryManager.getPool();
            assertThat(clusterPool).isNotNull();

            // Wait for the pools to become blocked
            while (clusterPool.getBlockedNodes() != 2) {
                MILLISECONDS.sleep(10);
            }

            // Ger query infos for both queries
            List<BasicQueryInfo> currentQueryInfos = queryRunner.getCoordinator().getQueryManager().getQueries();
            while (currentQueryInfos.size() != 2) {
                MILLISECONDS.sleep(10);
                currentQueryInfos = queryRunner.getCoordinator().getQueryManager().getQueries();
            }

            // Make sure the queries are blocked
            for (BasicQueryInfo info : currentQueryInfos) {
                assertThat(info.getState().isDone()).isFalse();
            }

            while (!currentQueryInfos.stream().allMatch(TestMemoryManager::isBlockedWaitingForMemory)) {
                MILLISECONDS.sleep(10);
                currentQueryInfos = queryRunner.getCoordinator().getQueryManager().getQueries();
                for (BasicQueryInfo info : currentQueryInfos) {
                    assertThat(info.getState().isDone()).isFalse();
                }
            }

            // Release the memory in the memory pool
            for (TestingTrinoServer server : queryRunner.getServers()) {
                MemoryPool pool = server.getLocalMemoryManager().getMemoryPool();
                // Free up the entire pool
                pool.free(fakeTaskId, "test", pool.getMaxBytes());
                assertThat(pool.getFreeBytes() > 0).isTrue();
            }

            // Make sure both queries finish now that there's memory free in the memory pool.
            for (Future<?> query : queryFutures) {
                query.get();
            }

            for (BasicQueryInfo info : queryRunner.getCoordinator().getQueryManager().getQueries()) {
                assertThat(info.getState()).isEqualTo(FINISHED);
            }

            // Make sure we didn't leak any memory on the workers
            for (TestingTrinoServer worker : queryRunner.getServers()) {
                MemoryPool pool = worker.getLocalMemoryManager().getMemoryPool();
                assertThat(pool.getMaxBytes()).isEqualTo(pool.getFreeBytes());
            }
        }
    }

    private static boolean isBlockedWaitingForMemory(BasicQueryInfo info)
    {
        BasicQueryStats stats = info.getQueryStats();
        boolean isWaitingForMemory = stats.getBlockedReasons().contains(WAITING_FOR_MEMORY);
        if (!isWaitingForMemory) {
            return false;
        }

        // queries are not marked as fully blocked if there are no running drivers
        return stats.isFullyBlocked() || stats.getRunningDrivers() == 0;
    }

    @Test
    @Timeout(60)
    public void testQueryUserMemoryLimit()
    {
        assertThatThrownBy(() -> {
            Map<String, String> properties = ImmutableMap.<String, String>builder()
                    .put("task.max-partial-aggregation-memory", "1B")
                    .put("query.max-memory", "1kB")
                    .put("query.max-total-memory", "1GB")
                    .buildOrThrow();
            try (QueryRunner queryRunner = createQueryRunner(SESSION, properties)) {
                queryRunner.execute(SESSION, "SELECT COUNT(*), repeat(orderstatus, 1000) FROM orders GROUP BY 2");
            }
        })
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Query exceeded distributed user memory limit of 1kB");
    }

    @Test
    @Timeout(60)
    public void testQueryTotalMemoryLimit()
    {
        assertThatThrownBy(() -> {
            Map<String, String> properties = ImmutableMap.<String, String>builder()
                    // Relatively high memory limit is required, so that the table scan memory usage alone does not cause the query to fail.
                    .put("query.max-memory", "120MB")
                    .put("query.max-total-memory", "120MB")
                    // The user memory enforcement is tested in testQueryTotalMemoryLimit().
                    // Total memory = user memory + revocable memory.
                    .put("spill-enabled", "true")
                    .put("spiller-spill-path", Paths.get(System.getProperty("java.io.tmpdir"), "trino", "spills", randomUUID().toString()).toString())
                    .put("spiller-max-used-space-threshold", "1.0")
                    .buildOrThrow();
            try (QueryRunner queryRunner = createQueryRunner(SESSION, properties)) {
                queryRunner.execute(SESSION, "SELECT * FROM tpch.sf10.orders ORDER BY orderkey");
            }
        })
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Query exceeded distributed total memory limit of 120MB");
    }

    @Test
    @Timeout(60)
    public void testQueryMemoryPerNodeLimit()
    {
        assertThatThrownBy(() -> {
            Map<String, String> properties = ImmutableMap.<String, String>builder()
                    .put("task.max-partial-aggregation-memory", "1B")
                    .put("query.max-memory-per-node", "1kB")
                    .buildOrThrow();
            try (QueryRunner queryRunner = createQueryRunner(SESSION, properties)) {
                queryRunner.execute(SESSION, "SELECT COUNT(*), repeat(orderstatus, 1000) FROM orders GROUP BY 2");
            }
        })
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Query exceeded per-node memory limit of 1kB");
    }

    public static DistributedQueryRunner createQueryRunner(Session session, Map<String, String> extraProperties)
            throws Exception
    {
        return TpchQueryRunner.builder()
                .amendSession(sessionBuilder -> Session.builder(session))
                .setWorkerCount(1)
                .setExtraProperties(extraProperties)
                .build();
    }
}
