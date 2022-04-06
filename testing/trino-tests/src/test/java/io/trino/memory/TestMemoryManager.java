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
import io.trino.server.BasicQueryInfo;
import io.trino.server.BasicQueryStats;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.QueryId;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tests.tpch.TpchQueryRunnerBuilder;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
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
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

// run single threaded to avoid creating multiple query runners at once
@Test(singleThreaded = true)
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

    @BeforeClass
    public void setUp()
    {
        executor = newCachedThreadPool();
    }

    @AfterClass(alwaysRun = true)
    public void shutdown()
    {
        executor.shutdownNow();
        executor = null;
    }

    @Test(timeOut = 240_000)
    public void testResourceOverCommit()
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("query.max-memory-per-node", "1kB")
                .put("query.max-memory", "1kB")
                .buildOrThrow();

        try (DistributedQueryRunner queryRunner = createQueryRunner(TINY_SESSION, properties)) {
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

    @Test(timeOut = 240_000)
    public void testOutOfMemoryKiller()
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("query.low-memory-killer.delay", "5s")
                .put("query.low-memory-killer.policy", "total-reservation")
                .buildOrThrow();

        try (DistributedQueryRunner queryRunner = createQueryRunner(TINY_SESSION, properties)) {
            // Reserve all the memory
            QueryId fakeQueryId = new QueryId("fake");
            for (TestingTrinoServer server : queryRunner.getServers()) {
                MemoryPool memoryPool = server.getLocalMemoryManager().getMemoryPool();
                assertTrue(memoryPool.tryReserve(fakeQueryId, "test", memoryPool.getMaxBytes()));
            }

            List<Future<?>> queryFutures = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                queryFutures.add(executor.submit(() -> queryRunner.execute("SELECT COUNT(*), clerk FROM orders GROUP BY clerk")));
            }

            // Wait for queries to start
            assertEventually(() -> assertThat(queryRunner.getCoordinator().getQueryManager().getQueries()).hasSize(queryFutures.size()));

            // Wait for one of the queries to die
            waitForQueryToBeKilled(queryRunner);

            for (TestingTrinoServer server : queryRunner.getServers()) {
                MemoryPool pool = server.getLocalMemoryManager().getMemoryPool();
                assertTrue(pool.getReservedBytes() > 0);
                // Free up the entire pool
                pool.free(fakeQueryId, "test", pool.getMaxBytes());
                assertTrue(pool.getFreeBytes() > 0);
            }

            assertThatThrownBy(() -> {
                for (Future<?> query : queryFutures) {
                    query.get();
                }
            })
                    .isInstanceOf(ExecutionException.class)
                    .hasMessageMatching(".*Query killed because the cluster is out of memory. Please try again in a few minutes.");
        }
    }

    private void waitForQueryToBeKilled(DistributedQueryRunner queryRunner)
            throws InterruptedException
    {
        while (true) {
            boolean hasRunningQuery = false;
            for (BasicQueryInfo info : queryRunner.getCoordinator().getQueryManager().getQueries()) {
                if (info.getState() == FAILED) {
                    assertNotNull(info.getErrorCode());
                    assertEquals(info.getErrorCode(), CLUSTER_OUT_OF_MEMORY.toErrorCode());
                    return;
                }
                assertNull(info.getErrorCode(), "errorCode unexpectedly present for " + info);
                if (!info.getState().isDone()) {
                    hasRunningQuery = true;
                }
            }
            checkState(hasRunningQuery, "All queries already completed without failure");
            MILLISECONDS.sleep(10);
        }
    }

    @Test(timeOut = 240_000)
    public void testNoLeak()
            throws Exception
    {
        testNoLeak("SELECT clerk FROM orders"); // TableScan operator
        testNoLeak("SELECT COUNT(*), clerk FROM orders WHERE orderstatus='O' GROUP BY clerk"); // ScanFilterProjectOperator, AggregationOperator
    }

    private void testNoLeak(@Language("SQL") String query)
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("task.verbose-stats", "true")
                .buildOrThrow();

        try (DistributedQueryRunner queryRunner = createQueryRunner(TINY_SESSION, properties)) {
            executor.submit(() -> queryRunner.execute(query)).get();

            for (BasicQueryInfo info : queryRunner.getCoordinator().getQueryManager().getQueries()) {
                assertEquals(info.getState(), FINISHED);
            }

            // Make sure we didn't leak any memory on the workers
            for (TestingTrinoServer worker : queryRunner.getServers()) {
                MemoryPool pool = worker.getLocalMemoryManager().getMemoryPool();
                assertEquals(pool.getMaxBytes(), pool.getFreeBytes());
            }
        }
    }

    @Test(timeOut = 240_000)
    public void testClusterPools()
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("task.verbose-stats", "true")
                .buildOrThrow();

        try (DistributedQueryRunner queryRunner = createQueryRunner(TINY_SESSION, properties)) {
            // Reserve all the memory
            QueryId fakeQueryId = new QueryId("fake");
            for (TestingTrinoServer server : queryRunner.getServers()) {
                MemoryPool pool = server.getLocalMemoryManager().getMemoryPool();
                assertTrue(pool.tryReserve(fakeQueryId, "test", pool.getMaxBytes()));
            }

            List<Future<?>> queryFutures = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                queryFutures.add(executor.submit(() -> queryRunner.execute("SELECT COUNT(*), clerk FROM orders GROUP BY clerk")));
            }

            ClusterMemoryManager memoryManager = queryRunner.getCoordinator().getClusterMemoryManager();

            ClusterMemoryPool clusterPool = memoryManager.getPool();
            assertNotNull(clusterPool);

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
                assertFalse(info.getState().isDone());
            }

            while (!currentQueryInfos.stream().allMatch(TestMemoryManager::isBlockedWaitingForMemory)) {
                MILLISECONDS.sleep(10);
                currentQueryInfos = queryRunner.getCoordinator().getQueryManager().getQueries();
                for (BasicQueryInfo info : currentQueryInfos) {
                    assertFalse(info.getState().isDone());
                }
            }

            // Release the memory in the memory pool
            for (TestingTrinoServer server : queryRunner.getServers()) {
                MemoryPool pool = server.getLocalMemoryManager().getMemoryPool();
                // Free up the entire pool
                pool.free(fakeQueryId, "test", pool.getMaxBytes());
                assertTrue(pool.getFreeBytes() > 0);
            }

            // Make sure both queries finish now that there's memory free in the memory pool.
            for (Future<?> query : queryFutures) {
                query.get();
            }

            for (BasicQueryInfo info : queryRunner.getCoordinator().getQueryManager().getQueries()) {
                assertEquals(info.getState(), FINISHED);
            }

            // Make sure we didn't leak any memory on the workers
            for (TestingTrinoServer worker : queryRunner.getServers()) {
                MemoryPool pool = worker.getLocalMemoryManager().getMemoryPool();
                assertEquals(pool.getMaxBytes(), pool.getFreeBytes());
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

    @Test(timeOut = 60_000, expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*Query exceeded distributed user memory limit of 1kB.*")
    public void testQueryUserMemoryLimit()
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("task.max-partial-aggregation-memory", "1B")
                .put("query.max-memory", "1kB")
                .put("query.max-total-memory", "1GB")
                .buildOrThrow();
        try (QueryRunner queryRunner = createQueryRunner(SESSION, properties)) {
            queryRunner.execute(SESSION, "SELECT COUNT(*), repeat(orderstatus, 1000) FROM orders GROUP BY 2");
        }
    }

    @Test(timeOut = 60_000, expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*Query exceeded distributed total memory limit of 120MB.*")
    public void testQueryTotalMemoryLimit()
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                // Relatively high memory limit is required, so that the table scan memory usage alone does not cause the query to fail.
                .put("query.max-memory", "120MB")
                .put("query.max-total-memory", "120MB")
                // The user memory enforcement is tested in testQueryTotalMemoryLimit().
                // Total memory = user memory + revocable memory.
                .put("spill-enabled", "true")
                .put("spiller-spill-path", Paths.get(System.getProperty("java.io.tmpdir"), "trino", "spills").toString())
                .put("spiller-max-used-space-threshold", "1.0")
                .buildOrThrow();
        try (QueryRunner queryRunner = createQueryRunner(SESSION, properties)) {
            queryRunner.execute(SESSION, "SELECT * FROM tpch.sf10.orders ORDER BY orderkey");
        }
    }

    @Test(timeOut = 60_000, expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*Query exceeded per-node memory limit of 1kB.*")
    public void testQueryMemoryPerNodeLimit()
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("task.max-partial-aggregation-memory", "1B")
                .put("query.max-memory-per-node", "1kB")
                .buildOrThrow();
        try (QueryRunner queryRunner = createQueryRunner(SESSION, properties)) {
            queryRunner.execute(SESSION, "SELECT COUNT(*), repeat(orderstatus, 1000) FROM orders GROUP BY 2");
        }
    }

    public static DistributedQueryRunner createQueryRunner(Session session, Map<String, String> extraProperties)
            throws Exception
    {
        return TpchQueryRunnerBuilder.builder()
                .amendSession(sessionBuilder -> Session.builder(session))
                .setNodeCount(2)
                .setExtraProperties(extraProperties)
                .build();
    }
}
