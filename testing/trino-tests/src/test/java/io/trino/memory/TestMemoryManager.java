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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static io.trino.SystemSessionProperties.RESOURCE_OVERCOMMIT;
import static io.trino.execution.QueryState.FINISHED;
import static io.trino.memory.LocalMemoryManager.GENERAL_POOL;
import static io.trino.memory.LocalMemoryManager.RESERVED_POOL;
import static io.trino.operator.BlockedReason.WAITING_FOR_MEMORY;
import static io.trino.spi.StandardErrorCode.CLUSTER_OUT_OF_MEMORY;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
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
                .put("query.max-total-memory-per-node", "1kB")
                .put("query.max-memory", "1kB")
                .build();

        try (DistributedQueryRunner queryRunner = createQueryRunner(TINY_SESSION, properties)) {
            assertThatThrownBy(() -> queryRunner.execute("SELECT COUNT(*), clerk FROM orders GROUP BY clerk"))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageStartingWith("Query exceeded per-node total memory limit of ");
            Session session = testSessionBuilder()
                    .setCatalog("tpch")
                    .setSchema("tiny")
                    .setSystemProperty(RESOURCE_OVERCOMMIT, "true")
                    .build();
            queryRunner.execute(session, "SELECT COUNT(*), clerk FROM orders GROUP BY clerk");
        }
    }

    @Test(timeOut = 240_000, expectedExceptions = ExecutionException.class, expectedExceptionsMessageRegExp = ".*Query killed because the cluster is out of memory. Please try again in a few minutes.")
    public void testOutOfMemoryKiller()
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("experimental.reserved-pool-disabled", "false")
                .put("task.verbose-stats", "true")
                .put("query.low-memory-killer.delay", "5s")
                .put("query.low-memory-killer.policy", "total-reservation")
                .build();

        try (DistributedQueryRunner queryRunner = createQueryRunner(TINY_SESSION, properties)) {
            // Reserve all the memory
            QueryId fakeQueryId = new QueryId("fake");
            for (TestingTrinoServer server : queryRunner.getServers()) {
                for (MemoryPool pool : server.getLocalMemoryManager().getPools()) {
                    assertTrue(pool.tryReserve(fakeQueryId, "test", pool.getMaxBytes()));
                }
            }

            List<Future<?>> queryFutures = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                queryFutures.add(executor.submit(() -> queryRunner.execute("SELECT COUNT(*), clerk FROM orders GROUP BY clerk")));
            }

            // Wait for one of the queries to die
            waitForQueryToBeKilled(queryRunner);

            // Release the memory in the reserved pool
            for (TestingTrinoServer server : queryRunner.getServers()) {
                Optional<MemoryPool> reserved = server.getLocalMemoryManager().getReservedPool();
                assertTrue(reserved.isPresent());
                // Free up the entire pool
                reserved.get().free(fakeQueryId, "test", reserved.get().getMaxBytes());
                assertTrue(reserved.get().getFreeBytes() > 0);
            }

            for (Future<?> query : queryFutures) {
                query.get();
            }
        }
    }

    private void waitForQueryToBeKilled(DistributedQueryRunner queryRunner)
            throws InterruptedException
    {
        while (true) {
            for (BasicQueryInfo info : queryRunner.getCoordinator().getQueryManager().getQueries()) {
                if (info.getState().isDone()) {
                    assertNotNull(info.getErrorCode());
                    assertEquals(info.getErrorCode(), CLUSTER_OUT_OF_MEMORY.toErrorCode());
                    return;
                }
            }
            MILLISECONDS.sleep(10);
        }
    }

    @Test(timeOut = 240_000, expectedExceptions = ExecutionException.class, expectedExceptionsMessageRegExp = ".*Query killed because the cluster is out of memory. Please try again in a few minutes.")
    public void testReservedPoolDisabled()
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("experimental.reserved-pool-disabled", "true")
                .put("query.low-memory-killer.delay", "5s")
                .put("query.low-memory-killer.policy", "total-reservation")
                .build();

        try (DistributedQueryRunner queryRunner = createQueryRunner(TINY_SESSION, properties)) {
            // Reserve all the memory
            QueryId fakeQueryId = new QueryId("fake");
            for (TestingTrinoServer server : queryRunner.getServers()) {
                List<MemoryPool> memoryPools = server.getLocalMemoryManager().getPools();
                assertEquals(memoryPools.size(), 1, "Only general pool should exist");
                assertTrue(memoryPools.get(0).tryReserve(fakeQueryId, "test", memoryPools.get(0).getMaxBytes()));
            }

            List<Future<?>> queryFutures = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                queryFutures.add(executor.submit(() -> queryRunner.execute("SELECT COUNT(*), clerk FROM orders GROUP BY clerk")));
            }

            // Wait for one of the queries to die
            waitForQueryToBeKilled(queryRunner);

            // Reserved pool shouldn't exist on the workers and allocation should have been done in the general pool
            for (TestingTrinoServer server : queryRunner.getServers()) {
                Optional<MemoryPool> reserved = server.getLocalMemoryManager().getReservedPool();
                MemoryPool general = server.getLocalMemoryManager().getGeneralPool();
                assertFalse(reserved.isPresent());
                assertTrue(general.getReservedBytes() > 0);
                // Free up the entire pool
                general.free(fakeQueryId, "test", general.getMaxBytes());
                assertTrue(general.getFreeBytes() > 0);
            }

            for (Future<?> query : queryFutures) {
                query.get();
            }
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
                .put("experimental.reserved-pool-disabled", "false")
                .put("task.verbose-stats", "true")
                .build();

        try (DistributedQueryRunner queryRunner = createQueryRunner(TINY_SESSION, properties)) {
            executor.submit(() -> queryRunner.execute(query)).get();

            for (BasicQueryInfo info : queryRunner.getCoordinator().getQueryManager().getQueries()) {
                assertEquals(info.getState(), FINISHED);
            }

            // Make sure we didn't leak any memory on the workers
            for (TestingTrinoServer worker : queryRunner.getServers()) {
                Optional<MemoryPool> reserved = worker.getLocalMemoryManager().getReservedPool();
                assertTrue(reserved.isPresent());
                assertEquals(reserved.get().getMaxBytes(), reserved.get().getFreeBytes());
                MemoryPool general = worker.getLocalMemoryManager().getGeneralPool();
                assertEquals(general.getMaxBytes(), general.getFreeBytes());
            }
        }
    }

    @Test(timeOut = 240_000)
    public void testClusterPools()
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("experimental.reserved-pool-disabled", "false")
                .put("task.verbose-stats", "true")
                .build();

        try (DistributedQueryRunner queryRunner = createQueryRunner(TINY_SESSION, properties)) {
            // Reserve all the memory
            QueryId fakeQueryId = new QueryId("fake");
            for (TestingTrinoServer server : queryRunner.getServers()) {
                for (MemoryPool pool : server.getLocalMemoryManager().getPools()) {
                    assertTrue(pool.tryReserve(fakeQueryId, "test", pool.getMaxBytes()));
                }
            }

            List<Future<?>> queryFutures = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                queryFutures.add(executor.submit(() -> queryRunner.execute("SELECT COUNT(*), clerk FROM orders GROUP BY clerk")));
            }

            ClusterMemoryManager memoryManager = queryRunner.getCoordinator().getClusterMemoryManager();
            ClusterMemoryPool reservedPool;
            while ((reservedPool = memoryManager.getPools().get(RESERVED_POOL)) == null) {
                MILLISECONDS.sleep(10);
            }

            ClusterMemoryPool generalPool = memoryManager.getPools().get(GENERAL_POOL);
            assertNotNull(generalPool);

            // Wait for the pools to become blocked
            while (generalPool.getBlockedNodes() != 2 || reservedPool.getBlockedNodes() != 2) {
                MILLISECONDS.sleep(10);
            }

            // Make sure the queries are assigned to different memory pools
            List<BasicQueryInfo> currentQueryInfos = queryRunner.getCoordinator().getQueryManager().getQueries();
            while (currentQueryInfos.size() != 2 || currentQueryInfos.get(0).getMemoryPool().equals(currentQueryInfos.get(1).getMemoryPool())) {
                MILLISECONDS.sleep(10);
                currentQueryInfos = queryRunner.getCoordinator().getQueryManager().getQueries();
            }

            // Make sure the queries are blocked
            for (BasicQueryInfo info : currentQueryInfos) {
                assertFalse(info.getState().isDone());
            }

            // Check that queries are assigned to expected pools
            assertThat(currentQueryInfos.get(0).getMemoryPool()).isIn(GENERAL_POOL, RESERVED_POOL);
            assertThat(currentQueryInfos.get(1).getMemoryPool()).isIn(GENERAL_POOL, RESERVED_POOL);

            while (!currentQueryInfos.stream().allMatch(TestMemoryManager::isBlockedWaitingForMemory)) {
                MILLISECONDS.sleep(10);
                currentQueryInfos = queryRunner.getCoordinator().getQueryManager().getQueries();
                for (BasicQueryInfo info : currentQueryInfos) {
                    assertFalse(info.getState().isDone());
                }
            }

            // Release the memory in the reserved pool
            for (TestingTrinoServer server : queryRunner.getServers()) {
                Optional<MemoryPool> reserved = server.getLocalMemoryManager().getReservedPool();
                assertTrue(reserved.isPresent());
                // Free up the entire pool
                reserved.get().free(fakeQueryId, "test", reserved.get().getMaxBytes());
                assertTrue(reserved.get().getFreeBytes() > 0);
            }

            // Make sure both queries finish now that there's memory free in the reserved pool.
            // This also checks that the query in the general pool is successfully moved to the reserved pool.
            for (Future<?> query : queryFutures) {
                query.get();
            }

            for (BasicQueryInfo info : queryRunner.getCoordinator().getQueryManager().getQueries()) {
                assertEquals(info.getState(), FINISHED);
            }

            // Make sure we didn't leak any memory on the workers
            for (TestingTrinoServer worker : queryRunner.getServers()) {
                Optional<MemoryPool> reserved = worker.getLocalMemoryManager().getReservedPool();
                assertTrue(reserved.isPresent());
                assertEquals(reserved.get().getMaxBytes(), reserved.get().getFreeBytes());
                MemoryPool general = worker.getLocalMemoryManager().getGeneralPool();
                // Free up the memory we reserved earlier
                general.free(fakeQueryId, "test", general.getMaxBytes());
                assertEquals(general.getMaxBytes(), general.getFreeBytes());
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
                .build();
        try (QueryRunner queryRunner = createQueryRunner(SESSION, properties)) {
            queryRunner.execute(SESSION, "SELECT COUNT(*), repeat(orderstatus, 1000) FROM orders GROUP BY 2");
        }
    }

    @Test(timeOut = 60_000, expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*Query exceeded distributed total memory limit of 2kB.*")
    public void testQueryTotalMemoryLimit()
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("query.max-memory", "1kB")
                .put("query.max-total-memory", "2kB")
                .build();
        try (QueryRunner queryRunner = createQueryRunner(SESSION, properties)) {
            queryRunner.execute(SESSION, "SELECT COUNT(*), repeat(orderstatus, 1000) FROM orders GROUP BY 2");
        }
    }

    @Test(timeOut = 60_000, expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*Query exceeded per-node user memory limit of 1kB.*")
    public void testQueryMemoryPerNodeLimit()
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("task.max-partial-aggregation-memory", "1B")
                .put("query.max-memory-per-node", "1kB")
                .build();
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
