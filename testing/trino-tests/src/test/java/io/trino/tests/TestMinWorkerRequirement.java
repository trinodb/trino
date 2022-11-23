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
package io.trino.tests;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.trino.Session;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryManager;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResultWithQueryId;
import io.trino.tests.tpch.TpchQueryRunnerBuilder;
import org.testng.annotations.Test;

import static io.trino.SystemSessionProperties.REQUIRED_WORKERS_COUNT;
import static io.trino.SystemSessionProperties.REQUIRED_WORKERS_MAX_WAIT_TIME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

// run single threaded to avoid creating multiple query runners at once
@Test(singleThreaded = true)
public class TestMinWorkerRequirement
{
    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Insufficient active worker nodes. Waited 1.00ns for at least 5 workers, but only 4 workers are active")
    public void testInsufficientWorkerNodes()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = TpchQueryRunnerBuilder.builder()
                .setCoordinatorProperties(ImmutableMap.<String, String>builder()
                        .put("query-manager.required-workers", "5")
                        .put("query-manager.required-workers-max-wait", "1ns")
                        .buildOrThrow())
                .setNodeCount(4)
                .build()) {
            queryRunner.execute("SELECT COUNT(*) from lineitem");
            fail("Expected exception due to insufficient active worker nodes");
        }
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Insufficient active worker nodes. Waited 1.00ns for at least 4 workers, but only 3 workers are active")
    public void testInsufficientWorkerNodesWithCoordinatorExcluded()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = TpchQueryRunnerBuilder.builder()
                .setCoordinatorProperties(ImmutableMap.<String, String>builder()
                        .put("node-scheduler.include-coordinator", "false")
                        .put("query-manager.required-workers", "4")
                        .put("query-manager.required-workers-max-wait", "1ns")
                        .buildOrThrow())
                .setNodeCount(4)
                .build()) {
            queryRunner.execute("SELECT COUNT(*) from lineitem");
            fail("Expected exception due to insufficient active worker nodes");
        }
    }

    @Test
    public void testInsufficientWorkerNodesInternalSystemQuery()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = TpchQueryRunnerBuilder.builder()
                .setCoordinatorProperties(ImmutableMap.<String, String>builder()
                        .put("query-manager.required-workers", "5")
                        .put("query-manager.required-workers-max-wait", "1ns")
                        .buildOrThrow())
                .setNodeCount(4)
                .build()) {
            queryRunner.execute("SELECT 1");
            queryRunner.execute("DESCRIBE lineitem");
            queryRunner.execute("SHOW TABLES");
            queryRunner.execute("SHOW SCHEMAS");
            queryRunner.execute("SHOW CATALOGS");
            queryRunner.execute("SET SESSION required_workers_count=5");
            queryRunner.execute("SELECT * from system.runtime.nodes");
            queryRunner.execute("EXPLAIN SELECT count(*) from lineitem");
        }
    }

    @Test
    public void testInsufficientWorkerNodesAfterDrop()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = TpchQueryRunnerBuilder.builder()
                .setCoordinatorProperties(ImmutableMap.<String, String>builder()
                        .put("query-manager.required-workers", "4")
                        .put("query-manager.required-workers-max-wait", "1ns")
                        .buildOrThrow())
                .setNodeCount(4)
                .build()) {
            queryRunner.execute("SELECT COUNT(*) from lineitem");
            assertEquals(queryRunner.getCoordinator().refreshNodes().getActiveNodes().size(), 4);

            queryRunner.getServers().get(0).close();
            assertEquals(queryRunner.getCoordinator().refreshNodes().getActiveNodes().size(), 3);
            assertThatThrownBy(() -> queryRunner.execute("SELECT COUNT(*) from lineitem"))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessage("Insufficient active worker nodes. Waited 1.00ns for at least 4 workers, but only 3 workers are active");
        }
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Insufficient active worker nodes. Waited 99.00ns for at least 3 workers, but only 2 workers are active")
    public void testRequiredNodesMaxWaitSessionOverride()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = TpchQueryRunnerBuilder.builder()
                .setCoordinatorProperties(ImmutableMap.<String, String>builder()
                        .put("query-manager.required-workers", "3")
                        .put("query-manager.required-workers-max-wait", "1ns")
                        .buildOrThrow())
                .setNodeCount(2)
                .build()) {
            Session session = testSessionBuilder()
                    .setSystemProperty(REQUIRED_WORKERS_COUNT, "3")
                    .setSystemProperty(REQUIRED_WORKERS_MAX_WAIT_TIME, "99ns")
                    .setCatalog("tpch")
                    .setSchema("tiny")
                    .build();
            queryRunner.execute(session, "SELECT COUNT(*) from lineitem");
            fail("Expected exception due to insufficient active worker nodes");
        }
    }

    @Test
    public void testRequiredWorkerNodesSessionOverride()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = TpchQueryRunnerBuilder.builder()
                .setCoordinatorProperties(ImmutableMap.<String, String>builder()
                        .put("query-manager.required-workers", "5")
                        .put("query-manager.required-workers-max-wait", "1ns")
                        .buildOrThrow())
                .setNodeCount(4)
                .build()) {
            // Query should be allowed to run if session override allows it
            Session session = testSessionBuilder()
                    .setSystemProperty(REQUIRED_WORKERS_COUNT, "4")
                    .setCatalog("tpch")
                    .setSchema("tiny")
                    .build();
            queryRunner.execute(session, "SELECT COUNT(*) from lineitem");

            // Query should not be allowed to run because we are 2 nodes short of requirement
            Session require6Workers = Session.builder(session)
                    .setSystemProperty(REQUIRED_WORKERS_COUNT, "6")
                    .build();
            assertThatThrownBy(() -> queryRunner.execute(require6Workers, "SELECT COUNT(*) from lineitem"))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessage("Insufficient active worker nodes. Waited 1.00ns for at least 6 workers, but only 4 workers are active");

            // After adding 2 nodes, query should run
            queryRunner.addServers(2);
            assertEquals(queryRunner.getCoordinator().refreshNodes().getActiveNodes().size(), 6);
            queryRunner.execute(require6Workers, "SELECT COUNT(*) from lineitem");
        }
    }

    @Test
    public void testMultipleRequiredWorkerNodesSessionOverride()
            throws Exception
    {
        ListeningExecutorService service = MoreExecutors.listeningDecorator(newFixedThreadPool(3));
        try (DistributedQueryRunner queryRunner = TpchQueryRunnerBuilder.builder().setNodeCount(1).build()) {
            Session session1 = testSessionBuilder()
                    .setSystemProperty(REQUIRED_WORKERS_COUNT, "2")
                    .setCatalog("tpch")
                    .setSchema("tiny")
                    .build();
            ListenableFuture<MaterializedResultWithQueryId> queryFuture1 = service.submit(() -> queryRunner.executeWithQueryId(session1, "SELECT COUNT(*) from lineitem"));

            Session session2 = Session.builder(session1)
                    .setSystemProperty(REQUIRED_WORKERS_COUNT, "3")
                    .build();
            ListenableFuture<MaterializedResultWithQueryId> queryFuture2 = service.submit(() -> queryRunner.executeWithQueryId(session2, "SELECT COUNT(*) from lineitem"));

            Session session3 = Session.builder(session1)
                    .setSystemProperty(REQUIRED_WORKERS_COUNT, "4")
                    .build();
            ListenableFuture<MaterializedResultWithQueryId> queryFuture3 = service.submit(() -> queryRunner.executeWithQueryId(session3, "SELECT COUNT(*) from lineitem"));

            MILLISECONDS.sleep(1000);
            // None of the queries should run
            assertFalse(queryFuture1.isDone());
            assertFalse(queryFuture2.isDone());
            assertFalse(queryFuture3.isDone());

            queryRunner.addServers(1);
            assertEquals(queryRunner.getCoordinator().refreshNodes().getActiveNodes().size(), 2);
            // After adding 1 node, only 1st query should run
            MILLISECONDS.sleep(1000);
            assertTrue(queryFuture1.get().getResult().getRowCount() > 0);
            QueryManager queryManager = queryRunner.getCoordinator().getQueryManager();
            QueryInfo completedQueryInfo = queryManager.getFullQueryInfo(queryFuture1.get().getQueryId());
            assertTrue(completedQueryInfo.getQueryStats().getResourceWaitingTime().roundTo(SECONDS) >= 1);

            assertFalse(queryFuture2.isDone());
            assertFalse(queryFuture3.isDone());

            // After adding 2 nodes, 2nd and 3rd query should also run
            queryRunner.addServers(2);
            assertEquals(queryRunner.getCoordinator().refreshNodes().getActiveNodes().size(), 4);
            assertTrue(queryFuture2.get().getResult().getRowCount() > 0);
            completedQueryInfo = queryManager.getFullQueryInfo(queryFuture2.get().getQueryId());
            assertTrue(completedQueryInfo.getQueryStats().getResourceWaitingTime().roundTo(SECONDS) >= 2);

            assertTrue(queryFuture3.get().getResult().getRowCount() > 0);
            completedQueryInfo = queryManager.getFullQueryInfo(queryFuture3.get().getQueryId());
            assertTrue(completedQueryInfo.getQueryStats().getResourceWaitingTime().roundTo(SECONDS) >= 2);
        }
        finally {
            service.shutdown();
        }
    }
}
