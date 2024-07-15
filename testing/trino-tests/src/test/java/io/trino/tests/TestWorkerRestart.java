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

import io.trino.execution.QueryManager;
import io.trino.server.BasicQueryInfo;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.tests.tpch.TpchQueryRunner;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.execution.QueryState.RUNNING;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Test that tasks are cleanly rejected when a node restarts (same node ID, different node instance ID).
 */
@Execution(SAME_THREAD) // run single threaded to avoid creating multiple query runners at once
public class TestWorkerRestart
{
    // When working with the test locally it's practical to run multiple iterations at once.
    private static final int TEST_ITERATIONS = 1;

    /**
     * Test that query passes even if worker is restarted just before query.
     */
    @RepeatedTest(TEST_ITERATIONS)
    @Timeout(90)
    public void testRestartBeforeQuery()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = TpchQueryRunner.builder().build();
                ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%d"))) {
            try {
                // Ensure everything initialized
                assertThat((long) queryRunner.execute("SELECT count(*) FROM tpch.tiny.lineitem").getOnlyValue())
                        .isEqualTo(60_175);

                restartWorker(queryRunner);
                // Even though the worker is restarted before we send a query, it is not fully announced to the coordinator.
                // Coordinator will still try to send the query to the worker thinking it is the previous instance of it.
                Future<MaterializedResult> future = executor.submit(() -> queryRunner.execute("SELECT count(*) FROM tpch.sf1.lineitem -- " + randomUUID()));
                future.get(); // query should succeed

                // Ensure that the restarted worker is able to serve queries.
                assertThat((long) queryRunner.execute("SELECT count(*) FROM tpch.tiny.lineitem").getOnlyValue())
                        .isEqualTo(60_175);
            }
            finally {
                cancelQueries(queryRunner);
            }
        }
    }

    /**
     * Test that query fails with when worker crashes during its execution, but next query (e.g. retried query) succeeds without issues.
     */
    @RepeatedTest(TEST_ITERATIONS)
    @Timeout(90)
    public void testRestartDuringQuery()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = TpchQueryRunner.builder().build();
                ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%d"))) {
            try {
                // Ensure everything initialized
                assertThat((long) queryRunner.execute("SELECT count(*) FROM tpch.tiny.lineitem").getOnlyValue())
                        .isEqualTo(60_175);

                String sql = "SELECT count(*) FROM tpch.sf1000000000.lineitem -- " + randomUUID();
                Future<MaterializedResult> future = executor.submit(() -> queryRunner.execute(sql));
                waitForQueryStart(queryRunner, sql);
                restartWorker(queryRunner);
                assertThatThrownBy(future::get)
                        .isInstanceOf(ExecutionException.class)
                        .cause().hasMessageFindingMatch("^Expected response code from \\S+ to be 200, but was 500" +
                                                        "|Error fetching \\S+: Expected response code to be 200, but was 500");

                // Ensure that the restarted worker is able to serve queries.
                assertThat((long) queryRunner.execute("SELECT count(*) FROM tpch.tiny.lineitem").getOnlyValue())
                        .isEqualTo(60_175);
            }
            finally {
                cancelQueries(queryRunner);
            }
        }
    }

    /**
     * Test that query passes if a worker crashed before query started but it still potentially starting up when query is being scheduled.
     */
    @RepeatedTest(TEST_ITERATIONS)
    @Timeout(90)
    public void testStartDuringQuery()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = TpchQueryRunner.builder().build();
                ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%d"))) {
            try {
                // Ensure everything initialized
                assertThat((long) queryRunner.execute("SELECT count(*) FROM tpch.tiny.lineitem").getOnlyValue())
                        .isEqualTo(60_175);

                TestingTrinoServer worker = queryRunner.getServers().stream()
                        .filter(server -> !server.isCoordinator())
                        .findFirst().orElseThrow();
                worker.close();
                Future<MaterializedResult> future = executor.submit(() -> queryRunner.execute("SELECT count(*) FROM tpch.tiny.lineitem -- " + randomUUID()));
                // the worker is shut down already, but restartWorker() will reuse its address
                queryRunner.restartWorker(worker);
                future.get(); // query should succeed

                // Ensure that the restarted worker is able to serve queries.
                assertThat((long) queryRunner.execute("SELECT count(*) FROM tpch.tiny.lineitem").getOnlyValue())
                        .isEqualTo(60_175);
            }
            finally {
                cancelQueries(queryRunner);
            }
        }
    }

    private static void waitForQueryStart(DistributedQueryRunner queryRunner, String sql)
    {
        assertEventually(() -> {
            BasicQueryInfo queryInfo = queryRunner.getCoordinator().getQueryManager().getQueries().stream()
                    .filter(query -> query.getQuery().equals(sql))
                    .collect(onlyElement());
            assertThat(queryInfo.getState()).isEqualTo(RUNNING);
        });
    }

    private static void restartWorker(DistributedQueryRunner queryRunner)
            throws Exception
    {
        TestingTrinoServer worker = queryRunner.getServers().stream()
                .filter(server -> !server.isCoordinator())
                .findFirst().orElseThrow();
        queryRunner.restartWorker(worker);
    }

    private static void cancelQueries(DistributedQueryRunner queryRunner)
    {
        QueryManager queryManager = queryRunner.getCoordinator().getQueryManager();
        queryManager.getQueries().stream()
                .map(BasicQueryInfo::getQueryId)
                .forEach(queryManager::cancelQuery);
    }
}
