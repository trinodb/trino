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
    @RepeatedTest(5
    )
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
                assertThatThrownBy(future::get)
                        .hasMessageMatching("""
                                .*QueryFailedException: Expected response code from \\S+ to be 200, but was 400
                                Node instance ID in the request \\[\\S+] does not match this node instance ID \\[\\S+].*\
                                """);

                // Ensure that the restarted worker eventually becomes usable to serve queries.
                // TODO this could perhaps happen quicker -- the worker being restarted could be temporarily not used and once it is fully started, the queries should go through.
                assertEventually(() -> assertThat((long) queryRunner.execute("SELECT count(*) FROM tpch.tiny.lineitem").getOnlyValue())
                        .isEqualTo(60_175));
            }
            finally {
                cancelQueries(queryRunner);
            }
        }
    }

    @RepeatedTest(5)
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
                        .hasMessageMatching("""
                                .*QueryFailedException: Expected response code from \\S+ to be 200, but was 400
                                Node instance ID in the request \\[\\S+] does not match this node instance ID \\[\\S+].*\
                                """);

                // Ensure that the restarted worker eventually becomes usable to serve queries.
                // TODO this could perhaps happen quicker -- the worker being restarted could be temporarily not used and once it is fully started, the queries should go through.
                assertEventually(() -> assertThat((long) queryRunner.execute("SELECT count(*) FROM tpch.tiny.lineitem").getOnlyValue())
                        .isEqualTo(60_175));
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
