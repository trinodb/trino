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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.SettableFuture;
import io.trino.client.NodeVersion;
import io.trino.cost.StatsAndCosts;
import io.trino.execution.MockRemoteTaskFactory.MockRemoteTask;
import io.trino.execution.scheduler.SplitSchedulerStats;
import io.trino.failuredetector.NoOpFailureDetector;
import io.trino.metadata.InternalNode;
import io.trino.server.DynamicFilterService;
import io.trino.spi.QueryId;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.util.FinalizerService;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.SqlStageExecution.createSqlStageExecution;
import static io.trino.execution.buffer.OutputBuffers.BufferType.ARBITRARY;
import static io.trino.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.operator.StageExecutionDescriptor.ungroupedExecution;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public class TestSqlStageExecution
{
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;

    @BeforeClass
    public void setUp()
    {
        executor = newFixedThreadPool(100, daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
        executor = null;
        scheduledExecutor.shutdownNow();
        scheduledExecutor = null;
    }

    @Test(timeOut = 2 * 60 * 1000)
    public void testFinalStageInfo()
            throws Exception
    {
        // run test a few times to catch any race conditions
        // this is not done with TestNG invocation count so there can be a global time limit on the test
        for (int iteration = 0; iteration < 10; iteration++) {
            testFinalStageInfoInternal();
        }
    }

    private void testFinalStageInfoInternal()
            throws Exception
    {
        NodeTaskMap nodeTaskMap = new NodeTaskMap(new FinalizerService());

        StageId stageId = new StageId(new QueryId("query"), 0);
        SqlStageExecution stage = createSqlStageExecution(
                stageId,
                createExchangePlanFragment(),
                ImmutableMap.of(),
                new MockRemoteTaskFactory(executor, scheduledExecutor),
                TEST_SESSION,
                true,
                nodeTaskMap,
                executor,
                new NoOpFailureDetector(),
                new DynamicFilterService(createTestMetadataManager(), new TypeOperators(), new DynamicFilterConfig()),
                new SplitSchedulerStats());
        stage.setOutputBuffers(createInitialEmptyOutputBuffers(ARBITRARY));

        // add listener that fetches stage info when the final status is available
        SettableFuture<StageInfo> finalStageInfo = SettableFuture.create();
        stage.addFinalStageInfoListener(finalStageInfo::set);

        // in a background thread add a ton of tasks
        CountDownLatch latch = new CountDownLatch(1000);
        Future<?> addTasksTask = executor.submit(() -> {
            try {
                for (int i = 0; i < 1_000_000; i++) {
                    if (Thread.interrupted()) {
                        return;
                    }
                    InternalNode node = new InternalNode(
                            "source" + i,
                            URI.create("http://10.0.0." + (i / 10_000) + ":" + (i % 10_000)),
                            NodeVersion.UNKNOWN,
                            false);
                    stage.scheduleTask(node, i);
                    latch.countDown();
                }
            }
            finally {
                while (latch.getCount() > 0) {
                    latch.countDown();
                }
            }
        });

        // wait for some tasks to be created, and then abort the query
        latch.await(1, MINUTES);
        assertFalse(stage.getStageInfo().getTasks().isEmpty());
        stage.abort();

        // once the final stage info is available, verify that it is complete
        StageInfo stageInfo = finalStageInfo.get(1, MINUTES);
        assertFalse(stageInfo.getTasks().isEmpty());
        assertTrue(stageInfo.isCompleteInfo());
        assertSame(stage.getStageInfo(), stageInfo);

        // cancel the background thread adding tasks
        addTasksTask.cancel(true);
    }

    @Test
    public void testIsAnyTaskBlocked()
    {
        NodeTaskMap nodeTaskMap = new NodeTaskMap(new FinalizerService());

        StageId stageId = new StageId(new QueryId("query"), 0);
        SqlStageExecution stage = createSqlStageExecution(
                stageId,
                createExchangePlanFragment(),
                ImmutableMap.of(),
                new MockRemoteTaskFactory(executor, scheduledExecutor),
                TEST_SESSION,
                true,
                nodeTaskMap,
                executor,
                new NoOpFailureDetector(),
                new DynamicFilterService(createTestMetadataManager(), new TypeOperators(), new DynamicFilterConfig()),
                new SplitSchedulerStats());
        stage.setOutputBuffers(createInitialEmptyOutputBuffers(ARBITRARY));

        InternalNode node1 = new InternalNode("other1", URI.create("http://127.0.0.1:11"), NodeVersion.UNKNOWN, false);
        InternalNode node2 = new InternalNode("other2", URI.create("http://127.0.0.2:12"), NodeVersion.UNKNOWN, false);
        MockRemoteTask task1 = (MockRemoteTask) stage.scheduleTask(node1, 1).get();
        MockRemoteTask task2 = (MockRemoteTask) stage.scheduleTask(node2, 2).get();

        // both tasks' buffers are under utilized
        assertFalse(stage.isAnyTaskBlocked());

        // set one of the task's buffer to be over utilized
        task1.setOutputBufferOverUtilized(true);
        assertTrue(stage.isAnyTaskBlocked());

        // set both the tasks' buffers to be over utilized
        task2.setOutputBufferOverUtilized(true);
        assertTrue(stage.isAnyTaskBlocked());
    }

    private static PlanFragment createExchangePlanFragment()
    {
        PlanNode planNode = new RemoteSourceNode(
                new PlanNodeId("exchange"),
                ImmutableList.of(new PlanFragmentId("source")),
                ImmutableList.of(new Symbol("column")),
                Optional.empty(),
                REPARTITION);

        ImmutableMap.Builder<Symbol, Type> types = ImmutableMap.builder();
        for (Symbol symbol : planNode.getOutputSymbols()) {
            types.put(symbol, VARCHAR);
        }
        return new PlanFragment(
                new PlanFragmentId("exchange_fragment_id"),
                planNode,
                types.build(),
                SOURCE_DISTRIBUTION,
                ImmutableList.of(planNode.getId()),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), planNode.getOutputSymbols()),
                ungroupedExecution(),
                StatsAndCosts.empty(),
                Optional.empty());
    }
}
