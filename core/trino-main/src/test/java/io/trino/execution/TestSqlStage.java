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
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.SettableFuture;
import io.opentelemetry.api.trace.Span;
import io.trino.client.NodeVersion;
import io.trino.cost.StatsAndCosts;
import io.trino.execution.buffer.PipelinedOutputBuffers;
import io.trino.execution.scheduler.SplitSchedulerStats;
import io.trino.metadata.InternalNode;
import io.trino.metadata.Split;
import io.trino.operator.RetryPolicy;
import io.trino.spi.QueryId;
import io.trino.spi.type.Type;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.testing.TestingSplit;
import io.trino.util.FinalizerService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.tracing.Tracing.noopTracer;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.SqlStage.createSqlStage;
import static io.trino.execution.buffer.PipelinedOutputBuffers.BufferType.ARBITRARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestSqlStage
{
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;

    @BeforeAll
    public void setUp()
    {
        executor = newFixedThreadPool(100, daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));
    }

    @AfterAll
    public void tearDown()
    {
        executor.shutdownNow();
        executor = null;
        scheduledExecutor.shutdownNow();
        scheduledExecutor = null;
    }

    @Test
    @Timeout(2 * 60)
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
        SqlStage stage = createSqlStage(
                stageId,
                createExchangePlanFragment(),
                ImmutableMap.of(),
                new MockRemoteTaskFactory(executor, scheduledExecutor),
                TEST_SESSION,
                true,
                nodeTaskMap,
                executor,
                noopTracer(),
                Span.getInvalid(),
                new SplitSchedulerStats());

        // add listener that fetches stage info when the final status is available
        SettableFuture<StageInfo> finalStageInfo = SettableFuture.create();
        stage.addFinalStageInfoListener(finalStageInfo::set);

        // in a background thread add a ton of tasks
        CompletableFuture<Void> stopped = new CompletableFuture<>();
        CountDownLatch countDownLatch = new CountDownLatch(1000);
        List<MockRemoteTaskFactory.MockRemoteTask> createdTasks = Collections.synchronizedList(new ArrayList<>(2000));
        Future<?> addTasksTask = executor.submit(() -> {
            try {
                PlanNodeId planNodeId = stage.getFragment().getPartitionedSources().get(0);
                ImmutableListMultimap<PlanNodeId, Split> initialSplits = ImmutableListMultimap.of(planNodeId, new Split(TEST_CATALOG_HANDLE, new TestingSplit(true, ImmutableList.of())));
                for (int i = 0; i < 1_000_000; i++) {
                    if (Thread.interrupted()) {
                        return;
                    }
                    InternalNode node = new InternalNode(
                            "source" + i,
                            URI.create("http://10.0.0." + (i / 10_000) + ":" + (i % 10_000)),
                            NodeVersion.UNKNOWN,
                            false);
                    Optional<RemoteTask> created = stage.createTask(
                            node,
                            i,
                            0,
                            Optional.empty(),
                            PipelinedOutputBuffers.createInitial(ARBITRARY),
                            initialSplits,
                            ImmutableSet.of(),
                            Optional.empty(),
                            false);
                    if (created.isPresent()) {
                        if (created.get() instanceof MockRemoteTaskFactory.MockRemoteTask mockTask) {
                            mockTask.start();
                            mockTask.startSplits(1);
                            createdTasks.add(mockTask);
                            countDownLatch.countDown();
                        }
                        else {
                            fail("Expected an instance of MockRemoteTask");
                        }
                    }
                }
            }
            finally {
                while (countDownLatch.getCount() > 0) {
                    countDownLatch.countDown();
                }
                stopped.complete(null);
            }
        });

        // wait for some tasks to be created, and then abort the stage
        countDownLatch.await();
        stage.finish();
        assertThat(createdTasks.size() >= 1000).isTrue();

        StageInfo stageInfo = stage.getStageInfo();
        // stage should not report final info because all tasks have a running driver, but
        // all tasks should be cancelling
        for (TaskInfo info : stageInfo.getTasks()) {
            // Tasks can race with the stage finish operation and be cancelled fully before
            // starting any splits running. These can report either cancelling or fully cancelled
            // depending on the timing of TaskInfo being created
            TaskState taskState = info.getTaskStatus().getState();
            int runningSplits = info.getTaskStatus().getRunningPartitionedDrivers();
            if (runningSplits == 0) {
                assertThat(taskState == TaskState.CANCELING || taskState == TaskState.CANCELED)
                        .describedAs("unexpected task state: " + taskState)
                        .isTrue();
            }
            else {
                assertThat(taskState).isEqualTo(TaskState.CANCELING);
                assertThat(runningSplits > 0)
                        .describedAs("must be running splits to not be already canceled")
                        .isTrue();
            }
        }
        assertThat(finalStageInfo.isDone()).isFalse();

        // cancel the background thread adding tasks
        addTasksTask.cancel(true);
        // wait for the background thread to acknowledge having stopped new task creations
        // so that we know that all tasks are present in the createdTasks list
        stopped.join();

        // finishing all running splits on the task should trigger termination complete
        createdTasks.forEach(task -> {
            task.clearSplits();
            assertThat(task.getTaskStatus().getState()).isEqualTo(TaskState.CANCELED);
        });

        // once the final stage info is available, verify that it is complete
        stageInfo = finalStageInfo.get(1, MINUTES);
        assertThat(stageInfo.getTasks().isEmpty()).isFalse();
        assertThat(stageInfo.isFinalStageInfo()).isTrue();
        assertThat(stage.getStageInfo()).isSameAs(stageInfo);
    }

    private static PlanFragment createExchangePlanFragment()
    {
        PlanNode planNode = new RemoteSourceNode(
                new PlanNodeId("exchange"),
                ImmutableList.of(new PlanFragmentId("source")),
                ImmutableList.of(new Symbol("column")),
                Optional.empty(),
                REPARTITION,
                RetryPolicy.NONE);

        ImmutableMap.Builder<Symbol, Type> types = ImmutableMap.builder();
        for (Symbol symbol : planNode.getOutputSymbols()) {
            types.put(symbol, VARCHAR);
        }
        return new PlanFragment(
                new PlanFragmentId("exchange_fragment_id"),
                planNode,
                types.buildOrThrow(),
                SOURCE_DISTRIBUTION,
                Optional.empty(),
                ImmutableList.of(planNode.getId()),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), planNode.getOutputSymbols()),
                StatsAndCosts.empty(),
                ImmutableList.of(),
                ImmutableList.of(),
                Optional.empty());
    }
}
