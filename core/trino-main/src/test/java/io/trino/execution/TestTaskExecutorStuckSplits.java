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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.node.NodeInfo;
import io.airlift.stats.TestingGcMonitor;
import io.airlift.testing.TestingTicker;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Span;
import io.trino.Session;
import io.trino.connector.CatalogProperties;
import io.trino.connector.ConnectorServices;
import io.trino.connector.ConnectorServicesProvider;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.execution.executor.TaskExecutor;
import io.trino.execution.executor.TaskHandle;
import io.trino.execution.executor.timesharing.TimeSharingTaskExecutor;
import io.trino.memory.LocalMemoryManager;
import io.trino.memory.NodeMemoryConfig;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spiller.LocalSpillManager;
import io.trino.spiller.NodeSpillConfig;
import io.trino.version.EmbedVersion;
import org.testng.annotations.Test;

import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.airlift.tracing.Tracing.noopTracer;
import static io.trino.execution.TaskTestUtils.createTestSplitMonitor;
import static io.trino.execution.TaskTestUtils.createTestingPlanner;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestTaskExecutorStuckSplits
{
    @Test
    public void testFailStuckSplitTasks()
            throws InterruptedException, ExecutionException, TimeoutException
    {
        TestingTicker ticker = new TestingTicker();
        TaskManagementExecutor taskManagementExecutor = new TaskManagementExecutor();

        TaskId taskId = new TaskId(new StageId("query", 0), 1, 0);

        TaskExecutor taskExecutor = new TimeSharingTaskExecutor(4, 8, 3, 4, ticker);
        TaskHandle taskHandle = taskExecutor.addTask(
                taskId,
                () -> 1.0,
                1,
                new Duration(1, SECONDS),
                OptionalInt.of(1));

        // Here we explicitly enqueue an indefinite running split runner
        MockSplitRunner mockSplitRunner = new MockSplitRunner();
        taskExecutor.enqueueSplits(taskHandle, false, ImmutableList.of(mockSplitRunner));

        taskExecutor.start();
        try {
            // wait for the task executor to start processing the split
            mockSplitRunner.waitForStart();

            TaskManagerConfig taskManagerConfig = new TaskManagerConfig()
                    .setInterruptStuckSplitTasksEnabled(true)
                    .setInterruptStuckSplitTasksDetectionInterval(new Duration(10, SECONDS))
                    .setInterruptStuckSplitTasksWarningThreshold(new Duration(10, SECONDS))
                    .setInterruptStuckSplitTasksTimeout(new Duration(10, SECONDS));

            try (SqlTaskManager sqlTaskManager = createSqlTaskManager(taskManagerConfig, new NodeMemoryConfig(), taskExecutor, taskManagementExecutor, stackTraceElements -> true)) {
                sqlTaskManager.addStateChangeListener(taskId, (state) -> {
                    if (state.isTerminatingOrDone() && !taskHandle.isDestroyed()) {
                        taskExecutor.removeTask(taskHandle);
                    }
                });

                ticker.increment(30, SECONDS);
                sqlTaskManager.failStuckSplitTasks();

                mockSplitRunner.waitForFinish();
                List<TaskInfo> taskInfos = sqlTaskManager.getAllTaskInfo();
                assertEquals(taskInfos.size(), 1);

                TaskInfo taskInfo = pollTerminatingTaskInfoUntilDone(sqlTaskManager, taskInfos.get(0));
                assertEquals(taskInfo.getTaskStatus().getState(), TaskState.FAILED);
            }
        }
        finally {
            taskExecutor.stop();
            taskManagementExecutor.close();
        }
    }

    private SqlTaskManager createSqlTaskManager(
            TaskManagerConfig taskManagerConfig,
            NodeMemoryConfig nodeMemoryConfig,
            TaskExecutor taskExecutor,
            TaskManagementExecutor taskManagementExecutor,
            Predicate<List<StackTraceElement>> stuckSplitStackTracePredicate)
    {
        return new SqlTaskManager(
                new EmbedVersion("testversion"),
                new NoConnectorServicesProvider(),
                createTestingPlanner(),
                new BaseTestSqlTaskManager.MockLocationFactory(),
                taskExecutor,
                createTestSplitMonitor(),
                new NodeInfo("test"),
                new LocalMemoryManager(new NodeMemoryConfig()),
                taskManagementExecutor,
                taskManagerConfig,
                nodeMemoryConfig,
                new LocalSpillManager(new NodeSpillConfig()),
                new NodeSpillConfig(),
                new TestingGcMonitor(),
                noopTracer(),
                new ExchangeManagerRegistry(),
                stuckSplitStackTracePredicate);
    }

    private static TaskInfo pollTerminatingTaskInfoUntilDone(SqlTaskManager taskManager, TaskInfo taskInfo)
            throws InterruptedException, ExecutionException, TimeoutException
    {
        assertTrue(taskInfo.getTaskStatus().getState().isTerminatingOrDone());
        int attempts = 3;
        while (attempts > 0 && taskInfo.getTaskStatus().getState().isTerminating()) {
            taskInfo = taskManager.getTaskInfo(taskInfo.getTaskStatus().getTaskId(), taskInfo.getTaskStatus().getVersion()).get(5, SECONDS);
            attempts--;
        }
        return taskInfo;
    }

    private static class NoConnectorServicesProvider
            implements ConnectorServicesProvider
    {
        @Override
        public void loadInitialCatalogs() {}

        @Override
        public void ensureCatalogsLoaded(Session session, List<CatalogProperties> catalogs) {}

        @Override
        public void pruneCatalogs(Set<CatalogHandle> catalogsInUse)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConnectorServices getConnectorServices(CatalogHandle catalogHandle)
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class MockSplitRunner
            implements SplitRunner
    {
        private final SettableFuture<Void> startedFuture = SettableFuture.create();
        private final SettableFuture<Void> finishedFuture = SettableFuture.create();

        @GuardedBy("this")
        private Thread runnerThread;
        @GuardedBy("this")
        private boolean closed;

        public void waitForStart()
                throws ExecutionException, InterruptedException, TimeoutException
        {
            startedFuture.get(10, SECONDS);
        }

        public void waitForFinish()
                throws ExecutionException, InterruptedException, TimeoutException
        {
            finishedFuture.get(10, SECONDS);
        }

        @Override
        public int getPipelineId()
        {
            return 0;
        }

        @Override
        public Span getPipelineSpan()
        {
            return Span.getInvalid();
        }

        @Override
        public synchronized boolean isFinished()
        {
            return closed;
        }

        @Override
        public ListenableFuture<Void> processFor(Duration duration)
        {
            startedFuture.set(null);
            synchronized (this) {
                runnerThread = Thread.currentThread();

                if (closed) {
                    finishedFuture.set(null);
                    return immediateVoidFuture();
                }
            }

            while (true) {
                try {
                    Thread.sleep(100000);
                }
                catch (InterruptedException e) {
                    break;
                }
            }

            synchronized (this) {
                closed = true;
            }
            finishedFuture.set(null);

            return immediateVoidFuture();
        }

        @Override
        public String getInfo()
        {
            return "MockSplitRunner";
        }

        @Override
        public synchronized void close()
        {
            closed = true;

            if (runnerThread != null) {
                runnerThread.interrupt();
            }
        }
    }
}
