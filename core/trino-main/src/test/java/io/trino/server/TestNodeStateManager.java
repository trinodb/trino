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
package io.trino.server;

import com.google.common.base.Ticker;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.testing.TestingTicker;
import io.trino.execution.StageId;
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.execution.TaskId;
import io.trino.execution.TaskInfo;
import io.trino.execution.TaskState;
import io.trino.node.NodeState;
import io.trino.operator.TaskStats;
import io.trino.server.NodeStateManager.CurrentNodeState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static io.trino.node.NodeState.ACTIVE;
import static io.trino.node.NodeState.DRAINED;
import static io.trino.node.NodeState.DRAINING;
import static io.trino.node.NodeState.SHUTTING_DOWN;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

class TestNodeStateManager
{
    public static final int GRACE_PERIOD_MILLIS = 200;
    private FakeScheduledExecutorService executor;
    private NodeStateManager nodeStateManager;
    private TestingTicker ticker;
    private TestingShutdownAction shutdownAction;
    private AtomicReference<List<TaskInfo>> tasks = new AtomicReference<>(new ArrayList<>());
    private TestTaskObservable sqlTasksObservable;

    @BeforeEach
    public void setUp()
    {
        ticker = new TestingTicker();
        executor = new FakeScheduledExecutorService(ticker);
        shutdownAction = new TestingShutdownAction();
        sqlTasksObservable = new TestTaskObservable();

        nodeStateManager = createNodeStateManager(GRACE_PERIOD_MILLIS);
    }

    @Test
    void testDefaultServerState()
    {
        assertThat(nodeStateManager.getServerState()).isEqualTo(ACTIVE);
    }

    @Test
    void testDrain()
    {
        nodeStateManager.transitionState(DRAINING);
        assertThat(nodeStateManager.getServerState()).isEqualTo(DRAINING);

        ticker.increment(1, SECONDS);
        executor.run();

        await().atMost(1, SECONDS).untilAsserted(() -> assertThat(nodeStateManager.getServerState()).isEqualTo(DRAINED));
    }

    @Test
    void testTransitionToShuttingDown()
    {
        assertThat(nodeStateManager.getServerState()).isEqualTo(ACTIVE);

        nodeStateManager.transitionState(NodeState.SHUTTING_DOWN);
        assertThat(nodeStateManager.getServerState()).isEqualTo(NodeState.SHUTTING_DOWN);

        // here wait for at least 2 grace periods, and add some slack to reduce test flakyness
        await().atMost(4 * GRACE_PERIOD_MILLIS + 100, MILLISECONDS).until(() -> shutdownAction.isShuttingDown());
    }

    @Test
    void testCannotReactivateShuttingDown()
    {
        assertThat(nodeStateManager.getServerState()).isEqualTo(ACTIVE);

        nodeStateManager.transitionState(NodeState.SHUTTING_DOWN);
        assertThat(nodeStateManager.getServerState()).isEqualTo(NodeState.SHUTTING_DOWN);

        // here wait for at least 2 grace periods, and add some slack to reduce test flakyness
        await().atMost(4 * GRACE_PERIOD_MILLIS, MILLISECONDS).until(() -> shutdownAction.isShuttingDown());

        assertThatThrownBy(() -> nodeStateManager.transitionState(ACTIVE))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Invalid state transition");
    }

    @Test
    void testImmediateTransitionToShuttingDownWhenDrained()
    {
        assertThat(nodeStateManager.getServerState()).isEqualTo(ACTIVE);

        nodeStateManager.transitionState(DRAINING);
        assertThat(nodeStateManager.getServerState()).isEqualTo(DRAINING);

        //advance time to execute drain
        ticker.increment(1, SECONDS);
        executor.run();
        // 2 gracePeriods or more
        await().atMost(2 * GRACE_PERIOD_MILLIS + 100, SECONDS)
                .untilAsserted(() -> assertThat(nodeStateManager.getServerState()).isEqualTo(DRAINED));

        // now test the shutdown from the DRAINED state
        nodeStateManager.transitionState(NodeState.SHUTTING_DOWN);
        assertThat(nodeStateManager.getServerState()).isEqualTo(NodeState.SHUTTING_DOWN);

        // here only wait for minimal amount of time, as shutdown should be immediate
        await().pollInterval(1, MILLISECONDS)
                .atMost(100, MILLISECONDS).until(() -> shutdownAction.isShuttingDown());
    }

    @Test
    void testWaitActiveTasksToFinishDuringShutdown()
            throws URISyntaxException
    {
        List<TaskInfo> taskInfos = new ArrayList<>();
        TaskInfo task = TaskInfo.createInitialTask(
                new TaskId(new StageId("query1", 1), 1, 1),
                new URI(""),
                "1",
                false,
                Optional.empty(),
                new TaskStats(Instant.now(), null));
        taskInfos.add(task);
        tasks.set(taskInfos);

        // Draining - will wait for tasks to finish
        nodeStateManager.transitionState(SHUTTING_DOWN);
        assertThat(nodeStateManager.getServerState()).isEqualTo(SHUTTING_DOWN);

        // make sure that nodeStateManager registered a listener for tasks to finish
        ticker.increment(1, SECONDS);
        executor.run();
        await().atMost(1, SECONDS).until(() -> sqlTasksObservable.getTasks().size() == 1);

        // simulate task completion after some time
        tasks.set(Collections.emptyList());
        sqlTasksObservable.getTasks().get(task.taskStatus().getTaskId())
                .stateChanged(TaskState.FINISHED);

        // when NodeStateManager sees task finished - it will drain after another drain period
        await().atMost(1, SECONDS)
                .untilAsserted(() -> assertThat(nodeStateManager.getServerState()).isEqualTo(SHUTTING_DOWN));
    }

    @Test
    void testWaitActiveTasksToFinishDuringDraining()
            throws URISyntaxException
    {
        List<TaskInfo> taskInfos = new ArrayList<>();
        TaskInfo task = TaskInfo.createInitialTask(
                new TaskId(new StageId("query1", 1), 1, 1),
                new URI(""),
                "1",
                false,
                Optional.empty(),
                new TaskStats(Instant.now(), null));
        taskInfos.add(task);
        tasks.set(taskInfos);

        // Draining - will wait for tasks to finish
        nodeStateManager.transitionState(DRAINING);
        assertThat(nodeStateManager.getServerState()).isEqualTo(DRAINING);

        // when that nodeStateManager registered a listener for tasks to finish
        ticker.increment(1, SECONDS);
        executor.run();
        await().atMost(1, SECONDS).until(() -> sqlTasksObservable.getTasks().size() == 1);

        // simulate task completion after some time
        tasks.set(Collections.emptyList());
        sqlTasksObservable.getTasks().get(task.taskStatus().getTaskId())
                .stateChanged(TaskState.FINISHED);

        // when NodeStateManager sees task finished - it will drain after another drain period
        await().atMost(1, SECONDS)
                .untilAsserted(() -> assertThat(nodeStateManager.getServerState()).isEqualTo(DRAINED));
    }

    /*
    This is a regression test for a possible race condition in NodeStateManager.
    A rapid sequence of drain/active/drain updates could be lost - not observed by NodeStateManager
    while it is in one of the sleep periods `sleepUninterruptibly(gracePeriod.toMillis(), MILLISECONDS);`.
    The test executes such sequence of updates exactly when the NodeStateManager is in the sleep - this is tricky,
    so if you see the test being flaky - it should be removed.

    The test fails on the code before the fix with versions was applied.
     */
    @Test
    void testDrainToActiveToDrain()
            throws URISyntaxException, InterruptedException
    {
        nodeStateManager = createNodeStateManager(400);

        List<TaskInfo> taskInfos = new ArrayList<>();
        TaskInfo task = TaskInfo.createInitialTask(
                new TaskId(new StageId("query1", 1), 1, 1),
                new URI(""),
                "1",
                false,
                Optional.empty(),
                new TaskStats(Instant.now(), null));
        taskInfos.add(task);
        tasks.set(taskInfos);

        // Draining - will wait for tasks to finish
        nodeStateManager.transitionState(DRAINING);
        assertThat(nodeStateManager.getServerState()).isEqualTo(DRAINING);

        // when that nodeStateManager registered a listener for tasks to finish
        ticker.increment(2, SECONDS);
        executor.run();
        await().atMost(1, SECONDS).until(() -> sqlTasksObservable.getTasks().size() == 1);

        // simulate task completion after some time
        tasks.set(Collections.emptyList());
        sqlTasksObservable.getTasks().get(task.taskStatus().getTaskId())
                .stateChanged(TaskState.FINISHED);

        // this is ugly, but we need to be in the sleep in waitActiveTasksToFinish just after
        // NodeStateManager exits from the loop and enters the sleepUninterruptibly
        // - so if you see the test being flaky - the test should be removed.
        Thread.sleep(200);

        nodeStateManager.transitionState(ACTIVE);
        tasks.set(taskInfos);
        nodeStateManager.transitionState(DRAINING);

        // and now await and be sure it is still draining!
        await().during(800, MILLISECONDS).atMost(1500, MILLISECONDS)
                .failFast("NodeState should never be drained, while there are still activeTasks",
                        () -> nodeStateManager.getServerState().equals(DRAINED))
                .until(() -> nodeStateManager.getServerState().equals(DRAINING));
    }

    private NodeStateManager createNodeStateManager(int gracePeriodMillis)
    {
        ServerConfig serverConfig = new ServerConfig();
        serverConfig.setCoordinator(false);
        serverConfig.setGracePeriod(new io.airlift.units.Duration(gracePeriodMillis, MILLISECONDS));

        Supplier<List<TaskInfo>> taskInfoSupplier = () -> tasks.get();
        return new NodeStateManager(
                new CurrentNodeState(),
                sqlTasksObservable,
                taskInfoSupplier,
                serverConfig,
                shutdownAction,
                new LifeCycleManager(Collections.emptyList(), null),
                executor);
    }

    private static class TestTaskObservable
            implements NodeStateManager.SqlTasksObservable
    {
        Map<TaskId, StateChangeListener<TaskState>> tasks = new HashMap<>();

        @Override
        public void addStateChangeListener(TaskId taskId, StateChangeListener<TaskState> stateChangeListener)
        {
            tasks.put(taskId, stateChangeListener);
        }

        Map<TaskId, StateChangeListener<TaskState>> getTasks()
        {
            return tasks;
        }
    }

    record Job(Runnable command, long time) {}

    private static class TestingShutdownAction
            implements ShutdownAction
    {
        private boolean shuttingDown;

        @Override
        public void onShutdown()
        {
            shuttingDown = true;
        }

        public boolean isShuttingDown()
        {
            return shuttingDown;
        }
    }

    private static class FakeScheduledExecutorService
            implements ScheduledExecutorService
    {
        private final Ticker clock;
        private List<Job> jobs = new ArrayList<>();

        public FakeScheduledExecutorService(Ticker clock)
        {
            this.clock = clock;
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit)
        {
            jobs.add(new Job(command, clock.read() + unit.toNanos(delay)));
            return null;
        }

        @Override
        public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit)
        {
            return null;
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit)
        {
            return null;
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit)
        {
            return null;
        }

        /**
         * Runs all jobs that are ready by now. Leaves the rest.
         */
        void run()
        {
            long now = clock.read();
            List<Job> ready = jobs.stream()
                    .filter(job -> job.time <= now)
                    .sorted(Comparator.comparing(Job::time))
                    .toList();
            jobs.removeAll(ready);

            new Thread(() -> ready.forEach(j -> j.command.run())).start();
        }

        @Override
        public void shutdown()
        {
        }

        @Override
        public List<Runnable> shutdownNow()
        {
            return List.of();
        }

        @Override
        public boolean isShutdown()
        {
            return false;
        }

        @Override
        public boolean isTerminated()
        {
            return false;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit)
                throws InterruptedException
        {
            return false;
        }

        @Override
        public <T> Future<T> submit(Callable<T> task)
        {
            return null;
        }

        @Override
        public <T> Future<T> submit(Runnable task, T result)
        {
            return null;
        }

        @Override
        public Future<?> submit(Runnable task)
        {
            return null;
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
                throws InterruptedException
        {
            return List.of();
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
                throws InterruptedException
        {
            return List.of();
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
                throws InterruptedException, ExecutionException
        {
            return null;
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException, TimeoutException
        {
            return null;
        }

        @Override
        public void execute(Runnable command)
        {
        }
    }
}
