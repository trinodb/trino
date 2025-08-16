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

import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.execution.SqlTaskManager;
import io.trino.execution.StateMachine;
import io.trino.execution.TaskId;
import io.trino.execution.TaskInfo;
import io.trino.execution.TaskState;
import io.trino.node.NodeState;
import io.trino.server.NodeStateManager.CurrentNodeState.VersionedState;
import org.assertj.core.util.VisibleForTesting;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.trino.node.NodeState.ACTIVE;
import static io.trino.node.NodeState.DRAINED;
import static io.trino.node.NodeState.DRAINING;
import static io.trino.node.NodeState.SHUTTING_DOWN;
import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class NodeStateManager
{
    private static final Logger log = Logger.get(NodeStateManager.class);
    private static final Duration LIFECYCLE_STOP_TIMEOUT = new Duration(30, SECONDS);

    private final ScheduledExecutorService shutdownHandler = newSingleThreadScheduledExecutor(threadsNamed("shutdown-handler-%s"));
    private final ExecutorService lifeCycleStopper = newSingleThreadExecutor(threadsNamed("lifecycle-stopper-%s"));
    private final LifeCycleManager lifeCycleManager;
    private final SqlTasksObservable sqlTasksObservable;
    private final Supplier<List<TaskInfo>> taskInfoSupplier;
    private final boolean isCoordinator;
    private final ShutdownAction shutdownAction;
    private final Duration gracePeriod;

    private final ScheduledExecutorService executor;
    private final CurrentNodeState nodeState;

    public interface SqlTasksObservable
    {
        void addStateChangeListener(TaskId taskId, StateMachine.StateChangeListener<TaskState> stateChangeListener);
    }

    @Inject
    public NodeStateManager(
            CurrentNodeState nodeState,
            SqlTaskManager sqlTaskManager,
            ServerConfig serverConfig,
            ShutdownAction shutdownAction,
            LifeCycleManager lifeCycleManager)
    {
        this(nodeState,
                requireNonNull(sqlTaskManager, "sqlTaskManager is null")::addStateChangeListener,
                requireNonNull(sqlTaskManager, "sqlTaskManager is null")::getAllTaskInfo,
                serverConfig,
                shutdownAction,
                lifeCycleManager,
                newSingleThreadScheduledExecutor(threadsNamed("drain-handler-%s")));
    }

    @VisibleForTesting
    public NodeStateManager(
            CurrentNodeState nodeState,
            SqlTasksObservable sqlTasksObservable,
            Supplier<List<TaskInfo>> taskInfoSupplier,
            ServerConfig serverConfig,
            ShutdownAction shutdownAction,
            LifeCycleManager lifeCycleManager,
            ScheduledExecutorService executor)
    {
        this.nodeState = requireNonNull(nodeState, "nodeState is null");
        this.sqlTasksObservable = requireNonNull(sqlTasksObservable, "sqlTasksObservable is null");
        this.taskInfoSupplier = requireNonNull(taskInfoSupplier, "taskInfoSupplier is null");
        this.shutdownAction = requireNonNull(shutdownAction, "shutdownAction is null");
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.isCoordinator = serverConfig.isCoordinator();
        this.gracePeriod = serverConfig.getGracePeriod();
        this.executor = requireNonNull(executor, "executor is null");
    }

    public NodeState getServerState()
    {
        return nodeState.getVersion().state();
    }

    /*
    Below is a diagram with possible states and transitions

    @startuml
    [*] --> ACTIVE
    note "state INACTIVE is not used internally\nis only used when the service is unavailable " as a
    ACTIVE --> SHUTTING_DOWN : shutdown
    ACTIVE --> DRAINING : drain
    DRAINING --> ACTIVE: reactivate
    DRAINING --> DRAINED
    DRAINING --> SHUTTING_DOWN : gracefulShutdown
    DRAINED --> ACTIVE: reactivate
    DRAINED --> SHUTTING_DOWN : terminate
    SHUTTING_DOWN --> [*]
    @enduml

    NOTE: SHUTTING_DOWN is treated as one-way transition to be 100% backwards compatible.
    */
    public synchronized void transitionState(NodeState state)
    {
        VersionedState currState = nodeState.getVersion();
        if (currState.state() == state) {
            return;
        }

        switch (state) {
            case ACTIVE -> {
                if (currState.state() == DRAINING && nodeState.compareAndSetVersion(currState, currState.toActive())) {
                    return;
                }
                if (currState.state() == DRAINED && nodeState.compareAndSetVersion(currState, currState.toActive())) {
                    return;
                }
            }
            case SHUTTING_DOWN -> {
                if (isCoordinator) {
                    throw new UnsupportedOperationException("Cannot shutdown coordinator");
                }
                VersionedState shuttingDown = currState.toShuttingDown();
                if (currState.state() == DRAINED && nodeState.compareAndSetVersion(currState, shuttingDown)) {
                    requestTerminate();
                    return;
                }
                nodeState.setVersion(shuttingDown);
                requestGracefulShutdown();
                return;
            }
            case DRAINING -> {
                if (isCoordinator) {
                    throw new UnsupportedOperationException("Cannot drain coordinator");
                }
                if (currState.state() == ACTIVE && nodeState.compareAndSetVersion(currState, currState.toDraining())) {
                    requestDrain();
                    return;
                }
            }

            case INACTIVE, DRAINED, INVALID, GONE ->
                    throw new IllegalArgumentException("Cannot transition state to internal state " + state);
        }

        throw new IllegalStateException(format("Invalid state transition from %s to %s", currState, state));
    }

    private synchronized void requestDrain()
    {
        log.debug("Drain requested, NodeState: %s", getServerState());

        // wait for a grace period (so that draining state is observed by the coordinator) before starting draining
        // when coordinator observes draining no new tasks are assigned to this worker
        VersionedState expectedState = nodeState.getVersion();
        executor.schedule(() -> drain(expectedState), gracePeriod.toMillis(), MILLISECONDS);
    }

    private void requestTerminate()
    {
        log.info("Immediate Shutdown requested");

        shutdownHandler.schedule(this::terminate, 0, MILLISECONDS);
    }

    private void requestGracefulShutdown()
    {
        log.info("Shutdown requested");

        VersionedState expectedState = nodeState.getVersion();
        // wait for a grace period (so that shutting down state is observed by the coordinator) to start the shutdown sequence
        shutdownHandler.schedule(() -> shutdown(expectedState), gracePeriod.toMillis(), MILLISECONDS);
    }

    private void shutdown(VersionedState expectedState)
    {
        waitActiveTasksToFinish(expectedState);

        terminate();
    }

    private void terminate()
    {
        Future<?> shutdownFuture = lifeCycleStopper.submit(() -> {
            lifeCycleManager.stop();
            return null;
        });
        // terminate the jvm if life cycle cannot be stopped in a timely manner
        try {
            shutdownFuture.get(LIFECYCLE_STOP_TIMEOUT.toMillis(), MILLISECONDS);
        }
        catch (TimeoutException e) {
            log.warn(e, "Timed out waiting for the life cycle to stop");
        }
        catch (InterruptedException e) {
            log.warn(e, "Interrupted while waiting for the life cycle to stop");
            currentThread().interrupt();
        }
        catch (ExecutionException e) {
            log.warn(e, "Problem stopping the life cycle");
        }
        shutdownAction.onShutdown();
    }

    private void drain(VersionedState expectedState)
    {
        if (nodeState.getVersion() == expectedState) {
            waitActiveTasksToFinish(expectedState);
        }
        drainingComplete(expectedState);
    }

    private synchronized void drainingComplete(VersionedState expectedState)
    {
        VersionedState drained = expectedState.toDrained();
        boolean success = nodeState.compareAndSetVersion(expectedState, drained);
        if (success) {
            log.info("Worker State change: DRAINING -> DRAINED, server can be safely SHUT DOWN.");
        }
        else {
            log.info("Worker State change: %s, expected: %s, will not transition to DRAINED", nodeState.getVersion(), expectedState);
        }
    }

    private void waitActiveTasksToFinish(VersionedState expectedState)
    {
        // At this point no new tasks should be scheduled by coordinator on this worker node.
        // Wait for all remaining tasks to finish.
        while (nodeState.getVersion() == expectedState) {
            List<TaskInfo> activeTasks = getActiveTasks();
            log.info("Waiting for %s active tasks to finish", activeTasks.size());
            if (activeTasks.isEmpty()) {
                break;
            }

            waitTasksToFinish(activeTasks, expectedState);
        }

        // wait for another grace period for all task states to be observed by the coordinator
        if (nodeState.getVersion() == expectedState) {
            sleepUninterruptibly(gracePeriod.toMillis(), MILLISECONDS);
        }
    }

    private void waitTasksToFinish(List<TaskInfo> activeTasks, VersionedState expectedState)
    {
        final CountDownLatch countDownLatch = new CountDownLatch(activeTasks.size());

        for (TaskInfo taskInfo : activeTasks) {
            sqlTasksObservable.addStateChangeListener(taskInfo.taskStatus().getTaskId(), newState -> {
                if (newState.isDone()) {
                    log.info("Task %s has finished", taskInfo.taskStatus().getTaskId());
                    countDownLatch.countDown();
                }
            });
        }

        try {
            while (!countDownLatch.await(1, SECONDS)) {
                if (nodeState.getVersion() != expectedState) {
                    log.info("Wait for tasks interrupted by state change, worker is no longer draining.");

                    break;
                }
            }
        }
        catch (InterruptedException e) {
            log.warn("Interrupted while waiting for all tasks to finish");
            currentThread().interrupt();
        }
    }

    private List<TaskInfo> getActiveTasks()
    {
        return taskInfoSupplier.get()
                .stream()
                .filter(taskInfo -> !taskInfo.taskStatus().getState().isDone())
                .collect(toImmutableList());
    }

    public static class CurrentNodeState
            implements Supplier<NodeState>
    {
        private final AtomicReference<VersionedState> nodeState = new AtomicReference<>(new VersionedState(ACTIVE, 0));
        private final AtomicLong stateVersionProvider = new AtomicLong(0);

        @Override
        public NodeState get()
        {
            return getVersion().state();
        }

        private VersionedState getVersion()
        {
            return nodeState.get();
        }

        private void setVersion(VersionedState newValue)
        {
            nodeState.set(newValue);
        }

        private boolean compareAndSetVersion(VersionedState expectedValue, VersionedState newValue)
        {
            return nodeState.compareAndSet(expectedValue, newValue);
        }

        private long nextStateVersion()
        {
            return stateVersionProvider.incrementAndGet();
        }

        class VersionedState
        {
            private final NodeState state;
            private final long version;

            private VersionedState(NodeState state, long version)
            {
                this.state = requireNonNull(state, "state is null");
                this.version = version;
            }

            public VersionedState toActive()
            {
                return new VersionedState(ACTIVE, nextStateVersion());
            }

            public VersionedState toDraining()
            {
                return new VersionedState(DRAINING, nextStateVersion());
            }

            public VersionedState toDrained()
            {
                return new VersionedState(DRAINED, nextStateVersion());
            }

            public VersionedState toShuttingDown()
            {
                return new VersionedState(SHUTTING_DOWN, nextStateVersion());
            }

            public NodeState state()
            {
                return state;
            }

            public long version()
            {
                return version;
            }

            @Override
            public boolean equals(Object o)
            {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                VersionedState that = (VersionedState) o;
                return version == that.version && state == that.state;
            }

            @Override
            public int hashCode()
            {
                return Objects.hash(state, version);
            }

            @Override
            public String toString()
            {
                return "%s-%s".formatted(state.toString(), version);
            }
        }
    }
}
