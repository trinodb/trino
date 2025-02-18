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
import io.trino.execution.TaskInfo;
import io.trino.metadata.NodeState;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.trino.metadata.NodeState.ACTIVE;
import static io.trino.metadata.NodeState.DRAINED;
import static io.trino.metadata.NodeState.DRAINING;
import static io.trino.metadata.NodeState.SHUTTING_DOWN;
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
    private final SqlTaskManager sqlTaskManager;
    private final boolean isCoordinator;
    private final ShutdownAction shutdownAction;
    private final Duration gracePeriod;

    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor(threadsNamed("drain-handler-%s"));
    private final AtomicReference<NodeState> nodeState = new AtomicReference<>(ACTIVE);

    @Inject
    public NodeStateManager(
            SqlTaskManager sqlTaskManager,
            ServerConfig serverConfig,
            ShutdownAction shutdownAction,
            LifeCycleManager lifeCycleManager)
    {
        this.sqlTaskManager = requireNonNull(sqlTaskManager, "sqlTaskManager is null");
        this.shutdownAction = requireNonNull(shutdownAction, "shutdownAction is null");
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.isCoordinator = serverConfig.isCoordinator();
        this.gracePeriod = serverConfig.getGracePeriod();
    }

    public NodeState getServerState()
    {
        return nodeState.get();
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
        NodeState currState = nodeState.get();
        if (currState == state) {
            return;
        }

        switch (state) {
            case ACTIVE -> {
                if (currState == DRAINING && nodeState.compareAndSet(DRAINING, ACTIVE)) {
                    return;
                }
                if (currState == DRAINED && nodeState.compareAndSet(DRAINED, ACTIVE)) {
                    return;
                }
            }
            case SHUTTING_DOWN -> {
                if (currState == DRAINED && nodeState.compareAndSet(DRAINED, SHUTTING_DOWN)) {
                    requestTerminate();
                    return;
                }
                requestGracefulShutdown();
                nodeState.set(SHUTTING_DOWN);
                return;
            }
            case DRAINING -> {
                if (currState == ACTIVE && nodeState.compareAndSet(ACTIVE, DRAINING)) {
                    requestDrain();
                    return;
                }
            }
            case DRAINED -> throw new IllegalStateException(format("Invalid state transition from %s to %s, transition to DRAINED is internal only", currState, state));

            case INACTIVE -> throw new IllegalStateException(format("Invalid state transition from %s to %s, INACTIVE is not a valid internal state", currState, state));
        }

        throw new IllegalStateException(format("Invalid state transition from %s to %s", currState, state));
    }

    private synchronized void requestDrain()
    {
        log.debug("Drain requested, NodeState: " + getServerState());
        if (isCoordinator) {
            throw new UnsupportedOperationException("Cannot drain coordinator");
        }

        // wait for a grace period (so that draining state is observed by the coordinator) before starting draining
        // when coordinator observes draining no new tasks are assigned to this worker
        executor.schedule(this::drain, gracePeriod.toMillis(), MILLISECONDS);
    }

    private void requestTerminate()
    {
        log.info("Immediate Shutdown requested");
        if (isCoordinator) {
            throw new UnsupportedOperationException("Cannot shutdown coordinator");
        }

        shutdownHandler.schedule(this::terminate, 0, MILLISECONDS);
    }

    private void requestGracefulShutdown()
    {
        log.info("Shutdown requested");
        if (isCoordinator) {
            throw new UnsupportedOperationException("Cannot shutdown coordinator");
        }

        // wait for a grace period (so that shutting down state is observed by the coordinator) to start the shutdown sequence
        shutdownHandler.schedule(this::shutdown, gracePeriod.toMillis(), MILLISECONDS);
    }

    private void shutdown()
    {
        waitActiveTasksToFinish();

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

    private void drain()
    {
        if (nodeState.get() == DRAINING) {
            waitActiveTasksToFinish();
        }
        drainingComplete();
    }

    private void drainingComplete()
    {
        boolean success = nodeState.compareAndSet(DRAINING, DRAINED);
        if (success) {
            log.info("Worker State change: DRAINING -> DRAINED, server can be safely SHUT DOWN.");
        }
        else {
            log.info("Worker State change: " + nodeState.get() + ", will not transition to DRAINED");
        }
    }

    private void waitActiveTasksToFinish()
    {
        // At this point no new tasks should be scheduled by coordinator on this worker node.
        // Wait for all remaining tasks to finish.
        while (isShuttingDownOrDraining()) {
            List<TaskInfo> activeTasks = getActiveTasks();
            log.info("Waiting for " + activeTasks.size() + " active tasks to finish");
            if (activeTasks.isEmpty()) {
                break;
            }

            waitTasksToFinish(activeTasks);
        }

        // wait for another grace period for all task states to be observed by the coordinator
        if (isShuttingDownOrDraining()) {
            sleepUninterruptibly(gracePeriod.toMillis(), MILLISECONDS);
        }
    }

    private void waitTasksToFinish(List<TaskInfo> activeTasks)
    {
        final CountDownLatch countDownLatch = new CountDownLatch(activeTasks.size());

        for (TaskInfo taskInfo : activeTasks) {
            sqlTaskManager.addStateChangeListener(taskInfo.taskStatus().getTaskId(), newState -> {
                if (newState.isDone()) {
                    countDownLatch.countDown();
                }
            });
        }

        try {
            while (!countDownLatch.await(1, TimeUnit.SECONDS)) {
                if (!isShuttingDownOrDraining()) {
                    log.info("Wait for tasks interrupted, worker is no longer draining.");

                    break;
                }
            }
        }
        catch (InterruptedException e) {
            log.warn("Interrupted while waiting for all tasks to finish");
            currentThread().interrupt();
        }
    }

    private boolean isShuttingDownOrDraining()
    {
        NodeState state = nodeState.get();
        return state == SHUTTING_DOWN || state == DRAINING;
    }

    private List<TaskInfo> getActiveTasks()
    {
        return sqlTaskManager.getAllTaskInfo()
                .stream()
                .filter(taskInfo -> !taskInfo.taskStatus().getState().isDone())
                .collect(toImmutableList());
    }
}
