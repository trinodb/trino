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

import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.execution.SqlTaskManager;
import io.trino.execution.TaskInfo;
import io.trino.metadata.NodeState;

import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.trino.metadata.NodeState.ACTIVE;
import static io.trino.metadata.NodeState.DECOMMISSIONED;
import static io.trino.metadata.NodeState.DECOMMISSIONING;
import static io.trino.metadata.NodeState.SHUTTING_DOWN;
import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;

public class UpdateNodeStateHandler
{
    private static final Logger log = Logger.get(UpdateNodeStateHandler.class);
    private static final Duration LIFECYCLE_STOP_TIMEOUT = new Duration(30, SECONDS);

    private final ScheduledExecutorService shutdownHandler = newSingleThreadScheduledExecutor(threadsNamed("shutdown-handler-%s"));
    private final ExecutorService lifeCycleStopper = newSingleThreadExecutor(threadsNamed("lifecycle-stopper-%s"));
    private final LifeCycleManager lifeCycleManager;
    private final SqlTaskManager sqlTaskManager;
    private final boolean isCoordinator;
    private final ShutdownAction shutdownAction;
    private final Duration gracePeriod;
    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor(
            threadsNamed("decommission-handler-%s"));
    private NodeState currState = NodeState.ACTIVE;

    @GuardedBy("this")
    private boolean shutdownRequested;

    @Inject
    public UpdateNodeStateHandler(
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
        return currState;
    }

    public synchronized Response updateState(NodeState state)
            throws WebApplicationException
    {
        requireNonNull(state, "state is null");
        log.info(String.format("Entre updateState %s -> %s", currState, state));

        // Supported state transitions:
        //   1. ? -> ?
        //   2. * -> SHUTTING_DOWN
        //   3. ACTIVE -> DECOMMISSIONING
        //   4. DECOMMISSIONING, DECOMMISSIONED -> ACTIVE

        if (currState == state || (state == DECOMMISSIONING && currState == DECOMMISSIONED)) {
            return Response.ok().build();
        }

        // Prefer using a switch instead of a chained if-else for enums
        switch (state) {
            case SHUTTING_DOWN:
                requestShutdown();
                currState = SHUTTING_DOWN;
                return Response.ok().build();
            case DECOMMISSIONING:
                if (currState == ACTIVE) {
                    requestDecommission();
                    currState = DECOMMISSIONING;
                    return Response.ok().build();
                }
                break;
            case ACTIVE:
                if (currState == DECOMMISSIONING || currState == DECOMMISSIONED) {
                    currState = ACTIVE;
                    return Response.ok().build();
                }
                break;
            case INACTIVE:
                break;
            default:
                return Response.status(BAD_REQUEST).type(TEXT_PLAIN)
                        .entity(format("Invalid state %s", state))
                        .build();
        }

        // Bad request once here.
        throw new WebApplicationException(Response
                .status(BAD_REQUEST)
                .type(MediaType.TEXT_PLAIN)
                .entity(format("Invalid state transition from %s to %s", currState, state))
                .build());
    }

    public synchronized void requestShutdown()
    {
        log.info("Shutdown requested");

        if (isCoordinator) {
            throw new UnsupportedOperationException("Cannot shutdown coordinator");
        }

        if (shutdownRequested) {
            return;
        }
        shutdownRequested = true;

        // wait for a grace period (so that shutting down state is observed by the coordinator) to start the shutdown sequence
        shutdownHandler.schedule(this::shutdown, gracePeriod.toMillis(), MILLISECONDS);
    }

    private void shutdown()
    {
        waitActiveTasksToFinish(sqlTaskManager);

        // wait for another grace period for all task states to be observed by the coordinator
        sleepUninterruptibly(gracePeriod.toMillis(), MILLISECONDS);

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

    static void waitActiveTasksToFinish(SqlTaskManager sqlTaskManager)
    {
        // At this point no new tasks should be scheduled by coordinator on this worker node.
        // Wait for all remaining tasks to finish.
        while (true) {
            List<TaskInfo> activeTasks = getActiveTasks(sqlTaskManager);
            log.info("Waiting for " + activeTasks.size() + " active tasks to finish");
            if (activeTasks.isEmpty()) {
                break;
            }
            CountDownLatch countDownLatch = new CountDownLatch(activeTasks.size());

            for (TaskInfo taskInfo : activeTasks) {
                sqlTaskManager.addStateChangeListener(taskInfo.getTaskStatus().getTaskId(), newState -> {
                    if (newState.isDone()) {
                        countDownLatch.countDown();
                    }
                });
            }

            try {
                countDownLatch.await();
            }
            catch (InterruptedException e) {
                log.warn("Interrupted while waiting for all tasks to finish");
                currentThread().interrupt();
            }
        }
    }

    private static List<TaskInfo> getActiveTasks(SqlTaskManager sqlTaskManager)
    {
        return sqlTaskManager.getAllTaskInfo()
                .stream()
                .filter(taskInfo -> !taskInfo.getTaskStatus().getState().isDone())
                .collect(toImmutableList());
    }

    public synchronized void requestDecommission()
    {
        log.info("enter requestDecommission " + getServerState());
        if (isCoordinator) {
            throw new UnsupportedOperationException("Cannot decommission coordinator");
        }

        // The decommission is normally initiated by the coordinator.
        // Here we wait a short grace period of 10 seconds for coordinator to no longer
        // assign new tasks to this worker node, before wait active tasks to finish.
        executor.schedule(new Runnable() {
            @Override
            public void run()
            {
                waitActiveTasksToFinish(sqlTaskManager);
                log.info("complete waitActiveTasksToFinish " + getServerState());
                NodeState state = onDecommissioned();
                log.info("onDecommissioned " + state);
            }
        }, 10000, MILLISECONDS);
    }

    // callback used by decommissionHandler
    NodeState onDecommissioned()
    {
        log.info("onDecommissioned " + (currState == null ? "null" : currState));
        if (currState == NodeState.DECOMMISSIONING) {
            currState = NodeState.DECOMMISSIONED;
        }
        return currState;
    }
}
