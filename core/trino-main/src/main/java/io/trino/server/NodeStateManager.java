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

import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.execution.SqlTaskManager;
import io.trino.execution.TaskInfo;

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

    @GuardedBy("this")
    private boolean shutdownRequested;

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
        List<TaskInfo> activeTasks = getActiveTasks();

        // At this point no new tasks should be scheduled by coordinator on this worker node.
        // Wait for all remaining tasks to finish.
        while (activeTasks.size() > 0) {
            CountDownLatch countDownLatch = new CountDownLatch(activeTasks.size());

            for (TaskInfo taskInfo : activeTasks) {
                sqlTaskManager.addStateChangeListener(taskInfo.taskStatus().getTaskId(), newState -> {
                    if (newState.isDone()) {
                        countDownLatch.countDown();
                    }
                });
            }

            log.info("Waiting for all tasks to finish");

            try {
                countDownLatch.await();
            }
            catch (InterruptedException e) {
                log.warn("Interrupted while waiting for all tasks to finish");
                currentThread().interrupt();
            }

            activeTasks = getActiveTasks();
        }

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

    private List<TaskInfo> getActiveTasks()
    {
        return sqlTaskManager.getAllTaskInfo()
                .stream()
                .filter(taskInfo -> !taskInfo.taskStatus().getState().isDone())
                .collect(toImmutableList());
    }

    public synchronized boolean isShutdownRequested()
    {
        return shutdownRequested;
    }
}
