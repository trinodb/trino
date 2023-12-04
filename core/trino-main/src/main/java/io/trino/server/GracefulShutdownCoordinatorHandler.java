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
import io.trino.execution.QueryManager;
import io.trino.server.ServerConfig.CoordinatorShutdownStrategy;
import io.trino.spi.QueryId;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.Threads.threadsNamed;
import static java.lang.Thread.currentThread;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class GracefulShutdownCoordinatorHandler
        implements GracefulShutdownHandler
{
    private static final Logger log = Logger.get(GracefulShutdownCoordinatorHandler.class);
    private static final Duration LIFECYCLE_STOP_TIMEOUT = new Duration(30, SECONDS);

    private final ExecutorService shutdownHandler = newSingleThreadExecutor(threadsNamed("shutdown-handler-%s"));
    private final ExecutorService lifeCycleStopper = newSingleThreadExecutor(threadsNamed("lifecycle-stopper-%s"));
    private final LifeCycleManager lifeCycleManager;
    private final ShutdownStatus status;
    private final QueryManager queryManager;
    private final ShutdownAction shutdownAction;
    private final Duration gracePeriod;
    private final CoordinatorShutdownStrategy strategy;

    @GuardedBy("this")
    private boolean shutdownRequested;

    @Inject
    public GracefulShutdownCoordinatorHandler(
            ShutdownStatus status,
            QueryManager queryManager,
            ServerConfig serverConfig,
            ShutdownAction shutdownAction,
            LifeCycleManager lifeCycleManager)
    {
        this.status = requireNonNull(status, "status is null");
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.shutdownAction = requireNonNull(shutdownAction, "shutdownAction is null");
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.gracePeriod = requireNonNull(serverConfig, "serverConfig is null").getGracePeriod();
        this.strategy = serverConfig.getShutdownStrategy();
    }

    @Override
    public synchronized void requestShutdown()
    {
        log.info("Coordinator shutdown requested");
        if (shutdownRequested) {
            return;
        }
        shutdownRequested = true;

        // mark coordinator as shutting down - new query submissions will be rejected
        status.shutdownStarted();
        log.info("Coordinator is now rejecting new query submissions");
        shutdownHandler.submit(this::shutdown);
    }

    private void shutdown()
    {
        List<QueryId> activeQueries = getActiveQueryIds();

        try {
            switch (strategy) {
                case ABORT_AFTER_WAITING -> {
                    if (!waitForQueriesCompletion(activeQueries, gracePeriod)) {
                        abortShutdown();
                        return;
                    }
                }
                case CANCEL_RUNNING -> cancelRunningQueries(activeQueries);
            }

            Thread.sleep(10_000); // wait 10s for the clients to receive final response, before http server is going down
        }
        catch (InterruptedException e) {
            log.warn("Interrupted while waiting for all queries to finish");
            currentThread().interrupt();
        }

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

    private boolean waitForQueriesCompletion(List<QueryId> activeQueries, Duration timeout)
            throws InterruptedException
    {
        log.info("Waiting for %d running queries to complete with timeout %s", activeQueries.size(), timeout);
        // At this point no new queries should be queued by the coordinator.
        // Wait for all remaining queries to finish with the specified timeout
        CountDownLatch countDownLatch = new CountDownLatch(activeQueries.size());

        for (QueryId queryId : activeQueries) {
            queryManager.addStateChangeListener(queryId, newState -> {
                if (newState.isDone()) {
                    countDownLatch.countDown();
                }
            });
        }

        return countDownLatch.await(timeout.toMillis(), MILLISECONDS);
    }

    private void cancelRunningQueries(List<QueryId> activeQueries)
            throws InterruptedException
    {
        log.info("Waiting for %d running queries to complete in %s", activeQueries.size(), gracePeriod);

        // First wait for queries to complete gracefully and then cancel remaining ones
        if (!waitForQueriesCompletion(activeQueries, gracePeriod)) {
            List<QueryId> stillActiveQueries = getActiveQueryIds();

            for (QueryId queryId : stillActiveQueries) {
                queryManager.cancelQuery(queryId);
            }

            log.info("Cancelling remaining %d queries", stillActiveQueries.size());

            if (!waitForQueriesCompletion(stillActiveQueries, gracePeriod)) {
                log.warn("Could not cancel running queries: %s", getActiveQueryIds());
            }
        }
    }

    private void abortShutdown()
    {
        log.info("%d queries still active after grace period %s has passed, aborting shutdown", getActiveQueryIds().size(), gracePeriod);
        status.shutdownAborted();
        shutdownRequested = false;
    }

    private List<QueryId> getActiveQueryIds()
    {
        return queryManager.getQueries()
                .stream()
                .filter(queryInfo -> !queryInfo.getState().isDone())
                .map(BasicQueryInfo::getQueryId)
                .collect(toImmutableList());
    }

    @Override
    public synchronized boolean isShutdownRequested()
    {
        return shutdownRequested;
    }
}
