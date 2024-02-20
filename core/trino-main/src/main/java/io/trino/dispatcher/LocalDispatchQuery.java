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
package io.trino.dispatcher;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.event.QueryMonitor;
import io.trino.execution.ClusterSizeMonitor;
import io.trino.execution.ExecutionFailureInfo;
import io.trino.execution.QueryExecution;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryState;
import io.trino.execution.QueryStateMachine;
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.server.BasicQueryInfo;
import io.trino.spi.ErrorCode;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import org.joda.time.DateTime;

import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static io.airlift.concurrent.MoreFutures.addExceptionCallback;
import static io.airlift.concurrent.MoreFutures.addSuccessCallback;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static io.trino.SystemSessionProperties.getRequiredWorkers;
import static io.trino.SystemSessionProperties.getRequiredWorkersMaxWait;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.util.Failures.toFailure;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class LocalDispatchQuery
        implements DispatchQuery
{
    private static final Logger log = Logger.get(LocalDispatchQuery.class);
    private final QueryStateMachine stateMachine;
    private final ListenableFuture<QueryExecution> queryExecutionFuture;

    private final QueryMonitor queryMonitor;
    private final ClusterSizeMonitor clusterSizeMonitor;

    private final Executor queryExecutor;

    private final Consumer<QueryExecution> querySubmitter;
    private final SettableFuture<Void> submitted = SettableFuture.create();

    private final AtomicBoolean notificationSentOrGuaranteed = new AtomicBoolean();

    public LocalDispatchQuery(
            QueryStateMachine stateMachine,
            ListenableFuture<QueryExecution> queryExecutionFuture,
            QueryMonitor queryMonitor,
            ClusterSizeMonitor clusterSizeMonitor,
            Executor queryExecutor,
            Consumer<QueryExecution> querySubmitter)
    {
        this.stateMachine = requireNonNull(stateMachine, "stateMachine is null");
        this.queryExecutionFuture = requireNonNull(queryExecutionFuture, "queryExecutionFuture is null");
        this.queryMonitor = requireNonNull(queryMonitor, "queryMonitor is null");
        this.clusterSizeMonitor = requireNonNull(clusterSizeMonitor, "clusterSizeMonitor is null");
        this.queryExecutor = requireNonNull(queryExecutor, "queryExecutor is null");
        this.querySubmitter = requireNonNull(querySubmitter, "querySubmitter is null");

        addExceptionCallback(queryExecutionFuture, throwable -> {
            // When we are at the terminal state transitionToFailed()
            // doesn't make sense and only pollutes log file.
            // This will be fired when queryExecutionFuture.cancel(true)
            // is called which happens when we reached terminal query state.
            if (!stateMachine.isDone()) {
                stateMachine.transitionToFailed(throwable);
            }
        });

        stateMachine.addStateChangeListener(state -> {
            if (state == QueryState.FAILED) {
                if (notificationSentOrGuaranteed.compareAndSet(false, true)) {
                    queryMonitor.queryImmediateFailureEvent(getBasicQueryInfo(), getFullQueryInfo().getFailureInfo());
                }
            }
            // any PLANNING or later state means the query has been submitted for execution
            if (state.ordinal() >= QueryState.PLANNING.ordinal()) {
                submitted.set(null);
            }
            if (state.isDone()) {
                queryExecutionFuture.cancel(true);
            }
        });
    }

    @Override
    public void startWaitingForResources()
    {
        if (stateMachine.transitionToWaitingForResources()) {
            waitForMinimumWorkers();
        }
    }

    private void waitForMinimumWorkers()
    {
        // wait for query execution to finish construction
        addSuccessCallback(queryExecutionFuture, queryExecution -> {
            Session session = stateMachine.getSession();
            int executionMinCount = 1; // always wait for 1 node to be up
            if (queryExecution.shouldWaitForMinWorkers()) {
                executionMinCount = getRequiredWorkers(session);
            }
            ListenableFuture<Void> minimumWorkerFuture = clusterSizeMonitor.waitForMinimumWorkers(executionMinCount, getRequiredWorkersMaxWait(session));
            // when worker requirement is met, start the execution
            addSuccessCallback(minimumWorkerFuture, () -> startExecution(queryExecution), queryExecutor);
            addExceptionCallback(minimumWorkerFuture, throwable -> stateMachine.transitionToFailed(throwable), queryExecutor);

            // cancel minimumWorkerFuture if query fails for some reason or is cancelled by user
            stateMachine.addStateChangeListener(state -> {
                if (state.isDone()) {
                    minimumWorkerFuture.cancel(true);
                }
            });
        });
    }

    private void startExecution(QueryExecution queryExecution)
    {
        if (stateMachine.transitionToDispatching()) {
            try {
                querySubmitter.accept(queryExecution);
                if (notificationSentOrGuaranteed.compareAndSet(false, true)) {
                    queryExecution.addFinalQueryInfoListener(queryMonitor::queryCompletedEvent);
                }
            }
            catch (Throwable t) {
                // this should never happen but be safe
                stateMachine.transitionToFailed(t);
                log.error(t, "query submitter threw exception");
                throw t;
            }
            finally {
                submitted.set(null);
            }
        }
    }

    @Override
    public void recordHeartbeat()
    {
        stateMachine.recordHeartbeat();
    }

    @Override
    public DateTime getLastHeartbeat()
    {
        return stateMachine.getLastHeartbeat();
    }

    @Override
    public ListenableFuture<Void> getDispatchedFuture()
    {
        return nonCancellationPropagating(submitted);
    }

    @Override
    public DispatchInfo getDispatchInfo()
    {
        // observe submitted before getting the state, to ensure a failed query stat is visible
        boolean dispatched = submitted.isDone();
        BasicQueryInfo queryInfo = stateMachine.getBasicQueryInfo(Optional.empty());

        if (queryInfo.getState() == QueryState.FAILED) {
            ExecutionFailureInfo failureInfo = stateMachine.getFailureInfo()
                    .orElseGet(() -> toFailure(new TrinoException(GENERIC_INTERNAL_ERROR, "Query failed for an unknown reason")));
            return DispatchInfo.failed(failureInfo, queryInfo.getQueryStats().getElapsedTime(), queryInfo.getQueryStats().getQueuedTime());
        }
        if (dispatched) {
            return DispatchInfo.dispatched(new LocalCoordinatorLocation(), queryInfo.getQueryStats().getElapsedTime(), queryInfo.getQueryStats().getQueuedTime());
        }
        return DispatchInfo.queued(queryInfo.getQueryStats().getElapsedTime(), queryInfo.getQueryStats().getQueuedTime());
    }

    @Override
    public QueryId getQueryId()
    {
        return stateMachine.getQueryId();
    }

    @Override
    public boolean isDone()
    {
        return stateMachine.getQueryState().isDone();
    }

    @Override
    public DateTime getCreateTime()
    {
        return stateMachine.getCreateTime();
    }

    @Override
    public Optional<DateTime> getExecutionStartTime()
    {
        return stateMachine.getExecutionStartTime();
    }

    @Override
    public Optional<Duration> getPlanningTime()
    {
        return stateMachine.getPlanningTime();
    }

    @Override
    public Optional<DateTime> getEndTime()
    {
        return stateMachine.getEndTime();
    }

    @Override
    public Duration getTotalCpuTime()
    {
        return tryGetQueryExecution()
                .map(QueryExecution::getTotalCpuTime)
                .orElseGet(() -> new Duration(0, MILLISECONDS));
    }

    @Override
    public DataSize getTotalMemoryReservation()
    {
        return tryGetQueryExecution()
                .map(QueryExecution::getTotalMemoryReservation)
                .orElseGet(() -> DataSize.ofBytes(0));
    }

    @Override
    public DataSize getUserMemoryReservation()
    {
        return tryGetQueryExecution()
                .map(QueryExecution::getUserMemoryReservation)
                .orElseGet(() -> DataSize.ofBytes(0));
    }

    @Override
    public BasicQueryInfo getBasicQueryInfo()
    {
        return tryGetQueryExecution()
                .map(QueryExecution::getBasicQueryInfo)
                .orElseGet(() -> stateMachine.getBasicQueryInfo(Optional.empty()));
    }

    @Override
    public QueryInfo getFullQueryInfo()
    {
        return tryGetQueryExecution()
                .map(QueryExecution::getQueryInfo)
                .orElseGet(() -> stateMachine.updateQueryInfo(Optional.empty()));
    }

    @Override
    public QueryState getState()
    {
        return stateMachine.getQueryState();
    }

    @Override
    public Session getSession()
    {
        return stateMachine.getSession();
    }

    @Override
    public void fail(Throwable throwable)
    {
        stateMachine.transitionToFailed(throwable);
    }

    @Override
    public void cancel()
    {
        stateMachine.transitionToCanceled();
    }

    @Override
    public void pruneInfo()
    {
        stateMachine.pruneQueryInfo();
    }

    @Override
    public Optional<ErrorCode> getErrorCode()
    {
        return stateMachine.getFailureInfo().map(ExecutionFailureInfo::getErrorCode);
    }

    @Override
    public void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener)
    {
        stateMachine.addStateChangeListener(stateChangeListener);
    }

    private Optional<QueryExecution> tryGetQueryExecution()
    {
        try {
            return tryGetFutureValue(queryExecutionFuture);
        }
        catch (Exception ignored) {
            return Optional.empty();
        }
        catch (Error e) {
            log.error(e, "Unhandled Error stored in queryExecutionFuture");
            return Optional.empty();
        }
    }
}
