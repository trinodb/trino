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
package io.prestosql.dispatcher;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.dispatcher.QueryAnalysisResponse.Status;
import io.prestosql.execution.ExecutionFailureInfo;
import io.prestosql.execution.QueryState;
import io.prestosql.execution.StateMachine.StateChangeListener;
import io.prestosql.server.BasicQueryInfo;
import io.prestosql.server.BasicQueryStats;
import io.prestosql.spi.ErrorCode;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.QueryId;
import org.joda.time.DateTime;

import java.util.Optional;
import java.util.concurrent.Executor;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.addExceptionCallback;
import static io.airlift.concurrent.MoreFutures.addSuccessCallback;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.prestosql.execution.QueryState.DISPATCHING;
import static io.prestosql.execution.QueryState.FAILED;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.util.Failures.toFailure;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class RemoteDispatchQuery
        implements DispatchQuery
{
    private final QueuedQueryStateMachine stateMachine;

    private final QueryDispatcher queryDispatcher;
    private final Executor executor;

    private final SettableFuture<?> dispatched = SettableFuture.create();
    private final ListenableFuture<QueryAnalysisResponse> analyzeQueryFuture;

    public RemoteDispatchQuery(QueuedQueryStateMachine stateMachine, QueryDispatcher queryDispatcher, Executor executor)
    {
        this.stateMachine = requireNonNull(stateMachine, "stateMachine is null");
        this.queryDispatcher = requireNonNull(queryDispatcher, "queryDispatcher is null");
        this.executor = requireNonNull(executor, "executor is null");

        // start analysis immediately
        analyzeQueryFuture = queryDispatcher.analyzeQuery(stateMachine);
        addSuccessCallback(analyzeQueryFuture, queryAnalysisResponse -> {
            if (queryAnalysisResponse.getStatus() == Status.FAILED) {
                Exception exception = queryAnalysisResponse.getFailureInfo()
                        .map(ExecutionFailureInfo::toException)
                        .orElseGet(() -> new PrestoException(GENERIC_INTERNAL_ERROR, "Query analysis failed for an unknown reason"));
                stateMachine.transitionToFailed(exception);
            }
        });

        stateMachine.addStateChangeListener(state -> {
            if (state.isDone()) {
                dispatched.set(null);
                analyzeQueryFuture.cancel(true);
            }
        });
    }

    @Override
    public void updateRemoteQueryInfo(BasicQueryInfo remoteQueryInfo)
    {
        stateMachine.updateRemoteQueryInfo(remoteQueryInfo);
    }

    @Override
    public void startWaitingForResources()
    {
        // If there this is an auto-commit transaction, simply abort the analysis, since the
        // query will be analysis will be performed again after dispatch.  But, if this is an
        // existing transaction, the we must wait for the analysis to complete since the
        // transaction can only be active on one coordinator at a time.
        if (!stateMachine.getSession().getTransactionId().isPresent()) {
            analyzeQueryFuture.cancel(true);
        }
        analyzeQueryFuture.addListener(() -> {
            try {
                if (stateMachine.transitionToWaitingForResources()) {
                    ListenableFuture<?> locationFuture = queryDispatcher.dispatchQuery(stateMachine);
                    addSuccessCallback(locationFuture, () -> dispatched.set(null));
                    addExceptionCallback(locationFuture, stateMachine::transitionToFailed);
                }
            }
            catch (Exception e) {
                stateMachine.transitionToFailed(e);
            }
        }, executor);
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
    public ListenableFuture<?> getDispatchedFuture()
    {
        return queryDispatchFuture(stateMachine.getQueryState());
    }

    private ListenableFuture<?> queryDispatchFuture(QueryState currentState)
    {
        if (currentState.ordinal() >= DISPATCHING.ordinal()) {
            return immediateFuture(null);
        }
        return Futures.transformAsync(stateMachine.getStateChange(currentState), this::queryDispatchFuture, directExecutor());
    }

    @Override
    public DispatchInfo getDispatchInfo()
    {
        BasicQueryInfo queryInfo = stateMachine.getBasicQueryInfo();
        BasicQueryStats queryStats = queryInfo.getQueryStats();
        if (queryInfo.getState() == FAILED) {
            ExecutionFailureInfo failureInfo = stateMachine.getFailureCause()
                    .orElseGet(() -> toFailure(new PrestoException(GENERIC_INTERNAL_ERROR, "Query failed for an unknown reason")));
            return DispatchInfo.failed(failureInfo, queryStats.getElapsedTime(), queryStats.getQueuedTime());
        }

        return stateMachine.getCoordinatorLocation()
                .map(coordinatorLocation -> DispatchInfo.dispatched(coordinatorLocation, queryStats.getElapsedTime(), queryStats.getQueuedTime()))
                .orElseGet(() -> DispatchInfo.queued(queryStats.getElapsedTime(), queryStats.getQueuedTime()));
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
        return Optional.empty();
    }

    @Override
    public Optional<DateTime> getEndTime()
    {
        return stateMachine.getEndTime();
    }

    @Override
    public Duration getTotalCpuTime()
    {
        return new Duration(0, MILLISECONDS);
    }

    @Override
    public DataSize getTotalMemoryReservation()
    {
        return new DataSize(0, BYTE);
    }

    @Override
    public DataSize getUserMemoryReservation()
    {
        return new DataSize(0, BYTE);
    }

    @Override
    public BasicQueryInfo getBasicQueryInfo()
    {
        return stateMachine.getBasicQueryInfo();
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
    public void pruneInfo() {}

    @Override
    public Optional<ErrorCode> getErrorCode()
    {
        return stateMachine.getErrorCode();
    }

    @Override
    public void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener)
    {
        stateMachine.addStateChangeListener(stateChangeListener);
    }
}
