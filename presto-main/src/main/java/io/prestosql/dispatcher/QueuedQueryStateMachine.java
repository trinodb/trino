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

import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.event.QueryMonitor;
import io.prestosql.execution.ExecutionFailureInfo;
import io.prestosql.execution.QueryState;
import io.prestosql.execution.QueryStateTimer;
import io.prestosql.execution.StateMachine;
import io.prestosql.execution.StateMachine.StateChangeListener;
import io.prestosql.server.BasicQueryInfo;
import io.prestosql.server.BasicQueryStats;
import io.prestosql.spi.ErrorCode;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import io.prestosql.transaction.TransactionManager;
import org.joda.time.DateTime;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.net.URI;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.prestosql.execution.QueryState.DISPATCHING;
import static io.prestosql.execution.QueryState.FAILED;
import static io.prestosql.execution.QueryState.QUEUED;
import static io.prestosql.execution.QueryState.WAITING_FOR_RESOURCES;
import static io.prestosql.memory.LocalMemoryManager.GENERAL_POOL;
import static io.prestosql.spi.StandardErrorCode.USER_CANCELED;
import static io.prestosql.util.Failures.toFailure;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@ThreadSafe
public class QueuedQueryStateMachine
{
    private static final Logger QUERY_STATE_LOG = Logger.get(QueuedQueryStateMachine.class);

    private final StateMachine<QueryStateHolder> queryState;
    private final QueryId queryId;
    private final String query;
    private final Session session;
    private final String slug;
    private final URI self;
    private final ResourceGroupId resourceGroup;
    private final QueryMonitor queryMonitor;
    private final TransactionManager transactionManager;

    private final QueryStateTimer queryStateTimer;

    private final AtomicReference<ExecutionFailureInfo> failureCause = new AtomicReference<>();

    private QueuedQueryStateMachine(
            Session session,
            String query,
            String slug,
            URI self,
            ResourceGroupId resourceGroup,
            QueryMonitor queryMonitor,
            TransactionManager transactionManager,
            Executor executor,
            Ticker ticker)
    {
        this.session = requireNonNull(session, "session is null");
        this.query = requireNonNull(query, "query is null");
        this.queryId = session.getQueryId();
        this.slug = requireNonNull(slug, "slug is null");
        this.self = requireNonNull(self, "self is null");
        this.resourceGroup = requireNonNull(resourceGroup, "resourceGroup is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.queryMonitor = queryMonitor;
        this.queryStateTimer = new QueryStateTimer(ticker);

        this.queryState = new StateMachine<>("query " + queryId, executor, new QueryStateHolder(QUEUED));
    }

    /**
     * Created QueryStateMachines must be transitioned to terminal states to clean up resources.
     */
    public static QueuedQueryStateMachine begin(
            Session session,
            String query,
            String slug,
            URI self,
            ResourceGroupId resourceGroup,
            QueryMonitor queryMonitor,
            TransactionManager transactionManager,
            Executor executor)
    {
        QueuedQueryStateMachine queryStateMachine = new QueuedQueryStateMachine(
                session,
                query,
                slug,
                self,
                resourceGroup,
                queryMonitor,
                transactionManager,
                executor,
                Ticker.systemTicker());

        queryStateMachine.addStateChangeListener(newState -> {
            QUERY_STATE_LOG.debug("Query %s is %s", session.getQueryId(), newState);
            if (newState.isDone()) {
                session.getTransactionId().ifPresent(transactionManager::trySetInactive);
            }
        });

        return queryStateMachine;
    }

    public QueryId getQueryId()
    {
        return queryId;
    }

    public String getQuery()
    {
        return query;
    }

    public Session getSession()
    {
        return session;
    }

    public String getSlug()
    {
        return slug;
    }

    public Optional<CoordinatorLocation> getCoordinatorLocation()
    {
        return queryState.get().getCoordinatorLocation();
    }

    public ResourceGroupId getResourceGroup()
    {
        return resourceGroup;
    }

    public QueryState getQueryState()
    {
        return queryState.get().getQueryState();
    }

    public boolean transitionToWaitingForResources()
    {
        queryStateTimer.beginWaitingForResources();

        // only change state if this query is not remotely managed and the new state is after this state
        return queryState.setIf(new QueryStateHolder(WAITING_FOR_RESOURCES),
                currentState -> !currentState.getBasicQueryInfo().isPresent() && currentState.getQueryState().ordinal() < WAITING_FOR_RESOURCES.ordinal());
    }

    public void transitionToDispatching(CoordinatorLocation coordinatorLocation)
    {
        queryStateTimer.beginDispatching();
        queryState.setIf(
                new QueryStateHolder(DISPATCHING, coordinatorLocation, localBasicQueryInfo(DISPATCHING)),
                currentState -> !currentState.getBasicQueryInfo().isPresent() && currentState.getQueryState().ordinal() < DISPATCHING.ordinal());
    }

    public boolean transitionToFailed(Throwable throwable)
    {
        queryStateTimer.endQuery();

        // NOTE: The failure cause must be set before triggering the state change, so
        // listeners can observe the exception. This is safe because the failure cause
        // can only be observed if the transition to FAILED is successful.
        requireNonNull(throwable, "throwable is null");
        failureCause.compareAndSet(null, toFailure(throwable));

        // only change state if this query is not remotely managed and the query isn't already done
        boolean failed = queryState.setIf(
                new QueryStateHolder(FAILED),
                currentState -> !currentState.getBasicQueryInfo().isPresent() && !currentState.getQueryState().isDone());
        if (failed) {
            QUERY_STATE_LOG.debug(throwable, "Query %s failed", queryId);
            // fail user transaction if present; this will never be an auto commit transaction
            localFailure();
        }
        else {
            QUERY_STATE_LOG.debug(throwable, "Failure after query %s finished", queryId);
        }

        return failed;
    }

    public boolean transitionToCanceled()
    {
        queryStateTimer.endQuery();

        // NOTE: The failure cause must be set before triggering the state change, so
        // listeners can observe the exception. This is safe because the failure cause
        // can only be observed if the transition to FAILED is successful.
        failureCause.compareAndSet(null, toFailure(new PrestoException(USER_CANCELED, "Query was canceled")));

        // only change state if this query is not remotely managed and the query isn't already done
        boolean failed = queryState.setIf(
                new QueryStateHolder(FAILED),
                currentState -> !currentState.getBasicQueryInfo().isPresent() && !currentState.getQueryState().isDone());
        if (failed) {
            localFailure();
        }
        return failed;
    }

    private void localFailure()
    {
        // fail user transaction if present; this will never be an auto commit transaction
        session.getTransactionId().ifPresent(transactionManager::fail);
        queryMonitor.queryImmediateFailureEvent(getBasicQueryInfo(), failureCause.get());
    }

    public void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener)
    {
        queryState.addStateChangeListener(new StateChangeListener<QueryStateHolder>()
        {
            @GuardedBy("this")
            private QueryState queryState;
            @Override
            public void stateChanged(QueryStateHolder queryStateHolder)
            {
                // only update on actual state change
                synchronized (this) {
                    if (queryState == queryStateHolder.getQueryState()) {
                        return;
                    }
                    queryState = queryStateHolder.getQueryState();
                }
                stateChangeListener.stateChanged(queryStateHolder.getQueryState());
            }
        });
    }

    public ListenableFuture<QueryState> getStateChange(QueryState currentState)
    {
        QueryStateHolder currentStateHolder = queryState.get();
        if (currentStateHolder.getQueryState() != currentState || currentState.isDone()) {
            return immediateFuture(currentStateHolder.getQueryState());
        }
        return Futures.transform(queryState.getStateChange(currentStateHolder), QueryStateHolder::getQueryState, directExecutor());
    }

    public void recordHeartbeat()
    {
        queryStateTimer.recordHeartbeat();
    }

    public DateTime getCreateTime()
    {
        return queryStateTimer.getCreateTime();
    }

    public DateTime getLastHeartbeat()
    {
        // if query is dispatched, return now
        if (queryState.get().getCoordinatorLocation().isPresent()) {
            return DateTime.now();
        }
        // not dispatched, so use local heartbeat
        return queryStateTimer.getLastHeartbeat();
    }

    public Duration getElapsedTime()
    {
        return queryState.get().getBasicQueryInfo()
                .map(info -> info.getQueryStats().getElapsedTime())
                .orElseGet(queryStateTimer::getElapsedTime);
    }

    public Duration getQueuedTime()
    {
        return queryStateTimer.getQueuedTime();
    }

    public Optional<DateTime> getEndTime()
    {
        Optional<DateTime> endTime = queryState.get().getBasicQueryInfo()
                .flatMap(info -> Optional.ofNullable(info.getQueryStats().getEndTime()));
        if (endTime.isPresent()) {
            return endTime;
        }
        return queryStateTimer.getEndTime();
    }

    public Optional<ExecutionFailureInfo> getFailureCause()
    {
        return Optional.ofNullable(failureCause.get());
    }

    public Optional<ErrorCode> getErrorCode()
    {
        return Optional.ofNullable(queryState.get().getBasicQueryInfo()
                .map(BasicQueryInfo::getErrorCode)
                .orElseGet(() -> {
                    ExecutionFailureInfo failureInfo = failureCause.get();
                    if (failureInfo == null) {
                        return null;
                    }
                    return failureInfo.getErrorCode();
                }));
    }

    public BasicQueryInfo getBasicQueryInfo()
    {
        QueryStateHolder queryStateHolder = this.queryState.get();
        if (queryStateHolder.getBasicQueryInfo().isPresent()) {
            return queryStateHolder.getBasicQueryInfo().get();
        }

        return localBasicQueryInfo(queryStateHolder.getQueryState());
    }

    public boolean isRemotelyManaged()
    {
        return queryState.get().getBasicQueryInfo().isPresent();
    }

    private BasicQueryInfo localBasicQueryInfo(QueryState queryState)
    {
        Optional<ErrorCode> errorCode = getFailureCause().map(ExecutionFailureInfo::getErrorCode);

        BasicQueryStats queryStats = new BasicQueryStats(
                queryStateTimer.getCreateTime(),
                getEndTime().orElse(null),
                queryStateTimer.getQueuedTime(),
                queryStateTimer.getElapsedTime(),
                queryStateTimer.getExecutionTime(),

                0,
                0,
                0,
                0,

                new DataSize(0, Unit.BYTE),
                0,

                0,
                new DataSize(0, Unit.BYTE),
                new DataSize(0, Unit.BYTE),
                new DataSize(0, Unit.BYTE),
                new DataSize(0, Unit.BYTE),

                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),

                false,
                ImmutableSet.of(),
                OptionalDouble.empty());

        return new BasicQueryInfo(
                queryId,
                session.toSessionRepresentation(),
                Optional.of(resourceGroup),
                queryState,
                GENERAL_POOL,
                false,
                self,
                query,
                queryStats,
                errorCode.map(ErrorCode::getType).orElse(null),
                errorCode.orElse(null));
    }

    public void updateRemoteQueryInfo(BasicQueryInfo remoteQueryInfo)
    {
        Optional<CoordinatorLocation> coordinatorLocation = queryState.get().getCoordinatorLocation();
        if (!coordinatorLocation.isPresent()) {
            // query is not remotely managed yet... this should not happen, but just skip the update for now
            return;
        }

        QueryStateHolder newState = new QueryStateHolder(remoteQueryInfo.getState(), coordinatorLocation.get(), remoteQueryInfo);

        // only change state if this query is not done and the new query info isn't before the current state
        queryState.setIf(
                newState,
                currentState -> !currentState.getQueryState().isDone() && currentState.getQueryState().ordinal() <= newState.getQueryState().ordinal());
    }

    private static class QueryStateHolder
    {
        private final QueryState queryState;
        private final Optional<CoordinatorLocation> coordinatorLocation;
        private final Optional<BasicQueryInfo> basicQueryInfo;

        public QueryStateHolder(QueryState queryState)
        {
            this.queryState = requireNonNull(queryState, "queryState is null");
            this.coordinatorLocation = Optional.empty();
            this.basicQueryInfo = Optional.empty();
        }

        public QueryStateHolder(QueryState queryState, CoordinatorLocation coordinatorLocation, BasicQueryInfo basicQueryInfo)
        {
            this.queryState = requireNonNull(queryState, "queryState is null");
            this.coordinatorLocation = Optional.of(requireNonNull(coordinatorLocation, "coordinatorLocation is null"));
            this.basicQueryInfo = Optional.of(requireNonNull(basicQueryInfo, "basicQueryInfo is null"));
        }

        public QueryState getQueryState()
        {
            return queryState;
        }

        public Optional<CoordinatorLocation> getCoordinatorLocation()
        {
            return coordinatorLocation;
        }

        public Optional<BasicQueryInfo> getBasicQueryInfo()
        {
            return basicQueryInfo;
        }
    }
}
