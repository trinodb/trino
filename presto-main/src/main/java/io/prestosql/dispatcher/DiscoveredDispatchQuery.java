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

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.execution.QueryState;
import io.prestosql.execution.StateMachine;
import io.prestosql.execution.StateMachine.StateChangeListener;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.server.BasicQueryInfo;
import io.prestosql.server.BasicQueryStats;
import io.prestosql.spi.ErrorCode;
import io.prestosql.spi.QueryId;
import org.joda.time.DateTime;

import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.execution.QueryState.QUEUED;
import static io.prestosql.execution.QueryState.TERMINAL_QUERY_STATES;
import static java.util.Objects.requireNonNull;

public class DiscoveredDispatchQuery
        implements DispatchQuery
{
    private final AtomicReference<BasicQueryInfo> queryInfo;
    private final CoordinatorLocation coordinatorLocation;
    private final Session session;
    private final StateMachine<QueryState> queryState;

    public DiscoveredDispatchQuery(
            BasicQueryInfo queryInfo,
            CoordinatorLocation coordinatorLocation,
            SessionPropertyManager sessionPropertyManager,
            Executor executor)
    {
        this.queryInfo = new AtomicReference<>(requireNonNull(queryInfo, "queryInfo is null"));
        this.coordinatorLocation = requireNonNull(coordinatorLocation, "coordinatorLocation is null");
        this.session = queryInfo.getSession().toSession(requireNonNull(sessionPropertyManager, "sessionPropertyManager is null"));
        this.queryState = new StateMachine<>("query " + queryInfo.getQueryId(), executor, QUEUED, TERMINAL_QUERY_STATES);
    }

    @Override
    public synchronized void updateRemoteQueryInfo(BasicQueryInfo remoteQueryInfo)
    {
        QueryState newState = remoteQueryInfo.getState();
        while (true) {
            QueryState currentState = this.queryState.get();

            // if query is done don't update
            if (currentState.isDone()) {
                return;
            }

            // skip if new state is behind current state
            if (currentState.ordinal() > newState.ordinal()) {
                return;
            }

            // update info if state is the same as the current state or if we can transition to the new state
            if (currentState == newState || queryState.compareAndSet(currentState, newState)) {
                this.queryInfo.set(remoteQueryInfo);
                return;
            }
            // otherwise try update again
        }
    }

    @Override
    public BasicQueryInfo getBasicQueryInfo()
    {
        return queryInfo.get();
    }

    @Override
    public Session getSession()
    {
        return session;
    }

    @Override
    public ListenableFuture<?> getDispatchedFuture()
    {
        return immediateFuture(null);
    }

    @Override
    public DispatchInfo getDispatchInfo()
    {
        BasicQueryStats queryStats = queryInfo.get().getQueryStats();
        return DispatchInfo.dispatched(coordinatorLocation, queryStats.getElapsedTime(), queryStats.getQueuedTime());
    }

    @Override
    public void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener)
    {
        queryState.addStateChangeListener(stateChangeListener);
    }

    @Override
    public void startWaitingForResources() {}

    @Override
    public void fail(Throwable throwable) {}

    @Override
    public void cancel()
    {
        // todo this should fire a remote cancel
    }

    @Override
    public void pruneInfo() {}

    @Override
    public QueryId getQueryId()
    {
        return queryInfo.get().getQueryId();
    }

    @Override
    public boolean isDone()
    {
        return queryInfo.get().getState().isDone();
    }

    @Override
    public Optional<ErrorCode> getErrorCode()
    {
        return Optional.ofNullable(queryInfo.get().getErrorCode());
    }

    @Override
    public void recordHeartbeat() {}

    @Override
    public DateTime getLastHeartbeat()
    {
        // query is remote so just return now
        return DateTime.now();
    }

    @Override
    public DateTime getCreateTime()
    {
        return queryInfo.get().getQueryStats().getCreateTime();
    }

    @Override
    public Optional<DateTime> getExecutionStartTime()
    {
        // stat time is used for enforcing time limits, but query managed by a remote coordinator return now
        return Optional.of(DateTime.now());
    }

    @Override
    public Optional<DateTime> getEndTime()
    {
        return Optional.ofNullable(queryInfo.get().getQueryStats().getEndTime());
    }

    @Override
    public Duration getTotalCpuTime()
    {
        return queryInfo.get().getQueryStats().getTotalCpuTime();
    }

    @Override
    public DataSize getTotalMemoryReservation()
    {
        return queryInfo.get().getQueryStats().getTotalMemoryReservation();
    }

    @Override
    public DataSize getUserMemoryReservation()
    {
        return queryInfo.get().getQueryStats().getUserMemoryReservation();
    }
}
