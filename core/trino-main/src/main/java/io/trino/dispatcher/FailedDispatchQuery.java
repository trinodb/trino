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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.execution.ExecutionFailureInfo;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryState;
import io.trino.execution.QueryStats;
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.server.BasicQueryInfo;
import io.trino.spi.ErrorCode;
import io.trino.spi.QueryId;
import io.trino.spi.resourcegroups.ResourceGroupId;
import org.joda.time.DateTime;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.Executor;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.memory.LocalMemoryManager.GENERAL_POOL;
import static io.trino.server.DynamicFilterService.DynamicFiltersStats;
import static io.trino.util.Failures.toFailure;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class FailedDispatchQuery
        implements DispatchQuery
{
    private final QueryInfo fullQueryInfo;
    private final BasicQueryInfo basicQueryInfo;
    private final Session session;
    private final Executor executor;
    private final DispatchInfo dispatchInfo;

    public FailedDispatchQuery(
            Session session,
            String query,
            Optional<String> preparedQuery,
            URI self,
            Optional<ResourceGroupId> resourceGroup,
            Throwable cause,
            Executor executor)
    {
        requireNonNull(session, "session is null");
        requireNonNull(query, "query is null");
        requireNonNull(self, "self is null");
        requireNonNull(resourceGroup, "resourceGroup is null");
        requireNonNull(cause, "cause is null");
        requireNonNull(executor, "executor is null");

        this.fullQueryInfo = immediateFailureQueryInfo(session, query, preparedQuery, self, resourceGroup, cause);
        this.basicQueryInfo = new BasicQueryInfo(fullQueryInfo);
        this.session = requireNonNull(session, "session is null");
        this.executor = requireNonNull(executor, "executor is null");

        this.dispatchInfo = DispatchInfo.failed(
                fullQueryInfo.getFailureInfo(),
                basicQueryInfo.getQueryStats().getElapsedTime(),
                basicQueryInfo.getQueryStats().getQueuedTime());
    }

    @Override
    public BasicQueryInfo getBasicQueryInfo()
    {
        return basicQueryInfo;
    }

    @Override
    public QueryInfo getFullQueryInfo()
    {
        return fullQueryInfo;
    }

    @Override
    public QueryState getState()
    {
        return fullQueryInfo.getState();
    }

    @Override
    public Session getSession()
    {
        return session;
    }

    @Override
    public ListenableFuture<Void> getDispatchedFuture()
    {
        return immediateVoidFuture();
    }

    @Override
    public DispatchInfo getDispatchInfo()
    {
        return dispatchInfo;
    }

    @Override
    public void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener)
    {
        executor.execute(() -> stateChangeListener.stateChanged(QueryState.FAILED));
    }

    @Override
    public void startWaitingForResources() {}

    @Override
    public void fail(Throwable throwable) {}

    @Override
    public void cancel() {}

    @Override
    public void pruneInfo() {}

    @Override
    public QueryId getQueryId()
    {
        return basicQueryInfo.getQueryId();
    }

    @Override
    public boolean isDone()
    {
        return true;
    }

    @Override
    public Optional<ErrorCode> getErrorCode()
    {
        return Optional.ofNullable(basicQueryInfo.getErrorCode());
    }

    @Override
    public void recordHeartbeat() {}

    @Override
    public DateTime getLastHeartbeat()
    {
        return basicQueryInfo.getQueryStats().getEndTime();
    }

    @Override
    public DateTime getCreateTime()
    {
        return basicQueryInfo.getQueryStats().getCreateTime();
    }

    @Override
    public Optional<DateTime> getExecutionStartTime()
    {
        return getEndTime();
    }

    @Override
    public Optional<Duration> getPlanningTime()
    {
        return Optional.empty();
    }

    @Override
    public Optional<DateTime> getEndTime()
    {
        return Optional.ofNullable(basicQueryInfo.getQueryStats().getEndTime());
    }

    @Override
    public Duration getTotalCpuTime()
    {
        return new Duration(0, MILLISECONDS);
    }

    @Override
    public DataSize getTotalMemoryReservation()
    {
        return DataSize.ofBytes(0);
    }

    @Override
    public DataSize getUserMemoryReservation()
    {
        return DataSize.ofBytes(0);
    }

    private static QueryInfo immediateFailureQueryInfo(
            Session session,
            String query,
            Optional<String> preparedQuery,
            URI self,
            Optional<ResourceGroupId> resourceGroupId,
            Throwable throwable)
    {
        ExecutionFailureInfo failureCause = toFailure(throwable);
        QueryInfo queryInfo = new QueryInfo(
                session.getQueryId(),
                session.toSessionRepresentation(),
                QueryState.FAILED,
                GENERAL_POOL,
                false,
                self,
                ImmutableList.of(),
                query,
                preparedQuery,
                immediateFailureQueryStats(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                Optional.empty(),
                false,
                null,
                Optional.empty(),
                failureCause,
                failureCause.getErrorCode(),
                ImmutableList.of(),
                ImmutableSet.of(),
                Optional.empty(),
                ImmutableList.of(),
                ImmutableList.of(),
                true,
                resourceGroupId,
                Optional.empty());

        return queryInfo;
    }

    private static QueryStats immediateFailureQueryStats()
    {
        DateTime now = DateTime.now();
        return new QueryStats(
                now,
                now,
                now,
                now,
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                false,
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                false,
                ImmutableSet.of(),
                DataSize.ofBytes(0),
                0,
                new Duration(0, MILLISECONDS),
                DataSize.ofBytes(0),
                0,
                DataSize.ofBytes(0),
                0,
                DataSize.ofBytes(0),
                0,
                DataSize.ofBytes(0),
                0,
                DataSize.ofBytes(0),
                ImmutableList.of(),
                DynamicFiltersStats.EMPTY,
                ImmutableList.of());
    }
}
