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
package io.prestosql.execution;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.memory.VersionedMemoryPoolId;
import io.prestosql.server.BasicQueryInfo;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.QueryId;
import io.prestosql.sql.planner.Plan;
import org.joda.time.DateTime;

import java.util.Optional;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.prestosql.execution.QueryState.FAILED;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class FailedQueryExecution
        implements QueryExecution
{
    private final QueryStateMachine stateMachine;
    private final String slug;

    public FailedQueryExecution(QueryStateMachine stateMachine, String slug)
    {
        checkArgument(stateMachine.getFailureInfo().isPresent(), "Query is not in failed state");
        this.stateMachine = requireNonNull(stateMachine, "stateMachine is null");
        this.slug = slug;
    }

    @Override
    public QueryState getState()
    {
        return FAILED;
    }

    @Override
    public ListenableFuture<QueryState> getStateChange(QueryState currentState)
    {
        return stateMachine.getStateChange(currentState);
    }

    @Override
    public void addStateChangeListener(StateMachine.StateChangeListener<QueryState> stateChangeListener)
    {
        stateMachine.addStateChangeListener(stateChangeListener);
    }

    @Override
    public void addOutputInfoListener(Consumer<QueryOutputInfo> listener)
    {
        stateMachine.addOutputInfoListener(listener);
    }

    @Override
    public Plan getQueryPlan()
    {
        return null;
    }

    @Override
    public BasicQueryInfo getBasicQueryInfo()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryInfo getQueryInfo()
    {
        return stateMachine.getQueryInfo(Optional.empty());
    }

    @Override
    public String getSlug()
    {
        return slug;
    }

    @Override
    public Duration getTotalCpuTime()
    {
        return new Duration(0, SECONDS);
    }

    @Override
    public DataSize getUserMemoryReservation()
    {
        return new DataSize(0, BYTE);
    }

    @Override
    public DataSize getTotalMemoryReservation()
    {
        return new DataSize(0, BYTE);
    }

    @Override
    public VersionedMemoryPoolId getMemoryPool()
    {
        return stateMachine.getMemoryPool();
    }

    @Override
    public void setMemoryPool(VersionedMemoryPoolId poolId)
    {
        stateMachine.setMemoryPool(poolId);
    }

    @Override
    public void start()
    {
    }

    @Override
    public void cancelQuery()
    {
    }

    @Override
    public void cancelStage(StageId stageId)
    {
    }

    @Override
    public void recordHeartbeat()
    {
        stateMachine.recordHeartbeat();
    }

    @Override
    public void addFinalQueryInfoListener(StateMachine.StateChangeListener<QueryInfo> stateChangeListener)
    {
        stateMachine.addQueryInfoStateChangeListener(stateChangeListener);
    }

    @Override
    public QueryId getQueryId()
    {
        return stateMachine.getQueryId();
    }

    @Override
    public boolean isDone()
    {
        return true;
    }

    @Override
    public Session getSession()
    {
        return stateMachine.getSession();
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
    public DateTime getLastHeartbeat()
    {
        return stateMachine.getLastHeartbeat();
    }

    @Override
    public Optional<DateTime> getEndTime()
    {
        return Optional.empty();
    }

    @Override
    public void fail(Throwable cause)
    {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "Query is already failed");
    }

    @Override
    public void pruneInfo()
    {
        stateMachine.pruneQueryInfo();
    }
}
