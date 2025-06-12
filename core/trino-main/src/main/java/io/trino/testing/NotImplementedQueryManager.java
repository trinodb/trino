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
package io.trino.testing;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.Session;
import io.trino.execution.QueryExecution;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryManager;
import io.trino.execution.QueryState;
import io.trino.execution.StageId;
import io.trino.execution.StateMachine;
import io.trino.execution.TaskId;
import io.trino.server.BasicQueryInfo;
import io.trino.server.ResultQueryInfo;
import io.trino.server.protocol.Slug;
import io.trino.spi.QueryId;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

public class NotImplementedQueryManager
        implements QueryManager
{
    @Override
    public List<BasicQueryInfo> getQueries()
    {
        return ImmutableList.of();
    }

    @Override
    public void setOutputInfoListener(QueryId queryId, Consumer<QueryExecution.QueryOutputInfo> listener)
    {
        throw new NoSuchElementException(queryId.toString());
    }

    @Override
    public void outputTaskFailed(TaskId taskId, Throwable failure)
    {
        throw new NoSuchElementException(taskId.getQueryId().toString());
    }

    @Override
    public void resultsConsumed(QueryId queryId)
    {
        throw new NoSuchElementException(queryId.toString());
    }

    @Override
    public void addStateChangeListener(QueryId queryId, StateMachine.StateChangeListener<QueryState> listener)
    {
        throw new NoSuchElementException(queryId.toString());
    }

    @Override
    public ListenableFuture<QueryState> getStateChange(QueryId queryId, QueryState currentState)
    {
        throw new NoSuchElementException(queryId.toString());
    }

    @Override
    public BasicQueryInfo getQueryInfo(QueryId queryId)
    {
        throw new NoSuchElementException(queryId.toString());
    }

    @Override
    public QueryInfo getFullQueryInfo(QueryId queryId)
    {
        throw new NoSuchElementException(queryId.toString());
    }

    @Override
    public ResultQueryInfo getResultQueryInfo(QueryId queryId)
            throws NoSuchElementException
    {
        throw new NoSuchElementException(queryId.toString());
    }

    @Override
    public Session getQuerySession(QueryId queryId)
    {
        throw new NoSuchElementException(queryId.toString());
    }

    @Override
    public Slug getQuerySlug(QueryId queryId)
    {
        throw new NoSuchElementException(queryId.toString());
    }

    @Override
    public QueryState getQueryState(QueryId queryId)
    {
        throw new NoSuchElementException(queryId.toString());
    }

    @Override
    public boolean hasQuery(QueryId queryId)
    {
        return false;
    }

    @Override
    public void recordHeartbeat(QueryId queryId) {}

    @Override
    public void createQuery(QueryExecution execution)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void failQuery(QueryId queryId, Throwable cause) {}

    @Override
    public void cancelQuery(QueryId queryId) {}

    @Override
    public void cancelStage(StageId stageId) {}
}
