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
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setOutputInfoListener(QueryId queryId, Consumer<QueryExecution.QueryOutputInfo> listener)
            throws NoSuchElementException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void outputTaskFailed(TaskId taskId, Throwable failure)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void resultsConsumed(QueryId queryId)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void addStateChangeListener(QueryId queryId, StateMachine.StateChangeListener<QueryState> listener)
            throws NoSuchElementException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public ListenableFuture<QueryState> getStateChange(QueryId queryId, QueryState currentState)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public BasicQueryInfo getQueryInfo(QueryId queryId)
            throws NoSuchElementException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public QueryInfo getFullQueryInfo(QueryId queryId)
            throws NoSuchElementException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public ResultQueryInfo getResultQueryInfo(QueryId queryId)
            throws NoSuchElementException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public Session getQuerySession(QueryId queryId)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public Slug getQuerySlug(QueryId queryId)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public QueryState getQueryState(QueryId queryId)
            throws NoSuchElementException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public boolean hasQuery(QueryId queryId)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void recordHeartbeat(QueryId queryId)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void createQuery(QueryExecution execution)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void failQuery(QueryId queryId, Throwable cause)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void cancelQuery(QueryId queryId)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void cancelStage(StageId stageId)
    {
        throw new RuntimeException("not implemented");
    }
}
