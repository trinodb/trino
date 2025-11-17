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
package io.trino.memory;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.execution.QueryExecution;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryState;
import io.trino.execution.StageId;
import io.trino.execution.StateMachine;
import io.trino.execution.TaskId;
import io.trino.server.BasicQueryInfo;
import io.trino.server.ResultQueryInfo;
import io.trino.server.protocol.Slug;
import io.trino.spi.QueryId;
import io.trino.sql.planner.Plan;
import it.unimi.dsi.fastutil.objects.Object2LongArrayMap;
import it.unimi.dsi.fastutil.objects.Object2LongMaps;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Optional;
import java.util.function.Consumer;

import static io.trino.execution.QueryState.FINISHED;
import static io.trino.execution.QueryState.RUNNING;
import static org.assertj.core.api.Assertions.assertThat;

public class TestClusterMemoryLeakDetector
{
    @Test
    public void testLeakDetector()
    {
        QueryId testQuery = new QueryId("test");
        ClusterMemoryLeakDetector leakDetector = new ClusterMemoryLeakDetector();

        leakDetector.checkForMemoryLeaks(_ -> Optional.empty(), new Object2LongArrayMap<>());
        assertThat(leakDetector.getNumberOfLeakedQueries()).isEqualTo(0);

        // the leak detector should report no leaked queries as the query is still running
        leakDetector.checkForMemoryLeaks(queryId -> Optional.of(createQueryExecution(queryId, testQuery, RUNNING)), Object2LongMaps.singleton(testQuery, 1L));
        assertThat(leakDetector.getNumberOfLeakedQueries()).isEqualTo(0);

        // the leak detector should report exactly one leaked query since the query is finished, and its end time is way in the past
        leakDetector.checkForMemoryLeaks(queryId -> Optional.of(createQueryExecution(queryId, testQuery, FINISHED)), Object2LongMaps.singleton(testQuery, 1L));
        assertThat(leakDetector.getNumberOfLeakedQueries()).isEqualTo(1);

        // the leak detector should report no leaked queries as the query doesn't have any memory reservation
        leakDetector.checkForMemoryLeaks(queryId -> Optional.of(createQueryExecution(queryId, testQuery, FINISHED)), Object2LongMaps.singleton(testQuery, 0L));
        assertThat(leakDetector.getNumberOfLeakedQueries()).isEqualTo(0);

        // the leak detector should report exactly one leaked query since the coordinator doesn't know of any query
        leakDetector.checkForMemoryLeaks(_ -> Optional.empty(), Object2LongMaps.singleton(testQuery, 1L));
        assertThat(leakDetector.getNumberOfLeakedQueries()).isEqualTo(1);
    }

    private static QueryExecution createQueryExecution(QueryId queryId, QueryId expectedQueryId, QueryState state)
    {
        assertThat(queryId).isEqualTo(expectedQueryId);

        // This ensures that only expected methods are ever called by the leak detector
        return new QueryExecution()
        {
            @Override
            public QueryId getQueryId()
            {
                return queryId;
            }

            @Override
            public boolean isDone()
            {
                return state.isDone();
            }

            @Override
            public QueryState getState()
            {
                return state;
            }

            @Override
            public Instant getCreateTime()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Optional<Instant> getEndTime()
            {
                return Optional.of(Instant.parse("2025-05-11T13:32:17.751968Z"));
            }

            @Override
            public Session getSession()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Optional<Instant> getExecutionStartTime()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Optional<Duration> getPlanningTime()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Instant getLastHeartbeat()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void fail(Throwable cause)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void pruneInfo()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isInfoPruned()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public ListenableFuture<QueryState> getStateChange(QueryState currentState)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void addStateChangeListener(StateMachine.StateChangeListener<QueryState> stateChangeListener)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void setOutputInfoListener(Consumer<QueryOutputInfo> listener)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void outputTaskFailed(TaskId taskId, Throwable failure)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void resultsConsumed()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Optional<Plan> getQueryPlan()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public BasicQueryInfo getBasicQueryInfo()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public QueryInfo getQueryInfo()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public ResultQueryInfo getResultQueryInfo()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Slug getSlug()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Duration getTotalCpuTime()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public DataSize getUserMemoryReservation()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public DataSize getTotalMemoryReservation()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void start()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void cancelQuery()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void cancelStage(StageId stageId)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void failTask(TaskId taskId, Exception reason)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void recordHeartbeat()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean shouldWaitForMinWorkers()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void addFinalQueryInfoListener(StateMachine.StateChangeListener<QueryInfo> stateChangeListener)
            {
                throw new UnsupportedOperationException();
            }
        };
    }
}
