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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.client.NodeVersion;
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.operator.RetryPolicy;
import io.trino.server.BasicQueryInfo;
import io.trino.server.BasicQueryStats;
import io.trino.spi.ErrorCode;
import io.trino.spi.QueryId;
import org.joda.time.DateTime;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.SystemSessionProperties.QUERY_PRIORITY;
import static io.trino.execution.QueryState.FAILED;
import static io.trino.execution.QueryState.FINISHED;
import static io.trino.execution.QueryState.QUEUED;
import static io.trino.execution.QueryState.RUNNING;
import static io.trino.server.DynamicFilterService.DynamicFiltersStats;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class MockManagedQueryExecution
        implements ManagedQueryExecution
{
    private final List<StateChangeListener<QueryState>> listeners = new ArrayList<>();
    private final Session session;

    private DataSize memoryUsage;
    private Duration cpuUsage;
    private QueryState state = QUEUED;
    private Throwable failureCause;

    private MockManagedQueryExecution(String queryId, int priority, DataSize memoryUsage, Duration cpuUsage)
    {
        requireNonNull(queryId, "queryId is null");
        this.session = testSessionBuilder()
                .setQueryId(QueryId.valueOf(queryId))
                .setSystemProperty(QUERY_PRIORITY, String.valueOf(priority))
                .build();

        this.memoryUsage = requireNonNull(memoryUsage, "memoryUsage is null");
        this.cpuUsage = requireNonNull(cpuUsage, "cpuUsage is null");
    }

    public void consumeCpuTimeMillis(long cpuTimeDeltaMillis)
    {
        checkState(state == RUNNING, "cannot consume CPU in a non-running state");
        long newCpuTime = cpuUsage.toMillis() + cpuTimeDeltaMillis;
        this.cpuUsage = new Duration(newCpuTime, MILLISECONDS);
    }

    public void setMemoryUsage(DataSize memoryUsage)
    {
        checkState(state == RUNNING, "cannot set memory usage in a non-running state");
        this.memoryUsage = memoryUsage;
    }

    public void complete()
    {
        memoryUsage = DataSize.ofBytes(0);
        state = FINISHED;
        fireStateChange();
    }

    public Throwable getThrowable()
    {
        return failureCause;
    }

    @Override
    public Session getSession()
    {
        return session;
    }

    @Override
    public Optional<ErrorCode> getErrorCode()
    {
        return Optional.empty();
    }

    @Override
    public BasicQueryInfo getBasicQueryInfo()
    {
        return new BasicQueryInfo(
                new QueryId("test"),
                session.toSessionRepresentation(),
                Optional.empty(),
                state,
                !state.isDone(),
                URI.create("http://test"),
                "SELECT 1",
                Optional.empty(),
                Optional.empty(),
                new BasicQueryStats(
                        new DateTime(1),
                        new DateTime(2),
                        new Duration(3, NANOSECONDS),
                        new Duration(4, NANOSECONDS),
                        new Duration(5, NANOSECONDS),
                        99,
                        6,
                        7,
                        8,
                        9,
                        DataSize.ofBytes(14),
                        15,
                        DataSize.ofBytes(13),
                        16.0,
                        17.0,
                        memoryUsage,
                        memoryUsage,
                        DataSize.ofBytes(19),
                        DataSize.ofBytes(20),
                        cpuUsage,
                        new Duration(21, NANOSECONDS),
                        new Duration(22, NANOSECONDS),
                        new Duration(23, NANOSECONDS),
                        false,
                        ImmutableSet.of(),
                        OptionalDouble.empty(),
                        OptionalDouble.empty()),
                null,
                null,
                Optional.empty(),
                RetryPolicy.NONE);
    }

    @Override
    public QueryInfo getFullQueryInfo()
    {
        return new QueryInfo(
                new QueryId("test"),
                session.toSessionRepresentation(),
                state,
                URI.create("http://test"),
                ImmutableList.of(),
                "SELECT 1",
                Optional.empty(),
                new QueryStats(
                        new DateTime(1),
                        new DateTime(2),
                        new DateTime(3),
                        new DateTime(4),
                        new Duration(6, NANOSECONDS),
                        new Duration(5, NANOSECONDS),
                        new Duration(31, NANOSECONDS),
                        new Duration(41, NANOSECONDS),
                        new Duration(7, NANOSECONDS),
                        new Duration(8, NANOSECONDS),

                        new Duration(100, NANOSECONDS),
                        new Duration(150, NANOSECONDS),
                        new Duration(200, NANOSECONDS),

                        9,
                        10,
                        11,
                        0,

                        12,
                        13,
                        15,
                        30,
                        16,

                        17.0,
                        0.0,
                        DataSize.ofBytes(18),
                        DataSize.ofBytes(19),
                        DataSize.ofBytes(20),
                        DataSize.ofBytes(21),
                        DataSize.ofBytes(22),
                        DataSize.ofBytes(23),
                        DataSize.ofBytes(24),
                        DataSize.ofBytes(25),
                        DataSize.ofBytes(26),

                        !state.isDone(),
                        state.isDone() ? OptionalDouble.empty() : OptionalDouble.of(8.88),
                        state.isDone() ? OptionalDouble.empty() : OptionalDouble.of(0),
                        new Duration(20, NANOSECONDS),
                        new Duration(21, NANOSECONDS),
                        new Duration(22, NANOSECONDS),
                        new Duration(0, NANOSECONDS),
                        new Duration(23, NANOSECONDS),
                        false,
                        ImmutableSet.of(),

                        DataSize.ofBytes(241),
                        DataSize.ofBytes(0),
                        251,
                        0,
                        new Duration(24, NANOSECONDS),
                        new Duration(0, NANOSECONDS),

                        DataSize.ofBytes(242),
                        DataSize.ofBytes(0),
                        252,
                        0,

                        DataSize.ofBytes(25),
                        DataSize.ofBytes(0),
                        26,
                        0,

                        DataSize.ofBytes(27),
                        DataSize.ofBytes(0),
                        28,
                        0,

                        new Duration(221, NANOSECONDS),
                        new Duration(222, NANOSECONDS),

                        DataSize.ofBytes(29),
                        DataSize.ofBytes(0),
                        30,
                        0,

                        new Duration(223, NANOSECONDS),
                        new Duration(224, NANOSECONDS),

                        DataSize.ofBytes(31),
                        DataSize.ofBytes(0),

                        ImmutableList.of(),
                        DynamicFiltersStats.EMPTY,
                        ImmutableList.of(),
                        ImmutableList.of()),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                false,
                ImmutableMap.of(),
                ImmutableSet.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                Optional.empty(),
                false,
                "",
                Optional.empty(),
                null,
                null,
                ImmutableList.of(),
                ImmutableSet.of(),
                Optional.empty(),
                ImmutableList.of(),
                ImmutableList.of(),
                state.isDone(),
                Optional.empty(),
                Optional.empty(),
                RetryPolicy.NONE,
                false,
                new NodeVersion("test"));
    }

    @Override
    public DataSize getUserMemoryReservation()
    {
        return memoryUsage;
    }

    @Override
    public DataSize getTotalMemoryReservation()
    {
        return memoryUsage;
    }

    @Override
    public Duration getTotalCpuTime()
    {
        return cpuUsage;
    }

    @Override
    public QueryState getState()
    {
        return state;
    }

    @Override
    public void startWaitingForResources()
    {
        state = RUNNING;
        fireStateChange();
    }

    @Override
    public void fail(Throwable cause)
    {
        memoryUsage = DataSize.ofBytes(0);
        state = FAILED;
        failureCause = cause;
        fireStateChange();
    }

    @Override
    public boolean isDone()
    {
        return getState().isDone();
    }

    @Override
    public void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener)
    {
        listeners.add(stateChangeListener);
    }

    private void fireStateChange()
    {
        for (StateChangeListener<QueryState> listener : listeners) {
            listener.stateChanged(state);
        }
    }

    public static class MockManagedQueryExecutionBuilder
    {
        private DataSize memoryUsage = DataSize.ofBytes(0);
        private Duration cpuUsage = new Duration(0, MILLISECONDS);
        private int priority = 1;
        private String queryId = "query_id";

        public MockManagedQueryExecutionBuilder() {}

        public MockManagedQueryExecutionBuilder withInitialMemoryUsage(DataSize memoryUsage)
        {
            this.memoryUsage = memoryUsage;
            return this;
        }

        public MockManagedQueryExecutionBuilder withInitialCpuUsageMillis(long cpuUsageMillis)
        {
            this.cpuUsage = new Duration(cpuUsageMillis, MILLISECONDS);
            return this;
        }

        public MockManagedQueryExecutionBuilder withPriority(int priority)
        {
            this.priority = priority;
            return this;
        }

        public MockManagedQueryExecutionBuilder withQueryId(String queryId)
        {
            this.queryId = queryId;
            return this;
        }

        public MockManagedQueryExecution build()
        {
            return new MockManagedQueryExecution(queryId, priority, memoryUsage, cpuUsage);
        }
    }
}
