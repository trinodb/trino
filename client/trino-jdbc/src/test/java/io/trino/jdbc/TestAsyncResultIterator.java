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
package io.trino.jdbc;

import com.google.common.collect.ImmutableList;
import io.trino.client.ClientSelectedRole;
import io.trino.client.Column;
import io.trino.client.QueryData;
import io.trino.client.QueryError;
import io.trino.client.QueryStatusInfo;
import io.trino.client.ResultRows;
import io.trino.client.StageStats;
import io.trino.client.StatementClient;
import io.trino.client.StatementStats;
import io.trino.client.Warning;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.URI;
import java.time.ZoneId;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

class TestAsyncResultIterator
{
    @Test
    @Timeout(10)
    public void testIteratorCloseWhenQueueNotFull()
            throws Exception
    {
        AtomicReference<Thread> thread = new AtomicReference<>();
        CountDownLatch interruptedButSwallowedLatch = new CountDownLatch(1);

        AsyncResultIterator iterator = new AsyncResultIterator(
                new MockStatementClient(() -> {
                    thread.compareAndSet(null, Thread.currentThread());
                    try {
                        TimeUnit.MILLISECONDS.sleep(1000);
                    }
                    catch (InterruptedException e) {
                        interruptedButSwallowedLatch.countDown();
                    }
                    return fromList(ImmutableList.of(ImmutableList.of(new Object())));
                }), ignored -> {},
                new WarningsManager(),
                Optional.of(new ArrayBlockingQueue<>(100)));

        while (thread.get() == null || thread.get().getState() != Thread.State.TIMED_WAITING) {
            // wait for thread being waiting
        }
        iterator.close();
        while (!iterator.getFuture().isDone() || !iterator.isBackgroundThreadFinished()) {
            TimeUnit.MILLISECONDS.sleep(10);
        }
        boolean interruptedButSwallowed = interruptedButSwallowedLatch.await(5000, TimeUnit.MILLISECONDS);
        assertThat(interruptedButSwallowed).isTrue();
    }

    @Test
    @Timeout(10)
    public void testIteratorCloseWhenQueueIsFull()
            throws Exception
    {
        BlockingQueue<List<Object>> queue = new ArrayBlockingQueue<>(1);
        queue.put(ImmutableList.of());
        // queue is full at the beginning
        AtomicReference<Thread> thread = new AtomicReference<>();

        AsyncResultIterator iterator = new AsyncResultIterator(
                new MockStatementClient(() -> {
                    thread.compareAndSet(null, Thread.currentThread());
                    return fromList(ImmutableList.of(ImmutableList.of(new Object())));
                }), ignored -> {},
                new WarningsManager(),
                Optional.of(queue));

        while (thread.get() == null || thread.get().getState() != Thread.State.WAITING) {
            // wait for thread being waiting (for queue being not full)
            TimeUnit.MILLISECONDS.sleep(10);
        }
        iterator.close();
        while (!iterator.isBackgroundThreadFinished()) {
            TimeUnit.MILLISECONDS.sleep(10);
        }
    }

    private static class MockStatementClient
            implements StatementClient
    {
        private final Supplier<ResultRows> queryData;

        public MockStatementClient(Supplier<ResultRows> queryData)
        {
            this.queryData = requireNonNull(queryData, "queryData is null");
        }

        @Override
        public String getQuery()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ZoneId getTimeZone()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isRunning()
        {
            return true;
        }

        @Override
        public boolean isClientAborted()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isClientError()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isFinished()
        {
            return true;
        }

        @Override
        public StatementStats getStats()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public QueryStatusInfo currentStatusInfo()
        {
            return statusInfo("RUNNING");
        }

        @Override
        public QueryData currentData()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ResultRows currentRows()
        {
            return queryData.get();
        }

        @Override
        public QueryStatusInfo finalStatusInfo()
        {
            return statusInfo("FINISHED");
        }

        @Override
        public Optional<String> getSetCatalog()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<String> getSetSchema()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<List<String>> getSetPath()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<String> getSetAuthorizationUser()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isResetAuthorizationUser()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<ClientSelectedRole> getSetOriginalRoles()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, String> getSetSessionProperties()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<String> getResetSessionProperties()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, ClientSelectedRole> getSetRoles()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, String> getAddedPreparedStatements()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<String> getDeallocatedPreparedStatements()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getStartedTransactionId()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isClearTransactionId()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean advance()
        {
            return true;
        }

        @Override
        public void cancelLeafStage()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close()
        {
            // do nothing
        }
    }

    private static QueryStatusInfo statusInfo(String status)
    {
        return new QueryStatusInfo()
        {
            @Override
            public String getId()
            {
                return "";
            }

            @Override
            public URI getInfoUri()
            {
                return null;
            }

            @Override
            public URI getPartialCancelUri()
            {
                return null;
            }

            @Override
            public URI getNextUri()
            {
                return null;
            }

            @Override
            public List<Column> getColumns()
            {
                return ImmutableList.of();
            }

            @Override
            public StatementStats getStats()
            {
                return new StatementStats(
                        status,
                        false,
                        true,
                        OptionalDouble.of(50),
                        OptionalDouble.of(50),
                        1,
                        100,
                        50,
                        25,
                        50,
                        100,
                        100,
                        100,
                        100,
                        100,
                        100,
                        100,
                        100,
                        100,
                        100,
                        100,
                        100,
                        100,
                        100,
                        100,
                        StageStats.builder()
                                .setStageId("id")
                                .setDone(false)
                                .setState(status)
                                .setSubStages(ImmutableList.of())
                                .build());
            }

            @Override
            public QueryError getError()
            {
                return null;
            }

            @Override
            public List<Warning> getWarnings()
            {
                return ImmutableList.of();
            }

            @Override
            public String getUpdateType()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public OptionalLong getUpdateCount()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    static ResultRows fromList(List<List<Object>> values)
    {
        return new ResultRows() {
            @Override
            public void close() {}

            @Override
            public Iterator<List<Object>> iterator()
            {
                return values.iterator();
            }

            @Override
            public String toString()
            {
                return "ResultRows{values=" + values + "}";
            }
        };
    }
}
