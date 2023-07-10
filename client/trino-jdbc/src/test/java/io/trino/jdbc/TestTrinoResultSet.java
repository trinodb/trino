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
import io.trino.client.QueryData;
import io.trino.client.QueryStatusInfo;
import io.trino.client.StatementClient;
import io.trino.client.StatementStats;
import org.testng.annotations.Test;

import java.time.ZoneId;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.testng.Assert.assertTrue;

/**
 * A unit test for {@link TrinoResultSet}.
 *
 * @see TestJdbcResultSet for an integration test.
 */
public class TestTrinoResultSet
{
    @Test(timeOut = 10000)
    public void testIteratorCancelWhenQueueNotFull()
            throws Exception
    {
        AtomicReference<Thread> thread = new AtomicReference<>();
        CountDownLatch interruptedButSwallowedLatch = new CountDownLatch(1);
        MockAsyncIterator<Iterable<List<Object>>> iterator = new MockAsyncIterator<>(
                new Iterator<Iterable<List<Object>>>()
                {
                    @Override
                    public boolean hasNext()
                    {
                        return true;
                    }

                    @Override
                    public Iterable<List<Object>> next()
                    {
                        thread.compareAndSet(null, Thread.currentThread());
                        try {
                            TimeUnit.MILLISECONDS.sleep(1000);
                        }
                        catch (InterruptedException e) {
                            interruptedButSwallowedLatch.countDown();
                        }
                        return ImmutableList.of((ImmutableList.of(new Object())));
                    }
                },
                new ArrayBlockingQueue<>(100));

        while (thread.get() == null || thread.get().getState() != Thread.State.TIMED_WAITING) {
            // wait for thread being waiting
        }
        iterator.cancel();
        while (!iterator.getFuture().isDone() || !iterator.isBackgroundThreadFinished()) {
            TimeUnit.MILLISECONDS.sleep(10);
        }
        boolean interruptedButSwallowed = interruptedButSwallowedLatch.await(5000, TimeUnit.MILLISECONDS);
        assertTrue(interruptedButSwallowed);
    }

    @Test(timeOut = 10000)
    public void testIteratorCancelWhenQueueIsFull()
            throws Exception
    {
        BlockingQueue<Iterable<List<Object>>> queue = new ArrayBlockingQueue<>(1);
        queue.put(ImmutableList.of());
        // queue is full at the beginning
        AtomicReference<Thread> thread = new AtomicReference<>();
        MockAsyncIterator<Iterable<List<Object>>> iterator = new MockAsyncIterator<>(
                new Iterator<Iterable<List<Object>>>()
                {
                    @Override
                    public boolean hasNext()
                    {
                        return true;
                    }

                    @Override
                    public Iterable<List<Object>> next()
                    {
                        thread.compareAndSet(null, Thread.currentThread());
                        return ImmutableList.of((ImmutableList.of(new Object())));
                    }
                },
                queue);

        while (thread.get() == null || thread.get().getState() != Thread.State.WAITING) {
            // wait for thread being waiting (for queue being not full)
            TimeUnit.MILLISECONDS.sleep(10);
        }
        iterator.cancel();
        while (!iterator.isBackgroundThreadFinished()) {
            TimeUnit.MILLISECONDS.sleep(10);
        }
    }

    private static class MockAsyncIterator<T>
            extends TrinoResultSet.AsyncIterator<T>
    {
        public MockAsyncIterator(Iterator<T> dataIterator, BlockingQueue<T> queue)
        {
            super(
                    dataIterator,
                    new StatementClient()
                    {
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
                            throw new UnsupportedOperationException();
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
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public StatementStats getStats()
                        {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public QueryStatusInfo currentStatusInfo()
                        {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public QueryData currentData()
                        {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public QueryStatusInfo finalStatusInfo()
                        {
                            throw new UnsupportedOperationException();
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
                        public Optional<String> getSetPath()
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
                            throw new UnsupportedOperationException();
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
                    },
                    Optional.of(queue));
        }
    }
}
