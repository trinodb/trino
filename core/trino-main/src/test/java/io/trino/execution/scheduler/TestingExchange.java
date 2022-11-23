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
package io.trino.execution.scheduler;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.exchange.Exchange;
import io.trino.spi.exchange.ExchangeId;
import io.trino.spi.exchange.ExchangeSinkHandle;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.spi.exchange.ExchangeSourceHandleSource;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static io.trino.spi.exchange.ExchangeId.createRandomExchangeId;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class TestingExchange
        implements Exchange
{
    private final ExchangeId exchangeId = createRandomExchangeId();
    private final Set<TestingExchangeSinkHandle> finishedSinks = newConcurrentHashSet();
    private final Set<TestingExchangeSinkHandle> allSinks = newConcurrentHashSet();
    private final AtomicBoolean noMoreSinks = new AtomicBoolean();
    private final CompletableFuture<List<ExchangeSourceHandle>> sourceHandles = new CompletableFuture<>();
    private final AtomicBoolean allRequiredSinksFinished = new AtomicBoolean();

    @Override
    public ExchangeId getId()
    {
        return exchangeId;
    }

    @Override
    public ExchangeSinkHandle addSink(int taskPartitionId)
    {
        TestingExchangeSinkHandle sinkHandle = new TestingExchangeSinkHandle(taskPartitionId);
        allSinks.add(sinkHandle);
        return sinkHandle;
    }

    @Override
    public void noMoreSinks()
    {
        noMoreSinks.set(true);
    }

    public boolean isNoMoreSinks()
    {
        return noMoreSinks.get();
    }

    @Override
    public ExchangeSinkInstanceHandle instantiateSink(ExchangeSinkHandle sinkHandle, int taskAttemptId)
    {
        return new TestingExchangeSinkInstanceHandle((TestingExchangeSinkHandle) sinkHandle, taskAttemptId);
    }

    @Override
    public ExchangeSinkInstanceHandle updateSinkInstanceHandle(ExchangeSinkHandle sinkHandle, int taskAttemptId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void sinkFinished(ExchangeSinkHandle sinkHandle, int taskAttemptId)
    {
        finishedSinks.add((TestingExchangeSinkHandle) sinkHandle);
    }

    @Override
    public void allRequiredSinksFinished()
    {
        allRequiredSinksFinished.set(true);
    }

    public boolean isAllRequiredSinksFinished()
    {
        return allRequiredSinksFinished.get();
    }

    public Set<TestingExchangeSinkHandle> getFinishedSinkHandles()
    {
        return ImmutableSet.copyOf(finishedSinks);
    }

    @Override
    public ExchangeSourceHandleSource getSourceHandles()
    {
        return new ExchangeSourceHandleSource()
        {
            @Override
            public CompletableFuture<ExchangeSourceHandleBatch> getNextBatch()
            {
                return sourceHandles.thenApply(handles -> new ExchangeSourceHandleBatch(handles, true));
            }

            @Override
            public void close() {}
        };
    }

    public void setSourceHandles(List<ExchangeSourceHandle> handles)
    {
        sourceHandles.complete(ImmutableList.copyOf(handles));
    }

    @Override
    public void close()
    {
    }

    public static class TestingExchangeSinkInstanceHandle
            implements ExchangeSinkInstanceHandle
    {
        private final TestingExchangeSinkHandle sinkHandle;
        private final int attemptId;

        public TestingExchangeSinkInstanceHandle(TestingExchangeSinkHandle sinkHandle, int attemptId)
        {
            this.sinkHandle = requireNonNull(sinkHandle, "sinkHandle is null");
            this.attemptId = attemptId;
        }

        public TestingExchangeSinkHandle getSinkHandle()
        {
            return sinkHandle;
        }

        public int getAttemptId()
        {
            return attemptId;
        }
    }

    public static class TestingExchangeSourceHandle
            implements ExchangeSourceHandle
    {
        private static final int INSTANCE_SIZE = toIntExact(ClassLayout.parseClass(TestingExchangeSourceHandle.class).instanceSize());

        private final int partitionId;
        private final long sizeInBytes;

        public TestingExchangeSourceHandle(int partitionId, long sizeInBytes)
        {
            this.partitionId = partitionId;
            this.sizeInBytes = sizeInBytes;
        }

        @Override
        public int getPartitionId()
        {
            return partitionId;
        }

        @Override
        public long getDataSizeInBytes()
        {
            return sizeInBytes;
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE;
        }

        public long getSizeInBytes()
        {
            return sizeInBytes;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestingExchangeSourceHandle that = (TestingExchangeSourceHandle) o;
            return partitionId == that.partitionId && sizeInBytes == that.sizeInBytes;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(partitionId, sizeInBytes);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("partitionId", partitionId)
                    .add("sizeInBytes", sizeInBytes)
                    .toString();
        }
    }

    public static class TestingExchangeSinkHandle
            implements ExchangeSinkHandle
    {
        private final int taskPartitionId;

        public TestingExchangeSinkHandle(int taskPartitionId)
        {
            this.taskPartitionId = taskPartitionId;
        }

        public int getTaskPartitionId()
        {
            return taskPartitionId;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestingExchangeSinkHandle sinkHandle = (TestingExchangeSinkHandle) o;
            return taskPartitionId == sinkHandle.taskPartitionId;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(taskPartitionId);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("taskPartitionId", taskPartitionId)
                    .toString();
        }
    }
}
