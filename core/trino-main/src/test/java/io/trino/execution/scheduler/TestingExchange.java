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
import io.trino.spi.exchange.ExchangeSinkHandle;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.spi.exchange.ExchangeSourceSplitter;
import io.trino.spi.exchange.ExchangeSourceStatistics;
import org.openjdk.jol.info.ClassLayout;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterators.cycle;
import static com.google.common.collect.Iterators.limit;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class TestingExchange
        implements Exchange
{
    private final boolean splitPartitionsEnabled;

    private final Set<TestingExchangeSinkHandle> finishedSinks = newConcurrentHashSet();
    private final Set<TestingExchangeSinkHandle> allSinks = newConcurrentHashSet();
    private final AtomicBoolean noMoreSinks = new AtomicBoolean();
    private final CompletableFuture<List<ExchangeSourceHandle>> sourceHandles = new CompletableFuture<>();

    public TestingExchange(boolean splitPartitionsEnabled)
    {
        this.splitPartitionsEnabled = splitPartitionsEnabled;
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
    public void sinkFinished(ExchangeSinkInstanceHandle handle)
    {
        finishedSinks.add(((TestingExchangeSinkInstanceHandle) handle).getSinkHandle());
    }

    public Set<TestingExchangeSinkHandle> getFinishedSinkHandles()
    {
        return ImmutableSet.copyOf(finishedSinks);
    }

    @Override
    public CompletableFuture<List<ExchangeSourceHandle>> getSourceHandles()
    {
        return sourceHandles;
    }

    public void setSourceHandles(List<ExchangeSourceHandle> handles)
    {
        sourceHandles.complete(ImmutableList.copyOf(handles));
    }

    @Override
    public ExchangeSourceSplitter split(ExchangeSourceHandle handle, long targetSizeInBytes)
    {
        List<ExchangeSourceHandle> splitHandles = splitIntoList(handle, targetSizeInBytes);
        Iterator<ExchangeSourceHandle> iterator = splitHandles.iterator();
        return new ExchangeSourceSplitter()
        {
            @Override
            public CompletableFuture<?> isBlocked()
            {
                return completedFuture(null);
            }

            @Override
            public Optional<ExchangeSourceHandle> getNext()
            {
                if (iterator.hasNext()) {
                    return Optional.of(iterator.next());
                }
                return Optional.empty();
            }

            @Override
            public void close()
            {
            }
        };
    }

    private List<ExchangeSourceHandle> splitIntoList(ExchangeSourceHandle handle, long targetSizeInBytes)
    {
        if (!splitPartitionsEnabled) {
            return ImmutableList.of(handle);
        }
        checkArgument(targetSizeInBytes > 0, "targetSizeInBytes must be positive: %s", targetSizeInBytes);
        TestingExchangeSourceHandle testingExchangeSourceHandle = (TestingExchangeSourceHandle) handle;
        long currentSize = testingExchangeSourceHandle.getSizeInBytes();
        int fullPartitions = toIntExact(currentSize / targetSizeInBytes);
        long remainder = currentSize % targetSizeInBytes;
        ImmutableList.Builder<ExchangeSourceHandle> result = ImmutableList.builder();
        if (fullPartitions > 0) {
            result.addAll(limit(cycle(new TestingExchangeSourceHandle(testingExchangeSourceHandle.getPartitionId(), targetSizeInBytes)), fullPartitions));
        }
        if (remainder > 0) {
            result.add(new TestingExchangeSourceHandle(testingExchangeSourceHandle.getPartitionId(), remainder));
        }
        return result.build();
    }

    @Override
    public ExchangeSourceStatistics getExchangeSourceStatistics(ExchangeSourceHandle handle)
    {
        return new ExchangeSourceStatistics(((TestingExchangeSourceHandle) handle).getSizeInBytes());
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
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(TestingExchangeSourceHandle.class).instanceSize();

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
