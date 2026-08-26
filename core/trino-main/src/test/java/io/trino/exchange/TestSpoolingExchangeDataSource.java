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
package io.trino.exchange;

import io.airlift.slice.Slice;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.spi.exchange.ExchangeSource;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.spi.exchange.ExchangeSourceOutputSelector;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSpoolingExchangeDataSource
{
    private static final Slice PAGE = utf8Slice("page");

    @Test
    public void testPollPageUpdatesMemoryUsage()
    {
        LocalMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext("test");
        SpoolingExchangeDataSource dataSource = new SpoolingExchangeDataSource(new TestingExchangeSource(PAGE, 123, () -> {}), memoryContext);

        assertThat(dataSource.pollPage()).isEqualTo(PAGE);
        assertThat(memoryContext.getBytes()).isEqualTo(123);

        dataSource.close();
        assertThat(memoryContext.getBytes()).isEqualTo(0);
    }

    @Test
    public void testCloseDuringPollPage()
    {
        AggregatedMemoryContext operatorMemoryContext = newSimpleAggregatedMemoryContext().newAggregatedMemoryContext();
        LocalMemoryContext memoryContext = operatorMemoryContext.newLocalMemoryContext("test");
        AtomicReference<SpoolingExchangeDataSource> dataSource = new AtomicReference<>();
        // Simulate another driver closing the shared data source and destroying the memory context
        // of the operator owning it while pollPage is reading from the exchange source
        TestingExchangeSource exchangeSource = new TestingExchangeSource(PAGE, 123, () -> {
            dataSource.get().close();
            operatorMemoryContext.close();
        });
        dataSource.set(new SpoolingExchangeDataSource(exchangeSource, memoryContext));

        assertThat(dataSource.get().pollPage()).isEqualTo(PAGE);
        assertThat(memoryContext.getBytes()).isEqualTo(0);
    }

    private static class TestingExchangeSource
            implements ExchangeSource
    {
        private final Slice data;
        private final long memoryUsage;
        private final Runnable onRead;

        public TestingExchangeSource(Slice data, long memoryUsage, Runnable onRead)
        {
            this.data = requireNonNull(data, "data is null");
            this.memoryUsage = memoryUsage;
            this.onRead = requireNonNull(onRead, "onRead is null");
        }

        @Override
        public void addSourceHandles(List<ExchangeSourceHandle> handles) {}

        @Override
        public void noMoreSourceHandles() {}

        @Override
        public void setOutputSelector(ExchangeSourceOutputSelector selector) {}

        @Override
        public CompletableFuture<Void> isBlocked()
        {
            return NOT_BLOCKED;
        }

        @Override
        public boolean isFinished()
        {
            return false;
        }

        @Override
        public Slice read()
        {
            onRead.run();
            return data;
        }

        @Override
        public long getMemoryUsage()
        {
            return memoryUsage;
        }

        @Override
        public void close() {}
    }
}
