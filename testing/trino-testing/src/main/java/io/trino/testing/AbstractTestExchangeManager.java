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
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.QueryId;
import io.trino.spi.exchange.Exchange;
import io.trino.spi.exchange.ExchangeContext;
import io.trino.spi.exchange.ExchangeManager;
import io.trino.spi.exchange.ExchangeSink;
import io.trino.spi.exchange.ExchangeSinkHandle;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;
import io.trino.spi.exchange.ExchangeSource;
import io.trino.spi.exchange.ExchangeSourceHandle;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.spi.exchange.ExchangeId.createRandomExchangeId;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertTrue;

public abstract class AbstractTestExchangeManager
{
    private ExchangeManager exchangeManager;

    @BeforeClass
    public void init()
            throws Exception
    {
        exchangeManager = createExchangeManager();
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
            throws Exception
    {
        if (exchangeManager != null) {
            exchangeManager = null;
        }
    }

    protected abstract ExchangeManager createExchangeManager();

    @Test
    public void testHappyPath()
            throws Exception
    {
        Exchange exchange = exchangeManager.createExchange(new ExchangeContext(new QueryId("query"), createRandomExchangeId()), 2);
        ExchangeSinkHandle sinkHandle0 = exchange.addSink(0);
        ExchangeSinkHandle sinkHandle1 = exchange.addSink(1);
        ExchangeSinkHandle sinkHandle2 = exchange.addSink(2);
        exchange.noMoreSinks();

        ExchangeSinkInstanceHandle sinkInstanceHandle = exchange.instantiateSink(sinkHandle0, 0);
        writeData(
                sinkInstanceHandle,
                ImmutableListMultimap.of(
                        0, "0-0-0",
                        1, "0-1-0",
                        0, "0-0-1",
                        1, "0-1-1"),
                true);
        exchange.sinkFinished(sinkInstanceHandle);
        sinkInstanceHandle = exchange.instantiateSink(sinkHandle0, 1);
        writeData(
                sinkInstanceHandle,
                ImmutableListMultimap.of(
                        0, "0-0-0",
                        1, "0-1-0",
                        0, "0-0-1",
                        1, "0-1-1"),
                true);
        exchange.sinkFinished(sinkInstanceHandle);
        sinkInstanceHandle = exchange.instantiateSink(sinkHandle0, 2);
        writeData(
                sinkInstanceHandle,
                ImmutableListMultimap.of(
                        0, "failed",
                        1, "another failed"),
                false);
        exchange.sinkFinished(sinkInstanceHandle);

        sinkInstanceHandle = exchange.instantiateSink(sinkHandle1, 0);
        writeData(
                sinkInstanceHandle,
                ImmutableListMultimap.of(
                        0, "1-0-0",
                        1, "1-1-0",
                        0, "1-0-1",
                        1, "1-1-1"),
                true);
        exchange.sinkFinished(sinkInstanceHandle);
        sinkInstanceHandle = exchange.instantiateSink(sinkHandle1, 1);
        writeData(
                sinkInstanceHandle,
                ImmutableListMultimap.of(
                        0, "1-0-0",
                        1, "1-1-0",
                        0, "1-0-1",
                        1, "1-1-1"),
                true);
        exchange.sinkFinished(sinkInstanceHandle);
        sinkInstanceHandle = exchange.instantiateSink(sinkHandle1, 2);
        writeData(
                sinkInstanceHandle,
                ImmutableListMultimap.of(
                        0, "more failed",
                        1, "another failed"),
                false);
        exchange.sinkFinished(sinkInstanceHandle);

        sinkInstanceHandle = exchange.instantiateSink(sinkHandle2, 2);
        writeData(
                sinkInstanceHandle,
                ImmutableListMultimap.of(
                        0, "2-0-0",
                        1, "2-1-0"),
                true);
        exchange.sinkFinished(sinkInstanceHandle);

        CompletableFuture<List<ExchangeSourceHandle>> inputPartitionHandlesFuture = exchange.getSourceHandles();
        assertTrue(inputPartitionHandlesFuture.isDone());

        List<ExchangeSourceHandle> partitionHandles = inputPartitionHandlesFuture.get();
        assertThat(partitionHandles).hasSize(2);

        Map<Integer, ExchangeSourceHandle> partitions = partitionHandles.stream()
                .collect(toImmutableMap(ExchangeSourceHandle::getPartitionId, Function.identity()));

        assertThat(readData(partitions.get(0)))
                .containsExactlyInAnyOrder("0-0-0", "0-0-1", "1-0-0", "1-0-1", "2-0-0");

        assertThat(readData(partitions.get(1)))
                .containsExactlyInAnyOrder("0-1-0", "0-1-1", "1-1-0", "1-1-1", "2-1-0");

        exchange.close();
    }

    private void writeData(ExchangeSinkInstanceHandle handle, Multimap<Integer, String> data, boolean finish)
    {
        ExchangeSink sink = exchangeManager.createSink(handle, false);
        data.forEach((key, value) -> {
            sink.add(key, Slices.utf8Slice(value));
        });
        if (finish) {
            getFutureValue(sink.finish());
        }
        else {
            getFutureValue(sink.abort());
        }
    }

    private List<String> readData(ExchangeSourceHandle handle)
    {
        return readData(ImmutableList.of(handle));
    }

    private List<String> readData(List<ExchangeSourceHandle> handles)
    {
        ImmutableList.Builder<String> result = ImmutableList.builder();
        try (ExchangeSource source = exchangeManager.createSource(handles)) {
            while (!source.isFinished()) {
                Slice data = source.read();
                if (data != null) {
                    result.add(data.toStringUtf8());
                }
            }
        }
        return result.build();
    }
}
