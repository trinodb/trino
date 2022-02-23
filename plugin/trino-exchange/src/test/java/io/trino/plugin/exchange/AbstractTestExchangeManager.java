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
package io.trino.plugin.exchange;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
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
import java.util.function.Function;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.spi.exchange.ExchangeId.createRandomExchangeId;
import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractTestExchangeManager
{
    private static final String SMALL_PAGE = "a".repeat(toIntExact(DataSize.of(123, BYTE).toBytes()));
    private static final String MEDIUM_PAGE = "b".repeat(toIntExact(DataSize.of(66, KILOBYTE).toBytes()));
    private static final String LARGE_PAGE = "c".repeat(toIntExact(DataSize.of(5, MEGABYTE).toBytes()) - Integer.BYTES);
    private static final String MAX_PAGE = "d".repeat(toIntExact(DataSize.of(16, MEGABYTE).toBytes()) - Integer.BYTES);

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

        List<ExchangeSourceHandle> partitionHandles = exchange.getSourceHandles().get();
        assertThat(partitionHandles).hasSize(2);

        Map<Integer, ExchangeSourceHandle> partitions = partitionHandles.stream()
                .collect(toImmutableMap(ExchangeSourceHandle::getPartitionId, Function.identity()));

        assertThat(readData(partitions.get(0)))
                .containsExactlyInAnyOrder("0-0-0", "0-0-1", "1-0-0", "1-0-1", "2-0-0");

        assertThat(readData(partitions.get(1)))
                .containsExactlyInAnyOrder("0-1-0", "0-1-1", "1-1-0", "1-1-1", "2-1-0");

        exchange.close();
    }

    @Test
    public void testLargePages()
            throws Exception
    {
        Exchange exchange = exchangeManager.createExchange(new ExchangeContext(new QueryId("query"), createRandomExchangeId()), 3);
        ExchangeSinkHandle sinkHandle0 = exchange.addSink(0);
        ExchangeSinkHandle sinkHandle1 = exchange.addSink(1);
        ExchangeSinkHandle sinkHandle2 = exchange.addSink(2);
        exchange.noMoreSinks();

        ExchangeSinkInstanceHandle sinkInstanceHandle = exchange.instantiateSink(sinkHandle0, 0);
        writeData(
                sinkInstanceHandle,
                new ImmutableListMultimap.Builder<Integer, String>()
                        .putAll(0, ImmutableList.of(SMALL_PAGE))
                        .putAll(1, ImmutableList.of(MAX_PAGE, MEDIUM_PAGE))
                        .putAll(2, ImmutableList.of())
                        .build(),
                true);
        exchange.sinkFinished(sinkInstanceHandle);

        sinkInstanceHandle = exchange.instantiateSink(sinkHandle1, 0);
        writeData(
                sinkInstanceHandle,
                new ImmutableListMultimap.Builder<Integer, String>()
                        .putAll(0, ImmutableList.of(MEDIUM_PAGE))
                        .putAll(1, ImmutableList.of(LARGE_PAGE))
                        .putAll(2, ImmutableList.of(SMALL_PAGE))
                        .build(),
                true);
        exchange.sinkFinished(sinkInstanceHandle);

        sinkInstanceHandle = exchange.instantiateSink(sinkHandle2, 0);
        writeData(
                sinkInstanceHandle,
                new ImmutableListMultimap.Builder<Integer, String>()
                        .putAll(0, ImmutableList.of(LARGE_PAGE, MAX_PAGE))
                        .putAll(1, ImmutableList.of(SMALL_PAGE))
                        .putAll(2, ImmutableList.of(MAX_PAGE, LARGE_PAGE, MEDIUM_PAGE))
                        .build(),
                true);
        exchange.sinkFinished(sinkInstanceHandle);

        List<ExchangeSourceHandle> partitionHandles = exchange.getSourceHandles().get();
        assertThat(partitionHandles).hasSize(3);

        Map<Integer, ExchangeSourceHandle> partitions = partitionHandles.stream()
                .collect(toImmutableMap(ExchangeSourceHandle::getPartitionId, Function.identity()));

        assertThat(readData(partitions.get(0)))
                .containsExactlyInAnyOrder(SMALL_PAGE, MEDIUM_PAGE, LARGE_PAGE, MAX_PAGE);

        assertThat(readData(partitions.get(1)))
                .containsExactlyInAnyOrder(SMALL_PAGE, MEDIUM_PAGE, LARGE_PAGE, MAX_PAGE);

        assertThat(readData(partitions.get(2)))
                .containsExactlyInAnyOrder(SMALL_PAGE, MEDIUM_PAGE, LARGE_PAGE, MAX_PAGE);

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
