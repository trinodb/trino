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
package io.trino.plugin.exchange.filesystem;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.trino.spi.QueryId;
import io.trino.spi.exchange.Exchange;
import io.trino.spi.exchange.ExchangeContext;
import io.trino.spi.exchange.ExchangeId;
import io.trino.spi.exchange.ExchangeManager;
import io.trino.spi.exchange.ExchangeSink;
import io.trino.spi.exchange.ExchangeSinkHandle;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;
import io.trino.spi.exchange.ExchangeSource;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.spi.exchange.ExchangeSourceHandleSource.ExchangeSourceHandleBatch;
import io.trino.spi.exchange.ExchangeSourceOutputSelector;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.function.Function;

import static com.google.common.collect.ImmutableListMultimap.toImmutableListMultimap;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.plugin.exchange.filesystem.FileSystemExchangeErrorCode.MAX_OUTPUT_PARTITION_COUNT_EXCEEDED;
import static io.trino.spi.exchange.ExchangeId.createRandomExchangeId;
import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
        ExchangeId exchangeId = createRandomExchangeId();
        Exchange exchange = exchangeManager.createExchange(new ExchangeContext(new QueryId("query"), exchangeId), 2, false);
        ExchangeSinkHandle sinkHandle0 = exchange.addSink(0);
        ExchangeSinkHandle sinkHandle1 = exchange.addSink(1);
        ExchangeSinkHandle sinkHandle2 = exchange.addSink(2);
        exchange.noMoreSinks();

        ExchangeSinkInstanceHandle sinkInstanceHandle = exchange.instantiateSink(sinkHandle0, 0).get();
        writeData(
                sinkInstanceHandle,
                ImmutableListMultimap.of(
                        0, "0-0-0",
                        1, "0-1-0",
                        0, "0-0-1",
                        1, "0-1-1"),
                true);
        exchange.sinkFinished(sinkHandle0, 0);
        sinkInstanceHandle = exchange.instantiateSink(sinkHandle0, 1).get();
        writeData(
                sinkInstanceHandle,
                ImmutableListMultimap.of(
                        0, "0-0-0",
                        1, "0-1-0",
                        0, "0-0-1",
                        1, "0-1-1"),
                true);
        exchange.sinkFinished(sinkHandle0, 1);
        sinkInstanceHandle = exchange.instantiateSink(sinkHandle0, 2).get();
        writeData(
                sinkInstanceHandle,
                ImmutableListMultimap.of(
                        0, "failed",
                        1, "another failed"),
                false);
        exchange.sinkFinished(sinkHandle0, 2);

        sinkInstanceHandle = exchange.instantiateSink(sinkHandle1, 0).get();
        writeData(
                sinkInstanceHandle,
                ImmutableListMultimap.of(
                        0, "1-0-0",
                        1, "1-1-0",
                        0, "1-0-1",
                        1, "1-1-1"),
                true);
        exchange.sinkFinished(sinkHandle1, 0);
        sinkInstanceHandle = exchange.instantiateSink(sinkHandle1, 1).get();
        writeData(
                sinkInstanceHandle,
                ImmutableListMultimap.of(
                        0, "1-0-0",
                        1, "1-1-0",
                        0, "1-0-1",
                        1, "1-1-1"),
                true);
        exchange.sinkFinished(sinkHandle1, 1);
        sinkInstanceHandle = exchange.instantiateSink(sinkHandle1, 2).get();
        writeData(
                sinkInstanceHandle,
                ImmutableListMultimap.of(
                        0, "more failed",
                        1, "another failed"),
                false);
        exchange.sinkFinished(sinkHandle1, 2);

        sinkInstanceHandle = exchange.instantiateSink(sinkHandle2, 2).get();
        writeData(
                sinkInstanceHandle,
                ImmutableListMultimap.of(
                        0, "2-0-0",
                        1, "2-1-0"),
                true);
        exchange.sinkFinished(sinkHandle2, 2);
        exchange.allRequiredSinksFinished();

        ExchangeSourceHandleBatch sourceHandleBatch = exchange.getSourceHandles().getNextBatch().get();
        assertTrue(sourceHandleBatch.lastBatch());
        List<ExchangeSourceHandle> partitionHandles = sourceHandleBatch.handles();
        assertThat(partitionHandles).hasSize(2);

        Map<Integer, ExchangeSourceHandle> partitions = partitionHandles.stream()
                .collect(toImmutableMap(ExchangeSourceHandle::getPartitionId, Function.identity()));

        ExchangeSourceOutputSelector outputSelector = ExchangeSourceOutputSelector.builder(ImmutableSet.of(exchangeId))
                .include(exchangeId, 0, 0)
                .include(exchangeId, 1, 0)
                .include(exchangeId, 2, 2)
                .setPartitionCount(exchangeId, 3)
                .setFinal()
                .build();
        assertThat(readData(partitions.get(0), outputSelector))
                .containsExactlyInAnyOrder("0-0-0", "0-0-1", "1-0-0", "1-0-1", "2-0-0");

        assertThat(readData(partitions.get(1), outputSelector))
                .containsExactlyInAnyOrder("0-1-0", "0-1-1", "1-1-0", "1-1-1", "2-1-0");

        exchange.close();
    }

    @Test
    public void testLargePages()
            throws Exception
    {
        String smallPage = "a".repeat(toIntExact(DataSize.of(123, BYTE).toBytes()));
        String mediumPage = "b".repeat(toIntExact(DataSize.of(66, KILOBYTE).toBytes()));
        String largePage = "c".repeat(toIntExact(DataSize.of(5, MEGABYTE).toBytes()) - Integer.BYTES);
        String maxPage = "d".repeat(toIntExact(DataSize.of(16, MEGABYTE).toBytes()) - Integer.BYTES);

        ExchangeId exchangeId = createRandomExchangeId();
        Exchange exchange = exchangeManager.createExchange(new ExchangeContext(new QueryId("query"), exchangeId), 3, false);
        ExchangeSinkHandle sinkHandle0 = exchange.addSink(0);
        ExchangeSinkHandle sinkHandle1 = exchange.addSink(1);
        ExchangeSinkHandle sinkHandle2 = exchange.addSink(2);
        exchange.noMoreSinks();

        ExchangeSinkInstanceHandle sinkInstanceHandle = exchange.instantiateSink(sinkHandle0, 0).get();
        writeData(
                sinkInstanceHandle,
                new ImmutableListMultimap.Builder<Integer, String>()
                        .putAll(0, ImmutableList.of(smallPage))
                        .putAll(1, ImmutableList.of(maxPage, mediumPage))
                        .putAll(2, ImmutableList.of())
                        .build(),
                true);
        exchange.sinkFinished(sinkHandle0, 0);

        sinkInstanceHandle = exchange.instantiateSink(sinkHandle1, 0).get();
        writeData(
                sinkInstanceHandle,
                new ImmutableListMultimap.Builder<Integer, String>()
                        .putAll(0, ImmutableList.of(mediumPage))
                        .putAll(1, ImmutableList.of(largePage))
                        .putAll(2, ImmutableList.of(smallPage))
                        .build(),
                true);
        exchange.sinkFinished(sinkHandle1, 0);

        sinkInstanceHandle = exchange.instantiateSink(sinkHandle2, 0).get();
        writeData(
                sinkInstanceHandle,
                new ImmutableListMultimap.Builder<Integer, String>()
                        .putAll(0, ImmutableList.of(largePage, maxPage))
                        .putAll(1, ImmutableList.of(smallPage))
                        .putAll(2, ImmutableList.of(maxPage, largePage, mediumPage))
                        .build(),
                true);
        exchange.sinkFinished(sinkHandle2, 0);
        exchange.allRequiredSinksFinished();

        ExchangeSourceHandleBatch sourceHandleBatch = exchange.getSourceHandles().getNextBatch().get();
        assertTrue(sourceHandleBatch.lastBatch());
        List<ExchangeSourceHandle> partitionHandles = sourceHandleBatch.handles();
        assertThat(partitionHandles).hasSize(10);

        ListMultimap<Integer, ExchangeSourceHandle> partitions = partitionHandles.stream()
                .collect(toImmutableListMultimap(ExchangeSourceHandle::getPartitionId, Function.identity()));

        ExchangeSourceOutputSelector outputSelector = ExchangeSourceOutputSelector.builder(ImmutableSet.of(exchangeId))
                .include(exchangeId, 0, 0)
                .include(exchangeId, 1, 0)
                .include(exchangeId, 2, 0)
                .setPartitionCount(exchangeId, 3)
                .setFinal()
                .build();

        assertThat(readData(partitions.get(0), outputSelector))
                .containsExactlyInAnyOrder(smallPage, mediumPage, largePage, maxPage);

        assertThat(readData(partitions.get(1), outputSelector))
                .containsExactlyInAnyOrder(smallPage, mediumPage, largePage, maxPage);

        assertThat(readData(partitions.get(2), outputSelector))
                .containsExactlyInAnyOrder(smallPage, mediumPage, largePage, maxPage);

        exchange.close();
    }

    @Test
    public void testMaxOutputPartitionCountCheck()
    {
        assertThatThrownBy(() -> exchangeManager.createExchange(new ExchangeContext(new QueryId("query"), createRandomExchangeId()), 51, false))
                .hasMessageContaining("Max number of output partitions exceeded for exchange")
                .hasFieldOrPropertyWithValue("errorCode", MAX_OUTPUT_PARTITION_COUNT_EXCEEDED.toErrorCode());
    }

    private void writeData(ExchangeSinkInstanceHandle handle, Multimap<Integer, String> data, boolean finish)
    {
        ExchangeSink sink = exchangeManager.createSink(handle);
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

    private List<String> readData(ExchangeSourceHandle handle, ExchangeSourceOutputSelector outputSelector)
    {
        return readData(ImmutableList.of(handle), outputSelector);
    }

    private List<String> readData(List<ExchangeSourceHandle> handles, ExchangeSourceOutputSelector outputSelector)
    {
        ImmutableList.Builder<String> result = ImmutableList.builder();
        try (ExchangeSource source = exchangeManager.createSource()) {
            source.setOutputSelector(outputSelector);
            Queue<ExchangeSourceHandle> remainingHandles = new ArrayDeque<>(handles);
            while (!source.isFinished()) {
                Slice data = source.read();
                if (data != null) {
                    result.add(data.toStringUtf8());
                }
                ExchangeSourceHandle handle = remainingHandles.poll();
                if (handle != null) {
                    source.addSourceHandles(ImmutableList.of(handle));
                }
                else {
                    source.noMoreSourceHandles();
                }
            }
        }
        return result.build();
    }
}
