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
package io.trino.operator.exchange;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.trino.SequencePageBuilder;
import io.trino.Session;
import io.trino.block.BlockAssertions;
import io.trino.execution.NodeTaskMap;
import io.trino.execution.scheduler.NodeScheduler;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.execution.scheduler.UniformNodeSelectorFactory;
import io.trino.metadata.InMemoryNodeManager;
import io.trino.operator.PageAssertions;
import io.trino.operator.exchange.LocalExchange.LocalExchangeSinkFactory;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorBucketNodeMap;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.planner.NodePartitioningManager;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.testing.TestingTransactionHandle;
import io.trino.util.FinalizerService;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.SystemSessionProperties.SKEWED_PARTITION_MIN_DATA_PROCESSED_REBALANCE_THRESHOLD;
import static io.trino.operator.InterpretedHashGenerator.createChannelsHashGenerator;
import static io.trino.spi.connector.ConnectorBucketNodeMap.createBucketNodeMap;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_PASSTHROUGH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SCALED_WRITER_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.stream.IntStream.range;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestLocalExchange
{
    private static final List<Type> TYPES = ImmutableList.of(BIGINT);
    private static final DataSize RETAINED_PAGE_SIZE = DataSize.ofBytes(createPage(42).getRetainedSizeInBytes());
    private static final DataSize PAGE_SIZE = DataSize.ofBytes(createPage(42).getSizeInBytes());
    private static final DataSize LOCAL_EXCHANGE_MAX_BUFFERED_BYTES = DataSize.of(32, MEGABYTE);
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();
    private static final Session SESSION = testSessionBuilder().build();
    private static final DataSize WRITER_SCALING_MIN_DATA_PROCESSED = DataSize.of(32, MEGABYTE);

    private final ConcurrentMap<CatalogHandle, ConnectorNodePartitioningProvider> partitionManagers = new ConcurrentHashMap<>();
    private NodePartitioningManager nodePartitioningManager;

    @BeforeMethod
    public void setUp()
    {
        NodeScheduler nodeScheduler = new NodeScheduler(new UniformNodeSelectorFactory(
                new InMemoryNodeManager(),
                new NodeSchedulerConfig().setIncludeCoordinator(true),
                new NodeTaskMap(new FinalizerService())));
        nodePartitioningManager = new NodePartitioningManager(
                nodeScheduler,
                new TypeOperators(),
                catalogHandle -> {
                    ConnectorNodePartitioningProvider result = partitionManagers.get(catalogHandle);
                    checkArgument(result != null, "No partition manager for catalog handle: %s", catalogHandle);
                    return result;
                });
    }

    @Test
    public void testGatherSingleWriter()
    {
        LocalExchange localExchange = new LocalExchange(
                nodePartitioningManager,
                SESSION,
                8,
                SINGLE_DISTRIBUTION,
                ImmutableList.of(),
                ImmutableList.of(),
                Optional.empty(),
                DataSize.ofBytes(retainedSizeOfPages(99)),
                TYPE_OPERATORS,
                WRITER_SCALING_MIN_DATA_PROCESSED);

        run(localExchange, exchange -> {
            assertEquals(exchange.getBufferCount(), 1);
            assertExchangeTotalBufferedBytes(exchange, 0);

            LocalExchangeSinkFactory sinkFactory = exchange.createSinkFactory();
            sinkFactory.noMoreSinkFactories();

            LocalExchangeSource source = exchange.getNextSource();
            assertSource(source, 0);

            LocalExchangeSink sink = sinkFactory.createSink();
            sinkFactory.close();

            assertSinkCanWrite(sink);
            assertSource(source, 0);

            // add the first page which should cause the reader to unblock
            ListenableFuture<Void> readFuture = source.waitForReading();
            assertFalse(readFuture.isDone());
            sink.addPage(createPage(0));
            assertTrue(readFuture.isDone());
            assertExchangeTotalBufferedBytes(exchange, 1);

            assertSource(source, 1);

            sink.addPage(createPage(1));
            assertSource(source, 2);
            assertExchangeTotalBufferedBytes(exchange, 2);

            assertRemovePage(source, createPage(0));
            assertSource(source, 1);
            assertExchangeTotalBufferedBytes(exchange, 1);

            assertRemovePage(source, createPage(1));
            assertSource(source, 0);
            assertExchangeTotalBufferedBytes(exchange, 0);

            sink.addPage(createPage(2));
            sink.addPage(createPage(3));
            assertSource(source, 2);
            assertExchangeTotalBufferedBytes(exchange, 2);

            sink.finish();
            assertSinkFinished(sink);

            assertSource(source, 2);

            assertRemovePage(source, createPage(2));
            assertSource(source, 1);
            assertSinkFinished(sink);
            assertExchangeTotalBufferedBytes(exchange, 1);

            assertRemovePage(source, createPage(3));
            assertSourceFinished(source);
            assertExchangeTotalBufferedBytes(exchange, 0);
        });
    }

    @Test
    public void testRandom()
    {
        LocalExchange localExchange = new LocalExchange(
                nodePartitioningManager,
                SESSION,
                2,
                FIXED_ARBITRARY_DISTRIBUTION,
                ImmutableList.of(),
                ImmutableList.of(),
                Optional.empty(),
                LOCAL_EXCHANGE_MAX_BUFFERED_BYTES,
                TYPE_OPERATORS,
                WRITER_SCALING_MIN_DATA_PROCESSED);

        run(localExchange, exchange -> {
            assertEquals(exchange.getBufferCount(), 2);
            assertExchangeTotalBufferedBytes(exchange, 0);

            LocalExchangeSinkFactory sinkFactory = exchange.createSinkFactory();
            sinkFactory.noMoreSinkFactories();
            LocalExchangeSink sink = sinkFactory.createSink();
            assertSinkCanWrite(sink);
            sinkFactory.close();

            LocalExchangeSource sourceA = exchange.getNextSource();
            assertSource(sourceA, 0);

            LocalExchangeSource sourceB = exchange.getNextSource();
            assertSource(sourceB, 0);

            for (int i = 0; i < 100; i++) {
                Page page = createPage(0);
                sink.addPage(page);
                assertExchangeTotalBufferedBytes(exchange, i + 1);

                LocalExchangeBufferInfo bufferInfoA = sourceA.getBufferInfo();
                LocalExchangeBufferInfo bufferInfoB = sourceB.getBufferInfo();
                assertEquals(bufferInfoA.getBufferedBytes() + bufferInfoB.getBufferedBytes(), retainedSizeOfPages(i + 1));
                assertEquals(bufferInfoA.getBufferedPages() + bufferInfoB.getBufferedPages(), i + 1);
            }

            // we should get ~50 pages per source, but we should get at least some pages in each buffer
            assertTrue(sourceA.getBufferInfo().getBufferedPages() > 0);
            assertTrue(sourceB.getBufferInfo().getBufferedPages() > 0);
            assertExchangeTotalBufferedBytes(exchange, 100);
        });
    }

    @Test
    public void testScaleWriter()
    {
        LocalExchange localExchange = new LocalExchange(
                nodePartitioningManager,
                SESSION,
                3,
                SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION,
                ImmutableList.of(),
                ImmutableList.of(),
                Optional.empty(),
                DataSize.ofBytes(retainedSizeOfPages(4)),
                TYPE_OPERATORS,
                DataSize.ofBytes(sizeOfPages(2)));

        run(localExchange, exchange -> {
            assertEquals(exchange.getBufferCount(), 3);
            assertExchangeTotalBufferedBytes(exchange, 0);

            LocalExchangeSinkFactory sinkFactory = exchange.createSinkFactory();
            sinkFactory.noMoreSinkFactories();
            LocalExchangeSink sink = sinkFactory.createSink();
            assertSinkCanWrite(sink);
            sinkFactory.close();

            LocalExchangeSource sourceA = exchange.getNextSource();
            assertSource(sourceA, 0);

            LocalExchangeSource sourceB = exchange.getNextSource();
            assertSource(sourceB, 0);

            LocalExchangeSource sourceC = exchange.getNextSource();
            assertSource(sourceC, 0);

            sink.addPage(createPage(0));
            sink.addPage(createPage(0));
            assertEquals(sourceA.getBufferInfo().getBufferedPages(), 2);
            assertEquals(sourceB.getBufferInfo().getBufferedPages(), 0);
            assertEquals(sourceC.getBufferInfo().getBufferedPages(), 0);

            // writer min output size and buffered data size limits are exceeded, so we should see pages in sourceB
            sink.addPage(createPage(0));
            assertEquals(sourceA.getBufferInfo().getBufferedPages(), 2);
            assertEquals(sourceB.getBufferInfo().getBufferedPages(), 1);
            assertEquals(sourceC.getBufferInfo().getBufferedPages(), 0);

            assertRemovePage(sourceA, createPage(0));
            assertRemovePage(sourceA, createPage(0));

            // writer min output size and buffered data size limits are exceeded again,
            sink.addPage(createPage(0));
            sink.addPage(createPage(0));
            sink.addPage(createPage(0));
            assertEquals(sourceA.getBufferInfo().getBufferedPages(), 1);
            assertEquals(sourceB.getBufferInfo().getBufferedPages(), 2);
            assertEquals(sourceC.getBufferInfo().getBufferedPages(), 1);
        });
    }

    @Test
    public void testNoWriterScalingWhenOnlyBufferSizeLimitIsExceeded()
    {
        LocalExchange localExchange = new LocalExchange(
                nodePartitioningManager,
                SESSION,
                3,
                SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION,
                ImmutableList.of(),
                ImmutableList.of(),
                Optional.empty(),
                DataSize.ofBytes(retainedSizeOfPages(4)),
                TYPE_OPERATORS,
                DataSize.ofBytes(sizeOfPages(10)));

        run(localExchange, exchange -> {
            assertEquals(exchange.getBufferCount(), 3);
            assertExchangeTotalBufferedBytes(exchange, 0);

            LocalExchangeSinkFactory sinkFactory = exchange.createSinkFactory();
            sinkFactory.noMoreSinkFactories();
            LocalExchangeSink sink = sinkFactory.createSink();
            assertSinkCanWrite(sink);
            sinkFactory.close();

            LocalExchangeSource sourceA = exchange.getNextSource();
            assertSource(sourceA, 0);

            LocalExchangeSource sourceB = exchange.getNextSource();
            assertSource(sourceB, 0);

            LocalExchangeSource sourceC = exchange.getNextSource();
            assertSource(sourceC, 0);

            range(0, 6).forEach(i -> sink.addPage(createPage(0)));
            assertEquals(sourceA.getBufferInfo().getBufferedPages(), 6);
            assertEquals(sourceB.getBufferInfo().getBufferedPages(), 0);
            assertEquals(sourceC.getBufferInfo().getBufferedPages(), 0);
        });
    }

    @Test
    public void testNoWriterScalingWhenOnlyWriterScalingMinDataProcessedLimitIsExceeded()
    {
        LocalExchange localExchange = new LocalExchange(
                nodePartitioningManager,
                SESSION,
                3,
                SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION,
                ImmutableList.of(),
                ImmutableList.of(),
                Optional.empty(),
                DataSize.ofBytes(retainedSizeOfPages(20)),
                TYPE_OPERATORS,
                DataSize.ofBytes(sizeOfPages(2)));

        run(localExchange, exchange -> {
            assertEquals(exchange.getBufferCount(), 3);
            assertExchangeTotalBufferedBytes(exchange, 0);

            LocalExchangeSinkFactory sinkFactory = exchange.createSinkFactory();
            sinkFactory.noMoreSinkFactories();
            LocalExchangeSink sink = sinkFactory.createSink();
            assertSinkCanWrite(sink);
            sinkFactory.close();

            AtomicLong physicalWrittenBytesA = new AtomicLong(0);
            LocalExchangeSource sourceA = exchange.getNextSource();
            assertSource(sourceA, 0);

            LocalExchangeSource sourceB = exchange.getNextSource();
            assertSource(sourceB, 0);

            LocalExchangeSource sourceC = exchange.getNextSource();
            assertSource(sourceC, 0);

            range(0, 8).forEach(i -> sink.addPage(createPage(0)));
            physicalWrittenBytesA.set(retainedSizeOfPages(8));
            sink.addPage(createPage(0));
            assertEquals(sourceA.getBufferInfo().getBufferedPages(), 9);
            assertEquals(sourceB.getBufferInfo().getBufferedPages(), 0);
            assertEquals(sourceC.getBufferInfo().getBufferedPages(), 0);
        });
    }

    @Test(dataProvider = "scalingPartitionHandles")
    public void testScalingForSkewedWriters(PartitioningHandle partitioningHandle)
    {
        LocalExchange localExchange = new LocalExchange(
                nodePartitioningManager,
                testSessionBuilder()
                        .setSystemProperty(SKEWED_PARTITION_MIN_DATA_PROCESSED_REBALANCE_THRESHOLD, "20kB")
                        .build(),
                4,
                partitioningHandle,
                ImmutableList.of(0),
                TYPES,
                Optional.empty(),
                DataSize.ofBytes(retainedSizeOfPages(2)),
                TYPE_OPERATORS,
                DataSize.of(10, KILOBYTE));

        run(localExchange, exchange -> {
            assertEquals(exchange.getBufferCount(), 4);
            assertExchangeTotalBufferedBytes(exchange, 0);

            LocalExchangeSinkFactory sinkFactory = exchange.createSinkFactory();
            sinkFactory.noMoreSinkFactories();
            LocalExchangeSink sink = sinkFactory.createSink();
            assertSinkCanWrite(sink);
            sinkFactory.close();

            LocalExchangeSource sourceA = exchange.getNextSource();
            assertSource(sourceA, 0);

            LocalExchangeSource sourceB = exchange.getNextSource();
            assertSource(sourceB, 0);

            LocalExchangeSource sourceC = exchange.getNextSource();
            assertSource(sourceC, 0);

            LocalExchangeSource sourceD = exchange.getNextSource();
            assertSource(sourceD, 0);

            sink.addPage(createSingleValuePage(0, 1000));
            sink.addPage(createSingleValuePage(0, 1000));
            sink.addPage(createSingleValuePage(1, 2));
            sink.addPage(createSingleValuePage(1, 2));

            // Two partitions are assigned to two different writers
            assertSource(sourceA, 2);
            assertSource(sourceB, 0);
            assertSource(sourceC, 0);
            assertSource(sourceD, 2);

            sink.addPage(createSingleValuePage(0, 1000));
            sink.addPage(createSingleValuePage(0, 1000));
            sink.addPage(createSingleValuePage(0, 1000));
            sink.addPage(createSingleValuePage(0, 1000));

            // Since writerD is skewed, scaling will happen for partition in writerD to writerB
            assertSource(sourceA, 2);
            assertSource(sourceB, 2);
            assertSource(sourceC, 0);
            assertSource(sourceD, 4);

            sink.addPage(createSingleValuePage(0, 1000));
            sink.addPage(createSingleValuePage(0, 1000));
            sink.addPage(createSingleValuePage(0, 1000));
            sink.addPage(createSingleValuePage(0, 1000));

            // Still there is a skewness across writers since writerA and writerC aren't writing any data.
            // Hence, scaling will happen for partition in writerD and writerB to writerC.
            assertSource(sourceA, 2);
            assertSource(sourceB, 4);
            assertSource(sourceC, 1);
            assertSource(sourceD, 5);

            sink.addPage(createSingleValuePage(0, 1000));
            sink.addPage(createSingleValuePage(0, 1000));
            sink.addPage(createSingleValuePage(0, 1000));
            sink.addPage(createSingleValuePage(0, 1000));
            sink.addPage(createSingleValuePage(0, 1000));
            sink.addPage(createSingleValuePage(0, 1000));

            // Still there is a skewness across writers since writerA isn't writing any data.
            // Hence, scaling will happen for partition in writerD and writerB to writerA.
            assertSource(sourceA, 4);
            assertSource(sourceB, 5);
            assertSource(sourceC, 3);
            assertSource(sourceD, 6);
        });
    }

    @Test(dataProvider = "scalingPartitionHandles")
    public void testNoScalingWhenDataWrittenIsLessThanMinFileSize(PartitioningHandle partitioningHandle)
    {
        LocalExchange localExchange = new LocalExchange(
                nodePartitioningManager,
                testSessionBuilder()
                        .setSystemProperty(SKEWED_PARTITION_MIN_DATA_PROCESSED_REBALANCE_THRESHOLD, "20kB")
                        .build(),
                4,
                partitioningHandle,
                ImmutableList.of(0),
                TYPES,
                Optional.empty(),
                DataSize.ofBytes(retainedSizeOfPages(2)),
                TYPE_OPERATORS,
                DataSize.of(50, MEGABYTE));

        run(localExchange, exchange -> {
            assertEquals(exchange.getBufferCount(), 4);
            assertExchangeTotalBufferedBytes(exchange, 0);

            LocalExchangeSinkFactory sinkFactory = exchange.createSinkFactory();
            sinkFactory.noMoreSinkFactories();
            LocalExchangeSink sink = sinkFactory.createSink();
            assertSinkCanWrite(sink);
            sinkFactory.close();

            LocalExchangeSource sourceA = exchange.getNextSource();
            assertSource(sourceA, 0);

            LocalExchangeSource sourceB = exchange.getNextSource();
            assertSource(sourceB, 0);

            LocalExchangeSource sourceC = exchange.getNextSource();
            assertSource(sourceC, 0);

            LocalExchangeSource sourceD = exchange.getNextSource();
            assertSource(sourceD, 0);

            sink.addPage(createSingleValuePage(0, 1000));
            sink.addPage(createSingleValuePage(0, 1000));
            sink.addPage(createSingleValuePage(1, 2));
            sink.addPage(createSingleValuePage(1, 2));

            // Two partitions are assigned to two different writers
            assertSource(sourceA, 2);
            assertSource(sourceB, 0);
            assertSource(sourceC, 0);
            assertSource(sourceD, 2);

            sink.addPage(createSingleValuePage(0, 1000));
            sink.addPage(createSingleValuePage(0, 1000));
            sink.addPage(createSingleValuePage(0, 1000));
            sink.addPage(createSingleValuePage(0, 1000));

            // No scaling since data written is less than 100 MBs
            assertSource(sourceA, 2);
            assertSource(sourceB, 0);
            assertSource(sourceC, 0);
            assertSource(sourceD, 6);
        });
    }

    @Test(dataProvider = "scalingPartitionHandles")
    public void testNoScalingWhenBufferUtilizationIsLessThanLimit(PartitioningHandle partitioningHandle)
    {
        LocalExchange localExchange = new LocalExchange(
                nodePartitioningManager,
                testSessionBuilder()
                        .setSystemProperty(SKEWED_PARTITION_MIN_DATA_PROCESSED_REBALANCE_THRESHOLD, "20kB")
                        .build(),
                4,
                partitioningHandle,
                ImmutableList.of(0),
                TYPES,
                Optional.empty(),
                DataSize.of(50, MEGABYTE),
                TYPE_OPERATORS,
                DataSize.of(10, KILOBYTE));

        run(localExchange, exchange -> {
            assertEquals(exchange.getBufferCount(), 4);
            assertExchangeTotalBufferedBytes(exchange, 0);

            LocalExchangeSinkFactory sinkFactory = exchange.createSinkFactory();
            sinkFactory.noMoreSinkFactories();
            LocalExchangeSink sink = sinkFactory.createSink();
            assertSinkCanWrite(sink);
            sinkFactory.close();

            LocalExchangeSource sourceA = exchange.getNextSource();
            assertSource(sourceA, 0);

            LocalExchangeSource sourceB = exchange.getNextSource();
            assertSource(sourceB, 0);

            LocalExchangeSource sourceC = exchange.getNextSource();
            assertSource(sourceC, 0);

            LocalExchangeSource sourceD = exchange.getNextSource();
            assertSource(sourceD, 0);

            sink.addPage(createSingleValuePage(0, 1000));
            sink.addPage(createSingleValuePage(0, 1000));
            sink.addPage(createSingleValuePage(1, 2));
            sink.addPage(createSingleValuePage(1, 2));

            // Two partitions are assigned to two different writers
            assertSource(sourceA, 2);
            assertSource(sourceB, 0);
            assertSource(sourceC, 0);
            assertSource(sourceD, 2);

            sink.addPage(createSingleValuePage(0, 1000));
            sink.addPage(createSingleValuePage(0, 1000));
            sink.addPage(createSingleValuePage(0, 1000));
            sink.addPage(createSingleValuePage(0, 1000));

            // No scaling since buffer utilization is less than 50%
            assertSource(sourceA, 2);
            assertSource(sourceB, 0);
            assertSource(sourceC, 0);
            assertSource(sourceD, 6);
        });
    }

    @Test
    public void testNoScalingWhenNoWriterSkewness()
    {
        LocalExchange localExchange = new LocalExchange(
                nodePartitioningManager,
                testSessionBuilder()
                        .setSystemProperty(SKEWED_PARTITION_MIN_DATA_PROCESSED_REBALANCE_THRESHOLD, "20kB")
                        .build(),
                2,
                SCALED_WRITER_HASH_DISTRIBUTION,
                ImmutableList.of(0),
                TYPES,
                Optional.empty(),
                DataSize.ofBytes(retainedSizeOfPages(2)),
                TYPE_OPERATORS,
                DataSize.of(50, KILOBYTE));

        run(localExchange, exchange -> {
            assertEquals(exchange.getBufferCount(), 2);
            assertExchangeTotalBufferedBytes(exchange, 0);

            LocalExchangeSinkFactory sinkFactory = exchange.createSinkFactory();
            sinkFactory.noMoreSinkFactories();
            LocalExchangeSink sink = sinkFactory.createSink();
            assertSinkCanWrite(sink);
            sinkFactory.close();

            LocalExchangeSource sourceA = exchange.getNextSource();
            assertSource(sourceA, 0);

            LocalExchangeSource sourceB = exchange.getNextSource();
            assertSource(sourceB, 0);

            sink.addPage(createSingleValuePage(0, 1000));
            sink.addPage(createSingleValuePage(1, 1000));

            // Two partitions are assigned to two different writers
            assertSource(sourceA, 1);
            assertSource(sourceB, 1);

            sink.addPage(createSingleValuePage(0, 1000));
            sink.addPage(createSingleValuePage(1, 1000));

            // No scaling since there is no skewness
            assertSource(sourceA, 2);
            assertSource(sourceB, 2);
        });
    }

    @Test
    public void testPassthrough()
    {
        LocalExchange localExchange = new LocalExchange(
                nodePartitioningManager,
                SESSION,
                2,
                FIXED_PASSTHROUGH_DISTRIBUTION,
                ImmutableList.of(),
                ImmutableList.of(),
                Optional.empty(),
                DataSize.ofBytes(retainedSizeOfPages(1)),
                TYPE_OPERATORS,
                WRITER_SCALING_MIN_DATA_PROCESSED);

        run(localExchange, exchange -> {
            assertEquals(exchange.getBufferCount(), 2);
            assertExchangeTotalBufferedBytes(exchange, 0);

            LocalExchangeSinkFactory sinkFactory = exchange.createSinkFactory();
            sinkFactory.noMoreSinkFactories();
            LocalExchangeSink sinkA = sinkFactory.createSink();
            LocalExchangeSink sinkB = sinkFactory.createSink();
            assertSinkCanWrite(sinkA);
            assertSinkCanWrite(sinkB);
            sinkFactory.close();

            LocalExchangeSource sourceA = exchange.getNextSource();
            assertSource(sourceA, 0);

            LocalExchangeSource sourceB = exchange.getNextSource();
            assertSource(sourceB, 0);

            sinkA.addPage(createPage(0));
            assertSource(sourceA, 1);
            assertSource(sourceB, 0);
            assertSinkWriteBlocked(sinkA);

            assertSinkCanWrite(sinkB);
            sinkB.addPage(createPage(1));
            assertSource(sourceA, 1);
            assertSource(sourceB, 1);
            assertSinkWriteBlocked(sinkA);

            assertExchangeTotalBufferedBytes(exchange, 2);

            assertRemovePage(sourceA, createPage(0));
            assertSource(sourceA, 0);
            assertSinkCanWrite(sinkA);
            assertSinkWriteBlocked(sinkB);
            assertExchangeTotalBufferedBytes(exchange, 1);

            sinkA.finish();
            assertSinkFinished(sinkA);
            assertSource(sourceB, 1);

            sourceA.finish();
            sourceB.finish();
            assertRemovePage(sourceB, createPage(1));
            assertSourceFinished(sourceA);
            assertSourceFinished(sourceB);

            assertSinkFinished(sinkB);
            assertExchangeTotalBufferedBytes(exchange, 0);
        });
    }

    @Test
    public void testPartition()
    {
        LocalExchange localExchange = new LocalExchange(
                nodePartitioningManager,
                SESSION,
                2,
                FIXED_HASH_DISTRIBUTION,
                ImmutableList.of(0),
                TYPES,
                Optional.empty(),
                LOCAL_EXCHANGE_MAX_BUFFERED_BYTES,
                TYPE_OPERATORS,
                WRITER_SCALING_MIN_DATA_PROCESSED);

        run(localExchange, exchange -> {
            assertEquals(exchange.getBufferCount(), 2);
            assertExchangeTotalBufferedBytes(exchange, 0);

            LocalExchangeSinkFactory sinkFactory = exchange.createSinkFactory();
            sinkFactory.noMoreSinkFactories();
            LocalExchangeSink sink = sinkFactory.createSink();
            assertSinkCanWrite(sink);
            sinkFactory.close();

            LocalExchangeSource sourceA = exchange.getNextSource();
            assertSource(sourceA, 0);

            LocalExchangeSource sourceB = exchange.getNextSource();
            assertSource(sourceB, 0);

            sink.addPage(createPage(0));

            assertSource(sourceA, 1);
            assertSource(sourceB, 1);
            assertTrue(sourceA.getBufferInfo().getBufferedBytes() + sourceB.getBufferInfo().getBufferedBytes() >= retainedSizeOfPages(1));

            sink.addPage(createPage(0));

            assertSource(sourceA, 2);
            assertSource(sourceB, 2);
            assertTrue(sourceA.getBufferInfo().getBufferedBytes() + sourceB.getBufferInfo().getBufferedBytes() >= retainedSizeOfPages(2));

            assertPartitionedRemovePage(sourceA, 0, 2);
            assertSource(sourceA, 1);
            assertSource(sourceB, 2);

            assertPartitionedRemovePage(sourceA, 0, 2);
            assertSource(sourceA, 0);
            assertSource(sourceB, 2);

            sink.finish();
            assertSinkFinished(sink);
            assertSourceFinished(sourceA);
            assertSource(sourceB, 2);

            assertPartitionedRemovePage(sourceB, 1, 2);
            assertSourceFinished(sourceA);
            assertSource(sourceB, 1);

            assertPartitionedRemovePage(sourceB, 1, 2);
            assertSourceFinished(sourceA);
            assertSourceFinished(sourceB);
            assertExchangeTotalBufferedBytes(exchange, 0);
        });
    }

    @Test
    public void testPartitionCustomPartitioning()
    {
        ConnectorPartitioningHandle connectorPartitioningHandle = new ConnectorPartitioningHandle() {};
        ConnectorNodePartitioningProvider connectorNodePartitioningProvider = new ConnectorNodePartitioningProvider()
        {
            @Override
            public Optional<ConnectorBucketNodeMap> getBucketNodeMapping(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
            {
                return Optional.of(createBucketNodeMap(2));
            }

            @Override
            public BucketFunction getBucketFunction(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle, List<Type> partitionChannelTypes, int bucketCount)
            {
                return (page, position) -> {
                    long rowValue = BIGINT.getLong(page.getBlock(0), position);
                    if (rowValue == 42) {
                        return 0;
                    }
                    return 1;
                };
            }
        };
        List<Type> types = ImmutableList.of(VARCHAR, BIGINT);
        partitionManagers.put(
                TEST_CATALOG_HANDLE,
                connectorNodePartitioningProvider);
        PartitioningHandle partitioningHandle = new PartitioningHandle(
                Optional.of(TEST_CATALOG_HANDLE),
                Optional.of(TestingTransactionHandle.create()),
                connectorPartitioningHandle);
        LocalExchange localExchange = new LocalExchange(
                nodePartitioningManager,
                SESSION,
                2,
                partitioningHandle,
                ImmutableList.of(1),
                ImmutableList.of(BIGINT),
                Optional.empty(),
                LOCAL_EXCHANGE_MAX_BUFFERED_BYTES,
                TYPE_OPERATORS,
                WRITER_SCALING_MIN_DATA_PROCESSED);

        run(localExchange, exchange -> {
            assertEquals(exchange.getBufferCount(), 2);
            assertExchangeTotalBufferedBytes(exchange, 0);

            LocalExchangeSinkFactory sinkFactory = exchange.createSinkFactory();
            sinkFactory.noMoreSinkFactories();
            LocalExchangeSink sink = sinkFactory.createSink();
            assertSinkCanWrite(sink);
            sinkFactory.close();

            LocalExchangeSource sourceB = exchange.getNextSource();
            assertSource(sourceB, 0);

            LocalExchangeSource sourceA = exchange.getNextSource();
            assertSource(sourceA, 0);

            Page pageA = SequencePageBuilder.createSequencePage(types, 1, 100, 42);
            sink.addPage(pageA);

            assertSource(sourceA, 1);
            assertSource(sourceB, 0);

            assertRemovePage(types, sourceA, pageA);
            assertSource(sourceA, 0);

            Page pageB = SequencePageBuilder.createSequencePage(types, 100, 100, 43);
            sink.addPage(pageB);

            assertSource(sourceA, 0);
            assertSource(sourceB, 1);

            assertRemovePage(types, sourceB, pageB);
            assertSource(sourceB, 0);
        });
    }

    @Test
    public void writeUnblockWhenAllReadersFinish()
    {
        LocalExchange localExchange = new LocalExchange(
                nodePartitioningManager,
                SESSION,
                2,
                FIXED_ARBITRARY_DISTRIBUTION,
                ImmutableList.of(),
                ImmutableList.of(),
                Optional.empty(),
                LOCAL_EXCHANGE_MAX_BUFFERED_BYTES,
                TYPE_OPERATORS,
                WRITER_SCALING_MIN_DATA_PROCESSED);

        run(localExchange, exchange -> {
            assertEquals(exchange.getBufferCount(), 2);
            assertExchangeTotalBufferedBytes(exchange, 0);

            LocalExchangeSinkFactory sinkFactory = exchange.createSinkFactory();
            sinkFactory.noMoreSinkFactories();
            LocalExchangeSink sinkA = sinkFactory.createSink();
            assertSinkCanWrite(sinkA);
            LocalExchangeSink sinkB = sinkFactory.createSink();
            assertSinkCanWrite(sinkB);
            sinkFactory.close();

            LocalExchangeSource sourceA = exchange.getNextSource();
            assertSource(sourceA, 0);

            LocalExchangeSource sourceB = exchange.getNextSource();
            assertSource(sourceB, 0);

            sourceA.finish();
            assertSourceFinished(sourceA);

            assertSinkCanWrite(sinkA);
            assertSinkCanWrite(sinkB);

            sourceB.finish();
            assertSourceFinished(sourceB);

            assertSinkFinished(sinkA);
            assertSinkFinished(sinkB);
        });
    }

    @Test
    public void writeUnblockWhenAllReadersFinishAndPagesConsumed()
    {
        LocalExchange localExchange = new LocalExchange(
                nodePartitioningManager,
                SESSION,
                2,
                FIXED_PASSTHROUGH_DISTRIBUTION,
                ImmutableList.of(),
                ImmutableList.of(),
                Optional.empty(),
                DataSize.ofBytes(2),
                TYPE_OPERATORS,
                WRITER_SCALING_MIN_DATA_PROCESSED);

        run(localExchange, exchange -> {
            assertEquals(exchange.getBufferCount(), 2);
            assertExchangeTotalBufferedBytes(exchange, 0);

            LocalExchangeSinkFactory sinkFactory = exchange.createSinkFactory();
            sinkFactory.noMoreSinkFactories();
            LocalExchangeSink sinkA = sinkFactory.createSink();
            assertSinkCanWrite(sinkA);
            ListenableFuture<Void> sinkAFinished = sinkA.isFinished();
            assertFalse(sinkAFinished.isDone());

            LocalExchangeSink sinkB = sinkFactory.createSink();
            assertSinkCanWrite(sinkB);
            ListenableFuture<Void> sinkBFinished = sinkB.isFinished();
            assertFalse(sinkBFinished.isDone());

            sinkFactory.close();

            LocalExchangeSource sourceA = exchange.getNextSource();
            assertSource(sourceA, 0);

            LocalExchangeSource sourceB = exchange.getNextSource();
            assertSource(sourceB, 0);

            sinkA.addPage(createPage(0));
            ListenableFuture<Void> sinkAFuture = assertSinkWriteBlocked(sinkA);
            sinkB.addPage(createPage(0));
            ListenableFuture<Void> sinkBFuture = assertSinkWriteBlocked(sinkB);

            assertSource(sourceA, 1);
            assertSource(sourceB, 1);
            assertExchangeTotalBufferedBytes(exchange, 2);

            sourceA.finish();
            assertSource(sourceA, 1);
            assertRemovePage(sourceA, createPage(0));
            assertSourceFinished(sourceA);
            assertExchangeTotalBufferedBytes(exchange, 1);

            assertSource(sourceB, 1);
            assertSinkWriteBlocked(sinkB);

            sourceB.finish();
            assertSource(sourceB, 1);
            assertRemovePage(sourceB, createPage(0));
            assertSourceFinished(sourceB);
            assertExchangeTotalBufferedBytes(exchange, 0);

            assertTrue(sinkAFuture.isDone());
            assertTrue(sinkBFuture.isDone());
            assertTrue(sinkAFinished.isDone());
            assertTrue(sinkBFinished.isDone());

            assertSinkFinished(sinkA);
            assertSinkFinished(sinkB);
        });
    }

    @DataProvider
    public Object[][] scalingPartitionHandles()
    {
        return new Object[][] {{SCALED_WRITER_HASH_DISTRIBUTION}, {getCustomScalingPartitioningHandle()}};
    }

    private PartitioningHandle getCustomScalingPartitioningHandle()
    {
        ConnectorPartitioningHandle connectorPartitioningHandle = new ConnectorPartitioningHandle() {};
        ConnectorNodePartitioningProvider connectorNodePartitioningProvider = new ConnectorNodePartitioningProvider()
        {
            @Override
            public Optional<ConnectorBucketNodeMap> getBucketNodeMapping(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
            {
                return Optional.of(createBucketNodeMap(4));
            }

            @Override
            public BucketFunction getBucketFunction(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle, List<Type> partitionChannelTypes, int bucketCount)
            {
                return (page, position) -> {
                    long rowValue = BIGINT.getLong(page.getBlock(0), position);
                    if (rowValue == 0) {
                        return 2;
                    }
                    return 1;
                };
            }
        };
        partitionManagers.put(
                TEST_CATALOG_HANDLE,
                connectorNodePartitioningProvider);
        return new PartitioningHandle(
                Optional.of(TEST_CATALOG_HANDLE),
                Optional.of(TestingTransactionHandle.create()),
                connectorPartitioningHandle,
                true);
    }

    private void run(LocalExchange localExchange, Consumer<LocalExchange> test)
    {
        test.accept(localExchange);
    }

    private static void assertSource(LocalExchangeSource source, int pageCount)
    {
        LocalExchangeBufferInfo bufferInfo = source.getBufferInfo();
        assertEquals(bufferInfo.getBufferedPages(), pageCount);
        assertFalse(source.isFinished());
        if (pageCount == 0) {
            assertFalse(source.waitForReading().isDone());
            assertNull(source.removePage());
            assertFalse(source.waitForReading().isDone());
            assertFalse(source.isFinished());
            assertEquals(bufferInfo.getBufferedBytes(), 0);
        }
        else {
            assertTrue(source.waitForReading().isDone());
            assertTrue(bufferInfo.getBufferedBytes() > 0);
        }
    }

    private static void assertSourceFinished(LocalExchangeSource source)
    {
        assertTrue(source.isFinished());
        LocalExchangeBufferInfo bufferInfo = source.getBufferInfo();
        assertEquals(bufferInfo.getBufferedPages(), 0);
        assertEquals(bufferInfo.getBufferedBytes(), 0);

        assertTrue(source.waitForReading().isDone());
        assertNull(source.removePage());
        assertTrue(source.waitForReading().isDone());

        assertTrue(source.isFinished());
    }

    private static void assertRemovePage(LocalExchangeSource source, Page expectedPage)
    {
        assertRemovePage(TYPES, source, expectedPage);
    }

    private static void assertRemovePage(List<Type> types, LocalExchangeSource source, Page expectedPage)
    {
        assertTrue(source.waitForReading().isDone());
        Page actualPage = source.removePage();
        assertNotNull(actualPage);

        assertEquals(actualPage.getChannelCount(), expectedPage.getChannelCount());
        PageAssertions.assertPageEquals(types, actualPage, expectedPage);
    }

    private static void assertPartitionedRemovePage(LocalExchangeSource source, int partition, int partitionCount)
    {
        assertTrue(source.waitForReading().isDone());
        Page page = source.removePage();
        assertNotNull(page);

        LocalPartitionGenerator partitionGenerator = new LocalPartitionGenerator(createChannelsHashGenerator(TYPES, new int[]{0}, TYPE_OPERATORS), partitionCount);
        for (int position = 0; position < page.getPositionCount(); position++) {
            assertEquals(partitionGenerator.getPartition(page, position), partition);
        }
    }

    private static void assertSinkCanWrite(LocalExchangeSink sink)
    {
        assertFalse(sink.isFinished().isDone());
        assertTrue(sink.waitForWriting().isDone());
    }

    private static ListenableFuture<Void> assertSinkWriteBlocked(LocalExchangeSink sink)
    {
        assertFalse(sink.isFinished().isDone());
        ListenableFuture<Void> writeFuture = sink.waitForWriting();
        assertFalse(writeFuture.isDone());
        return writeFuture;
    }

    private static void assertSinkFinished(LocalExchangeSink sink)
    {
        assertTrue(sink.isFinished().isDone());
        assertTrue(sink.waitForWriting().isDone());

        // this will be ignored
        sink.addPage(createPage(0));
        assertTrue(sink.isFinished().isDone());
        assertTrue(sink.waitForWriting().isDone());
    }

    private static void assertExchangeTotalBufferedBytes(LocalExchange exchange, int pageCount)
    {
        long bufferedBytes = 0;
        for (int i = 0; i < exchange.getBufferCount(); i++) {
            bufferedBytes += exchange.getSource(i).getBufferInfo().getBufferedBytes();
        }
        assertEquals(bufferedBytes, retainedSizeOfPages(pageCount));
    }

    private static Page createPage(int i)
    {
        return SequencePageBuilder.createSequencePage(TYPES, 100, i);
    }

    private static Page createSingleValuePage(int value, int length)
    {
        List<Long> values = range(0, length).mapToObj(i -> (long) value).collect(toImmutableList());
        Block block = BlockAssertions.createLongsBlock(values);
        return new Page(block);
    }

    private static long sizeOfPages(int count)
    {
        return PAGE_SIZE.toBytes() * count;
    }

    public static long retainedSizeOfPages(int count)
    {
        return RETAINED_PAGE_SIZE.toBytes() * count;
    }
}
