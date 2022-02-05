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
import io.trino.connector.CatalogHandle;
import io.trino.execution.NodeTaskMap;
import io.trino.execution.scheduler.NodeScheduler;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.execution.scheduler.UniformNodeSelectorFactory;
import io.trino.metadata.InMemoryNodeManager;
import io.trino.operator.InterpretedHashGenerator;
import io.trino.operator.PageAssertions;
import io.trino.operator.exchange.LocalExchange.LocalExchangeSinkFactory;
import io.trino.spi.Page;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.connector.ConnectorBucketNodeMap;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.planner.NodePartitioningManager;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.testing.TestingTransactionHandle;
import io.trino.type.BlockTypeOperators;
import io.trino.util.FinalizerService;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.connector.CatalogHandle.createRootCatalogHandle;
import static io.trino.spi.connector.ConnectorBucketNodeMap.createBucketNodeMap;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_PASSTHROUGH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.testing.TestingSession.testSessionBuilder;
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
    private static final DataSize LOCAL_EXCHANGE_MAX_BUFFERED_BYTES = DataSize.of(32, DataSize.Unit.MEGABYTE);
    private static final BlockTypeOperators TYPE_OPERATOR_FACTORY = new BlockTypeOperators(new TypeOperators());
    private static final Session SESSION = testSessionBuilder().build();

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
                new BlockTypeOperators(new TypeOperators()),
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
                TYPES,
                Optional.empty(),
                DataSize.ofBytes(retainedSizeOfPages(99)),
                TYPE_OPERATOR_FACTORY);

        run(localExchange, exchange -> {
            assertEquals(exchange.getBufferCount(), 1);
            assertExchangeTotalBufferedBytes(exchange, 0);

            LocalExchangeSinkFactory sinkFactory = exchange.createSinkFactory();
            sinkFactory.noMoreSinkFactories();

            LocalExchangeSource source = exchange.getSource(0);
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
    public void testBroadcast()
    {
        LocalExchange localExchange = new LocalExchange(
                nodePartitioningManager,
                SESSION,
                2,
                FIXED_BROADCAST_DISTRIBUTION,
                ImmutableList.of(),
                TYPES,
                Optional.empty(),
                LOCAL_EXCHANGE_MAX_BUFFERED_BYTES,
                TYPE_OPERATOR_FACTORY);

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

            LocalExchangeSource sourceA = exchange.getSource(0);
            assertSource(sourceA, 0);

            LocalExchangeSource sourceB = exchange.getSource(1);
            assertSource(sourceB, 0);

            sinkA.addPage(createPage(0));

            assertSource(sourceA, 1);
            assertSource(sourceB, 1);
            assertExchangeTotalBufferedBytes(exchange, 1);

            sinkA.addPage(createPage(0));

            assertSource(sourceA, 2);
            assertSource(sourceB, 2);
            assertExchangeTotalBufferedBytes(exchange, 2);

            assertRemovePage(sourceA, createPage(0));
            assertSource(sourceA, 1);
            assertSource(sourceB, 2);
            assertExchangeTotalBufferedBytes(exchange, 2);

            assertRemovePage(sourceA, createPage(0));
            assertSource(sourceA, 0);
            assertSource(sourceB, 2);
            assertExchangeTotalBufferedBytes(exchange, 2);

            sinkA.finish();
            assertSinkFinished(sinkA);
            assertExchangeTotalBufferedBytes(exchange, 2);

            sinkB.addPage(createPage(0));
            assertSource(sourceA, 1);
            assertSource(sourceB, 3);
            assertExchangeTotalBufferedBytes(exchange, 3);

            sinkB.finish();
            assertSinkFinished(sinkB);
            assertSource(sourceA, 1);
            assertSource(sourceB, 3);
            assertExchangeTotalBufferedBytes(exchange, 3);

            assertRemovePage(sourceA, createPage(0));
            assertSourceFinished(sourceA);
            assertSource(sourceB, 3);
            assertExchangeTotalBufferedBytes(exchange, 3);

            assertRemovePage(sourceB, createPage(0));
            assertRemovePage(sourceB, createPage(0));
            assertSourceFinished(sourceA);
            assertSource(sourceB, 1);
            assertExchangeTotalBufferedBytes(exchange, 1);

            assertRemovePage(sourceB, createPage(0));
            assertSourceFinished(sourceA);
            assertSourceFinished(sourceB);
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
                TYPES,
                Optional.empty(),
                LOCAL_EXCHANGE_MAX_BUFFERED_BYTES,
                TYPE_OPERATOR_FACTORY);

        run(localExchange, exchange -> {
            assertEquals(exchange.getBufferCount(), 2);
            assertExchangeTotalBufferedBytes(exchange, 0);

            LocalExchangeSinkFactory sinkFactory = exchange.createSinkFactory();
            sinkFactory.noMoreSinkFactories();
            LocalExchangeSink sink = sinkFactory.createSink();
            assertSinkCanWrite(sink);
            sinkFactory.close();

            LocalExchangeSource sourceA = exchange.getSource(0);
            assertSource(sourceA, 0);

            LocalExchangeSource sourceB = exchange.getSource(1);
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
    public void testPassthrough()
    {
        LocalExchange localExchange = new LocalExchange(
                nodePartitioningManager,
                SESSION,
                2,
                FIXED_PASSTHROUGH_DISTRIBUTION,
                ImmutableList.of(),
                TYPES,
                Optional.empty(),
                DataSize.ofBytes(retainedSizeOfPages(1)),
                TYPE_OPERATOR_FACTORY);

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

            LocalExchangeSource sourceA = exchange.getSource(0);
            assertSource(sourceA, 0);

            LocalExchangeSource sourceB = exchange.getSource(1);
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
                TYPE_OPERATOR_FACTORY);

        run(localExchange, exchange -> {
            assertEquals(exchange.getBufferCount(), 2);
            assertExchangeTotalBufferedBytes(exchange, 0);

            LocalExchangeSinkFactory sinkFactory = exchange.createSinkFactory();
            sinkFactory.noMoreSinkFactories();
            LocalExchangeSink sink = sinkFactory.createSink();
            assertSinkCanWrite(sink);
            sinkFactory.close();

            LocalExchangeSource sourceA = exchange.getSource(0);
            assertSource(sourceA, 0);

            LocalExchangeSource sourceB = exchange.getSource(1);
            assertSource(sourceB, 0);

            sink.addPage(createPage(0));

            assertSource(sourceA, 1);
            assertSource(sourceB, 1);
            assertTrue(exchange.getBufferedBytes() >= retainedSizeOfPages(1));

            sink.addPage(createPage(0));

            assertSource(sourceA, 2);
            assertSource(sourceB, 2);
            assertTrue(exchange.getBufferedBytes() >= retainedSizeOfPages(2));

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
            public ConnectorBucketNodeMap getBucketNodeMap(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
            {
                return createBucketNodeMap(2);
            }

            @Override
            public ToIntFunction<ConnectorSplit> getSplitBucketFunction(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
            {
                throw new UnsupportedOperationException();
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
                createRootCatalogHandle("foo"),
                connectorNodePartitioningProvider);
        PartitioningHandle partitioningHandle = new PartitioningHandle(
                Optional.of(createRootCatalogHandle("foo")),
                Optional.of(TestingTransactionHandle.create()),
                connectorPartitioningHandle);
        LocalExchange localExchange = new LocalExchange(
                nodePartitioningManager,
                SESSION,
                2,
                partitioningHandle,
                ImmutableList.of(1),
                types,
                Optional.empty(),
                LOCAL_EXCHANGE_MAX_BUFFERED_BYTES,
                TYPE_OPERATOR_FACTORY);

        run(localExchange, exchange -> {
            assertEquals(exchange.getBufferCount(), 2);
            assertExchangeTotalBufferedBytes(exchange, 0);

            LocalExchangeSinkFactory sinkFactory = exchange.createSinkFactory();
            sinkFactory.noMoreSinkFactories();
            LocalExchangeSink sink = sinkFactory.createSink();
            assertSinkCanWrite(sink);
            sinkFactory.close();

            LocalExchangeSource sourceA = exchange.getSource(1);
            assertSource(sourceA, 0);

            LocalExchangeSource sourceB = exchange.getSource(0);
            assertSource(sourceB, 0);

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
        ImmutableList<Type> types = ImmutableList.of(BIGINT);

        LocalExchange localExchange = new LocalExchange(
                nodePartitioningManager,
                SESSION,
                2,
                FIXED_BROADCAST_DISTRIBUTION,
                ImmutableList.of(),
                types,
                Optional.empty(),
                LOCAL_EXCHANGE_MAX_BUFFERED_BYTES,
                TYPE_OPERATOR_FACTORY);

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

            LocalExchangeSource sourceA = exchange.getSource(0);
            assertSource(sourceA, 0);

            LocalExchangeSource sourceB = exchange.getSource(1);
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
                FIXED_BROADCAST_DISTRIBUTION,
                ImmutableList.of(),
                TYPES,
                Optional.empty(),
                DataSize.ofBytes(1),
                TYPE_OPERATOR_FACTORY);

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

            LocalExchangeSource sourceA = exchange.getSource(0);
            assertSource(sourceA, 0);

            LocalExchangeSource sourceB = exchange.getSource(1);
            assertSource(sourceB, 0);

            sinkA.addPage(createPage(0));
            ListenableFuture<Void> sinkAFuture = assertSinkWriteBlocked(sinkA);
            ListenableFuture<Void> sinkBFuture = assertSinkWriteBlocked(sinkB);

            assertSource(sourceA, 1);
            assertSource(sourceB, 1);
            assertExchangeTotalBufferedBytes(exchange, 1);

            sourceA.finish();
            assertSource(sourceA, 1);
            assertRemovePage(sourceA, createPage(0));
            assertSourceFinished(sourceA);
            assertExchangeTotalBufferedBytes(exchange, 1);

            assertSource(sourceB, 1);
            assertSinkWriteBlocked(sinkA);
            assertSinkWriteBlocked(sinkB);

            sourceB.finish();
            assertSource(sourceB, 1);
            assertRemovePage(sourceB, createPage(0));
            assertSourceFinished(sourceB);
            assertExchangeTotalBufferedBytes(exchange, 0);

            assertTrue(sinkAFuture.isDone());
            assertTrue(sinkBFuture.isDone());

            assertSinkFinished(sinkA);
            assertSinkFinished(sinkB);
        });
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

        LocalPartitionGenerator partitionGenerator = new LocalPartitionGenerator(new InterpretedHashGenerator(TYPES, new int[] {0}, TYPE_OPERATOR_FACTORY), partitionCount);
        for (int position = 0; position < page.getPositionCount(); position++) {
            assertEquals(partitionGenerator.getPartition(page, position), partition);
        }
    }

    private static void assertSinkCanWrite(LocalExchangeSink sink)
    {
        assertFalse(sink.isFinished());
        assertTrue(sink.waitForWriting().isDone());
    }

    private static ListenableFuture<Void> assertSinkWriteBlocked(LocalExchangeSink sink)
    {
        assertFalse(sink.isFinished());
        ListenableFuture<Void> writeFuture = sink.waitForWriting();
        assertFalse(writeFuture.isDone());
        return writeFuture;
    }

    private static void assertSinkFinished(LocalExchangeSink sink)
    {
        assertTrue(sink.isFinished());
        assertTrue(sink.waitForWriting().isDone());

        // this will be ignored
        sink.addPage(createPage(0));
        assertTrue(sink.isFinished());
        assertTrue(sink.waitForWriting().isDone());
    }

    private static void assertExchangeTotalBufferedBytes(LocalExchange exchange, int pageCount)
    {
        assertEquals(exchange.getBufferedBytes(), retainedSizeOfPages(pageCount));
    }

    private static Page createPage(int i)
    {
        return SequencePageBuilder.createSequencePage(TYPES, 100, i);
    }

    public static long retainedSizeOfPages(int count)
    {
        return RETAINED_PAGE_SIZE.toBytes() * count;
    }
}
