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
package io.trino.operator.output;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.execution.StateMachine;
import io.trino.execution.TaskManagerConfig.PagePartitioningStrategy;
import io.trino.execution.buffer.BufferResult;
import io.trino.execution.buffer.BufferState;
import io.trino.execution.buffer.OutputBuffer;
import io.trino.execution.buffer.OutputBufferInfo;
import io.trino.execution.buffer.OutputBufferStatus;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.execution.buffer.PageDeserializer;
import io.trino.execution.buffer.PagesSerdeFactory;
import io.trino.execution.buffer.PipelinedOutputBuffers.OutputBufferId;
import io.trino.operator.BucketPartitionFunction;
import io.trino.operator.DriverContext;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.operator.OutputFactory;
import io.trino.operator.PartitionFunction;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.TestingBlockEncodingSerde;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.type.AbstractType;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.TestingTaskContext;
import io.trino.type.BlockTypeOperators;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.block.BlockAssertions.createLongDictionaryBlock;
import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.block.BlockAssertions.createRandomBlockForType;
import static io.trino.block.BlockAssertions.createRepeatedValuesBlock;
import static io.trino.execution.TaskManagerConfig.PagePartitioningStrategy.COLUMNAR;
import static io.trino.execution.TaskManagerConfig.PagePartitioningStrategy.MOST_EFFICIENT_FOR_FIRST_PAGE;
import static io.trino.execution.TaskManagerConfig.PagePartitioningStrategy.MOST_EFFICIENT_PER_PAGE;
import static io.trino.execution.TaskManagerConfig.PagePartitioningStrategy.ROW_WISE;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.sql.planner.SystemPartitioningHandle.SystemPartitionFunction.ROUND_ROBIN;
import static io.trino.type.IpAddressType.IPADDRESS;
import static java.lang.Math.toIntExact;
import static java.util.Collections.nCopies;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestPagePartitioner
{
    private static final DataSize MAX_MEMORY = DataSize.of(50, MEGABYTE);
    private static final DataSize PARTITION_MAX_MEMORY = DataSize.of(5, MEGABYTE);

    private static final int POSITIONS_PER_PAGE = 8;
    private static final int PARTITION_COUNT = 2;

    private static final PagesSerdeFactory PAGES_SERDE_FACTORY = new PagesSerdeFactory(new TestingBlockEncodingSerde(), false);
    private static final PageDeserializer PAGE_DESERIALIZER = PAGES_SERDE_FACTORY.createDeserializer(Optional.empty());

    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private TestOutputBuffer outputBuffer;

    @BeforeClass
    public void setUpClass()
    {
        executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(1, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));
    }

    @AfterClass(alwaysRun = true)
    public void tearDownClass()
    {
        executor.shutdownNow();
        executor = null;
        scheduledExecutor.shutdownNow();
        scheduledExecutor = null;
    }

    @BeforeMethod
    public void setUp()
    {
        outputBuffer = new TestOutputBuffer();
    }

    @Test
    public void testOutputForEmptyPage()
    {
        PagePartitioner pagePartitioner = pagePartitioner(BIGINT).build();
        Page page = new Page(createLongsBlock(ImmutableList.of()));

        pagePartitioner.partitionPage(page);
        pagePartitioner.close();

        List<Object> partitioned = readLongs(outputBuffer.getEnqueuedDeserialized(), 0);
        assertThat(partitioned).isEmpty();
    }

    @Test(dataProvider = "partitioningStrategy")
    public void testOutputEqualsInput(PagePartitioningStrategy partitioningStrategy)
    {
        PagePartitioner pagePartitioner = pagePartitioner(BIGINT)
                .withPartitioningStrategy(partitioningStrategy)
                .build();
        Page page = new Page(createLongSequenceBlock(0, POSITIONS_PER_PAGE));
        List<Object> expected = readLongs(Stream.of(page), 0);

        processPages(pagePartitioner, partitioningStrategy, page);

        List<Object> partitioned = readLongs(outputBuffer.getEnqueuedDeserialized(), 0);
        assertThat(partitioned).containsExactlyInAnyOrderElementsOf(expected); // order is different due to 2 partitions joined
    }

    @Test(dataProvider = "partitioningStrategy")
    public void testOutputForPageWithNoBlockPartitionFunction(PagePartitioningStrategy partitioningStrategy)
    {
        PagePartitioner pagePartitioner = pagePartitioner(BIGINT)
                .withPartitionFunction(new BucketPartitionFunction(
                        ROUND_ROBIN.createBucketFunction(null, false, PARTITION_COUNT, null),
                        IntStream.range(0, PARTITION_COUNT).toArray()))
                .withPartitionChannels(ImmutableList.of())
                .withPartitioningStrategy(partitioningStrategy)
                .build();
        Page page = new Page(createLongSequenceBlock(0, POSITIONS_PER_PAGE));

        processPages(pagePartitioner, partitioningStrategy, page);

        List<Object> partition0 = readLongs(outputBuffer.getEnqueuedDeserialized(0), 0);
        assertThat(partition0).containsExactly(0L, 2L, 4L, 6L);
        List<Object> partition1 = readLongs(outputBuffer.getEnqueuedDeserialized(1), 0);
        assertThat(partition1).containsExactly(1L, 3L, 5L, 7L);
    }

    @Test(dataProvider = "partitioningStrategy")
    public void testOutputForMultipleSimplePages(PagePartitioningStrategy partitioningStrategy)
    {
        PagePartitioner pagePartitioner = pagePartitioner(BIGINT)
                .withPartitioningStrategy(partitioningStrategy)
                .build();
        Page page1 = new Page(createLongSequenceBlock(0, POSITIONS_PER_PAGE));
        Page page2 = new Page(createLongSequenceBlock(1, POSITIONS_PER_PAGE));
        Page page3 = new Page(createLongSequenceBlock(2, POSITIONS_PER_PAGE));
        List<Object> expected = readLongs(Stream.of(page1, page2, page3), 0);

        processPages(pagePartitioner, partitioningStrategy, page1, page2, page3);

        List<Object> partitioned = readLongs(outputBuffer.getEnqueuedDeserialized(), 0);
        assertThat(partitioned).containsExactlyInAnyOrderElementsOf(expected); // order is different due to 2 partitions joined
    }

    @Test(dataProvider = "partitioningStrategy")
    public void testOutputForSimplePageWithReplication(PagePartitioningStrategy partitioningStrategy)
    {
        PagePartitioner pagePartitioner = pagePartitioner(BIGINT)
                .withPartitioningStrategy(partitioningStrategy)
                .replicate()
                .build();
        Page page = new Page(createLongsBlock(0L, 1L, 2L, 3L, null));

        processPages(pagePartitioner, partitioningStrategy, page);

        List<Object> partition0 = readLongs(outputBuffer.getEnqueuedDeserialized(0), 0);
        assertThat(partition0).containsExactly(0L, 2L, null);
        List<Object> partition1 = readLongs(outputBuffer.getEnqueuedDeserialized(1), 0);
        assertThat(partition1).containsExactly(0L, 1L, 3L); // position 0 copied to all partitions
    }

    @Test(dataProvider = "partitioningStrategy")
    public void testOutputForSimplePageWithNullChannel(PagePartitioningStrategy partitioningStrategy)
    {
        PagePartitioner pagePartitioner = pagePartitioner(BIGINT)
                .withPartitioningStrategy(partitioningStrategy)
                .withNullChannel(0)
                .build();
        Page page = new Page(createLongsBlock(0L, 1L, 2L, 3L, null));

        processPages(pagePartitioner, partitioningStrategy, page);

        List<Object> partition0 = readLongs(outputBuffer.getEnqueuedDeserialized(0), 0);
        assertThat(partition0).containsExactlyInAnyOrder(0L, 2L, null);
        List<Object> partition1 = readLongs(outputBuffer.getEnqueuedDeserialized(1), 0);
        assertThat(partition1).containsExactlyInAnyOrder(1L, 3L, null); // null copied to all partitions
    }

    @Test(dataProvider = "partitioningStrategy")
    public void testOutputForSimplePageWithPartitionConstant(PagePartitioningStrategy partitioningStrategy)
    {
        PagePartitioner pagePartitioner = pagePartitioner(BIGINT)
                .withPartitionConstants(ImmutableList.of(Optional.of(new NullableValue(BIGINT, 1L))))
                .withPartitionChannels(-1)
                .withPartitioningStrategy(partitioningStrategy)
                .build();
        Page page = new Page(createLongsBlock(0L, 1L, 2L, 3L, null));
        List<Object> allValues = readLongs(Stream.of(page), 0);

        processPages(pagePartitioner, partitioningStrategy, page);

        List<Object> partition0 = readLongs(outputBuffer.getEnqueuedDeserialized(0), 0);
        assertThat(partition0).isEmpty();
        List<Object> partition1 = readLongs(outputBuffer.getEnqueuedDeserialized(1), 0);
        assertThat(partition1).containsExactlyElementsOf(allValues);
    }

    @Test(dataProvider = "partitioningStrategy")
    public void testOutputForSimplePageWithPartitionConstantAndHashBlock(PagePartitioningStrategy partitioningStrategy)
    {
        PagePartitioner pagePartitioner = pagePartitioner(BIGINT)
                .withPartitionConstants(ImmutableList.of(Optional.empty(), Optional.of(new NullableValue(BIGINT, 1L))))
                .withPartitionChannels(0, -1) // use first block and constant block at index 1 as input to partitionFunction
                .withHashChannels(0, 1) // use both channels to calculate partition (a+b) mod 2
                .withPartitioningStrategy(partitioningStrategy)
                .build();
        Page page = new Page(createLongsBlock(0L, 1L, 2L, 3L));

        processPages(pagePartitioner, partitioningStrategy, page);

        List<Object> partition0 = readLongs(outputBuffer.getEnqueuedDeserialized(0), 0);
        assertThat(partition0).containsExactly(1L, 3L);
        List<Object> partition1 = readLongs(outputBuffer.getEnqueuedDeserialized(1), 0);
        assertThat(partition1).containsExactly(0L, 2L);
    }

    @Test(dataProvider = "partitioningStrategy")
    public void testPartitionPositionsWithRleNotNull(PagePartitioningStrategy partitioningStrategy)
    {
        PagePartitioner pagePartitioner = pagePartitioner(BIGINT, BIGINT)
                .withPartitioningStrategy(partitioningStrategy)
                .build();
        Page page = new Page(createRepeatedValuesBlock(0, POSITIONS_PER_PAGE), createLongSequenceBlock(0, POSITIONS_PER_PAGE));

        processPages(pagePartitioner, partitioningStrategy, page);

        List<Object> partition0 = readLongs(outputBuffer.getEnqueuedDeserialized(0), 1);
        assertThat(partition0).containsExactlyElementsOf(readLongs(Stream.of(page), 1));
        List<Object> partition0HashBlock = readLongs(outputBuffer.getEnqueuedDeserialized(0), 0);
        assertThat(partition0HashBlock).containsOnly(0L).hasSize(POSITIONS_PER_PAGE);
        assertThat(outputBuffer.getEnqueuedDeserialized(1)).isEmpty();
    }

    @Test(dataProvider = "partitioningStrategy")
    public void testPartitionPositionsWithRleNotNullWithReplication(PagePartitioningStrategy partitioningStrategy)
    {
        PagePartitioner pagePartitioner = pagePartitioner(BIGINT, BIGINT)
                .replicate()
                .withPartitioningStrategy(partitioningStrategy)
                .build();
        Page page = new Page(createRepeatedValuesBlock(0, POSITIONS_PER_PAGE), createLongSequenceBlock(0, POSITIONS_PER_PAGE));

        processPages(pagePartitioner, partitioningStrategy, page);

        List<Object> partition0 = readLongs(outputBuffer.getEnqueuedDeserialized(0), 1);
        assertThat(partition0).containsExactlyElementsOf(readLongs(Stream.of(page), 1));
        List<Object> partition1 = readLongs(outputBuffer.getEnqueuedDeserialized(1), 1);
        assertThat(partition1).containsExactly(0L); // position 0 copied to all partitions
    }

    @Test(dataProvider = "partitioningStrategy")
    public void testPartitionPositionsWithRleNullWithNullChannel(PagePartitioningStrategy partitioningStrategy)
    {
        PagePartitioner pagePartitioner = pagePartitioner(BIGINT, BIGINT)
                .withNullChannel(0)
                .withPartitioningStrategy(partitioningStrategy)
                .build();
        Page page = new Page(RunLengthEncodedBlock.create(createLongsBlock((Long) null), POSITIONS_PER_PAGE), createLongSequenceBlock(0, POSITIONS_PER_PAGE));

        processPages(pagePartitioner, partitioningStrategy, page);

        List<Object> partition0 = readLongs(outputBuffer.getEnqueuedDeserialized(0), 1);
        assertThat(partition0).containsExactlyElementsOf(readLongs(Stream.of(page), 1));
        List<Object> partition1 = readLongs(outputBuffer.getEnqueuedDeserialized(1), 1);
        assertThat(partition1).containsExactlyElementsOf(readLongs(Stream.of(page), 1));
    }

    @Test(dataProvider = "partitioningStrategy")
    public void testOutputForDictionaryBlock(PagePartitioningStrategy partitioningStrategy)
    {
        PagePartitioner pagePartitioner = pagePartitioner(BIGINT)
                .withPartitioningStrategy(partitioningStrategy)
                .build();
        Page page = new Page(createLongDictionaryBlock(0, 10)); // must have at least 10 position to have non-trivial dict

        processPages(pagePartitioner, partitioningStrategy, page);

        List<Object> partition0 = readLongs(outputBuffer.getEnqueuedDeserialized(0), 0);
        assertThat(partition0).containsExactlyElementsOf(nCopies(5, 0L));
        List<Object> partition1 = readLongs(outputBuffer.getEnqueuedDeserialized(1), 0);
        assertThat(partition1).containsExactlyElementsOf(nCopies(5, 1L));
    }

    @Test(dataProvider = "partitioningStrategy")
    public void testOutputForOneValueDictionaryBlock(PagePartitioningStrategy partitioningStrategy)
    {
        PagePartitioner pagePartitioner = pagePartitioner(BIGINT)
                .withPartitioningStrategy(partitioningStrategy)
                .build();
        Page page = new Page(DictionaryBlock.create(4, createLongsBlock(0), new int[] {0, 0, 0, 0}));

        processPages(pagePartitioner, partitioningStrategy, page);

        List<Object> partition0 = readLongs(outputBuffer.getEnqueuedDeserialized(0), 0);
        assertThat(partition0).containsExactlyElementsOf(nCopies(4, 0L));
        List<Object> partition1 = readLongs(outputBuffer.getEnqueuedDeserialized(1), 0);
        assertThat(partition1).isEmpty();
    }

    @Test(dataProvider = "partitioningStrategy")
    public void testOutputForViewDictionaryBlock(PagePartitioningStrategy partitioningStrategy)
    {
        PagePartitioner pagePartitioner = pagePartitioner(BIGINT)
                .withPartitioningStrategy(partitioningStrategy)
                .build();
        Page page = new Page(DictionaryBlock.create(4, createLongSequenceBlock(4, 8), new int[] {1, 0, 3, 2}));

        processPages(pagePartitioner, partitioningStrategy, page);

        List<Object> partition0 = readLongs(outputBuffer.getEnqueuedDeserialized(0), 0);
        assertThat(partition0).containsExactlyInAnyOrder(4L, 6L);
        List<Object> partition1 = readLongs(outputBuffer.getEnqueuedDeserialized(1), 0);
        assertThat(partition1).containsExactlyInAnyOrder(5L, 7L);
    }

    @Test(dataProvider = "typesWithPartitioningStrategy")
    public void testOutputForSimplePageWithType(Type type, PagePartitioningStrategy partitioningStrategy)
    {
        PagePartitioner pagePartitioner = pagePartitioner(BIGINT, type)
                .withPartitioningStrategy(partitioningStrategy)
                .build();
        Page page = new Page(
                createLongSequenceBlock(0, POSITIONS_PER_PAGE), // partition block
                createBlockForType(type, POSITIONS_PER_PAGE));
        List<Object> expected = readChannel(Stream.of(page), 1, type);

        processPages(pagePartitioner, partitioningStrategy, page);

        List<Object> partitioned = readChannel(outputBuffer.getEnqueuedDeserialized(), 1, type);
        assertThat(partitioned).containsExactlyInAnyOrderElementsOf(expected); // order is different due to 2 partitions joined
    }

    @Test(dataProvider = "typesWithPartitioningStrategy")
    public void testOutputForRowWisePreferredPages(Type type, PagePartitioningStrategy partitioningStrategy)
    {
        PagePartitioner pagePartitioner = pagePartitioner(BIGINT, type)
                .withPartitionFunction(new SumModuloPartitionFunction(POSITIONS_PER_PAGE, 0))
                .withPartitioningStrategy(partitioningStrategy)
                .build();

        // first page prefers row wise partitioning
        Page page1 = new Page(
                createLongSequenceBlock(0, POSITIONS_PER_PAGE),
                createBlockForType(type, POSITIONS_PER_PAGE));
        Page page2 = new Page(
                createLongSequenceBlock(0, POSITIONS_PER_PAGE),
                createBlockForType(type, POSITIONS_PER_PAGE));

        List<Object> expected = readChannel(Stream.of(page1, page2), 1, type);

        processPages(pagePartitioner, partitioningStrategy, page1, page2);

        List<Object> partitioned = readChannel(outputBuffer.getEnqueuedDeserialized(), 1, type);
        assertThat(partitioned).containsExactlyInAnyOrderElementsOf(expected); // order is different due to 2 partitions joined
    }

    @Test(dataProvider = "typesWithPartitioningStrategy")
    public void testOutputForMixedPages(Type type, PagePartitioningStrategy partitioningStrategy)
    {
        PagePartitioner pagePartitioner = pagePartitioner(BIGINT, type)
                .withPartitionFunction(new SumModuloPartitionFunction(POSITIONS_PER_PAGE, 0))
                .withPartitioningStrategy(partitioningStrategy)
                .build();

        // first page prefers row wise partitioning
        Page page1 = new Page(
                createLongSequenceBlock(0, POSITIONS_PER_PAGE),
                createBlockForType(type, POSITIONS_PER_PAGE));
        Page page2 = new Page(
                createLongSequenceBlock(0, POSITIONS_PER_PAGE * 4),
                createBlockForType(type, POSITIONS_PER_PAGE * 4));

        List<Object> expected = readChannel(Stream.of(page1, page2), 1, type);

        processPages(pagePartitioner, partitioningStrategy, page1, page2);

        List<Object> partitioned = readChannel(outputBuffer.getEnqueuedDeserialized(), 1, type);
        assertThat(partitioned).containsExactlyInAnyOrderElementsOf(expected); // order is different due to 2 partitions joined
    }

    @Test(dataProvider = "types")
    public void testOutputWithMixedRowWiseAndColumnarPartitioning(Type type)
    {
        testOutputEqualsInput(type, COLUMNAR, ROW_WISE);
        testOutputEqualsInput(type, ROW_WISE, COLUMNAR);
    }

    private void testOutputEqualsInput(Type type, PagePartitioningStrategy strategy1, PagePartitioningStrategy strategy2)
    {
        PagePartitionerBuilder pagePartitionerBuilder = pagePartitioner(BIGINT, type, type);
        Page input = new Page(
                createLongSequenceBlock(0, POSITIONS_PER_PAGE), // partition block
                createBlockForType(type, POSITIONS_PER_PAGE),
                createBlockForType(type, POSITIONS_PER_PAGE));

        List<Object> expected = readChannel(Stream.of(input, input), 1, type);

        PagePartitioner partitioner1 = pagePartitionerBuilder
                .withPartitioningStrategy(strategy1)
                .build();
        partitioner1.partitionPage(input);
        partitioner1.close();

        PagePartitioner partitioner2 = pagePartitionerBuilder
                .withPartitioningStrategy(strategy2)
                .build();
        partitioner2.partitionPage(input);
        partitioner2.close();

        List<Object> partitioned = readChannel(outputBuffer.getEnqueuedDeserialized(), 1, type);
        assertThat(partitioned).containsExactlyInAnyOrderElementsOf(expected); // output of the PagePartitioner can be reordered
        outputBuffer.clear();
    }

    @DataProvider(name = "partitioningStrategy")
    public static Object[][] partitioningStrategy()
    {
        return new Object[][] {{ROW_WISE}, {COLUMNAR}, {MOST_EFFICIENT_PER_PAGE}, {MOST_EFFICIENT_FOR_FIRST_PAGE}};
    }

    @DataProvider(name = "types")
    public static Object[][] types()
    {
        return getTypes().stream().map(type -> new Object[] {type}).toArray(Object[][]::new);
    }

    @DataProvider(name = "typesWithPartitioningStrategy")
    public static Object[][] typesWithPartitioningMode()
    {
        return getTypes().stream()
                .flatMap(type -> Stream.of(PagePartitioningStrategy.values())
                        .map(strategy -> new Object[] {type, strategy}))
                .toArray(Object[][]::new);
    }

    private static ImmutableList<AbstractType> getTypes()
    {
        return ImmutableList.of(
                BIGINT,
                BOOLEAN,
                INTEGER,
                createCharType(10),
                createUnboundedVarcharType(),
                DOUBLE,
                SMALLINT,
                TINYINT,
                UUID,
                VARBINARY,
                createDecimalType(1),
                createDecimalType(Decimals.MAX_SHORT_PRECISION + 1),
                new ArrayType(BIGINT),
                TimestampType.createTimestampType(9),
                TimestampType.createTimestampType(3),
                IPADDRESS);
    }

    private Block createBlockForType(Type type, int positionsPerPage)
    {
        return createRandomBlockForType(type, positionsPerPage, 0.2F);
    }

    private static void processPages(PagePartitioner pagePartitioner, PagePartitioningStrategy partitioningStrategy, Page... pages)
    {
        int columnarPreferred = 0;
        int rowWisePreferred = 0;
        PagePartitioningStrategy firstPagePreferred = null;
        for (Page page : pages) {
            if (pagePartitioner.isRowBasedPartitioningMoreEfficient(page)) {
                rowWisePreferred++;
                if (firstPagePreferred == null) {
                    firstPagePreferred = ROW_WISE;
                }
            }
            else {
                columnarPreferred++;
                if (firstPagePreferred == null) {
                    firstPagePreferred = COLUMNAR;
                }
            }
            pagePartitioner.partitionPage(page);
        }
        switch (partitioningStrategy) {
            case MOST_EFFICIENT_FOR_FIRST_PAGE -> {
                if (firstPagePreferred == COLUMNAR) {
                    assertThat(pagePartitioner.getPageBuildersSizeInBytes()).isZero();
                }
                else if (firstPagePreferred == ROW_WISE) {
                    assertThat(pagePartitioner.getPositionsAppendersSizeInBytes()).isZero();
                }
            }
            case MOST_EFFICIENT_PER_PAGE -> {
                if (columnarPreferred == 0) {
                    assertThat(pagePartitioner.getPositionsAppendersSizeInBytes()).isZero();
                }
                if (rowWisePreferred == 0) {
                    assertThat(pagePartitioner.getPageBuildersSizeInBytes()).isZero();
                }
            }
            case COLUMNAR -> assertThat(pagePartitioner.getPageBuildersSizeInBytes()).isZero();
            case ROW_WISE -> assertThat(pagePartitioner.getPositionsAppendersSizeInBytes()).isZero();
        }
        pagePartitioner.close();
    }

    private static List<Object> readLongs(Stream<Page> pages, int channel)
    {
        return readChannel(pages, channel, BIGINT);
    }

    private static List<Object> readChannel(Stream<Page> pages, int channel, Type type)
    {
        List<Object> result = new ArrayList<>();

        pages.forEach(page -> {
            Block block = page.getBlock(channel);
            for (int i = 0; i < block.getPositionCount(); i++) {
                if (block.isNull(i)) {
                    result.add(null);
                }
                else {
                    result.add(type.getObjectValue(null, block, i));
                }
            }
        });
        return unmodifiableList(result);
    }

    private PagePartitionerBuilder pagePartitioner(Type... types)
    {
        return pagePartitioner(ImmutableList.copyOf(types));
    }

    private PagePartitionerBuilder pagePartitioner(List<Type> types)
    {
        return pagePartitioner().withTypes(types);
    }

    private PagePartitionerBuilder pagePartitioner()
    {
        return new PagePartitionerBuilder(executor, scheduledExecutor, outputBuffer);
    }

    public static class PagePartitionerBuilder
    {
        public static final PositionsAppenderFactory POSITIONS_APPENDER_FACTORY = new PositionsAppenderFactory(new BlockTypeOperators());
        private final ExecutorService executor;
        private final ScheduledExecutorService scheduledExecutor;
        private final OutputBuffer outputBuffer;

        private ImmutableList<Integer> partitionChannels = ImmutableList.of(0);
        private List<Optional<NullableValue>> partitionConstants = ImmutableList.of();
        private PartitionFunction partitionFunction = new SumModuloPartitionFunction(PARTITION_COUNT, 0);
        private boolean shouldReplicate;
        private OptionalInt nullChannel = OptionalInt.empty();
        private List<Type> types;
        private PagePartitioningStrategy partitioningStrategy = MOST_EFFICIENT_PER_PAGE;

        PagePartitionerBuilder(ExecutorService executor, ScheduledExecutorService scheduledExecutor, OutputBuffer outputBuffer)
        {
            this.executor = requireNonNull(executor, "executor is null");
            this.scheduledExecutor = requireNonNull(scheduledExecutor, "scheduledExecutor is null");
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
        }

        public PagePartitionerBuilder withPartitionChannels(Integer... partitionChannels)
        {
            return withPartitionChannels(ImmutableList.copyOf(partitionChannels));
        }

        public PagePartitionerBuilder withPartitionChannels(ImmutableList<Integer> partitionChannels)
        {
            this.partitionChannels = partitionChannels;
            return this;
        }

        public PagePartitionerBuilder withPartitionConstants(List<Optional<NullableValue>> partitionConstants)
        {
            this.partitionConstants = partitionConstants;
            return this;
        }

        public PagePartitionerBuilder withHashChannels(int... hashChannels)
        {
            return withPartitionFunction(new SumModuloPartitionFunction(PARTITION_COUNT, hashChannels));
        }

        public PagePartitionerBuilder withPartitionFunction(PartitionFunction partitionFunction)
        {
            this.partitionFunction = partitionFunction;
            return this;
        }

        public PagePartitionerBuilder replicate()
        {
            return withShouldReplicate(true);
        }

        public PagePartitionerBuilder withShouldReplicate(boolean shouldReplicate)
        {
            this.shouldReplicate = shouldReplicate;
            return this;
        }

        public PagePartitionerBuilder withNullChannel(int nullChannel)
        {
            return withNullChannel(OptionalInt.of(nullChannel));
        }

        public PagePartitionerBuilder withNullChannel(OptionalInt nullChannel)
        {
            this.nullChannel = nullChannel;
            return this;
        }

        public PagePartitionerBuilder withTypes(Type... types)
        {
            return withTypes(ImmutableList.copyOf(types));
        }

        public PagePartitionerBuilder withTypes(List<Type> types)
        {
            this.types = types;
            return this;
        }

        public PagePartitionerBuilder withPartitioningStrategy(PagePartitioningStrategy partitioningStrategy)
        {
            this.partitioningStrategy = partitioningStrategy;
            return this;
        }

        public PartitionedOutputOperator buildPartitionedOutputOperator()
        {
            DriverContext driverContext = buildDriverContext();

            OutputFactory operatorFactory = new PartitionedOutputOperator.PartitionedOutputFactory(
                    partitionFunction,
                    partitionChannels,
                    partitionConstants,
                    shouldReplicate,
                    nullChannel,
                    outputBuffer,
                    PARTITION_MAX_MEMORY,
                    POSITIONS_APPENDER_FACTORY,
                    Optional.empty(),
                    newSimpleAggregatedMemoryContext(),
                    1,
                    partitioningStrategy);
            OperatorFactory factory = operatorFactory.createOutputOperator(0, new PlanNodeId("plan-node-0"), types, Function.identity(), PAGES_SERDE_FACTORY);
            PartitionedOutputOperator operator = (PartitionedOutputOperator) factory
                    .createOperator(driverContext);
            factory.noMoreOperators();
            return operator;
        }

        public PagePartitioner build()
        {
            DriverContext driverContext = buildDriverContext();

            OperatorContext operatorContext = driverContext.addOperatorContext(0, new PlanNodeId("plan-node-0"), PartitionedOutputOperator.class.getSimpleName());

            PagePartitioner pagePartitioner = new PagePartitioner(
                    partitionFunction,
                    partitionChannels,
                    partitionConstants,
                    shouldReplicate,
                    nullChannel,
                    outputBuffer,
                    PAGES_SERDE_FACTORY,
                    types,
                    PARTITION_MAX_MEMORY,
                    POSITIONS_APPENDER_FACTORY,
                    Optional.empty(),
                    newSimpleAggregatedMemoryContext(),
                    partitioningStrategy);
            pagePartitioner.setupOperator(operatorContext);

            return pagePartitioner;
        }

        private DriverContext buildDriverContext()
        {
            return TestingTaskContext.builder(executor, scheduledExecutor, TEST_SESSION)
                    .setMemoryPoolSize(MAX_MEMORY)
                    .build()
                    .addPipelineContext(0, true, true, false)
                    .addDriverContext();
        }
    }

    public static class TestOutputBuffer
            implements OutputBuffer
    {
        private final Multimap<Integer, Slice> enqueued = ArrayListMultimap.create();

        public Stream<Page> getEnqueuedDeserialized()
        {
            return getEnqueued().stream().map(PAGE_DESERIALIZER::deserialize);
        }

        public List<Slice> getEnqueued()
        {
            return ImmutableList.copyOf(enqueued.values());
        }

        public void clear()
        {
            enqueued.clear();
        }

        public Stream<Page> getEnqueuedDeserialized(int partition)
        {
            return getEnqueued(partition).stream().map(PAGE_DESERIALIZER::deserialize);
        }

        public List<Slice> getEnqueued(int partition)
        {
            Collection<Slice> serializedPages = enqueued.get(partition);
            return serializedPages == null ? ImmutableList.of() : ImmutableList.copyOf(serializedPages);
        }

        @Override
        public void enqueue(int partition, List<Slice> pages)
        {
            enqueued.putAll(partition, pages);
        }

        @Override
        public OutputBufferInfo getInfo()
        {
            return null;
        }

        @Override
        public BufferState getState()
        {
            return BufferState.NO_MORE_BUFFERS;
        }

        @Override
        public double getUtilization()
        {
            return 0;
        }

        @Override
        public OutputBufferStatus getStatus()
        {
            return OutputBufferStatus.initial();
        }

        @Override
        public void addStateChangeListener(StateMachine.StateChangeListener<BufferState> stateChangeListener)
        {
        }

        @Override
        public void setOutputBuffers(OutputBuffers newOutputBuffers)
        {
        }

        @Override
        public ListenableFuture<BufferResult> get(OutputBufferId bufferId, long token, DataSize maxSize)
        {
            return null;
        }

        @Override
        public void acknowledge(OutputBufferId bufferId, long token)
        {
        }

        @Override
        public void destroy(OutputBufferId bufferId)
        {
        }

        @Override
        public ListenableFuture<Void> isFull()
        {
            return null;
        }

        @Override
        public void enqueue(List<Slice> pages)
        {
        }

        @Override
        public void setNoMorePages()
        {
        }

        @Override
        public void destroy()
        {
        }

        @Override
        public void abort()
        {
        }

        @Override
        public long getPeakMemoryUsage()
        {
            return 0;
        }

        @Override
        public Optional<Throwable> getFailureCause()
        {
            return Optional.empty();
        }
    }

    private static class SumModuloPartitionFunction
            implements PartitionFunction
    {
        private final int[] hashChannels;
        private final int partitionCount;

        SumModuloPartitionFunction(int partitionCount, int... hashChannels)
        {
            checkArgument(partitionCount > 0);
            this.partitionCount = partitionCount;
            this.hashChannels = hashChannels;
        }

        @Override
        public int getPartitionCount()
        {
            return partitionCount;
        }

        @Override
        public int getPartition(Page page, int position)
        {
            long value = 0;
            for (int i = 0; i < hashChannels.length; i++) {
                value += page.getBlock(hashChannels[i]).getLong(position, 0);
            }

            return toIntExact(Math.abs(value) % partitionCount);
        }
    }
}
