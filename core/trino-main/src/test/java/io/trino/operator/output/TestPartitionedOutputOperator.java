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
import io.trino.Session;
import io.trino.execution.StateMachine;
import io.trino.execution.buffer.BufferResult;
import io.trino.execution.buffer.BufferState;
import io.trino.execution.buffer.OutputBuffer;
import io.trino.execution.buffer.OutputBufferInfo;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.execution.buffer.PagesSerde;
import io.trino.execution.buffer.PagesSerdeFactory;
import io.trino.operator.BucketPartitionFunction;
import io.trino.operator.DriverContext;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactories;
import io.trino.operator.OutputFactory;
import io.trino.operator.PartitionFunction;
import io.trino.operator.TaskContext;
import io.trino.operator.TrinoOperatorFactories;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.TestingBlockEncodingSerde;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.TestingTaskContext;
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
import static io.trino.block.BlockAssertions.createLongDictionaryBlock;
import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.block.BlockAssertions.createRLEBlock;
import static io.trino.block.BlockAssertions.createRandomBlockForType;
import static io.trino.execution.buffer.OutputBuffers.BufferType.PARTITIONED;
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
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.Math.toIntExact;
import static java.util.Collections.nCopies;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestPartitionedOutputOperator
{
    private static final OperatorFactories TRINO_OPERATOR_FACTORIES = new TrinoOperatorFactories();
    private static final Session TEST_SESSION = testSessionBuilder().build();
    private static final DataSize MAX_MEMORY = DataSize.of(50, MEGABYTE);
    private static final DataSize PARTITION_MAX_MEMORY = DataSize.of(5, MEGABYTE);

    private static final int POSITIONS_PER_PAGE = 8;
    private static final int PARTITION_COUNT = 2;

    private static final PagesSerdeFactory PAGES_SERDE_FACTORY = new PagesSerdeFactory(new TestingBlockEncodingSerde(), false);
    private static final PagesSerde PAGES_SERDE = PAGES_SERDE_FACTORY.createPagesSerde();

    private final Session testSession;
    private final OperatorFactories operatorFactories;

    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private TestOutputBuffer outputBuffer;

    public TestPartitionedOutputOperator()
    {
        this(TEST_SESSION, TRINO_OPERATOR_FACTORIES);
    }

    protected TestPartitionedOutputOperator(Session testSession, OperatorFactories operatorFactories)
    {
        this.testSession = testSession;
        this.operatorFactories = operatorFactories;
    }

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
    public void testOutputForSimplePage()
    {
        PartitionedOutputOperator partitionedOutputOperator = partitionedOutputOperator(BIGINT).build();
        Page page = new Page(createLongSequenceBlock(0, POSITIONS_PER_PAGE));
        List<Object> expected = readLongs(Stream.of(page), 0);

        processPages(partitionedOutputOperator, page);

        List<Object> partitioned = readLongs(outputBuffer.getEnqueuedDeserialized(), 0);
        assertThat(partitioned).containsExactlyInAnyOrderElementsOf(expected); // order is different due to 2 partitions joined
        OperatorContext operatorContext = partitionedOutputOperator.getOperatorContext();
        assertEquals(operatorContext.getOutputDataSize().getTotalCount(), page.getSizeInBytes());
        assertEquals(operatorContext.getOutputPositions().getTotalCount(), page.getPositionCount());
    }

    @Test
    public void testOutputForEmptyPage()
    {
        PartitionedOutputOperator partitionedOutputOperator = partitionedOutputOperator(BIGINT).build();
        Page page = new Page(createLongsBlock(ImmutableList.of()));

        processPages(partitionedOutputOperator, page);

        List<Object> partitioned = readLongs(outputBuffer.getEnqueuedDeserialized(), 0);
        assertThat(partitioned).isEmpty();
    }

    @Test
    public void testOutputForPageWithNoBlockPartitionFunction()
    {
        PartitionedOutputOperator partitionedOutputOperator = partitionedOutputOperator(BIGINT)
                .withPartitionFunction(new BucketPartitionFunction(
                        ROUND_ROBIN.createBucketFunction(null, false, PARTITION_COUNT, null),
                        IntStream.range(0, PARTITION_COUNT).toArray()))
                .withPartitionChannels(ImmutableList.of())
                .build();
        Page page = new Page(createLongSequenceBlock(0, POSITIONS_PER_PAGE));

        processPages(partitionedOutputOperator, page);

        List<Object> partition0 = readLongs(outputBuffer.getEnqueuedDeserialized(0), 0);
        assertThat(partition0).containsExactly(0L, 2L, 4L, 6L);
        List<Object> partition1 = readLongs(outputBuffer.getEnqueuedDeserialized(1), 0);
        assertThat(partition1).containsExactly(1L, 3L, 5L, 7L);
    }

    @Test
    public void testOutputForMultipleSimplePages()
    {
        PartitionedOutputOperator partitionedOutputOperator = partitionedOutputOperator(BIGINT).build();
        Page page1 = new Page(createLongSequenceBlock(0, POSITIONS_PER_PAGE));
        Page page2 = new Page(createLongSequenceBlock(1, POSITIONS_PER_PAGE));
        Page page3 = new Page(createLongSequenceBlock(2, POSITIONS_PER_PAGE));
        List<Object> expected = readLongs(Stream.of(page1, page2, page3), 0);

        processPages(partitionedOutputOperator, page1, page2, page3);

        List<Object> partitioned = readLongs(outputBuffer.getEnqueuedDeserialized(), 0);
        assertThat(partitioned).containsExactlyInAnyOrderElementsOf(expected); // order is different due to 2 partitions joined
    }

    @Test
    public void testOutputForSimplePageWithReplication()
    {
        PartitionedOutputOperator partitionedOutputOperator = partitionedOutputOperator(BIGINT).replicate().build();
        Page page = new Page(createLongsBlock(0L, 1L, 2L, 3L, null));

        processPages(partitionedOutputOperator, page);

        List<Object> partition0 = readLongs(outputBuffer.getEnqueuedDeserialized(0), 0);
        assertThat(partition0).containsExactly(0L, 2L, null);
        List<Object> partition1 = readLongs(outputBuffer.getEnqueuedDeserialized(1), 0);
        assertThat(partition1).containsExactly(0L, 1L, 3L); // position 0 copied to all partitions
    }

    @Test
    public void testOutputForSimplePageWithNullChannel()
    {
        PartitionedOutputOperator partitionedOutputOperator = partitionedOutputOperator(BIGINT).withNullChannel(0).build();
        Page page = new Page(createLongsBlock(0L, 1L, 2L, 3L, null));

        processPages(partitionedOutputOperator, page);

        List<Object> partition0 = readLongs(outputBuffer.getEnqueuedDeserialized(0), 0);
        assertThat(partition0).containsExactlyInAnyOrder(0L, 2L, null);
        List<Object> partition1 = readLongs(outputBuffer.getEnqueuedDeserialized(1), 0);
        assertThat(partition1).containsExactlyInAnyOrder(1L, 3L, null); // null copied to all partitions
    }

    @Test
    public void testOutputForSimplePageWithPartitionConstant()
    {
        PartitionedOutputOperator partitionedOutputOperator = partitionedOutputOperator(BIGINT)
                .withPartitionConstants(ImmutableList.of(Optional.of(new NullableValue(BIGINT, 1L))))
                .withPartitionChannels(-1)
                .build();
        Page page = new Page(createLongsBlock(0L, 1L, 2L, 3L, null));
        List<Object> allValues = readLongs(Stream.of(page), 0);

        processPages(partitionedOutputOperator, page);

        List<Object> partition0 = readLongs(outputBuffer.getEnqueuedDeserialized(0), 0);
        assertThat(partition0).isEmpty();
        List<Object> partition1 = readLongs(outputBuffer.getEnqueuedDeserialized(1), 0);
        assertThat(partition1).containsExactlyElementsOf(allValues);
    }

    @Test
    public void testOutputForSimplePageWithPartitionConstantAndHashBlock()
    {
        PartitionedOutputOperator partitionedOutputOperator = partitionedOutputOperator(BIGINT)
                .withPartitionConstants(ImmutableList.of(Optional.empty(), Optional.of(new NullableValue(BIGINT, 1L))))
                .withPartitionChannels(0, -1) // use first block and constant block at index 1 as input to partitionFunction
                .withHashChannels(0, 1) // use both channels to calculate partition (a+b) mod 2
                .build();
        Page page = new Page(createLongsBlock(0L, 1L, 2L, 3L));

        processPages(partitionedOutputOperator, page);

        List<Object> partition0 = readLongs(outputBuffer.getEnqueuedDeserialized(0), 0);
        assertThat(partition0).containsExactly(1L, 3L);
        List<Object> partition1 = readLongs(outputBuffer.getEnqueuedDeserialized(1), 0);
        assertThat(partition1).containsExactly(0L, 2L);
    }

    @Test
    public void testPartitionPositionsWithRleNotNull()
    {
        PartitionedOutputOperator partitionedOutputOperator = partitionedOutputOperator(BIGINT, BIGINT).build();
        Page page = new Page(createRLEBlock(0, POSITIONS_PER_PAGE), createLongSequenceBlock(0, POSITIONS_PER_PAGE));

        processPages(partitionedOutputOperator, page);

        List<Object> partition0 = readLongs(outputBuffer.getEnqueuedDeserialized(0), 1);
        assertThat(partition0).containsExactlyElementsOf(readLongs(Stream.of(page), 1));
        List<Object> partition0HashBlock = readLongs(outputBuffer.getEnqueuedDeserialized(0), 0);
        assertThat(partition0HashBlock).containsOnly(0L).hasSize(POSITIONS_PER_PAGE);
        assertThat(outputBuffer.getEnqueuedDeserialized(1)).isEmpty();
    }

    @Test
    public void testPartitionPositionsWithRleNotNullWithReplication()
    {
        PartitionedOutputOperator partitionedOutputOperator = partitionedOutputOperator(BIGINT, BIGINT).replicate().build();
        Page page = new Page(createRLEBlock(0, POSITIONS_PER_PAGE), createLongSequenceBlock(0, POSITIONS_PER_PAGE));

        processPages(partitionedOutputOperator, page);

        List<Object> partition0 = readLongs(outputBuffer.getEnqueuedDeserialized(0), 1);
        assertThat(partition0).containsExactlyElementsOf(readLongs(Stream.of(page), 1));
        List<Object> partition1 = readLongs(outputBuffer.getEnqueuedDeserialized(1), 1);
        assertThat(partition1).containsExactly(0L); // position 0 copied to all partitions
    }

    @Test
    public void testPartitionPositionsWithRleNullWithNullChannel()
    {
        PartitionedOutputOperator partitionedOutputOperator = partitionedOutputOperator(BIGINT, BIGINT).withNullChannel(0).build();
        Page page = new Page(new RunLengthEncodedBlock(createLongsBlock((Long) null), POSITIONS_PER_PAGE), createLongSequenceBlock(0, POSITIONS_PER_PAGE));

        processPages(partitionedOutputOperator, page);

        List<Object> partition0 = readLongs(outputBuffer.getEnqueuedDeserialized(0), 1);
        assertThat(partition0).containsExactlyElementsOf(readLongs(Stream.of(page), 1));
        List<Object> partition1 = readLongs(outputBuffer.getEnqueuedDeserialized(1), 1);
        assertThat(partition1).containsExactlyElementsOf(readLongs(Stream.of(page), 1));
    }

    @Test
    public void testOutputForDictionaryBlock()
    {
        PartitionedOutputOperator partitionedOutputOperator = partitionedOutputOperator(BIGINT).build();
        Page page = new Page(createLongDictionaryBlock(0, 10)); // must have at least 10 position to have non-trivial dict

        processPages(partitionedOutputOperator, page);

        List<Object> partition0 = readLongs(outputBuffer.getEnqueuedDeserialized(0), 0);
        assertThat(partition0).containsExactlyElementsOf(nCopies(5, 0L));
        List<Object> partition1 = readLongs(outputBuffer.getEnqueuedDeserialized(1), 0);
        assertThat(partition1).containsExactlyElementsOf(nCopies(5, 1L));
    }

    @Test
    public void testOutputForOneValueDictionaryBlock()
    {
        PartitionedOutputOperator partitionedOutputOperator = partitionedOutputOperator(BIGINT).build();
        Page page = new Page(new DictionaryBlock(createLongsBlock(0), new int[] {0, 0, 0, 0}));

        processPages(partitionedOutputOperator, page);

        List<Object> partition0 = readLongs(outputBuffer.getEnqueuedDeserialized(0), 0);
        assertThat(partition0).containsExactlyElementsOf(nCopies(4, 0L));
        List<Object> partition1 = readLongs(outputBuffer.getEnqueuedDeserialized(1), 0);
        assertThat(partition1).isEmpty();
    }

    @Test
    public void testOutputForViewDictionaryBlock()
    {
        PartitionedOutputOperator partitionedOutputOperator = partitionedOutputOperator(BIGINT).build();
        Page page = new Page(new DictionaryBlock(createLongSequenceBlock(4, 8), new int[] {1, 0, 3, 2}));

        processPages(partitionedOutputOperator, page);

        List<Object> partition0 = readLongs(outputBuffer.getEnqueuedDeserialized(0), 0);
        assertThat(partition0).containsExactlyInAnyOrder(4L, 6L);
        List<Object> partition1 = readLongs(outputBuffer.getEnqueuedDeserialized(1), 0);
        assertThat(partition1).containsExactlyInAnyOrder(5L, 7L);
    }

    @Test(dataProvider = "types")
    public void testOutputForSimplePageWithType(Type type)
    {
        PartitionedOutputOperator partitionedOutputOperator = partitionedOutputOperator(BIGINT, type).build();
        Page page = new Page(
                createLongSequenceBlock(0, POSITIONS_PER_PAGE), // partition block
                createBlockForType(type, POSITIONS_PER_PAGE));
        List<Object> expected = readChannel(Stream.of(page), 1, type);

        processPages(partitionedOutputOperator, page);

        List<Object> partitioned = readChannel(outputBuffer.getEnqueuedDeserialized(), 1, type);
        assertThat(partitioned).containsExactlyInAnyOrderElementsOf(expected); // order is different due to 2 partitions joined
    }

    @DataProvider(name = "types")
    public static Object[][] types()
    {
        return new Object[][]
                {
                        {BIGINT},
                        {BOOLEAN},
                        {INTEGER},
                        {createCharType(10)},
                        {createUnboundedVarcharType()},
                        {DOUBLE},
                        {SMALLINT},
                        {TINYINT},
                        {UUID},
                        {VARBINARY},
                        {createDecimalType(1)},
                        {createDecimalType(Decimals.MAX_SHORT_PRECISION + 1)},
                        {new ArrayType(BIGINT)}
                };
    }

    private Block createBlockForType(Type type, int positionsPerPage)
    {
        return createRandomBlockForType(type, positionsPerPage, 0.2F);
    }

    private static void processPages(PartitionedOutputOperator partitionedOutputOperator, Page... pages)
    {
        for (Page page : pages) {
            partitionedOutputOperator.addInput(page);
        }
        partitionedOutputOperator.finish();
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

    private PartitionedOutputOperatorBuilder partitionedOutputOperator(Type... types)
    {
        return partitionedOutputOperator(ImmutableList.copyOf(types));
    }

    private PartitionedOutputOperatorBuilder partitionedOutputOperator(List<Type> types)
    {
        return partitionedOutputOperator().withTypes(types);
    }

    private PartitionedOutputOperatorBuilder partitionedOutputOperator()
    {
        return new PartitionedOutputOperatorBuilder(operatorFactories, testSession, executor, scheduledExecutor, outputBuffer);
    }

    static class PartitionedOutputOperatorBuilder
    {
        private final OperatorFactories operatorFactories;
        private final Session testSession;
        private final ExecutorService executor;
        private final ScheduledExecutorService scheduledExecutor;
        private final OutputBuffer outputBuffer;

        private ImmutableList<Integer> partitionChannels = ImmutableList.of(0);
        private List<Optional<NullableValue>> partitionConstants = ImmutableList.of();
        private PartitionFunction partitionFunction = new SumModuloPartitionFunction(PARTITION_COUNT, 0);
        private boolean shouldReplicate;
        private OptionalInt nullChannel = OptionalInt.empty();
        private List<Type> types;

        PartitionedOutputOperatorBuilder(OperatorFactories operatorFactories, Session testSession, ExecutorService executor, ScheduledExecutorService scheduledExecutor, OutputBuffer outputBuffer)
        {
            this.operatorFactories = requireNonNull(operatorFactories, "operatorFactories is null");
            this.testSession = requireNonNull(testSession, "testSession is null");
            this.executor = requireNonNull(executor, "executor is null");
            this.scheduledExecutor = requireNonNull(scheduledExecutor, "scheduledExecutor is null");
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
        }

        public PartitionedOutputOperatorBuilder withPartitionChannels(Integer... partitionChannels)
        {
            return withPartitionChannels(ImmutableList.copyOf(partitionChannels));
        }

        public PartitionedOutputOperatorBuilder withPartitionChannels(ImmutableList<Integer> partitionChannels)
        {
            this.partitionChannels = partitionChannels;
            return this;
        }

        public PartitionedOutputOperatorBuilder withPartitionConstants(List<Optional<NullableValue>> partitionConstants)
        {
            this.partitionConstants = partitionConstants;
            return this;
        }

        public PartitionedOutputOperatorBuilder withHashChannels(int... hashChannels)
        {
            return withPartitionFunction(new SumModuloPartitionFunction(PARTITION_COUNT, hashChannels));
        }

        public PartitionedOutputOperatorBuilder withPartitionFunction(PartitionFunction partitionFunction)
        {
            this.partitionFunction = partitionFunction;
            return this;
        }

        public PartitionedOutputOperatorBuilder replicate()
        {
            return withShouldReplicate(true);
        }

        public PartitionedOutputOperatorBuilder withShouldReplicate(boolean shouldReplicate)
        {
            this.shouldReplicate = shouldReplicate;
            return this;
        }

        public PartitionedOutputOperatorBuilder withNullChannel(int nullChannel)
        {
            return withNullChannel(OptionalInt.of(nullChannel));
        }

        public PartitionedOutputOperatorBuilder withNullChannel(OptionalInt nullChannel)
        {
            this.nullChannel = nullChannel;
            return this;
        }

        public PartitionedOutputOperatorBuilder withTypes(List<Type> types)
        {
            this.types = types;
            return this;
        }

        public PartitionedOutputOperator build()
        {
            TaskContext taskContext = TestingTaskContext.builder(executor, scheduledExecutor, testSession)
                    .setMemoryPoolSize(MAX_MEMORY)
                    .build();
            DriverContext driverContext = taskContext
                    .addPipelineContext(0, true, true, false)
                    .addDriverContext();

            OutputBuffers buffers = OutputBuffers.createInitialEmptyOutputBuffers(PARTITIONED);
            for (int partition = 0; partition < PARTITION_COUNT; partition++) {
                buffers = buffers.withBuffer(new OutputBuffers.OutputBufferId(partition), partition);
            }

            OutputFactory operatorFactory = operatorFactories.partitionedOutput(
                    taskContext,
                    partitionFunction,
                    partitionChannels,
                    partitionConstants,
                    shouldReplicate,
                    nullChannel,
                    outputBuffer,
                    PARTITION_MAX_MEMORY);

            return (PartitionedOutputOperator) operatorFactory
                    .createOutputOperator(0, new PlanNodeId("plan-node-0"), types, Function.identity(), PAGES_SERDE_FACTORY)
                    .createOperator(driverContext);
        }
    }

    private static class TestOutputBuffer
            implements OutputBuffer
    {
        private final Multimap<Integer, Slice> enqueued = ArrayListMultimap.create();

        public Stream<Page> getEnqueuedDeserialized()
        {
            return getEnqueued().stream().map(PAGES_SERDE::deserialize);
        }

        public List<Slice> getEnqueued()
        {
            return ImmutableList.copyOf(enqueued.values());
        }

        public Stream<Page> getEnqueuedDeserialized(int partition)
        {
            return getEnqueued(partition).stream().map(PAGES_SERDE::deserialize);
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
        public boolean isOverutilized()
        {
            return false;
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
        public ListenableFuture<BufferResult> get(OutputBuffers.OutputBufferId bufferId, long token, DataSize maxSize)
        {
            return null;
        }

        @Override
        public void acknowledge(OutputBuffers.OutputBufferId bufferId, long token)
        {
        }

        @Override
        public void destroy(OutputBuffers.OutputBufferId bufferId)
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
        public void fail()
        {
        }

        @Override
        public long getPeakMemoryUsage()
        {
            return 0;
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
