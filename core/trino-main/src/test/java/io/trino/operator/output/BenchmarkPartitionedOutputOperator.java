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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.execution.buffer.OutputBufferStateMachine;
import io.trino.execution.buffer.PagesSerdeFactory;
import io.trino.execution.buffer.PartitionedOutputBuffer;
import io.trino.execution.buffer.PipelinedOutputBuffers;
import io.trino.execution.buffer.PipelinedOutputBuffers.OutputBufferId;
import io.trino.jmh.Benchmarks;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.memory.context.SimpleLocalMemoryContext;
import io.trino.operator.BucketPartitionFunction;
import io.trino.operator.DriverContext;
import io.trino.operator.PageTestUtils;
import io.trino.operator.PartitionFunction;
import io.trino.operator.PrecomputedHashGenerator;
import io.trino.operator.output.PartitionedOutputOperator.PartitionedOutputFactory;
import io.trino.spi.Page;
import io.trino.spi.QueryId;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.TestingBlockEncodingSerde;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;
import io.trino.sql.planner.HashBucketFunction;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.TestingTaskContext;
import io.trino.type.BlockTypeOperators;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.block.BlockAssertions.chooseNullPositions;
import static io.trino.block.BlockAssertions.createLongDictionaryBlock;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.block.BlockAssertions.createRandomBlockForType;
import static io.trino.block.BlockAssertions.createRandomLongsBlock;
import static io.trino.block.BlockAssertions.createRepeatedValuesBlock;
import static io.trino.execution.buffer.PipelinedOutputBuffers.BufferType.PARTITIONED;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.operator.output.BenchmarkPartitionedOutputOperator.BenchmarkData.TestType;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.Decimals.MAX_SHORT_PRECISION;
import static java.util.Collections.nCopies;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@State(Scope.Thread)
@OutputTimeUnit(MILLISECONDS)
@Fork(2)
@Warmup(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkPartitionedOutputOperator
{
    private static final PositionsAppenderFactory POSITIONS_APPENDER_FACTORY = new PositionsAppenderFactory(new BlockTypeOperators());

    @Benchmark
    public void addPage(BenchmarkData data)
    {
        PartitionedOutputOperator operator = data.createPartitionedOutputOperator();
        for (int i = 0; i < data.getPageCount(); i++) {
            operator.addInput(data.getDataPage());
        }
        operator.finish();
    }

    @Test
    public void verifyAddPage()
    {
        BenchmarkData data = new BenchmarkData();
        data.setup(null);
        new BenchmarkPartitionedOutputOperator().addPage(data);
    }

    @State(Scope.Thread)
    @SuppressWarnings("unused")
    public static class BenchmarkData
    {
        private static final int DEFAULT_POSITION_COUNT = 8192;
        private static final DataSize MAX_PARTITION_BUFFER_SIZE = DataSize.of(256, MEGABYTE);
        private static final ExecutorService EXECUTOR = newCachedThreadPool(daemonThreadsNamed("BenchmarkPartitionedOutputOperator-executor-%s"));
        private static final ScheduledExecutorService SCHEDULER = newScheduledThreadPool(1, daemonThreadsNamed("BenchmarkPartitionedOutputOperator-scheduledExecutor-%s"));

        @Param({"2", "16", "256"})
        private int partitionCount = 256;

        @Param({"true", "false"})
        private boolean enableCompression;

        @Param({"1", "2"})
        private int channelCount = 1;

        @Param("8192")
        private int positionCount = DEFAULT_POSITION_COUNT;

        @Param({
                // Flat BIGINT data channel, flat BIGINT partition channel.
                "BIGINT",
                // Flat BIGINT data channel, flat BIGINT partition channel with only 2 values.
                "BIGINT_PARTITION_CHANNEL_SKEWED",
                // Dictionary BIGINT data channel, flat BIGINT partition channel.
                "DICTIONARY_BIGINT",
                // Rle BIGINT data channel, flat BIGINT partition channel.
                "RLE_BIGINT",
                // Flat BIGINT data channel, flat BIGINT partition channel with number of distinct values equal to 20% of data page size.
                "BIGINT_PARTITION_CHANNEL_20_PERCENT",
                // Flat BIGINT data channel, dictionary BIGINT partition channel with dictionary size equal to 20% of data page size.
                // To be compared with BIGINT_PARTITION_CHANNEL_20_PERCENT.
                "BIGINT_PARTITION_CHANNEL_DICTIONARY_20_PERCENT",
                // Flat BIGINT data channel, dictionary BIGINT partition channel with dictionary size equal to 50% of data page size.
                "BIGINT_PARTITION_CHANNEL_DICTIONARY_50_PERCENT",
                // Flat BIGINT data channel, dictionary BIGINT partition channel with dictionary size equal to 80% of data page size.
                "BIGINT_PARTITION_CHANNEL_DICTIONARY_80_PERCENT",
                // Flat BIGINT data channel, dictionary BIGINT partition channel with dictionary size equal to data page size.
                "BIGINT_PARTITION_CHANNEL_DICTIONARY_100_PERCENT",
                // Flat BIGINT data channel, dictionary BIGINT partition channel with dictionary size equal to data page size - 1.
                // To be compared with BIGINT_PARTITION_CHANNEL_DICTIONARY_100_PERCENT.
                "BIGINT_PARTITION_CHANNEL_DICTIONARY_100_PERCENT_MINUS_1",
                // Flat BIGINT data channel, rle BIGINT partition channel with not null value.
                "BIGINT_PARTITION_CHANNEL_RLE",
                // Flat BIGINT data channel, rle BIGINT partition channel with null value.
                "BIGINT_PARTITION_CHANNEL_RLE_NULL",
                // Flat LONG_DECIMAL data channel, flat BIGINT partition channel.
                "LONG_DECIMAL",
                // Dictionary LONG_DECIMAL data channel, flat BIGINT partition channel.
                "DICTIONARY_LONG_DECIMAL",
                // Flat INTEGER data channel, flat BIGINT partition channel.
                "INTEGER",
                // Dictionary INTEGER data channel, flat BIGINT partition channel.
                "DICTIONARY_INTEGER",
                // Flat SMALLINT data channel, flat BIGINT partition channel.
                "SMALLINT",
                // Dictionary SMALLINT data channel, flat BIGINT partition channel.
                "DICTIONARY_SMALLINT",
                // Flat BOOLEAN data channel, flat BIGINT partition channel.
                "BOOLEAN",
                // Dictionary BOOLEAN data channel, flat BIGINT partition channel.
                "DICTIONARY_BOOLEAN",
                // Flat VARCHAR data channel, flat BIGINT partition channel.
                "VARCHAR",
                // Dictionary VARCHAR data channel, flat BIGINT partition channel.
                "DICTIONARY_VARCHAR",
                // Flat array of BIGINT data channel, flat BIGINT partition channel.
                "ARRAY_BIGINT",
                // Flat array of VARCHAR data channel, flat BIGINT partition channel.
                "ARRAY_VARCHAR",
                // Flat array of array of BIGINT data channel, flat BIGINT partition channel.
                "ARRAY_ARRAY_BIGINT",
                // Flat map<BIGINT, BIGINT> data channel, flat BIGINT partition channel.
                "MAP_BIGINT_BIGINT",
                // Flat map<BIGINT map<BIGINT, BIGINT>> data channel, flat BIGINT partition channel.
                "MAP_BIGINT_MAP_BIGINT_BIGINT",
                // Flat RowType with two BIGINT fields data channel, flat BIGINT partition channel.
                "ROW_BIGINT_BIGINT",
                // Flat RowType with BIGINT and array of BIGINT fields data channel, flat BIGINT partition channel.
                "ROW_ARRAY_BIGINT_ARRAY_BIGINT",
                // Flat RowType with rle BIGINT and flat BIGINT fields data channel, flat BIGINT partition channel.
                "ROW_RLE_BIGINT_BIGINT",
        })
        private TestType type = TestType.BIGINT;

        @Param({"0", "0.2"})
        private float nullRate = 0.2F;

        private OptionalInt nullChannel;

        private List<Type> types;
        private int pageCount;
        private Page dataPage;
        private Blackhole blackhole;

        public enum TestType
        {
            BIGINT(BigintType.BIGINT, 5000),
            BIGINT_PARTITION_CHANNEL_SKEWED(BigintType.BIGINT, 5000, (types, positionCount, nullRate) -> {
                return page(
                        positionCount,
                        types.size(),
                        () -> createRandomBlockForType(BigintType.BIGINT, positionCount, nullRate),
                        createRandomLongsBlock(positionCount, 2));
            }),
            DICTIONARY_BIGINT(BigintType.BIGINT, 5000, PageTestUtils::createRandomDictionaryPage),
            RLE_BIGINT(BigintType.BIGINT, 3000, PageTestUtils::createRandomRlePage),
            BIGINT_PARTITION_CHANNEL_20_PERCENT(BigintType.BIGINT, 3000, (types, positionCount, nullRate) -> {
                return page(
                        positionCount,
                        types.size(),
                        () -> createRandomBlockForType(BigintType.BIGINT, positionCount, nullRate),
                        createLongsBlock(LongStream.range(0, positionCount)
                                .mapToObj(value -> value % (positionCount / 5))
                                .collect(toImmutableList())));
            }),
            BIGINT_PARTITION_CHANNEL_DICTIONARY_20_PERCENT(BigintType.BIGINT, 3000, (types, positionCount, nullRate) ->
                    createDictionaryPartitionChannelPage(types, positionCount, nullRate, positionCount / 5)),
            BIGINT_PARTITION_CHANNEL_DICTIONARY_50_PERCENT(BigintType.BIGINT, 3000, (types, positionCount, nullRate) ->
                    createDictionaryPartitionChannelPage(types, positionCount, nullRate, positionCount / 2)),
            BIGINT_PARTITION_CHANNEL_DICTIONARY_80_PERCENT(BigintType.BIGINT, 3000, (types, positionCount, nullRate) ->
                    createDictionaryPartitionChannelPage(types, positionCount, nullRate, (int) (positionCount * 0.8))),
            BIGINT_PARTITION_CHANNEL_DICTIONARY_100_PERCENT(BigintType.BIGINT, 3000, (types, positionCount, nullRate) ->
                    createDictionaryPartitionChannelPage(types, positionCount, nullRate, positionCount)),
            BIGINT_PARTITION_CHANNEL_DICTIONARY_100_PERCENT_MINUS_1(BigintType.BIGINT, 3000, (types, positionCount, nullRate) ->
                    createDictionaryPartitionChannelPage(types, positionCount, nullRate, positionCount - 1)),
            BIGINT_PARTITION_CHANNEL_RLE(BigintType.BIGINT, 5000, (types, positionCount, nullRate) -> {
                return page(
                        positionCount,
                        types.size(),
                        () -> createRandomBlockForType(BigintType.BIGINT, positionCount, nullRate),
                        createRepeatedValuesBlock(42, positionCount));
            }),
            BIGINT_PARTITION_CHANNEL_RLE_NULL(BigintType.BIGINT, 20, (types, positionCount, nullRate) -> {
                return page(
                        positionCount,
                        types.size(),
                        () -> createRandomBlockForType(BigintType.BIGINT, positionCount, nullRate),
                        RunLengthEncodedBlock.create(createLongsBlock((Long) null), positionCount));
            }),
            LONG_DECIMAL(createDecimalType(MAX_SHORT_PRECISION + 1), 5000),
            DICTIONARY_LONG_DECIMAL(createDecimalType(MAX_SHORT_PRECISION + 1), 5000, PageTestUtils::createRandomDictionaryPage),
            INTEGER(IntegerType.INTEGER, 5000),
            DICTIONARY_INTEGER(IntegerType.INTEGER, 5000, PageTestUtils::createRandomDictionaryPage),
            SMALLINT(SmallintType.SMALLINT, 5000),
            DICTIONARY_SMALLINT(SmallintType.SMALLINT, 5000, PageTestUtils::createRandomDictionaryPage),
            BOOLEAN(BooleanType.BOOLEAN, 5000),
            DICTIONARY_BOOLEAN(BooleanType.BOOLEAN, 5000, PageTestUtils::createRandomDictionaryPage),
            VARCHAR(VarcharType.VARCHAR, 5000),
            DICTIONARY_VARCHAR(VarcharType.VARCHAR, 5000, PageTestUtils::createRandomDictionaryPage),
            ARRAY_BIGINT(new ArrayType(BigintType.BIGINT), 1000),
            ARRAY_VARCHAR(new ArrayType(VarcharType.VARCHAR), 1000),
            ARRAY_ARRAY_BIGINT(new ArrayType(new ArrayType(BigintType.BIGINT)), 1000),
            MAP_BIGINT_BIGINT(createMapType(BigintType.BIGINT, BigintType.BIGINT), 1000),
            MAP_BIGINT_MAP_BIGINT_BIGINT(createMapType(BigintType.BIGINT, createMapType(BigintType.BIGINT, BigintType.BIGINT)), 1000),
            ROW_BIGINT_BIGINT(rowTypeWithDefaultFieldNames(ImmutableList.of(BigintType.BIGINT, BigintType.BIGINT)), 1000),
            ROW_ARRAY_BIGINT_ARRAY_BIGINT(rowTypeWithDefaultFieldNames(ImmutableList.of(new ArrayType(BigintType.BIGINT), new ArrayType(BigintType.BIGINT))), 1000),
            ROW_RLE_BIGINT_BIGINT(rowTypeWithDefaultFieldNames(ImmutableList.of(BigintType.BIGINT, BigintType.BIGINT)), 1000, (types, positionCount, nullRate) -> {
                return PageTestUtils.createPage(
                        types,
                        positionCount,
                        Optional.of(ImmutableList.of(0)),
                        types.stream()
                                .map(type -> {
                                    boolean[] isNull = null;
                                    int nullPositionCount = 0;
                                    if (nullRate > 0) {
                                        isNull = new boolean[positionCount];
                                        Set<Integer> nullPositions = chooseNullPositions(positionCount, nullRate);
                                        for (int nullPosition : nullPositions) {
                                            isNull[nullPosition] = true;
                                        }
                                        nullPositionCount = nullPositions.size();
                                    }

                                    int notNullPositionsCount = positionCount - nullPositionCount;
                                    return RowBlock.fromFieldBlocks(
                                            positionCount,
                                            Optional.ofNullable(isNull),
                                            new Block[] {
                                                    RunLengthEncodedBlock.create(createLongsBlock(-65128734213L), notNullPositionsCount),
                                                    createRandomLongsBlock(notNullPositionsCount, nullRate)});
                                })
                                .collect(toImmutableList()));
            });

            private final Type type;
            private final int pageCount;

            private final PageGenerator pageGenerator;

            TestType(Type type, int pageCount)
            {
                this(type, pageCount, PageTestUtils::createRandomPage);
            }

            TestType(Type type, int pageCount, PageGenerator pageGenerator)
            {
                this.type = requireNonNull(type, "type is null");
                this.pageCount = pageCount;
                this.pageGenerator = requireNonNull(pageGenerator, "pageGenerator is null");
            }

            public PageGenerator getPageGenerator()
            {
                return pageGenerator;
            }

            public int getPageCount()
            {
                return pageCount;
            }

            public OptionalInt getNullChannel()
            {
                return OptionalInt.empty();
            }

            public List<Type> getTypes(int channelCount)
            {
                return nCopies(channelCount, type);
            }

            interface PageGenerator
            {
                Page createPage(List<Type> types, int positionCount, float nullRate);
            }

            private static Page createDictionaryPartitionChannelPage(List<Type> types, int positionCount, float nullRate, int dictionarySize)
            {
                return page(
                        positionCount,
                        types.size(),
                        () -> createRandomBlockForType(types.get(0), positionCount, nullRate),
                        createLongDictionaryBlock(0, positionCount, dictionarySize));
            }
        }

        public int getPageCount()
        {
            return pageCount;
        }

        public void setPageCount(int pageCount)
        {
            this.pageCount = pageCount;
        }

        public void setPartitionCount(int partitionCount)
        {
            this.partitionCount = partitionCount;
        }

        public void setPositionCount(int positionCount)
        {
            this.positionCount = positionCount;
        }

        public void setType(TestType type)
        {
            this.type = requireNonNull(type, "type is null");
        }

        public Page getDataPage()
        {
            return dataPage;
        }

        @Setup
        public void setup(Blackhole blackhole)
        {
            setupData(blackhole);
            pollute();
        }

        private void setupData(Blackhole blackhole)
        {
            // We don't check blackhole is not null, because blackhole has to be injected by jmh (should not be created manually)
            // and in case of unit test it will be null
            this.blackhole = blackhole;
            types = type.getTypes(channelCount);
            dataPage = type.getPageGenerator().createPage(types, positionCount, nullRate);
            pageCount = type.getPageCount();
            nullChannel = type.getNullChannel();
            types = ImmutableList.<Type>builder()
                    .addAll(types)
                    .add(BIGINT) // dataPage has pre-computed hash block at the last channel
                    .build();
        }

        private static Page page(int positionCount, int channelCount, Supplier<Block> standardBlock, Block partitionBlock)
        {
            ImmutableList.Builder<Block> blocks = ImmutableList.builder();
            for (int i = 0; i < channelCount; i++) {
                blocks.add(standardBlock.get());
            }
            blocks.add(partitionBlock);
            return new Page(
                    positionCount,
                    blocks.build().toArray(new Block[0]));
        }

        private PartitionedOutputBuffer createPartitionedOutputBuffer()
        {
            PipelinedOutputBuffers buffers = PipelinedOutputBuffers.createInitial(PARTITIONED);
            for (int partition = 0; partition < partitionCount; partition++) {
                buffers = buffers.withBuffer(new OutputBufferId(partition), partition);
            }

            return createPartitionedBuffer(
                    buffers.withNoMoreBufferIds(),
                    DataSize.of(Long.MAX_VALUE, BYTE));
        }

        private PartitionedOutputOperator createPartitionedOutputOperator()
        {
            PartitionFunction partitionFunction = new BucketPartitionFunction(
                    new HashBucketFunction(new PrecomputedHashGenerator(0), partitionCount),
                    IntStream.range(0, partitionCount).toArray());
            PagesSerdeFactory serdeFactory = new PagesSerdeFactory(new TestingBlockEncodingSerde(), enableCompression);

            PartitionedOutputBuffer buffer = createPartitionedOutputBuffer();

            PartitionedOutputFactory operatorFactory = new PartitionedOutputFactory(
                    partitionFunction,
                    ImmutableList.of(types.size() - 1), // hash block is at the last channel
                    ImmutableList.of(Optional.empty()),
                    false,
                    OptionalInt.empty(),
                    buffer,
                    MAX_PARTITION_BUFFER_SIZE,
                    POSITIONS_APPENDER_FACTORY,
                    Optional.empty(),
                    newSimpleAggregatedMemoryContext(),
                    0,
                    Optional.empty());
            return (PartitionedOutputOperator) operatorFactory
                    .createOutputOperator(0, new PlanNodeId("plan-node-0"), types, Function.identity(), serdeFactory)
                    .createOperator(createDriverContext());
        }

        private DriverContext createDriverContext()
        {
            return TestingTaskContext.builder(EXECUTOR, SCHEDULER, TEST_SESSION)
                    .build()
                    .addPipelineContext(0, true, true, false)
                    .addDriverContext();
        }

        private TestingPartitionedOutputBuffer createPartitionedBuffer(PipelinedOutputBuffers buffers, DataSize dataSize)
        {
            return new TestingPartitionedOutputBuffer(
                    "task-instance-id",
                    new OutputBufferStateMachine(new TaskId(new StageId(new QueryId("query"), 0), 0, 0), SCHEDULER),
                    buffers,
                    dataSize,
                    () -> new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                    SCHEDULER,
                    blackhole);
        }

        private static class TestingPartitionedOutputBuffer
                extends PartitionedOutputBuffer
        {
            private final Blackhole blackhole;

            public TestingPartitionedOutputBuffer(
                    String taskInstanceId,
                    OutputBufferStateMachine stateMachine,
                    PipelinedOutputBuffers outputBuffers,
                    DataSize maxBufferSize,
                    Supplier<LocalMemoryContext> memoryContextSupplier,
                    Executor notificationExecutor,
                    Blackhole blackhole)
            {
                super(taskInstanceId, stateMachine, outputBuffers, maxBufferSize, memoryContextSupplier, notificationExecutor);
                this.blackhole = blackhole;
            }

            // Use a dummy enqueue method to avoid OutOfMemory error
            @Override
            public void enqueue(int partitionNumber, List<Slice> pages)
            {
                // The blackhole will be null only for not benchmark runs (test and profile pollution).
                // For the benchmarks, the instance will be provided by jmh infra via setup method.
                if (blackhole != null) {
                    blackhole.consume(pages);
                }
            }
        }
    }

    private static RowType rowTypeWithDefaultFieldNames(List<Type> types)
    {
        List<RowType.Field> fields = new ArrayList<>();
        for (int i = 0; i < types.size(); i++) {
            fields.add(new RowType.Field(Optional.of("field" + i), types.get(i)));
        }

        fields = unmodifiableList(fields);
        return RowType.from(fields);
    }

    private static MapType createMapType(Type keyType, Type valueType)
    {
        return new MapType(
                keyType,
                valueType,
                new TypeOperators());
    }

    private static void pollute()
    {
        try {
            List<TestType> types = List.of(
                    TestType.BIGINT,
                    TestType.DICTIONARY_BIGINT,
                    TestType.RLE_BIGINT,
                    TestType.LONG_DECIMAL,
                    TestType.INTEGER,
                    TestType.SMALLINT,
                    TestType.BOOLEAN,
                    TestType.VARCHAR,
                    TestType.ARRAY_BIGINT);
            // use multiple types to pollute the profile
            BenchmarkPartitionedOutputOperator benchmark = new BenchmarkPartitionedOutputOperator();
            types.forEach(type -> {
                BenchmarkData data = new BenchmarkData();
                data.setType(type);
                data.setupData(null);
                data.setPageCount(1);
                benchmark.addPage(data);
                // pollute row-wise processing
                data = new BenchmarkData();
                data.setType(type);
                data.setPartitionCount(256);
                data.setPositionCount(256);
                data.setupData(null);
                data.setPageCount(50);
                benchmark.addPage(data);
            });
        }
        catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        Benchmarks.benchmark(BenchmarkPartitionedOutputOperator.class)
                .withOptions(optionsBuilder -> optionsBuilder.jvmArgs("-Xmx16g"))
                .run();
    }
}
