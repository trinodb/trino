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
import io.trino.Session;
import io.trino.execution.StateMachine;
import io.trino.execution.buffer.BufferState;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.execution.buffer.PagesSerdeFactory;
import io.trino.execution.buffer.PartitionedOutputBuffer;
import io.trino.jmh.Benchmarks;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.memory.context.SimpleLocalMemoryContext;
import io.trino.operator.BucketPartitionFunction;
import io.trino.operator.DriverContext;
import io.trino.operator.OperatorFactories;
import io.trino.operator.OutputFactory;
import io.trino.operator.PartitionFunction;
import io.trino.operator.PrecomputedHashGenerator;
import io.trino.operator.TaskContext;
import io.trino.operator.TrinoOperatorFactories;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
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
import static io.trino.block.BlockAssertions.createLongDictionaryBlock;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.block.BlockAssertions.createRLEBlock;
import static io.trino.block.BlockAssertions.createRandomBlockForType;
import static io.trino.block.BlockAssertions.createRandomLongsBlock;
import static io.trino.execution.buffer.BufferState.OPEN;
import static io.trino.execution.buffer.BufferState.TERMINAL_BUFFER_STATES;
import static io.trino.execution.buffer.OutputBuffers.BufferType.PARTITIONED;
import static io.trino.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.operator.PageTestUtils.createRandomDictionaryPage;
import static io.trino.operator.PageTestUtils.createRandomPage;
import static io.trino.operator.PageTestUtils.createRandomRlePage;
import static io.trino.operator.output.BenchmarkPartitionedOutputOperator.BenchmarkData.TestType;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.Decimals.MAX_SHORT_PRECISION;
import static io.trino.testing.TestingSession.testSessionBuilder;
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
    private static final OperatorFactories OPERATOR_FACTORIES = new TrinoOperatorFactories();

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

        private final OperatorFactories operatorFactories;
        private final Session session;

        @Param({"2", "16", "256"})
        private int partitionCount = 256;

        @Param({"true", "false"})
        private boolean enableCompression;

        @Param({"1", "2"})
        private int channelCount = 1;

        @Param("8192")
        private int positionCount = DEFAULT_POSITION_COUNT;

        @Param({
                "BIGINT",
                "BIGINT_SKEWED_HASH",
                "DICTIONARY_BIGINT",
                "RLE_BIGINT",
                "BIGINT_PARTITION_CHANNEL_20_PERCENT",
                "BIGINT_DICTIONARY_PARTITION_CHANNEL_20_PERCENT",
                "BIGINT_DICTIONARY_PARTITION_CHANNEL_50_PERCENT",
                "BIGINT_DICTIONARY_PARTITION_CHANNEL_80_PERCENT",
                "BIGINT_DICTIONARY_PARTITION_CHANNEL_100_PERCENT",
                "RLE_PARTITION_BIGINT",
                "RLE_PARTITION_NULL_BIGINT",
                "LONG_DECIMAL",
                "INTEGER",
                "SMALLINT",
                "BOOLEAN",
                "VARCHAR",
                "ARRAY_BIGINT",
                "ARRAY_VARCHAR",
                "ARRAY_ARRAY_BIGINT",
                "MAP_BIGINT_BIGINT",
                "MAP_BIGINT_MAP_BIGINT_BIGINT",
                "ROW_BIGINT_BIGINT",
                "ROW_ARRAY_BIGINT_ARRAY_BIGINT"
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
            BIGINT_SKEWED_HASH(BigintType.BIGINT, 5000) {
                @Override
                public Page createPage(List<Type> types, int positionCount, float nullRate)
                {
                    return page(
                            positionCount,
                            types.size(),
                            () -> createRandomBlockForType(BigintType.BIGINT, positionCount, nullRate),
                            createRandomLongsBlock(positionCount, 2));
                }
            },
            DICTIONARY_BIGINT(BigintType.BIGINT, 3000) {
                @Override
                public Page createPage(List<Type> types, int positionCount, float nullRate)
                {
                    return createRandomDictionaryPage(types, positionCount, nullRate);
                }
            },
            RLE_BIGINT(BigintType.BIGINT, 3000) {
                @Override
                public Page createPage(List<Type> types, int positionCount, float nullRate)
                {
                    return createRandomRlePage(types, positionCount, nullRate);
                }
            },
            BIGINT_PARTITION_CHANNEL_20_PERCENT(BigintType.BIGINT, 3000) {
                @Override
                public Page createPage(List<Type> types, int positionCount, float nullRate)
                {
                    return page(
                            positionCount,
                            types.size(),
                            () -> createRandomBlockForType(BigintType.BIGINT, positionCount, nullRate),
                            createLongsBlock(LongStream.range(0, positionCount)
                                    .mapToObj(value -> value % (positionCount / 5))
                                    .collect(toImmutableList())));
                }
            },
            BIGINT_DICTIONARY_PARTITION_CHANNEL_20_PERCENT(BigintType.BIGINT, 3000) {
                @Override
                public Page createPage(List<Type> types, int positionCount, float nullRate)
                {
                    return page(
                            positionCount,
                            types.size(),
                            () -> createRandomBlockForType(BigintType.BIGINT, positionCount, nullRate),
                            createLongDictionaryBlock(0, positionCount, positionCount / 5));
                }
            },
            BIGINT_DICTIONARY_PARTITION_CHANNEL_50_PERCENT(BigintType.BIGINT, 3000) {
                @Override
                public Page createPage(List<Type> types, int positionCount, float nullRate)
                {
                    return page(
                            positionCount,
                            types.size(),
                            () -> createRandomBlockForType(BigintType.BIGINT, positionCount, nullRate),
                            createLongDictionaryBlock(0, positionCount, positionCount / 2));
                }
            },
            BIGINT_DICTIONARY_PARTITION_CHANNEL_80_PERCENT(BigintType.BIGINT, 3000) {
                @Override
                public Page createPage(List<Type> types, int positionCount, float nullRate)
                {
                    return page(
                            positionCount,
                            types.size(),
                            () -> createRandomBlockForType(BigintType.BIGINT, positionCount, nullRate),
                            createLongDictionaryBlock(0, positionCount, (int) (positionCount * 0.8)));
                }
            },
            BIGINT_DICTIONARY_PARTITION_CHANNEL_100_PERCENT(BigintType.BIGINT, 3000) {
                @Override
                public Page createPage(List<Type> types, int positionCount, float nullRate)
                {
                    return page(
                            positionCount,
                            types.size(),
                            () -> createRandomBlockForType(BigintType.BIGINT, positionCount, nullRate),
                            createLongDictionaryBlock(0, positionCount, positionCount));
                }
            },
            RLE_PARTITION_BIGINT(BigintType.BIGINT, 5000) {
                @Override
                public Page createPage(List<Type> types, int positionCount, float nullRate)
                {
                    return page(
                            positionCount,
                            types.size(),
                            () -> createRandomBlockForType(BigintType.BIGINT, positionCount, nullRate),
                            createRLEBlock(42, positionCount));
                }
            },
            RLE_PARTITION_NULL_BIGINT(BigintType.BIGINT, 20) {
                @Override
                public Page createPage(List<Type> types, int positionCount, float nullRate)
                {
                    return page(
                            positionCount,
                            types.size(),
                            () -> createRandomBlockForType(BigintType.BIGINT, positionCount, nullRate),
                            new RunLengthEncodedBlock(createLongsBlock((Long) null), positionCount));
                }

                @Override
                public OptionalInt getNullChannel()
                {
                    return OptionalInt.of(1);
                }
            },
            LONG_DECIMAL(createDecimalType(MAX_SHORT_PRECISION + 1), 5000),
            INTEGER(IntegerType.INTEGER, 5000),
            SMALLINT(SmallintType.SMALLINT, 5000),
            BOOLEAN(BooleanType.BOOLEAN, 5000),
            VARCHAR(VarcharType.VARCHAR, 5000),
            ARRAY_BIGINT(new ArrayType(BigintType.BIGINT), 1000),
            ARRAY_VARCHAR(new ArrayType(VarcharType.VARCHAR), 1000),
            ARRAY_ARRAY_BIGINT(new ArrayType(new ArrayType(BigintType.BIGINT)), 1000),
            MAP_BIGINT_BIGINT(createMapType(BigintType.BIGINT, BigintType.BIGINT), 1000),
            MAP_BIGINT_MAP_BIGINT_BIGINT(createMapType(BigintType.BIGINT, createMapType(BigintType.BIGINT, BigintType.BIGINT)), 1000),
            ROW_BIGINT_BIGINT(rowTypeWithDefaultFieldNames(ImmutableList.of(BigintType.BIGINT, BigintType.BIGINT)), 1000),
            ROW_ARRAY_BIGINT_ARRAY_BIGINT(rowTypeWithDefaultFieldNames(ImmutableList.of(new ArrayType(BigintType.BIGINT), new ArrayType(BigintType.BIGINT))), 1000);

            private final Type type;
            private final int pageCount;

            TestType(Type type, int pageCount)
            {
                this.type = requireNonNull(type, "type is null");
                this.pageCount = pageCount;
            }

            public Page createPage(List<Type> types, int positionCount, float nullRate)
            {
                return createRandomPage(types, positionCount, nullRate);
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
        }

        public BenchmarkData()
        {
            this(OPERATOR_FACTORIES, testSessionBuilder().build());
        }

        protected BenchmarkData(OperatorFactories operatorFactories, Session session)
        {
            this.operatorFactories = requireNonNull(operatorFactories, "operatorFactories is null");
            this.session = requireNonNull(session, "session is null");
        }

        public int getPageCount()
        {
            return pageCount;
        }

        public void setPageCount(int pageCount)
        {
            this.pageCount = pageCount;
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
            // We don't check blackhole is not null, because blackhole has to be injected by jmh (should not be created manually)
            // and in case of unit test it will be null
            this.blackhole = blackhole;
            types = type.getTypes(channelCount);
            dataPage = type.createPage(types, positionCount, nullRate);
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
            OutputBuffers buffers = createInitialEmptyOutputBuffers(PARTITIONED);
            for (int partition = 0; partition < partitionCount; partition++) {
                buffers = buffers.withBuffer(new OutputBuffers.OutputBufferId(partition), partition);
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
            TaskContext taskContext = createTaskContext();

            OutputFactory operatorFactory = operatorFactories.partitionedOutput(
                    taskContext,
                    partitionFunction,
                    ImmutableList.of(types.size() - 1), // hash block is at the last channel
                    ImmutableList.of(Optional.empty()),
                    false,
                    nullChannel,
                    buffer,
                    MAX_PARTITION_BUFFER_SIZE);
            return (PartitionedOutputOperator) operatorFactory
                    .createOutputOperator(0, new PlanNodeId("plan-node-0"), types, Function.identity(), serdeFactory)
                    .createOperator(createDriverContext(taskContext));
        }

        private DriverContext createDriverContext(TaskContext taskContext)
        {
            return taskContext
                    .addPipelineContext(0, true, true, false)
                    .addDriverContext();
        }

        private TaskContext createTaskContext()
        {
            return TestingTaskContext.builder(EXECUTOR, SCHEDULER, session).build();
        }

        private TestingPartitionedOutputBuffer createPartitionedBuffer(OutputBuffers buffers, DataSize dataSize)
        {
            return new TestingPartitionedOutputBuffer(
                    "task-instance-id",
                    new StateMachine<>("bufferState", SCHEDULER, OPEN, TERMINAL_BUFFER_STATES),
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
                    StateMachine<BufferState> state,
                    OutputBuffers outputBuffers,
                    DataSize maxBufferSize,
                    Supplier<LocalMemoryContext> memoryContextSupplier,
                    Executor notificationExecutor,
                    Blackhole blackhole)
            {
                super(taskInstanceId, state, outputBuffers, maxBufferSize, memoryContextSupplier, notificationExecutor);
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

    static {
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
                data.setup(null);
                data.setPageCount(1);
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
