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
package io.trino.operator.partition;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.execution.StateMachine;
import io.trino.execution.buffer.BufferState;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.execution.buffer.PagesSerdeFactory;
import io.trino.execution.buffer.PartitionedOutputBuffer;
import io.trino.execution.buffer.SerializedPage;
import io.trino.jmh.Benchmarks;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.memory.context.SimpleLocalMemoryContext;
import io.trino.operator.DriverContext;
import io.trino.operator.InterpretedHashGenerator;
import io.trino.operator.OperatorFactories;
import io.trino.operator.OutputFactory;
import io.trino.operator.PartitionFunction;
import io.trino.operator.TaskContext;
import io.trino.operator.TrinoOperatorFactories;
import io.trino.operator.exchange.LocalPartitionGenerator;
import io.trino.spi.Page;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
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

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.Session.SessionBuilder;
import static io.trino.execution.buffer.BufferState.OPEN;
import static io.trino.execution.buffer.BufferState.TERMINAL_BUFFER_STATES;
import static io.trino.execution.buffer.OutputBuffers.BufferType.PARTITIONED;
import static io.trino.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.operator.PageTestUtils.createDictionaryPageWithRandomData;
import static io.trino.operator.PageTestUtils.createPageWithRandomData;
import static io.trino.operator.PageTestUtils.createRlePageWithRandomData;
import static io.trino.operator.PageTestUtils.createUpdatedBlockTypesWithHashBlockAndNullBlock;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.Decimals.MAX_SHORT_PRECISION;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TestingTypeManager.createMapType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Collections.nCopies;
import static java.util.Collections.unmodifiableList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@State(Scope.Thread)
@OutputTimeUnit(MILLISECONDS)
@Fork(3)
@Warmup(iterations = 5, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkPartitionedOutputOperator
{
    private static final OperatorFactories OPERATOR_FACTORIES = new TrinoOperatorFactories();

    @Benchmark
    public void addPage(BenchmarkData data)
    {
        addPage(data, OPERATOR_FACTORIES, testSessionBuilder());
    }

    protected void addPage(BenchmarkData data, OperatorFactories operatorFactories, SessionBuilder sessionBuilder)
    {
        PartitionedOutputOperator operator = data.createPartitionedOutputOperator(operatorFactories, sessionBuilder);
        for (int i = 0; i < data.pageCount; i++) {
            operator.addInput(data.dataPage);
        }
        operator.finish();
    }

    @Test
    public void verifyAddPage()
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkPartitionedOutputOperator().addPage(data);
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private static final int PARTITION_COUNT = 256;
        private static final int POSITION_COUNT = 8192;
        private static final DataSize MAX_PARTITION_BUFFER_SIZE = DataSize.of(256, MEGABYTE);
        private static final ExecutorService EXECUTOR = newCachedThreadPool(daemonThreadsNamed("BenchmarkPartitionedOutputOperator-executor-%s"));
        private static final ScheduledExecutorService SCHEDULER = newScheduledThreadPool(1, daemonThreadsNamed("BenchmarkPartitionedOutputOperator-scheduledExecutor-%s"));

        @SuppressWarnings("unused")
        @Param({"true", "false"})
        private boolean enableCompression;

        @Param({"1", "2"})
        private int channelCount = 1;

        @Param({
                "BIGINT",
                "DICTIONARY(BIGINT)",
                "RLE(BIGINT)",
                "LONG_DECIMAL",
                "INTEGER",
                "SMALLINT",
                "BOOLEAN",
                "VARCHAR",
                "ARRAY(BIGINT)",
                "ARRAY(VARCHAR)",
                "ARRAY(ARRAY(BIGINT))",
                "MAP(BIGINT,BIGINT)",
                "MAP(BIGINT,MAP(BIGINT,BIGINT))",
                "ROW(BIGINT,BIGINT)",
                "ROW(ARRAY(BIGINT),ARRAY(BIGINT))"
        })
        private String type = "BIGINT";

        @SuppressWarnings("unused")
        @Param({"true", "false"})
        private boolean hasNull = true;

        private List<Type> types;
        private int pageCount;
        private Page dataPage;

        @Setup
        public void setup()
        {
            createPages(type);
        }

        private void createPages(String inputType)
        {
            float primitiveNullRate = 0.0f;
            float nestedNullRate = 0.0f;

            if (hasNull) {
                primitiveNullRate = 0.2f;
                nestedNullRate = 0.2f;
            }
            switch (inputType) {
                case "BIGINT":
                    types = nCopies(channelCount, BIGINT);
                    dataPage = createPageWithRandomData(types, POSITION_COUNT, primitiveNullRate, nestedNullRate);
                    pageCount = 5000;
                    break;
                case "DICTIONARY(BIGINT)":
                    types = nCopies(channelCount, BIGINT);
                    dataPage = createDictionaryPageWithRandomData(types, POSITION_COUNT, primitiveNullRate, nestedNullRate);
                    pageCount = 3000;
                    break;
                case "RLE(BIGINT)":
                    types = nCopies(channelCount, BIGINT);
                    dataPage = createRlePageWithRandomData(types, POSITION_COUNT, primitiveNullRate, nestedNullRate);
                    pageCount = 3000;
                    break;
                case "LONG_DECIMAL":
                    types = nCopies(channelCount, createDecimalType(MAX_SHORT_PRECISION + 1));
                    dataPage = createPageWithRandomData(types, POSITION_COUNT, primitiveNullRate, nestedNullRate);
                    pageCount = 5000;
                    break;
                case "INTEGER":
                    types = nCopies(channelCount, INTEGER);
                    dataPage = createPageWithRandomData(types, POSITION_COUNT, primitiveNullRate, nestedNullRate);
                    pageCount = 5000;
                    break;
                case "SMALLINT":
                    types = nCopies(channelCount, SMALLINT);
                    dataPage = createPageWithRandomData(types, POSITION_COUNT, primitiveNullRate, nestedNullRate);
                    pageCount = 5000;
                    break;
                case "BOOLEAN":
                    types = nCopies(channelCount, BOOLEAN);
                    dataPage = createPageWithRandomData(types, POSITION_COUNT, primitiveNullRate, nestedNullRate);
                    pageCount = 5000;
                    break;
                case "VARCHAR":
                    types = nCopies(channelCount, VARCHAR);
                    dataPage = createPageWithRandomData(types, POSITION_COUNT, primitiveNullRate, nestedNullRate);
                    pageCount = 5000;
                    break;
                case "ARRAY(BIGINT)":
                    types = nCopies(channelCount, new ArrayType(BIGINT));
                    dataPage = createPageWithRandomData(types, POSITION_COUNT, primitiveNullRate, nestedNullRate);
                    pageCount = 1000;
                    break;
                case "ARRAY(VARCHAR)":
                    types = nCopies(channelCount, new ArrayType(VARCHAR));
                    dataPage = createPageWithRandomData(types, POSITION_COUNT, primitiveNullRate, nestedNullRate);
                    pageCount = 1000;
                    break;
                case "ARRAY(ARRAY(BIGINT))":
                    types = nCopies(channelCount, new ArrayType(new ArrayType(BIGINT)));
                    dataPage = createPageWithRandomData(types, POSITION_COUNT, primitiveNullRate, nestedNullRate);
                    pageCount = 1000;
                    break;
                case "MAP(BIGINT,BIGINT)":
                    types = nCopies(channelCount, createMapType(BIGINT, BIGINT));
                    dataPage = createPageWithRandomData(types, POSITION_COUNT, primitiveNullRate, nestedNullRate);
                    pageCount = 1000;
                    break;
                case "MAP(BIGINT,MAP(BIGINT,BIGINT))":
                    types = nCopies(channelCount, createMapType(BIGINT, createMapType(BIGINT, BIGINT)));
                    dataPage = createPageWithRandomData(types, POSITION_COUNT, primitiveNullRate, nestedNullRate);
                    pageCount = 1000;
                    break;
                case "ROW(BIGINT,BIGINT)":
                    types = nCopies(channelCount, withDefaultFieldNames(ImmutableList.of(BIGINT, BIGINT)));
                    dataPage = createPageWithRandomData(types, POSITION_COUNT, primitiveNullRate, nestedNullRate);
                    pageCount = 1000;
                    break;
                case "ROW(ARRAY(BIGINT),ARRAY(BIGINT))":
                    types = nCopies(channelCount, withDefaultFieldNames(ImmutableList.of(new ArrayType(BIGINT), new ArrayType(BIGINT))));
                    dataPage = createPageWithRandomData(types, POSITION_COUNT, primitiveNullRate, nestedNullRate);
                    pageCount = 1000;
                    break;

                default:
                    throw new UnsupportedOperationException("Unsupported dataType");
            }
            // We built the dataPage with added pre-computed hash block at channel 0, so types needs to be updated
            types = createUpdatedBlockTypesWithHashBlockAndNullBlock(types, true, false);
        }

        private PartitionedOutputBuffer createPartitionedOutputBuffer()
        {
            OutputBuffers buffers = createInitialEmptyOutputBuffers(PARTITIONED);
            for (int partition = 0; partition < PARTITION_COUNT; partition++) {
                buffers = buffers.withBuffer(new OutputBuffers.OutputBufferId(partition), partition);
            }
            PartitionedOutputBuffer buffer = createPartitionedBuffer(
                    buffers.withNoMoreBufferIds(),
                    DataSize.of(Long.MAX_VALUE, BYTE)); // don't let output buffer block

            return buffer;
        }

        private PartitionedOutputOperator createPartitionedOutputOperator(OperatorFactories operatorFactories, SessionBuilder sessionBuilder)
        {
            BlockTypeOperators blockTypeOperators = new BlockTypeOperators(new TypeOperators());
            PartitionFunction partitionFunction = new LocalPartitionGenerator(
                    new InterpretedHashGenerator(ImmutableList.of(BIGINT), new int[] {0}, blockTypeOperators),
                    PARTITION_COUNT);
            PagesSerdeFactory serdeFactory = new PagesSerdeFactory(createTestMetadataManager().getBlockEncodingSerde(), enableCompression);

            PartitionedOutputBuffer buffer = createPartitionedOutputBuffer();
            TaskContext taskContext = createTaskContext(sessionBuilder);

            OutputFactory operatorFactory = operatorFactories.partitionedOutput(
                    taskContext,
                    partitionFunction,
                    ImmutableList.of(0),
                    ImmutableList.of(Optional.empty()),
                    false,
                    OptionalInt.empty(),
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

        private TaskContext createTaskContext(SessionBuilder sessionBuilder)
        {
            Session testSession = sessionBuilder
                    .setCatalog("tpch")
                    .setSchema(TINY_SCHEMA_NAME)
                    .build();

            return TestingTaskContext.builder(EXECUTOR, SCHEDULER, testSession).build();
        }

        private TestingPartitionedOutputBuffer createPartitionedBuffer(OutputBuffers buffers, DataSize dataSize)
        {
            return new TestingPartitionedOutputBuffer(
                    "task-instance-id",
                    new StateMachine<>("bufferState", SCHEDULER, OPEN, TERMINAL_BUFFER_STATES),
                    buffers,
                    dataSize,
                    () -> new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                    SCHEDULER);
        }

        private static class TestingPartitionedOutputBuffer
                extends PartitionedOutputBuffer
        {
            public TestingPartitionedOutputBuffer(
                    String taskInstanceId,
                    StateMachine<BufferState> state,
                    OutputBuffers outputBuffers,
                    DataSize maxBufferSize,
                    Supplier<LocalMemoryContext> systemMemoryContextSupplier,
                    Executor notificationExecutor)
            {
                super(taskInstanceId, state, outputBuffers, maxBufferSize, systemMemoryContextSupplier, notificationExecutor);
            }

            // Use a dummy enqueue method to avoid OutOfMemory error
            @Override
            public void enqueue(int partitionNumber, List<SerializedPage> pages)
            {
            }
        }
    }

    public static RowType withDefaultFieldNames(List<Type> types)
    {
        List<RowType.Field> fields = new ArrayList<>();
        for (int i = 0; i < types.size(); i++) {
            fields.add(new RowType.Field(Optional.of("field" + i), types.get(i)));
        }

        fields = unmodifiableList(fields);
        return RowType.from(fields);
    }

    static {
        try {
            List<String> types = List.of("BIGINT",
                    "DICTIONARY(BIGINT)",
                    "RLE(BIGINT)",
                    "LONG_DECIMAL",
                    "INTEGER",
                    "SMALLINT",
                    "BOOLEAN",
                    "VARCHAR",
                    "ARRAY(BIGINT)");
            // use multiple types to pollute the profile
            types.forEach(type -> {
                BenchmarkData data = new BenchmarkData();
                data.type = type;
                data.setup();
                data.pageCount = 1;
                new BenchmarkPartitionedOutputOperator().addPage(data);
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
                .withOptions(optionsBuilder -> optionsBuilder.jvmArgs("-Xmx16g")
                ).run();
    }
}
