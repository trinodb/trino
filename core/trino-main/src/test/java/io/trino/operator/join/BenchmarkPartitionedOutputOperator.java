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
package io.trino.operator.join;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.execution.StateMachine;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.execution.buffer.PagesSerdeFactory;
import io.trino.execution.buffer.PartitionedOutputBuffer;
import io.trino.memory.context.SimpleLocalMemoryContext;
import io.trino.operator.DriverContext;
import io.trino.operator.InterpretedHashGenerator;
import io.trino.operator.PartitionFunction;
import io.trino.operator.PartitionedOutputOperator;
import io.trino.operator.PartitionedOutputOperator.PartitionedOutputFactory;
import io.trino.operator.exchange.LocalPartitionGenerator;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
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
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.buffer.BufferState.OPEN;
import static io.trino.execution.buffer.BufferState.TERMINAL_BUFFER_STATES;
import static io.trino.execution.buffer.OutputBuffers.BufferType.PARTITIONED;
import static io.trino.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@State(Scope.Thread)
@OutputTimeUnit(MILLISECONDS)
@Fork(2)
@Warmup(iterations = 20, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 20, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkPartitionedOutputOperator
{
    @Benchmark
    public void addPage(BenchmarkData data)
    {
        PartitionedOutputOperator operator = data.createPartitionedOutputOperator();
        for (int i = 0; i < data.getPageCount(); i++) {
            operator.addInput(data.getDataPage());
        }
        operator.finish();
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private static final int PAGE_COUNT = 5000;
        private static final int PARTITION_COUNT = 512;
        private static final int ENTRIES_PER_PAGE = 256;
        private static final DataSize MAX_MEMORY = DataSize.of(1, GIGABYTE);
        private static final RowType rowType = RowType.anonymous(ImmutableList.of(VARCHAR, VARCHAR, VARCHAR, VARCHAR));
        private static final List<Type> TYPES = ImmutableList.of(BIGINT, rowType, rowType, rowType);
        private static final ExecutorService EXECUTOR = newCachedThreadPool(daemonThreadsNamed("BenchmarkPartitionedOutputOperator-executor-%s"));
        private static final ScheduledExecutorService SCHEDULER = newScheduledThreadPool(1, daemonThreadsNamed("BenchmarkPartitionedOutputOperator-scheduledExecutor-%s"));

        private final Page dataPage = createPage();

        private int getPageCount()
        {
            return PAGE_COUNT;
        }

        public Page getDataPage()
        {
            return dataPage;
        }

        private PartitionedOutputOperator createPartitionedOutputOperator()
        {
            BlockTypeOperators blockTypeOperators = new BlockTypeOperators(new TypeOperators());
            PartitionFunction partitionFunction = new LocalPartitionGenerator(
                    new InterpretedHashGenerator(ImmutableList.of(BIGINT), new int[] {0}, blockTypeOperators),
                    PARTITION_COUNT);
            PagesSerdeFactory serdeFactory = new PagesSerdeFactory(createTestMetadataManager().getBlockEncodingSerde(), false);
            OutputBuffers buffers = createInitialEmptyOutputBuffers(PARTITIONED);
            for (int partition = 0; partition < PARTITION_COUNT; partition++) {
                buffers = buffers.withBuffer(new OutputBuffers.OutputBufferId(partition), partition);
            }
            PartitionedOutputBuffer buffer = createPartitionedBuffer(
                    buffers.withNoMoreBufferIds(),
                    DataSize.ofBytes(Long.MAX_VALUE)); // don't let output buffer block
            PartitionedOutputFactory operatorFactory = new PartitionedOutputFactory(
                    partitionFunction,
                    ImmutableList.of(0),
                    ImmutableList.of(Optional.empty()),
                    false,
                    OptionalInt.empty(),
                    buffer,
                    DataSize.of(1, GIGABYTE));
            return (PartitionedOutputOperator) operatorFactory
                    .createOutputOperator(0, new PlanNodeId("plan-node-0"), TYPES, Function.identity(), serdeFactory)
                    .createOperator(createDriverContext());
        }

        private Page createPage()
        {
            List<Object>[] testRows = generateTestRows(ImmutableList.of(VARCHAR, VARCHAR, VARCHAR, VARCHAR), ENTRIES_PER_PAGE);
            PageBuilder pageBuilder = new PageBuilder(TYPES);
            BlockBuilder bigintBlockBuilder = pageBuilder.getBlockBuilder(0);
            BlockBuilder rowBlockBuilder = pageBuilder.getBlockBuilder(1);
            BlockBuilder rowBlockBuilder2 = pageBuilder.getBlockBuilder(2);
            BlockBuilder rowBlockBuilder3 = pageBuilder.getBlockBuilder(3);
            for (int i = 0; i < ENTRIES_PER_PAGE; i++) {
                BIGINT.writeLong(bigintBlockBuilder, i);
                writeRow(testRows[i], rowBlockBuilder);
                writeRow(testRows[i], rowBlockBuilder2);
                writeRow(testRows[i], rowBlockBuilder3);
            }
            pageBuilder.declarePositions(ENTRIES_PER_PAGE);
            return pageBuilder.build();
        }

        private void writeRow(List<Object> testRow, BlockBuilder rowBlockBuilder)
        {
            BlockBuilder singleRowBlockWriter = rowBlockBuilder.beginBlockEntry();
            for (Object fieldValue : testRow) {
                if (fieldValue instanceof String) {
                    VARCHAR.writeSlice(singleRowBlockWriter, utf8Slice((String) fieldValue));
                }
                else {
                    throw new UnsupportedOperationException();
                }
            }
            rowBlockBuilder.closeEntry();
        }

        // copied & modifed from TestRowBlock
        private List<Object>[] generateTestRows(List<Type> fieldTypes, int numRows)
        {
            @SuppressWarnings("unchecked")
            List<Object>[] testRows = new List[numRows];
            for (int i = 0; i < numRows; i++) {
                List<Object> testRow = new ArrayList<>(fieldTypes.size());
                for (int j = 0; j < fieldTypes.size(); j++) {
                    if (fieldTypes.get(j) == VARCHAR) {
                        byte[] data = new byte[ThreadLocalRandom.current().nextInt(128)];
                        ThreadLocalRandom.current().nextBytes(data);
                        testRow.add(new String(data, ISO_8859_1));
                    }
                    else {
                        throw new UnsupportedOperationException();
                    }
                }
                testRows[i] = testRow;
            }
            return testRows;
        }

        private DriverContext createDriverContext()
        {
            return TestingTaskContext.builder(EXECUTOR, SCHEDULER, TEST_SESSION)
                    .setMemoryPoolSize(MAX_MEMORY)
                    .build()
                    .addPipelineContext(0, true, true, false)
                    .addDriverContext();
        }

        private PartitionedOutputBuffer createPartitionedBuffer(OutputBuffers buffers, DataSize dataSize)
        {
            return new PartitionedOutputBuffer(
                    "task-instance-id",
                    new StateMachine<>("bufferState", SCHEDULER, OPEN, TERMINAL_BUFFER_STATES),
                    buffers,
                    dataSize,
                    () -> new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                    SCHEDULER);
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        new BenchmarkPartitionedOutputOperator().addPage(data);

        benchmark(BenchmarkPartitionedOutputOperator.class)
                .withOptions(optionsBuilder -> optionsBuilder.jvmArgs("-Xmx10g"))
                .run();
    }
}
