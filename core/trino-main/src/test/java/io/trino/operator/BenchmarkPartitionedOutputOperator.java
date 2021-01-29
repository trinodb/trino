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
package io.trino.operator;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.execution.StateMachine;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.execution.buffer.PagesSerdeFactory;
import io.trino.execution.buffer.PartitionedOutputBuffer;
import io.trino.memory.context.SimpleLocalMemoryContext;
import io.trino.operator.PartitionedOutputOperator.PartitionedOutputFactory;
import io.trino.operator.exchange.LocalPartitionGenerator;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.TestingTaskContext;
import io.trino.type.BlockTypeOperators;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.buffer.BufferState.OPEN;
import static io.trino.execution.buffer.BufferState.TERMINAL_BUFFER_STATES;
import static io.trino.execution.buffer.OutputBuffers.BufferType.PARTITIONED;
import static io.trino.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.function.Function.identity;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;
import static org.openjdk.jmh.runner.options.VerboseMode.NORMAL;

@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@Fork(3)
@Warmup(iterations = 20, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 20, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(AverageTime)
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

    @State(Thread)
    public static class BenchmarkData
    {
        private static final int ENTRIES = 256 * 1024;

        private static final DataSize MAX_MEMORY = DataSize.of(1, GIGABYTE);
        private static final List<Type> TYPES = ImmutableList.of(BIGINT, VARCHAR, VARCHAR, VARCHAR);
        private static final ExecutorService EXECUTOR = newCachedThreadPool(daemonThreadsNamed("BenchmarkPartitionedOutputOperator-executor-%s"));
        private static final ScheduledExecutorService SCHEDULER = newScheduledThreadPool(1, daemonThreadsNamed("BenchmarkPartitionedOutputOperator-scheduledExecutor-%s"));

        @Param({"4", "16", "64", "512"})
        protected int partitionCount = 512;

        @Param({"256", "1024"})
        protected int entriesPerPage = 256;
        protected int pageCount = ENTRIES / entriesPerPage;

        @Param({"0", ".1", ".3"})
        protected double nullChance;

        private final Page dataPage = createPage();

        private int getPageCount()
        {
            return pageCount;
        }

        public Page getDataPage()
        {
            return dataPage;
        }

        private PartitionedOutputOperator createPartitionedOutputOperator()
        {
            BlockTypeOperators blockTypeOperators = new BlockTypeOperators(new TypeOperators());
            PartitionFunction partitionFunction = new LocalPartitionGenerator(
                    new InterpretedHashGenerator(ImmutableList.of(BIGINT), new int[] {0}, blockTypeOperators), partitionCount);
            PagesSerdeFactory serdeFactory = new PagesSerdeFactory(createTestMetadataManager().getBlockEncodingSerde(), false);
            OutputBuffers buffers = createInitialEmptyOutputBuffers(PARTITIONED);
            for (int partition = 0; partition < partitionCount; partition++) {
                buffers = buffers.withBuffer(new OutputBuffers.OutputBufferId(partition), partition);
            }
            PartitionedOutputBuffer buffer = createPartitionedBuffer(
                    buffers.withNoMoreBufferIds(),
                    DataSize.ofBytes(Long.MAX_VALUE)); // don't let output buffer block
            OptionalInt nullChannel = nullChance == 0 ? OptionalInt.empty() : OptionalInt.of(0);
            PartitionedOutputFactory operatorFactory = new PartitionedOutputFactory(
                    partitionFunction,
                    ImmutableList.of(0),
                    ImmutableList.of(Optional.empty()),
                    false,
                    nullChannel,
                    buffer,
                    DataSize.of(1, GIGABYTE));
            return (PartitionedOutputOperator) operatorFactory
                    .createOutputOperator(0, new PlanNodeId("plan-node-0"), TYPES, identity(), serdeFactory)
                    .createOperator(createDriverContext());
        }

        private Page createPage()
        {
            Object[][] testRows = generateTestData(ImmutableList.of(VARCHAR, VARCHAR, VARCHAR), entriesPerPage);
            PageBuilder pageBuilder = new PageBuilder(TYPES);
            BlockBuilder bigintBlockBuilder = pageBuilder.getBlockBuilder(0);
            BlockBuilder varcharBlockBuilder = pageBuilder.getBlockBuilder(1);
            BlockBuilder varcharBlockBuilder2 = pageBuilder.getBlockBuilder(2);
            BlockBuilder varcharBlockBuilder3 = pageBuilder.getBlockBuilder(3);
            for (int i = 0; i < entriesPerPage; i++) {
                if (ThreadLocalRandom.current().nextDouble() < nullChance) {
                    bigintBlockBuilder.appendNull();
                }
                else {
                    BIGINT.writeLong(bigintBlockBuilder, i);
                }
                VARCHAR.writeSlice(varcharBlockBuilder, utf8Slice((String) testRows[0][i]));
                VARCHAR.writeSlice(varcharBlockBuilder2, utf8Slice((String) testRows[1][i]));
                VARCHAR.writeSlice(varcharBlockBuilder3, utf8Slice((String) testRows[2][i]));
            }
            pageBuilder.declarePositions(entriesPerPage);
            return pageBuilder.build();
        }

        private Object[][] generateTestData(List<Type> fieldTypes, int numRows)
        {
            Object[][] testRows = new Object[fieldTypes.size()][];
            for (int i = 0; i < fieldTypes.size(); i++) {
                Object[] testRow = new Object[numRows];
                for (int j = 0; j < numRows; j++) {
                    if (fieldTypes.get(i) == VARCHAR) {
                        byte[] data = new byte[ThreadLocalRandom.current().nextInt(128)];
                        ThreadLocalRandom.current().nextBytes(data);
                        testRow[j] = new String(data, ISO_8859_1);
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

    @Test
    public void testAddPage()
    {
        addPage(new BenchmarkData());
    }

    public static void main(String[] args)
            throws RunnerException
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        new BenchmarkPartitionedOutputOperator().addPage(data);
        Options options = new OptionsBuilder()
                .verbosity(NORMAL)
                .jvmArgs("-Xmx10g")
                .include(".*" + BenchmarkPartitionedOutputOperator.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
