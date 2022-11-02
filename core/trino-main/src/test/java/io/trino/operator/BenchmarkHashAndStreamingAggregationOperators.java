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
import io.trino.RowPagesBuilder;
import io.trino.jmh.Benchmarks;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.HashAggregationOperator.HashAggregationOperatorFactory;
import io.trino.operator.aggregation.TestingAggregationFunction;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spiller.SpillerFactory;
import io.trino.sql.gen.JoinCompiler;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.tree.QualifiedName;
import io.trino.testing.TestingTaskContext;
import io.trino.type.BlockTypeOperators;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.operator.BenchmarkHashAndStreamingAggregationOperators.Context.ROWS_PER_PAGE;
import static io.trino.operator.BenchmarkHashAndStreamingAggregationOperators.Context.TOTAL_PAGES;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;
import static org.testng.Assert.assertEquals;

@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(3)
@Warmup(iterations = 10)
@Measurement(iterations = 10, time = 2, timeUnit = SECONDS)
public class BenchmarkHashAndStreamingAggregationOperators
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();
    private static final BlockTypeOperators BLOCK_TYPE_OPERATORS = new BlockTypeOperators(TYPE_OPERATORS);
    private static final JoinCompiler JOIN_COMPILER = new JoinCompiler(TYPE_OPERATORS);

    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();
    private static final TestingAggregationFunction LONG_SUM = FUNCTION_RESOLUTION.getAggregateFunction(QualifiedName.of("sum"), fromTypes(BIGINT));
    private static final TestingAggregationFunction COUNT = FUNCTION_RESOLUTION.getAggregateFunction(QualifiedName.of("count"), ImmutableList.of());

    @State(Thread)
    public static class Context
    {
        public static final int TOTAL_PAGES = 140;
        public static final int ROWS_PER_PAGE = 10_000;

        @Param({"1", "10", "1000"})
        public int rowsPerGroup;

        @Param({"streaming", "hash"})
        public String operatorType;

        @Param({"bigint", "varchar", "mixed"})
        public String groupByTypes;

        private ExecutorService executor;
        private ScheduledExecutorService scheduledExecutor;
        private OperatorFactory operatorFactory;
        private List<Page> pages;

        @Setup
        public void setup()
        {
            executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
            scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));

            int groupsPerPage = ROWS_PER_PAGE / rowsPerGroup;

            boolean hashAggregation = operatorType.equalsIgnoreCase("hash");

            List<Type> hashTypes;
            List<Integer> hashChannels;
            int sumChannel;
            switch (groupByTypes) {
                case "bigint":
                    hashTypes = ImmutableList.of(BIGINT);
                    hashChannels = ImmutableList.of(0);
                    sumChannel = 1;
                    break;

                case "varchar":
                    hashTypes = ImmutableList.of(VARCHAR);
                    hashChannels = ImmutableList.of(0);
                    sumChannel = 1;
                    break;

                case "mixed":
                    hashTypes = ImmutableList.of(BIGINT, VARCHAR, DOUBLE);
                    hashChannels = ImmutableList.of(0, 1, 2);
                    sumChannel = 3;
                    break;

                default:
                    throw new IllegalStateException();
            }

            RowPagesBuilder pagesBuilder = RowPagesBuilder.rowPagesBuilder(
                    hashAggregation,
                    hashChannels,
                    ImmutableList.<Type>builder()
                            .addAll(hashTypes)
                            .add(BIGINT)
                            .build());
            for (int i = 0; i < TOTAL_PAGES; i++) {
                BlockBuilder bigintBlockBuilder = BIGINT.createBlockBuilder(null, ROWS_PER_PAGE);
                BlockBuilder varcharBlockBuilder = VARCHAR.createBlockBuilder(null, ROWS_PER_PAGE);
                BlockBuilder doubleBlockBuilder = DOUBLE.createBlockBuilder(null, ROWS_PER_PAGE);

                for (int j = 0; j < groupsPerPage; j++) {
                    long groupKey = i * groupsPerPage + j;

                    switch (groupByTypes) {
                        case "bigint":
                            repeatToBigintBlock(groupKey, rowsPerGroup, bigintBlockBuilder);
                            break;

                        case "varchar":
                            repeatToStringBlock(Long.toString(groupKey), rowsPerGroup, varcharBlockBuilder);
                            break;

                        case "mixed":
                            repeatToBigintBlock(groupKey, rowsPerGroup, bigintBlockBuilder);
                            repeatToStringBlock(Long.toString(groupKey), rowsPerGroup, varcharBlockBuilder);
                            repeatToDoubleBlock(groupKey, rowsPerGroup, doubleBlockBuilder);
                            break;

                        default:
                            throw new IllegalStateException();
                    }
                }

                List<Block> blocks;
                switch (groupByTypes) {
                    case "bigint":
                        blocks = ImmutableList.of(bigintBlockBuilder.build());
                        break;

                    case "varchar":
                        blocks = ImmutableList.of(varcharBlockBuilder.build());
                        break;

                    case "mixed":
                        blocks = ImmutableList.of(bigintBlockBuilder.build(), varcharBlockBuilder.build(), doubleBlockBuilder.build());
                        break;

                    default:
                        throw new IllegalStateException();
                }

                pagesBuilder.addBlocksPage(
                        ImmutableList.<Block>builder()
                                .addAll(blocks)
                                .add(createLongSequenceBlock(0, ROWS_PER_PAGE))
                                .build());
            }

            pages = pagesBuilder.build();

            if (hashAggregation) {
                operatorFactory = createHashAggregationOperatorFactory(pagesBuilder.getHashChannel(), hashTypes, hashChannels, sumChannel);
            }
            else {
                operatorFactory = createStreamingAggregationOperatorFactory(hashTypes, hashChannels, sumChannel);
            }
        }

        @TearDown
        public void cleanup()
        {
            executor.shutdownNow();
            scheduledExecutor.shutdownNow();
        }

        private OperatorFactory createStreamingAggregationOperatorFactory(
                List<Type> hashTypes,
                List<Integer> hashChannels,
                int sumChannel)
        {
            return StreamingAggregationOperator.createOperatorFactory(
                    0,
                    new PlanNodeId("test"),
                    hashTypes,
                    hashTypes,
                    hashChannels,
                    ImmutableList.of(
                            COUNT.createAggregatorFactory(SINGLE, ImmutableList.of(0), OptionalInt.empty()),
                            LONG_SUM.createAggregatorFactory(SINGLE, ImmutableList.of(sumChannel), OptionalInt.empty())),
                    JOIN_COMPILER);
        }

        private OperatorFactory createHashAggregationOperatorFactory(
                Optional<Integer> hashChannel,
                List<Type> hashTypes,
                List<Integer> hashChannels,
                int sumChannel)
        {
            SpillerFactory spillerFactory = (types, localSpillContext, aggregatedMemoryContext) -> null;

            return new HashAggregationOperatorFactory(
                    0,
                    new PlanNodeId("test"),
                    hashTypes,
                    hashChannels,
                    ImmutableList.of(),
                    SINGLE,
                    false,
                    ImmutableList.of(
                            COUNT.createAggregatorFactory(SINGLE, ImmutableList.of(0), OptionalInt.empty()),
                            LONG_SUM.createAggregatorFactory(SINGLE, ImmutableList.of(sumChannel), OptionalInt.empty())),
                    hashChannel,
                    Optional.empty(),
                    100_000,
                    Optional.of(DataSize.of(16, MEGABYTE)),
                    false,
                    succinctBytes(8),
                    succinctBytes(Integer.MAX_VALUE),
                    spillerFactory,
                    JOIN_COMPILER,
                    BLOCK_TYPE_OPERATORS,
                    Optional.empty());
        }

        private static void repeatToBigintBlock(long value, int count, BlockBuilder blockBuilder)
        {
            for (int i = 0; i < count; ++i) {
                BIGINT.writeLong(blockBuilder, value);
            }
        }

        private static void repeatToStringBlock(String value, int count, BlockBuilder blockBuilder)
        {
            for (int i = 0; i < count; i++) {
                VARCHAR.writeString(blockBuilder, value);
            }
        }

        private static void repeatToDoubleBlock(double value, int count, BlockBuilder blockBuilder)
        {
            for (int i = 0; i < count; ++i) {
                DOUBLE.writeDouble(blockBuilder, value);
            }
        }

        public TaskContext createTaskContext()
        {
            return TestingTaskContext.createTaskContext(executor, scheduledExecutor, TEST_SESSION, DataSize.of(2, GIGABYTE));
        }

        public OperatorFactory getOperatorFactory()
        {
            return operatorFactory;
        }

        public List<Page> getPages()
        {
            return pages;
        }
    }

    @Benchmark
    public List<Page> benchmark(Context context)
    {
        DriverContext driverContext = context.createTaskContext().addPipelineContext(0, true, true, false).addDriverContext();
        Operator operator = context.getOperatorFactory().createOperator(driverContext);

        Iterator<Page> input = context.getPages().iterator();
        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();

        boolean finishing = false;
        for (int loops = 0; !operator.isFinished() && loops < 1_000_000; loops++) {
            if (operator.needsInput()) {
                if (input.hasNext()) {
                    Page inputPage = input.next();
                    operator.addInput(inputPage);
                }
                else if (!finishing) {
                    operator.finish();
                    finishing = true;
                }
            }

            Page outputPage = operator.getOutput();
            if (outputPage != null) {
                outputPages.add(outputPage);
            }
        }

        return outputPages.build();
    }

    @Test
    public void verifyStreaming()
    {
        verify(1, "streaming", "bigint");
        verify(10, "streaming", "varchar");
        verify(1000, "streaming", "mixed");
    }

    @Test
    public void verifyHash()
    {
        verify(1, "hash", "bigint");
        verify(10, "hash", "varchar");
        verify(1000, "hash", "mixed");
    }

    private void verify(int rowsPerGroup, String operatorType, String groupByTypes)
    {
        Context context = new Context();
        context.operatorType = operatorType;
        context.rowsPerGroup = rowsPerGroup;
        context.groupByTypes = groupByTypes;
        context.setup();

        assertEquals(TOTAL_PAGES, context.getPages().size());
        for (int i = 0; i < TOTAL_PAGES; i++) {
            assertEquals(ROWS_PER_PAGE, context.getPages().get(i).getPositionCount());
        }

        List<Page> outputPages = benchmark(context);
        assertEquals(TOTAL_PAGES * ROWS_PER_PAGE / rowsPerGroup, outputPages.stream().mapToInt(Page::getPositionCount).sum());

        context.cleanup();
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Benchmarks.benchmark(BenchmarkHashAndStreamingAggregationOperators.class).run();
    }
}
