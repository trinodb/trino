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
import io.airlift.stats.TestingGcMonitor;
import io.airlift.units.DataSize;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.memory.MemoryPool;
import io.trino.memory.QueryContext;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.AggregationOperator.AggregationOperatorFactory;
import io.trino.operator.aggregation.AccumulatorFactory;
import io.trino.operator.aggregation.AggregatorFactory;
import io.trino.operator.aggregation.OrderedAccumulatorFactory;
import io.trino.operator.aggregation.TestingAggregationFunction;
import io.trino.spi.Page;
import io.trino.spi.QueryId;
import io.trino.spi.block.BitArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spiller.SpillSpaceTracker;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.MaterializedResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.block.BlockAssertions.createBooleansBlock;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.operator.OperatorAssertion.assertOperatorEquals;
import static io.trino.operator.OperatorAssertion.toPages;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static java.util.Collections.emptyIterator;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
public class TestAggregationOperator
{
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();

    private static final TestingAggregationFunction LONG_AVERAGE = FUNCTION_RESOLUTION.getAggregateFunction("avg", fromTypes(BIGINT));
    private static final TestingAggregationFunction DOUBLE_SUM = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(DOUBLE));
    private static final TestingAggregationFunction LONG_SUM = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(BIGINT));
    private static final TestingAggregationFunction REAL_SUM = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(REAL));
    private static final TestingAggregationFunction COUNT = FUNCTION_RESOLUTION.getAggregateFunction("count", ImmutableList.of());

    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;

    @BeforeEach
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));
    }

    @AfterEach
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testMaskWithDirtyNulls()
    {
        List<Page> input = ImmutableList.of(new Page(
                4,
                createLongsBlock(1, 2, 3, 4),
                new BitArrayBlock(
                        4,
                        Optional.of(new long[] {0b1100}),
                        new long[] {0b1010 /* dirty null at position 1, true value at position 3 */})));

        OperatorFactory operatorFactory = new AggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(COUNT.createAggregatorFactory(SINGLE, ImmutableList.of(0), OptionalInt.of(1))));

        DriverContext driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        MaterializedResult expected = resultBuilder(BIGINT)
                .row(1L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testDistinctMaskWithNulls()
    {
        AggregatorFactory distinctFactory = LONG_SUM.createDistinctAggregatorFactory(SINGLE, ImmutableList.of(0), OptionalInt.of(1));

        DriverContext driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        OperatorFactory operatorFactory = new AggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(distinctFactory));

        BitArrayBlock trueMaskAllNull = new BitArrayBlock(
                4,
                Optional.of(new long[] {0}), /* all positions are null */
                new long[] {0b1111}); /* all masks are true */

        Block trueNullRleMask = RunLengthEncodedBlock.create(trueMaskAllNull.getSingleValueBlock(0), 4);

        List<Page> nullTrueMaskInput = ImmutableList.of(
                new Page(4, createLongsBlock(1, 2, 3, 4), trueMaskAllNull),
                new Page(4, createLongsBlock(10, 11, 10, 11), createBooleansBlock(true, true, true, true)),
                new Page(4, createLongsBlock(5, 6, 7, 8), trueNullRleMask));

        MaterializedResult expected = resultBuilder(BIGINT)
                .row(21L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, nullTrueMaskInput, expected);
    }

    @Test
    public void testAggregation()
    {
        TestingAggregationFunction countVarcharColumn = FUNCTION_RESOLUTION.getAggregateFunction("count", fromTypes(VARCHAR));
        TestingAggregationFunction maxVarcharColumn = FUNCTION_RESOLUTION.getAggregateFunction("max", fromTypes(VARCHAR));
        List<Page> input = rowPagesBuilder(VARCHAR, BIGINT, VARCHAR, BIGINT, REAL, DOUBLE, VARCHAR)
                .addSequencePage(100, 0, 0, 300, 500, 400, 500, 500)
                .build();

        OperatorFactory operatorFactory = new AggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(COUNT.createAggregatorFactory(SINGLE, ImmutableList.of(0), OptionalInt.empty()),
                        LONG_SUM.createAggregatorFactory(SINGLE, ImmutableList.of(1), OptionalInt.empty()),
                        LONG_AVERAGE.createAggregatorFactory(SINGLE, ImmutableList.of(1), OptionalInt.empty()),
                        maxVarcharColumn.createAggregatorFactory(SINGLE, ImmutableList.of(2), OptionalInt.empty()),
                        countVarcharColumn.createAggregatorFactory(SINGLE, ImmutableList.of(0), OptionalInt.empty()),
                        LONG_SUM.createAggregatorFactory(SINGLE, ImmutableList.of(3), OptionalInt.empty()),
                        REAL_SUM.createAggregatorFactory(SINGLE, ImmutableList.of(4), OptionalInt.empty()),
                        DOUBLE_SUM.createAggregatorFactory(SINGLE, ImmutableList.of(5), OptionalInt.empty()),
                        maxVarcharColumn.createAggregatorFactory(SINGLE, ImmutableList.of(6), OptionalInt.empty())));

        DriverContext driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        MaterializedResult expected = resultBuilder(BIGINT, BIGINT, DOUBLE, VARCHAR, BIGINT, BIGINT, REAL, DOUBLE, VARCHAR)
                .row(100L, 4950L, 49.5, "399", 100L, 54950L, 44950.0f, 54950.0, "599")
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
        assertThat(driverContext.getMemoryUsage()).isEqualTo(0);
    }

    @Test
    public void testMemoryTracking()
            throws Exception
    {
        Page input = rowPagesBuilder(BIGINT).addSequencePage(100, 0).buildPage();

        OperatorFactory operatorFactory = new AggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(LONG_SUM.createAggregatorFactory(SINGLE, ImmutableList.of(0), OptionalInt.empty())));

        DriverContext driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        try (Operator operator = operatorFactory.createOperator(driverContext)) {
            assertThat(operator.needsInput()).isTrue();
            operator.addInput(input);

            assertThat(driverContext.getMemoryUsage()).isGreaterThan(0);

            toPages(operator, emptyIterator());
        }

        assertThat(driverContext.getMemoryUsage()).isEqualTo(0);
    }

    @Test
    public void testOrderedAggregationYieldsWhenMemoryIsBlocked()
            throws Exception
    {
        TestingAggregationFunction arrayAgg = FUNCTION_RESOLUTION.getAggregateFunction("array_agg", fromTypes(BIGINT));
        AccumulatorFactory distinctOrderedFactory = new OrderedAccumulatorFactory(
                arrayAgg.getDistinctFactory(),
                ImmutableList.of(BIGINT),
                ImmutableList.of(0),
                ImmutableList.of(0),
                ImmutableList.of(ASC_NULLS_LAST),
                new PagesIndex.TestingFactory(false));
        AggregatorFactory aggregatorFactory = new AggregatorFactory(
                distinctOrderedFactory,
                SINGLE,
                arrayAgg.getIntermediateType(),
                arrayAgg.getFinalType(),
                ImmutableList.of(0),
                OptionalInt.empty(),
                false, // spillable
                ImmutableList.of());

        // all values distinct and in descending order
        int positionsPerPage = 100_000;
        int pageCount = 4;
        long rows = (long) pageCount * positionsPerPage;
        List<Page> input = new ArrayList<>();
        for (int page = 0; page < pageCount; page++) {
            long[] values = new long[positionsPerPage];
            for (int i = 0; i < positionsPerPage; i++) {
                values[i] = rows - 1 - ((long) page * positionsPerPage) - i;
            }
            input.add(new Page(new LongArrayBlock(positionsPerPage, Optional.empty(), values)));
        }

        TaskId otherTaskId = new TaskId(new StageId("other_query", 0), 0, 0);
        MemoryPool memoryPool = new MemoryPool(DataSize.of(1, GIGABYTE));
        QueryContext queryContext = new QueryContext(
                new QueryId("test_query"),
                DataSize.of(512, MEGABYTE),
                memoryPool,
                new TestingGcMonitor(),
                executor,
                scheduledExecutor,
                scheduledExecutor,
                DataSize.of(512, MEGABYTE),
                new SpillSpaceTracker(DataSize.of(512, MEGABYTE)));
        DriverContext driverContext = createTaskContext(queryContext, executor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        OperatorFactory operatorFactory = new AggregationOperatorFactory(0, new PlanNodeId("test"), ImmutableList.of(aggregatorFactory, aggregatorFactory));

        try (Operator operator = operatorFactory.createOperator(driverContext)) {
            for (Page page : input) {
                operator.addInput(page);
            }
            operator.finish();

            // each aggregator yields while the memory pool is full
            memoryPool.reserve(otherTaskId, "test", memoryPool.getFreeBytes());
            int yields = 0;
            Page output = operator.getOutput();
            while (output == null) {
                assertThat(operator.getOperatorContext().isWaitingForMemory().isDone()).isFalse();
                yields++;
                output = operator.getOutput();
            }
            assertThat(yields).isGreaterThan(1);

            memoryPool.free(otherTaskId, "test", memoryPool.getTaskMemoryReservations().get(otherTaskId));
            assertThat(operator.getOperatorContext().isWaitingForMemory().isDone()).isTrue();

            assertThat(output.getPositionCount()).isEqualTo(1);
            for (int channel = 0; channel < 2; channel++) {
                Block values = (Block) arrayAgg.getFinalType().getObject(output.getBlock(channel), 0);
                long[] actual = IntStream.range(0, values.getPositionCount())
                        .mapToLong(position -> BIGINT.getLong(values, position))
                        .toArray();
                assertThat(actual).isEqualTo(LongStream.range(0, rows).toArray());
            }
            assertThat(operator.isFinished()).isTrue();
        }
    }
}
