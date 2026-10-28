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
import io.trino.RowPagesBuilder;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.memory.MemoryPool;
import io.trino.memory.QueryContext;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.aggregation.AccumulatorFactory;
import io.trino.operator.aggregation.AggregatorFactory;
import io.trino.operator.aggregation.OrderedAccumulatorFactory;
import io.trino.operator.aggregation.TestingAggregationFunction;
import io.trino.spi.Page;
import io.trino.spi.QueryId;
import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.type.TypeOperators;
import io.trino.spiller.SpillSpaceTracker;
import io.trino.sql.gen.JoinCompiler;
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
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.OperatorAssertion.assertOperatorEquals;
import static io.trino.operator.OperatorAssertion.finishOperator;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
public class TestStreamingAggregationOperator
{
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();
    private static final TestingAggregationFunction LONG_SUM = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(BIGINT));
    private static final TestingAggregationFunction COUNT = FUNCTION_RESOLUTION.getAggregateFunction("count", ImmutableList.of());

    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private DriverContext driverContext;
    private OperatorFactory operatorFactory;

    @BeforeEach
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));

        driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        operatorFactory = StreamingAggregationOperator.createOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BOOLEAN, VARCHAR, BIGINT),
                ImmutableList.of(VARCHAR),
                ImmutableList.of(1),
                ImmutableList.of(COUNT.createAggregatorFactory(SINGLE, ImmutableList.of(0), OptionalInt.empty()),
                        LONG_SUM.createAggregatorFactory(SINGLE, ImmutableList.of(2), OptionalInt.empty())),
                new JoinCompiler(new TypeOperators()));
    }

    @AfterEach
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void test()
    {
        OperatorFactory operatorFactory = StreamingAggregationOperator.createOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BOOLEAN, DOUBLE, BIGINT),
                ImmutableList.of(DOUBLE),
                ImmutableList.of(1),
                ImmutableList.of(COUNT.createAggregatorFactory(SINGLE, ImmutableList.of(0), OptionalInt.empty()),
                        LONG_SUM.createAggregatorFactory(SINGLE, ImmutableList.of(2), OptionalInt.empty())),
                new JoinCompiler(new TypeOperators()));

        RowPagesBuilder rowPagesBuilder = RowPagesBuilder.rowPagesBuilder(BOOLEAN, DOUBLE, BIGINT);
        List<Page> input = rowPagesBuilder
                .addSequencePage(3, 0, 0, 1)
                .row(true, 3.0, 4)
                .row(false, 3.0, 5)
                .pageBreak()
                .row(true, 3.0, 6)
                .row(false, 4.0, 7)
                .row(true, 4.0, 8)
                .row(false, 4.0, 9)
                .row(true, 4.0, 10)
                .pageBreak()
                .row(false, 5.0, 11)
                .row(true, 5.0, 12)
                .row(false, 5.0, 13)
                .row(true, 5.0, 14)
                .row(false, 5.0, 15)
                .pageBreak()
                .addSequencePage(3, 0, 6, 16)
                .row(false, Double.NaN, 1)
                .row(false, Double.NaN, 10)
                .row(false, null, 2)
                .row(false, null, 20)
                .build();

        MaterializedResult expected = resultBuilder(DOUBLE, BIGINT, BIGINT)
                .row(0.0, 1L, 1L)
                .row(1.0, 1L, 2L)
                .row(2.0, 1L, 3L)
                .row(3.0, 3L, 15L)
                .row(4.0, 4L, 34L)
                .row(5.0, 5L, 65L)
                .row(6.0, 1L, 16L)
                .row(7.0, 1L, 17L)
                .row(8.0, 1L, 18L)
                .row(Double.NaN, 2L, 11L)
                .row(null, 2L, 22L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testLargeInputPage()
    {
        RowPagesBuilder rowPagesBuilder = RowPagesBuilder.rowPagesBuilder(BOOLEAN, VARCHAR, BIGINT);
        List<Page> input = rowPagesBuilder
                .addSequencePage(50_000, 0, 0, 1)
                .build();

        MaterializedResult.Builder expectedBuilder = resultBuilder(VARCHAR, BIGINT, BIGINT);
        for (int i = 0; i < 50_000; ++i) {
            expectedBuilder.row(String.valueOf(i), 1L, i + 1L);
        }

        assertOperatorEquals(operatorFactory, driverContext, input, expectedBuilder.build());
    }

    @Test
    public void testEmptyInput()
    {
        RowPagesBuilder rowPagesBuilder = RowPagesBuilder.rowPagesBuilder(BOOLEAN, VARCHAR, BIGINT);
        List<Page> input = rowPagesBuilder.build();

        MaterializedResult expected = resultBuilder(VARCHAR, BIGINT, BIGINT).build();
        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testSinglePage()
    {
        RowPagesBuilder rowPagesBuilder = RowPagesBuilder.rowPagesBuilder(BOOLEAN, VARCHAR, BIGINT);
        List<Page> input = rowPagesBuilder
                .row(false, "a", 5)
                .build();

        MaterializedResult expected = resultBuilder(VARCHAR, BIGINT, BIGINT)
                .row("a", 1L, 5L)
                .build();
        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testUniqueGroupingValues()
    {
        RowPagesBuilder rowPagesBuilder = RowPagesBuilder.rowPagesBuilder(BOOLEAN, VARCHAR, BIGINT);
        List<Page> input = rowPagesBuilder
                .addSequencePage(10, 0, 0, 0)
                .addSequencePage(10, 0, 10, 10)
                .build();

        MaterializedResult.Builder builder = resultBuilder(VARCHAR, BIGINT, BIGINT);
        for (int i = 0; i < 20; i++) {
            builder.row(format("%s", i), 1L, Long.valueOf(i));
        }

        assertOperatorEquals(operatorFactory, driverContext, input, builder.build());
    }

    @Test
    public void testSingleGroupingValue()
    {
        RowPagesBuilder rowPagesBuilder = RowPagesBuilder.rowPagesBuilder(BOOLEAN, VARCHAR, BIGINT);
        List<Page> input = rowPagesBuilder
                .row(true, "a", 1)
                .row(false, "a", 2)
                .row(true, "a", 3)
                .row(false, "a", 4)
                .row(true, "a", 5)
                .pageBreak()
                .row(false, "a", 6)
                .row(true, "a", 7)
                .row(false, "a", 8)
                .pageBreak()
                .pageBreak()
                .row(true, "a", 9)
                .row(false, "a", 10)
                .build();

        MaterializedResult expected = resultBuilder(VARCHAR, BIGINT, BIGINT)
                .row("a", 10L, 55L)
                .build();
        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    // group evaluation yields and resumes mid-page, at a page start, and on finish
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
                ImmutableList.of(1),
                OptionalInt.empty(),
                false, // spillable
                ImmutableList.of());

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

        OperatorFactory operatorFactory = StreamingAggregationOperator.createOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT, BIGINT),
                ImmutableList.of(BIGINT),
                ImmutableList.of(0),
                ImmutableList.of(aggregatorFactory),
                new JoinCompiler(new TypeOperators()));

        int groupSize = 300_000;
        List<Page> output = new ArrayList<>();
        try (Operator operator = operatorFactory.createOperator(driverContext)) {
            // group 0 ends mid-page
            operator.addInput(createGroupsPage(0, 2, groupSize));
            assertYieldsWhileMemoryIsBlocked(operator, memoryPool);
            for (Page page = operator.getOutput(); page != null; page = operator.getOutput()) {
                output.add(page);
            }

            // group 1 ends at a page boundary
            assertThat(operator.needsInput()).isTrue();
            operator.addInput(createGroupsPage(2, 1, groupSize));
            assertYieldsWhileMemoryIsBlocked(operator, memoryPool);
            for (Page page = operator.getOutput(); page != null; page = operator.getOutput()) {
                output.add(page);
            }

            // group 2 ends with the input
            operator.finish();
            assertYieldsWhileMemoryIsBlocked(operator, memoryPool);
            output.addAll(finishOperator(operator));
        }

        List<Long> groups = new ArrayList<>();
        for (Page page : output) {
            for (int position = 0; position < page.getPositionCount(); position++) {
                long group = BIGINT.getLong(page.getBlock(0), position);
                Block values = (Block) arrayAgg.getFinalType().getObject(page.getBlock(1), position);
                long[] actual = IntStream.range(0, values.getPositionCount())
                        .mapToLong(valuePosition -> BIGINT.getLong(values, valuePosition))
                        .toArray();
                assertThat(actual).isEqualTo(LongStream.range(group * groupSize, (group + 1) * groupSize).toArray());
                groups.add(group);
            }
        }
        assertThat(groups).containsExactly(0L, 1L, 2L);
    }

    private static void assertYieldsWhileMemoryIsBlocked(Operator operator, MemoryPool memoryPool)
    {
        TaskId otherTaskId = new TaskId(new StageId("other_query", 0), 0, 0);
        memoryPool.reserve(otherTaskId, "test", memoryPool.getFreeBytes());
        assertThat(operator.getOutput()).isNull();
        assertThat(operator.getOperatorContext().isWaitingForMemory().isDone()).isFalse();

        memoryPool.free(otherTaskId, "test", memoryPool.getTaskMemoryReservations().get(otherTaskId));
        assertThat(operator.getOperatorContext().isWaitingForMemory().isDone()).isTrue();
    }

    // consecutive groups of distinct values in descending order; group g holds g * groupSize to (g + 1) * groupSize - 1
    private static Page createGroupsPage(int firstGroup, int groupCount, int groupSize)
    {
        int positionCount = groupCount * groupSize;
        long[] groups = new long[positionCount];
        long[] values = new long[positionCount];
        for (int position = 0; position < positionCount; position++) {
            long group = firstGroup + (position / groupSize);
            groups[position] = group;
            values[position] = ((group + 1) * groupSize) - 1 - (position % groupSize);
        }
        return new Page(
                new LongArrayBlock(positionCount, Optional.empty(), groups),
                new LongArrayBlock(positionCount, Optional.empty(), values));
    }
}
