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
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.trino.ExceededMemoryLimitException;
import io.trino.RowPagesBuilder;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.HashAggregationOperator.HashAggregationOperatorFactory;
import io.trino.operator.aggregation.TestingAggregationFunction;
import io.trino.operator.aggregation.builder.HashAggregationBuilder;
import io.trino.operator.aggregation.builder.InMemoryHashAggregationBuilder;
import io.trino.operator.aggregation.partial.PartialAggregationController;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.plugin.base.metrics.TDigestHistogram;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.PageBuilderStatus;
import io.trino.spi.metrics.Metric;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spiller.Spiller;
import io.trino.spiller.SpillerFactory;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.MaterializedResult;
import io.trino.testing.TestingTaskContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.block.BlockAssertions.createRepeatedValuesBlock;
import static io.trino.operator.AggregationMetrics.INPUT_ROWS_WITH_PARTIAL_AGGREGATION_DISABLED_METRIC_NAME;
import static io.trino.operator.GroupByHashYieldAssertion.GroupByHashYieldResult;
import static io.trino.operator.GroupByHashYieldAssertion.createPages;
import static io.trino.operator.GroupByHashYieldAssertion.finishOperatorWithYieldingGroupByHash;
import static io.trino.operator.OperatorAssertion.assertOperatorEqualsIgnoreOrder;
import static io.trino.operator.OperatorAssertion.assertPagesEqualIgnoreOrder;
import static io.trino.operator.OperatorAssertion.toMaterializedResult;
import static io.trino.operator.OperatorAssertion.toPages;
import static io.trino.operator.SpillMetrics.SPILL_COUNT_METRIC_NAME;
import static io.trino.operator.SpillMetrics.SPILL_DATA_SIZE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static java.lang.String.format;
import static java.util.Collections.emptyIterator;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestHashAggregationOperator
{
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();

    private static final TestingAggregationFunction LONG_AVERAGE = FUNCTION_RESOLUTION.getAggregateFunction("avg", fromTypes(BIGINT));
    private static final TestingAggregationFunction LONG_SUM = FUNCTION_RESOLUTION.getAggregateFunction("sum", fromTypes(BIGINT));
    private static final TestingAggregationFunction COUNT = FUNCTION_RESOLUTION.getAggregateFunction("count", ImmutableList.of());
    private static final TestingAggregationFunction LONG_MIN = FUNCTION_RESOLUTION.getAggregateFunction("min", fromTypes(BIGINT));

    private static final int MAX_BLOCK_SIZE_IN_BYTES = 64 * 1024;

    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
    private final ScheduledExecutorService scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));
    private final FlatHashStrategyCompiler hashStrategyCompiler = new FlatHashStrategyCompiler(new TypeOperators());

    @AfterAll
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testHashAggregation()
    {
        testHashAggregation(true, true, 8, Integer.MAX_VALUE);
        testHashAggregation(true, false, 8, Integer.MAX_VALUE);
        testHashAggregation(false, false, 0, 0);
        testHashAggregation(true, true, 0, 0);
        testHashAggregation(true, false, 0, 0);
        testHashAggregation(true, true, 8, 0);
        testHashAggregation(true, false, 8, 0);
    }

    private void testHashAggregation(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimitForMerge, long memoryLimitForMergeWithMemory)
    {
        DummySpillerFactory spillerFactory = new DummySpillerFactory();

        // make operator produce multiple pages during finish phase
        int numberOfRows = 40_000;
        TestingAggregationFunction countVarcharColumn = FUNCTION_RESOLUTION.getAggregateFunction("count", fromTypes(VARCHAR));
        TestingAggregationFunction countBooleanColumn = FUNCTION_RESOLUTION.getAggregateFunction("count", fromTypes(BOOLEAN));
        TestingAggregationFunction maxVarcharColumn = FUNCTION_RESOLUTION.getAggregateFunction("max", fromTypes(VARCHAR));
        List<Integer> hashChannels = Ints.asList(1);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashChannels, VARCHAR, VARCHAR, VARCHAR, BIGINT, BOOLEAN);
        List<Page> input = rowPagesBuilder
                .addSequencePage(numberOfRows, 100, 0, 100_000, 0, 500)
                .addSequencePage(numberOfRows, 100, 0, 200_000, 0, 500)
                .addSequencePage(numberOfRows, 100, 0, 300_000, 0, 500)
                .build();

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(VARCHAR),
                hashChannels,
                ImmutableList.of(),
                SINGLE,
                false,
                ImmutableList.of(COUNT.createAggregatorFactory(SINGLE, ImmutableList.of(0), OptionalInt.empty()),
                        LONG_SUM.createAggregatorFactory(SINGLE, ImmutableList.of(3), OptionalInt.empty()),
                        LONG_AVERAGE.createAggregatorFactory(SINGLE, ImmutableList.of(3), OptionalInt.empty()),
                        maxVarcharColumn.createAggregatorFactory(SINGLE, ImmutableList.of(2), OptionalInt.empty()),
                        countVarcharColumn.createAggregatorFactory(SINGLE, ImmutableList.of(0), OptionalInt.empty()),
                        countBooleanColumn.createAggregatorFactory(SINGLE, ImmutableList.of(4), OptionalInt.empty())),
                Optional.empty(),
                100_000,
                Optional.of(DataSize.of(16, MEGABYTE)),
                spillEnabled,
                succinctBytes(memoryLimitForMerge),
                succinctBytes(memoryLimitForMergeWithMemory),
                spillerFactory,
                hashStrategyCompiler,
                Optional.empty());

        DriverContext driverContext = createDriverContext(memoryLimitForMerge);

        MaterializedResult.Builder expectedBuilder = resultBuilder(driverContext.getSession(), VARCHAR, BIGINT, BIGINT, DOUBLE, VARCHAR, BIGINT, BIGINT);
        for (int i = 0; i < numberOfRows; ++i) {
            expectedBuilder.row(Integer.toString(i), 3L, 3L * i, (double) i, Integer.toString(300_000 + i), 3L, 3L);
        }
        MaterializedResult expected = expectedBuilder.build();

        List<Page> pages = toPages(operatorFactory, driverContext, input, revokeMemoryWhenAddingPages);
        assertThat(pages).as("Expected more than one output page").hasSizeGreaterThan(1);
        assertPagesEqualIgnoreOrder(driverContext, pages, expected);

        assertThat(spillEnabled == (spillerFactory.getSpillsCount() > 0))
                .describedAs(format("Spill state mismatch. Expected spill: %s, spill count: %s", spillEnabled, spillerFactory.getSpillsCount()))
                .isTrue();
    }

    @Test
    public void testHashAggregationWithGlobals()
    {
        testHashAggregationWithGlobals(true, true, 8, Integer.MAX_VALUE);
        testHashAggregationWithGlobals(true, false, 8, Integer.MAX_VALUE);
        testHashAggregationWithGlobals(false, false, 0, 0);
        testHashAggregationWithGlobals(true, true, 0, 0);
        testHashAggregationWithGlobals(true, false, 0, 0);
        testHashAggregationWithGlobals(true, true, 8, 0);
        testHashAggregationWithGlobals(true, false, 8, 0);
    }

    private void testHashAggregationWithGlobals(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimitForMerge, long memoryLimitForMergeWithMemory)
    {
        DummySpillerFactory spillerFactory = new DummySpillerFactory();

        TestingAggregationFunction countVarcharColumn = FUNCTION_RESOLUTION.getAggregateFunction("count", fromTypes(VARCHAR));
        TestingAggregationFunction countBooleanColumn = FUNCTION_RESOLUTION.getAggregateFunction("count", fromTypes(BOOLEAN));
        TestingAggregationFunction maxVarcharColumn = FUNCTION_RESOLUTION.getAggregateFunction("max", fromTypes(VARCHAR));

        Optional<Integer> groupIdChannel = Optional.of(1);
        List<Integer> groupByChannels = Ints.asList(1, 2);
        List<Integer> globalAggregationGroupIds = Ints.asList(42, 49);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(groupByChannels, VARCHAR, VARCHAR, VARCHAR, BIGINT, BIGINT, BOOLEAN);
        List<Page> input = rowPagesBuilder.build();

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(VARCHAR, BIGINT),
                groupByChannels,
                globalAggregationGroupIds,
                SINGLE,
                true,
                ImmutableList.of(COUNT.createAggregatorFactory(SINGLE, ImmutableList.of(0), OptionalInt.empty()),
                        LONG_MIN.createAggregatorFactory(SINGLE, ImmutableList.of(4), OptionalInt.empty()),
                        LONG_AVERAGE.createAggregatorFactory(SINGLE, ImmutableList.of(4), OptionalInt.empty()),
                        maxVarcharColumn.createAggregatorFactory(SINGLE, ImmutableList.of(2), OptionalInt.empty()),
                        countVarcharColumn.createAggregatorFactory(SINGLE, ImmutableList.of(0), OptionalInt.empty()),
                        countBooleanColumn.createAggregatorFactory(SINGLE, ImmutableList.of(5), OptionalInt.empty())),
                groupIdChannel,
                100_000,
                Optional.of(DataSize.of(16, MEGABYTE)),
                spillEnabled,
                succinctBytes(memoryLimitForMerge),
                succinctBytes(memoryLimitForMergeWithMemory),
                spillerFactory,
                hashStrategyCompiler,
                Optional.empty());

        DriverContext driverContext = createDriverContext(memoryLimitForMerge);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, BIGINT, BIGINT, BIGINT, DOUBLE, VARCHAR, BIGINT, BIGINT)
                .row(null, 42L, 0L, null, null, null, 0L, 0L)
                .row(null, 49L, 0L, null, null, null, 0L, 0L)
                .build();

        assertOperatorEqualsIgnoreOrder(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test
    public void testHashAggregationMemoryReservation()
    {
        testHashAggregationMemoryReservation(true, true, 8, Integer.MAX_VALUE);
        testHashAggregationMemoryReservation(true, false, 8, Integer.MAX_VALUE);
        testHashAggregationMemoryReservation(false, false, 0, 0);
        testHashAggregationMemoryReservation(true, true, 0, 0);
        testHashAggregationMemoryReservation(true, false, 0, 0);
        testHashAggregationMemoryReservation(true, true, 8, 0);
        testHashAggregationMemoryReservation(true, false, 8, 0);
    }

    private void testHashAggregationMemoryReservation(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimitForMerge, long memoryLimitForMergeWithMemory)
    {
        DummySpillerFactory spillerFactory = new DummySpillerFactory();

        TestingAggregationFunction arrayAggColumn = FUNCTION_RESOLUTION.getAggregateFunction("array_agg", fromTypes(BIGINT));

        List<Integer> hashChannels = Ints.asList(1);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashChannels, BIGINT, BIGINT);
        List<Page> input = rowPagesBuilder
                .addSequencePage(10, 100, 0)
                .addSequencePage(10, 200, 0)
                .addSequencePage(10, 300, 0)
                .build();

        DriverContext driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION, DataSize.of(11, Unit.MEGABYTE))
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT),
                hashChannels,
                ImmutableList.of(),
                SINGLE,
                true,
                ImmutableList.of(arrayAggColumn.createAggregatorFactory(SINGLE, ImmutableList.of(0), OptionalInt.empty())),
                Optional.empty(),
                100_000,
                Optional.of(DataSize.of(16, MEGABYTE)),
                spillEnabled,
                succinctBytes(memoryLimitForMerge),
                succinctBytes(memoryLimitForMergeWithMemory),
                spillerFactory,
                hashStrategyCompiler,
                Optional.empty());

        Operator operator = operatorFactory.createOperator(driverContext);
        toPages(operator, input.iterator(), revokeMemoryWhenAddingPages);
        // TODO (https://github.com/trinodb/trino/issues/10596): it should be 0, since operator is finished
        assertThat(operator.getOperatorContext().getOperatorStats().getUserMemoryReservation().toBytes()).isEqualTo(spillEnabled && revokeMemoryWhenAddingPages ? 4752672 : 0);
        assertThat(operator.getOperatorContext().getOperatorStats().getRevocableMemoryReservation().toBytes()).isEqualTo(0);
    }

    @Test
    public void testMemoryLimit()
    {
        assertThatThrownBy(() -> {
            TestingAggregationFunction maxVarcharColumn = FUNCTION_RESOLUTION.getAggregateFunction("max", fromTypes(VARCHAR));

            List<Integer> hashChannels = Ints.asList(1);
            RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashChannels, VARCHAR, BIGINT, VARCHAR, BIGINT);
            List<Page> input = rowPagesBuilder
                    .addSequencePage(10, 100, 0, 100, 0)
                    .addSequencePage(10, 100, 0, 200, 0)
                    .addSequencePage(10, 100, 0, 300, 0)
                    .build();

            DriverContext driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION, DataSize.ofBytes(10))
                    .addPipelineContext(0, true, true, false)
                    .addDriverContext();

            HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                    0,
                    new PlanNodeId("test"),
                    ImmutableList.of(BIGINT),
                    hashChannels,
                    ImmutableList.of(),
                    SINGLE,
                    ImmutableList.of(COUNT.createAggregatorFactory(SINGLE, ImmutableList.of(0), OptionalInt.empty()),
                            LONG_MIN.createAggregatorFactory(SINGLE, ImmutableList.of(3), OptionalInt.empty()),
                            LONG_AVERAGE.createAggregatorFactory(SINGLE, ImmutableList.of(3), OptionalInt.empty()),
                            maxVarcharColumn.createAggregatorFactory(SINGLE, ImmutableList.of(2), OptionalInt.empty())),
                    Optional.empty(),
                    100_000,
                    Optional.of(DataSize.of(16, MEGABYTE)),
                    hashStrategyCompiler,
                    Optional.empty());

            toPages(operatorFactory, driverContext, input);
        })
                .isInstanceOf(ExceededMemoryLimitException.class)
                .hasMessageMatching("Query exceeded per-node memory limit of 10B.*");
    }

    @Test
    public void testHashBuilderResize()
    {
        testHashBuilderResize(true, true, 8, Integer.MAX_VALUE);
        testHashBuilderResize(true, false, 8, Integer.MAX_VALUE);
        testHashBuilderResize(false, false, 0, 0);
        testHashBuilderResize(true, true, 0, 0);
        testHashBuilderResize(true, false, 0, 0);
        testHashBuilderResize(true, true, 8, 0);
        testHashBuilderResize(true, false, 8, 0);
    }

    private void testHashBuilderResize(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimitForMerge, long memoryLimitForMergeWithMemory)
    {
        DummySpillerFactory spillerFactory = new DummySpillerFactory();

        BlockBuilder builder = VARCHAR.createBlockBuilder(null, 1, MAX_BLOCK_SIZE_IN_BYTES);
        VARCHAR.writeSlice(builder, Slices.allocate(200_000)); // this must be larger than MAX_BLOCK_SIZE_IN_BYTES, 64K
        builder.build();

        List<Integer> hashChannels = Ints.asList(0);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashChannels, VARCHAR);
        List<Page> input = rowPagesBuilder
                .addSequencePage(10, 100)
                .addBlocksPage(builder.build())
                .addSequencePage(10, 100)
                .build();

        DriverContext driverContext = createDriverContext(memoryLimitForMerge);

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(VARCHAR),
                hashChannels,
                ImmutableList.of(),
                SINGLE,
                false,
                ImmutableList.of(COUNT.createAggregatorFactory(SINGLE, ImmutableList.of(0), OptionalInt.empty())),
                Optional.empty(),
                100_000,
                Optional.of(DataSize.of(16, MEGABYTE)),
                spillEnabled,
                succinctBytes(memoryLimitForMerge),
                succinctBytes(memoryLimitForMergeWithMemory),
                spillerFactory,
                hashStrategyCompiler,
                Optional.empty());

        toPages(operatorFactory, driverContext, input, revokeMemoryWhenAddingPages);
    }

    @Test
    public void testMemoryReservationYield()
    {
        testMemoryReservationYield(VARCHAR);
        testMemoryReservationYield(BIGINT);
    }

    public void testMemoryReservationYield(Type type)
    {
        List<Page> input = createPages(type, 6_000, 600);
        OperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(type),
                ImmutableList.of(0),
                ImmutableList.of(),
                SINGLE,
                ImmutableList.of(COUNT.createAggregatorFactory(SINGLE, ImmutableList.of(0), OptionalInt.empty())),
                Optional.of(1),
                1,
                Optional.of(DataSize.of(16, MEGABYTE)),
                hashStrategyCompiler,
                Optional.empty());

        // get result with yield; pick a relatively small buffer for aggregator's memory usage
        GroupByHashYieldResult result;
        result = finishOperatorWithYieldingGroupByHash(input, type, operatorFactory, this::getHashCapacity, 450_000);
        assertThat(result.yieldCount()).isGreaterThanOrEqualTo(5);
        assertThat(result.maxReservedBytes()).isGreaterThanOrEqualTo(20L << 20);

        int count = 0;
        for (Page page : result.output()) {
            // value + aggregation result
            assertThat(page.getChannelCount()).isEqualTo(2);
            for (int i = 0; i < page.getPositionCount(); i++) {
                assertThat(BIGINT.getLong(page.getBlock(1), i)).isEqualTo(1);
                count++;
            }
        }
        assertThat(count).isEqualTo(6_000 * 600);
    }

    @Test
    public void testHashBuilderResizeLimit()
    {
        assertThatThrownBy(() -> {
            BlockBuilder builder = VARCHAR.createBlockBuilder(null, 1, MAX_BLOCK_SIZE_IN_BYTES);
            VARCHAR.writeSlice(builder, Slices.allocate(5_000_000)); // this must be larger than MAX_BLOCK_SIZE_IN_BYTES, 64K
            builder.build();

            List<Integer> hashChannels = Ints.asList(0);
            RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashChannels, VARCHAR);
            List<Page> input = rowPagesBuilder
                    .addSequencePage(10, 100)
                    .addBlocksPage(builder.build())
                    .addSequencePage(10, 100)
                    .build();

            DriverContext driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION, DataSize.of(3, MEGABYTE))
                    .addPipelineContext(0, true, true, false)
                    .addDriverContext();

            HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                    0,
                    new PlanNodeId("test"),
                    ImmutableList.of(VARCHAR),
                    hashChannels,
                    ImmutableList.of(),
                    SINGLE,
                    ImmutableList.of(COUNT.createAggregatorFactory(SINGLE, ImmutableList.of(0), OptionalInt.empty())),
                    Optional.empty(),
                    100_000,
                    Optional.of(DataSize.of(16, MEGABYTE)),
                    hashStrategyCompiler,
                    Optional.empty());

            toPages(operatorFactory, driverContext, input);
        })
                .isInstanceOf(ExceededMemoryLimitException.class)
                .hasMessageMatching("Query exceeded per-node memory limit of 3MB.*");
    }

    @Test
    public void testMultiSliceAggregationOutput()
    {
        // estimate the number of entries required to create 1.5 pages of results
        // See InMemoryHashAggregationBuilder.buildTypes()
        int fixedWidthSize = SIZE_OF_LONG + SIZE_OF_LONG +  // Used by BigintGroupByHash, see BigintGroupByHash.TYPES_WITH_RAW_HASH
                SIZE_OF_LONG + SIZE_OF_DOUBLE;              // Used by COUNT and LONG_AVERAGE aggregators;
        int multiSlicePositionCount = (int) (1.5 * PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES / fixedWidthSize);

        List<Integer> hashChannels = Ints.asList(1);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashChannels, BIGINT, BIGINT);
        List<Page> input = rowPagesBuilder
                .addSequencePage(multiSlicePositionCount, 0, 0)
                .build();

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT),
                hashChannels,
                ImmutableList.of(),
                SINGLE,
                ImmutableList.of(COUNT.createAggregatorFactory(SINGLE, ImmutableList.of(0), OptionalInt.empty()),
                        LONG_AVERAGE.createAggregatorFactory(SINGLE, ImmutableList.of(1), OptionalInt.empty())),
                Optional.empty(),
                100_000,
                Optional.of(DataSize.of(16, MEGABYTE)),
                hashStrategyCompiler,
                Optional.empty());

        assertThat(toPages(operatorFactory, createDriverContext(), input)).hasSize(2);
    }

    @Test
    public void testMultiplePartialFlushes()
            throws Exception
    {
        List<Integer> hashChannels = Ints.asList(0);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashChannels, BIGINT);
        List<Page> input = rowPagesBuilder
                .addSequencePage(500, 0)
                .addSequencePage(500, 500)
                .addSequencePage(500, 1000)
                .addSequencePage(500, 1500)
                .build();

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT),
                hashChannels,
                ImmutableList.of(),
                PARTIAL,
                ImmutableList.of(LONG_MIN.createAggregatorFactory(PARTIAL, ImmutableList.of(0), OptionalInt.empty())),
                Optional.empty(),
                100_000,
                Optional.of(DataSize.of(1, KILOBYTE)),
                hashStrategyCompiler,
                Optional.empty());

        DriverContext driverContext = createDriverContext(1024);

        try (Operator operator = operatorFactory.createOperator(driverContext)) {
            List<Page> expectedPages = rowPagesBuilder(BIGINT, BIGINT)
                    .addSequencePage(2000, 0, 0)
                    .build();
            MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, BIGINT)
                    .pages(expectedPages)
                    .build();

            Iterator<Page> inputIterator = input.iterator();

            // Fill up the aggregation
            while (operator.needsInput() && inputIterator.hasNext()) {
                operator.addInput(inputIterator.next());
            }

            assertThat(driverContext.getMemoryUsage()).isGreaterThan(0);

            // Drain the output (partial flush)
            List<Page> outputPages = new ArrayList<>();
            while (true) {
                Page output = operator.getOutput();
                if (output == null) {
                    break;
                }
                outputPages.add(output);
            }

            // There should be some pages that were drained
            assertThat(outputPages).isNotEmpty();

            // The operator need input again since this was a partial flush
            assertThat(operator.needsInput()).isTrue();

            // Now, drive the operator to completion
            outputPages.addAll(toPages(operator, inputIterator));

            MaterializedResult actual = toMaterializedResult(operator.getOperatorContext().getSession(), expected.getTypes(), outputPages);

            assertThat(actual.getTypes()).isEqualTo(expected.getTypes());
            assertThat(actual.getMaterializedRows()).containsExactlyInAnyOrderElementsOf(expected.getMaterializedRows());
        }

        assertThat(driverContext.getMemoryUsage()).isEqualTo(0);
        assertThat(driverContext.getRevocableMemoryUsage()).isEqualTo(0);
    }

    @Test
    public void testMergeWithMemorySpill()
    {
        DummySpillerFactory spillerFactory = new DummySpillerFactory();

        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(BIGINT);

        int smallPagesSpillThresholdSize = 150000;

        List<Page> input = rowPagesBuilder
                .addSequencePage(smallPagesSpillThresholdSize, 0)
                .addSequencePage(10, smallPagesSpillThresholdSize)
                .build();

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT),
                ImmutableList.of(0),
                ImmutableList.of(),
                SINGLE,
                false,
                ImmutableList.of(LONG_MIN.createAggregatorFactory(SINGLE, ImmutableList.of(0), OptionalInt.empty())),
                Optional.empty(),
                1,
                Optional.of(DataSize.of(16, MEGABYTE)),
                true,
                DataSize.ofBytes(smallPagesSpillThresholdSize),
                succinctBytes(Integer.MAX_VALUE),
                spillerFactory,
                hashStrategyCompiler,
                Optional.empty());

        DriverContext driverContext = createDriverContext(smallPagesSpillThresholdSize);

        MaterializedResult.Builder resultBuilder = resultBuilder(driverContext.getSession(), BIGINT, BIGINT);
        for (int i = 0; i < smallPagesSpillThresholdSize + 10; ++i) {
            resultBuilder.row((long) i, (long) i);
        }

        assertOperatorEqualsIgnoreOrder(operatorFactory, driverContext, input, resultBuilder.build());
    }

    @Test
    public void testSpillMetricsRecorded()
    {
        /*
         * Force the operator to spill by setting ridiculous per-operator memory
         * limits (8 bytes) and feeding it more rows than can possibly stay in
         * memory.  After the run we assert that the driver-level metric map
         * contains the histogram entries produced by SpillMetrics – and that
         * their counts/values are strictly positive.
         */

        DummySpillerFactory spillerFactory = new DummySpillerFactory();

        RowPagesBuilder pages = rowPagesBuilder(BIGINT)
                // ~0.8 MB of data – comfortably larger than the 8 B quota
                .addSequencePage(50_000, 0);

        HashAggregationOperatorFactory factory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("spill-metrics"),
                ImmutableList.of(BIGINT),
                ImmutableList.of(0),
                ImmutableList.of(),
                SINGLE,
                false,
                ImmutableList.of(LONG_MIN.createAggregatorFactory(SINGLE, ImmutableList.of(0), OptionalInt.empty())),
                Optional.empty(),
                10,
                Optional.of(DataSize.of(16, MEGABYTE)),
                /* spill enabled */ true,
                /* memoryLimitForMerge */ DataSize.ofBytes(8),
                /* memoryLimitForMergeWithMemory */ succinctBytes(Integer.MAX_VALUE),
                spillerFactory,
                hashStrategyCompiler,
                Optional.empty());

        DriverContext context = createDriverContext(8);

        // run the operator; we don’t care about the output pages here
        toPages(factory, context, pages.build());

        Metrics metrics = context.getDriverStats().getOperatorStats().get(0).getMetrics();
        Metric<?> spillCountMetric = metrics.getMetrics().get(SPILL_COUNT_METRIC_NAME);
        Metric<?> spillSizeMetric = metrics.getMetrics().get(SPILL_DATA_SIZE);

        assertThat(spillCountMetric).describedAs("metric present").isNotNull();
        assertThat(spillSizeMetric).describedAs("metric present").isNotNull();

        TDigestHistogram spillCountHistogram = (TDigestHistogram) spillCountMetric;
        TDigestHistogram spillSizeHist = (TDigestHistogram) spillSizeMetric;

        assertThat(spillCountHistogram.getDigest().getCount())
                .describedAs("exact number of spills recorded")
                .isEqualTo(spillerFactory.getSpillsCount());

        assertThat(spillSizeHist.getDigest().getCount())
                .describedAs("histogram contains at least one entry")
                .isGreaterThan(0);
    }

    @Test
    public void testSpillerFailure()
    {
        TestingAggregationFunction maxVarcharColumn = FUNCTION_RESOLUTION.getAggregateFunction("max", fromTypes(VARCHAR));

        List<Integer> hashChannels = Ints.asList(1);
        List<Type> types = ImmutableList.of(VARCHAR, BIGINT, VARCHAR, BIGINT);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashChannels, types);
        List<Page> input = rowPagesBuilder
                .addSequencePage(10, 100, 0, 100, 0)
                // current accumulator allows 1024 values without using revocable memory, so add enough values to cause revocable memory usage
                .addSequencePage(2_000, 100, 0, 200, 0)
                .addSequencePage(10, 100, 0, 300, 0)
                .build();

        DriverContext driverContext = TestingTaskContext.builder(executor, scheduledExecutor, TEST_SESSION)
                .setQueryMaxMemory(DataSize.valueOf("7MB"))
                .setMemoryPoolSize(DataSize.valueOf("1GB"))
                .build()
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT),
                hashChannels,
                ImmutableList.of(),
                SINGLE,
                false,
                ImmutableList.of(COUNT.createAggregatorFactory(SINGLE, ImmutableList.of(0), OptionalInt.empty()),
                        LONG_MIN.createAggregatorFactory(SINGLE, ImmutableList.of(3), OptionalInt.empty()),
                        LONG_AVERAGE.createAggregatorFactory(SINGLE, ImmutableList.of(3), OptionalInt.empty()),
                        maxVarcharColumn.createAggregatorFactory(SINGLE, ImmutableList.of(2), OptionalInt.empty())),
                Optional.empty(),
                100_000,
                Optional.of(DataSize.of(16, MEGABYTE)),
                true,
                succinctBytes(8),
                succinctBytes(Integer.MAX_VALUE),
                new FailingSpillerFactory(),
                hashStrategyCompiler,
                Optional.empty());

        assertThatThrownBy(() -> toPages(operatorFactory, driverContext, input))
                .isInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(IOException.class)
                .hasMessageEndingWith("Failed to spill");
    }

    @Test
    public void testMemoryTracking()
            throws Exception
    {
        List<Integer> hashChannels = Ints.asList(0);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashChannels, BIGINT);
        Page input = getOnlyElement(rowPagesBuilder.addSequencePage(500, 0).build());

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT),
                hashChannels,
                ImmutableList.of(),
                SINGLE,
                ImmutableList.of(LONG_MIN.createAggregatorFactory(SINGLE, ImmutableList.of(0), OptionalInt.empty())),
                Optional.empty(),
                100_000,
                Optional.of(DataSize.of(16, MEGABYTE)),
                hashStrategyCompiler,
                Optional.empty());

        DriverContext driverContext = createDriverContext(1024);

        try (Operator operator = operatorFactory.createOperator(driverContext)) {
            assertThat(operator.needsInput()).isTrue();
            operator.addInput(input);

            assertThat(driverContext.getMemoryUsage()).isGreaterThan(0);

            toPages(operator, emptyIterator());
        }

        assertThat(driverContext.getMemoryUsage()).isEqualTo(0);
        assertThat(driverContext.getRevocableMemoryUsage()).isEqualTo(0);
    }

    @Test
    public void testAdaptivePartialAggregation()
    {
        List<Integer> hashChannels = Ints.asList(0);

        DataSize maxPartialMemory = DataSize.ofBytes(1);
        PartialAggregationController partialAggregationController = new PartialAggregationController(maxPartialMemory, 0.8);
        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT),
                hashChannels,
                ImmutableList.of(),
                PARTIAL,
                ImmutableList.of(LONG_MIN.createAggregatorFactory(PARTIAL, ImmutableList.of(0), OptionalInt.empty())),
                Optional.empty(),
                100,
                Optional.of(maxPartialMemory), // this setting makes operator to flush after each page
                hashStrategyCompiler,
                // 1 byte maxPartialMemory causes adaptive partial aggregation to be triggered after each page flush
                Optional.of(partialAggregationController));

        // at the start partial aggregation is enabled
        assertThat(partialAggregationController.isPartialAggregationDisabled()).isFalse();
        // First operator will trigger adaptive partial aggregation after the first page
        List<Page> operator1Input = rowPagesBuilder(hashChannels, BIGINT)
                .addBlocksPage(createLongsBlock(0, 1, 2, 3, 4, 5, 6, 7, 8, 8)) // first page will be hashed but the values are almost unique, so it will trigger adaptation
                .addBlocksPage(createRepeatedValuesBlock(1, 10)) // second page would be hashed to existing value 1. but if adaptive PA kicks in, the raw values will be passed on
                .build();
        List<Page> operator1Expected = rowPagesBuilder(BIGINT, BIGINT)
                .addBlocksPage(createLongsBlock(0, 1, 2, 3, 4, 5, 6, 7, 8), createLongsBlock(0, 1, 2, 3, 4, 5, 6, 7, 8)) // the last position was aggregated
                .addBlocksPage(createRepeatedValuesBlock(1, 10), createRepeatedValuesBlock(1, 10)) // we are expecting second page with raw values
                .build();
        assertOperatorEquals(operatorFactory, operator1Input, operator1Expected);

        // the first operator flush disables partial aggregation
        assertThat(partialAggregationController.isPartialAggregationDisabled()).isTrue();
        // second operator using the same factory, reuses PartialAggregationControl, so it will only produce raw pages (partial aggregation is disabled at this point)
        List<Page> operator2Input = rowPagesBuilder(hashChannels, BIGINT)
                .addBlocksPage(createRepeatedValuesBlock(1, 10))
                .addBlocksPage(createRepeatedValuesBlock(2, 10))
                .build();
        List<Page> operator2Expected = rowPagesBuilder(BIGINT, BIGINT)
                .addBlocksPage(createRepeatedValuesBlock(1, 10), createRepeatedValuesBlock(1, 10))
                .addBlocksPage(createRepeatedValuesBlock(2, 10), createRepeatedValuesBlock(2, 10))
                .build();
        assertOperatorEquals(operatorFactory, operator2Input, operator2Expected);

        // partial aggregation should be enabled again after enough data is processed
        for (int i = 1; i <= 4; ++i) {
            List<Page> operatorInput = rowPagesBuilder(hashChannels, BIGINT)
                    .addBlocksPage(createLongsBlock(0, 1, 2, 3, 4, 5, 6, 7, 8))
                    .build();
            List<Page> operatorExpected = rowPagesBuilder(BIGINT, BIGINT)
                    .addBlocksPage(createLongsBlock(0, 1, 2, 3, 4, 5, 6, 7, 8), createLongsBlock(0, 1, 2, 3, 4, 5, 6, 7, 8))
                    .build();
            assertOperatorEquals(operatorFactory, operatorInput, operatorExpected);
            if (i <= 3) {
                assertThat(partialAggregationController.isPartialAggregationDisabled()).isTrue();
            }
            else {
                assertThat(partialAggregationController.isPartialAggregationDisabled()).isFalse();
            }
        }

        // partial aggregation should still be enabled even after some late flush comes from disabled PA
        partialAggregationController.onFlush(1_000_000, 1_000_000, OptionalLong.empty());

        // partial aggregation should keep being enabled after good reduction has been observed
        List<Page> operator3Input = rowPagesBuilder(hashChannels, BIGINT)
                .addBlocksPage(createRepeatedValuesBlock(1, 100))
                .addBlocksPage(createRepeatedValuesBlock(2, 100))
                .build();
        List<Page> operator3Expected = rowPagesBuilder(BIGINT, BIGINT)
                .addBlocksPage(createRepeatedValuesBlock(1, 1), createRepeatedValuesBlock(1, 1))
                .addBlocksPage(createRepeatedValuesBlock(2, 1), createRepeatedValuesBlock(2, 1))
                .build();
        assertOperatorEquals(operatorFactory, operator3Input, operator3Expected);
        assertThat(partialAggregationController.isPartialAggregationDisabled()).isFalse();
    }

    @Test
    public void testAdaptivePartialAggregationTriggeredOnlyOnFlush()
    {
        List<Integer> hashChannels = Ints.asList(0);

        PartialAggregationController partialAggregationController = new PartialAggregationController(DataSize.ofBytes(1), 0.8);
        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT),
                hashChannels,
                ImmutableList.of(),
                PARTIAL,
                ImmutableList.of(LONG_MIN.createAggregatorFactory(PARTIAL, ImmutableList.of(0), OptionalInt.empty())),
                Optional.empty(),
                10,
                Optional.of(DataSize.of(16, MEGABYTE)), // this setting makes operator to flush only after all pages
                hashStrategyCompiler,
                // 1 byte maxPartialMemory causes adaptive partial aggregation to be triggered after each page flush
                Optional.of(partialAggregationController));

        DriverContext driverContext = createDriverContext(1024);
        List<Page> operator1Input = rowPagesBuilder(hashChannels, BIGINT)
                .addSequencePage(10, 0) // first page are unique values, so it would trigger adaptation, but it won't because flush is not called
                .addBlocksPage(createRepeatedValuesBlock(1, 2)) // second page will be hashed to existing value 1
                .build();
        // the total unique ows ratio for the first operator will be 10/12 so > 0.8 (adaptive partial aggregation uniqueRowsRatioThreshold)
        List<Page> operator1Expected = rowPagesBuilder(BIGINT, BIGINT)
                .addSequencePage(10, 0, 0) // we are expecting second page to be squashed with the first
                .build();
        assertOperatorEquals(driverContext, operatorFactory, operator1Input, operator1Expected);

        // the first operator flush disables partial aggregation
        assertThat(partialAggregationController.isPartialAggregationDisabled()).isTrue();
        assertInputRowsWithPartialAggregationDisabled(driverContext, 0);

        // second operator using the same factory, reuses PartialAggregationControl, so it will only produce raw pages (partial aggregation is disabled at this point)
        List<Page> operator2Input = rowPagesBuilder(hashChannels, BIGINT)
                .addBlocksPage(createRepeatedValuesBlock(1, 10))
                .addBlocksPage(createRepeatedValuesBlock(2, 10))
                .build();
        List<Page> operator2Expected = rowPagesBuilder(BIGINT, BIGINT)
                .addBlocksPage(createRepeatedValuesBlock(1, 10), createRepeatedValuesBlock(1, 10))
                .addBlocksPage(createRepeatedValuesBlock(2, 10), createRepeatedValuesBlock(2, 10))
                .build();

        driverContext = createDriverContext(1024);
        assertOperatorEquals(driverContext, operatorFactory, operator2Input, operator2Expected);
        assertInputRowsWithPartialAggregationDisabled(driverContext, 20);
    }

    @Test
    public void testAsyncSpillBlocksAndUnblocksDriver()
            throws Exception
    {
        /*
         *  – force:  revocable bytes   > 0
         *            spiller.present  == true
         *            shouldMergeWithMemory(size) == false
         *
         *  so buildResult() will hit `blocked(spillToDisk())`
         */
        SlowSpiller spiller = new SlowSpiller();
        SlowSpillerFactory spillerFactory = new SlowSpillerFactory(spiller);

        //  tiny memory limits → convert-to-user will fail
        long memoryLimitForMerge = 8;
        long memoryLimitForMergeWithMemory = 0;

        // plenty of rows → revocable mem >
        RowPagesBuilder pages = rowPagesBuilder(Ints.asList(0), BIGINT)
                .addSequencePage(5_000, 0);

        HashAggregationOperatorFactory factory =
                new HashAggregationOperatorFactory(
                        0,
                        new PlanNodeId("async"),
                        ImmutableList.of(BIGINT),
                        Ints.asList(0),
                        ImmutableList.of(),
                        SINGLE,
                        false,
                        ImmutableList.of(COUNT.createAggregatorFactory(SINGLE, ImmutableList.of(0), OptionalInt.empty())),
                        Optional.empty(),
                        /* expectedGroups */ 1,
                        Optional.of(DataSize.of(16, MEGABYTE)),
                        /* spill enabled */  true,
                        succinctBytes(memoryLimitForMerge),
                        succinctBytes(memoryLimitForMergeWithMemory),
                        spillerFactory,
                        hashStrategyCompiler,
                        Optional.empty());

        DriverContext context = createDriverContext(memoryLimitForMerge);

        try (Operator operator = factory.createOperator(context)) {
            // feed all input
            for (Page page : pages.build()) {
                assertThat(operator.needsInput()).isTrue();
                operator.addInput(page);
            }
            operator.finish();

            // first call returns null, operator is now blocked
            assertThat(operator.getOutput()).isNull();
            ListenableFuture<Void> blocked = operator.isBlocked();
            assertThat(blocked.isDone()).isFalse();

            // unblock the spiller
            spiller.complete();

            // driver sees the unblock
            blocked.get();
            // drive operator to completion
            toPages(operator, emptyIterator());
            assertThat(operator.isFinished())
                    .as("operator must finish after async spill")
                    .isTrue();
        }
    }

    @Test
    public void testRevocableMemoryConvertedAfterAsyncSpill()
            throws Exception
    {
        long memoryLimitForMerge = DataSize.of(64, KILOBYTE).toBytes();   // force spill early
        long memoryLimitForMergeWithMemory = 0;                                     // make shouldMergeWithMemory() return false

        // plenty of rows to allocate >64 kB in the hash builder
        RowPagesBuilder pagesBuilder = rowPagesBuilder(Ints.asList(0), BIGINT)
                .addSequencePage(50_000, 0);

        SlowSpiller slowSpiller = new SlowSpiller();
        SlowSpillerFactory slowSpillerFactory = new SlowSpillerFactory(slowSpiller);

        HashAggregationOperatorFactory factory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("async-spill"),
                ImmutableList.of(BIGINT),
                Ints.asList(0),
                ImmutableList.of(),
                SINGLE,
                false,
                ImmutableList.of(
                        COUNT.createAggregatorFactory(SINGLE,
                                ImmutableList.of(0),
                                OptionalInt.empty())),
                Optional.empty(),
                10,
                Optional.of(DataSize.of(16, MEGABYTE)),
                /* spill enabled */ true,
                DataSize.ofBytes(memoryLimitForMerge),
                DataSize.ofBytes(memoryLimitForMergeWithMemory),
                slowSpillerFactory,
                hashStrategyCompiler,
                Optional.empty());

        DriverContext context = createDriverContext(memoryLimitForMerge);

        try (Operator operator = factory.createOperator(context)) {
            for (Page page : pagesBuilder.build()) {
                operator.addInput(page);
            }
            operator.finish();

            // first call returns null, operator is now blocked
            assertThat(operator.getOutput()).isNull();
            ListenableFuture<Void> blocked = operator.isBlocked();
            assertThat(blocked.isDone()).isFalse();

            assertThat(context.getRevocableMemoryUsage())
                    .as("revocable bytes should be > 0 while spill is running")
                    .isGreaterThan(0L);

            // complete the spill asynchronously
            slowSpiller.complete();        // finish spill
            blocked.get();                 // wait until operator is unblocked

            // advance state
            operator.getOutput();
            // revocable memory must have been cleared by updateMemory()
            long revocableAfterSpill = context.getRevocableMemoryUsage();
            assertThat(revocableAfterSpill)
                    .as("revocable bytes must be 0 right after spill completion")
                    .isZero();

            // drive operator to completion
            toPages(operator, emptyIterator());

            assertThat(operator.isFinished()).isTrue();

            // all reservations are released at the very end
            assertThat(context.getRevocableMemoryUsage()).isZero();
            assertThat(context.getMemoryUsage()).isZero();
        }
    }

    private static class SlowSpillerFactory
            implements SpillerFactory
    {
        private final SlowSpiller spiller;

        SlowSpillerFactory(SlowSpiller spiller)
        {
            this.spiller = spiller;
        }

        @Override
        public Spiller create(List<Type> t, SpillContext sc, AggregatedMemoryContext mc)
        {
            return spiller;
        }
    }

    private static class SlowSpiller
            implements Spiller
    {
        private final SettableFuture<DataSize> future = SettableFuture.create();

        @Override
        public ListenableFuture<DataSize> spill(Iterator<Page> i)
        {
            return future;
        }

        @Override
        public List<Iterator<Page>> getSpills()
        {
            return ImmutableList.of();
        }

        @Override
        public void close() {}

        void complete()
        {
            future.set(DataSize.ofBytes(0));
        }
    }

    private void assertInputRowsWithPartialAggregationDisabled(DriverContext context, long expectedRowCount)
    {
        LongCount metric = ((LongCount) context.getDriverStats().getOperatorStats().get(0).getMetrics().getMetrics().get(INPUT_ROWS_WITH_PARTIAL_AGGREGATION_DISABLED_METRIC_NAME));
        if (metric == null) {
            assertThat(0).isEqualTo(expectedRowCount);
        }
        else {
            assertThat(metric.getTotal()).isEqualTo(expectedRowCount);
        }
    }

    private void assertOperatorEquals(OperatorFactory operatorFactory, List<Page> input, List<Page> expectedPages)
    {
        assertOperatorEquals(createDriverContext(1024), operatorFactory, input, expectedPages);
    }

    private void assertOperatorEquals(DriverContext driverContext, OperatorFactory operatorFactory, List<Page> input, List<Page> expectedPages)
    {
        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, BIGINT)
                .pages(expectedPages)
                .build();
        OperatorAssertion.assertOperatorEquals(operatorFactory, driverContext, input, expected, false, false);
    }

    private DriverContext createDriverContext()
    {
        return createDriverContext(Integer.MAX_VALUE);
    }

    private DriverContext createDriverContext(long memoryLimit)
    {
        return TestingTaskContext.builder(executor, scheduledExecutor, TEST_SESSION)
                .setMemoryPoolSize(succinctBytes(memoryLimit))
                .build()
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }

    private int getHashCapacity(Operator operator)
    {
        assertThat(operator instanceof HashAggregationOperator).isTrue();
        HashAggregationBuilder aggregationBuilder = ((HashAggregationOperator) operator).getAggregationBuilder();
        if (aggregationBuilder == null) {
            return 0;
        }
        assertThat(aggregationBuilder).isInstanceOf(InMemoryHashAggregationBuilder.class);
        return ((InMemoryHashAggregationBuilder) aggregationBuilder).getCapacity();
    }

    private static class FailingSpillerFactory
            implements SpillerFactory
    {
        @Override
        public Spiller create(List<Type> types, SpillContext spillContext, AggregatedMemoryContext memoryContext)
        {
            return new Spiller()
            {
                @Override
                public ListenableFuture<DataSize> spill(Iterator<Page> pageIterator)
                {
                    return immediateFailedFuture(new IOException("Failed to spill"));
                }

                @Override
                public List<Iterator<Page>> getSpills()
                {
                    return ImmutableList.of();
                }

                @Override
                public void close() {}
            };
        }
    }
}
