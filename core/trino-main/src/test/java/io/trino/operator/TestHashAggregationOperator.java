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
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.PageBuilderStatus;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spiller.Spiller;
import io.trino.spiller.SpillerFactory;
import io.trino.sql.gen.JoinCompiler;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.tree.QualifiedName;
import io.trino.testing.MaterializedResult;
import io.trino.testing.TestingTaskContext;
import io.trino.type.BlockTypeOperators;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static io.airlift.testing.Assertions.assertGreaterThan;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.GroupByHashYieldAssertion.GroupByHashYieldResult;
import static io.trino.operator.GroupByHashYieldAssertion.createPagesWithDistinctHashKeys;
import static io.trino.operator.GroupByHashYieldAssertion.finishOperatorWithYieldingGroupByHash;
import static io.trino.operator.OperatorAssertion.assertOperatorEqualsIgnoreOrder;
import static io.trino.operator.OperatorAssertion.assertPagesEqualIgnoreOrder;
import static io.trino.operator.OperatorAssertion.dropChannel;
import static io.trino.operator.OperatorAssertion.toMaterializedResult;
import static io.trino.operator.OperatorAssertion.toPages;
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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestHashAggregationOperator
{
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();

    private static final TestingAggregationFunction LONG_AVERAGE = FUNCTION_RESOLUTION.getAggregateFunction(QualifiedName.of("avg"), fromTypes(BIGINT));
    private static final TestingAggregationFunction LONG_SUM = FUNCTION_RESOLUTION.getAggregateFunction(QualifiedName.of("sum"), fromTypes(BIGINT));
    private static final TestingAggregationFunction COUNT = FUNCTION_RESOLUTION.getAggregateFunction(QualifiedName.of("count"), ImmutableList.of());
    private static final TestingAggregationFunction LONG_MIN = FUNCTION_RESOLUTION.getAggregateFunction(QualifiedName.of("min"), fromTypes(BIGINT));

    private static final int MAX_BLOCK_SIZE_IN_BYTES = 64 * 1024;

    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private final TypeOperators typeOperators = new TypeOperators();
    private final BlockTypeOperators blockTypeOperators = new BlockTypeOperators(typeOperators);
    private final JoinCompiler joinCompiler = new JoinCompiler(typeOperators);
    private DummySpillerFactory spillerFactory;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));
        spillerFactory = new DummySpillerFactory();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        spillerFactory = null;
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @DataProvider(name = "hashEnabled")
    public static Object[][] hashEnabled()
    {
        return new Object[][] {{true}, {false}};
    }

    @DataProvider(name = "hashEnabledAndMemoryLimitForMergeValues")
    public static Object[][] hashEnabledAndMemoryLimitForMergeValuesProvider()
    {
        return new Object[][] {
                {true, true, true, 8, Integer.MAX_VALUE},
                {true, true, false, 8, Integer.MAX_VALUE},
                {false, false, false, 0, 0},
                {false, true, true, 0, 0},
                {false, true, false, 0, 0},
                {false, true, true, 8, 0},
                {false, true, false, 8, 0},
                {false, true, true, 8, Integer.MAX_VALUE},
                {false, true, false, 8, Integer.MAX_VALUE}};
    }

    @DataProvider
    public Object[][] dataType()
    {
        return new Object[][] {{VARCHAR}, {BIGINT}};
    }

    @Test(dataProvider = "hashEnabledAndMemoryLimitForMergeValues")
    public void testHashAggregation(boolean hashEnabled, boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimitForMerge, long memoryLimitForMergeWithMemory)
    {
        // make operator produce multiple pages during finish phase
        int numberOfRows = 40_000;
        TestingAggregationFunction countVarcharColumn = FUNCTION_RESOLUTION.getAggregateFunction(QualifiedName.of("count"), fromTypes(VARCHAR));
        TestingAggregationFunction countBooleanColumn = FUNCTION_RESOLUTION.getAggregateFunction(QualifiedName.of("count"), fromTypes(BOOLEAN));
        TestingAggregationFunction maxVarcharColumn = FUNCTION_RESOLUTION.getAggregateFunction(QualifiedName.of("max"), fromTypes(VARCHAR));
        List<Integer> hashChannels = Ints.asList(1);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, hashChannels, VARCHAR, VARCHAR, VARCHAR, BIGINT, BOOLEAN);
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
                rowPagesBuilder.getHashChannel(),
                Optional.empty(),
                100_000,
                Optional.of(DataSize.of(16, MEGABYTE)),
                spillEnabled,
                succinctBytes(memoryLimitForMerge),
                succinctBytes(memoryLimitForMergeWithMemory),
                spillerFactory,
                joinCompiler,
                blockTypeOperators);

        DriverContext driverContext = createDriverContext(memoryLimitForMerge);

        MaterializedResult.Builder expectedBuilder = resultBuilder(driverContext.getSession(), VARCHAR, BIGINT, BIGINT, DOUBLE, VARCHAR, BIGINT, BIGINT);
        for (int i = 0; i < numberOfRows; ++i) {
            expectedBuilder.row(Integer.toString(i), 3L, 3L * i, (double) i, Integer.toString(300_000 + i), 3L, 3L);
        }
        MaterializedResult expected = expectedBuilder.build();

        List<Page> pages = toPages(operatorFactory, driverContext, input, revokeMemoryWhenAddingPages);
        assertGreaterThan(pages.size(), 1, "Expected more than one output page");
        assertPagesEqualIgnoreOrder(driverContext, pages, expected, hashEnabled, Optional.of(hashChannels.size()));

        assertTrue(spillEnabled == (spillerFactory.getSpillsCount() > 0), format("Spill state mismatch. Expected spill: %s, spill count: %s", spillEnabled, spillerFactory.getSpillsCount()));
    }

    @Test(dataProvider = "hashEnabledAndMemoryLimitForMergeValues")
    public void testHashAggregationWithGlobals(boolean hashEnabled, boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimitForMerge, long memoryLimitForMergeWithMemory)
    {
        TestingAggregationFunction countVarcharColumn = FUNCTION_RESOLUTION.getAggregateFunction(QualifiedName.of("count"), fromTypes(VARCHAR));
        TestingAggregationFunction countBooleanColumn = FUNCTION_RESOLUTION.getAggregateFunction(QualifiedName.of("count"), fromTypes(BOOLEAN));
        TestingAggregationFunction maxVarcharColumn = FUNCTION_RESOLUTION.getAggregateFunction(QualifiedName.of("max"), fromTypes(VARCHAR));

        Optional<Integer> groupIdChannel = Optional.of(1);
        List<Integer> groupByChannels = Ints.asList(1, 2);
        List<Integer> globalAggregationGroupIds = Ints.asList(42, 49);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, groupByChannels, VARCHAR, VARCHAR, VARCHAR, BIGINT, BIGINT, BOOLEAN);
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
                rowPagesBuilder.getHashChannel(),
                groupIdChannel,
                100_000,
                Optional.of(DataSize.of(16, MEGABYTE)),
                spillEnabled,
                succinctBytes(memoryLimitForMerge),
                succinctBytes(memoryLimitForMergeWithMemory),
                spillerFactory,
                joinCompiler,
                blockTypeOperators);

        DriverContext driverContext = createDriverContext(memoryLimitForMerge);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, BIGINT, BIGINT, BIGINT, DOUBLE, VARCHAR, BIGINT, BIGINT)
                .row(null, 42L, 0L, null, null, null, 0L, 0L)
                .row(null, 49L, 0L, null, null, null, 0L, 0L)
                .build();

        assertOperatorEqualsIgnoreOrder(operatorFactory, driverContext, input, expected, hashEnabled, Optional.of(groupByChannels.size()), revokeMemoryWhenAddingPages);
    }

    @Test(dataProvider = "hashEnabledAndMemoryLimitForMergeValues")
    public void testHashAggregationMemoryReservation(boolean hashEnabled, boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimitForMerge, long memoryLimitForMergeWithMemory)
    {
        TestingAggregationFunction arrayAggColumn = FUNCTION_RESOLUTION.getAggregateFunction(QualifiedName.of("array_agg"), fromTypes(BIGINT));

        List<Integer> hashChannels = Ints.asList(1);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, hashChannels, BIGINT, BIGINT);
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
                rowPagesBuilder.getHashChannel(),
                Optional.empty(),
                100_000,
                Optional.of(DataSize.of(16, MEGABYTE)),
                spillEnabled,
                succinctBytes(memoryLimitForMerge),
                succinctBytes(memoryLimitForMergeWithMemory),
                spillerFactory,
                joinCompiler,
                blockTypeOperators);

        Operator operator = operatorFactory.createOperator(driverContext);
        toPages(operator, input.iterator(), revokeMemoryWhenAddingPages);
        // TODO (https://github.com/trinodb/trino/issues/10596): it should be 0, since operator is finished
        assertEquals(getOnlyElement(operator.getOperatorContext().getNestedOperatorStats()).getUserMemoryReservation().toBytes(), spillEnabled && revokeMemoryWhenAddingPages ? 5_350_968 : 0);
        assertEquals(getOnlyElement(operator.getOperatorContext().getNestedOperatorStats()).getRevocableMemoryReservation().toBytes(), 0);
    }

    @Test(dataProvider = "hashEnabled", expectedExceptions = ExceededMemoryLimitException.class, expectedExceptionsMessageRegExp = "Query exceeded per-node memory limit of 10B.*")
    public void testMemoryLimit(boolean hashEnabled)
    {
        TestingAggregationFunction maxVarcharColumn = FUNCTION_RESOLUTION.getAggregateFunction(QualifiedName.of("max"), fromTypes(VARCHAR));

        List<Integer> hashChannels = Ints.asList(1);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, hashChannels, VARCHAR, BIGINT, VARCHAR, BIGINT);
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
                rowPagesBuilder.getHashChannel(),
                Optional.empty(),
                100_000,
                Optional.of(DataSize.of(16, MEGABYTE)),
                joinCompiler,
                blockTypeOperators);

        toPages(operatorFactory, driverContext, input);
    }

    @Test(dataProvider = "hashEnabledAndMemoryLimitForMergeValues")
    public void testHashBuilderResize(boolean hashEnabled, boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimitForMerge, long memoryLimitForMergeWithMemory)
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, 1, MAX_BLOCK_SIZE_IN_BYTES);
        VARCHAR.writeSlice(builder, Slices.allocate(200_000)); // this must be larger than MAX_BLOCK_SIZE_IN_BYTES, 64K
        builder.build();

        List<Integer> hashChannels = Ints.asList(0);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, hashChannels, VARCHAR);
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
                rowPagesBuilder.getHashChannel(),
                Optional.empty(),
                100_000,
                Optional.of(DataSize.of(16, MEGABYTE)),
                spillEnabled,
                succinctBytes(memoryLimitForMerge),
                succinctBytes(memoryLimitForMergeWithMemory),
                spillerFactory,
                joinCompiler,
                blockTypeOperators);

        toPages(operatorFactory, driverContext, input, revokeMemoryWhenAddingPages);
    }

    @Test(dataProvider = "dataType")
    public void testMemoryReservationYield(Type type)
    {
        List<Page> input = createPagesWithDistinctHashKeys(type, 6_000, 600);
        OperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(type),
                ImmutableList.of(0),
                ImmutableList.of(),
                SINGLE,
                ImmutableList.of(COUNT.createAggregatorFactory(SINGLE, ImmutableList.of(0), OptionalInt.empty())),
                Optional.of(1),
                Optional.empty(),
                1,
                Optional.of(DataSize.of(16, MEGABYTE)),
                joinCompiler,
                blockTypeOperators);

        // get result with yield; pick a relatively small buffer for aggregator's memory usage
        GroupByHashYieldResult result;
        result = finishOperatorWithYieldingGroupByHash(input, type, operatorFactory, this::getHashCapacity, 1_400_000);
        assertGreaterThan(result.getYieldCount(), 5);
        assertGreaterThan(result.getMaxReservedBytes(), 20L << 20);

        int count = 0;
        for (Page page : result.getOutput()) {
            // value + hash + aggregation result
            assertEquals(page.getChannelCount(), 3);
            for (int i = 0; i < page.getPositionCount(); i++) {
                assertEquals(page.getBlock(2).getLong(i, 0), 1);
                count++;
            }
        }
        assertEquals(count, 6_000 * 600);
    }

    @Test(dataProvider = "hashEnabled", expectedExceptions = ExceededMemoryLimitException.class, expectedExceptionsMessageRegExp = "Query exceeded per-node memory limit of 3MB.*")
    public void testHashBuilderResizeLimit(boolean hashEnabled)
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, 1, MAX_BLOCK_SIZE_IN_BYTES);
        VARCHAR.writeSlice(builder, Slices.allocate(5_000_000)); // this must be larger than MAX_BLOCK_SIZE_IN_BYTES, 64K
        builder.build();

        List<Integer> hashChannels = Ints.asList(0);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, hashChannels, VARCHAR);
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
                rowPagesBuilder.getHashChannel(),
                Optional.empty(),
                100_000,
                Optional.of(DataSize.of(16, MEGABYTE)),
                joinCompiler,
                blockTypeOperators);

        toPages(operatorFactory, driverContext, input);
    }

    @Test(dataProvider = "hashEnabled")
    public void testMultiSliceAggregationOutput(boolean hashEnabled)
    {
        // estimate the number of entries required to create 1.5 pages of results
        // See InMemoryHashAggregationBuilder.buildTypes()
        int fixedWidthSize = SIZE_OF_LONG + SIZE_OF_LONG +  // Used by BigintGroupByHash, see BigintGroupByHash.TYPES_WITH_RAW_HASH
                SIZE_OF_LONG + SIZE_OF_DOUBLE;              // Used by COUNT and LONG_AVERAGE aggregators;
        int multiSlicePositionCount = (int) (1.5 * PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES / fixedWidthSize);

        List<Integer> hashChannels = Ints.asList(1);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, hashChannels, BIGINT, BIGINT);
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
                rowPagesBuilder.getHashChannel(),
                Optional.empty(),
                100_000,
                Optional.of(DataSize.of(16, MEGABYTE)),
                joinCompiler,
                blockTypeOperators);

        assertEquals(toPages(operatorFactory, createDriverContext(), input).size(), 2);
    }

    @Test(dataProvider = "hashEnabled")
    public void testMultiplePartialFlushes(boolean hashEnabled)
            throws Exception
    {
        List<Integer> hashChannels = Ints.asList(0);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, hashChannels, BIGINT);
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
                rowPagesBuilder.getHashChannel(),
                Optional.empty(),
                100_000,
                Optional.of(DataSize.of(1, KILOBYTE)),
                joinCompiler,
                blockTypeOperators);

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
            assertTrue(!outputPages.isEmpty());

            // The operator need input again since this was a partial flush
            assertTrue(operator.needsInput());

            // Now, drive the operator to completion
            outputPages.addAll(toPages(operator, inputIterator));

            MaterializedResult actual;
            if (hashEnabled) {
                // Drop the hashChannel for all pages
                outputPages = dropChannel(outputPages, ImmutableList.of(1));
            }
            actual = toMaterializedResult(operator.getOperatorContext().getSession(), expected.getTypes(), outputPages);

            assertEquals(actual.getTypes(), expected.getTypes());
            assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
        }

        assertEquals(driverContext.getMemoryUsage(), 0);
        assertEquals(driverContext.getRevocableMemoryUsage(), 0);
    }

    @Test
    public void testMergeWithMemorySpill()
    {
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
                rowPagesBuilder.getHashChannel(),
                Optional.empty(),
                1,
                Optional.of(DataSize.of(16, MEGABYTE)),
                true,
                DataSize.ofBytes(smallPagesSpillThresholdSize),
                succinctBytes(Integer.MAX_VALUE),
                spillerFactory,
                joinCompiler,
                blockTypeOperators);

        DriverContext driverContext = createDriverContext(smallPagesSpillThresholdSize);

        MaterializedResult.Builder resultBuilder = resultBuilder(driverContext.getSession(), BIGINT, BIGINT);
        for (int i = 0; i < smallPagesSpillThresholdSize + 10; ++i) {
            resultBuilder.row((long) i, (long) i);
        }

        assertOperatorEqualsIgnoreOrder(operatorFactory, driverContext, input, resultBuilder.build());
    }

    @Test
    public void testSpillerFailure()
    {
        TestingAggregationFunction maxVarcharColumn = FUNCTION_RESOLUTION.getAggregateFunction(QualifiedName.of("max"), fromTypes(VARCHAR));

        List<Integer> hashChannels = Ints.asList(1);
        ImmutableList<Type> types = ImmutableList.of(VARCHAR, BIGINT, VARCHAR, BIGINT);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(false, hashChannels, types);
        List<Page> input = rowPagesBuilder
                .addSequencePage(10, 100, 0, 100, 0)
                .addSequencePage(10, 100, 0, 200, 0)
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
                rowPagesBuilder.getHashChannel(),
                Optional.empty(),
                100_000,
                Optional.of(DataSize.of(16, MEGABYTE)),
                true,
                succinctBytes(8),
                succinctBytes(Integer.MAX_VALUE),
                new FailingSpillerFactory(),
                joinCompiler,
                blockTypeOperators);

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
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(false, hashChannels, BIGINT);
        Page input = getOnlyElement(rowPagesBuilder.addSequencePage(500, 0).build());

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT),
                hashChannels,
                ImmutableList.of(),
                SINGLE,
                ImmutableList.of(LONG_MIN.createAggregatorFactory(SINGLE, ImmutableList.of(0), OptionalInt.empty())),
                rowPagesBuilder.getHashChannel(),
                Optional.empty(),
                100_000,
                Optional.of(DataSize.of(16, MEGABYTE)),
                joinCompiler,
                blockTypeOperators);

        DriverContext driverContext = createDriverContext(1024);

        try (Operator operator = operatorFactory.createOperator(driverContext)) {
            assertTrue(operator.needsInput());
            operator.addInput(input);

            assertThat(driverContext.getMemoryUsage()).isGreaterThan(0);

            toPages(operator, emptyIterator());
        }

        assertEquals(driverContext.getMemoryUsage(), 0);
        assertEquals(driverContext.getRevocableMemoryUsage(), 0);
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
        assertTrue(operator instanceof HashAggregationOperator);
        HashAggregationBuilder aggregationBuilder = ((HashAggregationOperator) operator).getAggregationBuilder();
        if (aggregationBuilder == null) {
            return 0;
        }
        assertTrue(aggregationBuilder instanceof InMemoryHashAggregationBuilder);
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
                public ListenableFuture<Void> spill(Iterator<Page> pageIterator)
                {
                    return immediateFailedFuture(new IOException("Failed to spill"));
                }

                @Override
                public List<Iterator<Page>> getSpills()
                {
                    return ImmutableList.of();
                }

                @Override
                public void close()
                {
                }
            };
        }
    }
}
