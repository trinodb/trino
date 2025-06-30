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
package io.trino.operator.join.unspilled;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.trino.ExceededMemoryLimitException;
import io.trino.RowPagesBuilder;
import io.trino.connector.CatalogServiceProvider;
import io.trino.operator.DriverContext;
import io.trino.operator.JoinOperatorType;
import io.trino.operator.Operator;
import io.trino.operator.OperatorFactory;
import io.trino.operator.TaskContext;
import io.trino.operator.join.InternalJoinFilterFunction;
import io.trino.operator.join.JoinBridgeManager;
import io.trino.operator.join.JoinOperatorInfo;
import io.trino.operator.join.unspilled.JoinTestUtils.BuildSideSetup;
import io.trino.operator.join.unspilled.JoinTestUtils.TestInternalJoinFilterFunction;
import io.trino.spi.Page;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.planner.PartitionFunctionProvider;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.MaterializedResult;
import io.trino.testing.TestingTaskContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.operator.JoinOperatorType.fullOuterJoin;
import static io.trino.operator.JoinOperatorType.innerJoin;
import static io.trino.operator.JoinOperatorType.probeOuterJoin;
import static io.trino.operator.OperatorAssertion.assertOperatorEquals;
import static io.trino.operator.OperatorAssertion.toMaterializedResult;
import static io.trino.operator.OperatorAssertion.toPages;
import static io.trino.operator.OperatorFactories.join;
import static io.trino.operator.join.unspilled.JoinTestUtils.buildLookupSource;
import static io.trino.operator.join.unspilled.JoinTestUtils.innerJoinOperatorFactory;
import static io.trino.operator.join.unspilled.JoinTestUtils.instantiateBuildDrivers;
import static io.trino.operator.join.unspilled.JoinTestUtils.setupBuildSide;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestHashJoinOperator
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
    private final ScheduledExecutorService scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));
    private final PartitionFunctionProvider partitionFunctionProvider = new PartitionFunctionProvider(
            TYPE_OPERATORS,
            CatalogServiceProvider.fail());

    @AfterAll
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testInnerJoin()
    {
        testInnerJoin(false);
        testInnerJoin(true);
    }

    private void testInnerJoin(boolean parallelBuild)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        RowPagesBuilder buildPages = rowPagesBuilder(Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT, BIGINT))
                .addSequencePage(10, 20, 30, 40);
        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, parallelBuild, taskContext, buildPages, Optional.empty());
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        RowPagesBuilder probePages = rowPagesBuilder(Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT, BIGINT));
        List<Page> probeInput = probePages
                .addSequencePage(1000, 0, 1000, 2000)
                .build();
        OperatorFactory joinOperatorFactory = innerJoinOperatorFactory(lookupSourceFactory, probePages, false);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probePages.getTypes(), buildPages.getTypes()))
                .row("20", 1020L, 2020L, "20", 30L, 40L)
                .row("21", 1021L, 2021L, "21", 31L, 41L)
                .row("22", 1022L, 2022L, "22", 32L, 42L)
                .row("23", 1023L, 2023L, "23", 33L, 43L)
                .row("24", 1024L, 2024L, "24", 34L, 44L)
                .row("25", 1025L, 2025L, "25", 35L, 45L)
                .row("26", 1026L, 2026L, "26", 36L, 46L)
                .row("27", 1027L, 2027L, "27", 37L, 47L)
                .row("28", 1028L, 2028L, "28", 38L, 48L)
                .row("29", 1029L, 2029L, "29", 39L, 49L)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true);
    }

    @Test
    public void testInnerJoinWithRunLengthEncodedProbe()
    {
        testInnerJoinWithRunLengthEncodedProbe(false, false);
        testInnerJoinWithRunLengthEncodedProbe(false, true);
        testInnerJoinWithRunLengthEncodedProbe(true, false);
        testInnerJoinWithRunLengthEncodedProbe(true, true);
    }

    private void testInnerJoinWithRunLengthEncodedProbe(boolean withFilter, boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        RowPagesBuilder buildPages = rowPagesBuilder(Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT))
                .row("20", 1L)
                .row("21", 2L)
                .row("21", 3L);
        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, false, taskContext, buildPages, Optional.empty(), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        RowPagesBuilder probePagesBuilder = rowPagesBuilder(Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT))
                .addBlocksPage(
                        RunLengthEncodedBlock.create(VARCHAR, Slices.utf8Slice("20"), 2),
                        createLongsBlock(42, 43))
                .addBlocksPage(
                        RunLengthEncodedBlock.create(VARCHAR, Slices.utf8Slice("-1"), 2),
                        createLongsBlock(52, 53))
                .addBlocksPage(
                        RunLengthEncodedBlock.create(VARCHAR, Slices.utf8Slice("21"), 2),
                        createLongsBlock(62, 63));
        OperatorFactory joinOperatorFactory = innerJoinOperatorFactory(lookupSourceFactory, probePagesBuilder, withFilter);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        DriverContext driverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();
        List<Page> pages = toPages(joinOperatorFactory, driverContext, probePagesBuilder.build(), true, true);

        assertThat(pages).hasSize(2);
        if (withFilter) {
            assertThat(pages.get(0).getBlock(2)).isInstanceOf(VariableWidthBlock.class);
            assertThat(pages.get(0).getBlock(3)).isInstanceOf(LongArrayBlock.class);
        }
        else {
            assertThat(pages.get(0).getBlock(2)).isInstanceOf(RunLengthEncodedBlock.class);
            assertThat(pages.get(0).getBlock(3)).isInstanceOf(RunLengthEncodedBlock.class);
        }
        assertThat(pages.get(1).getBlock(2)).isInstanceOf(VariableWidthBlock.class);

        assertThat(getJoinOperatorInfo(driverContext).getRleProbes()).isEqualTo(withFilter ? 0 : 2);
        assertThat(getJoinOperatorInfo(driverContext).getTotalProbes()).isEqualTo(3);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probePagesBuilder.getTypes(), buildPages.getTypes()))
                .row("20", 42L, "20", 1L)
                .row("20", 43L, "20", 1L)
                .row("21", 62L, "21", 3L)
                .row("21", 62L, "21", 2L)
                .row("21", 63L, "21", 3L)
                .row("21", 63L, "21", 2L)
                .build();
        MaterializedResult actual = toMaterializedResult(driverContext.getSession(), expected.getTypes(), pages);
        assertThat(actual).containsExactlyElementsOf(expected);
    }

    private JoinOperatorInfo getJoinOperatorInfo(DriverContext driverContext)
    {
        return (JoinOperatorInfo) getOnlyElement(driverContext.getOperatorStats()).getInfo();
    }

    @Test
    public void testYield()
    {
        testYield(false);
        testYield(true);
    }

    private void testYield(boolean singleBigintLookupSource)
    {
        // create a filter function that yields for every probe match
        // verify we will yield #match times totally

        TaskContext taskContext = createTaskContext();
        DriverContext driverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();

        // force a yield for every match
        AtomicInteger filterFunctionCalls = new AtomicInteger();
        InternalJoinFilterFunction filterFunction = new TestInternalJoinFilterFunction(
                (leftPosition, leftPage, rightPosition, rightPage) -> {
                    filterFunctionCalls.incrementAndGet();
                    driverContext.getYieldSignal().forceYieldForTesting();
                    return true;
                });

        // build with 40 entries
        int entries = 40;
        RowPagesBuilder buildPages = rowPagesBuilder(Ints.asList(0), ImmutableList.of(BIGINT))
                .addSequencePage(entries, 42);
        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, true, taskContext, buildPages, Optional.of(filterFunction), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe matching the above 40 entries
        RowPagesBuilder probePages = rowPagesBuilder(Ints.asList(0), ImmutableList.of(BIGINT));
        List<Page> probeInput = probePages.addSequencePage(100, 0).build();
        OperatorFactory joinOperatorFactory = join(
                innerJoin(false, false),
                0,
                new PlanNodeId("test"),
                lookupSourceFactory,
                true,
                probePages.getTypes(),
                Ints.asList(0),
                Optional.empty());

        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);
        Operator operator = joinOperatorFactory.createOperator(driverContext);
        assertThat(operator.needsInput()).isTrue();
        operator.addInput(probeInput.get(0));
        operator.finish();

        // we will yield 40 times due to filterFunction
        for (int i = 0; i < entries; i++) {
            driverContext.getYieldSignal().setWithDelay(5 * SECONDS.toNanos(1), driverContext.getYieldExecutor());
            filterFunctionCalls.set(0);
            assertThat(operator.getOutput()).isNull();
            assertThat(filterFunctionCalls.get())
                    .describedAs("Expected join to stop processing (yield) after calling filter function once")
                    .isEqualTo(1);
            driverContext.getYieldSignal().reset();
        }
        // delayed yield is not going to prevent operator from producing a page now (yield won't be forced because filter function won't be called anymore)
        driverContext.getYieldSignal().setWithDelay(5 * SECONDS.toNanos(1), driverContext.getYieldExecutor());
        // expect output page to be produced within few calls to getOutput(), e.g. to facilitate spill
        Page output = null;
        for (int i = 0; output == null && i < 5; i++) {
            output = operator.getOutput();
        }
        assertThat(output).isNotNull();
        driverContext.getYieldSignal().reset();

        // make sure we have all 4 entries
        assertThat(output.getPositionCount()).isEqualTo(entries);
    }

    @Test
    public void testInnerJoinWithNullProbe()
    {
        testInnerJoinWithNullProbe(false, false);
        testInnerJoinWithNullProbe(false, true);
        testInnerJoinWithNullProbe(true, false);
        testInnerJoinWithNullProbe(true, true);
    }

    private void testInnerJoinWithNullProbe(boolean parallelBuild, boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(Ints.asList(0), buildTypes)
                .row(1L)
                .row(2L)
                .row(3L);
        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, parallelBuild, taskContext, buildPages, Optional.empty(), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row(1L)
                .row((String) null)
                .row((String) null)
                .row(1L)
                .row(2L)
                .build();
        OperatorFactory joinOperatorFactory = innerJoinOperatorFactory(lookupSourceFactory, probePages, false);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildPages.getTypes()))
                .row(1L, 1L)
                .row(1L, 1L)
                .row(2L, 2L)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true);
    }

    @Test
    public void testInnerJoinWithOutputSingleMatch()
    {
        testInnerJoinWithOutputSingleMatch(false, false);
        testInnerJoinWithOutputSingleMatch(false, true);
        testInnerJoinWithOutputSingleMatch(true, false);
        testInnerJoinWithOutputSingleMatch(true, true);
    }

    private void testInnerJoinWithOutputSingleMatch(boolean parallelBuild, boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();
        // build factory
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(Ints.asList(0), buildTypes)
                .row(1L)
                .row(1L)
                .row(2L);
        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, parallelBuild, taskContext, buildPages, Optional.empty(), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row(1L)
                .row(2L)
                .row(3L)
                .build();
        OperatorFactory joinOperatorFactory = innerJoinOperatorFactory(lookupSourceFactory, probePages, true, false);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row(1L, 1L)
                .row(2L, 2L)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true);
    }

    @Test
    public void testInnerJoinWithNullBuild()
    {
        testInnerJoinWithNullBuild(false);
        testInnerJoinWithNullBuild(true);
    }

    private void testInnerJoinWithNullBuild(boolean parallelBuild)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(Ints.asList(0), buildTypes)
                .row(1L)
                .row((String) null)
                .row((String) null)
                .row(1L)
                .row(2L);
        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, parallelBuild, taskContext, buildPages, Optional.empty());
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row(1L)
                .row(2L)
                .row(3L)
                .build();
        OperatorFactory joinOperatorFactory = innerJoinOperatorFactory(lookupSourceFactory, probePages, false);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row(1L, 1L)
                .row(1L, 1L)
                .row(2L, 2L)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true);
    }

    @Test
    public void testInnerJoinWithNullOnBothSides()
    {
        testInnerJoinWithNullOnBothSides(false);
        testInnerJoinWithNullOnBothSides(true);
    }

    private void testInnerJoinWithNullOnBothSides(boolean parallelBuild)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(Ints.asList(0), buildTypes)
                .row(1L)
                .row((String) null)
                .row((String) null)
                .row(1L)
                .row(2L);
        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, parallelBuild, taskContext, buildPages, Optional.empty());
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row(1L)
                .row(2L)
                .row((String) null)
                .row(3L)
                .build();
        OperatorFactory joinOperatorFactory = innerJoinOperatorFactory(lookupSourceFactory, probePages, false);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row(1L, 1L)
                .row(1L, 1L)
                .row(2L, 2L)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true);
    }

    @Test
    public void testProbeOuterJoin()
    {
        testProbeOuterJoin(false);
        testProbeOuterJoin(true);
    }

    private void testProbeOuterJoin(boolean parallelBuild)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(VARCHAR, BIGINT, BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT, BIGINT))
                .addSequencePage(10, 20, 30, 40);
        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, parallelBuild, taskContext, buildPages, Optional.empty());
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(VARCHAR, BIGINT, BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .addSequencePage(15, 20, 1020, 2020)
                .build();
        OperatorFactory joinOperatorFactory = probeOuterJoinOperatorFactory(lookupSourceFactory, probePages, false);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("20", 1020L, 2020L, "20", 30L, 40L)
                .row("21", 1021L, 2021L, "21", 31L, 41L)
                .row("22", 1022L, 2022L, "22", 32L, 42L)
                .row("23", 1023L, 2023L, "23", 33L, 43L)
                .row("24", 1024L, 2024L, "24", 34L, 44L)
                .row("25", 1025L, 2025L, "25", 35L, 45L)
                .row("26", 1026L, 2026L, "26", 36L, 46L)
                .row("27", 1027L, 2027L, "27", 37L, 47L)
                .row("28", 1028L, 2028L, "28", 38L, 48L)
                .row("29", 1029L, 2029L, "29", 39L, 49L)
                .row("30", 1030L, 2030L, null, null, null)
                .row("31", 1031L, 2031L, null, null, null)
                .row("32", 1032L, 2032L, null, null, null)
                .row("33", 1033L, 2033L, null, null, null)
                .row("34", 1034L, 2034L, null, null, null)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true);
    }

    @Test
    public void testProbeOuterJoinWithFilterFunction()
    {
        testProbeOuterJoinWithFilterFunction(false);
        testProbeOuterJoinWithFilterFunction(true);
    }

    private void testProbeOuterJoinWithFilterFunction(boolean parallelBuild)
    {
        TaskContext taskContext = createTaskContext();

        InternalJoinFilterFunction filterFunction = new TestInternalJoinFilterFunction(
                (leftPosition, leftPage, rightPosition, rightPage) -> BIGINT.getLong(rightPage.getBlock(1), rightPosition) >= 1025);

        // build factory
        List<Type> buildTypes = ImmutableList.of(VARCHAR, BIGINT, BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT, BIGINT))
                .addSequencePage(10, 20, 30, 40);
        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, parallelBuild, taskContext, buildPages, Optional.of(filterFunction));
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(VARCHAR, BIGINT, BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .addSequencePage(15, 20, 1020, 2020)
                .build();
        OperatorFactory joinOperatorFactory = probeOuterJoinOperatorFactory(lookupSourceFactory, probePages, true);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("20", 1020L, 2020L, null, null, null)
                .row("21", 1021L, 2021L, null, null, null)
                .row("22", 1022L, 2022L, null, null, null)
                .row("23", 1023L, 2023L, null, null, null)
                .row("24", 1024L, 2024L, null, null, null)
                .row("25", 1025L, 2025L, "25", 35L, 45L)
                .row("26", 1026L, 2026L, "26", 36L, 46L)
                .row("27", 1027L, 2027L, "27", 37L, 47L)
                .row("28", 1028L, 2028L, "28", 38L, 48L)
                .row("29", 1029L, 2029L, "29", 39L, 49L)
                .row("30", 1030L, 2030L, null, null, null)
                .row("31", 1031L, 2031L, null, null, null)
                .row("32", 1032L, 2032L, null, null, null)
                .row("33", 1033L, 2033L, null, null, null)
                .row("34", 1034L, 2034L, null, null, null)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true);
    }

    @Test
    public void testOuterJoinWithNullProbe()
    {
        testOuterJoinWithNullProbe(false, false);
        testOuterJoinWithNullProbe(false, true);
        testOuterJoinWithNullProbe(true, false);
        testOuterJoinWithNullProbe(true, true);
    }

    private void testOuterJoinWithNullProbe(boolean parallelBuild, boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(Ints.asList(0), buildTypes)
                .row(1L)
                .row(2L)
                .row(3L);
        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, parallelBuild, taskContext, buildPages, Optional.empty(), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row(1L)
                .row((String) null)
                .row((String) null)
                .row(1L)
                .row(2L)
                .build();
        OperatorFactory joinOperatorFactory = probeOuterJoinOperatorFactory(lookupSourceFactory, probePages, false);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row(1L, 1L)
                .row(null, null)
                .row(null, null)
                .row(1L, 1L)
                .row(2L, 2L)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true);
    }

    @Test
    public void testOuterJoinWithNullProbeAndFilterFunction()
    {
        testOuterJoinWithNullProbeAndFilterFunction(false, false);
        testOuterJoinWithNullProbeAndFilterFunction(false, true);
        testOuterJoinWithNullProbeAndFilterFunction(true, false);
        testOuterJoinWithNullProbeAndFilterFunction(true, true);
    }

    private void testOuterJoinWithNullProbeAndFilterFunction(boolean parallelBuild, boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();

        InternalJoinFilterFunction filterFunction = new TestInternalJoinFilterFunction(
                (leftPosition, leftPage, rightPosition, rightPage) -> BIGINT.getLong(rightPage.getBlock(0), rightPosition) == 1L);

        // build factory
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(Ints.asList(0), buildTypes)
                .row(1L)
                .row(2L)
                .row(3L);
        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, parallelBuild, taskContext, buildPages, Optional.of(filterFunction), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row(1L)
                .row((String) null)
                .row((String) null)
                .row(1L)
                .row(2L)
                .build();
        OperatorFactory joinOperatorFactory = probeOuterJoinOperatorFactory(lookupSourceFactory, probePages, true);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row(1L, 1L)
                .row(null, null)
                .row(null, null)
                .row(1L, 1L)
                .row(2L, null)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true);
    }

    @Test
    public void testOuterJoinWithNullBuild()
    {
        testOuterJoinWithNullBuild(false, false);
        testOuterJoinWithNullBuild(false, true);
        testOuterJoinWithNullBuild(true, false);
        testOuterJoinWithNullBuild(true, true);
    }

    private void testOuterJoinWithNullBuild(boolean parallelBuild, boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(Ints.asList(0), ImmutableList.of(BIGINT))
                .row(1L)
                .row((String) null)
                .row((String) null)
                .row(1L)
                .row(2L);
        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, parallelBuild, taskContext, buildPages, Optional.empty(), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row(1L)
                .row(2L)
                .row(3L)
                .build();
        OperatorFactory joinOperatorFactory = probeOuterJoinOperatorFactory(lookupSourceFactory, probePages, false);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row(1L, 1L)
                .row(1L, 1L)
                .row(2L, 2L)
                .row(3L, null)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true);
    }

    @Test
    public void testOuterJoinWithNullBuildAndFilterFunction()
    {
        testOuterJoinWithNullBuildAndFilterFunction(false, false);
        testOuterJoinWithNullBuildAndFilterFunction(false, true);
        testOuterJoinWithNullBuildAndFilterFunction(true, false);
        testOuterJoinWithNullBuildAndFilterFunction(true, true);
    }

    private void testOuterJoinWithNullBuildAndFilterFunction(boolean parallelBuild, boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();

        InternalJoinFilterFunction filterFunction = new TestInternalJoinFilterFunction(
                (leftPosition, leftPage, rightPosition, rightPage) ->
                        ImmutableSet.of(1L, 3L).contains(BIGINT.getLong(rightPage.getBlock(0), rightPosition)));

        // build factory
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(Ints.asList(0), ImmutableList.of(BIGINT))
                .row(1L)
                .row((String) null)
                .row((String) null)
                .row(1L)
                .row(2L);
        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, parallelBuild, taskContext, buildPages, Optional.of(filterFunction), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row(1L)
                .row(2L)
                .row(3L)
                .build();
        OperatorFactory joinOperatorFactory = probeOuterJoinOperatorFactory(lookupSourceFactory, probePages, true);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row(1L, 1L)
                .row(1L, 1L)
                .row(2L, null)
                .row(3L, null)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true);
    }

    @Test
    public void testOuterJoinWithNullOnBothSides()
    {
        testOuterJoinWithNullOnBothSides(false, false);
        testOuterJoinWithNullOnBothSides(false, true);
        testOuterJoinWithNullOnBothSides(true, false);
        testOuterJoinWithNullOnBothSides(true, true);
    }

    private void testOuterJoinWithNullOnBothSides(boolean parallelBuild, boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        RowPagesBuilder buildPages = rowPagesBuilder(Ints.asList(0), ImmutableList.of(BIGINT))
                .row(1L)
                .row((String) null)
                .row((String) null)
                .row(1L)
                .row(2L);
        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, parallelBuild, taskContext, buildPages, Optional.empty(), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row(1L)
                .row(2L)
                .row((String) null)
                .row(3L)
                .build();
        OperatorFactory joinOperatorFactory = probeOuterJoinOperatorFactory(lookupSourceFactory, probePages, false);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildPages.getTypes()))
                .row(1L, 1L)
                .row(1L, 1L)
                .row(2L, 2L)
                .row(null, null)
                .row(3L, null)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true);
    }

    @Test
    public void testOuterJoinWithNullOnBothSidesAndFilterFunction()
    {
        testOuterJoinWithNullOnBothSidesAndFilterFunction(false, false);
        testOuterJoinWithNullOnBothSidesAndFilterFunction(false, true);
        testOuterJoinWithNullOnBothSidesAndFilterFunction(true, false);
        testOuterJoinWithNullOnBothSidesAndFilterFunction(true, true);
    }

    private void testOuterJoinWithNullOnBothSidesAndFilterFunction(boolean parallelBuild, boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();

        InternalJoinFilterFunction filterFunction = new TestInternalJoinFilterFunction(
                (leftPosition, leftPage, rightPosition, rightPage) ->
                        ImmutableSet.of(1L, 3L).contains(BIGINT.getLong(rightPage.getBlock(0), rightPosition)));

        // build factory
        RowPagesBuilder buildPages = rowPagesBuilder(Ints.asList(0), ImmutableList.of(BIGINT))
                .row(1L)
                .row((String) null)
                .row((String) null)
                .row(1L)
                .row(2L);
        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, parallelBuild, taskContext, buildPages, Optional.of(filterFunction), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row(1L)
                .row(2L)
                .row((String) null)
                .row(3L)
                .build();
        OperatorFactory joinOperatorFactory = probeOuterJoinOperatorFactory(lookupSourceFactory, probePages, true);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildPages.getTypes()))
                .row(1L, 1L)
                .row(1L, 1L)
                .row(2L, null)
                .row(null, null)
                .row(3L, null)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true);
    }

    @Test
    public void testMemoryLimit()
    {
        testMemoryLimit(false);
        testMemoryLimit(true);
    }

    private void testMemoryLimit(boolean parallelBuild)
    {
        TaskContext taskContext = TestingTaskContext.createTaskContext(executor, scheduledExecutor, TEST_SESSION, DataSize.ofBytes(100));

        RowPagesBuilder buildPages = rowPagesBuilder(Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT, BIGINT))
                .addSequencePage(10, 20, 30, 40);
        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, parallelBuild, taskContext, buildPages, Optional.empty());
        instantiateBuildDrivers(buildSideSetup, taskContext);

        assertThatThrownBy(() -> buildLookupSource(executor, buildSideSetup))
                .isInstanceOf(ExceededMemoryLimitException.class)
                .hasMessageMatching("Query exceeded per-node memory limit of.*");
    }

    @Test
    public void testInnerJoinWithEmptyLookupSource()
    {
        testInnerJoinWithEmptyLookupSource(false, false);
        testInnerJoinWithEmptyLookupSource(false, true);
        testInnerJoinWithEmptyLookupSource(true, false);
        testInnerJoinWithEmptyLookupSource(true, true);
    }

    private void testInnerJoinWithEmptyLookupSource(boolean parallelBuild, boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(Ints.asList(0), buildTypes);
        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, parallelBuild, taskContext, buildPages, Optional.empty(), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(Ints.asList(0), probeTypes);
        OperatorFactory joinOperatorFactory = join(
                innerJoin(false, false),
                0,
                new PlanNodeId("test"),
                lookupSourceFactoryManager,
                false,
                probePages.getTypes(),
                Ints.asList(0),
                Optional.empty());

        // drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);
        Operator operator = joinOperatorFactory.createOperator(taskContext.addPipelineContext(0, true, true, false).addDriverContext());

        List<Page> pages = probePages.row(1L).build();
        operator.addInput(pages.get(0));
        Page outputPage = operator.getOutput();
        assertThat(outputPage).isNull();
    }

    @Test
    public void testLookupOuterJoinWithEmptyLookupSource()
    {
        testLookupOuterJoinWithEmptyLookupSource(false, false);
        testLookupOuterJoinWithEmptyLookupSource(false, true);
        testLookupOuterJoinWithEmptyLookupSource(true, false);
        testLookupOuterJoinWithEmptyLookupSource(true, true);
    }

    private void testLookupOuterJoinWithEmptyLookupSource(boolean parallelBuild, boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(Ints.asList(0), buildTypes);
        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, parallelBuild, taskContext, buildPages, Optional.empty(), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(Ints.asList(0), probeTypes);
        OperatorFactory joinOperatorFactory = join(
                JoinOperatorType.lookupOuterJoin(false),
                0,
                new PlanNodeId("test"),
                lookupSourceFactoryManager,
                false,
                probePages.getTypes(),
                Ints.asList(0),
                Optional.empty());

        // drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);
        Operator operator = joinOperatorFactory.createOperator(taskContext.addPipelineContext(0, true, true, false).addDriverContext());

        List<Page> pages = probePages.row(1L).build();
        operator.addInput(pages.get(0));
        Page outputPage = operator.getOutput();
        assertThat(outputPage).isNull();
    }

    @Test
    public void testProbeOuterJoinWithEmptyLookupSource()
    {
        testProbeOuterJoinWithEmptyLookupSource(false, false);
        testProbeOuterJoinWithEmptyLookupSource(false, true);
        testProbeOuterJoinWithEmptyLookupSource(true, false);
        testProbeOuterJoinWithEmptyLookupSource(true, true);
    }

    private void testProbeOuterJoinWithEmptyLookupSource(boolean parallelBuild, boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(Ints.asList(0), buildTypes);
        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, parallelBuild, taskContext, buildPages, Optional.empty(), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row(1L)
                .row(2L)
                .row((String) null)
                .row(3L)
                .build();
        OperatorFactory joinOperatorFactory = join(
                probeOuterJoin(false),
                0,
                new PlanNodeId("test"),
                lookupSourceFactoryManager,
                false,
                probePages.getTypes(),
                Ints.asList(0),
                Optional.empty());

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row(1L, null)
                .row(2L, null)
                .row(null, null)
                .row(3L, null)
                .build();
        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true);
    }

    @Test
    public void testFullOuterJoinWithEmptyLookupSource()
    {
        testFullOuterJoinWithEmptyLookupSource(false, false);
        testFullOuterJoinWithEmptyLookupSource(false, true);
        testFullOuterJoinWithEmptyLookupSource(true, false);
        testFullOuterJoinWithEmptyLookupSource(true, true);
    }

    private void testFullOuterJoinWithEmptyLookupSource(boolean parallelBuild, boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(Ints.asList(0), buildTypes);
        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, parallelBuild, taskContext, buildPages, Optional.empty(), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row(1L)
                .row(2L)
                .row((String) null)
                .row(3L)
                .build();
        OperatorFactory joinOperatorFactory = join(
                fullOuterJoin(),
                0,
                new PlanNodeId("test"),
                lookupSourceFactoryManager,
                false,
                probePages.getTypes(),
                Ints.asList(0),
                Optional.empty());

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row(1L, null)
                .row(2L, null)
                .row(null, null)
                .row(3L, null)
                .build();
        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true);
    }

    @Test
    public void testInnerJoinWithNonEmptyLookupSourceAndEmptyProbe()
    {
        testInnerJoinWithNonEmptyLookupSourceAndEmptyProbe(false, false);
        testInnerJoinWithNonEmptyLookupSourceAndEmptyProbe(false, true);
        testInnerJoinWithNonEmptyLookupSourceAndEmptyProbe(true, false);
        testInnerJoinWithNonEmptyLookupSourceAndEmptyProbe(true, true);
    }

    private void testInnerJoinWithNonEmptyLookupSourceAndEmptyProbe(boolean parallelBuild, boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(Ints.asList(0), buildTypes)
                .row(1L)
                .row(2L)
                .row((String) null)
                .row(3L);
        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, parallelBuild, taskContext, buildPages, Optional.empty(), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages.build();
        OperatorFactory joinOperatorFactory = join(
                innerJoin(false, false),
                0,
                new PlanNodeId("test"),
                lookupSourceFactoryManager,
                false,
                probePages.getTypes(),
                Ints.asList(0),
                Optional.empty());

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes)).build();
        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true);
    }

    @Test
    public void testInnerJoinWithBlockingLookupSourceAndEmptyProbe()
            throws Exception
    {
        testInnerJoinWithBlockingLookupSourceAndEmptyProbe(false);
        testInnerJoinWithBlockingLookupSourceAndEmptyProbe(true);
    }

    private void testInnerJoinWithBlockingLookupSourceAndEmptyProbe(boolean parallelBuild)
            throws Exception
    {
        // join that waits for build side to be collected
        TaskContext taskContext = createTaskContext();
        OperatorFactory joinOperatorFactory = createJoinOperatorFactoryWithBlockingLookupSource(taskContext, parallelBuild, true);
        DriverContext driverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();
        try (Operator joinOperator = joinOperatorFactory.createOperator(driverContext)) {
            joinOperatorFactory.noMoreOperators();
            assertThat(joinOperator.needsInput()).isFalse();
            joinOperator.finish();
            assertThat(joinOperator.getOutput()).isNull();

            // lookup join operator got blocked waiting for build side
            assertThat(joinOperator.isBlocked().isDone()).isFalse();
            assertThat(joinOperator.isFinished()).isFalse();
        }

        // join that doesn't wait for build side to be collected
        taskContext = createTaskContext();
        joinOperatorFactory = createJoinOperatorFactoryWithBlockingLookupSource(taskContext, parallelBuild, false);
        driverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();
        try (Operator joinOperator = joinOperatorFactory.createOperator(driverContext)) {
            joinOperatorFactory.noMoreOperators();
            assertThat(joinOperator.needsInput()).isTrue();
            joinOperator.finish();
            assertThat(joinOperator.getOutput()).isNull();

            // lookup join operator will yield once before finishing
            assertThat(joinOperator.getOutput()).isNull();
            assertThat(joinOperator.isBlocked().isDone()).isTrue();
            assertThat(joinOperator.isFinished()).isTrue();
        }
    }

    @Test
    public void testInnerJoinWithBlockingLookupSource()
            throws Exception
    {
        testInnerJoinWithBlockingLookupSource(false);
        testInnerJoinWithBlockingLookupSource(true);
    }

    private void testInnerJoinWithBlockingLookupSource(boolean parallelBuild)
            throws Exception
    {
        RowPagesBuilder probePages = rowPagesBuilder(Ints.asList(0), ImmutableList.of(VARCHAR));
        Page probePage = getOnlyElement(probePages.addSequencePage(1, 0).build());

        // join that waits for build side to be collected
        TaskContext taskContext = createTaskContext();
        OperatorFactory joinOperatorFactory = createJoinOperatorFactoryWithBlockingLookupSource(taskContext, parallelBuild, true);
        DriverContext driverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();
        try (Operator joinOperator = joinOperatorFactory.createOperator(driverContext)) {
            joinOperatorFactory.noMoreOperators();
            assertThat(joinOperator.needsInput()).isFalse();
            assertThat(joinOperator.getOutput()).isNull();

            // lookup join operator got blocked waiting for build side
            assertThat(joinOperator.isBlocked().isDone()).isFalse();
            assertThat(joinOperator.isFinished()).isFalse();
        }

        // join that doesn't wait for build side to be collected
        taskContext = createTaskContext();
        joinOperatorFactory = createJoinOperatorFactoryWithBlockingLookupSource(taskContext, parallelBuild, false);
        driverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();
        try (Operator joinOperator = joinOperatorFactory.createOperator(driverContext)) {
            joinOperatorFactory.noMoreOperators();
            assertThat(joinOperator.needsInput()).isTrue();
            assertThat(joinOperator.getOutput()).isNull();

            // join needs input page
            assertThat(joinOperator.isBlocked().isDone()).isTrue();
            assertThat(joinOperator.isFinished()).isFalse();
            joinOperator.addInput(probePage);
            assertThat(joinOperator.getOutput()).isNull();

            // lookup join operator got blocked waiting for build side
            assertThat(joinOperator.isBlocked().isDone()).isFalse();
            assertThat(joinOperator.isFinished()).isFalse();
        }
    }

    private OperatorFactory createJoinOperatorFactoryWithBlockingLookupSource(TaskContext taskContext, boolean parallelBuild, boolean waitForBuild)
    {
        // build factory
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(Ints.asList(0), buildTypes);
        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, parallelBuild, taskContext, buildPages, Optional.empty());
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(Ints.asList(0), probeTypes);
        OperatorFactory joinOperatorFactory = join(
                innerJoin(false, waitForBuild),
                0,
                new PlanNodeId("test"),
                lookupSourceFactoryManager,
                false,
                probePages.getTypes(),
                Ints.asList(0),
                Optional.empty());

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);

        return joinOperatorFactory;
    }

    private TaskContext createTaskContext()
    {
        return TestingTaskContext.createTaskContext(executor, scheduledExecutor, TEST_SESSION);
    }

    private OperatorFactory probeOuterJoinOperatorFactory(
            JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager,
            RowPagesBuilder probePages,
            boolean hasFilter)
    {
        return join(
                probeOuterJoin(false),
                0,
                new PlanNodeId("test"),
                lookupSourceFactoryManager,
                hasFilter,
                probePages.getTypes(),
                Ints.asList(0),
                Optional.empty());
    }

    private static <T> List<T> concat(List<T> initialElements, List<T> moreElements)
    {
        return ImmutableList.copyOf(Iterables.concat(initialElements, moreElements));
    }
}
