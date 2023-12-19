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
import io.trino.execution.NodeTaskMap;
import io.trino.execution.scheduler.NodeScheduler;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.execution.scheduler.UniformNodeSelectorFactory;
import io.trino.metadata.InMemoryNodeManager;
import io.trino.operator.DriverContext;
import io.trino.operator.JoinOperatorType;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.operator.ProcessorContext;
import io.trino.operator.TaskContext;
import io.trino.operator.WorkProcessor;
import io.trino.operator.WorkProcessorOperator;
import io.trino.operator.WorkProcessorOperatorFactory;
import io.trino.operator.join.InternalJoinFilterFunction;
import io.trino.operator.join.JoinBridgeManager;
import io.trino.operator.join.JoinOperatorInfo;
import io.trino.operator.join.unspilled.JoinTestUtils.BuildSideSetup;
import io.trino.operator.join.unspilled.JoinTestUtils.TestInternalJoinFilterFunction;
import io.trino.spi.Page;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.planner.NodePartitioningManager;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.MaterializedResult;
import io.trino.testing.TestingTaskContext;
import io.trino.util.FinalizerService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.operator.JoinOperatorType.fullOuterJoin;
import static io.trino.operator.JoinOperatorType.innerJoin;
import static io.trino.operator.JoinOperatorType.probeOuterJoin;
import static io.trino.operator.OperatorAssertion.assertOperatorEquals;
import static io.trino.operator.OperatorAssertion.dropChannel;
import static io.trino.operator.OperatorAssertion.toMaterializedResult;
import static io.trino.operator.OperatorAssertion.toPages;
import static io.trino.operator.OperatorFactories.join;
import static io.trino.operator.WorkProcessor.ProcessState.finished;
import static io.trino.operator.WorkProcessor.ProcessState.ofResult;
import static io.trino.operator.join.unspilled.JoinTestUtils.buildLookupSource;
import static io.trino.operator.join.unspilled.JoinTestUtils.getHashChannelAsInt;
import static io.trino.operator.join.unspilled.JoinTestUtils.innerJoinOperatorFactory;
import static io.trino.operator.join.unspilled.JoinTestUtils.instantiateBuildDrivers;
import static io.trino.operator.join.unspilled.JoinTestUtils.setupBuildSide;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
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
    private final NodePartitioningManager nodePartitioningManager = new NodePartitioningManager(
            new NodeScheduler(new UniformNodeSelectorFactory(
                    new InMemoryNodeManager(),
                    new NodeSchedulerConfig().setIncludeCoordinator(true),
                    new NodeTaskMap(new FinalizerService()))),
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
        testInnerJoin(false, false, false);
        testInnerJoin(false, false, true);
        testInnerJoin(false, true, false);
        testInnerJoin(false, true, true);
        testInnerJoin(true, false, false);
        testInnerJoin(true, false, true);
        testInnerJoin(true, true, false);
        testInnerJoin(true, true, true);
    }

    private void testInnerJoin(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT, BIGINT))
                .addSequencePage(10, 20, 30, 40);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, parallelBuild, taskContext, buildPages, Optional.empty());
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT, BIGINT));
        List<Page> probeInput = probePages
                .addSequencePage(1000, 0, 1000, 2000)
                .build();
        OperatorFactory joinOperatorFactory = innerJoinOperatorFactory(lookupSourceFactory, probePages, false);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probePages.getTypesWithoutHash(), buildPages.getTypesWithoutHash()))
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

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test
    public void testInnerJoinWithRunLengthEncodedProbe()
    {
        testInnerJoinWithRunLengthEncodedProbe(false, false, false);
        testInnerJoinWithRunLengthEncodedProbe(false, false, true);
        testInnerJoinWithRunLengthEncodedProbe(false, true, false);
        testInnerJoinWithRunLengthEncodedProbe(false, true, true);
        testInnerJoinWithRunLengthEncodedProbe(true, false, false);
        testInnerJoinWithRunLengthEncodedProbe(true, false, true);
        testInnerJoinWithRunLengthEncodedProbe(true, true, false);
        testInnerJoinWithRunLengthEncodedProbe(true, true, true);
    }

    private void testInnerJoinWithRunLengthEncodedProbe(boolean withFilter, boolean probeHashEnabled, boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        RowPagesBuilder buildPages = rowPagesBuilder(false, Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT))
                .row("20", 1L)
                .row("21", 2L)
                .row("21", 3L);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, false, taskContext, buildPages, Optional.empty(), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        RowPagesBuilder probePagesBuilder = rowPagesBuilder(probeHashEnabled, Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT))
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
        if (probeHashEnabled) {
            // Drop the hashChannel for all pages
            pages = dropChannel(pages, getHashChannels(probePagesBuilder, buildPages));
        }

        assertThat(pages.size()).isEqualTo(2);
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
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probePagesBuilder.getTypesWithoutHash(), buildPages.getTypesWithoutHash()))
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
    public void testUnwrapsLazyBlocks()
    {
        testUnwrapsLazyBlocks(false);
        testUnwrapsLazyBlocks(true);
    }

    private void testUnwrapsLazyBlocks(boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();
        DriverContext driverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();

        InternalJoinFilterFunction filterFunction = new TestInternalJoinFilterFunction(
                (leftPosition, leftPage, rightPosition, rightPage) -> {
                    // force loading of probe block
                    rightPage.getBlock(1).getLoadedBlock();
                    return true;
                });

        RowPagesBuilder buildPages = rowPagesBuilder(false, Ints.asList(0), ImmutableList.of(BIGINT)).addSequencePage(1, 0);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, true, taskContext, buildPages, Optional.of(filterFunction), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        RowPagesBuilder probePages = rowPagesBuilder(false, Ints.asList(0), ImmutableList.of(BIGINT, BIGINT));
        List<Page> probeInput = probePages.addSequencePage(1, 0, 0).build();
        probeInput = probeInput.stream()
                .map(page -> new Page(page.getBlock(0), new LazyBlock(1, () -> page.getBlock(1))))
                .collect(toImmutableList());

        OperatorFactory joinOperatorFactory = join(
                innerJoin(false, false),
                0,
                new PlanNodeId("test"),
                lookupSourceFactory,
                true,
                probePages.getTypes(),
                Ints.asList(0),
                getHashChannelAsInt(probePages),
                Optional.empty(),
                TYPE_OPERATORS);

        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);
        Operator operator = joinOperatorFactory.createOperator(driverContext);
        assertThat(operator.needsInput()).isTrue();
        operator.addInput(probeInput.get(0));
        operator.finish();

        Page output = operator.getOutput();
        assertThat(output.getBlock(1) instanceof LazyBlock).isFalse();
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
        RowPagesBuilder buildPages = rowPagesBuilder(true, Ints.asList(0), ImmutableList.of(BIGINT))
                .addSequencePage(entries, 42);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, true, taskContext, buildPages, Optional.of(filterFunction), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe matching the above 40 entries
        RowPagesBuilder probePages = rowPagesBuilder(false, Ints.asList(0), ImmutableList.of(BIGINT));
        List<Page> probeInput = probePages.addSequencePage(100, 0).build();
        OperatorFactory joinOperatorFactory = join(
                innerJoin(false, false),
                0,
                new PlanNodeId("test"),
                lookupSourceFactory,
                true,
                probePages.getTypes(),
                Ints.asList(0),
                getHashChannelAsInt(probePages),
                Optional.empty(),
                TYPE_OPERATORS);

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
        testInnerJoinWithNullProbe(false, false, false, false);
        testInnerJoinWithNullProbe(false, false, false, true);
        testInnerJoinWithNullProbe(false, false, true, false);
        testInnerJoinWithNullProbe(false, false, true, true);
        testInnerJoinWithNullProbe(false, true, false, false);
        testInnerJoinWithNullProbe(false, true, false, true);
        testInnerJoinWithNullProbe(false, true, true, false);
        testInnerJoinWithNullProbe(false, true, true, true);
        testInnerJoinWithNullProbe(true, false, false, false);
        testInnerJoinWithNullProbe(true, false, false, true);
        testInnerJoinWithNullProbe(true, false, true, false);
        testInnerJoinWithNullProbe(true, false, true, true);
        testInnerJoinWithNullProbe(true, true, false, false);
        testInnerJoinWithNullProbe(true, true, false, true);
        testInnerJoinWithNullProbe(true, true, true, false);
        testInnerJoinWithNullProbe(true, true, true, true);
    }

    private void testInnerJoinWithNullProbe(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled, boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), buildTypes)
                .row(1L)
                .row(2L)
                .row(3L);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, parallelBuild, taskContext, buildPages, Optional.empty(), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
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
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildPages.getTypesWithoutHash()))
                .row(1L, 1L)
                .row(1L, 1L)
                .row(2L, 2L)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test
    public void testInnerJoinWithOutputSingleMatch()
    {
        testInnerJoinWithOutputSingleMatch(false, false, false, false);
        testInnerJoinWithOutputSingleMatch(false, false, false, true);
        testInnerJoinWithOutputSingleMatch(false, false, true, false);
        testInnerJoinWithOutputSingleMatch(false, false, true, true);
        testInnerJoinWithOutputSingleMatch(false, true, false, false);
        testInnerJoinWithOutputSingleMatch(false, true, false, true);
        testInnerJoinWithOutputSingleMatch(false, true, true, false);
        testInnerJoinWithOutputSingleMatch(false, true, true, true);
        testInnerJoinWithOutputSingleMatch(true, false, false, false);
        testInnerJoinWithOutputSingleMatch(true, false, false, true);
        testInnerJoinWithOutputSingleMatch(true, false, true, false);
        testInnerJoinWithOutputSingleMatch(true, false, true, true);
        testInnerJoinWithOutputSingleMatch(true, true, false, false);
        testInnerJoinWithOutputSingleMatch(true, true, false, true);
        testInnerJoinWithOutputSingleMatch(true, true, true, false);
        testInnerJoinWithOutputSingleMatch(true, true, true, true);
    }

    private void testInnerJoinWithOutputSingleMatch(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled, boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();
        // build factory
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), buildTypes)
                .row(1L)
                .row(1L)
                .row(2L);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, parallelBuild, taskContext, buildPages, Optional.empty(), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
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

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test
    public void testInnerJoinWithNullBuild()
    {
        testInnerJoinWithNullBuild(false, false, false);
        testInnerJoinWithNullBuild(false, false, true);
        testInnerJoinWithNullBuild(false, true, false);
        testInnerJoinWithNullBuild(false, true, true);
        testInnerJoinWithNullBuild(true, false, false);
        testInnerJoinWithNullBuild(true, false, true);
        testInnerJoinWithNullBuild(true, true, false);
        testInnerJoinWithNullBuild(true, true, true);
    }

    private void testInnerJoinWithNullBuild(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), buildTypes)
                .row(1L)
                .row((String) null)
                .row((String) null)
                .row(1L)
                .row(2L);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, parallelBuild, taskContext, buildPages, Optional.empty());
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
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

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test
    public void testInnerJoinWithNullOnBothSides()
    {
        testInnerJoinWithNullOnBothSides(false, false, false);
        testInnerJoinWithNullOnBothSides(false, false, true);
        testInnerJoinWithNullOnBothSides(false, true, false);
        testInnerJoinWithNullOnBothSides(false, true, true);
        testInnerJoinWithNullOnBothSides(true, false, false);
        testInnerJoinWithNullOnBothSides(true, false, true);
        testInnerJoinWithNullOnBothSides(true, true, false);
        testInnerJoinWithNullOnBothSides(true, true, true);
    }

    private void testInnerJoinWithNullOnBothSides(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), buildTypes)
                .row(1L)
                .row((String) null)
                .row((String) null)
                .row(1L)
                .row(2L);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, parallelBuild, taskContext, buildPages, Optional.empty());
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
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

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test
    public void testProbeOuterJoin()
    {
        testProbeOuterJoin(false, false, false);
        testProbeOuterJoin(false, false, true);
        testProbeOuterJoin(false, true, false);
        testProbeOuterJoin(false, true, true);
        testProbeOuterJoin(true, false, false);
        testProbeOuterJoin(true, false, true);
        testProbeOuterJoin(true, true, false);
        testProbeOuterJoin(true, true, true);
    }

    private void testProbeOuterJoin(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(VARCHAR, BIGINT, BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT, BIGINT))
                .addSequencePage(10, 20, 30, 40);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, parallelBuild, taskContext, buildPages, Optional.empty());
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(VARCHAR, BIGINT, BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
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

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test
    public void testProbeOuterJoinWithFilterFunction()
    {
        testProbeOuterJoinWithFilterFunction(false, false, false);
        testProbeOuterJoinWithFilterFunction(false, false, true);
        testProbeOuterJoinWithFilterFunction(false, true, false);
        testProbeOuterJoinWithFilterFunction(false, true, true);
        testProbeOuterJoinWithFilterFunction(true, false, false);
        testProbeOuterJoinWithFilterFunction(true, false, true);
        testProbeOuterJoinWithFilterFunction(true, true, false);
        testProbeOuterJoinWithFilterFunction(true, true, true);
    }

    private void testProbeOuterJoinWithFilterFunction(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
    {
        TaskContext taskContext = createTaskContext();

        InternalJoinFilterFunction filterFunction = new TestInternalJoinFilterFunction(
                (leftPosition, leftPage, rightPosition, rightPage) -> BIGINT.getLong(rightPage.getBlock(1), rightPosition) >= 1025);

        // build factory
        List<Type> buildTypes = ImmutableList.of(VARCHAR, BIGINT, BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT, BIGINT))
                .addSequencePage(10, 20, 30, 40);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, parallelBuild, taskContext, buildPages, Optional.of(filterFunction));
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(VARCHAR, BIGINT, BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
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

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test
    public void testOuterJoinWithNullProbe()
    {
        testOuterJoinWithNullProbe(false, false, false, false);
        testOuterJoinWithNullProbe(false, false, false, true);
        testOuterJoinWithNullProbe(false, false, true, false);
        testOuterJoinWithNullProbe(false, false, true, true);
        testOuterJoinWithNullProbe(false, true, false, false);
        testOuterJoinWithNullProbe(false, true, false, true);
        testOuterJoinWithNullProbe(false, true, true, false);
        testOuterJoinWithNullProbe(false, true, true, true);
        testOuterJoinWithNullProbe(true, false, false, false);
        testOuterJoinWithNullProbe(true, false, false, true);
        testOuterJoinWithNullProbe(true, false, true, false);
        testOuterJoinWithNullProbe(true, false, true, true);
        testOuterJoinWithNullProbe(true, true, false, false);
        testOuterJoinWithNullProbe(true, true, false, true);
        testOuterJoinWithNullProbe(true, true, true, false);
        testOuterJoinWithNullProbe(true, true, true, true);
    }

    private void testOuterJoinWithNullProbe(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled, boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), buildTypes)
                .row(1L)
                .row(2L)
                .row(3L);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, parallelBuild, taskContext, buildPages, Optional.empty(), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
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

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test
    public void testOuterJoinWithNullProbeAndFilterFunction()
    {
        testOuterJoinWithNullProbeAndFilterFunction(false, false, false, false);
        testOuterJoinWithNullProbeAndFilterFunction(false, false, false, true);
        testOuterJoinWithNullProbeAndFilterFunction(false, false, true, false);
        testOuterJoinWithNullProbeAndFilterFunction(false, false, true, true);
        testOuterJoinWithNullProbeAndFilterFunction(false, true, false, false);
        testOuterJoinWithNullProbeAndFilterFunction(false, true, false, true);
        testOuterJoinWithNullProbeAndFilterFunction(false, true, true, false);
        testOuterJoinWithNullProbeAndFilterFunction(false, true, true, true);
        testOuterJoinWithNullProbeAndFilterFunction(true, false, false, false);
        testOuterJoinWithNullProbeAndFilterFunction(true, false, false, true);
        testOuterJoinWithNullProbeAndFilterFunction(true, false, true, false);
        testOuterJoinWithNullProbeAndFilterFunction(true, false, true, true);
        testOuterJoinWithNullProbeAndFilterFunction(true, true, false, false);
        testOuterJoinWithNullProbeAndFilterFunction(true, true, false, true);
        testOuterJoinWithNullProbeAndFilterFunction(true, true, true, false);
        testOuterJoinWithNullProbeAndFilterFunction(true, true, true, true);
    }

    private void testOuterJoinWithNullProbeAndFilterFunction(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled, boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();

        InternalJoinFilterFunction filterFunction = new TestInternalJoinFilterFunction(
                (leftPosition, leftPage, rightPosition, rightPage) -> BIGINT.getLong(rightPage.getBlock(0), rightPosition) == 1L);

        // build factory
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), buildTypes)
                .row(1L)
                .row(2L)
                .row(3L);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, parallelBuild, taskContext, buildPages, Optional.of(filterFunction), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
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

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test
    public void testOuterJoinWithNullBuild()
    {
        testOuterJoinWithNullBuild(false, false, false, false);
        testOuterJoinWithNullBuild(false, false, false, true);
        testOuterJoinWithNullBuild(false, false, true, false);
        testOuterJoinWithNullBuild(false, false, true, true);
        testOuterJoinWithNullBuild(false, true, false, false);
        testOuterJoinWithNullBuild(false, true, false, true);
        testOuterJoinWithNullBuild(false, true, true, false);
        testOuterJoinWithNullBuild(false, true, true, true);
        testOuterJoinWithNullBuild(true, false, false, false);
        testOuterJoinWithNullBuild(true, false, false, true);
        testOuterJoinWithNullBuild(true, false, true, false);
        testOuterJoinWithNullBuild(true, false, true, true);
        testOuterJoinWithNullBuild(true, true, false, false);
        testOuterJoinWithNullBuild(true, true, false, true);
        testOuterJoinWithNullBuild(true, true, true, false);
        testOuterJoinWithNullBuild(true, true, true, true);
    }

    private void testOuterJoinWithNullBuild(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled, boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), ImmutableList.of(BIGINT))
                .row(1L)
                .row((String) null)
                .row((String) null)
                .row(1L)
                .row(2L);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, parallelBuild, taskContext, buildPages, Optional.empty(), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
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

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test
    public void testOuterJoinWithNullBuildAndFilterFunction()
    {
        testOuterJoinWithNullBuildAndFilterFunction(false, false, false, false);
        testOuterJoinWithNullBuildAndFilterFunction(false, false, false, true);
        testOuterJoinWithNullBuildAndFilterFunction(false, false, true, false);
        testOuterJoinWithNullBuildAndFilterFunction(false, false, true, true);
        testOuterJoinWithNullBuildAndFilterFunction(false, true, false, false);
        testOuterJoinWithNullBuildAndFilterFunction(false, true, false, true);
        testOuterJoinWithNullBuildAndFilterFunction(false, true, true, false);
        testOuterJoinWithNullBuildAndFilterFunction(false, true, true, true);
        testOuterJoinWithNullBuildAndFilterFunction(true, false, false, false);
        testOuterJoinWithNullBuildAndFilterFunction(true, false, false, true);
        testOuterJoinWithNullBuildAndFilterFunction(true, false, true, false);
        testOuterJoinWithNullBuildAndFilterFunction(true, false, true, true);
        testOuterJoinWithNullBuildAndFilterFunction(true, true, false, false);
        testOuterJoinWithNullBuildAndFilterFunction(true, true, false, true);
        testOuterJoinWithNullBuildAndFilterFunction(true, true, true, false);
        testOuterJoinWithNullBuildAndFilterFunction(true, true, true, true);
    }

    private void testOuterJoinWithNullBuildAndFilterFunction(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled, boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();

        InternalJoinFilterFunction filterFunction = new TestInternalJoinFilterFunction(
                (leftPosition, leftPage, rightPosition, rightPage) ->
                        ImmutableSet.of(1L, 3L).contains(BIGINT.getLong(rightPage.getBlock(0), rightPosition)));

        // build factory
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), ImmutableList.of(BIGINT))
                .row(1L)
                .row((String) null)
                .row((String) null)
                .row(1L)
                .row(2L);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, parallelBuild, taskContext, buildPages, Optional.of(filterFunction), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
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

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test
    public void testOuterJoinWithNullOnBothSides()
    {
        testOuterJoinWithNullOnBothSides(false, false, false, false);
        testOuterJoinWithNullOnBothSides(false, false, false, true);
        testOuterJoinWithNullOnBothSides(false, false, true, false);
        testOuterJoinWithNullOnBothSides(false, false, true, true);
        testOuterJoinWithNullOnBothSides(false, true, false, false);
        testOuterJoinWithNullOnBothSides(false, true, false, true);
        testOuterJoinWithNullOnBothSides(false, true, true, false);
        testOuterJoinWithNullOnBothSides(false, true, true, true);
        testOuterJoinWithNullOnBothSides(true, false, false, false);
        testOuterJoinWithNullOnBothSides(true, false, false, true);
        testOuterJoinWithNullOnBothSides(true, false, true, false);
        testOuterJoinWithNullOnBothSides(true, false, true, true);
        testOuterJoinWithNullOnBothSides(true, true, false, false);
        testOuterJoinWithNullOnBothSides(true, true, false, true);
        testOuterJoinWithNullOnBothSides(true, true, true, false);
        testOuterJoinWithNullOnBothSides(true, true, true, true);
    }

    private void testOuterJoinWithNullOnBothSides(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled, boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), ImmutableList.of(BIGINT))
                .row(1L)
                .row((String) null)
                .row((String) null)
                .row(1L)
                .row(2L);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, parallelBuild, taskContext, buildPages, Optional.empty(), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
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
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildPages.getTypesWithoutHash()))
                .row(1L, 1L)
                .row(1L, 1L)
                .row(2L, 2L)
                .row(null, null)
                .row(3L, null)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test
    public void testOuterJoinWithNullOnBothSidesAndFilterFunction()
    {
        testOuterJoinWithNullOnBothSidesAndFilterFunction(false, false, false, false);
        testOuterJoinWithNullOnBothSidesAndFilterFunction(false, false, false, true);
        testOuterJoinWithNullOnBothSidesAndFilterFunction(false, false, true, false);
        testOuterJoinWithNullOnBothSidesAndFilterFunction(false, false, true, true);
        testOuterJoinWithNullOnBothSidesAndFilterFunction(false, true, false, false);
        testOuterJoinWithNullOnBothSidesAndFilterFunction(false, true, false, true);
        testOuterJoinWithNullOnBothSidesAndFilterFunction(false, true, true, false);
        testOuterJoinWithNullOnBothSidesAndFilterFunction(false, true, true, true);
        testOuterJoinWithNullOnBothSidesAndFilterFunction(true, false, false, false);
        testOuterJoinWithNullOnBothSidesAndFilterFunction(true, false, false, true);
        testOuterJoinWithNullOnBothSidesAndFilterFunction(true, false, true, false);
        testOuterJoinWithNullOnBothSidesAndFilterFunction(true, false, true, true);
        testOuterJoinWithNullOnBothSidesAndFilterFunction(true, true, false, false);
        testOuterJoinWithNullOnBothSidesAndFilterFunction(true, true, false, true);
        testOuterJoinWithNullOnBothSidesAndFilterFunction(true, true, true, false);
        testOuterJoinWithNullOnBothSidesAndFilterFunction(true, true, true, true);
    }

    private void testOuterJoinWithNullOnBothSidesAndFilterFunction(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled, boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();

        InternalJoinFilterFunction filterFunction = new TestInternalJoinFilterFunction(
                (leftPosition, leftPage, rightPosition, rightPage) ->
                        ImmutableSet.of(1L, 3L).contains(BIGINT.getLong(rightPage.getBlock(0), rightPosition)));

        // build factory
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), ImmutableList.of(BIGINT))
                .row(1L)
                .row((String) null)
                .row((String) null)
                .row(1L)
                .row(2L);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, parallelBuild, taskContext, buildPages, Optional.of(filterFunction), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
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
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildPages.getTypesWithoutHash()))
                .row(1L, 1L)
                .row(1L, 1L)
                .row(2L, null)
                .row(null, null)
                .row(3L, null)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test
    public void testMemoryLimit()
    {
        testMemoryLimit(false, false);
        testMemoryLimit(false, true);
        testMemoryLimit(true, false);
        testMemoryLimit(true, true);
    }

    private void testMemoryLimit(boolean parallelBuild, boolean buildHashEnabled)
    {
        TaskContext taskContext = TestingTaskContext.createTaskContext(executor, scheduledExecutor, TEST_SESSION, DataSize.ofBytes(100));

        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT, BIGINT))
                .addSequencePage(10, 20, 30, 40);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, parallelBuild, taskContext, buildPages, Optional.empty());
        instantiateBuildDrivers(buildSideSetup, taskContext);

        assertThatThrownBy(() -> buildLookupSource(executor, buildSideSetup))
                .isInstanceOf(ExceededMemoryLimitException.class)
                .hasMessageMatching("Query exceeded per-node memory limit of.*");
    }

    @Test
    public void testInnerJoinWithEmptyLookupSource()
    {
        testInnerJoinWithEmptyLookupSource(false, false, false, false);
        testInnerJoinWithEmptyLookupSource(false, false, false, true);
        testInnerJoinWithEmptyLookupSource(false, false, true, false);
        testInnerJoinWithEmptyLookupSource(false, false, true, true);
        testInnerJoinWithEmptyLookupSource(false, true, false, false);
        testInnerJoinWithEmptyLookupSource(false, true, false, true);
        testInnerJoinWithEmptyLookupSource(false, true, true, false);
        testInnerJoinWithEmptyLookupSource(false, true, true, true);
        testInnerJoinWithEmptyLookupSource(true, false, false, false);
        testInnerJoinWithEmptyLookupSource(true, false, false, true);
        testInnerJoinWithEmptyLookupSource(true, false, true, false);
        testInnerJoinWithEmptyLookupSource(true, false, true, true);
        testInnerJoinWithEmptyLookupSource(true, true, false, false);
        testInnerJoinWithEmptyLookupSource(true, true, false, true);
        testInnerJoinWithEmptyLookupSource(true, true, true, false);
        testInnerJoinWithEmptyLookupSource(true, true, true, true);
    }

    private void testInnerJoinWithEmptyLookupSource(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled, boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), buildTypes);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, parallelBuild, taskContext, buildPages, Optional.empty(), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        OperatorFactory joinOperatorFactory = join(
                innerJoin(false, false),
                0,
                new PlanNodeId("test"),
                lookupSourceFactoryManager,
                false,
                probePages.getTypes(),
                Ints.asList(0),
                getHashChannelAsInt(probePages),
                Optional.empty(),
                TYPE_OPERATORS);

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
        testLookupOuterJoinWithEmptyLookupSource(false, false, false, false);
        testLookupOuterJoinWithEmptyLookupSource(false, false, false, true);
        testLookupOuterJoinWithEmptyLookupSource(false, false, true, false);
        testLookupOuterJoinWithEmptyLookupSource(false, false, true, true);
        testLookupOuterJoinWithEmptyLookupSource(false, true, false, false);
        testLookupOuterJoinWithEmptyLookupSource(false, true, false, true);
        testLookupOuterJoinWithEmptyLookupSource(false, true, true, false);
        testLookupOuterJoinWithEmptyLookupSource(false, true, true, true);
        testLookupOuterJoinWithEmptyLookupSource(true, false, false, false);
        testLookupOuterJoinWithEmptyLookupSource(true, false, false, true);
        testLookupOuterJoinWithEmptyLookupSource(true, false, true, false);
        testLookupOuterJoinWithEmptyLookupSource(true, false, true, true);
        testLookupOuterJoinWithEmptyLookupSource(true, true, false, false);
        testLookupOuterJoinWithEmptyLookupSource(true, true, false, true);
        testLookupOuterJoinWithEmptyLookupSource(true, true, true, false);
        testLookupOuterJoinWithEmptyLookupSource(true, true, true, true);
    }

    private void testLookupOuterJoinWithEmptyLookupSource(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled, boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), buildTypes);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, parallelBuild, taskContext, buildPages, Optional.empty(), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        OperatorFactory joinOperatorFactory = join(
                JoinOperatorType.lookupOuterJoin(false),
                0,
                new PlanNodeId("test"),
                lookupSourceFactoryManager,
                false,
                probePages.getTypes(),
                Ints.asList(0),
                getHashChannelAsInt(probePages),
                Optional.empty(),
                TYPE_OPERATORS);

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
        testProbeOuterJoinWithEmptyLookupSource(false, false, false, false);
        testProbeOuterJoinWithEmptyLookupSource(false, false, false, true);
        testProbeOuterJoinWithEmptyLookupSource(false, false, true, false);
        testProbeOuterJoinWithEmptyLookupSource(false, false, true, true);
        testProbeOuterJoinWithEmptyLookupSource(false, true, false, false);
        testProbeOuterJoinWithEmptyLookupSource(false, true, false, true);
        testProbeOuterJoinWithEmptyLookupSource(false, true, true, false);
        testProbeOuterJoinWithEmptyLookupSource(false, true, true, true);
        testProbeOuterJoinWithEmptyLookupSource(true, false, false, false);
        testProbeOuterJoinWithEmptyLookupSource(true, false, false, true);
        testProbeOuterJoinWithEmptyLookupSource(true, false, true, false);
        testProbeOuterJoinWithEmptyLookupSource(true, false, true, true);
        testProbeOuterJoinWithEmptyLookupSource(true, true, false, false);
        testProbeOuterJoinWithEmptyLookupSource(true, true, false, true);
        testProbeOuterJoinWithEmptyLookupSource(true, true, true, false);
        testProbeOuterJoinWithEmptyLookupSource(true, true, true, true);
    }

    private void testProbeOuterJoinWithEmptyLookupSource(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled, boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), buildTypes);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, parallelBuild, taskContext, buildPages, Optional.empty(), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
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
                getHashChannelAsInt(probePages),
                Optional.empty(),
                TYPE_OPERATORS);

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
        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test
    public void testFullOuterJoinWithEmptyLookupSource()
    {
        testFullOuterJoinWithEmptyLookupSource(false, false, false, false);
        testFullOuterJoinWithEmptyLookupSource(false, false, false, true);
        testFullOuterJoinWithEmptyLookupSource(false, false, true, false);
        testFullOuterJoinWithEmptyLookupSource(false, false, true, true);
        testFullOuterJoinWithEmptyLookupSource(false, true, false, false);
        testFullOuterJoinWithEmptyLookupSource(false, true, false, true);
        testFullOuterJoinWithEmptyLookupSource(false, true, true, false);
        testFullOuterJoinWithEmptyLookupSource(false, true, true, true);
        testFullOuterJoinWithEmptyLookupSource(true, false, false, false);
        testFullOuterJoinWithEmptyLookupSource(true, false, false, true);
        testFullOuterJoinWithEmptyLookupSource(true, false, true, false);
        testFullOuterJoinWithEmptyLookupSource(true, false, true, true);
        testFullOuterJoinWithEmptyLookupSource(true, true, false, false);
        testFullOuterJoinWithEmptyLookupSource(true, true, false, true);
        testFullOuterJoinWithEmptyLookupSource(true, true, true, false);
        testFullOuterJoinWithEmptyLookupSource(true, true, true, true);
    }

    private void testFullOuterJoinWithEmptyLookupSource(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled, boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), buildTypes);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, parallelBuild, taskContext, buildPages, Optional.empty(), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
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
                getHashChannelAsInt(probePages),
                Optional.empty(),
                TYPE_OPERATORS);

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
        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test
    public void testInnerJoinWithNonEmptyLookupSourceAndEmptyProbe()
    {
        testInnerJoinWithNonEmptyLookupSourceAndEmptyProbe(false, false, false, false);
        testInnerJoinWithNonEmptyLookupSourceAndEmptyProbe(false, false, false, true);
        testInnerJoinWithNonEmptyLookupSourceAndEmptyProbe(false, false, true, false);
        testInnerJoinWithNonEmptyLookupSourceAndEmptyProbe(false, false, true, true);
        testInnerJoinWithNonEmptyLookupSourceAndEmptyProbe(false, true, false, false);
        testInnerJoinWithNonEmptyLookupSourceAndEmptyProbe(false, true, false, true);
        testInnerJoinWithNonEmptyLookupSourceAndEmptyProbe(false, true, true, false);
        testInnerJoinWithNonEmptyLookupSourceAndEmptyProbe(false, true, true, true);
        testInnerJoinWithNonEmptyLookupSourceAndEmptyProbe(true, false, false, false);
        testInnerJoinWithNonEmptyLookupSourceAndEmptyProbe(true, false, false, true);
        testInnerJoinWithNonEmptyLookupSourceAndEmptyProbe(true, false, true, false);
        testInnerJoinWithNonEmptyLookupSourceAndEmptyProbe(true, false, true, true);
        testInnerJoinWithNonEmptyLookupSourceAndEmptyProbe(true, true, false, false);
        testInnerJoinWithNonEmptyLookupSourceAndEmptyProbe(true, true, false, true);
        testInnerJoinWithNonEmptyLookupSourceAndEmptyProbe(true, true, true, false);
        testInnerJoinWithNonEmptyLookupSourceAndEmptyProbe(true, true, true, true);
    }

    private void testInnerJoinWithNonEmptyLookupSourceAndEmptyProbe(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled, boolean singleBigintLookupSource)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), buildTypes)
                .row(1L)
                .row(2L)
                .row((String) null)
                .row(3L);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, parallelBuild, taskContext, buildPages, Optional.empty(), singleBigintLookupSource);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages.build();
        OperatorFactory joinOperatorFactory = join(
                innerJoin(false, false),
                0,
                new PlanNodeId("test"),
                lookupSourceFactoryManager,
                false,
                probePages.getTypes(),
                Ints.asList(0),
                getHashChannelAsInt(probePages),
                Optional.empty(),
                TYPE_OPERATORS);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes)).build();
        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test
    public void testInnerJoinWithBlockingLookupSourceAndEmptyProbe()
            throws Exception
    {
        testInnerJoinWithBlockingLookupSourceAndEmptyProbe(false, false, false);
        testInnerJoinWithBlockingLookupSourceAndEmptyProbe(false, false, true);
        testInnerJoinWithBlockingLookupSourceAndEmptyProbe(false, true, false);
        testInnerJoinWithBlockingLookupSourceAndEmptyProbe(false, true, true);
        testInnerJoinWithBlockingLookupSourceAndEmptyProbe(true, false, false);
        testInnerJoinWithBlockingLookupSourceAndEmptyProbe(true, false, true);
        testInnerJoinWithBlockingLookupSourceAndEmptyProbe(true, true, false);
        testInnerJoinWithBlockingLookupSourceAndEmptyProbe(true, true, true);
    }

    private void testInnerJoinWithBlockingLookupSourceAndEmptyProbe(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
            throws Exception
    {
        // join that waits for build side to be collected
        TaskContext taskContext = createTaskContext();
        OperatorFactory joinOperatorFactory = createJoinOperatorFactoryWithBlockingLookupSource(taskContext, parallelBuild, probeHashEnabled, buildHashEnabled, true);
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
        joinOperatorFactory = createJoinOperatorFactoryWithBlockingLookupSource(taskContext, parallelBuild, probeHashEnabled, buildHashEnabled, false);
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
        testInnerJoinWithBlockingLookupSource(false, false, false);
        testInnerJoinWithBlockingLookupSource(false, false, true);
        testInnerJoinWithBlockingLookupSource(false, true, false);
        testInnerJoinWithBlockingLookupSource(false, true, true);
        testInnerJoinWithBlockingLookupSource(true, false, false);
        testInnerJoinWithBlockingLookupSource(true, false, true);
        testInnerJoinWithBlockingLookupSource(true, true, false);
        testInnerJoinWithBlockingLookupSource(true, true, true);
    }

    private void testInnerJoinWithBlockingLookupSource(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
            throws Exception
    {
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), ImmutableList.of(VARCHAR));
        Page probePage = getOnlyElement(probePages.addSequencePage(1, 0).build());

        // join that waits for build side to be collected
        TaskContext taskContext = createTaskContext();
        OperatorFactory joinOperatorFactory = createJoinOperatorFactoryWithBlockingLookupSource(taskContext, parallelBuild, probeHashEnabled, buildHashEnabled, true);
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
        joinOperatorFactory = createJoinOperatorFactoryWithBlockingLookupSource(taskContext, parallelBuild, probeHashEnabled, buildHashEnabled, false);
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

    @Test
    public void testInnerJoinLoadsPagesInOrder()
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(false, Ints.asList(0), buildTypes);
        for (int i = 0; i < 100_000; ++i) {
            buildPages.row("a");
        }
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, false, taskContext, buildPages, Optional.empty());
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(VARCHAR, INTEGER, INTEGER);
        RowPagesBuilder probePages = rowPagesBuilder(false, Ints.asList(0), probeTypes);
        probePages.row("a", 1L, 2L);
        WorkProcessorOperatorFactory joinOperatorFactory = (WorkProcessorOperatorFactory) innerJoinOperatorFactory(lookupSourceFactory, probePages, false);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        Page probePage = getOnlyElement(probePages.build());
        AtomicInteger totalProbePages = new AtomicInteger();
        WorkProcessor<Page> inputPages = WorkProcessor.create(() -> {
            int probePageNumber = totalProbePages.incrementAndGet();
            if (probePageNumber == 5) {
                return finished();
            }

            return ofResult(new Page(
                    1,
                    probePage.getBlock(0),
                    // this block should not be loaded by join operator as it's not being used by join
                    new LazyBlock(1, () -> probePage.getBlock(1)),
                    // this block is force loaded in test to ensure join doesn't fetch next probe page before joining
                    // and outputting current probe page
                    new LazyBlock(
                            1,
                            () -> {
                                // when loaded this block should be the latest one
                                assertThat(probePageNumber).isEqualTo(totalProbePages.get());
                                return probePage.getBlock(2);
                            })));
        });

        DriverContext driverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();
        OperatorContext operatorContext = driverContext.addOperatorContext(joinOperatorFactory.getOperatorId(), joinOperatorFactory.getPlanNodeId(), joinOperatorFactory.getOperatorType());
        WorkProcessorOperator joinOperator = joinOperatorFactory.create(
                new ProcessorContext(taskContext.getSession(), taskContext.getTaskMemoryContext(), operatorContext),
                inputPages);
        WorkProcessor<Page> outputPages = joinOperator.getOutputPages();
        int totalOutputPages = 0;
        for (int i = 0; i < 1_000_000; ++i) {
            if (!outputPages.process()) {
                // allow join to progress
                driverContext.getYieldSignal().resetYieldForTesting();
                continue;
            }

            if (outputPages.isFinished()) {
                break;
            }

            Page page = outputPages.getResult();
            totalOutputPages++;
            assertThat(page.getBlock(1).isLoaded()).isFalse();
            page.getBlock(2).getLoadedBlock();

            // yield to enforce more complex execution
            driverContext.getYieldSignal().forceYieldForTesting();
        }

        // make sure that multiple pages were produced for some probe pages
        assertThat(totalOutputPages > totalProbePages.get()).isTrue();
        assertThat(outputPages.isFinished()).isTrue();
    }

    private OperatorFactory createJoinOperatorFactoryWithBlockingLookupSource(TaskContext taskContext, boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled, boolean waitForBuild)
    {
        // build factory
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), buildTypes);
        BuildSideSetup buildSideSetup = setupBuildSide(nodePartitioningManager, parallelBuild, taskContext, buildPages, Optional.empty());
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        OperatorFactory joinOperatorFactory = join(
                innerJoin(false, waitForBuild),
                0,
                new PlanNodeId("test"),
                lookupSourceFactoryManager,
                false,
                probePages.getTypes(),
                Ints.asList(0),
                getHashChannelAsInt(probePages),
                Optional.empty(),
                TYPE_OPERATORS);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);

        return joinOperatorFactory;
    }

    private TaskContext createTaskContext()
    {
        return TestingTaskContext.createTaskContext(executor, scheduledExecutor, TEST_SESSION);
    }

    private static List<Integer> getHashChannels(RowPagesBuilder probe, RowPagesBuilder build)
    {
        ImmutableList.Builder<Integer> hashChannels = ImmutableList.builder();
        if (probe.getHashChannel().isPresent()) {
            hashChannels.add(probe.getHashChannel().get());
        }
        if (build.getHashChannel().isPresent()) {
            hashChannels.add(probe.getTypes().size() + build.getHashChannel().get());
        }
        return hashChannels.build();
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
                getHashChannelAsInt(probePages),
                Optional.empty(),
                TYPE_OPERATORS);
    }

    private static <T> List<T> concat(List<T> initialElements, List<T> moreElements)
    {
        return ImmutableList.copyOf(Iterables.concat(initialElements, moreElements));
    }
}
