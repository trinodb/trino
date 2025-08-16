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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.ExceededMemoryLimitException;
import io.trino.RowPagesBuilder;
import io.trino.connector.CatalogServiceProvider;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.execution.TaskStateMachine;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.Driver;
import io.trino.operator.DriverContext;
import io.trino.operator.Operator;
import io.trino.operator.OperatorAssertion;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.operator.PagesIndex;
import io.trino.operator.TaskContext;
import io.trino.operator.ValuesOperator.ValuesOperatorFactory;
import io.trino.operator.index.PageBuffer;
import io.trino.operator.index.PageBufferOperator.PageBufferOperatorFactory;
import io.trino.operator.join.JoinTestUtils.BuildSideSetup;
import io.trino.operator.join.JoinTestUtils.DummySpillerFactory;
import io.trino.operator.join.JoinTestUtils.TestInternalJoinFilterFunction;
import io.trino.plugin.base.metrics.TDigestHistogram;
import io.trino.spi.Page;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spiller.GenericPartitioningSpillerFactory;
import io.trino.spiller.PartitioningSpillerFactory;
import io.trino.spiller.SingleStreamSpillerFactory;
import io.trino.sql.planner.PartitionFunctionProvider;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.MaterializedResult;
import io.trino.testing.TestingTaskContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.SequencePageBuilder.createSequencePage;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.memory.context.CoarseGrainLocalMemoryContext.DEFAULT_GRANULARITY;
import static io.trino.operator.HashArraySizeSupplier.defaultHashArraySizeSupplier;
import static io.trino.operator.JoinOperatorType.fullOuterJoin;
import static io.trino.operator.JoinOperatorType.innerJoin;
import static io.trino.operator.JoinOperatorType.lookupOuterJoin;
import static io.trino.operator.JoinOperatorType.probeOuterJoin;
import static io.trino.operator.OperatorAssertion.assertOperatorEquals;
import static io.trino.operator.OperatorFactories.spillingJoin;
import static io.trino.operator.SpillMetrics.SPILL_COUNT_METRIC_NAME;
import static io.trino.operator.join.JoinTestUtils.buildLookupSource;
import static io.trino.operator.join.JoinTestUtils.innerJoinOperatorFactory;
import static io.trino.operator.join.JoinTestUtils.instantiateBuildDrivers;
import static io.trino.operator.join.JoinTestUtils.runDriverInThread;
import static io.trino.operator.join.JoinTestUtils.setupBuildSide;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Arrays.asList;
import static java.util.Collections.nCopies;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestHashJoinOperator
{
    private static final int PARTITION_COUNT = 4;
    private static final SingleStreamSpillerFactory SINGLE_STREAM_SPILLER_FACTORY = new DummySpillerFactory();
    private static final PartitioningSpillerFactory PARTITIONING_SPILLER_FACTORY = new GenericPartitioningSpillerFactory(SINGLE_STREAM_SPILLER_FACTORY);
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();
    private static final long SMALL_MEMORY_POOL_BYTES = DataSize.of(1, DataSize.Unit.MEGABYTE).toBytes();

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
        testInnerJoin(true);
        testInnerJoin(false);
    }

    private void testInnerJoin(boolean parallelBuild)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        RowPagesBuilder buildPages = rowPagesBuilder(Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT, BIGINT))
                .addSequencePage(10, 20, 30, 40);
        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, parallelBuild, taskContext, buildPages, Optional.empty(), false, SINGLE_STREAM_SPILLER_FACTORY);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        RowPagesBuilder probePages = rowPagesBuilder(Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT, BIGINT));
        List<Page> probeInput = probePages
                .addSequencePage(1000, 0, 1000, 2000)
                .build();
        OperatorFactory joinOperatorFactory = innerJoinOperatorFactory(lookupSourceFactory, probePages, PARTITIONING_SPILLER_FACTORY, false);

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

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected);
    }

    @Test
    public void testInnerJoinWithRunLengthEncodedProbe()
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        RowPagesBuilder buildPages = rowPagesBuilder(Ints.asList(0), ImmutableList.of(VARCHAR))
                .addSequencePage(10, 20)
                .addSequencePage(10, 21);
        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, false, taskContext, buildPages, Optional.empty(), false, SINGLE_STREAM_SPILLER_FACTORY);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        RowPagesBuilder probePages = rowPagesBuilder(Ints.asList(0), ImmutableList.of(VARCHAR));
        List<Page> probeInput = ImmutableList.of(
                new Page(RunLengthEncodedBlock.create(VARCHAR, Slices.utf8Slice("20"), 2)),
                new Page(RunLengthEncodedBlock.create(VARCHAR, Slices.utf8Slice("-1"), 2)),
                new Page(RunLengthEncodedBlock.create(VARCHAR, Slices.utf8Slice("21"), 2)));
        OperatorFactory joinOperatorFactory = innerJoinOperatorFactory(lookupSourceFactory, probePages, PARTITIONING_SPILLER_FACTORY, false);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probePages.getTypes(), buildPages.getTypes()))
                .row("20", "20")
                .row("20", "20")
                .row("21", "21")
                .row("21", "21")
                .row("21", "21")
                .row("21", "21")
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected);
    }

    @Test
    public void testYield()
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
        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, true, taskContext, buildPages, Optional.of(filterFunction), false, SINGLE_STREAM_SPILLER_FACTORY);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe matching the above 40 entries
        RowPagesBuilder probePages = rowPagesBuilder(Ints.asList(0), ImmutableList.of(BIGINT));
        List<Page> probeInput = probePages.addSequencePage(100, 0).build();
        OperatorFactory joinOperatorFactory = spillingJoin(
                innerJoin(false, false),
                0,
                new PlanNodeId("test"),
                lookupSourceFactory,
                probePages.getTypes(),
                Ints.asList(0),
                Optional.empty(),
                OptionalInt.of(1),
                PARTITIONING_SPILLER_FACTORY,
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

    private enum WhenSpill
    {
        DURING_BUILD, AFTER_BUILD, DURING_USAGE, NEVER
    }

    @Test
    public void testInnerJoinWithSpill()
            throws Exception
    {
        // spill all
        innerJoinWithSpill(nCopies(PARTITION_COUNT, WhenSpill.NEVER), SINGLE_STREAM_SPILLER_FACTORY, PARTITIONING_SPILLER_FACTORY);
        innerJoinWithSpill(nCopies(PARTITION_COUNT, WhenSpill.DURING_BUILD), SINGLE_STREAM_SPILLER_FACTORY, PARTITIONING_SPILLER_FACTORY);
        innerJoinWithSpill(nCopies(PARTITION_COUNT, WhenSpill.AFTER_BUILD), SINGLE_STREAM_SPILLER_FACTORY, PARTITIONING_SPILLER_FACTORY);
        innerJoinWithSpill(nCopies(PARTITION_COUNT, WhenSpill.DURING_USAGE), SINGLE_STREAM_SPILLER_FACTORY, PARTITIONING_SPILLER_FACTORY);

        // spill one
        innerJoinWithSpill(concat(singletonList(WhenSpill.DURING_BUILD), nCopies(PARTITION_COUNT - 1, WhenSpill.NEVER)), SINGLE_STREAM_SPILLER_FACTORY, PARTITIONING_SPILLER_FACTORY);
        innerJoinWithSpill(concat(singletonList(WhenSpill.AFTER_BUILD), nCopies(PARTITION_COUNT - 1, WhenSpill.NEVER)), SINGLE_STREAM_SPILLER_FACTORY, PARTITIONING_SPILLER_FACTORY);
        innerJoinWithSpill(concat(singletonList(WhenSpill.DURING_USAGE), nCopies(PARTITION_COUNT - 1, WhenSpill.NEVER)), SINGLE_STREAM_SPILLER_FACTORY, PARTITIONING_SPILLER_FACTORY);

        innerJoinWithSpill(concat(asList(WhenSpill.DURING_BUILD, WhenSpill.AFTER_BUILD), nCopies(PARTITION_COUNT - 2, WhenSpill.NEVER)), SINGLE_STREAM_SPILLER_FACTORY, PARTITIONING_SPILLER_FACTORY);
        innerJoinWithSpill(concat(asList(WhenSpill.DURING_BUILD, WhenSpill.DURING_USAGE), nCopies(PARTITION_COUNT - 2, WhenSpill.NEVER)), SINGLE_STREAM_SPILLER_FACTORY, PARTITIONING_SPILLER_FACTORY);
    }

    @Test
    public void testInnerJoinWithFailingSpill()
    {
        // spill all
        testInnerJoinWithFailingSpill(nCopies(PARTITION_COUNT, WhenSpill.DURING_USAGE));
        testInnerJoinWithFailingSpill(nCopies(PARTITION_COUNT, WhenSpill.DURING_BUILD));
        testInnerJoinWithFailingSpill(nCopies(PARTITION_COUNT, WhenSpill.AFTER_BUILD));

        // spill one
        testInnerJoinWithFailingSpill(concat(singletonList(WhenSpill.DURING_USAGE), nCopies(PARTITION_COUNT - 1, WhenSpill.NEVER)));
        testInnerJoinWithFailingSpill(concat(singletonList(WhenSpill.DURING_BUILD), nCopies(PARTITION_COUNT - 1, WhenSpill.NEVER)));
        testInnerJoinWithFailingSpill(concat(singletonList(WhenSpill.AFTER_BUILD), nCopies(PARTITION_COUNT - 1, WhenSpill.NEVER)));

        testInnerJoinWithFailingSpill(concat(asList(WhenSpill.DURING_BUILD, WhenSpill.AFTER_BUILD), nCopies(PARTITION_COUNT - 2, WhenSpill.NEVER)));
        testInnerJoinWithFailingSpill(concat(asList(WhenSpill.DURING_BUILD, WhenSpill.DURING_USAGE), nCopies(PARTITION_COUNT - 2, WhenSpill.NEVER)));
    }

    private void testInnerJoinWithFailingSpill(List<WhenSpill> whenSpill)
    {
        assertThatThrownBy(() -> innerJoinWithSpill(
                whenSpill,
                new DummySpillerFactory().failSpill(),
                new GenericPartitioningSpillerFactory(new DummySpillerFactory())))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Spill failed");

        assertThatThrownBy(() -> innerJoinWithSpill(
                whenSpill,
                new DummySpillerFactory(),
                new GenericPartitioningSpillerFactory(new DummySpillerFactory().failSpill())))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Spill failed");

        assertThatThrownBy(() -> innerJoinWithSpill(
                whenSpill,
                new DummySpillerFactory().failUnspill(),
                new GenericPartitioningSpillerFactory(new DummySpillerFactory())))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Unspill failed");

        assertThatThrownBy(() -> innerJoinWithSpill(
                whenSpill,
                new DummySpillerFactory(),
                new GenericPartitioningSpillerFactory(new DummySpillerFactory().failUnspill())))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Unspill failed");
    }

    private void innerJoinWithSpill(List<WhenSpill> whenSpill, SingleStreamSpillerFactory buildSpillerFactory, PartitioningSpillerFactory joinSpillerFactory)
            throws Exception
    {
        TaskStateMachine taskStateMachine = new TaskStateMachine(new TaskId(new StageId("query", 0), 0, 0), executor);
        TaskContext taskContext = TestingTaskContext.createTaskContext(executor, scheduledExecutor, TEST_SESSION, taskStateMachine);

        DriverContext joinDriverContext = taskContext.addPipelineContext(2, true, true, false).addDriverContext();

        // force a yield for every match in LookupJoinOperator, set called to true after first
        AtomicBoolean called = new AtomicBoolean(false);
        InternalJoinFilterFunction filterFunction = new TestInternalJoinFilterFunction(
                (leftPosition, leftPage, rightPosition, rightPage) -> {
                    called.set(true);
                    joinDriverContext.getYieldSignal().forceYieldForTesting();
                    return true;
                });

        // build factory
        RowPagesBuilder buildPages = rowPagesBuilder(Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT))
                .addSequencePage(4, 20, 200)
                .addSequencePage(4, 20, 200)
                .addSequencePage(4, 30, 300)
                .addSequencePage(4, 40, 400);

        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, true, taskContext, buildPages, Optional.of(filterFunction), true, buildSpillerFactory);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        RowPagesBuilder probePages = rowPagesBuilder(Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT))
                .row("20", 123_000L)
                .row("20", 123_000L)
                .pageBreak()
                .addSequencePage(20, 0, 123_000)
                .addSequencePage(10, 30, 123_000);
        OperatorFactory joinOperatorFactory = innerJoinOperatorFactory(lookupSourceFactoryManager, probePages, joinSpillerFactory);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        List<Driver> buildDrivers = buildSideSetup.getBuildDrivers();
        int buildOperatorCount = buildDrivers.size();
        checkState(buildOperatorCount == whenSpill.size());
        LookupSourceFactory lookupSourceFactory = lookupSourceFactoryManager.getJoinBridge();

        try (Operator joinOperator = joinOperatorFactory.createOperator(joinDriverContext)) {
            // build lookup source
            ListenableFuture<LookupSourceProvider> lookupSourceProvider = lookupSourceFactory.createLookupSourceProvider();
            List<Boolean> revoked = new ArrayList<>(nCopies(buildOperatorCount, false));
            while (!lookupSourceProvider.isDone()) {
                for (int i = 0; i < buildOperatorCount; i++) {
                    checkErrors(taskStateMachine);
                    buildDrivers.get(i).processForNumberOfIterations(1);
                    HashBuilderOperator buildOperator = buildSideSetup.getBuildOperators().get(i);
                    if (whenSpill.get(i) == WhenSpill.DURING_BUILD && buildOperator.getOperatorContext().getReservedRevocableBytes() > 0) {
                        checkState(!lookupSourceProvider.isDone(), "Too late, LookupSource already done");
                        revokeMemory(buildOperator);
                        revoked.set(i, true);
                    }
                }
            }
            getFutureValue(lookupSourceProvider).close();
            assertThat(revoked)
                    .describedAs("Some operators not spilled before LookupSource built")
                    .isEqualTo(whenSpill.stream().map(WhenSpill.DURING_BUILD::equals).collect(toImmutableList()));

            for (int i = 0; i < buildOperatorCount; i++) {
                if (whenSpill.get(i) == WhenSpill.AFTER_BUILD) {
                    revokeMemory(buildSideSetup.getBuildOperators().get(i));
                }
            }

            for (Driver buildDriver : buildDrivers) {
                runDriverInThread(executor, buildDriver);
            }

            ValuesOperatorFactory valuesOperatorFactory = new ValuesOperatorFactory(17, new PlanNodeId("values"), probePages.build());

            PageBuffer pageBuffer = new PageBuffer(10);
            PageBufferOperatorFactory pageBufferOperatorFactory = new PageBufferOperatorFactory(18, new PlanNodeId("pageBuffer"), pageBuffer, "PageBuffer");

            Driver joinDriver = Driver.createDriver(joinDriverContext,
                    valuesOperatorFactory.createOperator(joinDriverContext),
                    joinOperator,
                    pageBufferOperatorFactory.createOperator(joinDriverContext));
            while (!called.get()) { // process first row of first page of LookupJoinOperator
                processRow(joinDriver, taskStateMachine);
            }

            for (int i = 0; i < buildOperatorCount; i++) {
                if (whenSpill.get(i) == WhenSpill.DURING_USAGE) {
                    triggerMemoryRevokingAndWait(buildSideSetup.getBuildOperators().get(i), taskStateMachine);
                }
            }

            // process remaining LookupJoinOperator pages
            while (!joinDriver.isFinished()) {
                checkErrors(taskStateMachine);
                processRow(joinDriver, taskStateMachine);
            }
            checkErrors(taskStateMachine);

            List<Page> actualPages = getPages(pageBuffer);

            MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probePages.getTypes(), buildPages.getTypes()))
                    .row("20", 123_000L, "20", 200L)
                    .row("20", 123_000L, "20", 200L)
                    .row("20", 123_000L, "20", 200L)
                    .row("20", 123_000L, "20", 200L)
                    .row("30", 123_000L, "30", 300L)
                    .row("31", 123_001L, "31", 301L)
                    .row("32", 123_002L, "32", 302L)
                    .row("33", 123_003L, "33", 303L)
                    .build();

            assertThat(getProperColumns(joinOperator, concat(probePages.getTypes(), buildPages.getTypes()), actualPages).getMaterializedRows()).containsExactlyInAnyOrderElementsOf(expected.getMaterializedRows());

            Metrics probeMetrics = joinOperator.getOperatorContext()
                    .getOperatorStats()
                    .getMetrics();

            // The lookup-join (probe side) keeps its own SpillMetrics.
            if (!whenSpill.stream().allMatch(when -> when == WhenSpill.NEVER)) {
                TDigestHistogram probeSpillCount = (TDigestHistogram) probeMetrics.getMetrics().get("Probe: " + SPILL_COUNT_METRIC_NAME);
                assertThat(probeSpillCount.getDigest().getMax()).isNotNegative();
            }
        }
        finally {
            joinOperatorFactory.noMoreOperators();
        }
    }

    private static void processRow(Driver joinDriver, TaskStateMachine taskStateMachine)
    {
        joinDriver.process(new Duration(1, NANOSECONDS), 1);
        checkErrors(taskStateMachine);
    }

    private static void checkErrors(TaskStateMachine taskStateMachine)
    {
        if (taskStateMachine.getFailureCauses().size() > 0) {
            Throwable exception = requireNonNull(taskStateMachine.getFailureCauses().peek());
            throw new RuntimeException(exception.getMessage(), exception);
        }
    }

    private static void revokeMemory(HashBuilderOperator operator)
    {
        getFutureValue(operator.startMemoryRevoke());
        operator.finishMemoryRevoke();
        checkState(operator.getState() == HashBuilderOperator.State.SPILLING_INPUT || operator.getState() == HashBuilderOperator.State.INPUT_SPILLED);
    }

    private static void triggerMemoryRevokingAndWait(HashBuilderOperator operator, TaskStateMachine taskStateMachine)
            throws Exception
    {
        // When there is background thread running Driver, we must delegate memory revoking to that thread
        operator.getOperatorContext().requestMemoryRevoking();
        while (operator.getOperatorContext().isMemoryRevokingRequested()) {
            checkErrors(taskStateMachine);
            Thread.sleep(10);
        }
        checkErrors(taskStateMachine);
        checkState(operator.getState() == HashBuilderOperator.State.SPILLING_INPUT || operator.getState() == HashBuilderOperator.State.INPUT_SPILLED);
    }

    private static List<Page> getPages(PageBuffer pageBuffer)
    {
        List<Page> result = new ArrayList<>();

        Page page = pageBuffer.poll();
        while (page != null) {
            result.add(page);
            page = pageBuffer.poll();
        }
        return result;
    }

    private static MaterializedResult getProperColumns(Operator joinOperator, List<Type> types, List<Page> actualPages)
    {
        return OperatorAssertion.toMaterializedResult(joinOperator.getOperatorContext().getSession(), types, actualPages);
    }

    @Test
    @Timeout(30)
    public void testBuildGracefulSpill()
            throws Exception
    {
        TaskStateMachine taskStateMachine = new TaskStateMachine(new TaskId(new StageId("query", 0), 0, 0), executor);
        TaskContext taskContext = TestingTaskContext.createTaskContext(executor, scheduledExecutor, TEST_SESSION, taskStateMachine);

        // build factory
        RowPagesBuilder buildPages = rowPagesBuilder(Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT))
                .addSequencePage(4, 20, 200);

        DummySpillerFactory buildSpillerFactory = new DummySpillerFactory();

        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, true, taskContext, buildPages, Optional.empty(), true, buildSpillerFactory);
        instantiateBuildDrivers(buildSideSetup, taskContext);

        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = buildSideSetup.getLookupSourceFactoryManager();
        PartitionedLookupSourceFactory lookupSourceFactory = lookupSourceFactoryManager.getJoinBridge();

        // finish probe before any build partition is spilled
        lookupSourceFactory.finishProbeOperator(OptionalInt.of(1));

        // spill build partition after probe is finished
        HashBuilderOperator hashBuilderOperator = buildSideSetup.getBuildOperators().get(0);
        hashBuilderOperator.startMemoryRevoke().get();
        hashBuilderOperator.finishMemoryRevoke();
        hashBuilderOperator.finish();

        // hash builder operator should not deadlock waiting for spilled lookup source to be disposed
        hashBuilderOperator.isBlocked().get();

        lookupSourceFactory.destroy();
        assertThat(hashBuilderOperator.isFinished()).isTrue();

        // Take the latest metrics the HashBuilderOperator reported
        Metrics metrics = hashBuilderOperator.getOperatorContext().getOperatorStats().getMetrics();

        TDigestHistogram spillCount = (TDigestHistogram) metrics.getMetrics().get("Index: " + SPILL_COUNT_METRIC_NAME);

        // The test triggers exactly one graceful spill
        assertThat(spillCount.getDigest().getMax())
                .describedAs("exact number of spills recorded")
                .isEqualTo(1);
    }

    @Test
    public void testInnerJoinWithNullProbe()
    {
        testInnerJoinWithNullProbe(true);
        testInnerJoinWithNullProbe(false);
    }

    private void testInnerJoinWithNullProbe(boolean parallelBuild)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(Ints.asList(0), buildTypes)
                .row("a")
                .row("b")
                .row("c");
        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, parallelBuild, taskContext, buildPages, Optional.empty(), false, SINGLE_STREAM_SPILLER_FACTORY);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b")
                .build();
        OperatorFactory joinOperatorFactory = innerJoinOperatorFactory(lookupSourceFactory, probePages, PARTITIONING_SPILLER_FACTORY);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildPages.getTypes()))
                .row("a", "a")
                .row("a", "a")
                .row("b", "b")
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected);
    }

    @Test
    public void testInnerJoinWithOutputSingleMatch()
    {
        testInnerJoinWithOutputSingleMatch(true);
        testInnerJoinWithOutputSingleMatch(false);
    }

    private void testInnerJoinWithOutputSingleMatch(boolean parallelBuild)
    {
        TaskContext taskContext = createTaskContext();
        // build factory
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(Ints.asList(0), buildTypes)
                .row("a")
                .row("a")
                .row("b");
        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, parallelBuild, taskContext, buildPages, Optional.empty(), false, SINGLE_STREAM_SPILLER_FACTORY);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row("a")
                .row("b")
                .row("c")
                .build();
        OperatorFactory joinOperatorFactory = innerJoinOperatorFactory(lookupSourceFactory, probePages, PARTITIONING_SPILLER_FACTORY, true);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("a", "a")
                .row("b", "b")
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected);
    }

    @Test
    public void testInnerJoinWithNullBuild()
    {
        testInnerJoinWithNullBuild(true);
        testInnerJoinWithNullBuild(false);
    }

    private void testInnerJoinWithNullBuild(boolean parallelBuild)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(Ints.asList(0), buildTypes)
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b");
        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, parallelBuild, taskContext, buildPages, Optional.empty(), false, SINGLE_STREAM_SPILLER_FACTORY);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row("a")
                .row("b")
                .row("c")
                .build();
        OperatorFactory joinOperatorFactory = innerJoinOperatorFactory(lookupSourceFactory, probePages, PARTITIONING_SPILLER_FACTORY, false);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("a", "a")
                .row("a", "a")
                .row("b", "b")
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected);
    }

    @Test
    public void testInnerJoinWithNullOnBothSides()
    {
        testInnerJoinWithNullOnBothSides(true);
        testInnerJoinWithNullOnBothSides(false);
    }

    private void testInnerJoinWithNullOnBothSides(boolean parallelBuild)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(Ints.asList(0), buildTypes)
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b");
        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, parallelBuild, taskContext, buildPages, Optional.empty(), false, SINGLE_STREAM_SPILLER_FACTORY);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row("a")
                .row("b")
                .row((String) null)
                .row("c")
                .build();
        OperatorFactory joinOperatorFactory = innerJoinOperatorFactory(lookupSourceFactory, probePages, PARTITIONING_SPILLER_FACTORY);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("a", "a")
                .row("a", "a")
                .row("b", "b")
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected);
    }

    @Test
    public void testProbeOuterJoin()
    {
        testProbeOuterJoin(true);
        testProbeOuterJoin(false);
    }

    private void testProbeOuterJoin(boolean parallelBuild)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(VARCHAR, BIGINT, BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT, BIGINT))
                .addSequencePage(10, 20, 30, 40);
        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, parallelBuild, taskContext, buildPages, Optional.empty(), false, SINGLE_STREAM_SPILLER_FACTORY);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(VARCHAR, BIGINT, BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .addSequencePage(15, 20, 1020, 2020)
                .build();
        OperatorFactory joinOperatorFactory = probeOuterJoinOperatorFactory(lookupSourceFactory, probePages);

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

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected);
    }

    @Test
    public void testProbeOuterJoinWithFilterFunction()
    {
        testProbeOuterJoinWithFilterFunction(true);
        testProbeOuterJoinWithFilterFunction(false);
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
        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, parallelBuild, taskContext, buildPages, Optional.of(filterFunction), false, SINGLE_STREAM_SPILLER_FACTORY);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(VARCHAR, BIGINT, BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .addSequencePage(15, 20, 1020, 2020)
                .build();
        OperatorFactory joinOperatorFactory = probeOuterJoinOperatorFactory(lookupSourceFactory, probePages);

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

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected);
    }

    @Test
    public void testOuterJoinWithNullProbe()
    {
        testOuterJoinWithNullProbe(true);
        testOuterJoinWithNullProbe(false);
    }

    private void testOuterJoinWithNullProbe(boolean parallelBuild)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(Ints.asList(0), buildTypes)
                .row("a")
                .row("b")
                .row("c");
        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, parallelBuild, taskContext, buildPages, Optional.empty(), false, SINGLE_STREAM_SPILLER_FACTORY);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b")
                .build();
        OperatorFactory joinOperatorFactory = probeOuterJoinOperatorFactory(lookupSourceFactory, probePages);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("a", "a")
                .row(null, null)
                .row(null, null)
                .row("a", "a")
                .row("b", "b")
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected);
    }

    @Test
    public void testOuterJoinWithNullProbeAndFilterFunction()
    {
        testOuterJoinWithNullProbeAndFilterFunction(true);
        testOuterJoinWithNullProbeAndFilterFunction(false);
    }

    private void testOuterJoinWithNullProbeAndFilterFunction(boolean parallelBuild)
    {
        TaskContext taskContext = createTaskContext();

        InternalJoinFilterFunction filterFunction = new TestInternalJoinFilterFunction(
                (leftPosition, leftPage, rightPosition, rightPage) -> VARCHAR.getSlice(rightPage.getBlock(0), rightPosition).toStringAscii().equals("a"));

        // build factory
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(Ints.asList(0), buildTypes)
                .row("a")
                .row("b")
                .row("c");
        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, parallelBuild, taskContext, buildPages, Optional.of(filterFunction), false, SINGLE_STREAM_SPILLER_FACTORY);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b")
                .build();
        OperatorFactory joinOperatorFactory = probeOuterJoinOperatorFactory(lookupSourceFactory, probePages);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("a", "a")
                .row(null, null)
                .row(null, null)
                .row("a", "a")
                .row("b", null)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected);
    }

    @Test
    public void testOuterJoinWithNullBuild()
    {
        testOuterJoinWithNullBuild(true);
        testOuterJoinWithNullBuild(false);
    }

    private void testOuterJoinWithNullBuild(boolean parallelBuild)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(Ints.asList(0), ImmutableList.of(VARCHAR))
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b");
        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, parallelBuild, taskContext, buildPages, Optional.empty(), false, SINGLE_STREAM_SPILLER_FACTORY);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row("a")
                .row("b")
                .row("c")
                .build();
        OperatorFactory joinOperatorFactory = probeOuterJoinOperatorFactory(lookupSourceFactory, probePages);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("a", "a")
                .row("a", "a")
                .row("b", "b")
                .row("c", null)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected);
    }

    @Test
    public void testOuterJoinWithNullBuildAndFilterFunction()
    {
        testOuterJoinWithNullBuildAndFilterFunction(true);
        testOuterJoinWithNullBuildAndFilterFunction(false);
    }

    private void testOuterJoinWithNullBuildAndFilterFunction(boolean parallelBuild)
    {
        TaskContext taskContext = createTaskContext();

        InternalJoinFilterFunction filterFunction = new TestInternalJoinFilterFunction(
                (leftPosition, leftPage, rightPosition, rightPage) ->
                        ImmutableSet.of("a", "c").contains(VARCHAR.getSlice(rightPage.getBlock(0), rightPosition).toStringAscii()));

        // build factory
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(Ints.asList(0), ImmutableList.of(VARCHAR))
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b");
        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, parallelBuild, taskContext, buildPages, Optional.of(filterFunction), false, SINGLE_STREAM_SPILLER_FACTORY);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row("a")
                .row("b")
                .row("c")
                .build();
        OperatorFactory joinOperatorFactory = probeOuterJoinOperatorFactory(lookupSourceFactory, probePages);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("a", "a")
                .row("a", "a")
                .row("b", null)
                .row("c", null)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected);
    }

    @Test
    public void testOuterJoinWithNullOnBothSides()
    {
        testOuterJoinWithNullOnBothSides(true);
        testOuterJoinWithNullOnBothSides(false);
    }

    private void testOuterJoinWithNullOnBothSides(boolean parallelBuild)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        RowPagesBuilder buildPages = rowPagesBuilder(Ints.asList(0), ImmutableList.of(VARCHAR))
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b");
        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, parallelBuild, taskContext, buildPages, Optional.empty(), false, SINGLE_STREAM_SPILLER_FACTORY);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row("a")
                .row("b")
                .row((String) null)
                .row("c")
                .build();
        OperatorFactory joinOperatorFactory = probeOuterJoinOperatorFactory(lookupSourceFactory, probePages);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildPages.getTypes()))
                .row("a", "a")
                .row("a", "a")
                .row("b", "b")
                .row(null, null)
                .row("c", null)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected);
    }

    @Test
    public void testOuterJoinWithNullOnBothSidesAndFilterFunction()
    {
        testOuterJoinWithNullOnBothSidesAndFilterFunction(true);
        testOuterJoinWithNullOnBothSidesAndFilterFunction(false);
    }

    private void testOuterJoinWithNullOnBothSidesAndFilterFunction(boolean parallelBuild)
    {
        TaskContext taskContext = createTaskContext();

        InternalJoinFilterFunction filterFunction = new TestInternalJoinFilterFunction(
                (leftPosition, leftPage, rightPosition, rightPage) ->
                        ImmutableSet.of("a", "c").contains(VARCHAR.getSlice(rightPage.getBlock(0), rightPosition).toStringAscii()));

        // build factory
        RowPagesBuilder buildPages = rowPagesBuilder(Ints.asList(0), ImmutableList.of(VARCHAR))
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b");
        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, parallelBuild, taskContext, buildPages, Optional.of(filterFunction), false, SINGLE_STREAM_SPILLER_FACTORY);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row("a")
                .row("b")
                .row((String) null)
                .row("c")
                .build();
        OperatorFactory joinOperatorFactory = probeOuterJoinOperatorFactory(lookupSourceFactory, probePages);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildPages.getTypes()))
                .row("a", "a")
                .row("a", "a")
                .row("b", null)
                .row(null, null)
                .row("c", null)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected);
    }

    @Test
    public void testMemoryLimit()
    {
        testMemoryLimit(true);
        testMemoryLimit(false);
    }

    private void testMemoryLimit(boolean parallelBuild)
    {
        TaskContext taskContext = TestingTaskContext.createTaskContext(executor, scheduledExecutor, TEST_SESSION, DataSize.ofBytes(100));

        RowPagesBuilder buildPages = rowPagesBuilder(Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT, BIGINT))
                .addSequencePage(10, 20, 30, 40);
        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, parallelBuild, taskContext, buildPages, Optional.empty(), false, SINGLE_STREAM_SPILLER_FACTORY);
        instantiateBuildDrivers(buildSideSetup, taskContext);

        assertThatThrownBy(() -> buildLookupSource(executor, buildSideSetup))
                .isInstanceOf(ExceededMemoryLimitException.class)
                .hasMessageMatching("Query exceeded per-node memory limit of.*");
    }

    @Test
    public void testHashBuilderFinishInputWaitsForMemory()
    {
        testHashBuilderFinishInputWaitsForMemory(true);
        testHashBuilderFinishInputWaitsForMemory(false);
    }

    private void testHashBuilderFinishInputWaitsForMemory(boolean spillEnabled)
    {
        DriverTestContext contexts = createDriverTestContext();
        OperatorContext operatorContext = contexts.operatorContext;
        OperatorContext anotherOperatorContext = contexts.anotherOperatorContext;
        ImmutableList<Type> types = ImmutableList.of(BIGINT, BIGINT);
        PartitionedLookupSourceFactory lookupSourceFactory = new PartitionedLookupSourceFactory(
                types,
                ImmutableList.of(BIGINT),
                ImmutableList.of(BIGINT),
                1,
                false,
                TYPE_OPERATORS);
        try (HashBuilderOperator operator = new HashBuilderOperator(
                operatorContext,
                lookupSourceFactory,
                0,
                ImmutableList.of(0),
                ImmutableList.of(1),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(),
                10_000,
                new PagesIndex.TestingFactory(false),
                spillEnabled,
                SINGLE_STREAM_SPILLER_FACTORY,
                defaultHashArraySizeSupplier(),
                1)) {
            // add enough pages to require memory reservation when finish() is called
            for (int i = 0; i < 100; i++) {
                operator.addInput(createSequencePage(types, 1));
            }

            // occupy the whole memory pool with another operator so finish() has to wait
            anotherOperatorContext.getOperatorMemoryContext().localUserMemoryContext().setBytes(SMALL_MEMORY_POOL_BYTES);
            operator.finish();
            assertThat(operator.getState()).isEqualTo(HashBuilderOperator.State.CONSUMING_INPUT);
            assertThat(operator.isFinished()).isFalse();
            if (spillEnabled) {
                assertThat(operatorContext.isWaitingForRevocableMemory()).isNotDone();
            }
            else {
                assertThat(operatorContext.isWaitingForMemory()).isNotDone();
            }

            // still not enough memory to create lookup source
            operator.finish();
            assertThat(operator.getState()).isEqualTo(HashBuilderOperator.State.CONSUMING_INPUT);
            assertThat(operator.isFinished()).isFalse();
            if (spillEnabled) {
                assertThat(operatorContext.isWaitingForRevocableMemory()).isNotDone();
            }
            else {
                assertThat(operatorContext.isWaitingForMemory()).isNotDone();
            }

            // free memory and let finish() proceed
            anotherOperatorContext.getOperatorMemoryContext().localUserMemoryContext().setBytes(0);
            operator.finish();
            assertThat(operator.getState()).isEqualTo(HashBuilderOperator.State.LOOKUP_SOURCE_BUILT);
            assertThat(operator.isFinished()).isFalse();
            if (spillEnabled) {
                assertThat(operatorContext.isWaitingForRevocableMemory()).isDone();
            }
            else {
                assertThat(operatorContext.isWaitingForMemory()).isDone();
            }
        }
        finally {
            operatorContext.destroy();
        }
    }

    @Test
    public void testHashBuilderUnspillWaitsForMemory()
            throws Exception
    {
        DriverTestContext contexts = createDriverTestContext();
        OperatorContext operatorContext = contexts.operatorContext;
        OperatorContext anotherOperatorContext = contexts.anotherOperatorContext;
        ImmutableList<Type> types = ImmutableList.of(BIGINT);
        PartitionedLookupSourceFactory lookupSourceFactory = new PartitionedLookupSourceFactory(
                types,
                types,
                ImmutableList.of(BIGINT),
                2,
                false,
                TYPE_OPERATORS);

        try (HashBuilderOperator operator = new HashBuilderOperator(
                operatorContext,
                lookupSourceFactory,
                0,
                ImmutableList.of(0),
                ImmutableList.of(0),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(),
                10_000,
                new PagesIndex.TestingFactory(false),
                true,
                SINGLE_STREAM_SPILLER_FACTORY,
                defaultHashArraySizeSupplier(),
                1)) {
            for (int i = 0; i < 100; i++) {
                operator.addInput(createSequencePage(types, 1));
            }

            // spill the index
            revokeMemory(operator);
            assertThat(operator.getState()).isEqualTo(HashBuilderOperator.State.SPILLING_INPUT);
            operator.finish();
            assertThat(operator.getState()).isEqualTo(HashBuilderOperator.State.INPUT_SPILLED);

            // request partition to trigger unspilling
            PartitionedConsumption<Supplier<LookupSource>> consumption = lookupSourceFactory.finishProbeOperator(OptionalInt.of(1)).get();
            PartitionedConsumption.Partition<Supplier<LookupSource>> partition = consumption.beginConsumption().next();
            ListenableFuture<Supplier<LookupSource>> lookupSourceFuture = partition.load();
            operator.finish();
            assertThat(operatorContext.isWaitingForMemory()).isDone();
            assertThat(operator.getState()).isEqualTo(HashBuilderOperator.State.INPUT_UNSPILLING);

            // block memory so unspilling cannot reserve memory
            anotherOperatorContext.getOperatorMemoryContext().localUserMemoryContext().setBytes(SMALL_MEMORY_POOL_BYTES);
            operator.finish();
            assertThat(operator.getState()).isEqualTo(HashBuilderOperator.State.INPUT_UNSPILLING);
            assertThat(operatorContext.isWaitingForMemory()).isNotDone();
            assertThat(lookupSourceFuture).isNotDone();

            // release memory and continue
            anotherOperatorContext.getOperatorMemoryContext().localUserMemoryContext().setBytes(0);
            operator.finish();
            assertThat(operator.getState()).isEqualTo(HashBuilderOperator.State.INPUT_UNSPILLED_AND_BUILT);

            assertThat(lookupSourceFuture).isDone();
            assertThat(operatorContext.isWaitingForMemory()).isDone();

            try (LookupSource lookupSource = lookupSourceFuture.get().get()) {
                assertThat(lookupSource.getJoinPositionCount()).isEqualTo(100);
            }
        }
        finally {
            operatorContext.destroy();
        }
    }

    @Test
    public void testMemoryRevokeCompactionUpdatesRevocableMemory()
    {
        DriverTestContext contexts = createDriverTestContext();
        OperatorContext operatorContext = contexts.operatorContext;
        ImmutableList<Type> types = ImmutableList.of(VARCHAR);
        PartitionedLookupSourceFactory lookupSourceFactory = new PartitionedLookupSourceFactory(
                types,
                types,
                ImmutableList.of(VARCHAR),
                2,
                false,
                TYPE_OPERATORS);

        try (HashBuilderOperator operator = new HashBuilderOperator(
                operatorContext,
                lookupSourceFactory,
                0,
                ImmutableList.of(0),
                ImmutableList.of(0),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(),
                10_000,
                new PagesIndex.TestingFactory(false),
                true,
                SINGLE_STREAM_SPILLER_FACTORY,
                defaultHashArraySizeSupplier(),
                DEFAULT_GRANULARITY)) {
            // add page to build index
            operator.addInput(new Page(new VariableWidthBlock(1, Slices.allocate(100000), new int[] {0, 1}, Optional.empty())));

            LocalMemoryContext revocableMemoryContext = operatorContext.localRevocableMemoryContext();
            long beforeBytes = revocableMemoryContext.getBytes();

            // trigger memory revocation which performs compaction
            getFutureValue(operator.startMemoryRevoke());
            operator.finishMemoryRevoke();
            assertThat(operator.getState()).isEqualTo(HashBuilderOperator.State.CONSUMING_INPUT);

            long afterBytes = revocableMemoryContext.getBytes();
            assertThat(afterBytes).isLessThan(beforeBytes);

            // subsequent revocation should revoke all memory
            getFutureValue(operator.startMemoryRevoke());
            operator.finishMemoryRevoke();
            assertThat(revocableMemoryContext.getBytes()).isEqualTo(0);
            assertThat(operator.getState()).isEqualTo(HashBuilderOperator.State.SPILLING_INPUT);
        }
        finally {
            operatorContext.destroy();
        }
    }

    @Test
    public void testInnerJoinWithEmptyLookupSource()
    {
        testInnerJoinWithEmptyLookupSource(true);
        testInnerJoinWithEmptyLookupSource(false);
    }

    private void testInnerJoinWithEmptyLookupSource(boolean parallelBuild)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(Ints.asList(0), buildTypes);
        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, parallelBuild, taskContext, buildPages, Optional.empty(), false, SINGLE_STREAM_SPILLER_FACTORY);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(Ints.asList(0), probeTypes);
        OperatorFactory joinOperatorFactory = spillingJoin(
                innerJoin(false, false),
                0,
                new PlanNodeId("test"),
                lookupSourceFactoryManager,
                probePages.getTypes(),
                Ints.asList(0),
                Optional.empty(),
                OptionalInt.of(1),
                PARTITIONING_SPILLER_FACTORY,
                TYPE_OPERATORS);

        // drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);
        Operator operator = joinOperatorFactory.createOperator(taskContext.addPipelineContext(0, true, true, false).addDriverContext());

        List<Page> pages = probePages.row("test").build();
        operator.addInput(pages.get(0));
        Page outputPage = operator.getOutput();
        assertThat(outputPage).isNull();
    }

    @Test
    public void testLookupOuterJoinWithEmptyLookupSource()
    {
        testLookupOuterJoinWithEmptyLookupSource(true);
        testLookupOuterJoinWithEmptyLookupSource(false);
    }

    private void testLookupOuterJoinWithEmptyLookupSource(boolean parallelBuild)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(Ints.asList(0), buildTypes);
        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, parallelBuild, taskContext, buildPages, Optional.empty(), false, SINGLE_STREAM_SPILLER_FACTORY);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(Ints.asList(0), probeTypes);
        OperatorFactory joinOperatorFactory = spillingJoin(
                lookupOuterJoin(false),
                0,
                new PlanNodeId("test"),
                lookupSourceFactoryManager,
                probePages.getTypes(),
                Ints.asList(0),
                Optional.empty(),
                OptionalInt.of(1),
                PARTITIONING_SPILLER_FACTORY,
                TYPE_OPERATORS);

        // drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);
        Operator operator = joinOperatorFactory.createOperator(taskContext.addPipelineContext(0, true, true, false).addDriverContext());

        List<Page> pages = probePages.row("test").build();
        operator.addInput(pages.get(0));
        Page outputPage = operator.getOutput();
        assertThat(outputPage).isNull();
    }

    @Test
    public void testProbeOuterJoinWithEmptyLookupSource()
    {
        testProbeOuterJoinWithEmptyLookupSource(true);
        testProbeOuterJoinWithEmptyLookupSource(false);
    }

    private void testProbeOuterJoinWithEmptyLookupSource(boolean parallelBuild)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(Ints.asList(0), buildTypes);
        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, parallelBuild, taskContext, buildPages, Optional.empty(), false, SINGLE_STREAM_SPILLER_FACTORY);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row("a")
                .row("b")
                .row((String) null)
                .row("c")
                .build();
        OperatorFactory joinOperatorFactory = spillingJoin(
                probeOuterJoin(false),
                0,
                new PlanNodeId("test"),
                lookupSourceFactoryManager,
                probePages.getTypes(),
                Ints.asList(0),
                Optional.empty(),
                OptionalInt.of(1),
                PARTITIONING_SPILLER_FACTORY,
                TYPE_OPERATORS);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("a", null)
                .row("b", null)
                .row(null, null)
                .row("c", null)
                .build();
        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected);
    }

    @Test
    public void testFullOuterJoinWithEmptyLookupSource()
    {
        testFullOuterJoinWithEmptyLookupSource(true);
        testFullOuterJoinWithEmptyLookupSource(false);
    }

    private void testFullOuterJoinWithEmptyLookupSource(boolean parallelBuild)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(Ints.asList(0), buildTypes);
        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, parallelBuild, taskContext, buildPages, Optional.empty(), false, SINGLE_STREAM_SPILLER_FACTORY);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row("a")
                .row("b")
                .row((String) null)
                .row("c")
                .build();
        OperatorFactory joinOperatorFactory = spillingJoin(
                fullOuterJoin(),
                0,
                new PlanNodeId("test"),
                lookupSourceFactoryManager,
                probePages.getTypes(),
                Ints.asList(0),
                Optional.empty(),
                OptionalInt.of(1),
                PARTITIONING_SPILLER_FACTORY,
                TYPE_OPERATORS);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("a", null)
                .row("b", null)
                .row(null, null)
                .row("c", null)
                .build();
        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected);
    }

    @Test
    public void testInnerJoinWithNonEmptyLookupSourceAndEmptyProbe()
    {
        testInnerJoinWithNonEmptyLookupSourceAndEmptyProbe(true);
        testInnerJoinWithNonEmptyLookupSourceAndEmptyProbe(false);
    }

    private void testInnerJoinWithNonEmptyLookupSourceAndEmptyProbe(boolean parallelBuild)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(Ints.asList(0), buildTypes)
                .row("a")
                .row("b")
                .row((String) null)
                .row("c");
        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, parallelBuild, taskContext, buildPages, Optional.empty(), false, SINGLE_STREAM_SPILLER_FACTORY);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages.build();
        OperatorFactory joinOperatorFactory = spillingJoin(
                innerJoin(false, false),
                0,
                new PlanNodeId("test"),
                lookupSourceFactoryManager,
                probePages.getTypes(),
                Ints.asList(0),
                Optional.empty(),
                OptionalInt.of(1),
                PARTITIONING_SPILLER_FACTORY,
                TYPE_OPERATORS);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(executor, buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes)).build();
        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected);
    }

    @Test
    public void testInnerJoinWithBlockingLookupSourceAndEmptyProbe()
            throws Exception
    {
        testInnerJoinWithBlockingLookupSourceAndEmptyProbe(true);
        testInnerJoinWithBlockingLookupSourceAndEmptyProbe(false);
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
        testInnerJoinWithBlockingLookupSource(true);
        testInnerJoinWithBlockingLookupSource(false);
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
        BuildSideSetup buildSideSetup = setupBuildSide(partitionFunctionProvider, parallelBuild, taskContext, buildPages, Optional.empty(), false, SINGLE_STREAM_SPILLER_FACTORY);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(Ints.asList(0), probeTypes);
        OperatorFactory joinOperatorFactory = spillingJoin(
                innerJoin(false, waitForBuild),
                0,
                new PlanNodeId("test"),
                lookupSourceFactoryManager,
                probePages.getTypes(),
                Ints.asList(0),
                Optional.empty(),
                OptionalInt.of(1),
                PARTITIONING_SPILLER_FACTORY,
                TYPE_OPERATORS);

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
            RowPagesBuilder probePages)
    {
        return spillingJoin(
                probeOuterJoin(false),
                0,
                new PlanNodeId("test"),
                lookupSourceFactoryManager,
                probePages.getTypes(),
                Ints.asList(0),
                Optional.empty(),
                OptionalInt.of(1),
                PARTITIONING_SPILLER_FACTORY,
                TYPE_OPERATORS);
    }

    private DriverTestContext createDriverTestContext()
    {
        TaskContext taskContext = TestingTaskContext.builder(executor, scheduledExecutor, TEST_SESSION)
                .setMemoryPoolSize(DataSize.ofBytes(SMALL_MEMORY_POOL_BYTES))
                .build();
        DriverContext driverContext = taskContext
                .addPipelineContext(0, false, false, false)
                .addDriverContext();
        OperatorContext operatorContext = driverContext
                .addOperatorContext(0, new PlanNodeId("0"), HashBuilderOperator.class.getName());
        OperatorContext anotherOperatorContext = driverContext
                .addOperatorContext(1, new PlanNodeId("1"), "another operator");
        return new DriverTestContext(taskContext, driverContext, operatorContext, anotherOperatorContext);
    }

    private record DriverTestContext(TaskContext taskContext, DriverContext driverContext, OperatorContext operatorContext, OperatorContext anotherOperatorContext) {}

    private static <T> List<T> concat(List<T> initialElements, List<T> moreElements)
    {
        return ImmutableList.copyOf(Iterables.concat(initialElements, moreElements));
    }
}
