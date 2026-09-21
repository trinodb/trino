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
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;
import io.airlift.units.DataSize;
import io.trino.connector.TestingColumnHandle;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.SetBuilderOperator.SetBuilderOperatorFactory;
import io.trino.operator.SetBuilderOperator.SetSupplier;
import io.trino.operator.project.PageProcessor;
import io.trino.spi.Page;
import io.trino.spi.QueryId;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.JoinCompiler;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Logical;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintId;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintWiringReport;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintWiringReport.CollectedConstraint;
import io.trino.sql.planner.runtimeconstraint.RuntimeMembershipPayload;
import io.trino.testing.NullOutputOperator.NullOutputOperatorFactory;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.predicate.Domain.all;
import static io.trino.spi.predicate.Domain.none;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN;
import static io.trino.sql.planner.runtimeconstraint.DistributedCompletionPolicy.UNION_ALL_PARTITIONS;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

class TestRuntimeConstraintPipelineInitialization
{
    private static final RuntimeConstraintId CONSTRAINT_ID = new RuntimeConstraintId("join_7_key_0");

    @Test
    void testCapturesScanBindingAfterAllFactoriesAreConstructed()
    {
        TestingColumnHandle column = new TestingColumnHandle("orderkey");
        OperatorFactory scan = new ScanFactory(new PlanNodeId("scan"), ImmutableList.of(new TestingColumnHandle("ignored"), column));
        OperatorFactory projection = new ChannelMappingFactory(0, 1);
        OperatorFactory join = new OriginFactory(new RuntimeConstraintRequest(CONSTRAINT_ID, 0));

        DriverFactory driverFactory = new DriverFactory(0, true, false, ImmutableList.of(scan, projection, join), OptionalInt.empty());
        RuntimeConstraintWiringContext context = new RuntimeConstraintWiringContext();

        driverFactory.initializeRuntimeConstraints(context);

        assertThat(context.getScanBindings())
                .containsExactly(new RuntimeConstraintWiringContext.ScanBinding(
                        new PlanNodeId("scan"),
                        column,
                        new RuntimeConstraintRequest(CONSTRAINT_ID, 1)));
        assertThat(context.getStoppedRequests()).isEmpty();
        assertThat(context.getReport().scans())
                .containsExactly(new RuntimeConstraintWiringReport.ScanWiring(
                        new PlanNodeId("scan"),
                        ImmutableList.of(new RuntimeConstraintWiringReport.Binding(new RuntimeConstraintRequest(CONSTRAINT_ID, 1), column))));
    }

    @Test
    void testProjectionMapsBothComparisonChannels()
    {
        OperatorFactory projection = FilterAndProjectOperator.createOperatorFactory(
                0,
                new PlanNodeId("projection"),
                () -> new PageProcessor(Optional.empty(), ImmutableList.of()),
                ImmutableList.of(BIGINT, BIGINT, BIGINT),
                ImmutableList.of(OptionalInt.of(0), OptionalInt.of(2), OptionalInt.of(1)),
                DataSize.ofBytes(0),
                0);
        ImmutableList.Builder<RuntimeConstraintRequest> input = ImmutableList.builder();

        projection.propagateRuntimeConstraint(
                RuntimeConstraintRequest.comparisonDemand(0, 1, LESS_THAN, false),
                input::add,
                new RuntimeConstraintWiringContext());

        assertThat(input.build()).containsExactly(RuntimeConstraintRequest.comparisonDemand(0, 2, LESS_THAN, false));
    }

    @Test
    void testLateRequestStopsAfterDriverFactoryIsClosed()
    {
        DriverFactory driverFactory = new DriverFactory(
                0,
                false,
                true,
                ImmutableList.of(new TestingOperatorFactory() {}),
                OptionalInt.empty());
        RuntimeConstraintWiringContext context = new RuntimeConstraintWiringContext();
        RuntimeConstraintRequest request = new RuntimeConstraintRequest(CONSTRAINT_ID, 0);
        driverFactory.noMoreDrivers();

        driverFactory.propagateRuntimeConstraints(ImmutableList.of(request), context);

        assertThat(context.getStoppedRequests())
                .extracting(RuntimeConstraintWiringContext.StoppedRequest::request)
                .containsExactly(request);
    }

    @Test
    void testLateCollectionRequestIsNotAppliedAfterDriverFactoryIsClosed()
    {
        DriverFactory driverFactory = new DriverFactory(
                0,
                false,
                true,
                ImmutableList.of(new TestingOperatorFactory() {}),
                OptionalInt.empty());
        TaskRuntimeConstraintManager manager = createManager();
        RuntimeConstraintWiringContext context = new RuntimeConstraintWiringContext(manager);
        RuntimeConstraintRequest request = RuntimeConstraintRequest.collection(
                CONSTRAINT_ID,
                0,
                EQUAL,
                false,
                BIGINT,
                true);
        driverFactory.initializeRuntimeConstraints(context);
        context.registerOutput(ImmutableList.of(driverFactory));
        driverFactory.noMoreDrivers();

        manager.addRuntimeConstraintWiringRequests(ImmutableList.of(request));

        assertThat(context.getStoppedRequests())
                .extracting(RuntimeConstraintWiringContext.StoppedRequest::request)
                .containsExactly(request);
        assertThat(context.getReport().sources()).isEmpty();
        assertThat(context.getReport().appliedOutputRequests()).isEmpty();
        assertThat(context.getReport().rejectedOutputRequests()).containsExactly(request);
    }

    @Test
    void testCollectionWithMultipleOutputDriversIsRejected()
    {
        TaskRuntimeConstraintManager manager = createManager();
        RuntimeConstraintWiringContext context = new RuntimeConstraintWiringContext(manager);
        RuntimeConstraintOutputOperatorFactory output = new RuntimeConstraintOutputOperatorFactory(
                new NullOutputOperatorFactory(1, new PlanNodeId("output")),
                ImmutableList.of(0),
                new RuntimeConstraintCollectionLimits(10, DataSize.ofBytes(1_024), 10, DataSize.ofBytes(1_024)),
                new TypeOperators());
        DriverFactory unmatched = new DriverFactory(0, false, true, ImmutableList.of(output.duplicate()), OptionalInt.of(1));
        DriverFactory matched = new DriverFactory(1, true, true, ImmutableList.of(output), OptionalInt.empty());
        List<DriverFactory> drivers = ImmutableList.of(unmatched, matched);
        drivers.forEach(driver -> driver.initializeRuntimeConstraints(context));
        RuntimeConstraintRequest request = RuntimeConstraintRequest.collection(CONSTRAINT_ID, 0, EQUAL, false, BIGINT, false);
        manager.addRuntimeConstraintWiringRequests(ImmutableList.of(request));

        context.registerOutput(drivers);
        drivers.forEach(DriverFactory::noMoreDrivers);

        assertThat(context.getReport().rejectedOutputRequests()).containsExactly(request);
        assertThat(context.getReport().appliedOutputRequests()).isEmpty();
        assertThat(context.getReport().sources()).isEmpty();
        assertThat(manager.acknowledgeContributionsAndGetBatch(0).contributions()).isEmpty();
    }

    @Test
    void testConstraintReachesEveryOutputDriver()
    {
        TaskRuntimeConstraintManager manager = createManager();
        RuntimeConstraintWiringContext context = new RuntimeConstraintWiringContext(manager);
        TestingColumnHandle column = new TestingColumnHandle("key");
        PlanNodeId firstScan = new PlanNodeId("first");
        PlanNodeId secondScan = new PlanNodeId("second");
        List<DriverFactory> drivers = ImmutableList.of(
                new DriverFactory(0, true, true, ImmutableList.of(new ScanFactory(firstScan, ImmutableList.of(column))), OptionalInt.empty()),
                new DriverFactory(1, true, true, ImmutableList.of(new ScanFactory(secondScan, ImmutableList.of(column))), OptionalInt.empty()));
        drivers.forEach(driver -> driver.initializeRuntimeConstraints(context));
        context.registerOutput(drivers);
        RuntimeConstraintRequest request = new RuntimeConstraintRequest(CONSTRAINT_ID, 0);

        manager.addRuntimeConstraintWiringRequests(ImmutableList.of(request));

        assertThat(context.getScanBindings()).extracting(RuntimeConstraintWiringContext.ScanBinding::scanId).containsExactly(firstScan, secondScan);
        assertThat(context.getScanBindings()).allSatisfy(binding -> {
            assertThat(binding.column()).isEqualTo(column);
            assertThat(binding.request().constraintId()).isEqualTo(CONSTRAINT_ID);
            assertThat(binding.request().channel()).isZero();
            assertThat(binding.request().subscriptionId()).isPresent();
        });
        assertThat(context.getReport().appliedOutputRequests()).containsExactly(request);
        assertThat(context.getReport().rejectedOutputRequests()).isEmpty();
    }

    @Test
    void testLateConstraintRequestTraversesClosedDriverFactory()
    {
        DriverFactory driverFactory = new DriverFactory(
                0,
                false,
                true,
                ImmutableList.of(new ChannelMappingFactory(0, 1)),
                OptionalInt.empty());
        TaskRuntimeConstraintManager manager = createManager();
        RuntimeConstraintWiringContext context = new RuntimeConstraintWiringContext(manager);
        RuntimeConstraintRequest constraint = new RuntimeConstraintRequest(CONSTRAINT_ID, 0);
        RuntimeConstraintRequest collection = RuntimeConstraintRequest.collection(
                new RuntimeConstraintId("collection"),
                0,
                EQUAL,
                false,
                BIGINT,
                true);
        driverFactory.initializeRuntimeConstraints(context);
        context.registerOutput(ImmutableList.of(driverFactory));
        driverFactory.noMoreDrivers();

        manager.addRuntimeConstraintWiringRequests(ImmutableList.of(constraint, collection));

        assertThat(context.getStoppedRequests())
                .extracting(RuntimeConstraintWiringContext.StoppedRequest::request)
                .satisfiesExactlyInAnyOrder(
                        request -> {
                            assertThat(request.constraintId()).isEqualTo(CONSTRAINT_ID);
                            assertThat(request.channel()).isEqualTo(1);
                            assertThat(request.isConstraint()).isTrue();
                            assertThat(request.subscriptionId()).isPresent();
                        },
                        request -> assertThat(request).isEqualTo(collection));
        assertThat(context.getReport().appliedOutputRequests()).containsExactly(constraint);
        assertThat(context.getReport().rejectedOutputRequests()).containsExactly(collection);
    }

    @Test
    void testLateRequestIsAcknowledgedAfterPropagationCompletes()
    {
        BlockingFactory output = new BlockingFactory();
        DriverFactory driverFactory = new DriverFactory(
                0,
                false,
                true,
                ImmutableList.of(output),
                OptionalInt.empty());
        TaskRuntimeConstraintManager manager = createManager();
        RuntimeConstraintWiringContext context = new RuntimeConstraintWiringContext(manager);
        RuntimeConstraintRequest request = new RuntimeConstraintRequest(CONSTRAINT_ID, 0);
        driverFactory.initializeRuntimeConstraints(context);
        context.registerOutput(ImmutableList.of(driverFactory));

        CompletableFuture<Void> propagation = CompletableFuture.runAsync(() -> manager.addRuntimeConstraintWiringRequests(ImmutableList.of(request)));
        assertThat(Uninterruptibles.awaitUninterruptibly(output.propagationStarted, 10, SECONDS)).isTrue();
        assertThat(context.getReport().appliedOutputRequests()).isEmpty();

        output.allowPropagation.countDown();
        propagation.join();
        assertThat(context.getReport().appliedOutputRequests()).containsExactly(request);
    }

    @Test
    void testReportSnapshotConcurrentWithRemoteSourceBinding()
    {
        RuntimeConstraintWiringContext context = new RuntimeConstraintWiringContext();
        PlanFragmentId sourceFragment = new PlanFragmentId("source");
        CountDownLatch start = new CountDownLatch(1);
        int bindings = 10_000;

        CompletableFuture<Void> writer = CompletableFuture.runAsync(() -> {
            Uninterruptibles.awaitUninterruptibly(start);
            for (int index = 0; index < bindings; index++) {
                context.bindRemoteSource(
                        ImmutableList.of(sourceFragment),
                        new RuntimeConstraintRequest(new RuntimeConstraintId("constraint_" + index), 0));
            }
        });
        CompletableFuture<Void> reader = CompletableFuture.runAsync(() -> {
            Uninterruptibles.awaitUninterruptibly(start);
            for (int index = 0; index < bindings; index++) {
                context.getReport();
            }
        });

        start.countDown();
        CompletableFuture.allOf(writer, reader).join();

        assertThat(context.getReport().remoteRequests()).hasSize(bindings);
    }

    private static TaskRuntimeConstraintManager createManager()
    {
        LocalMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext("test");
        return new TaskRuntimeConstraintManager(
                new TaskId(new StageId(new QueryId("query"), 0), 0, 0),
                0,
                memoryContext,
                () -> {});
    }

    @Test
    void testCollectionStoppedAtBarrierIsRejected()
    {
        RuntimeConstraintRequest request = RuntimeConstraintRequest.collection(CONSTRAINT_ID, 0, EQUAL, false, BIGINT, false);
        OperatorFactory barrier = new TestingOperatorFactory() {};
        DriverFactory driverFactory = new DriverFactory(0, false, true, ImmutableList.of(barrier, new OriginFactory(request)), OptionalInt.empty());
        TaskRuntimeConstraintManager manager = createManager();
        RuntimeConstraintWiringContext context = new RuntimeConstraintWiringContext(manager);
        driverFactory.initializeRuntimeConstraints(context);
        context.registerOutput(ImmutableList.of(driverFactory));

        assertThat(context.getReport().rejectedOutputRequests()).containsExactly(request);
        manager.addRuntimeConstraintWiringRequests(ImmutableList.of(request));
        assertThat(context.getReport().rejectedOutputRequests()).containsExactly(request);
        assertThat(context.getReport().sources()).isEmpty();
    }

    @Test
    void testUnknownOperatorStopsRequest()
    {
        OperatorFactory scan = new ScanFactory(new PlanNodeId("scan"), ImmutableList.of(new TestingColumnHandle("orderkey")));
        OperatorFactory barrier = new TestingOperatorFactory() {};
        OperatorFactory join = new OriginFactory(new RuntimeConstraintRequest(CONSTRAINT_ID, 0));

        DriverFactory driverFactory = new DriverFactory(0, true, false, ImmutableList.of(scan, barrier, join), OptionalInt.empty());
        RuntimeConstraintWiringContext context = new RuntimeConstraintWiringContext();

        driverFactory.initializeRuntimeConstraints(context);

        assertThat(context.getScanBindings()).isEmpty();
        assertThat(context.getStoppedRequests())
                .extracting(RuntimeConstraintWiringContext.StoppedRequest::request)
                .containsExactly(new RuntimeConstraintRequest(CONSTRAINT_ID, 0));
        assertThat(context.getReport().scans())
                .containsExactly(new RuntimeConstraintWiringReport.ScanWiring(new PlanNodeId("scan"), ImmutableList.of()));
    }

    @Test
    void testFilteringSemiJoinDemandActivatesPhysicalSetProducer()
    {
        PlanNodeId semiJoinId = new PlanNodeId("semi_join");
        TestingColumnHandle column = new TestingColumnHandle("orderkey");
        TypeOperators typeOperators = new TypeOperators();
        SetBuilderOperatorFactory setBuilder = new SetBuilderOperatorFactory(
                0,
                semiJoinId,
                BIGINT,
                0,
                10,
                new JoinCompiler(typeOperators),
                typeOperators);
        SetSupplier setSupplier = setBuilder.getSetProvider();
        OperatorFactory semiJoin = HashSemiJoinOperator.createOperatorFactory(1, semiJoinId, setSupplier, ImmutableList.of(BIGINT), 0);
        OperatorFactory filter = new OriginFactory(RuntimeConstraintRequest.requireTrue(1));
        OperatorFactory scan = new ScanFactory(new PlanNodeId("scan"), ImmutableList.of(column));

        DriverFactory buildDriver = new DriverFactory(0, false, false, ImmutableList.of(setBuilder), OptionalInt.of(1));
        DriverFactory probeDriver = new DriverFactory(1, true, false, ImmutableList.of(scan, semiJoin, filter), OptionalInt.empty());
        RuntimeConstraintWiringContext context = new RuntimeConstraintWiringContext();

        buildDriver.initializeRuntimeConstraints(context);
        probeDriver.initializeRuntimeConstraints(context);
        context.finish();

        RuntimeConstraintId constraintId = RuntimeConstraintRequest.semiJoinConstraintId(semiJoinId);
        assertThat(context.getReport().sources())
                .containsExactly(new RuntimeConstraintWiringReport.Source(
                        semiJoinId,
                        ImmutableList.of(new CollectedConstraint(constraintId, 0)),
                        ImmutableList.of(BIGINT)));
        assertThat(context.getReport().scans())
                .containsExactly(new RuntimeConstraintWiringReport.ScanWiring(
                        new PlanNodeId("scan"),
                        ImmutableList.of(new RuntimeConstraintWiringReport.Binding(constraintId, column))));
    }

    @Test
    void testFilterDerivesRequiredTrueDemandFromConjunct()
    {
        Symbol semiJoinOutput = new Symbol(BOOLEAN, "semi_join_output");
        OperatorFactory filter = FilterAndProjectOperator.createOperatorFactory(
                0,
                new PlanNodeId("filter"),
                () -> new PageProcessor(Optional.empty(), ImmutableList.of()),
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableList.of(),
                Optional.of(new Logical(Logical.Operator.AND, ImmutableList.of(
                        semiJoinOutput.toSymbolReference(),
                        new Constant(BOOLEAN, true)))),
                ImmutableMap.of(semiJoinOutput, 3),
                DataSize.ofBytes(0),
                0);

        assertThat(filter.getInputRuntimeConstraints())
                .containsExactly(RuntimeConstraintRequest.requireTrue(3));
    }

    @Test
    void testLocalExchangeFansOutLateRequestToEveryInputPipeline()
    {
        Object exchange = new Object();
        TestingColumnHandle first = new TestingColumnHandle("first");
        TestingColumnHandle second = new TestingColumnHandle("second");
        RuntimeConstraintWiringContext context = new RuntimeConstraintWiringContext();
        DriverFactory firstInput = new DriverFactory(
                0,
                true,
                false,
                ImmutableList.of(new ScanFactory(new PlanNodeId("first_scan"), ImmutableList.of(first)), new LocalSinkFactory(exchange, 0)),
                OptionalInt.empty());
        DriverFactory secondInput = new DriverFactory(
                1,
                true,
                false,
                ImmutableList.of(new ScanFactory(new PlanNodeId("second_scan"), ImmutableList.of(second)), new LocalSinkFactory(exchange, 0)),
                OptionalInt.empty());
        firstInput.initializeRuntimeConstraints(context);
        secondInput.initializeRuntimeConstraints(context);

        DriverFactory output = new DriverFactory(
                2,
                false,
                true,
                ImmutableList.of(new LocalSourceFactory(exchange), new OriginFactory(new RuntimeConstraintRequest(CONSTRAINT_ID, 0))),
                OptionalInt.empty());
        output.initializeRuntimeConstraints(context);

        assertThat(context.getScanBindings())
                .containsExactlyInAnyOrder(
                        new RuntimeConstraintWiringContext.ScanBinding(new PlanNodeId("first_scan"), first, new RuntimeConstraintRequest(CONSTRAINT_ID, 0)),
                        new RuntimeConstraintWiringContext.ScanBinding(new PlanNodeId("second_scan"), second, new RuntimeConstraintRequest(CONSTRAINT_ID, 0)));
    }

    @Test
    void testReplicatedCollectionIsCollectedAtFragmentOutput()
    {
        TypeOperators typeOperators = new TypeOperators();
        RuntimeConstraintOutputOperatorFactory output = new RuntimeConstraintOutputOperatorFactory(
                new TestingOperatorFactory() {},
                ImmutableList.of(0),
                new RuntimeConstraintCollectionLimits(10, DataSize.ofBytes(1_024), 10, DataSize.ofBytes(1_024)),
                typeOperators);
        RuntimeConstraintRequest request = RuntimeConstraintRequest.collection(
                CONSTRAINT_ID,
                0,
                EQUAL,
                false,
                BIGINT,
                true);
        RuntimeConstraintWiringContext context = new RuntimeConstraintWiringContext();
        ImmutableList.Builder<RuntimeConstraintRequest> upstream = ImmutableList.builder();

        output.propagateRuntimeConstraint(request, upstream::add, context);

        assertThat(upstream.build()).isEmpty();
        assertThat(context.getReport().sources())
                .containsExactly(new RuntimeConstraintWiringReport.Source(
                        request.collectionSourceId(),
                        ImmutableList.of(new CollectedConstraint(CONSTRAINT_ID, EQUAL, false, 0)),
                        ImmutableList.of(BIGINT),
                        UNION_ALL_PARTITIONS,
                        true));
    }

    @Test
    void testCollectionWithoutDriversReportsEmptyInput()
    {
        TaskRuntimeConstraintManager manager = createManager();
        RuntimeConstraintWiringContext context = new RuntimeConstraintWiringContext(manager);
        RuntimeConstraintOutputOperatorFactory output = new RuntimeConstraintOutputOperatorFactory(
                new NullOutputOperatorFactory(1, new PlanNodeId("output")),
                ImmutableList.of(0),
                new RuntimeConstraintCollectionLimits(10, DataSize.ofBytes(1_024), 10, DataSize.ofBytes(1_024)),
                new TypeOperators());
        RuntimeConstraintRequest request = RuntimeConstraintRequest.collection(CONSTRAINT_ID, 0, EQUAL, false, BIGINT, false);
        output.propagateRuntimeConstraint(request, _ -> {}, context);

        output.noMoreOperators();

        assertThat(manager.acknowledgeContributionsAndGetBatch(0).contributions())
                .singleElement().satisfies(contribution -> assertThat(((RuntimeMembershipPayload) contribution.payload()).scalarDomains())
                        .containsExactly(none(BIGINT)));
    }

    @Test
    void testPartialLateCollectionCannotReplaceUnrestrictedContribution()
    {
        try (var executor = newCachedThreadPool();
                var scheduledExecutor = newScheduledThreadPool(1)) {
            TaskContext taskContext = createTaskContext(executor, scheduledExecutor, testSessionBuilder().build());
            PipelineContext pipelineContext = taskContext.addPipelineContext(0, true, true, false);
            TaskRuntimeConstraintManager manager = taskContext.getRuntimeConstraintManager();
            RuntimeConstraintWiringContext context = new RuntimeConstraintWiringContext(manager);
            RuntimeConstraintOutputOperatorFactory output = new RuntimeConstraintOutputOperatorFactory(
                    new NullOutputOperatorFactory(1, new PlanNodeId("output")),
                    ImmutableList.of(0),
                    new RuntimeConstraintCollectionLimits(10, DataSize.ofBytes(1_024), 10, DataSize.ofBytes(1_024)),
                    new TypeOperators());
            Operator first = output.createOperator(pipelineContext.addDriverContext());
            first.addInput(new Page(createLongsBlock(11L)));
            first.finish();
            RuntimeConstraintRequest request = RuntimeConstraintRequest.collection(CONSTRAINT_ID, 0, EQUAL, false, BIGINT, false);

            output.propagateRuntimeConstraint(request, _ -> {}, context);
            long disabledSequence = manager.getContributionSequence();
            Operator second = output.createOperator(pipelineContext.addDriverContext());
            second.finish();
            output.noMoreOperators();

            assertThat(manager.getContributionSequence()).isEqualTo(disabledSequence);
            assertThat(manager.acknowledgeContributionsAndGetBatch(0).contributions())
                    .singleElement().satisfies(contribution -> assertThat(((RuntimeMembershipPayload) contribution.payload()).scalarDomains())
                            .containsExactly(all(BIGINT)));
        }
    }

    @Test
    void testCollectionInstalledAfterFactoryClosesBeforeInput()
    {
        try (var executor = newCachedThreadPool();
                var scheduledExecutor = newScheduledThreadPool(1)) {
            TaskContext taskContext = createTaskContext(executor, scheduledExecutor, testSessionBuilder().build());
            DriverContext driverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();
            TaskRuntimeConstraintManager manager = taskContext.getRuntimeConstraintManager();
            RuntimeConstraintWiringContext context = new RuntimeConstraintWiringContext(manager);
            RuntimeConstraintOutputOperatorFactory output = new RuntimeConstraintOutputOperatorFactory(
                    new NullOutputOperatorFactory(1, new PlanNodeId("output")),
                    ImmutableList.of(0),
                    new RuntimeConstraintCollectionLimits(10, DataSize.ofBytes(1_024), 10, DataSize.ofBytes(1_024)),
                    new TypeOperators());
            DriverFactory factory = new DriverFactory(
                    0,
                    true,
                    true,
                    ImmutableList.of(
                            new ValuesOperator.ValuesOperatorFactory(0, new PlanNodeId("values"), ImmutableList.of(new Page(createLongsBlock(11L)))),
                            output),
                    OptionalInt.empty());
            factory.initializeRuntimeConstraints(context);
            context.registerOutput(ImmutableList.of(factory));
            try (Driver driver = factory.createDriver(driverContext)) {
                factory.noMoreDrivers();
                RuntimeConstraintRequest request = RuntimeConstraintRequest.collection(CONSTRAINT_ID, 0, EQUAL, false, BIGINT, false);
                manager.addRuntimeConstraintWiringRequests(ImmutableList.of(request));
                assertThat(context.getReport().appliedOutputRequests()).containsExactly(request);
                assertThat(context.getStoppedRequests()).isEmpty();
                driver.processForNumberOfIterations(10);
                assertThat(driver.isFinished()).isTrue();
                assertThat(manager.acknowledgeContributionsAndGetBatch(0).contributions())
                        .singleElement().satisfies(contribution -> assertThat(((RuntimeMembershipPayload) contribution.payload()).scalarDomains())
                                .containsExactly(singleValue(BIGINT, 11L)));
            }
        }
    }

    private static final class OriginFactory
            extends TestingOperatorFactory
    {
        private final RuntimeConstraintRequest request;

        private OriginFactory(RuntimeConstraintRequest request)
        {
            this.request = request;
        }

        @Override
        public List<RuntimeConstraintRequest> getInputRuntimeConstraints()
        {
            return ImmutableList.of(request);
        }
    }

    private static final class ChannelMappingFactory
            extends TestingOperatorFactory
    {
        private final int outputChannel;
        private final int inputChannel;

        private ChannelMappingFactory(int outputChannel, int inputChannel)
        {
            this.outputChannel = outputChannel;
            this.inputChannel = inputChannel;
        }

        @Override
        public void propagateRuntimeConstraint(
                RuntimeConstraintRequest request,
                Consumer<RuntimeConstraintRequest> input,
                RuntimeConstraintWiringContext context)
        {
            if (request.channel() != outputChannel) {
                context.stop(this, request);
                return;
            }
            input.accept(request.withChannel(inputChannel));
        }
    }

    private static final class BlockingFactory
            extends TestingOperatorFactory
    {
        private final CountDownLatch propagationStarted = new CountDownLatch(1);
        private final CountDownLatch allowPropagation = new CountDownLatch(1);

        @Override
        public void propagateRuntimeConstraint(
                RuntimeConstraintRequest request,
                Consumer<RuntimeConstraintRequest> input,
                RuntimeConstraintWiringContext context)
        {
            propagationStarted.countDown();
            assertThat(Uninterruptibles.awaitUninterruptibly(allowPropagation, 10, SECONDS)).isTrue();
            context.stop(this, request);
        }
    }

    private static final class ScanFactory
            extends TestingOperatorFactory
    {
        private final PlanNodeId scanId;
        private final List<TestingColumnHandle> columns;

        private ScanFactory(PlanNodeId scanId, List<TestingColumnHandle> columns)
        {
            this.scanId = scanId;
            this.columns = columns;
        }

        @Override
        public void propagateRuntimeConstraint(
                RuntimeConstraintRequest request,
                Consumer<RuntimeConstraintRequest> input,
                RuntimeConstraintWiringContext context)
        {
            context.bindScan(scanId, columns.get(request.channel()), request);
        }

        @Override
        public void completeRuntimeConstraintWiring(RuntimeConstraintWiringContext context)
        {
            context.completeScan(scanId);
        }
    }

    private static final class LocalSourceFactory
            extends TestingOperatorFactory
    {
        private final Object exchange;

        private LocalSourceFactory(Object exchange)
        {
            this.exchange = exchange;
        }

        @Override
        public void propagateRuntimeConstraint(
                RuntimeConstraintRequest request,
                Consumer<RuntimeConstraintRequest> input,
                RuntimeConstraintWiringContext context)
        {
            context.bindLocalSource(exchange, request);
        }
    }

    private static final class LocalSinkFactory
            extends TestingOperatorFactory
    {
        private final Object exchange;
        private final int inputChannel;

        private LocalSinkFactory(Object exchange, int inputChannel)
        {
            this.exchange = exchange;
            this.inputChannel = inputChannel;
        }

        @Override
        public void propagateRuntimeConstraint(
                RuntimeConstraintRequest request,
                Consumer<RuntimeConstraintRequest> input,
                RuntimeConstraintWiringContext context)
        {
            input.accept(request.withChannel(inputChannel));
        }

        @Override
        public void registerRuntimeConstraintInput(Consumer<List<RuntimeConstraintRequest>> requests, RuntimeConstraintWiringContext context)
        {
            context.registerLocalConsumer(exchange, requests);
        }
    }

    private abstract static class TestingOperatorFactory
            implements OperatorFactory
    {
        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void noMoreOperators() {}

        @Override
        public OperatorFactory duplicate()
        {
            return this;
        }
    }
}
