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
package io.trino.sql.planner.runtimeconstraint;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.connector.TestingColumnHandle;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.operator.DriverContext;
import io.trino.operator.DriverFactory;
import io.trino.operator.FilterAndProjectOperator;
import io.trino.operator.Operator;
import io.trino.operator.OperatorFactory;
import io.trino.operator.RuntimeConstraintRequest;
import io.trino.operator.RuntimeConstraintWiringContext;
import io.trino.operator.TaskRuntimeConstraintManager;
import io.trino.operator.project.PageProcessor;
import io.trino.spi.QueryId;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.plan.PlanNodeId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintNullMatchMode.ORDINARY;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintPublicationState.CLOSED;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintPublicationState.DISABLED;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintPublicationState.FINAL;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintTransform.IDENTITY;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestRuntimeConstraintSubscriptions
        extends BasePlanTest
{
    private static final RuntimeConstraintId ROOT = new RuntimeConstraintId("root");
    private static final long GENERATION = 9;

    @Test
    void testFactoriesTransformActualUpdatesInOrder()
    {
        var planner = getPlanTester().getPlannerContext();
        TaskRuntimeConstraintManager manager = new TaskRuntimeConstraintManager(
                new TaskId(new StageId(new QueryId("query"), 1), 0, 0),
                GENERATION,
                newSimpleAggregatedMemoryContext().newLocalMemoryContext("subscriptions"),
                () -> {});
        RuntimeConstraintWiringContext context = new RuntimeConstraintWiringContext(
                manager,
                true,
                planner.getMetadata(),
                planner.getFunctionManager(),
                planner.getTypeOperators(),
                getPlanTester().getDefaultSession());
        List<Domain> observed = new ArrayList<>();
        TestingColumnHandle column = new TestingColumnHandle("key");
        PlanNodeId scanId = new PlanNodeId("scan");
        AtomicReference<DynamicFilter> scanFilter = new AtomicReference<>();
        OperatorFactory scan = new TestFactory()
        {
            @Override
            public void propagateRuntimeConstraint(RuntimeConstraintRequest request, Consumer<RuntimeConstraintRequest> input, RuntimeConstraintWiringContext wiring)
            {
                wiring.bindScan(scanId, column, request);
            }

            @Override
            public void completeRuntimeConstraintWiring(RuntimeConstraintWiringContext wiring)
            {
                wiring.completeScan(scanId, ImmutableList.of(column), scanFilter::set);
            }
        };
        OperatorFactory origin = new TestFactory()
        {
            @Override
            public List<RuntimeConstraintRequest> getInputRuntimeConstraints()
            {
                return ImmutableList.of(new RuntimeConstraintRequest(ROOT, 0, EQUAL, false, DOUBLE));
            }
        };
        DriverFactory driver = new DriverFactory(0, true, true, ImmutableList.of(
                scan,
                observer(observed),
                projection(0, INTEGER, SMALLINT),
                observer(observed),
                projection(1, DOUBLE, INTEGER),
                observer(observed),
                origin), OptionalInt.empty());
        driver.initializeRuntimeConstraints(context);
        assertThat(observed).isEmpty();
        assertThat(scanFilter.get().isComplete()).isFalse();
        assertThat(context.getReport().subscriptionInputs()).containsExactly(new RuntimeConstraintSubscription.Input(ROOT, ROOT));
        assertThat(manager.getSnapshots()).containsOnlyKeys(ROOT);

        Domain source = Domain.create(ValueSet.ofRanges(Range.lessThan(DOUBLE, 17.5)), false);
        manager.applyUpdates(new RuntimeConstraintUpdateBatch(RuntimeConstraintProtocol.CURRENT_FORMAT_VERSION, 1, GENERATION, ImmutableList.of(snapshot(source))));

        assertThat(observed).containsExactly(
                source,
                Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(INTEGER, 17L)), false),
                Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(SMALLINT, 17L)), false));
        assertThat(scanFilter.get().getCurrentPredicate().getDomains().orElseThrow().get(column)).isEqualTo(observed.getLast());

        // The coordinator instantiates the graph emitted by the factories, including when parents arrive last.
        try (RuntimeConstraintSubscriptions coordinator = graph(Long.MAX_VALUE, new AtomicLong())) {
            coordinator.registerInput(new RuntimeConstraintSubscription.Input(ROOT, ROOT), CompletableFuture.completedFuture(snapshot(source)));
            context.getReport().subscriptions().reversed().forEach(coordinator::register);
            DynamicFilter coordinatorFilter = RuntimeConstraintDynamicFilter.create(coordinator, context.getReport().scans().getFirst().bindings(), ImmutableList.of(column));
            assertThat(coordinatorFilter.getCurrentPredicate()).isEqualTo(scanFilter.get().getCurrentPredicate());
        }
        assertThat(manager.getRetainedBytes()).isPositive();
        manager.close();
        assertThat(manager.getRetainedBytes()).isZero();
    }

    @Test
    void testRemoteEndpointContinuesTheTransformationChain()
    {
        CompletableFuture<RuntimeConstraintSnapshot> root = new CompletableFuture<>();
        try (RuntimeConstraintSubscriptions downstream = graph(Long.MAX_VALUE, new AtomicLong());
                RuntimeConstraintSubscriptions upstream = graph(Long.MAX_VALUE, new AtomicLong())) {
            RuntimeConstraintSubscription first = RuntimeConstraintSubscription.create(ROOT, ROOT, "downstream projection", RuntimeConstraintTransform.cast(INTEGER));
            downstream.registerInput(new RuntimeConstraintSubscription.Input(ROOT, ROOT), root);
            downstream.register(first);
            upstream.registerInput(new RuntimeConstraintSubscription.Input(first.id(), ROOT), downstream.waitForUpdate(first.id(), 0));
            RuntimeConstraintSubscription second = RuntimeConstraintSubscription.create(first.id(), ROOT, "upstream projection", RuntimeConstraintTransform.cast(SMALLINT));
            upstream.register(second);
            var result = upstream.waitForUpdate(second.id(), 0);
            assertThat(result).isNotDone();
            root.complete(snapshot(Domain.create(ValueSet.ofRanges(Range.lessThan(DOUBLE, 17.5)), false)));
            assertThat(domain(result.join())).isEqualTo(Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(SMALLINT, 17L)), false));
        }
    }

    @Test
    void testLateRegistrationFanOutAndDuplicateRegistration()
    {
        try (RuntimeConstraintSubscriptions graph = graph(Long.MAX_VALUE, new AtomicLong())) {
            graph.registerInput(new RuntimeConstraintSubscription.Input(ROOT, ROOT), CompletableFuture.completedFuture(snapshot(Domain.singleValue(DOUBLE, 11.0))));
            RuntimeConstraintSubscription first = RuntimeConstraintSubscription.create(ROOT, ROOT, "first", RuntimeConstraintTransform.cast(INTEGER));
            RuntimeConstraintSubscription second = RuntimeConstraintSubscription.create(ROOT, ROOT, "second", RuntimeConstraintTransform.cast(SMALLINT));
            graph.register(first);
            long retained = graph.retainedBytes();
            graph.register(first);
            assertThat(graph.retainedBytes()).isEqualTo(retained);
            graph.register(second);
            assertThat(domain(graph.waitForUpdate(first.id(), 0).join())).isEqualTo(Domain.singleValue(INTEGER, 11L));
            assertThat(domain(graph.waitForUpdate(second.id(), 0).join())).isEqualTo(Domain.singleValue(SMALLINT, 11L));
        }
    }

    @Test
    void testTerminalStatesPropagateWithoutTransformingValues()
    {
        for (RuntimeConstraintPublicationState state : List.of(DISABLED, CLOSED)) {
            try (RuntimeConstraintSubscriptions graph = graph(Long.MAX_VALUE, new AtomicLong())) {
                RuntimeConstraintSubscription child = RuntimeConstraintSubscription.create(ROOT, ROOT, "cast", RuntimeConstraintTransform.cast(INTEGER));
                graph.register(child);
                graph.registerInput(new RuntimeConstraintSubscription.Input(ROOT, ROOT), CompletableFuture.completedFuture(RuntimeConstraintSnapshot.terminal(ROOT, GENERATION, 1, state)));
                assertThat(graph.waitForUpdate(child.id(), 0).join().state()).isEqualTo(state);
                assertThat(graph.retainedBytes()).isZero();
            }
        }
    }

    @Test
    void testMemoryLimitDisablesOnlyTheAffectedPath()
    {
        try (RuntimeConstraintSubscriptions graph = graph(0, new AtomicLong())) {
            graph.registerInput(new RuntimeConstraintSubscription.Input(ROOT, ROOT), CompletableFuture.completedFuture(snapshot(Domain.singleValue(DOUBLE, 11.0))));
            RuntimeConstraintSubscription cast = RuntimeConstraintSubscription.create(ROOT, ROOT, "cast", RuntimeConstraintTransform.cast(INTEGER));
            RuntimeConstraintSubscription identity = RuntimeConstraintSubscription.create(ROOT, ROOT, "identity", IDENTITY);
            graph.register(cast);
            graph.register(identity);
            assertThat(graph.waitForUpdate(cast.id(), 0).join().state()).isEqualTo(DISABLED);
            assertThat(graph.waitForUpdate(identity.id(), 0).join().state()).isEqualTo(FINAL);
            assertThat(graph.retainedBytes()).isZero();
        }
    }

    @Test
    void testClosureReleasesMemoryAndPendingSubscribers()
    {
        AtomicLong memory = new AtomicLong();
        RuntimeConstraintSubscriptions graph = graph(Long.MAX_VALUE, memory);
        RuntimeConstraintSubscription child = RuntimeConstraintSubscription.create(ROOT, ROOT, "cast", RuntimeConstraintTransform.cast(INTEGER));
        graph.registerInput(new RuntimeConstraintSubscription.Input(ROOT, ROOT), CompletableFuture.completedFuture(snapshot(Domain.singleValue(DOUBLE, 11.0))));
        graph.register(child);
        assertThat(memory.get()).isPositive();
        var pending = graph.waitForUpdate(new RuntimeConstraintId("missing"), 0);
        graph.close();
        assertThat(memory.get()).isZero();
        assertThat(pending.join().state()).isEqualTo(CLOSED);
        assertThat(graph.waitForUpdate(child.id(), 0).join().state()).isEqualTo(CLOSED);
        graph.close();
        assertThat(memory.get()).isZero();
    }

    @Test
    void testRepeatedInputRegistrationDoesNotResubscribe()
    {
        try (RuntimeConstraintSubscriptions graph = graph(Long.MAX_VALUE, new AtomicLong())) {
            AtomicLong registrations = new AtomicLong();
            CompletableFuture<RuntimeConstraintSnapshot> pending = new CompletableFuture<>();
            for (int index = 0; index < 10; index++) {
                graph.registerInput(new RuntimeConstraintSubscription.Input(ROOT, ROOT), () -> {
                    registrations.incrementAndGet();
                    return pending;
                });
            }
            assertThat(registrations.get()).isEqualTo(1);
            pending.complete(snapshot(Domain.singleValue(BIGINT, 11L)));
            assertThat(domain(graph.waitForUpdate(ROOT, 0).join())).isEqualTo(Domain.singleValue(BIGINT, 11L));
        }
    }

    @Test
    void testInvalidInputFailsItsSubscribersWithoutStallingOtherInputs()
    {
        try (RuntimeConstraintSubscriptions graph = graph(Long.MAX_VALUE, new AtomicLong())) {
            RuntimeConstraintId invalid = new RuntimeConstraintId("invalid");
            RuntimeConstraintSubscription child = RuntimeConstraintSubscription.create(invalid, invalid, "identity", IDENTITY);
            graph.register(child);
            graph.registerInput(new RuntimeConstraintSubscription.Input(invalid, invalid), CompletableFuture.completedFuture(snapshot(Domain.singleValue(BIGINT, 11L))));
            assertThat(graph.waitForUpdate(child.id(), 0)).isCompletedExceptionally();

            graph.registerInput(new RuntimeConstraintSubscription.Input(ROOT, ROOT), CompletableFuture.completedFuture(snapshot(Domain.singleValue(BIGINT, 12L))));
            assertThat(domain(graph.waitForUpdate(ROOT, 0).join())).isEqualTo(Domain.singleValue(BIGINT, 12L));
        }
    }

    @Test
    void testOldGenerationCannotPublish()
    {
        try (RuntimeConstraintSubscriptions graph = graph(Long.MAX_VALUE, new AtomicLong())) {
            RuntimeConstraintSubscription child = RuntimeConstraintSubscription.create(ROOT, ROOT, "identity", IDENTITY);
            graph.register(child);
            graph.registerInput(new RuntimeConstraintSubscription.Input(ROOT, ROOT), CompletableFuture.completedFuture(snapshot(Domain.singleValue(BIGINT, 11L)).withGeneration(GENERATION - 1)));
            assertThat(graph.waitForUpdate(child.id(), 0)).isNotDone();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testDeepGraphDrainsWithoutRecursiveCallbacks(boolean reverseRegistration)
    {
        try (RuntimeConstraintSubscriptions graph = graph(Long.MAX_VALUE, new AtomicLong())) {
            List<RuntimeConstraintSubscription> chain = new ArrayList<>();
            RuntimeConstraintId previous = ROOT;
            for (int index = 0; index < 2_000; index++) {
                RuntimeConstraintSubscription node = RuntimeConstraintSubscription.create(previous, ROOT, "operator " + index, IDENTITY);
                chain.add(node);
                previous = node.id();
            }
            (reverseRegistration ? chain.reversed() : chain).forEach(graph::register);
            var result = graph.waitForUpdate(previous, 0);
            graph.registerInput(new RuntimeConstraintSubscription.Input(ROOT, ROOT), CompletableFuture.completedFuture(snapshot(Domain.singleValue(BIGINT, 11L))));
            assertThat(domain(result.join())).isEqualTo(Domain.singleValue(BIGINT, 11L));
        }
    }

    @Test
    void testRejectsCyclesAndConflictingDefinitions()
    {
        try (RuntimeConstraintSubscriptions graph = graph(Long.MAX_VALUE, new AtomicLong())) {
            RuntimeConstraintId first = new RuntimeConstraintId("first");
            RuntimeConstraintId second = new RuntimeConstraintId("second");
            graph.register(new RuntimeConstraintSubscription(first, second, ROOT, "first", IDENTITY));
            assertThatThrownBy(() -> graph.register(new RuntimeConstraintSubscription(second, first, ROOT, "second", IDENTITY)))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("cycle");
            assertThatThrownBy(() -> graph.register(new RuntimeConstraintSubscription(first, ROOT, ROOT, "different", IDENTITY)))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("conflicting");
        }
    }

    @Test
    void testMalformedEndpointFailsAndUnblocksAdapter()
    {
        try (RuntimeConstraintSubscriptions graph = graph(Long.MAX_VALUE, new AtomicLong())) {
            CompletableFuture<RuntimeConstraintSnapshot> input = new CompletableFuture<>();
            graph.registerInput(new RuntimeConstraintSubscription.Input(ROOT, ROOT), input);
            TestingColumnHandle column = new TestingColumnHandle("key");
            DynamicFilter filter = RuntimeConstraintDynamicFilter.create(
                    graph,
                    ImmutableList.of(new RuntimeConstraintWiringReport.Binding(ROOT, column)),
                    ImmutableList.of(column));
            CompletableFuture<?> blocked = filter.isBlocked();
            input.complete(RuntimeConstraintSnapshot.finalSnapshot(
                    ROOT,
                    GENERATION,
                    1,
                    new RuntimeMembershipPayload(ImmutableList.of(Domain.singleValue(BIGINT, 1L), Domain.singleValue(BIGINT, 2L)), ORDINARY)));
            assertThat(blocked).isCompletedExceptionally();
            assertThat(filter.isBlocked()).isCompletedExceptionally();
            assertThatThrownBy(filter::getCurrentPredicate).hasRootCauseMessage("runtime constraint payload has wrong lane count");
        }
    }

    @Test
    void testManagerCloseFromSubscriptionCallback()
    {
        var memory = newSimpleAggregatedMemoryContext().newLocalMemoryContext("test");
        TaskRuntimeConstraintManager manager = new TaskRuntimeConstraintManager(new TaskId(new StageId(new QueryId("test"), 0), 0, 0), GENERATION, memory, () -> {});
        RuntimeConstraintSubscriptions graph = manager.createSubscriptions(null, Long.MAX_VALUE);
        CompletableFuture<Void> closing = graph.waitForUpdate(ROOT, 0).thenRun(manager::close);
        manager.taskFinished();
        assertThat(closing).isCompleted();
        assertThat(manager.getRetainedBytes()).isZero();
        assertThatThrownBy(() -> memory.setBytes(1)).hasMessageContaining("already closed");
    }

    @Test
    void testManagerCompletionRacesWithClose()
            throws Exception
    {
        try (var executor = newFixedThreadPool(1)) {
            for (int graphs : List.of(1, 3)) {
                var memory = newSimpleAggregatedMemoryContext().newLocalMemoryContext("test");
                TaskRuntimeConstraintManager manager = new TaskRuntimeConstraintManager(new TaskId(new StageId(new QueryId("test"), 0), 0, 0), GENERATION, memory, () -> {});
                RuntimeConstraintSubscriptions first = manager.createSubscriptions(null, Long.MAX_VALUE);
                CountDownLatch closing = new CountDownLatch(1);
                CountDownLatch resume = new CountDownLatch(1);
                first.waitForUpdate(ROOT, 0).thenRun(() -> {
                    closing.countDown();
                    await(resume);
                });
                List<CompletableFuture<RuntimeConstraintSnapshot>> pending = new ArrayList<>();
                for (int index = 1; index < graphs; index++) {
                    RuntimeConstraintSubscriptions graph = manager.createSubscriptions(null, Long.MAX_VALUE);
                    graph.registerInput(new RuntimeConstraintSubscription.Input(ROOT, ROOT), CompletableFuture.completedFuture(snapshot(Domain.singleValue(BIGINT, 17L))));
                    graph.register(RuntimeConstraintSubscription.create(ROOT, ROOT, "comparison", RuntimeConstraintTransform.comparison(LESS_THAN, false)));
                    pending.add(graph.waitForUpdate(new RuntimeConstraintId("pending"), 0));
                }
                var finishing = executor.submit(manager::taskFinished);
                try {
                    assertThat(closing.await(5, SECONDS)).isTrue();
                    manager.close();
                }
                finally {
                    resume.countDown();
                }
                finishing.get(5, SECONDS);
                for (CompletableFuture<RuntimeConstraintSnapshot> waiter : pending) {
                    assertThat(waiter.get(5, SECONDS).state()).isEqualTo(CLOSED);
                }
                assertThat(manager.getRetainedBytes()).isZero();
                assertThatThrownBy(() -> memory.setBytes(1)).hasMessageContaining("already closed");
                assertThat(manager.createSubscriptions(null, Long.MAX_VALUE).waitForUpdate(ROOT, 0).join().state()).isEqualTo(CLOSED);
            }
        }
    }

    private static void await(CountDownLatch latch)
    {
        try {
            assertThat(latch.await(5, SECONDS)).isTrue();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private RuntimeConstraintSubscriptions graph(long limit, AtomicLong memory)
    {
        var planner = getPlanTester().getPlannerContext();
        return new RuntimeConstraintSubscriptions(
                GENERATION,
                limit,
                new RuntimeConstraintTransform.Context(planner.getMetadata(), planner.getFunctionManager(), planner.getTypeOperators(), getPlanTester().getDefaultSession()),
                bytes -> {
                    memory.addAndGet(bytes);
                    return true;
                },
                null);
    }

    private static RuntimeConstraintSnapshot snapshot(Domain domain)
    {
        return RuntimeConstraintSnapshot.finalSnapshot(ROOT, GENERATION, 1, new RuntimeMembershipPayload(ImmutableList.of(domain), ORDINARY));
    }

    private static Domain domain(RuntimeConstraintSnapshot snapshot)
    {
        return ((RuntimeMembershipPayload) snapshot.payload().orElseThrow()).lanes().getFirst().domain();
    }

    private static OperatorFactory projection(int id, Type output, Type input)
    {
        return FilterAndProjectOperator.createOperatorFactory(
                id,
                new PlanNodeId("projection" + id),
                () -> new PageProcessor(Optional.empty(), ImmutableList.of()),
                ImmutableList.of(output),
                ImmutableList.of(OptionalInt.of(0)),
                ImmutableList.of(Optional.of(input)),
                ImmutableList.of(),
                DataSize.ofBytes(0),
                0);
    }

    private static OperatorFactory observer(List<Domain> values)
    {
        return new TestFactory()
        {
            @Override
            public void propagateRuntimeConstraint(RuntimeConstraintRequest request, Consumer<RuntimeConstraintRequest> input, RuntimeConstraintWiringContext context)
            {
                context.subscribe(request).thenAccept(snapshot -> values.add(domain(snapshot)));
                input.accept(request);
            }
        };
    }

    private abstract static class TestFactory
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
