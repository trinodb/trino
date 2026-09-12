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
import com.google.common.collect.ImmutableMap;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.RuntimeConstraintRequest;
import io.trino.operator.TaskRuntimeConstraintManager;
import io.trino.spi.QueryId;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintWiringReport.CollectedConstraint;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.planner.DomainCoercer.applySaturatedCasts;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintNullMatchMode.NULL_SAFE;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintNullMatchMode.ORDINARY;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintPublicationState.DISABLED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestRuntimeConstraintDynamicFilter
        extends BasePlanTest
{
    private static final long GENERATION = 9;
    private static final RuntimeConstraintId ID = new RuntimeConstraintId("constraint");
    private static final TestingColumnHandle COLUMN = new TestingColumnHandle("key");
    private static final TestingColumnHandle SECOND_COLUMN = new TestingColumnHandle("second_key");

    @Test
    public void testFailureRemainsVisibleAfterAnotherSubscriptionCompletes()
    {
        RuntimeConstraintId second = new RuntimeConstraintId("second");
        try (RuntimeConstraintSubscriptions graph = new RuntimeConstraintSubscriptions(GENERATION, Long.MAX_VALUE, null, _ -> true, null)) {
            CompletableFuture<RuntimeConstraintSnapshot> firstUpdate = new CompletableFuture<>();
            CompletableFuture<RuntimeConstraintSnapshot> secondUpdate = new CompletableFuture<>();
            graph.registerInput(new RuntimeConstraintSubscription.Input(ID, ID), firstUpdate);
            graph.registerInput(new RuntimeConstraintSubscription.Input(second, second), secondUpdate);
            DynamicFilter filter = RuntimeConstraintDynamicFilter.create(
                    graph,
                    ImmutableList.of(
                            new RuntimeConstraintWiringReport.Binding(new RuntimeConstraintRequest(ID, 0).withSubscription(ID), COLUMN),
                            new RuntimeConstraintWiringReport.Binding(new RuntimeConstraintRequest(second, 0).withSubscription(second), SECOND_COLUMN)),
                    ImmutableList.of(COLUMN, SECOND_COLUMN));
            CompletableFuture<?> blocked = filter.isBlocked();
            firstUpdate.completeExceptionally(new IllegalArgumentException("invalid update"));
            assertThat(blocked).isCompletedExceptionally();

            secondUpdate.complete(RuntimeConstraintSnapshot.terminal(second, GENERATION, 1, DISABLED));
            assertThat(filter.isBlocked()).isCompletedExceptionally();
            assertThatThrownBy(filter::getCurrentPredicate).hasRootCauseMessage("invalid update");
        }
    }

    @Test
    public void testAwaitableFilterPublishesFinalPredicate()
    {
        TaskRuntimeConstraintManager manager = manager();
        DynamicFilter filter = createFilter(manager, bindings(COLUMN), ImmutableList.of(COLUMN));
        var blocked = filter.isBlocked();

        assertThat(filter.getColumnsCovered()).containsExactly(COLUMN);
        assertThat(filter.isAwaitable()).isTrue();
        assertThat(filter.isComplete()).isFalse();
        assertThat(blocked).isNotDone();
        assertThat(filter.getCurrentPredicate()).isEqualTo(TupleDomain.all());

        Domain domain = Domain.singleValue(BIGINT, 17L);
        manager.applyUpdates(new RuntimeConstraintUpdateBatch(
                RuntimeConstraintProtocol.CURRENT_FORMAT_VERSION,
                1,
                GENERATION,
                ImmutableList.of(RuntimeConstraintSnapshot.finalSnapshot(
                        ID,
                        GENERATION,
                        1,
                        new RuntimeMembershipPayload(ImmutableList.of(domain), ORDINARY)))));

        assertThat(blocked).isDone();
        assertThat(filter.isAwaitable()).isFalse();
        assertThat(filter.isComplete()).isTrue();
        assertThat(filter.getCurrentPredicate()).isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(COLUMN, domain)));
    }

    @Test
    public void testDisabledConstraintCompletes()
    {
        TaskRuntimeConstraintManager manager = manager();
        DynamicFilter filter = createFilter(manager, bindings(COLUMN), ImmutableList.of(COLUMN));

        assertThat(filter.isAwaitable()).isTrue();
        assertThat(filter.isBlocked()).isNotDone();
        assertThat(filter.isComplete()).isFalse();

        manager.applyUpdates(new RuntimeConstraintUpdateBatch(
                RuntimeConstraintProtocol.CURRENT_FORMAT_VERSION,
                1,
                GENERATION,
                ImmutableList.of(RuntimeConstraintSnapshot.terminal(ID, GENERATION, 1, DISABLED))));

        assertThat(filter.isComplete()).isTrue();
        assertThat(filter.getCurrentPredicate()).isEqualTo(TupleDomain.all());
    }

    @Test
    public void testConstraintAppliesToEveryBoundScanColumn()
    {
        TaskRuntimeConstraintManager manager = manager();
        DynamicFilter filter = createFilter(manager, bindings(COLUMN, SECOND_COLUMN), ImmutableList.of(COLUMN, SECOND_COLUMN));

        Domain domain = Domain.singleValue(BIGINT, 23L);
        manager.applyUpdates(new RuntimeConstraintUpdateBatch(
                RuntimeConstraintProtocol.CURRENT_FORMAT_VERSION,
                1,
                GENERATION,
                ImmutableList.of(RuntimeConstraintSnapshot.finalSnapshot(
                        ID,
                        GENERATION,
                        1,
                        new RuntimeMembershipPayload(ImmutableList.of(domain), ORDINARY)))));

        assertThat(filter.getCurrentPredicate()).isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(
                COLUMN, domain,
                SECOND_COLUMN, domain)));
    }

    @Test
    public void testBlockedFutureIgnoresCancellationAndCompletesWhenTaskCloses()
    {
        TaskRuntimeConstraintManager manager = manager();
        DynamicFilter filter = createFilter(manager, bindings(COLUMN), ImmutableList.of(COLUMN));
        CompletableFuture<?> blocked = filter.isBlocked();

        assertThat(blocked.cancel(false)).isFalse();
        assertThat(blocked).isNotDone();
        assertThat(filter.isAwaitable()).isTrue();

        manager.taskFinished();

        assertThat(blocked).isDone();
        assertThat(filter.isAwaitable()).isFalse();
        assertThat(filter.isComplete()).isTrue();
        assertThat(filter.getCurrentPredicate()).isEqualTo(TupleDomain.all());
    }

    @Test
    public void testEmptyNullSafeBuildUsesConnectorCompatibilityProjection()
    {
        TaskRuntimeConstraintManager manager = manager();
        DynamicFilter filter = createFilter(manager, bindings(COLUMN), ImmutableList.of(COLUMN));

        manager.applyUpdates(new RuntimeConstraintUpdateBatch(
                RuntimeConstraintProtocol.CURRENT_FORMAT_VERSION,
                1,
                GENERATION,
                ImmutableList.of(RuntimeConstraintSnapshot.finalSnapshot(
                        ID,
                        GENERATION,
                        1,
                        new RuntimeMembershipPayload(
                                ImmutableList.of(new RuntimeMembershipPayload.Lane(Domain.none(BIGINT), false)),
                                NULL_SAFE,
                                false)))));

        assertThat(filter.getCurrentPredicate())
                .isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(COLUMN, Domain.onlyNull(BIGINT))));
    }

    @Test
    public void testAppliesSaturatedFloorCastPreimage()
    {
        Domain sourceDomain = Domain.create(ValueSet.ofRanges(Range.lessThan(DOUBLE, 17.5)), false);
        Domain expectedDomain = Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(INTEGER, 17L)), false);
        assertThat(applySaturatedCasts(
                getPlanTester().getPlannerContext().getMetadata(),
                getPlanTester().getPlannerContext().getFunctionManager(),
                getPlanTester().getPlannerContext().getTypeOperators(),
                getPlanTester().getDefaultSession(),
                sourceDomain,
                INTEGER))
                .isEqualTo(expectedDomain);

        TaskRuntimeConstraintManager manager = manager();
        DynamicFilter filter = createFilter(manager, bindings(COLUMN, INTEGER), ImmutableList.of(COLUMN));

        manager.applyUpdates(new RuntimeConstraintUpdateBatch(
                RuntimeConstraintProtocol.CURRENT_FORMAT_VERSION,
                1,
                GENERATION,
                ImmutableList.of(RuntimeConstraintSnapshot.finalSnapshot(
                        ID,
                        GENERATION,
                        1,
                        new RuntimeMembershipPayload(
                                ImmutableList.of(sourceDomain),
                                ORDINARY)))));

        assertThat(filter.getCurrentPredicate())
                .isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(
                        COLUMN, expectedDomain)));
    }

    @Test
    public void testFilterCreatedAfterInitialUnblockRemainsSubscribed()
    {
        RuntimeConstraintHub hub = new RuntimeConstraintHub(0, Long.MAX_VALUE);
        PlanNodeId sourceId = new PlanNodeId("source");
        RuntimeConstraintId first = new RuntimeConstraintId("first");
        RuntimeConstraintId second = new RuntimeConstraintId("second");
        hub.registerSource(
                new TaskId(new StageId(new QueryId("query"), 1), 0, 0),
                new RuntimeConstraintWiringReport.Source(
                        sourceId,
                        ImmutableList.of(new CollectedConstraint(first, 0), new CollectedConstraint(second, 1)),
                        ImmutableList.of(BIGINT, BIGINT),
                        DistributedCompletionPolicy.UNION_ALL_PARTITIONS));
        ImmutableList<ColumnHandle> columns = ImmutableList.of(COLUMN, SECOND_COLUMN);
        ImmutableList<RuntimeConstraintWiringReport.Binding> bindings = ImmutableList.of(
                new RuntimeConstraintWiringReport.Binding(first, COLUMN),
                new RuntimeConstraintWiringReport.Binding(second, SECOND_COLUMN));
        hub.registerConsumers(bindings);
        hub.unblockStageDynamicFilters(new PlanFragmentId("1"));

        RuntimeConstraintSubscriptions graph = new RuntimeConstraintSubscriptions(hub.getGeneration(), Long.MAX_VALUE, transformationContext(), _ -> true, hub::waitForInitialUnblock);
        bindings.forEach(binding -> graph.registerInput(new RuntimeConstraintSubscription.Input(binding.constraintId(), binding.constraintId()), hub.waitForUpdate(binding.constraintId(), 0)));
        DynamicFilter filter = RuntimeConstraintDynamicFilter.create(graph, bindings.stream().map(binding -> graph.registerBinding("test", binding)).toList(), columns);

        assertThat(filter.isBlocked()).isDone();
        assertThat(filter.isComplete()).isFalse();

        Domain firstDomain = Domain.singleValue(BIGINT, 11L);
        Domain secondDomain = Domain.singleValue(BIGINT, 22L);
        hub.publishFinal(first, 0, new RuntimeMembershipPayload(ImmutableList.of(firstDomain), ORDINARY));
        hub.publishFinal(second, 0, new RuntimeMembershipPayload(ImmutableList.of(secondDomain), ORDINARY));

        assertThat(filter.isComplete()).isTrue();
        assertThat(filter.getCurrentPredicate()).isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(
                COLUMN, firstDomain,
                SECOND_COLUMN, secondDomain)));
    }

    private DynamicFilter createFilter(TaskRuntimeConstraintManager manager, List<RuntimeConstraintWiringReport.Binding> bindings, List<ColumnHandle> columns)
    {
        RuntimeConstraintSubscriptions graph = manager.createSubscriptions(transformationContext(), Long.MAX_VALUE);
        manager.registerConsumers(bindings);
        bindings.forEach(binding -> graph.registerInput(new RuntimeConstraintSubscription.Input(binding.constraintId(), binding.constraintId()), manager.waitForUpdate(binding.constraintId(), 0)));
        return RuntimeConstraintDynamicFilter.create(graph, bindings.stream().map(binding -> graph.registerBinding("test", binding)).toList(), columns);
    }

    private RuntimeConstraintTransform.Context transformationContext()
    {
        var plannerContext = getPlanTester().getPlannerContext();
        return new RuntimeConstraintTransform.Context(plannerContext.getMetadata(), plannerContext.getFunctionManager(), plannerContext.getTypeOperators(), getPlanTester().getDefaultSession());
    }

    private static TaskRuntimeConstraintManager manager()
    {
        LocalMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext("test");
        return new TaskRuntimeConstraintManager(
                new TaskId(new StageId(new QueryId("query"), 1), 0, 0),
                GENERATION,
                memoryContext,
                () -> {});
    }

    private static ImmutableList<RuntimeConstraintWiringReport.Binding> bindings(TestingColumnHandle... columns)
    {
        return Arrays.stream(columns)
                .map(column -> new RuntimeConstraintWiringReport.Binding(new RuntimeConstraintRequest(ID, 0), column))
                .collect(toImmutableList());
    }

    private static ImmutableList<RuntimeConstraintWiringReport.Binding> bindings(TestingColumnHandle column, io.trino.spi.type.Type targetType)
    {
        return ImmutableList.of(new RuntimeConstraintWiringReport.Binding(
                new RuntimeConstraintRequest(ID, 0).withChannelAndTargetType(0, targetType),
                column));
    }

    private record TestingColumnHandle(String name)
            implements ColumnHandle {}
}
