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
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.memory.context.MemoryReservationHandler;
import io.trino.spi.QueryId;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.runtimeconstraint.ProducerBindingId;
import io.trino.sql.planner.runtimeconstraint.ProducerGroupId;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintHub;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintId;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintProtocol;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintPublicationState;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintSnapshot;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintUpdateBatch;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintWiringReport;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintWiringReport.CollectedConstraint;
import io.trino.sql.planner.runtimeconstraint.RuntimeMembershipPayload;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.memory.context.AggregatedMemoryContext.newRootAggregatedMemoryContext;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.runtimeconstraint.DistributedCompletionPolicy.EQUIVALENT_REPLICAS;
import static io.trino.sql.planner.runtimeconstraint.DistributedCompletionPolicy.UNION_ALL_PARTITIONS;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintNullMatchMode.ORDINARY;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintPublicationState.CLOSED;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintPublicationState.DISABLED;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintPublicationState.FINAL;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTaskRuntimeConstraintManager
{
    private static final long GENERATION = 7;
    private static final PlanNodeId JOIN_ID = new PlanNodeId("join");
    private static final ProducerGroupId GROUP_ID = RuntimeConstraintHub.dynamicGroupId(JOIN_ID);
    private static final ProducerBindingId BINDING_ID = RuntimeConstraintHub.dynamicBindingId(JOIN_ID);
    private static final RuntimeConstraintId CONSTRAINT_ID = new RuntimeConstraintId("constraint");

    @Test
    public void testAcknowledgedContributionsAreRetainedAndReleased()
    {
        LocalMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext("test");
        AtomicInteger notifications = new AtomicInteger();
        TaskRuntimeConstraintManager manager = manager(false, memoryContext, notifications::incrementAndGet);
        RuntimeMembershipPayload payload = new RuntimeMembershipPayload(ImmutableList.of(singleValue(BIGINT, 11L)), ORDINARY);

        assertThat(manager.addContribution(BINDING_ID, payload)).isTrue();
        assertThat(manager.addContribution(BINDING_ID, payload)).isFalse();
        assertThat(notifications).hasValue(1);
        assertThat(manager.getContributionSequence()).isEqualTo(1);
        assertThat(manager.getRetainedBytes()).isEqualTo(payload.getRetainedSizeInBytes());

        var batch = manager.acknowledgeContributionsAndGetBatch(0);
        assertThat(batch.sequence()).isEqualTo(1);
        assertThat(batch.contributions()).singleElement().satisfies(contribution -> {
            assertThat(contribution.groupId()).isEqualTo(GROUP_ID);
            assertThat(contribution.bindingId()).isEqualTo(BINDING_ID);
            assertThat(contribution.logicalPartitionId()).isEqualTo(3);
            assertThat(contribution.taskAttemptId()).isEqualTo(2);
        });
        assertThat(manager.acknowledgeContributionsAndGetBatch(0)).isEqualTo(batch);

        assertThat(manager.acknowledgeContributionsAndGetBatch(1)).satisfies(acknowledgement -> {
            assertThat(acknowledgement.sequence()).isEqualTo(1);
            assertThat(acknowledgement.contributions()).isEmpty();
        });
        assertThat(manager.getRetainedBytes()).isZero();
        assertThat(memoryContext.getBytes()).isZero();
    }

    @Test
    public void testContributionResponsesAreBatched()
    {
        LocalMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext("test");
        TaskRuntimeConstraintManager manager = manager(129, memoryContext);
        RuntimeMembershipPayload payload = new RuntimeMembershipPayload(ImmutableList.of(singleValue(BIGINT, 11L)), ORDINARY);
        IntStream.range(0, 129)
                .forEach(index -> assertThat(manager.addContribution(new PlanNodeId("join_" + index), payload)).isTrue());

        var first = manager.acknowledgeContributionsAndGetBatch(0);
        assertThat(first.contributions()).hasSize(128);
        assertThat(first.sequence()).isEqualTo(128);

        var second = manager.acknowledgeContributionsAndGetBatch(first.sequence());
        assertThat(second.contributions()).hasSize(1);
        assertThat(second.sequence()).isEqualTo(129);

        assertThat(manager.acknowledgeContributionsAndGetBatch(second.sequence())).satisfies(acknowledgement -> {
            assertThat(acknowledgement.sequence()).isEqualTo(129);
            assertThat(acknowledgement.contributions()).isEmpty();
        });
        assertThat(memoryContext.getBytes()).isZero();
    }

    @Test
    public void testAcknowledgementAfterCloseDoesNotTouchReleasedMemory()
    {
        LocalMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext("test");
        TaskRuntimeConstraintManager manager = manager(false, memoryContext, () -> {});
        RuntimeMembershipPayload payload = new RuntimeMembershipPayload(ImmutableList.of(singleValue(BIGINT, 11L)), ORDINARY);
        assertThat(manager.addContribution(BINDING_ID, payload)).isTrue();

        manager.close();

        assertThat(manager.acknowledgeContributionsAndGetBatch(1)).satisfies(acknowledgement -> {
            assertThat(acknowledgement.sequence()).isEqualTo(1);
            assertThat(acknowledgement.contributions()).isEmpty();
        });
        manager.acknowledgeContributions(1);
        assertThat(memoryContext.getBytes()).isZero();
    }

    @Test
    public void testAppliesUpdatesAndCompletesWaiters()
    {
        LocalMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext("test");
        AtomicInteger notifications = new AtomicInteger();
        TaskRuntimeConstraintManager manager = manager(false, memoryContext, notifications::incrementAndGet);
        CompletableFuture<RuntimeConstraintSnapshot> waiter = manager.waitForUpdate(CONSTRAINT_ID, 0);
        RuntimeMembershipPayload payload = new RuntimeMembershipPayload(ImmutableList.of(singleValue(BIGINT, 13L)), ORDINARY);
        RuntimeConstraintSnapshot snapshot = RuntimeConstraintSnapshot.finalSnapshot(CONSTRAINT_ID, GENERATION, 1, payload);

        assertThat(manager.applyUpdates(new RuntimeConstraintUpdateBatch(RuntimeConstraintProtocol.CURRENT_FORMAT_VERSION, 4, GENERATION, ImmutableList.of(snapshot)))).isTrue();
        assertThat(manager.getUpdateAcknowledgement()).isEqualTo(4);
        assertThat(manager.getSnapshot(CONSTRAINT_ID).orElseThrow().state()).isEqualTo(FINAL);
        assertThat(waiter).isCompletedWithValue(snapshot);
        assertThat(notifications).hasValue(1);
        assertThat(memoryContext.getBytes()).isEqualTo(payload.getRetainedSizeInBytes());

        assertThat(manager.applyUpdates(new RuntimeConstraintUpdateBatch(RuntimeConstraintProtocol.CURRENT_FORMAT_VERSION, 4, GENERATION, ImmutableList.of(snapshot)))).isFalse();
        assertThat(manager.applyUpdates(new RuntimeConstraintUpdateBatch(RuntimeConstraintProtocol.CURRENT_FORMAT_VERSION, 3, GENERATION, ImmutableList.of(snapshot)))).isFalse();
        assertThat(manager.getUpdateAcknowledgement()).isEqualTo(4);
        assertThat(manager.applyUpdates(new RuntimeConstraintUpdateBatch(RuntimeConstraintProtocol.CURRENT_FORMAT_VERSION, 5, GENERATION + 1, ImmutableList.of(
                RuntimeConstraintSnapshot.finalSnapshot(CONSTRAINT_ID, GENERATION + 1, 1, payload))))).isFalse();

        manager.close();
        assertThat(manager.getSnapshot(CONSTRAINT_ID).orElseThrow().state()).isEqualTo(CLOSED);
        assertThat(memoryContext.getBytes()).isZero();
    }

    @Test
    public void testEquivalentReplicaPublishesToLocalConsumer()
    {
        LocalMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext("test");
        TaskRuntimeConstraintManager manager = manager(true, memoryContext, () -> {});
        CompletableFuture<RuntimeConstraintSnapshot> waiter = manager.waitForUpdate(CONSTRAINT_ID, 0);
        RuntimeMembershipPayload payload = new RuntimeMembershipPayload(ImmutableList.of(singleValue(BIGINT, 17L)), ORDINARY);

        assertThat(manager.addContribution(BINDING_ID, payload)).isTrue();
        assertThat(waiter).isCompleted();
        assertThat(manager.getSnapshot(CONSTRAINT_ID).orElseThrow())
                .satisfies(snapshot -> {
                    assertThat(snapshot.state()).isEqualTo(FINAL);
                    assertThat(snapshot.payload()).contains(payload);
                });
    }

    @Test
    public void testPartitionedGroupWaitsForCoordinator()
    {
        LocalMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext("test");
        TaskRuntimeConstraintManager manager = manager(false, memoryContext, () -> {});
        CompletableFuture<RuntimeConstraintSnapshot> waiter = manager.waitForUpdate(CONSTRAINT_ID, 0);

        assertThat(manager.addContribution(BINDING_ID, new RuntimeMembershipPayload(ImmutableList.of(singleValue(BIGINT, 19L)), ORDINARY))).isTrue();
        assertThat(waiter).isNotDone();
        assertThat(manager.getSnapshot(CONSTRAINT_ID).orElseThrow().state()).isEqualTo(RuntimeConstraintPublicationState.PENDING);
    }

    @Test
    public void testMatchingCoordinatorFinalAfterLocalPublicationIsIdempotent()
    {
        LocalMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext("test");
        TaskRuntimeConstraintManager manager = manager(true, memoryContext, () -> {});
        RuntimeMembershipPayload payload = new RuntimeMembershipPayload(ImmutableList.of(singleValue(BIGINT, 23L)), ORDINARY);
        manager.addContribution(BINDING_ID, payload);

        RuntimeConstraintSnapshot snapshot = RuntimeConstraintSnapshot.finalSnapshot(CONSTRAINT_ID, GENERATION, 1, payload);
        assertThat(manager.applyUpdates(new RuntimeConstraintUpdateBatch(RuntimeConstraintProtocol.CURRENT_FORMAT_VERSION, 1, GENERATION, ImmutableList.of(snapshot)))).isFalse();
        assertThat(manager.getUpdateAcknowledgement()).isEqualTo(1);
        assertThat(manager.getSnapshot(CONSTRAINT_ID)).contains(snapshot);
    }

    @Test
    public void testCoordinatorFinalBeforeLocalPublicationIsIdempotent()
    {
        LocalMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext("test");
        TaskRuntimeConstraintManager manager = manager(true, memoryContext, () -> {});
        RuntimeMembershipPayload payload = new RuntimeMembershipPayload(ImmutableList.of(singleValue(BIGINT, 23L)), ORDINARY);
        RuntimeConstraintSnapshot snapshot = RuntimeConstraintSnapshot.finalSnapshot(CONSTRAINT_ID, GENERATION, 1, payload);
        assertThat(manager.applyUpdates(new RuntimeConstraintUpdateBatch(RuntimeConstraintProtocol.CURRENT_FORMAT_VERSION, 1, GENERATION, ImmutableList.of(snapshot)))).isTrue();

        assertThat(manager.addContribution(BINDING_ID, payload)).isTrue();
        assertThat(manager.getSnapshot(CONSTRAINT_ID)).contains(snapshot);
    }

    @Test
    public void testCoordinatorDisableDoesNotWidenLocalFinal()
    {
        LocalMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext("test");
        TaskRuntimeConstraintManager manager = manager(true, memoryContext, () -> {});
        RuntimeMembershipPayload payload = new RuntimeMembershipPayload(ImmutableList.of(singleValue(BIGINT, 29L)), ORDINARY);
        manager.addContribution(BINDING_ID, payload);

        RuntimeConstraintSnapshot disabled = RuntimeConstraintSnapshot.terminal(CONSTRAINT_ID, GENERATION, 1, DISABLED);
        assertThat(manager.applyUpdates(new RuntimeConstraintUpdateBatch(RuntimeConstraintProtocol.CURRENT_FORMAT_VERSION, 1, GENERATION, ImmutableList.of(disabled)))).isFalse();
        assertThat(manager.getUpdateAcknowledgement()).isEqualTo(1);
        assertThat(manager.getSnapshot(CONSTRAINT_ID).orElseThrow().payload()).contains(payload);
    }

    @Test
    public void testCoordinatorFinalDoesNotReplaceEquivalentLocalFinal()
    {
        LocalMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext("test");
        TaskRuntimeConstraintManager manager = manager(true, memoryContext, () -> {});
        RuntimeMembershipPayload localPayload = new RuntimeMembershipPayload(ImmutableList.of(singleValue(BIGINT, 29L)), ORDINARY);
        manager.addContribution(BINDING_ID, localPayload);

        RuntimeMembershipPayload coordinatorPayload = new RuntimeMembershipPayload(ImmutableList.of(singleValue(BIGINT, 30L)), ORDINARY);
        RuntimeConstraintSnapshot coordinatorFinal = RuntimeConstraintSnapshot.finalSnapshot(CONSTRAINT_ID, GENERATION, 1, coordinatorPayload);
        assertThat(manager.applyUpdates(new RuntimeConstraintUpdateBatch(RuntimeConstraintProtocol.CURRENT_FORMAT_VERSION, 1, GENERATION, ImmutableList.of(coordinatorFinal)))).isFalse();
        assertThat(manager.getSnapshot(CONSTRAINT_ID).orElseThrow().payload()).contains(localPayload);
    }

    @Test
    public void testTaskCompletionReleasesConsumerState()
    {
        LocalMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext("test");
        TaskRuntimeConstraintManager manager = manager(true, memoryContext, () -> {});
        RuntimeMembershipPayload payload = new RuntimeMembershipPayload(ImmutableList.of(singleValue(BIGINT, 31L)), ORDINARY);
        manager.addContribution(BINDING_ID, payload);
        assertThat(memoryContext.getBytes()).isGreaterThan(0);

        manager.taskFinished();

        assertThat(manager.getSnapshot(CONSTRAINT_ID).orElseThrow().state()).isEqualTo(CLOSED);
        assertThat(memoryContext.getBytes()).isEqualTo(payload.getRetainedSizeInBytes());
        assertThat(manager.acknowledgeContributionsAndGetBatch(0).contributions()).hasSize(1);

        manager.acknowledgeContributions(Long.MAX_VALUE);
        assertThat(memoryContext.getBytes()).isZero();

        manager.close();
        assertThat(manager.getRetainedBytes()).isZero();
    }

    @Test
    public void testMemoryAdmissionFailureDisablesConstraint()
    {
        RuntimeMembershipPayload payload = new RuntimeMembershipPayload(ImmutableList.of(singleValue(BIGINT, 37L)), ORDINARY);
        TaskRuntimeConstraintManager consumer = manager(false, rejectingMemoryContext(), () -> {});
        CompletableFuture<RuntimeConstraintSnapshot> waiter = consumer.waitForUpdate(CONSTRAINT_ID, 0);

        assertThat(consumer.applyUpdates(new RuntimeConstraintUpdateBatch(
                RuntimeConstraintProtocol.CURRENT_FORMAT_VERSION,
                1,
                GENERATION,
                ImmutableList.of(RuntimeConstraintSnapshot.finalSnapshot(CONSTRAINT_ID, GENERATION, 1, payload))))).isTrue();
        assertThat(waiter.join().state()).isEqualTo(DISABLED);
        assertThat(consumer.getSnapshot(CONSTRAINT_ID).orElseThrow().state()).isEqualTo(DISABLED);
        assertThat(consumer.getRetainedBytes()).isZero();

        TaskRuntimeConstraintManager producer = manager(true, rejectingMemoryContext(), () -> {});
        assertThat(producer.addContribution(BINDING_ID, payload)).isTrue();
        assertThat(producer.getSnapshot(CONSTRAINT_ID).orElseThrow().state()).isEqualTo(DISABLED);
        assertThat(producer.getRetainedBytes()).isZero();
        assertThat(producer.acknowledgeContributionsAndGetBatch(0).contributions())
                .singleElement()
                .satisfies(contribution -> assertThat(((RuntimeMembershipPayload) contribution.payload()).scalarDomains())
                        .allMatch(Domain::isAll));
    }

    @Test
    public void testCoordinatorFinalIsAcknowledgedAfterLocalSnapshotAdmissionFailure()
    {
        RuntimeMembershipPayload payload = new RuntimeMembershipPayload(ImmutableList.of(singleValue(BIGINT, 41L)), ORDINARY);
        TaskRuntimeConstraintManager manager = manager(true, firstPositiveReservationOnlyMemoryContext(), () -> {});

        assertThat(manager.addContribution(BINDING_ID, payload)).isTrue();
        assertThat(manager.getSnapshot(CONSTRAINT_ID).orElseThrow().state()).isEqualTo(DISABLED);
        manager.acknowledgeContributions(1);

        RuntimeConstraintSnapshot coordinatorFinal = RuntimeConstraintSnapshot.finalSnapshot(CONSTRAINT_ID, GENERATION, 1, payload);
        assertThat(manager.applyUpdates(new RuntimeConstraintUpdateBatch(
                RuntimeConstraintProtocol.CURRENT_FORMAT_VERSION,
                1,
                GENERATION,
                ImmutableList.of(coordinatorFinal))))
                .isFalse();
        assertThat(manager.getUpdateAcknowledgement()).isEqualTo(1);
        assertThat(manager.getSnapshot(CONSTRAINT_ID).orElseThrow().state()).isEqualTo(DISABLED);
    }

    private static TaskId taskId()
    {
        return new TaskId(new StageId(new QueryId("query"), 1), 3, 2);
    }

    private static LocalMemoryContext rejectingMemoryContext()
    {
        MemoryReservationHandler reservationHandler = new MemoryReservationHandler()
        {
            @Override
            public ListenableFuture<Void> reserveMemory(String allocationTag, long delta)
            {
                assertThat(delta).isLessThanOrEqualTo(0);
                return immediateVoidFuture();
            }

            @Override
            public boolean tryReserveMemory(String allocationTag, long delta)
            {
                return delta <= 0;
            }
        };
        return newRootAggregatedMemoryContext(reservationHandler, 0).newLocalMemoryContext("test");
    }

    private static LocalMemoryContext firstPositiveReservationOnlyMemoryContext()
    {
        AtomicInteger positiveReservations = new AtomicInteger();
        MemoryReservationHandler reservationHandler = new MemoryReservationHandler()
        {
            @Override
            public ListenableFuture<Void> reserveMemory(String allocationTag, long delta)
            {
                assertThat(delta).isLessThanOrEqualTo(0);
                return immediateVoidFuture();
            }

            @Override
            public boolean tryReserveMemory(String allocationTag, long delta)
            {
                return delta <= 0 || positiveReservations.incrementAndGet() == 1;
            }
        };
        return newRootAggregatedMemoryContext(reservationHandler, 0).newLocalMemoryContext("test");
    }

    private static TaskRuntimeConstraintManager manager(
            boolean localBypass,
            LocalMemoryContext memoryContext,
            Runnable notification)
    {
        TaskRuntimeConstraintManager manager = new TaskRuntimeConstraintManager(taskId(), GENERATION, memoryContext, notification);
        manager.registerSource(new RuntimeConstraintWiringReport.Source(
                JOIN_ID,
                ImmutableList.of(new CollectedConstraint(CONSTRAINT_ID, 0)),
                ImmutableList.of(BIGINT),
                localBypass ? EQUIVALENT_REPLICAS : UNION_ALL_PARTITIONS));
        manager.registerConsumers(ImmutableList.of(new RuntimeConstraintWiringReport.Binding(
                new RuntimeConstraintRequest(CONSTRAINT_ID, 0),
                new TestingColumnHandle("key"))));
        return manager;
    }

    private static TaskRuntimeConstraintManager manager(int producerCount, LocalMemoryContext memoryContext)
    {
        TaskRuntimeConstraintManager manager = new TaskRuntimeConstraintManager(taskId(), GENERATION, memoryContext, () -> {});
        IntStream.range(0, producerCount)
                .forEach(index -> manager.registerSource(new RuntimeConstraintWiringReport.Source(
                        new PlanNodeId("join_" + index),
                        ImmutableList.of(new CollectedConstraint(new RuntimeConstraintId("constraint_" + index), 0)),
                        ImmutableList.of(BIGINT))));
        return manager;
    }

    private record TestingColumnHandle(String name)
            implements ColumnHandle {}
}
