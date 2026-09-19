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
import com.google.common.collect.ImmutableSet;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.operator.RuntimeConstraintRequest;
import io.trino.spi.QueryId;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintWiringReport.CollectedConstraint;
import io.trino.sql.planner.runtimeconstraint.RuntimeMembershipPayload.Lane;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN;
import static io.trino.sql.planner.runtimeconstraint.DistributedCompletionPolicy.EQUIVALENT_REPLICAS;
import static io.trino.sql.planner.runtimeconstraint.DistributedCompletionPolicy.UNION_ALL_PARTITIONS;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintNullMatchMode.NULL_SAFE;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintNullMatchMode.ORDINARY;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintProducerGroupState.FINAL;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintPublicationState.CLOSED;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintPublicationState.DISABLED;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintPublicationState.PENDING;
import static org.assertj.core.api.Assertions.assertThat;

class TestRuntimeConstraintHub
{
    private static final QueryId QUERY_ID = new QueryId("query");
    private static final PlanNodeId SOURCE_ID = new PlanNodeId("join");
    private static final RuntimeConstraintId CONSTRAINT_ID = RuntimeConstraintRequest.joinConstraintId(SOURCE_ID, 0);
    private static final ProducerGroupId GROUP_ID = RuntimeConstraintHub.dynamicGroupId(SOURCE_ID);
    private static final ProducerBindingId BINDING_ID = RuntimeConstraintHub.dynamicBindingId(SOURCE_ID);

    @Test
    void testReorderedConstraintsPreserveSharedLanesAndNullObservations()
    {
        RuntimeConstraintHub hub = new RuntimeConstraintHub(0, Long.MAX_VALUE, false);
        RuntimeConstraintId greater = new RuntimeConstraintId("greater");
        RuntimeConstraintId nullSafe = new RuntimeConstraintId("null_safe");
        RuntimeConstraintId less = new RuntimeConstraintId("less");
        hub.registerSource(task(0, 0), new RuntimeConstraintWiringReport.Source(
                SOURCE_ID,
                ImmutableList.of(
                        new CollectedConstraint(greater, GREATER_THAN, false, 1),
                        new CollectedConstraint(nullSafe, EQUAL, true, 0),
                        new CollectedConstraint(less, LESS_THAN, false, 1)),
                ImmutableList.of(BIGINT, BIGINT),
                EQUIVALENT_REPLICAS));

        hub.acceptContributions(task(0, 0), batch(0, 0, new RuntimeMembershipPayload(
                ImmutableList.of(new Lane(Domain.none(BIGINT), true), new Lane(Domain.singleValue(BIGINT, 11L), false)),
                ORDINARY,
                true)));

        assertThat(hub.get(greater).payload()).contains(new RuntimeMembershipPayload(
                ImmutableList.of(Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 11L)), false)), ORDINARY));
        assertThat(hub.get(nullSafe).payload()).contains(new RuntimeMembershipPayload(ImmutableList.of(Domain.onlyNull(BIGINT)), NULL_SAFE));
        assertThat(hub.get(less).payload()).contains(new RuntimeMembershipPayload(
                ImmutableList.of(Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 11L)), false)), ORDINARY));
    }

    @Test
    void testEquivalentReplicaPublishesFirstContribution()
    {
        RuntimeConstraintHub hub = hub(EQUIVALENT_REPLICAS, false);
        CompletableFuture<RuntimeConstraintSnapshot> update = hub.waitForUpdate(CONSTRAINT_ID, 0);

        RuntimeMembershipPayload payload = payload(11);
        assertThat(hub.acceptContributions(task(0, 0), batch(0, 0, payload))).isTrue();

        assertThat(update.join().payload()).contains(payload);
        assertThat(hub.getGroupState(GROUP_ID)).isEqualTo(FINAL);
        assertThat(hub.isCollectingTaskNeeded(new PlanFragmentId("1"))).isTrue();
    }

    @Test
    void testPartitionedSourceUnionsEverySealedPartition()
    {
        RuntimeConstraintHub hub = hub(UNION_ALL_PARTITIONS, false);
        hub.acceptContributions(task(0, 0), batch(0, 0, payload(11)));
        hub.acceptContributions(task(1, 0), batch(1, 0, payload(13)));
        assertThat(hub.get(CONSTRAINT_ID).state()).isEqualTo(PENDING);

        hub.sealProducerStage(new PlanFragmentId("1"), ImmutableSet.of(0, 1));

        RuntimeMembershipPayload result = (RuntimeMembershipPayload) hub.get(CONSTRAINT_ID).payload().orElseThrow();
        assertThat(result.scalarDomains()).containsExactly(Domain.multipleValues(BIGINT, ImmutableList.of(11L, 13L)));
        assertThat(hub.isStageSchedulingNeeded(new PlanFragmentId("1"))).isTrue();
    }

    @Test
    void testEmptyPartitionedSourcePublishesNone()
    {
        RuntimeConstraintHub hub = hub(UNION_ALL_PARTITIONS, false);

        hub.sealProducerStage(new PlanFragmentId("1"), ImmutableSet.of());

        RuntimeMembershipPayload result = (RuntimeMembershipPayload) hub.get(CONSTRAINT_ID).payload().orElseThrow();
        assertThat(result.scalarDomains()).containsExactly(Domain.none(BIGINT));
    }

    @Test
    void testTaskRetryPublishesFirstEquivalentReplica()
    {
        RuntimeConstraintHub hub = hub(EQUIVALENT_REPLICAS, true);
        RuntimeMembershipPayload payload = payload(11);

        hub.acceptContributions(task(0, 0), batch(0, 0, payload));

        assertThat(hub.get(CONSTRAINT_ID).state()).isEqualTo(PENDING);
        hub.taskFinished(task(0, 0), true, 1);

        assertThat(hub.get(CONSTRAINT_ID).payload()).contains(payload);
    }

    @Test
    void testTaskRetryPublishesFirstCompleteAttemptForEachPartition()
    {
        RuntimeConstraintHub hub = hub(UNION_ALL_PARTITIONS, true);
        hub.sealProducerStage(new PlanFragmentId("1"), ImmutableSet.of(0));

        hub.acceptContributions(task(0, 0), batch(0, 0, payload(11)));
        hub.taskFinished(task(0, 0), false, 1);
        hub.acceptContributions(task(0, 1), batch(0, 1, payload(17)));
        hub.taskFinished(task(0, 1), true, 1);

        assertThat(hub.get(CONSTRAINT_ID).payload()).contains(payload(17));
    }

    @Test
    void testPartitionIdentityIncludesProducerFragment()
    {
        RuntimeConstraintHub hub = hub(UNION_ALL_PARTITIONS, true);
        TaskId secondFragmentTask = new TaskId(new StageId(QUERY_ID, 2), 0, 0);
        hub.registerSource(secondFragmentTask, new RuntimeConstraintWiringReport.Source(
                SOURCE_ID,
                ImmutableList.of(new CollectedConstraint(CONSTRAINT_ID, 0)),
                ImmutableList.of(BIGINT),
                UNION_ALL_PARTITIONS));
        hub.sealProducerStage(new PlanFragmentId("1"), ImmutableSet.of(0));
        hub.sealProducerStage(new PlanFragmentId("2"), ImmutableSet.of(0));

        hub.acceptContributions(task(0, 0), batch(0, 0, payload(11)));
        hub.taskFinished(task(0, 0), true, 1);
        assertThat(hub.get(CONSTRAINT_ID).state()).isEqualTo(PENDING);

        hub.acceptContributions(secondFragmentTask, batch(0, 0, payload(22)));
        hub.taskFinished(secondFragmentTask, true, 1);

        RuntimeMembershipPayload result = (RuntimeMembershipPayload) hub.get(CONSTRAINT_ID).payload().orElseThrow();
        assertThat(result.scalarDomains()).containsExactly(Domain.multipleValues(BIGINT, ImmutableList.of(11L, 22L)));
    }

    @Test
    void testSourceRegistrationBarrierWaitsForEveryProducerFragment()
    {
        RuntimeConstraintHub hub = new RuntimeConstraintHub(0, Long.MAX_VALUE, false, true);
        register(hub, UNION_ALL_PARTITIONS);
        hub.registerConsumers(ImmutableList.of(new RuntimeConstraintWiringReport.Binding(
                new RuntimeConstraintRequest(CONSTRAINT_ID, 0),
                new TestingColumnHandle("key"))));
        TaskId secondFragmentTask = new TaskId(new StageId(QUERY_ID, 2), 0, 0);
        hub.registerSource(secondFragmentTask, new RuntimeConstraintWiringReport.Source(
                SOURCE_ID,
                ImmutableList.of(new CollectedConstraint(CONSTRAINT_ID, 0)),
                ImmutableList.of(BIGINT),
                UNION_ALL_PARTITIONS));

        hub.acceptContributions(task(0, 0), batch(0, 0, payload(11)));
        hub.sealProducerStage(new PlanFragmentId("1"), ImmutableSet.of(0));
        hub.acceptContributions(secondFragmentTask, batch(0, 0, payload(22)));
        hub.sealProducerStage(new PlanFragmentId("2"), ImmutableSet.of(0));
        assertThat(hub.get(CONSTRAINT_ID).state()).isEqualTo(PENDING);

        hub.completeSourceRegistration(
                ImmutableSet.of(CONSTRAINT_ID),
                ImmutableSet.of(new PlanFragmentId("1"), new PlanFragmentId("2")));

        RuntimeMembershipPayload result = (RuntimeMembershipPayload) hub.get(CONSTRAINT_ID).payload().orElseThrow();
        assertThat(result.scalarDomains()).containsExactly(Domain.multipleValues(BIGINT, ImmutableList.of(11L, 22L)));
    }

    @Test
    void testSuccessfulSourceWithoutContributionDisablesPruning()
    {
        RuntimeConstraintHub hub = hub(UNION_ALL_PARTITIONS, true);
        hub.sealProducerStage(new PlanFragmentId("1"), ImmutableSet.of(0));

        hub.taskFinished(task(0, 0), true, 0);

        assertThat(hub.get(CONSTRAINT_ID).state()).isEqualTo(DISABLED);
    }

    @Test
    void testSuccessfulEmptyTaskContributesExplicitNone()
    {
        RuntimeConstraintHub hub = hub(UNION_ALL_PARTITIONS, true);
        hub.sealProducerStage(new PlanFragmentId("1"), ImmutableSet.of(0));
        hub.acceptContributions(task(0, 0), batch(0, 0, new RuntimeMembershipPayload(ImmutableList.of(Domain.none(BIGINT)), ORDINARY)));

        hub.taskFinished(task(0, 0), true, 1);

        RuntimeMembershipPayload result = (RuntimeMembershipPayload) hub.get(CONSTRAINT_ID).payload().orElseThrow();
        assertThat(result.scalarDomains()).containsExactly(Domain.none(BIGINT));
    }

    @Test
    void testLateSourceDoesNotInferEmptyInputFromCompletedTask()
    {
        RuntimeConstraintHub hub = new RuntimeConstraintHub(0, Long.MAX_VALUE, true);
        hub.sealProducerStage(new PlanFragmentId("1"), ImmutableSet.of(0));
        hub.taskFinished(task(0, 0), true, 0);

        register(hub, UNION_ALL_PARTITIONS);
        hub.acceptContributions(task(0, 0), batch(0, 0, payload(47)));

        assertThat(hub.get(CONSTRAINT_ID).state()).isEqualTo(DISABLED);
        assertThat(hub.get(CONSTRAINT_ID).payload()).isEmpty();
    }

    @Test
    void testRetiredAttemptCannotBecomeSuccessfulAgain()
    {
        RuntimeConstraintHub hub = hub(UNION_ALL_PARTITIONS, true);
        hub.sealProducerStage(new PlanFragmentId("1"), ImmutableSet.of(0));
        hub.taskFinished(task(0, 0), false, 0);
        hub.taskFinished(task(0, 0), true, 0);
        assertThat(hub.get(CONSTRAINT_ID).state()).isEqualTo(PENDING);

        RuntimeMembershipPayload payload = payload(11);
        hub.acceptContributions(task(0, 1), batch(0, 1, payload));
        hub.taskFinished(task(0, 1), true, 1);
        assertThat(hub.get(CONSTRAINT_ID).payload()).contains(payload);
    }

    @Test
    void testSuccessfulTaskWaitsForFinalContributionSequence()
    {
        RuntimeConstraintHub hub = hub(UNION_ALL_PARTITIONS, true);
        hub.sealProducerStage(new PlanFragmentId("1"), ImmutableSet.of(0));

        hub.taskFinished(task(0, 0), true, 1);

        assertThat(hub.get(CONSTRAINT_ID).state()).isEqualTo(PENDING);

        RuntimeMembershipPayload payload = payload(17);
        hub.acceptContributions(task(0, 0), batch(0, 0, payload));

        assertThat(hub.get(CONSTRAINT_ID).payload()).contains(payload);
    }

    @Test
    void testTaskSuccessBeforeSourceRegistration()
    {
        RuntimeConstraintHub hub = new RuntimeConstraintHub(0, Long.MAX_VALUE, true, true);
        hub.acceptContributions(task(0, 0), batch(0, 0, payload(11)));
        hub.taskFinished(task(0, 0), true, 1);

        register(hub, UNION_ALL_PARTITIONS);
        hub.completeSourceRegistration(ImmutableSet.of(CONSTRAINT_ID), ImmutableSet.of(new PlanFragmentId("1")));
        hub.sealProducerStage(new PlanFragmentId("1"), ImmutableSet.of(0));

        assertThat(hub.get(CONSTRAINT_ID).payload()).contains(payload(11));
    }

    @Test
    void testTaskSuccessAndContributionBeforeSourceRegistration()
    {
        RuntimeConstraintHub hub = new RuntimeConstraintHub(0, Long.MAX_VALUE, true, true);
        hub.taskFinished(task(0, 0), true, 1);
        hub.acceptContributions(task(0, 0), batch(0, 0, payload(11)));

        register(hub, UNION_ALL_PARTITIONS);
        hub.completeSourceRegistration(ImmutableSet.of(CONSTRAINT_ID), ImmutableSet.of(new PlanFragmentId("1")));
        hub.sealProducerStage(new PlanFragmentId("1"), ImmutableSet.of(0));

        assertThat(hub.get(CONSTRAINT_ID).payload()).contains(payload(11));
    }

    @Test
    void testSourceRegistrationAtomicallyAttachesBufferedContribution()
    {
        for (int iteration = 0; iteration < 1_000; iteration++) {
            RuntimeConstraintHub hub = new RuntimeConstraintHub(0, Long.MAX_VALUE, true, true);
            hub.acceptContributions(task(0, 0), batch(0, 0, payload(11)));
            CountDownLatch start = new CountDownLatch(1);

            CompletableFuture<Void> registration = CompletableFuture.runAsync(() -> {
                awaitUninterruptibly(start);
                register(hub, UNION_ALL_PARTITIONS);
            });
            CompletableFuture<Void> completion = CompletableFuture.runAsync(() -> {
                awaitUninterruptibly(start);
                hub.taskFinished(task(0, 0), true, 1);
            });
            start.countDown();
            CompletableFuture.allOf(registration, completion).join();

            hub.completeSourceRegistration(ImmutableSet.of(CONSTRAINT_ID), ImmutableSet.of(new PlanFragmentId("1")));
            hub.sealProducerStage(new PlanFragmentId("1"), ImmutableSet.of(0));
            assertThat(hub.get(CONSTRAINT_ID).payload()).contains(payload(11));
        }
    }

    @Test
    void testContributionCanArriveBeforeSourceRegistration()
    {
        RuntimeConstraintHub hub = new RuntimeConstraintHub(0, Long.MAX_VALUE);
        RuntimeMembershipPayload payload = payload(19);
        assertThat(hub.acceptContributions(task(0, 0), batch(0, 0, payload))).isTrue();

        register(hub, EQUIVALENT_REPLICAS);

        assertThat(hub.get(CONSTRAINT_ID).payload()).contains(payload);
    }

    @Test
    void testConsumerAndInitialUnblockCanPrecedeSourceRegistration()
    {
        RuntimeConstraintHub hub = new RuntimeConstraintHub(0, Long.MAX_VALUE);
        RuntimeConstraintWiringReport.Binding binding = new RuntimeConstraintWiringReport.Binding(
                new RuntimeConstraintRequest(CONSTRAINT_ID, 0),
                new TestingColumnHandle("key"));
        hub.registerConsumers(ImmutableList.of(binding));
        CompletableFuture<RuntimeConstraintSnapshot> update = hub.waitForUpdate(CONSTRAINT_ID, 0);
        CompletableFuture<Void> unblock = hub.waitForInitialUnblock(CONSTRAINT_ID);

        hub.unblockStageDynamicFilters(new PlanFragmentId("1"));
        register(hub, EQUIVALENT_REPLICAS);

        assertThat(unblock).isDone();
        assertThat(update).isNotDone();
    }

    @Test
    void testMemoryLimitDisablesConstraint()
    {
        RuntimeConstraintHub hub = new RuntimeConstraintHub(0, 0);
        register(hub, EQUIVALENT_REPLICAS);

        hub.acceptContributions(task(0, 0), batch(0, 0, payload(23)));

        assertThat(hub.get(CONSTRAINT_ID).state()).isEqualTo(DISABLED);
        assertThat(hub.getRetainedBytes()).isZero();
    }

    @Test
    void testPendingUnregisteredContributionsAreMemoryLimited()
    {
        RuntimeConstraintHub hub = new RuntimeConstraintHub(0, 1024, true);

        for (int partition = 0; partition < 100; partition++) {
            hub.acceptContributions(task(partition, 0), batch(partition, 0, payload(partition)));
        }

        assertThat(hub.getRetainedBytes()).isLessThanOrEqualTo(1024);
        register(hub, UNION_ALL_PARTITIONS);
        assertThat(hub.get(CONSTRAINT_ID).state()).isEqualTo(DISABLED);
        assertThat(hub.getRetainedBytes()).isZero();
    }

    @Test
    void testFailedAttemptReleasesPendingUnregisteredContribution()
    {
        RuntimeConstraintHub hub = new RuntimeConstraintHub(0, Long.MAX_VALUE, true);
        RuntimeMembershipPayload payload = payload(11);
        hub.acceptContributions(task(0, 0), batch(0, 0, payload));
        assertThat(hub.getRetainedBytes()).isEqualTo(payload.getRetainedSizeInBytes());

        hub.taskFinished(task(0, 0), false, 1);

        assertThat(hub.getRetainedBytes()).isZero();
    }

    @Test
    void testQueryRetryStartsNewGeneration()
    {
        RuntimeConstraintHub first = hub(EQUIVALENT_REPLICAS, false);
        RuntimeConstraintHub retry = first.forQueryRetry(1);
        register(retry, EQUIVALENT_REPLICAS);

        assertThat(retry.getGeneration()).isEqualTo(1);
        assertThat(retry.getQueryAttempt()).isEqualTo(1);
        assertThat(retry.get(CONSTRAINT_ID).state()).isEqualTo(PENDING);
    }

    @Test
    void testCloseCompletesWaiters()
    {
        RuntimeConstraintHub hub = hub(EQUIVALENT_REPLICAS, false);
        CompletableFuture<RuntimeConstraintSnapshot> update = hub.waitForUpdate(CONSTRAINT_ID, 0);

        hub.close();

        assertThat(update.join().state()).isEqualTo(CLOSED);
        assertThat(hub.getRetainedBytes()).isZero();
    }

    private static RuntimeConstraintHub hub(DistributedCompletionPolicy policy, boolean taskRetries)
    {
        RuntimeConstraintHub hub = new RuntimeConstraintHub(0, Long.MAX_VALUE, taskRetries);
        register(hub, policy);
        hub.registerConsumers(ImmutableList.of(new RuntimeConstraintWiringReport.Binding(
                new RuntimeConstraintRequest(CONSTRAINT_ID, 0),
                new TestingColumnHandle("key"))));
        return hub;
    }

    private static void register(RuntimeConstraintHub hub, DistributedCompletionPolicy policy)
    {
        hub.registerSource(task(0, 0), new RuntimeConstraintWiringReport.Source(
                SOURCE_ID,
                ImmutableList.of(new CollectedConstraint(CONSTRAINT_ID, 0)),
                ImmutableList.of(BIGINT),
                policy));
    }

    private static RuntimeConstraintContributionBatch batch(int partition, int attempt, RuntimeMembershipPayload payload)
    {
        return new RuntimeConstraintContributionBatch(
                RuntimeConstraintProtocol.CURRENT_FORMAT_VERSION,
                1,
                0,
                ImmutableList.of(new RuntimeConstraintContribution(
                        GROUP_ID,
                        BINDING_ID,
                        partition,
                        attempt,
                        1,
                        payload)));
    }

    private static TaskId task(int partition, int attempt)
    {
        return new TaskId(new StageId(QUERY_ID, 1), partition, attempt);
    }

    private static RuntimeMembershipPayload payload(long value)
    {
        return new RuntimeMembershipPayload(ImmutableList.of(Domain.singleValue(BIGINT, value)), ORDINARY);
    }

    private record TestingColumnHandle(String name)
            implements ColumnHandle {}
}
