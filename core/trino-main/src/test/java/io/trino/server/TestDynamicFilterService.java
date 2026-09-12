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
package io.trino.server;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.connector.TestingColumnHandle;
import io.trino.cost.StatsAndCosts;
import io.trino.execution.DynamicFilterConfig;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.operator.RuntimeConstraintRequest;
import io.trino.server.DynamicFilterService.DynamicFiltersStats;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.ir.ComparisonOperator;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.SubPlan;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.planner.runtimeconstraint.DistributedCompletionPolicy;
import io.trino.sql.planner.runtimeconstraint.ProducerBindingId;
import io.trino.sql.planner.runtimeconstraint.ProducerGroupId;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintContribution;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintContributionBatch;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintHub;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintId;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintProtocol;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintSnapshot;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintSubscription;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintTransform;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintWiringReport;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintWiringReport.CollectedConstraint;
import io.trino.sql.planner.runtimeconstraint.RuntimeMembershipPayload;
import io.trino.testing.TestingSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;

import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.planner.runtimeconstraint.DistributedCompletionPolicy.EQUIVALENT_REPLICAS;
import static io.trino.sql.planner.runtimeconstraint.DistributedCompletionPolicy.UNION_ALL_PARTITIONS;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintNullMatchMode.ORDINARY;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintPublicationState.CLOSED;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintPublicationState.DISABLED;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintPublicationState.PENDING;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDynamicFilterService
{
    private static final Session SESSION = TestingSession.testSessionBuilder()
            .setSystemProperty("legacy_dynamic_filtering", "false")
            .build();
    private static final Session TASK_RETRY_SESSION = Session.builder(SESSION)
            .setSystemProperty("retry_policy", "TASK")
            .build();
    private static final TestingColumnHandle COLUMN = new TestingColumnHandle("value");

    @Test
    public void testReportsRuntimeConstraintsAsDynamicFilters()
    {
        DynamicFilterService service = createDynamicFilterService();
        PlanFragment fragment = fragment();
        PlanNodeId sourceId = fragment.getRoot().getId();
        RuntimeConstraintId constraintId = RuntimeConstraintRequest.joinConstraintId(sourceId, 0);
        service.registerQuery(SESSION, new SubPlan(fragment, ImmutableList.of()));

        CompletableFuture<DynamicFilter> discovered = service.discoverRuntimeConstraintDynamicFilter(SESSION, sourceId, ImmutableList.of(COLUMN));
        assertThat(discovered).isNotDone();
        service.addTaskRuntimeConstraintWiring(taskId(0), wiring(sourceId, constraintId));
        assertThat(discovered.join().getColumnsCovered()).containsExactly(COLUMN);

        DynamicFiltersStats pending = service.getDynamicFilteringStats(SESSION.getQueryId());
        assertThat(pending.getTotalDynamicFilters()).isEqualTo(1);
        assertThat(pending.getLazyDynamicFilters()).isEqualTo(1);
        assertThat(pending.getReplicatedDynamicFilters()).isEqualTo(1);
        assertThat(pending.getDynamicFiltersCompleted()).isZero();

        RuntimeMembershipPayload payload = new RuntimeMembershipPayload(ImmutableList.of(singleValue(BIGINT, 11L)), ORDINARY);
        service.getRuntimeConstraintHub(SESSION.getQueryId()).orElseThrow().publishFinal(constraintId, 0, payload);

        DynamicFiltersStats completed = service.getDynamicFilteringStats(SESSION.getQueryId());
        assertThat(completed.getDynamicFiltersCompleted()).isEqualTo(1);
        assertThat(completed.getDynamicFilterDomainStats()).singleElement().satisfies(statistics -> {
            assertThat(statistics.getDynamicFilterId()).isEqualTo(new DynamicFilterId(constraintId.toString()));
            assertThat(statistics.getSimplifiedDomain()).isEqualTo(singleValue(BIGINT, 11L).toString(2));
            assertThat(statistics.getCollectionDuration()).isPresent();
        });
    }

    @Test
    public void testFailedTransformationUnblocksRemoteConsumer()
    {
        DynamicFilterService service = createDynamicFilterService();
        PlanFragment fragment = fragment();
        RuntimeConstraintId root = new RuntimeConstraintId("root");
        RuntimeConstraintSubscription cast = RuntimeConstraintSubscription.create(root, root, "unsupported cast", RuntimeConstraintTransform.cast(BIGINT));
        service.registerQuery(SESSION, new SubPlan(fragment, ImmutableList.of()));
        service.addTaskRuntimeConstraintWiring(taskId(0), new RuntimeConstraintWiringReport(
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableList.of(cast),
                ImmutableList.of(new RuntimeConstraintSubscription.Input(root, root))));
        ArrayList<RuntimeConstraintSnapshot> updates = new ArrayList<>();
        service.registerRuntimeConstraintConsumer(SESSION.getQueryId(), 0, 0, ImmutableSet.of(cast.id()), updates::addAll);

        service.getRuntimeConstraintHub(SESSION.getQueryId()).orElseThrow().publishFinal(
                root, 0, new RuntimeMembershipPayload(ImmutableList.of(singleValue(DOUBLE, 11.0)), ORDINARY));

        assertThat(updates).containsExactly(RuntimeConstraintSnapshot.terminal(cast.id(), 0, 1, DISABLED));
    }

    @Test
    public void testOwnsRuntimeConstraintHubForQueryLifetimeAndRetry()
    {
        DynamicFilterService service = createDynamicFilterService();
        PlanFragment fragment = fragment();
        PlanNodeId sourceId = fragment.getRoot().getId();
        RuntimeConstraintId constraintId = RuntimeConstraintRequest.joinConstraintId(sourceId, 0);
        service.registerQuery(SESSION, new SubPlan(fragment, ImmutableList.of()));
        service.addTaskRuntimeConstraintWiring(taskId(0), wiring(sourceId, constraintId));

        RuntimeConstraintHub hub = service.getRuntimeConstraintHub(SESSION.getQueryId()).orElseThrow();
        assertThat(hub.getGeneration()).isZero();
        assertThat(hub.get(constraintId).state()).isEqualTo(PENDING);
        assertThat(service.isCollectingTaskNeeded(SESSION.getQueryId(), fragment)).isTrue();
        assertThat(service.isStageSchedulingNeededToCollectDynamicFilters(SESSION.getQueryId(), fragment)).isFalse();

        DynamicFilter dynamicFilter = service.discoverRuntimeConstraintDynamicFilter(SESSION, sourceId, ImmutableList.of(COLUMN)).join();
        CompletableFuture<?> blocked = dynamicFilter.isBlocked();
        assertThat(blocked).isNotDone();
        service.unblockStageDynamicFilters(SESSION.getQueryId(), 0, fragment);
        assertThat(blocked).isDone();

        RuntimeMembershipPayload initialPayload = new RuntimeMembershipPayload(ImmutableList.of(singleValue(BIGINT, 17L)), ORDINARY);
        hub.publishFinal(constraintId, 0, initialPayload);
        assertThat(dynamicFilter.getCurrentPredicate()).isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(
                COLUMN, singleValue(BIGINT, 17L))));

        service.registerQueryRetry(SESSION.getQueryId(), 1);
        CompletableFuture<DynamicFilter> retriedDiscovery = service.discoverRuntimeConstraintDynamicFilter(SESSION, sourceId, ImmutableList.of(COLUMN));
        assertThat(retriedDiscovery).isNotCompletedExceptionally();
        assertThat(retriedDiscovery).isNotDone();
        service.addTaskRuntimeConstraintWiring(taskId(1), wiring(sourceId, constraintId));
        assertThat(retriedDiscovery.join().getColumnsCovered()).containsExactly(COLUMN);
        RuntimeConstraintHub retryHub = service.getRuntimeConstraintHub(SESSION.getQueryId()).orElseThrow();
        assertThat(retryHub).isNotSameAs(hub);
        assertThat(retryHub.getGeneration()).isEqualTo(1);
        assertThat(retryHub.getQueryAttempt()).isEqualTo(1);
        assertThat(retryHub.get(constraintId).state()).isEqualTo(PENDING);
        assertThat(hub.get(constraintId).state()).isEqualTo(CLOSED);

        ArrayList<RuntimeConstraintSnapshot> staleAttemptUpdates = new ArrayList<>();
        service.registerRuntimeConstraintConsumer(SESSION.getQueryId(), 0, 0, ImmutableSet.of(constraintId), staleAttemptUpdates::addAll);
        ArrayList<RuntimeConstraintSnapshot> updates = new ArrayList<>();
        service.registerRuntimeConstraintConsumer(SESSION.getQueryId(), 1, 1, ImmutableSet.of(constraintId), updates::addAll);

        RuntimeMembershipPayload payload = new RuntimeMembershipPayload(ImmutableList.of(singleValue(BIGINT, 13L)), ORDINARY);
        ProducerGroupId groupId = RuntimeConstraintHub.dynamicGroupId(sourceId);
        ProducerBindingId bindingId = RuntimeConstraintHub.dynamicBindingId(sourceId);
        TaskId retryTask = taskId(1);
        service.addTaskRuntimeConstraintContributions(
                retryTask,
                new RuntimeConstraintContributionBatch(
                        RuntimeConstraintProtocol.CURRENT_FORMAT_VERSION,
                        1,
                        1,
                        ImmutableList.of(new RuntimeConstraintContribution(
                                groupId,
                                bindingId,
                                retryTask.partitionId(),
                                retryTask.attemptId(),
                                1,
                                payload))));

        assertThat(retryHub.get(constraintId).payload()).contains(payload);
        assertThat(updates).singleElement().satisfies(snapshot -> assertThat(snapshot.payload()).contains(payload));
        assertThat(staleAttemptUpdates).singleElement().satisfies(snapshot -> assertThat(snapshot.state()).isEqualTo(CLOSED));

        service.removeQuery(SESSION.getQueryId());
        assertThat(service.getRuntimeConstraintHub(SESSION.getQueryId())).isEmpty();
        assertThat(retryHub.get(constraintId).state()).isEqualTo(CLOSED);
    }

    @Test
    public void testRejectedCollectionDisablesConstraintWithAnotherRegisteredSource()
    {
        DynamicFilterService service = createDynamicFilterService();
        PlanFragment fragment = fragment();
        PlanNodeId sourceId = fragment.getRoot().getId();
        RuntimeConstraintId constraintId = RuntimeConstraintRequest.joinConstraintId(sourceId, 0);
        service.registerQuery(TASK_RETRY_SESSION, new SubPlan(fragment, ImmutableList.of()));
        service.addTaskRuntimeConstraintWiring(taskId(0), wiring(sourceId, constraintId));
        DynamicFilter filter = service.discoverRuntimeConstraintDynamicFilter(TASK_RETRY_SESSION, sourceId, ImmutableList.of(COLUMN)).join();
        CompletableFuture<?> blocked = filter.isBlocked();
        assertThat(blocked).isNotDone();

        service.addTaskRuntimeConstraintWiring(taskId(0), new RuntimeConstraintWiringReport(
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableList.of(RuntimeConstraintRequest.collection(constraintId, 0, ComparisonOperator.EQUAL, false, BIGINT, false))));

        assertThat(blocked).isDone();
        assertThat(filter.isComplete()).isTrue();
        assertThat(filter.getCurrentPredicate().isAll()).isTrue();
    }

    @ParameterizedTest
    @EnumSource(DistributedCompletionPolicy.class)
    public void testRejectedCollectionPrecedesBufferedContribution(DistributedCompletionPolicy completionPolicy)
    {
        DynamicFilterService service = createDynamicFilterService();
        PlanFragment fragment = fragment();
        PlanNodeId sourceId = fragment.getRoot().getId();
        RuntimeConstraintId constraintId = RuntimeConstraintRequest.joinConstraintId(sourceId, 0);
        TaskId task = taskId(0);
        service.registerQuery(SESSION, new SubPlan(fragment, ImmutableList.of()));
        service.stageCannotScheduleMoreTasks(task.stageId(), task.attemptId(), ImmutableSet.of(task.partitionId()));
        service.addTaskRuntimeConstraintContributions(task, new RuntimeConstraintContributionBatch(
                RuntimeConstraintProtocol.CURRENT_FORMAT_VERSION,
                1,
                0,
                ImmutableList.of(new RuntimeConstraintContribution(
                        RuntimeConstraintHub.dynamicGroupId(sourceId),
                        RuntimeConstraintHub.dynamicBindingId(sourceId),
                        task.partitionId(),
                        task.attemptId(),
                        1,
                        new RuntimeMembershipPayload(ImmutableList.of(singleValue(BIGINT, 11L)), ORDINARY)))));
        service.taskFinished(task, true, 1);

        service.addTaskRuntimeConstraintWiring(task, new RuntimeConstraintWiringReport(
                ImmutableList.of(new RuntimeConstraintWiringReport.ScanWiring(
                        sourceId,
                        ImmutableList.of(new RuntimeConstraintWiringReport.Binding(constraintId, COLUMN)))),
                ImmutableList.of(new RuntimeConstraintWiringReport.Source(
                        sourceId,
                        ImmutableList.of(new CollectedConstraint(constraintId, 0)),
                        ImmutableList.of(BIGINT),
                        completionPolicy)),
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableList.of(RuntimeConstraintRequest.collection(constraintId, 0, ComparisonOperator.EQUAL, false, BIGINT, false))));

        DynamicFilter filter = service.discoverRuntimeConstraintDynamicFilter(SESSION, sourceId, ImmutableList.of(COLUMN)).join();
        assertThat(filter.isComplete()).isTrue();
        assertThat(filter.getCurrentPredicate()).isEqualTo(TupleDomain.all());
        assertThat(service.getRuntimeConstraintHub(SESSION.getQueryId()).orElseThrow().get(constraintId).state()).isEqualTo(DISABLED);
    }

    @Test
    public void testRejectedCollectionAfterChannelMappingUnblocksDiscovery()
    {
        DynamicFilterService service = createDynamicFilterService();
        PlanFragment consumer = fragment("0", "consumer");
        PlanFragment producer = fragment("1", "producer");
        service.registerQuery(SESSION, new SubPlan(consumer, ImmutableList.of(new SubPlan(producer, ImmutableList.of()))));
        RuntimeConstraintRequest request = RuntimeConstraintRequest.collection(new RuntimeConstraintId("collection"), 0, ComparisonOperator.EQUAL, false, BIGINT, false);
        service.addTaskRuntimeConstraintWiring(taskId(0, 0), new RuntimeConstraintWiringReport(
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableList.of(new RuntimeConstraintWiringReport.RemoteRequest(ImmutableList.of(producer.getId()), request))));
        service.addTaskRuntimeConstraintWiring(taskId(1, 0), new RuntimeConstraintWiringReport(ImmutableList.of(
                new RuntimeConstraintWiringReport.ScanWiring(producer.getRoot().getId(), ImmutableList.of()))));
        CompletableFuture<DynamicFilter> discovered = service.discoverRuntimeConstraintDynamicFilter(SESSION, producer.getRoot().getId(), ImmutableList.of(COLUMN));
        assertThat(discovered).isNotDone();

        service.addTaskRuntimeConstraintWiring(taskId(1, 0), new RuntimeConstraintWiringReport(
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableList.of(request.withChannel(1))));

        assertThat(discovered).isCompletedWithValue(DynamicFilter.EMPTY);
    }

    @Test
    public void testQueryRetryDoesNotWaitForSurvivingCoordinatorStage()
    {
        DynamicFilterService service = createDynamicFilterService();
        PlanFragment coordinator = fragment("0", "finish", COORDINATOR_DISTRIBUTION);
        PlanFragment producer = fragment("1", "scan");
        service.registerQuery(SESSION, new SubPlan(coordinator, ImmutableList.of(new SubPlan(producer, ImmutableList.of()))));
        RuntimeConstraintWiringReport scanReport = new RuntimeConstraintWiringReport(ImmutableList.of(
                new RuntimeConstraintWiringReport.ScanWiring(producer.getRoot().getId(), ImmutableList.of())));
        CompletableFuture<DynamicFilter> initial = service.discoverRuntimeConstraintDynamicFilter(SESSION, producer.getRoot().getId(), ImmutableList.of(COLUMN));
        service.addTaskRuntimeConstraintWiring(taskId(1, 0), scanReport);
        assertThat(initial).isNotDone();
        service.addTaskRuntimeConstraintWiring(taskId(0, 0), RuntimeConstraintWiringReport.EMPTY);
        assertThat(initial).isCompletedWithValue(DynamicFilter.EMPTY);

        service.registerQueryRetry(SESSION.getQueryId(), 1);
        CompletableFuture<DynamicFilter> retried = service.discoverRuntimeConstraintDynamicFilter(SESSION, producer.getRoot().getId(), ImmutableList.of(COLUMN));
        service.addTaskRuntimeConstraintWiring(taskId(1, 0), scanReport);
        assertThat(retried).isNotDone();
        service.addTaskRuntimeConstraintWiring(taskId(1, 1), scanReport);
        assertThat(retried).isCompletedWithValue(DynamicFilter.EMPTY);
    }

    @Test
    public void testDiscoversScanConstraintAcrossPhysicalFragments()
    {
        DynamicFilterService service = createDynamicFilterService();
        PlanFragment consumer = fragment("0", "consumer");
        PlanFragment producer = fragment("1", "producer");
        PlanFragment unrelated = fragment("2", "unrelated");
        service.registerQuery(SESSION, new SubPlan(consumer, ImmutableList.of(
                new SubPlan(producer, ImmutableList.of()),
                new SubPlan(unrelated, ImmutableList.of()))));
        RuntimeConstraintId constraintId = new RuntimeConstraintId("remote_constraint");
        RuntimeConstraintRequest request = new RuntimeConstraintRequest(constraintId, 0);
        CompletableFuture<DynamicFilter> discovered = service.discoverRuntimeConstraintDynamicFilter(
                SESSION,
                producer.getRoot().getId(),
                ImmutableList.of(COLUMN));
        List<RuntimeConstraintRequest> delivered = new ArrayList<>();
        service.registerTaskRuntimeConstraintWiring(taskId(1, 0), producer.getId(), delivered::addAll);

        service.addTaskRuntimeConstraintWiring(
                taskId(0, 0),
                new RuntimeConstraintWiringReport(
                        ImmutableList.of(),
                        ImmutableList.of(),
                        ImmutableList.of(new RuntimeConstraintWiringReport.RemoteRequest(ImmutableList.of(producer.getId()), request)),
                        ImmutableList.of()));
        assertThat(delivered).containsExactly(request);
        assertThat(discovered).isNotDone();

        service.addTaskRuntimeConstraintWiring(
                taskId(1, 0),
                new RuntimeConstraintWiringReport(
                        ImmutableList.of(new RuntimeConstraintWiringReport.ScanWiring(
                                producer.getRoot().getId(),
                                ImmutableList.of(new RuntimeConstraintWiringReport.Binding(request, COLUMN)))),
                        ImmutableList.of(new RuntimeConstraintWiringReport.Source(
                                producer.getRoot().getId(),
                                ImmutableList.of(new CollectedConstraint(constraintId, 0)),
                                ImmutableList.of(BIGINT),
                                EQUIVALENT_REPLICAS)),
                        ImmutableList.of(),
                        ImmutableList.of(request)));

        assertThat(discovered.join().getColumnsCovered()).containsExactly(COLUMN);
    }

    @Test
    public void testLateScanRequestIsRetainedForFutureTask()
    {
        DynamicFilterService service = createDynamicFilterService();
        PlanFragment consumer = fragment("0", "consumer");
        PlanFragment producer = fragment("1", "producer");
        service.registerQuery(SESSION, new SubPlan(consumer, ImmutableList.of(new SubPlan(producer, ImmutableList.of()))));
        RuntimeConstraintRequest request = new RuntimeConstraintRequest(new RuntimeConstraintId("remote_constraint"), 0);
        TaskId producerTask = taskId(1, 0);
        service.registerTaskRuntimeConstraintWiring(producerTask, producer.getId(), _ -> {});
        service.unregisterTaskRuntimeConstraintWiring(producerTask, producer.getId());

        service.addTaskRuntimeConstraintWiring(
                taskId(0, 0),
                new RuntimeConstraintWiringReport(
                        ImmutableList.of(),
                        ImmutableList.of(),
                        ImmutableList.of(new RuntimeConstraintWiringReport.RemoteRequest(ImmutableList.of(producer.getId()), request)),
                        ImmutableList.of()));

        List<RuntimeConstraintRequest> delivered = new ArrayList<>();
        service.registerTaskRuntimeConstraintWiring(producerTask, producer.getId(), delivered::addAll);
        assertThat(delivered).containsExactly(request);
    }

    @Test
    public void testFragmentWiringAccumulatesReportsAcrossTasks()
    {
        DynamicFilterService service = createDynamicFilterService();
        PlanFragment consumer = fragment("0", "consumer");
        PlanFragment intermediate = fragment("1", "intermediate");
        PlanFragment producer = fragment("2", "producer");
        service.registerQuery(SESSION, new SubPlan(consumer, ImmutableList.of(
                new SubPlan(intermediate, ImmutableList.of(new SubPlan(producer, ImmutableList.of()))))));
        RuntimeConstraintId constraintId = new RuntimeConstraintId("remote_constraint");
        RuntimeConstraintRequest request = new RuntimeConstraintRequest(constraintId, 0);
        CompletableFuture<DynamicFilter> discovered = service.discoverRuntimeConstraintDynamicFilter(
                SESSION,
                producer.getRoot().getId(),
                ImmutableList.of(COLUMN));

        service.addTaskRuntimeConstraintWiring(
                taskId(1, 0),
                new RuntimeConstraintWiringReport(
                        ImmutableList.of(),
                        ImmutableList.of(),
                        ImmutableList.of(new RuntimeConstraintWiringReport.RemoteRequest(ImmutableList.of(producer.getId()), request)),
                        ImmutableList.of()));
        service.addTaskRuntimeConstraintWiring(
                taskId(2, 0, 0),
                new RuntimeConstraintWiringReport(
                        ImmutableList.of(new RuntimeConstraintWiringReport.ScanWiring(
                                producer.getRoot().getId(),
                                ImmutableList.of(new RuntimeConstraintWiringReport.Binding(request, COLUMN)))),
                        ImmutableList.of(),
                        ImmutableList.of(),
                        ImmutableList.of(request)));
        service.addTaskRuntimeConstraintWiring(taskId(2, 1, 0), RuntimeConstraintWiringReport.EMPTY);
        assertThat(discovered).isNotDone();

        service.addTaskRuntimeConstraintWiring(taskId(0, 0), RuntimeConstraintWiringReport.EMPTY);

        assertThat(discovered.join().getColumnsCovered()).containsExactly(COLUMN);
    }

    @Test
    public void testScanRequestIsRetiredAfterEveryPartitionFinishes()
    {
        DynamicFilterService service = createDynamicFilterService();
        PlanFragment consumer = fragment("0", "consumer");
        PlanFragment producer = fragment("1", "producer");
        service.registerQuery(SESSION, new SubPlan(consumer, ImmutableList.of(new SubPlan(producer, ImmutableList.of()))));
        RuntimeConstraintRequest request = new RuntimeConstraintRequest(new RuntimeConstraintId("remote_constraint"), 0);
        CompletableFuture<DynamicFilter> discovered = service.discoverRuntimeConstraintDynamicFilter(
                SESSION,
                producer.getRoot().getId(),
                ImmutableList.of(COLUMN));

        service.addTaskRuntimeConstraintWiring(
                taskId(0, 0),
                new RuntimeConstraintWiringReport(
                        ImmutableList.of(),
                        ImmutableList.of(),
                        ImmutableList.of(new RuntimeConstraintWiringReport.RemoteRequest(ImmutableList.of(producer.getId()), request)),
                        ImmutableList.of()));
        service.addTaskRuntimeConstraintWiring(
                taskId(1, 0),
                new RuntimeConstraintWiringReport(
                        ImmutableList.of(new RuntimeConstraintWiringReport.ScanWiring(producer.getRoot().getId(), ImmutableList.of())),
                        ImmutableList.of(),
                        ImmutableList.of(),
                        ImmutableList.of()));
        service.stageCannotScheduleMoreTasks(taskId(1, 0).stageId(), 0, ImmutableSet.of(0, 1));
        service.taskRuntimeConstraintWiringFinished(taskId(1, 0, 0), producer.getId(), true);
        assertThat(discovered).isNotDone();

        service.taskRuntimeConstraintWiringFinished(taskId(1, 1, 0), producer.getId(), true);

        assertThat(discovered.join()).isSameAs(DynamicFilter.EMPTY);
    }

    @Test
    public void testReplacementTaskCanDiscoverScanAfterFailedEmptyReport()
    {
        DynamicFilterService service = createDynamicFilterService();
        PlanFragment fragment = fragment();
        service.registerQuery(TASK_RETRY_SESSION, new SubPlan(fragment, ImmutableList.of()));
        PlanNodeId scanId = fragment.getRoot().getId();
        CompletableFuture<DynamicFilter> discovered = service.discoverRuntimeConstraintDynamicFilter(
                TASK_RETRY_SESSION,
                scanId,
                ImmutableList.of(COLUMN));

        TaskId failedTask = taskId(TASK_RETRY_SESSION, 0, 0);
        service.addTaskRuntimeConstraintWiring(failedTask, RuntimeConstraintWiringReport.EMPTY);
        service.taskFinished(failedTask, false, 0);
        assertThat(discovered).isNotDone();

        service.addTaskRuntimeConstraintWiring(
                taskId(TASK_RETRY_SESSION, 0, 1),
                new RuntimeConstraintWiringReport(
                        ImmutableList.of(new RuntimeConstraintWiringReport.ScanWiring(scanId, ImmutableList.of())),
                        ImmutableList.of(),
                        ImmutableList.of(),
                        ImmutableList.of()));

        assertThat(discovered.join()).isSameAs(DynamicFilter.EMPTY);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testSuccessfulProducerPublishesAfterWiringUnregisters(boolean taskStatusArrivesFirst)
    {
        DynamicFilterService service = createDynamicFilterService();
        PlanFragment consumer = fragment("0", "consumer");
        PlanFragment producer = fragment("1", "producer");
        service.registerQuery(TASK_RETRY_SESSION, new SubPlan(consumer, ImmutableList.of(new SubPlan(producer, ImmutableList.of()))));
        RuntimeConstraintId constraintId = new RuntimeConstraintId("constraint");
        RuntimeConstraintRequest request = RuntimeConstraintRequest.collection(
                constraintId,
                0,
                ComparisonOperator.EQUAL,
                false,
                BIGINT,
                false);
        PlanNodeId sourceId = request.collectionSourceId();
        TaskId consumerTask = taskId(TASK_RETRY_SESSION, 0, 0);
        TaskId producerTask = taskId(TASK_RETRY_SESSION, 1, 0);
        service.registerTaskRuntimeConstraintWiring(producerTask, producer.getId(), _ -> {});
        service.addTaskRuntimeConstraintWiring(
                consumerTask,
                new RuntimeConstraintWiringReport(
                        ImmutableList.of(),
                        ImmutableList.of(),
                        ImmutableList.of(new RuntimeConstraintWiringReport.RemoteRequest(ImmutableList.of(producer.getId()), request)),
                        ImmutableList.of()));
        service.stageCannotScheduleMoreTasks(producerTask.stageId(), 0, ImmutableSet.of(0));
        if (taskStatusArrivesFirst) {
            service.taskFinished(producerTask, true, 1);
        }
        service.addTaskRuntimeConstraintWiring(
                producerTask,
                new RuntimeConstraintWiringReport(
                        ImmutableList.of(),
                        ImmutableList.of(new RuntimeConstraintWiringReport.Source(
                                sourceId,
                                ImmutableList.of(new CollectedConstraint(constraintId, 0)),
                                ImmutableList.of(BIGINT),
                                UNION_ALL_PARTITIONS)),
                        ImmutableList.of(),
                        ImmutableList.of(request)));
        service.stageCannotScheduleMoreTasks(producerTask.stageId(), 0, ImmutableSet.of(0));
        service.taskRuntimeConstraintWiringFinished(producerTask, producer.getId(), true);
        service.addTaskRuntimeConstraintWiring(
                consumerTask,
                new RuntimeConstraintWiringReport(
                        ImmutableList.of(),
                        ImmutableList.of(),
                        ImmutableList.of(new RuntimeConstraintWiringReport.RemoteRequest(ImmutableList.of(producer.getId()), request)),
                        ImmutableList.of()));
        RuntimeMembershipPayload payload = new RuntimeMembershipPayload(ImmutableList.of(singleValue(BIGINT, 11L)), ORDINARY);
        service.addTaskRuntimeConstraintContributions(
                producerTask,
                new RuntimeConstraintContributionBatch(
                        RuntimeConstraintProtocol.CURRENT_FORMAT_VERSION,
                        1,
                        0,
                        ImmutableList.of(new RuntimeConstraintContribution(
                                RuntimeConstraintHub.dynamicGroupId(sourceId),
                                RuntimeConstraintHub.dynamicBindingId(sourceId),
                                0,
                                0,
                                1,
                                payload))));

        service.taskFinished(producerTask, true, 1);

        assertThat(service.getRuntimeConstraintHub(TASK_RETRY_SESSION.getQueryId()).orElseThrow().get(constraintId).payload()).contains(payload);
    }

    @Test
    public void testMissingCollectionInOneSourceFragmentDisablesConstraint()
    {
        DynamicFilterService service = createDynamicFilterService();
        PlanFragment consumer = fragment("0", "consumer");
        PlanFragment firstProducer = fragment("1", "first_producer");
        PlanFragment secondProducer = fragment("2", "second_producer");
        service.registerQuery(SESSION, new SubPlan(consumer, ImmutableList.of(
                new SubPlan(firstProducer, ImmutableList.of()),
                new SubPlan(secondProducer, ImmutableList.of()))));

        RuntimeConstraintId constraintId = new RuntimeConstraintId("constraint");
        RuntimeConstraintRequest request = RuntimeConstraintRequest.collection(
                constraintId,
                0,
                ComparisonOperator.EQUAL,
                false,
                BIGINT,
                false);
        PlanNodeId sourceId = request.collectionSourceId();
        TaskId firstTask = taskId(1, 0, 0);
        TaskId secondTask = taskId(2, 0, 0);

        service.addTaskRuntimeConstraintWiring(
                taskId(0, 0),
                new RuntimeConstraintWiringReport(
                        ImmutableList.of(),
                        ImmutableList.of(),
                        ImmutableList.of(new RuntimeConstraintWiringReport.RemoteRequest(
                                ImmutableList.of(firstProducer.getId(), secondProducer.getId()),
                                request))));
        service.addTaskRuntimeConstraintWiring(
                firstTask,
                new RuntimeConstraintWiringReport(
                        ImmutableList.of(),
                        ImmutableList.of(new RuntimeConstraintWiringReport.Source(
                                sourceId,
                                ImmutableList.of(new CollectedConstraint(constraintId, 0)),
                                ImmutableList.of(BIGINT),
                                UNION_ALL_PARTITIONS)),
                        ImmutableList.of(),
                        ImmutableList.of(request)));
        service.addTaskRuntimeConstraintWiring(secondTask, RuntimeConstraintWiringReport.EMPTY);

        RuntimeMembershipPayload payload = new RuntimeMembershipPayload(ImmutableList.of(singleValue(BIGINT, 11L)), ORDINARY);
        service.addTaskRuntimeConstraintContributions(
                firstTask,
                new RuntimeConstraintContributionBatch(
                        RuntimeConstraintProtocol.CURRENT_FORMAT_VERSION,
                        1,
                        0,
                        ImmutableList.of(new RuntimeConstraintContribution(
                                RuntimeConstraintHub.dynamicGroupId(sourceId),
                                RuntimeConstraintHub.dynamicBindingId(sourceId),
                                firstTask.partitionId(),
                                firstTask.attemptId(),
                                1,
                                payload))));
        service.stageCannotScheduleMoreTasks(firstTask.stageId(), 0, ImmutableSet.of(0));
        service.stageCannotScheduleMoreTasks(secondTask.stageId(), 0, ImmutableSet.of(0));

        service.taskRuntimeConstraintWiringFinished(firstTask, firstProducer.getId(), true);
        RuntimeConstraintHub hub = service.getRuntimeConstraintHub(SESSION.getQueryId()).orElseThrow();
        assertThat(hub.get(constraintId).state()).isEqualTo(PENDING);

        service.taskRuntimeConstraintWiringFinished(secondTask, secondProducer.getId(), true);

        assertThat(hub.get(constraintId).state()).isEqualTo(DISABLED);
        assertThat(hub.get(constraintId).payload()).isEmpty();
    }

    @Test
    public void testMissingCollectionIsDisabledBeforeProducerStageIsSealed()
    {
        DynamicFilterService service = createDynamicFilterService();
        PlanFragment consumer = fragment("0", "consumer");
        PlanFragment producer = fragment("1", "producer");
        service.registerQuery(TASK_RETRY_SESSION, new SubPlan(consumer, ImmutableList.of(new SubPlan(producer, ImmutableList.of()))));

        RuntimeConstraintId constraintId = new RuntimeConstraintId("constraint");
        RuntimeConstraintRequest request = RuntimeConstraintRequest.collection(
                constraintId,
                0,
                ComparisonOperator.EQUAL,
                false,
                BIGINT,
                false);
        PlanNodeId sourceId = request.collectionSourceId();
        TaskId collectingTask = taskId(TASK_RETRY_SESSION, 1, 0, 0);
        TaskId missingTask = taskId(TASK_RETRY_SESSION, 1, 1, 0);

        service.addTaskRuntimeConstraintWiring(
                taskId(TASK_RETRY_SESSION, 0, 0),
                new RuntimeConstraintWiringReport(
                        ImmutableList.of(),
                        ImmutableList.of(),
                        ImmutableList.of(new RuntimeConstraintWiringReport.RemoteRequest(ImmutableList.of(producer.getId()), request)),
                        ImmutableList.of()));
        service.addTaskRuntimeConstraintWiring(
                collectingTask,
                new RuntimeConstraintWiringReport(
                        ImmutableList.of(),
                        ImmutableList.of(new RuntimeConstraintWiringReport.Source(
                                sourceId,
                                ImmutableList.of(new CollectedConstraint(constraintId, 0)),
                                ImmutableList.of(BIGINT),
                                UNION_ALL_PARTITIONS)),
                        ImmutableList.of(),
                        ImmutableList.of(request)));
        service.addTaskRuntimeConstraintWiring(missingTask, RuntimeConstraintWiringReport.EMPTY);

        RuntimeMembershipPayload payload = new RuntimeMembershipPayload(ImmutableList.of(singleValue(BIGINT, 11L)), ORDINARY);
        service.addTaskRuntimeConstraintContributions(
                collectingTask,
                new RuntimeConstraintContributionBatch(
                        RuntimeConstraintProtocol.CURRENT_FORMAT_VERSION,
                        1,
                        0,
                        ImmutableList.of(new RuntimeConstraintContribution(
                                RuntimeConstraintHub.dynamicGroupId(sourceId),
                                RuntimeConstraintHub.dynamicBindingId(sourceId),
                                collectingTask.partitionId(),
                                collectingTask.attemptId(),
                                1,
                                payload))));

        service.taskFinished(collectingTask, true, 1);
        service.taskFinished(missingTask, true, 0);
        service.taskRuntimeConstraintWiringFinished(collectingTask, producer.getId(), true);
        service.taskRuntimeConstraintWiringFinished(missingTask, producer.getId(), true);
        RuntimeConstraintHub hub = service.getRuntimeConstraintHub(TASK_RETRY_SESSION.getQueryId()).orElseThrow();
        assertThat(hub.get(constraintId).state()).isEqualTo(PENDING);

        service.stageCannotScheduleMoreTasks(collectingTask.stageId(), 0, ImmutableSet.of(0, 1));

        assertThat(hub.get(constraintId).state()).isEqualTo(DISABLED);
        assertThat(hub.get(constraintId).payload()).isEmpty();
    }

    private static RuntimeConstraintWiringReport wiring(PlanNodeId sourceId, RuntimeConstraintId constraintId)
    {
        return new RuntimeConstraintWiringReport(
                ImmutableList.of(new RuntimeConstraintWiringReport.ScanWiring(
                        sourceId,
                        ImmutableList.of(new RuntimeConstraintWiringReport.Binding(constraintId, COLUMN)))),
                ImmutableList.of(new RuntimeConstraintWiringReport.Source(
                        sourceId,
                        ImmutableList.of(new CollectedConstraint(constraintId, 0)),
                        ImmutableList.of(BIGINT),
                        EQUIVALENT_REPLICAS)));
    }

    private static TaskId taskId(int attempt)
    {
        return taskId(0, attempt);
    }

    private static TaskId taskId(int stage, int attempt)
    {
        return taskId(stage, 0, attempt);
    }

    private static TaskId taskId(int stage, int partition, int attempt)
    {
        return taskId(SESSION, stage, partition, attempt);
    }

    private static TaskId taskId(Session session, int stage, int attempt)
    {
        return taskId(session, stage, 0, attempt);
    }

    private static TaskId taskId(Session session, int stage, int partition, int attempt)
    {
        return new TaskId(new StageId(session.getQueryId(), stage), partition, attempt);
    }

    private static PlanFragment fragment()
    {
        return fragment("0", "values");
    }

    private static PlanFragment fragment(String fragmentId, String nodeId)
    {
        return fragment(fragmentId, nodeId, SOURCE_DISTRIBUTION);
    }

    private static PlanFragment fragment(String fragmentId, String nodeId, PartitioningHandle partitioning)
    {
        Symbol symbol = new Symbol(BIGINT, "value");
        ValuesNode root = new ValuesNode(new PlanNodeId(nodeId), ImmutableList.of(symbol), ImmutableList.<Expression>of());
        return new PlanFragment(
                new PlanFragmentId(fragmentId),
                root,
                ImmutableSet.of(symbol),
                partitioning,
                OptionalInt.empty(),
                ImmutableList.of(),
                new PartitioningScheme(Partitioning.create(SOURCE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(symbol)),
                OptionalInt.empty(),
                StatsAndCosts.empty(),
                ImmutableList.of(),
                ImmutableMap.of(),
                Optional.empty());
    }

    private static DynamicFilterService createDynamicFilterService()
    {
        return new DynamicFilterService(
                PLANNER_CONTEXT.getMetadata(),
                PLANNER_CONTEXT.getFunctionManager(),
                PLANNER_CONTEXT.getTypeOperators(),
                new DynamicFilterConfig());
    }
}
