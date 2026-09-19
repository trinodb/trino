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
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.execution.DynamicFilterConfig;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.Metadata;
import io.trino.operator.RetryPolicy;
import io.trino.operator.RuntimeConstraintRequest;
import io.trino.spi.QueryId;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.DynamicFilters;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.SubPlan;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintContributionBatch;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintDynamicFilter;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintHub;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintId;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintSnapshot;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintSubscription;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintSubscriptions;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintTransform;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintWiringReport;
import jakarta.inject.Inject;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.SystemSessionProperties.isEnableDynamicFiltering;
import static io.trino.SystemSessionProperties.isLegacyDynamicFiltering;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintPublicationState.CLOSED;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintPublicationState.DISABLED;
import static java.util.Objects.requireNonNull;

public class DynamicFilterService
{
    private final LegacyDynamicFilterService legacy;
    private final Metadata metadata;
    private final FunctionManager functionManager;
    private final TypeOperators typeOperators;
    private final DataSize maxSizePerFilter;
    private final Map<QueryId, RuntimeConstraintState> runtimeConstraintStates = new ConcurrentHashMap<>();
    private final Set<QueryId> wiringEnabledQueries = ConcurrentHashMap.newKeySet();

    @Inject
    public DynamicFilterService(Metadata metadata, FunctionManager functionManager, TypeOperators typeOperators, DynamicFilterConfig dynamicFilterConfig)
    {
        this.legacy = new LegacyDynamicFilterService(metadata, functionManager, typeOperators, dynamicFilterConfig);
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
        this.typeOperators = requireNonNull(typeOperators, "typeOperators is null");
        this.maxSizePerFilter = requireNonNull(dynamicFilterConfig, "dynamicFilterConfig is null").getMaxSizePerFilter();
    }

    public void registerQuery(Session session, PlanNode queryPlan, SubPlan fragmentedPlan)
    {
        if (isLegacyDynamicFiltering(session)) {
            legacy.registerQuery(session, queryPlan, fragmentedPlan);
            return;
        }
        registerQuery(session, fragmentedPlan);
    }

    public void registerQuery(Session session, SubPlan fragmentedPlan)
    {
        if (isEnableDynamicFiltering(session)) {
            wiringEnabledQueries.add(session.getQueryId());
        }
        RuntimeConstraintState state = RuntimeConstraintState.create(
                fragmentedPlan,
                maxSizePerFilter.toBytes(),
                getRetryPolicy(session) == RetryPolicy.TASK,
                new RuntimeConstraintTransform.Context(metadata, functionManager, typeOperators, session));
        RuntimeConstraintState existing = runtimeConstraintStates.putIfAbsent(session.getQueryId(), state);
        if (existing != null) {
            state.close();
            checkState(existing.hub().getGeneration() == 0, "Query %s runtime constraint generation is already registered", session.getQueryId());
        }
    }

    public void registerQueryRetry(QueryId queryId, int attemptId)
    {
        if (!runtimeConstraintStates.containsKey(queryId)) {
            legacy.registerQueryRetry(queryId, attemptId);
            return;
        }
        runtimeConstraintStates.computeIfPresent(queryId, (_, state) -> {
            RuntimeConstraintState replacement = state.forQueryRetry(attemptId);
            state.close();
            return replacement;
        });
    }

    public DynamicFiltersStats getDynamicFilteringStats(QueryId queryId)
    {
        if (!runtimeConstraintStates.containsKey(queryId)) {
            return legacy.getDynamicFilteringStats(queryId);
        }
        RuntimeConstraintState state = runtimeConstraintStates.get(queryId);
        RuntimeConstraintHub hub = state == null ? null : state.hub();
        if (hub == null) {
            return DynamicFiltersStats.EMPTY;
        }

        List<RuntimeConstraintHub.ConstraintStatistics> statistics = hub.getStatistics();
        ImmutableList.Builder<DynamicFilterDomainStats> domainStatistics = ImmutableList.builder();
        int completed = 0;
        for (RuntimeConstraintHub.ConstraintStatistics constraint : statistics) {
            if (constraint.summary().isEmpty()) {
                continue;
            }
            completed++;
            domainStatistics.add(new DynamicFilterDomainStats(
                    new DynamicFilterId(constraint.constraintId().toString()),
                    constraint.summary().orElseThrow().toString(2),
                    constraint.collectionDuration()));
        }
        return new DynamicFiltersStats(
                domainStatistics.build(),
                (int) statistics.stream().filter(RuntimeConstraintHub.ConstraintStatistics::lazy).count(),
                (int) statistics.stream().filter(RuntimeConstraintHub.ConstraintStatistics::replicated).count(),
                statistics.size(),
                completed);
    }

    public void removeQuery(QueryId queryId)
    {
        legacy.removeQuery(queryId);
        wiringEnabledQueries.remove(queryId);
        RuntimeConstraintState state = runtimeConstraintStates.remove(queryId);
        if (state != null) {
            state.close();
        }
    }

    public DynamicFilter createDynamicFilter(QueryId queryId, List<DynamicFilters.Descriptor> descriptors, Map<Symbol, ColumnHandle> columns)
    {
        return legacy.createDynamicFilter(queryId, descriptors, columns);
    }

    public void registerDynamicFilterConsumer(QueryId queryId, int attemptId, Set<DynamicFilterId> ids, Consumer<Map<DynamicFilterId, Domain>> consumer)
    {
        legacy.registerDynamicFilterConsumer(queryId, attemptId, ids, consumer);
    }

    public void addTaskDynamicFilters(TaskId taskId, Map<DynamicFilterId, Domain> domains)
    {
        legacy.addTaskDynamicFilters(taskId, domains);
    }

    public long getRuntimeConstraintGeneration(QueryId queryId)
    {
        RuntimeConstraintState state = runtimeConstraintStates.get(requireNonNull(queryId, "queryId is null"));
        RuntimeConstraintHub hub = state == null ? null : state.hub();
        return hub == null ? 0 : hub.getGeneration();
    }

    @VisibleForTesting
    Optional<RuntimeConstraintHub> getRuntimeConstraintHub(QueryId queryId)
    {
        return Optional.ofNullable(runtimeConstraintStates.get(queryId)).map(RuntimeConstraintState::hub);
    }

    public boolean isCollectingTaskNeeded(QueryId queryId, PlanFragment plan)
    {
        if (!runtimeConstraintStates.containsKey(queryId)) {
            return legacy.isCollectingTaskNeeded(queryId, plan);
        }
        RuntimeConstraintState state = runtimeConstraintStates.get(queryId);
        RuntimeConstraintHub hub = state == null ? null : state.hub();
        return hub != null && hub.isCollectingTaskNeeded(plan.getId());
    }

    public boolean isRuntimeConstraintWiringEnabled(QueryId queryId)
    {
        return wiringEnabledQueries.contains(queryId);
    }

    public boolean isStageSchedulingNeededToCollectDynamicFilters(QueryId queryId, PlanFragment plan)
    {
        if (!runtimeConstraintStates.containsKey(queryId)) {
            return legacy.isStageSchedulingNeededToCollectDynamicFilters(queryId, plan);
        }
        RuntimeConstraintState state = runtimeConstraintStates.get(queryId);
        RuntimeConstraintHub hub = state == null ? null : state.hub();
        return hub != null && hub.isStageSchedulingNeeded(plan.getId());
    }

    public void unblockStageDynamicFilters(QueryId queryId, int attemptId, PlanFragment plan)
    {
        if (!runtimeConstraintStates.containsKey(queryId)) {
            legacy.unblockStageDynamicFilters(queryId, attemptId, plan);
            return;
        }
        RuntimeConstraintState state = runtimeConstraintStates.get(queryId);
        RuntimeConstraintHub hub = state == null ? null : state.hub();
        if (hub != null && hub.acceptsTaskAttempt(attemptId)) {
            hub.unblockStageDynamicFilters(plan.getId());
        }
    }

    public CompletableFuture<DynamicFilter> discoverRuntimeConstraintDynamicFilter(
            Session session,
            PlanNodeId scanId,
            List<ColumnHandle> columns)
    {
        RuntimeConstraintState state = runtimeConstraintStates.get(session.getQueryId());
        if (state == null) {
            return CompletableFuture.completedFuture(DynamicFilter.EMPTY);
        }
        return state.discovery().get(scanId).thenApply(bindings -> {
            if (bindings.isEmpty()) {
                return DynamicFilter.EMPTY;
            }
            return RuntimeConstraintDynamicFilter.create(state.subscriptions(), bindings, columns);
        });
    }

    public void addTaskRuntimeConstraintWiring(TaskId taskId, RuntimeConstraintWiringReport report)
    {
        RuntimeConstraintState state = runtimeConstraintStates.get(taskId.queryId());
        if (state == null || !state.hub().acceptsTaskAttempt(taskId.attemptId())) {
            return;
        }
        // Source registration can replay buffered contributions and publish a final domain.
        // Disable incomplete collection before any source in this report can publish.
        state.broker().reject(state.coordinator().resolveFragmentId(taskId), report.rejectedOutputRequests());
        state.broker().addTaskSources(taskId, report.sources());
        report.sources().forEach(source -> state.hub().registerSource(taskId, source));
        report.scans().forEach(scan -> state.hub().registerConsumers(scan.bindings()));
        Set<RuntimeConstraintId> roots = new LinkedHashSet<>();
        report.subscriptions().forEach(subscription -> roots.add(subscription.constraintId()));
        report.subscriptionInputs().forEach(input -> roots.add(input.constraintId()));
        report.scans().forEach(scan -> scan.bindings().forEach(binding -> roots.add(binding.constraintId())));
        for (RuntimeConstraintId root : roots) {
            state.hub().registerConsumer(root);
            state.subscriptions().registerInput(new RuntimeConstraintSubscription.Input(root, root), () -> state.hub().waitForUpdate(root, 0));
        }
        report.subscriptions().forEach(state.subscriptions()::register);
        RuntimeConstraintWiringReport resolved = new RuntimeConstraintWiringReport(
                report.scans().stream()
                        .map(scan -> new RuntimeConstraintWiringReport.ScanWiring(scan.scanId(), scan.bindings().stream()
                                .map(binding -> state.subscriptions().registerBinding("scan " + scan.scanId(), binding))
                                .toList()))
                        .toList(),
                report.sources(),
                report.remoteRequests(),
                report.appliedOutputRequests(),
                report.rejectedOutputRequests(),
                report.subscriptions(),
                report.subscriptionInputs());
        state.coordinator().accept(taskId, resolved)
                .forEach(scan -> state.discovery().complete(scan.scanId(), scan.bindings()));
    }

    public void registerTaskRuntimeConstraintWiring(
            TaskId taskId,
            PlanFragmentId fragmentId,
            Consumer<List<RuntimeConstraintRequest>> consumer)
    {
        RuntimeConstraintState state = runtimeConstraintStates.get(taskId.queryId());
        if (state != null && state.hub().acceptsTaskAttempt(taskId.attemptId())) {
            state.broker().register(taskId, fragmentId, consumer);
        }
    }

    public void unregisterTaskRuntimeConstraintWiring(TaskId taskId, PlanFragmentId fragmentId)
    {
        RuntimeConstraintState state = runtimeConstraintStates.get(taskId.queryId());
        if (state != null) {
            state.broker().unregister(taskId, fragmentId);
        }
    }

    public void taskRuntimeConstraintWiringFinished(TaskId taskId, PlanFragmentId fragmentId, boolean successful)
    {
        RuntimeConstraintState state = runtimeConstraintStates.get(taskId.queryId());
        if (state != null && state.hub().acceptsTaskAttempt(taskId.attemptId()) && state.broker().taskFinished(taskId, fragmentId, successful)) {
            state.coordinator().requestsChanged()
                    .forEach(scan -> state.discovery().complete(scan.scanId(), scan.bindings()));
        }
    }

    public void addTaskRuntimeConstraintContributions(TaskId taskId, RuntimeConstraintContributionBatch contributions)
    {
        RuntimeConstraintState state = runtimeConstraintStates.get(taskId.queryId());
        RuntimeConstraintHub hub = state == null ? null : state.hub();
        if (hub != null) {
            hub.acceptContributions(taskId, contributions.withGeneration(hub.getGeneration()));
        }
    }

    public void taskFinished(TaskId taskId, boolean successful, long finalContributionSequence)
    {
        RuntimeConstraintState state = runtimeConstraintStates.get(taskId.queryId());
        RuntimeConstraintHub hub = state == null ? null : state.hub();
        if (hub != null) {
            hub.taskFinished(taskId, successful, finalContributionSequence);
        }
    }

    public void registerRuntimeConstraintConsumer(
            QueryId queryId,
            int attemptId,
            long generation,
            Set<RuntimeConstraintId> constraintIds,
            Consumer<List<RuntimeConstraintSnapshot>> consumer)
    {
        requireNonNull(constraintIds, "constraintIds is null");
        requireNonNull(consumer, "consumer is null");
        RuntimeConstraintState state = runtimeConstraintStates.get(queryId);
        RuntimeConstraintHub hub = state == null ? null : state.hub();
        if (hub == null || !hub.acceptsTaskAttempt(attemptId)) {
            consumer.accept(constraintIds.stream()
                    .map(constraintId -> RuntimeConstraintSnapshot.terminal(constraintId, generation, 1, CLOSED))
                    .collect(toImmutableList()));
            return;
        }
        for (RuntimeConstraintId constraintId : constraintIds) {
            state.subscriptions().waitForUpdate(constraintId, 0)
                    .whenComplete((snapshot, failure) -> consumer.accept(ImmutableList.of(failure == null
                            ? snapshot.withGeneration(generation)
                            : RuntimeConstraintSnapshot.terminal(constraintId, generation, 1, DISABLED))));
        }
    }

    public void stageCannotScheduleMoreTasks(StageId stageId, int attemptId, int numberOfTasks)
    {
        if (!runtimeConstraintStates.containsKey(stageId.queryId())) {
            legacy.stageCannotScheduleMoreTasks(stageId, attemptId, numberOfTasks);
            return;
        }
        stageCannotScheduleMoreTasks(stageId, attemptId, IntStream.range(0, numberOfTasks).boxed().collect(toImmutableSet()));
    }

    public void stageCannotScheduleMoreTasks(StageId stageId, int attemptId, Set<Integer> logicalPartitionIds)
    {
        if (!runtimeConstraintStates.containsKey(stageId.queryId())) {
            legacy.stageCannotScheduleMoreTasks(stageId, attemptId, logicalPartitionIds.size());
            return;
        }
        RuntimeConstraintState state = runtimeConstraintStates.get(stageId.queryId());
        RuntimeConstraintHub hub = state == null ? null : state.hub();
        if (hub != null && hub.acceptsTaskAttempt(attemptId)) {
            PlanFragmentId fragmentId = state.coordinator().resolveFragmentId(stageId);
            Set<Integer> partitions = requireNonNull(logicalPartitionIds, "logicalPartitionIds is null");
            if (state.broker().noMoreTasks(fragmentId, partitions)) {
                state.coordinator().requestsChanged()
                        .forEach(scan -> state.discovery().complete(scan.scanId(), scan.bindings()));
            }
            hub.sealProducerStage(fragmentId, partitions);
        }
    }

    public static class DynamicFiltersStats
    {
        public static final DynamicFiltersStats EMPTY = new DynamicFiltersStats(ImmutableList.of(), 0, 0, 0, 0);

        private final List<DynamicFilterDomainStats> dynamicFilterDomainStats;
        private final int lazyDynamicFilters;
        private final int replicatedDynamicFilters;
        private final int totalDynamicFilters;
        private final int dynamicFiltersCompleted;

        @JsonCreator
        public DynamicFiltersStats(
                @JsonProperty("dynamicFilterDomainStats") List<DynamicFilterDomainStats> dynamicFilterDomainStats,
                @JsonProperty("lazyDynamicFilters") int lazyDynamicFilters,
                @JsonProperty("replicatedDynamicFilters") int replicatedDynamicFilters,
                @JsonProperty("totalDynamicFilters") int totalDynamicFilters,
                @JsonProperty("dynamicFiltersCompleted") int dynamicFiltersCompleted)
        {
            this.dynamicFilterDomainStats = requireNonNull(dynamicFilterDomainStats, "dynamicFilterDomainStats is null");
            this.lazyDynamicFilters = lazyDynamicFilters;
            this.replicatedDynamicFilters = replicatedDynamicFilters;
            this.totalDynamicFilters = totalDynamicFilters;
            this.dynamicFiltersCompleted = dynamicFiltersCompleted;
        }

        @JsonProperty
        public List<DynamicFilterDomainStats> getDynamicFilterDomainStats()
        {
            return dynamicFilterDomainStats;
        }

        @JsonProperty
        public int getLazyDynamicFilters()
        {
            return lazyDynamicFilters;
        }

        @JsonProperty
        public int getReplicatedDynamicFilters()
        {
            return replicatedDynamicFilters;
        }

        @JsonProperty
        public int getTotalDynamicFilters()
        {
            return totalDynamicFilters;
        }

        @JsonProperty
        public int getDynamicFiltersCompleted()
        {
            return dynamicFiltersCompleted;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DynamicFiltersStats that = (DynamicFiltersStats) o;
            return lazyDynamicFilters == that.lazyDynamicFilters &&
                    replicatedDynamicFilters == that.replicatedDynamicFilters &&
                    totalDynamicFilters == that.totalDynamicFilters &&
                    dynamicFiltersCompleted == that.dynamicFiltersCompleted &&
                    Objects.equals(dynamicFilterDomainStats, that.dynamicFilterDomainStats);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(dynamicFilterDomainStats, lazyDynamicFilters, replicatedDynamicFilters, totalDynamicFilters, dynamicFiltersCompleted);
        }
    }

    private record RuntimeConstraintState(
            SubPlan fragmentedPlan,
            RuntimeConstraintHub hub,
            ScanDiscovery discovery,
            RuntimeConstraintWiringBroker broker,
            RuntimeConstraintWiringCoordinator coordinator,
            RuntimeConstraintSubscriptions subscriptions,
            RuntimeConstraintTransform.Context transformationContext,
            long maxSubscriptionBytes)
            implements AutoCloseable
    {
        private static RuntimeConstraintState create(SubPlan fragmentedPlan, long maxRetainedBytes, boolean taskRetriesEnabled, RuntimeConstraintTransform.Context context)
        {
            RuntimeConstraintHub hub = new RuntimeConstraintHub(0, maxRetainedBytes, taskRetriesEnabled, true);
            return create(fragmentedPlan, hub, maxRetainedBytes, context);
        }

        private static RuntimeConstraintState create(SubPlan fragmentedPlan, RuntimeConstraintHub hub, long maxRetainedBytes, RuntimeConstraintTransform.Context context)
        {
            RuntimeConstraintWiringBroker broker = new RuntimeConstraintWiringBroker(hub);
            return new RuntimeConstraintState(
                    fragmentedPlan,
                    hub,
                    new ScanDiscovery(),
                    broker,
                    new RuntimeConstraintWiringCoordinator(fragmentedPlan, broker, hub),
                    new RuntimeConstraintSubscriptions(hub.getGeneration(), maxRetainedBytes, context, _ -> true, hub::waitForInitialUnblock),
                    context,
                    maxRetainedBytes);
        }

        private RuntimeConstraintState forQueryRetry(int attemptId)
        {
            return create(fragmentedPlan, hub.forQueryRetry(attemptId), maxSubscriptionBytes, transformationContext);
        }

        @Override
        public void close()
        {
            discovery.close();
            subscriptions.close();
            hub.close();
        }
    }

    private static final class RuntimeConstraintWiringBroker
    {
        private final RuntimeConstraintHub hub;
        private final Map<PlanFragmentId, Set<RuntimeConstraintRequest>> requests = new HashMap<>();
        private final Map<PlanFragmentId, Map<TaskId, Consumer<List<RuntimeConstraintRequest>>>> consumers = new HashMap<>();
        private final Map<PlanFragmentId, Set<Integer>> expectedPartitions = new HashMap<>();
        private final Map<PlanFragmentId, Set<Integer>> successfulPartitions = new HashMap<>();
        private final Map<TaskId, Set<RuntimeConstraintId>> taskSources = new HashMap<>();
        private final Map<PlanFragmentId, Map<RuntimeConstraintId, Set<Integer>>> successfulSourcePartitions = new HashMap<>();
        private final Set<PlanFragmentId> retiringFragments = new HashSet<>();
        private final Set<PlanFragmentId> finishedFragments = new HashSet<>();

        private RuntimeConstraintWiringBroker(RuntimeConstraintHub hub)
        {
            this.hub = requireNonNull(hub, "hub is null");
        }

        public void add(PlanFragmentId fragmentId, RuntimeConstraintRequest request)
        {
            List<Consumer<List<RuntimeConstraintRequest>>> listeners;
            boolean rejected;
            synchronized (this) {
                if (retiringFragments.contains(fragmentId) || finishedFragments.contains(fragmentId)) {
                    rejected = request.isCollection() && !hasCompleteCollectionCoverage(fragmentId, request.constraintId());
                    listeners = ImmutableList.of();
                }
                else {
                    if (!requests.computeIfAbsent(fragmentId, _ -> new HashSet<>()).add(request)) {
                        return;
                    }
                    listeners = ImmutableList.copyOf(consumers.getOrDefault(fragmentId, Map.of()).values());
                    rejected = false;
                }
            }
            if (rejected) {
                hub.disableIfPending(request.constraintId());
                return;
            }
            listeners.forEach(listener -> listener.accept(ImmutableList.of(request)));
        }

        public synchronized void addTaskSources(TaskId taskId, List<RuntimeConstraintWiringReport.Source> sources)
        {
            Set<RuntimeConstraintId> sourceConstraints = sources.stream()
                    .flatMap(source -> source.constraintIds().stream())
                    .collect(toImmutableSet());
            if (!sourceConstraints.isEmpty()) {
                taskSources.computeIfAbsent(taskId, _ -> new HashSet<>()).addAll(sourceConstraints);
            }
        }

        public void register(TaskId taskId, PlanFragmentId fragmentId, Consumer<List<RuntimeConstraintRequest>> consumer)
        {
            List<RuntimeConstraintRequest> pending;
            synchronized (this) {
                consumers.computeIfAbsent(fragmentId, _ -> new HashMap<>()).put(taskId, consumer);
                pending = ImmutableList.copyOf(requests.getOrDefault(fragmentId, Set.of()));
            }
            if (!pending.isEmpty()) {
                consumer.accept(pending);
            }
        }

        public synchronized void unregister(TaskId taskId, PlanFragmentId fragmentId)
        {
            unregisterLocked(taskId, fragmentId);
        }

        private void unregisterLocked(TaskId taskId, PlanFragmentId fragmentId)
        {
            Map<TaskId, Consumer<List<RuntimeConstraintRequest>>> fragmentConsumers = consumers.get(fragmentId);
            if (fragmentConsumers != null) {
                fragmentConsumers.remove(taskId);
            }
        }

        public boolean noMoreTasks(PlanFragmentId fragmentId, Set<Integer> logicalPartitionIds)
        {
            synchronized (this) {
                expectedPartitions.put(fragmentId, Set.copyOf(logicalPartitionIds));
            }
            return finishFragmentIfComplete(fragmentId);
        }

        public boolean taskFinished(TaskId taskId, PlanFragmentId fragmentId, boolean successful)
        {
            synchronized (this) {
                unregisterLocked(taskId, fragmentId);
                if (successful) {
                    successfulPartitions.computeIfAbsent(fragmentId, _ -> new HashSet<>()).add(taskId.partitionId());
                    for (RuntimeConstraintId constraintId : taskSources.getOrDefault(taskId, Set.of())) {
                        successfulSourcePartitions
                                .computeIfAbsent(fragmentId, _ -> new HashMap<>())
                                .computeIfAbsent(constraintId, _ -> new HashSet<>())
                                .add(taskId.partitionId());
                    }
                }
                taskSources.remove(taskId);
            }
            return finishFragmentIfComplete(fragmentId);
        }

        private boolean finishFragmentIfComplete(PlanFragmentId fragmentId)
        {
            Set<RuntimeConstraintRequest> removed;
            Set<RuntimeConstraintId> incompleteCollections;
            synchronized (this) {
                Set<Integer> expected = expectedPartitions.get(fragmentId);
                if (expected == null ||
                        !successfulPartitions.getOrDefault(fragmentId, Set.of()).containsAll(expected) ||
                        finishedFragments.contains(fragmentId) ||
                        !retiringFragments.add(fragmentId)) {
                    return false;
                }
                removed = Set.copyOf(requests.getOrDefault(fragmentId, Set.of()));
                incompleteCollections = removed.stream()
                        .filter(RuntimeConstraintRequest::isCollection)
                        .map(RuntimeConstraintRequest::constraintId)
                        .filter(constraintId -> !hasCompleteCollectionCoverage(fragmentId, constraintId))
                        .collect(toImmutableSet());
            }

            incompleteCollections.forEach(hub::disableIfPending);
            synchronized (this) {
                requests.remove(fragmentId);
                retiringFragments.remove(fragmentId);
                finishedFragments.add(fragmentId);
            }
            return !removed.isEmpty();
        }

        private boolean hasCompleteCollectionCoverage(PlanFragmentId fragmentId, RuntimeConstraintId constraintId)
        {
            Set<Integer> expected = expectedPartitions.get(fragmentId);
            return expected != null && successfulSourcePartitions
                    .getOrDefault(fragmentId, Map.of())
                    .getOrDefault(constraintId, Set.of())
                    .containsAll(expected);
        }

        public boolean reject(PlanFragmentId fragmentId, List<RuntimeConstraintRequest> rejectedRequests)
        {
            Set<RuntimeConstraintId> rejected = requireNonNull(rejectedRequests, "rejectedRequests is null").stream()
                    .filter(RuntimeConstraintRequest::isCollection)
                    .map(RuntimeConstraintRequest::constraintId)
                    .collect(toImmutableSet());
            if (rejected.isEmpty()) {
                return false;
            }
            rejected.forEach(hub::disableIfPending);
            synchronized (this) {
                Set<RuntimeConstraintRequest> requestsForFragment = requests.get(fragmentId);
                if (requestsForFragment != null) {
                    requestsForFragment.removeIf(request -> rejected.contains(request.constraintId()));
                }
            }
            return true;
        }

        public synchronized Set<RuntimeConstraintRequest> getRequests(PlanFragmentId fragmentId)
        {
            return Set.copyOf(requests.getOrDefault(fragmentId, Set.of()));
        }

        public synchronized Set<PlanFragmentId> getRequestedFragments(RuntimeConstraintId constraintId)
        {
            return requests.entrySet().stream()
                    .filter(entry -> entry.getValue().stream().anyMatch(request -> request.constraintId().equals(constraintId)))
                    .map(Map.Entry::getKey)
                    .collect(toImmutableSet());
        }
    }

    private static final class RuntimeConstraintWiringCoordinator
    {
        private final Set<PlanFragmentId> expectedFragments;
        private final Map<PlanFragmentId, Set<PlanFragmentId>> prerequisites;
        private final RuntimeConstraintWiringBroker broker;
        private final RuntimeConstraintHub hub;
        private final Map<PlanFragmentId, RuntimeConstraintWiringReport> reports = new HashMap<>();
        private final Set<PlanNodeId> completedScans = new HashSet<>();
        private final Set<RuntimeConstraintId> completedSourceRegistrations = new HashSet<>();

        private RuntimeConstraintWiringCoordinator(SubPlan fragmentedPlan, RuntimeConstraintWiringBroker broker, RuntimeConstraintHub hub)
        {
            requireNonNull(fragmentedPlan, "fragmentedPlan is null");
            Map<PlanFragmentId, Set<PlanFragmentId>> prerequisites = new HashMap<>();
            collectPrerequisites(fragmentedPlan, Set.of(), prerequisites, hub.getQueryAttempt() > 0);
            this.prerequisites = Map.copyOf(prerequisites);
            this.expectedFragments = this.prerequisites.keySet();
            this.broker = requireNonNull(broker, "broker is null");
            this.hub = requireNonNull(hub, "hub is null");
        }

        private static void collectPrerequisites(
                SubPlan plan,
                Set<PlanFragmentId> ancestors,
                Map<PlanFragmentId, Set<PlanFragmentId>> prerequisites,
                boolean queryRetry)
        {
            Set<PlanFragmentId> path = new HashSet<>(ancestors);
            // Coordinator-only tasks survive a query retry. Their old-generation constraints are closed,
            // and they cannot initialize wiring for the replacement distributed tasks.
            if (!queryRetry || !plan.getFragment().getPartitioning().isCoordinatorOnly()) {
                path.add(plan.getFragment().getId());
                prerequisites.put(plan.getFragment().getId(), Set.copyOf(path));
            }
            plan.getChildren().forEach(child -> collectPrerequisites(child, path, prerequisites, queryRetry));
        }

        public List<RuntimeConstraintWiringReport.ScanWiring> accept(TaskId taskId, RuntimeConstraintWiringReport report)
        {
            report.remoteRequests().forEach(request -> request.sourceFragmentIds().forEach(
                    sourceFragmentId -> broker.add(sourceFragmentId, request.request())));
            WiringUpdate update;
            synchronized (this) {
                reports.merge(resolveFragmentId(taskId), report, RuntimeConstraintWiringCoordinator::mergeReports);
                update = evaluateLocked();
            }
            return complete(update);
        }

        private static RuntimeConstraintWiringReport mergeReports(RuntimeConstraintWiringReport first, RuntimeConstraintWiringReport second)
        {
            Map<PlanNodeId, Set<RuntimeConstraintWiringReport.Binding>> scanBindings = new LinkedHashMap<>();
            first.scans().forEach(scan -> scanBindings.computeIfAbsent(scan.scanId(), _ -> new LinkedHashSet<>()).addAll(scan.bindings()));
            second.scans().forEach(scan -> scanBindings.computeIfAbsent(scan.scanId(), _ -> new LinkedHashSet<>()).addAll(scan.bindings()));

            return new RuntimeConstraintWiringReport(
                    scanBindings.entrySet().stream()
                            .map(entry -> new RuntimeConstraintWiringReport.ScanWiring(entry.getKey(), ImmutableList.copyOf(entry.getValue())))
                            .toList(),
                    union(first.sources(), second.sources()),
                    union(first.remoteRequests(), second.remoteRequests()),
                    union(first.appliedOutputRequests(), second.appliedOutputRequests()),
                    union(first.rejectedOutputRequests(), second.rejectedOutputRequests()),
                    union(first.subscriptions(), second.subscriptions()),
                    union(first.subscriptionInputs(), second.subscriptionInputs()));
        }

        private static <T> List<T> union(List<T> first, List<T> second)
        {
            Set<T> values = new LinkedHashSet<>(first);
            values.addAll(second);
            return ImmutableList.copyOf(values);
        }

        public List<RuntimeConstraintWiringReport.ScanWiring> requestsChanged()
        {
            WiringUpdate update;
            synchronized (this) {
                update = evaluateLocked();
            }
            return complete(update);
        }

        private WiringUpdate evaluateLocked()
        {
            List<PlanFragmentId> readyFragments = expectedFragments.stream()
                    .filter(expectedFragment -> reports.keySet().containsAll(prerequisites.get(expectedFragment)))
                    .filter(expectedFragment -> prerequisites.get(expectedFragment).stream().allMatch(requiredFragment ->
                            reportFor(requiredFragment).appliedOutputRequests().containsAll(broker.getRequests(requiredFragment))))
                    .toList();
            List<RuntimeConstraintWiringReport.ScanWiring> readyScans = readyFragments.stream()
                    .flatMap(readyFragment -> reportFor(readyFragment).scans().stream())
                    .filter(scan -> !completedScans.contains(scan.scanId()))
                    .toList();
            readyScans.forEach(scan -> completedScans.add(scan.scanId()));
            return new WiringUpdate(readyScans, findCompletedSourceRegistrations());
        }

        private List<RuntimeConstraintWiringReport.ScanWiring> complete(WiringUpdate update)
        {
            update.sourceRegistrations().forEach(registration -> hub.completeSourceRegistration(registration.constraintIds(), registration.sourceFragments()));
            return update.readyScans();
        }

        private List<SourceRegistration> findCompletedSourceRegistrations()
        {
            Map<RuntimeConstraintId, Set<PlanFragmentId>> sourceFragments = new HashMap<>();
            reports.forEach((sourceFragment, sourceReport) -> sourceReport.sources().forEach(source -> source.constraintIds().forEach(constraintId ->
                    sourceFragments.computeIfAbsent(constraintId, _ -> new HashSet<>()).add(sourceFragment))));
            ImmutableList.Builder<SourceRegistration> completed = ImmutableList.builder();
            sourceFragments.forEach((constraintId, fragments) -> {
                if (completedSourceRegistrations.contains(constraintId)) {
                    return;
                }
                Set<PlanFragmentId> requestedFragments = broker.getRequestedFragments(constraintId);
                boolean routingComplete = requestedFragments.stream().allMatch(requestedFragment -> {
                    RuntimeConstraintWiringReport requestedReport = reports.get(requestedFragment);
                    return requestedReport != null && requestedReport.appliedOutputRequests().containsAll(broker.getRequests(requestedFragment));
                });
                if (routingComplete) {
                    completedSourceRegistrations.add(constraintId);
                    completed.add(new SourceRegistration(Set.of(constraintId), Set.copyOf(fragments)));
                }
            });
            return completed.build();
        }

        private record SourceRegistration(Set<RuntimeConstraintId> constraintIds, Set<PlanFragmentId> sourceFragments) {}

        private record WiringUpdate(List<RuntimeConstraintWiringReport.ScanWiring> readyScans, List<SourceRegistration> sourceRegistrations) {}

        private RuntimeConstraintWiringReport reportFor(PlanFragmentId fragmentId)
        {
            return requireNonNull(reports.get(fragmentId), "fragment wiring report is missing");
        }

        private PlanFragmentId resolveFragmentId(TaskId taskId)
        {
            return resolveFragmentId(taskId.stageId());
        }

        private PlanFragmentId resolveFragmentId(StageId stageId)
        {
            PlanFragmentId stageFragmentId = new PlanFragmentId(Integer.toString(stageId.id()));
            if (expectedFragments.contains(stageFragmentId)) {
                return stageFragmentId;
            }
            if (expectedFragments.size() == 1) {
                return expectedFragments.iterator().next();
            }
            throw new IllegalArgumentException("stage does not identify a plan fragment: " + stageId);
        }
    }

    private static final class ScanDiscovery
    {
        private final Map<PlanNodeId, CompletableFuture<List<RuntimeConstraintWiringReport.Binding>>> scans = new ConcurrentHashMap<>();

        public CompletableFuture<List<RuntimeConstraintWiringReport.Binding>> get(PlanNodeId scanId)
        {
            return scans.computeIfAbsent(scanId, _ -> new CompletableFuture<>());
        }

        public void complete(PlanNodeId scanId, List<RuntimeConstraintWiringReport.Binding> bindings)
        {
            get(scanId).complete(ImmutableList.copyOf(bindings));
        }

        public void close()
        {
            scans.values().forEach(future -> future.complete(ImmutableList.of()));
        }
    }

    public static class DynamicFilterDomainStats
    {
        private final DynamicFilterId dynamicFilterId;
        private final String simplifiedDomain;
        private final Optional<Duration> collectionDuration;

        @VisibleForTesting
        DynamicFilterDomainStats(DynamicFilterId dynamicFilterId, String simplifiedDomain)
        {
            this(dynamicFilterId, simplifiedDomain, Optional.empty());
        }

        @JsonCreator
        public DynamicFilterDomainStats(
                @JsonProperty("dynamicFilterId") DynamicFilterId dynamicFilterId,
                @JsonProperty("simplifiedDomain") String simplifiedDomain,
                @JsonProperty("collectionDuration") Optional<Duration> collectionDuration)
        {
            this.dynamicFilterId = requireNonNull(dynamicFilterId, "dynamicFilterId is null");
            this.simplifiedDomain = requireNonNull(simplifiedDomain, "simplifiedDomain is null");
            this.collectionDuration = requireNonNull(collectionDuration, "collectionDuration is null");
        }

        @JsonProperty
        public DynamicFilterId getDynamicFilterId()
        {
            return dynamicFilterId;
        }

        @JsonProperty
        public String getSimplifiedDomain()
        {
            return simplifiedDomain;
        }

        @JsonProperty
        public Optional<Duration> getCollectionDuration()
        {
            return collectionDuration;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DynamicFilterDomainStats stats = (DynamicFilterDomainStats) o;
            return Objects.equals(dynamicFilterId, stats.dynamicFilterId) &&
                    Objects.equals(simplifiedDomain, stats.simplifiedDomain);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(dynamicFilterId, simplifiedDomain);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("dynamicFilterId", dynamicFilterId)
                    .add("simplifiedDomain", simplifiedDomain)
                    .add("collectionDuration", collectionDuration)
                    .toString();
        }
    }
}
