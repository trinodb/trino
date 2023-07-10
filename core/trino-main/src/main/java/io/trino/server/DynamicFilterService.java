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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.execution.DynamicFilterConfig;
import io.trino.execution.SqlQueryExecution;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.Metadata;
import io.trino.operator.RetryPolicy;
import io.trino.operator.join.JoinUtils;
import io.trino.spi.QueryId;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.DynamicFilters;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.SubPlan;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.DynamicFilterSourceNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.google.common.base.Functions.identity;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Sets.intersection;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static com.google.common.collect.Sets.union;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.addSuccessCallback;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.airlift.concurrent.MoreFutures.unmodifiableFuture;
import static io.airlift.concurrent.MoreFutures.whenAnyComplete;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.SystemSessionProperties.isEnableLargeDynamicFilters;
import static io.trino.spi.connector.DynamicFilter.EMPTY;
import static io.trino.spi.predicate.Domain.union;
import static io.trino.sql.DynamicFilters.extractDynamicFilters;
import static io.trino.sql.DynamicFilters.extractSourceSymbols;
import static io.trino.sql.planner.DomainCoercer.applySaturatedCasts;
import static io.trino.sql.planner.ExpressionExtractor.extractExpressions;
import static io.trino.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class DynamicFilterService
{
    private final Metadata metadata;
    private final FunctionManager functionManager;
    private final TypeOperators typeOperators;
    private final DataSize largeMaxSizePerFilter;
    private final DataSize smallMaxSizePerFilter;
    private final Map<QueryId, DynamicFilterContext> dynamicFilterContexts = new ConcurrentHashMap<>();

    @Inject
    public DynamicFilterService(Metadata metadata, FunctionManager functionManager, TypeOperators typeOperators, DynamicFilterConfig dynamicFilterConfig)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
        this.typeOperators = requireNonNull(typeOperators, "typeOperators is null");
        this.largeMaxSizePerFilter = dynamicFilterConfig.getLargeMaxSizePerFilter();
        this.smallMaxSizePerFilter = dynamicFilterConfig.getSmallMaxSizePerFilter();
    }

    public void registerQuery(SqlQueryExecution sqlQueryExecution, SubPlan fragmentedPlan)
    {
        PlanNode queryPlan = sqlQueryExecution.getQueryPlan().getRoot();
        Set<DynamicFilterId> dynamicFilters = getProducedDynamicFilters(queryPlan);
        Set<DynamicFilterId> replicatedDynamicFilters = getReplicatedDynamicFilters(queryPlan);

        Set<DynamicFilterId> lazyDynamicFilters = fragmentedPlan.getAllFragments().stream()
                .flatMap(plan -> getLazyDynamicFilters(plan).stream())
                .collect(toImmutableSet());

        // register query only if it contains dynamic filters
        if (!dynamicFilters.isEmpty()) {
            registerQuery(
                    sqlQueryExecution.getQueryId(),
                    sqlQueryExecution.getSession(),
                    dynamicFilters,
                    lazyDynamicFilters,
                    replicatedDynamicFilters);
        }
    }

    @VisibleForTesting
    public void registerQuery(
            QueryId queryId,
            Session session,
            Set<DynamicFilterId> dynamicFilters,
            Set<DynamicFilterId> lazyDynamicFilters,
            Set<DynamicFilterId> replicatedDynamicFilters)
    {
        dynamicFilterContexts.putIfAbsent(queryId, new DynamicFilterContext(
                session,
                dynamicFilters,
                lazyDynamicFilters,
                replicatedDynamicFilters,
                getDynamicFilterSizeLimit(session),
                0));
    }

    private DataSize getDynamicFilterSizeLimit(Session session)
    {
        if (isEnableLargeDynamicFilters(session)) {
            return largeMaxSizePerFilter;
        }
        return smallMaxSizePerFilter;
    }

    public void registerQueryRetry(QueryId queryId, int attemptId)
    {
        DynamicFilterContext context = dynamicFilterContexts.get(queryId);
        if (context == null) {
            // dynamic filtering is not enabled
            return;
        }
        checkState(
                attemptId == context.getAttemptId() + 1,
                "Query %s retry attempt %s was already registered",
                queryId,
                attemptId);
        dynamicFilterContexts.put(queryId, context.createContextForQueryRetry(attemptId));
    }

    public DynamicFiltersStats getDynamicFilteringStats(QueryId queryId, Session session)
    {
        DynamicFilterContext context = dynamicFilterContexts.get(queryId);
        if (context == null) {
            // query has been removed or dynamic filtering is not enabled
            return DynamicFiltersStats.EMPTY;
        }

        int lazyFilters = context.getLazyDynamicFilters().size();
        int replicatedFilters = context.getReplicatedDynamicFilters().size();
        int totalDynamicFilters = context.getTotalDynamicFilters();

        ConnectorSession connectorSession = session.toConnectorSession();
        List<DynamicFilterDomainStats> dynamicFilterDomainStats = context.getDynamicFilterSummaries().entrySet().stream()
                .map(entry -> {
                    DynamicFilterId dynamicFilterId = entry.getKey();
                    return new DynamicFilterDomainStats(
                            dynamicFilterId,
                            // use small limit for readability
                            entry.getValue().toString(connectorSession, 2),
                            context.getDynamicFilterCollectionDuration(dynamicFilterId));
                })
                .collect(toImmutableList());
        return new DynamicFiltersStats(
                dynamicFilterDomainStats,
                lazyFilters,
                replicatedFilters,
                totalDynamicFilters,
                dynamicFilterDomainStats.size());
    }

    public void removeQuery(QueryId queryId)
    {
        dynamicFilterContexts.remove(queryId);
    }

    /**
     * Dynamic filters are collected in same stage as the join operator in pipelined execution. This can result in deadlock
     * for source stage joins and connectors that wait for dynamic filters before generating splits
     * (probe splits might be blocked on dynamic filters which require at least one probe task in order to be collected).
     * To overcome this issue an initial task is created for source stages running broadcast join operator.
     * This task allows for dynamic filters collection without any probe side splits being scheduled.
     */
    public boolean isCollectingTaskNeeded(QueryId queryId, PlanFragment plan)
    {
        DynamicFilterContext context = dynamicFilterContexts.get(queryId);
        if (context == null) {
            // query has been removed or not registered (e.g. dynamic filtering is disabled)
            return false;
        }

        // dynamic filters are collected by additional task only for non-fixed source stage
        return plan.getPartitioning().equals(SOURCE_DISTRIBUTION) && !getLazyDynamicFilters(plan).isEmpty();
    }

    public boolean isStageSchedulingNeededToCollectDynamicFilters(QueryId queryId, PlanFragment plan)
    {
        DynamicFilterContext context = dynamicFilterContexts.get(queryId);
        if (context == null) {
            // query has been removed or not registered (e.g. dynamic filtering is disabled)
            return false;
        }

        // stage scheduling is not needed to collect dynamic filters for non-fixed source stage, because
        // for such stage collecting task is created
        return !plan.getPartitioning().equals(SOURCE_DISTRIBUTION) && !getLazyDynamicFilters(plan).isEmpty();
    }

    /**
     * Join build source tasks might become blocked waiting for join stage to collect build data.
     * In such case dynamic filters must be unblocked (and probe split generation resumed) for
     * source stage containing joins to allow build source tasks to flush data and complete.
     */
    public void unblockStageDynamicFilters(QueryId queryId, int attemptId, PlanFragment plan)
    {
        DynamicFilterContext context = dynamicFilterContexts.get(queryId);
        if (context == null || attemptId < context.getAttemptId()) {
            // query has been removed or not registered (e.g. dynamic filtering is disabled)
            // or a newer attempt has already been triggered
            return;
        }
        checkState(!context.isTaskRetriesEnabled(), "unblockStageDynamicFilters is not required for task retry mode");
        checkState(
                attemptId == context.getAttemptId(),
                "Query %s retry attempt %s has not been registered with dynamic filter service",
                queryId,
                attemptId);
        getSourceStageInnerLazyDynamicFilters(plan).forEach(filter ->
                requireNonNull(context.getLazyDynamicFilters().get(filter), "Future not found").set(null));
    }

    public DynamicFilter createDynamicFilter(
            QueryId queryId,
            List<DynamicFilters.Descriptor> dynamicFilterDescriptors,
            Map<Symbol, ColumnHandle> columnHandles,
            TypeProvider typeProvider)
    {
        Multimap<DynamicFilterId, DynamicFilters.Descriptor> symbolsMap = extractSourceSymbols(dynamicFilterDescriptors);
        Set<DynamicFilterId> dynamicFilters = ImmutableSet.copyOf(symbolsMap.keySet());
        DynamicFilterContext context = dynamicFilterContexts.get(queryId);
        if (context == null) {
            // query has been removed
            return EMPTY;
        }

        List<ListenableFuture<Void>> lazyDynamicFilterFutures = dynamicFilters.stream()
                .map(context.getLazyDynamicFilters()::get)
                .filter(Objects::nonNull)
                .collect(toImmutableList());
        AtomicReference<CurrentDynamicFilter> currentDynamicFilter = new AtomicReference<>(new CurrentDynamicFilter(0, TupleDomain.all()));

        Set<ColumnHandle> columnsCovered = symbolsMap.values().stream()
                .map(DynamicFilters.Descriptor::getInput)
                .map(Symbol::from)
                .map(probeSymbol -> requireNonNull(columnHandles.get(probeSymbol), () -> "Missing probe column for " + probeSymbol))
                .collect(toImmutableSet());

        return new DynamicFilter()
        {
            @Override
            public Set<ColumnHandle> getColumnsCovered()
            {
                return columnsCovered;
            }

            @Override
            public CompletableFuture<?> isBlocked()
            {
                // wait for any of the requested dynamic filter domains to be completed
                List<ListenableFuture<Void>> undoneFutures = lazyDynamicFilterFutures.stream()
                        .filter(future -> !future.isDone())
                        .collect(toImmutableList());

                if (undoneFutures.isEmpty()) {
                    return NOT_BLOCKED;
                }

                return unmodifiableFuture(toCompletableFuture(whenAnyComplete(undoneFutures)));
            }

            @Override
            public boolean isComplete()
            {
                return dynamicFilters.stream()
                        .allMatch(filterId -> context.getDynamicFilterSummary(filterId).isPresent());
            }

            @Override
            public boolean isAwaitable()
            {
                return lazyDynamicFilterFutures.stream()
                        .anyMatch(future -> !future.isDone());
            }

            @Override
            public TupleDomain<ColumnHandle> getCurrentPredicate()
            {
                ImmutableMap.Builder<DynamicFilterId, Domain> completedFiltersBuilder = ImmutableMap.builder();
                for (DynamicFilterId filterId : dynamicFilters) {
                    Optional<Domain> summary = context.getDynamicFilterSummary(filterId);
                    summary.ifPresent(domain -> completedFiltersBuilder.put(filterId, domain));
                }
                Map<DynamicFilterId, Domain> completedDynamicFilters = completedFiltersBuilder.buildOrThrow();

                CurrentDynamicFilter currentFilter = currentDynamicFilter.get();
                if (currentFilter.getCompletedDynamicFiltersCount() >= completedDynamicFilters.size()) {
                    // return current dynamic filter as it's more complete
                    return currentFilter.getDynamicFilter();
                }

                TupleDomain<ColumnHandle> dynamicFilter = TupleDomain.intersect(
                        completedDynamicFilters.entrySet().stream()
                                .map(filter -> translateSummaryToTupleDomain(context.getSession(), filter.getKey(), filter.getValue(), symbolsMap, columnHandles, typeProvider))
                                .collect(toImmutableList()));

                // It could happen that two threads update currentDynamicFilter concurrently.
                // In such case, currentDynamicFilter might be set to dynamic filter with less domains.
                // However, this isn't an issue since in the next getCurrentPredicate() call currentDynamicFilter
                // will be updated again with most accurate dynamic filter.
                currentDynamicFilter.set(new CurrentDynamicFilter(completedDynamicFilters.size(), dynamicFilter));
                return dynamicFilter;
            }
        };
    }

    public void registerDynamicFilterConsumer(QueryId queryId, int attemptId, Set<DynamicFilterId> dynamicFilterIds, Consumer<Map<DynamicFilterId, Domain>> consumer)
    {
        DynamicFilterContext context = dynamicFilterContexts.get(queryId);
        if (context == null || attemptId < context.getAttemptId()) {
            // query has been removed or not registered (e.g. dynamic filtering is disabled)
            // or a newer attempt has already been triggered
            return;
        }
        checkState(
                context.isTaskRetriesEnabled() || attemptId == context.getAttemptId(),
                "Query %s retry attempt %s has not been registered with dynamic filter service",
                queryId,
                attemptId);
        context.addDynamicFilterConsumer(dynamicFilterIds, consumer);
    }

    public void addTaskDynamicFilters(TaskId taskId, Map<DynamicFilterId, Domain> newDynamicFilters)
    {
        DynamicFilterContext context = dynamicFilterContexts.get(taskId.getQueryId());
        int taskAttemptId = taskId.getAttemptId();
        if (context == null || taskAttemptId < context.getAttemptId()) {
            // query has been removed or dynamic filters are from a previous query attempt
            return;
        }
        checkState(
                context.isTaskRetriesEnabled() || taskAttemptId == context.getAttemptId(),
                "Query %s retry attempt %s has not been registered with dynamic filter service",
                taskId.getQueryId(),
                taskAttemptId);
        context.addTaskDynamicFilters(taskId, newDynamicFilters);
    }

    public void stageCannotScheduleMoreTasks(StageId stageId, int attemptId, int numberOfTasks)
    {
        DynamicFilterContext context = dynamicFilterContexts.get(stageId.getQueryId());
        if (context == null || attemptId < context.getAttemptId()) {
            // query has been removed or not registered (e.g. dynamic filtering is disabled)
            // or a newer attempt has already been triggered
            return;
        }
        checkState(
                attemptId == context.getAttemptId(),
                "Stage %s retry attempt %s has not been registered with dynamic filter service",
                stageId,
                attemptId);
        context.stageCannotScheduleMoreTasks(stageId, numberOfTasks);
    }

    public static Set<DynamicFilterId> getOutboundDynamicFilters(PlanFragment plan)
    {
        // dynamic filters which are consumed by the given stage but produced by a different stage
        return ImmutableSet.copyOf(difference(
                getConsumedDynamicFilters(plan.getRoot()),
                getProducedDynamicFilters(plan.getRoot())));
    }

    @VisibleForTesting
    Optional<Domain> getSummary(QueryId queryId, DynamicFilterId filterId)
    {
        return dynamicFilterContexts.get(queryId).getDynamicFilterSummary(filterId);
    }

    private TupleDomain<ColumnHandle> translateSummaryToTupleDomain(
            Session session,
            DynamicFilterId filterId,
            Domain summary,
            Multimap<DynamicFilterId, DynamicFilters.Descriptor> descriptorMultimap,
            Map<Symbol, ColumnHandle> columnHandles,
            TypeProvider typeProvider)
    {
        Collection<DynamicFilters.Descriptor> descriptors = descriptorMultimap.get(filterId);
        return TupleDomain.withColumnDomains(descriptors.stream()
                .collect(toImmutableMap(
                        descriptor -> {
                            Symbol probeSymbol = Symbol.from(descriptor.getInput());
                            return requireNonNull(columnHandles.get(probeSymbol), () -> format("Missing probe column for %s", probeSymbol));
                        },
                        descriptor -> {
                            Type targetType = typeProvider.get(Symbol.from(descriptor.getInput()));
                            Domain updatedSummary = descriptor.applyComparison(summary);
                            if (!updatedSummary.getType().equals(targetType)) {
                                return applySaturatedCasts(metadata, functionManager, typeOperators, session, updatedSummary, targetType);
                            }
                            return updatedSummary;
                        })));
    }

    private static Set<DynamicFilterId> getLazyDynamicFilters(PlanFragment plan)
    {
        // To prevent deadlock dynamic filter can be lazy only when:
        // 1. it's consumed by different stage from where it's produced
        // 2. or it's produced by replicated join in source stage. In such case an extra
        //    task is created that will collect dynamic filter and prevent deadlock.
        Set<DynamicFilterId> interStageDynamicFilters = difference(getProducedDynamicFilters(plan.getRoot()), getConsumedDynamicFilters(plan.getRoot()));
        return ImmutableSet.copyOf(union(interStageDynamicFilters, getSourceStageInnerLazyDynamicFilters(plan)));
    }

    @VisibleForTesting
    static Set<DynamicFilterId> getSourceStageInnerLazyDynamicFilters(PlanFragment plan)
    {
        if (!plan.getPartitioning().equals(SOURCE_DISTRIBUTION)) {
            // Only non-fixed source stages can have (replicated) lazy dynamic filters that are
            // produced and consumed within stage. This is because for such stages an extra
            // dynamic filtering collecting task can be added.
            return ImmutableSet.of();
        }

        PlanNode planNode = plan.getRoot();
        Set<DynamicFilterId> innerStageDynamicFilters = intersection(getProducedDynamicFilters(planNode), getConsumedDynamicFilters(planNode));
        Set<DynamicFilterId> replicatedDynamicFilters = getReplicatedDynamicFilters(planNode);
        return ImmutableSet.copyOf(intersection(innerStageDynamicFilters, replicatedDynamicFilters));
    }

    private static Set<DynamicFilterId> getReplicatedDynamicFilters(PlanNode planNode)
    {
        return PlanNodeSearcher.searchFrom(planNode)
                .whereIsInstanceOfAny(JoinNode.class, SemiJoinNode.class)
                .findAll().stream()
                .filter(JoinUtils::isBuildSideReplicated)
                .flatMap(node -> getDynamicFiltersProducedInPlanNode(node).stream())
                .collect(toImmutableSet());
    }

    private static Set<DynamicFilterId> getProducedDynamicFilters(PlanNode planNode)
    {
        return PlanNodeSearcher.searchFrom(planNode)
                .whereIsInstanceOfAny(JoinNode.class, SemiJoinNode.class, DynamicFilterSourceNode.class)
                .findAll().stream()
                .flatMap(node -> getDynamicFiltersProducedInPlanNode(node).stream())
                .collect(toImmutableSet());
    }

    private static Set<DynamicFilterId> getDynamicFiltersProducedInPlanNode(PlanNode planNode)
    {
        if (planNode instanceof JoinNode) {
            return ((JoinNode) planNode).getDynamicFilters().keySet();
        }
        if (planNode instanceof SemiJoinNode) {
            return ((SemiJoinNode) planNode).getDynamicFilterId().map(ImmutableSet::of).orElse(ImmutableSet.of());
        }
        if (planNode instanceof DynamicFilterSourceNode) {
            return ((DynamicFilterSourceNode) planNode).getDynamicFilters().keySet();
        }
        throw new IllegalStateException("getDynamicFiltersProducedInPlanNode called with neither JoinNode nor SemiJoinNode");
    }

    private static Set<DynamicFilterId> getConsumedDynamicFilters(PlanNode planNode)
    {
        return extractExpressions(planNode).stream()
                .flatMap(expression -> extractDynamicFilters(expression).getDynamicConjuncts().stream())
                .map(DynamicFilters.Descriptor::getId)
                .collect(toImmutableSet());
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

    private static class DynamicFilterCollectionContext
    {
        private final boolean replicated;
        private final long domainSizeLimitInBytes;
        @GuardedBy("collectedTasks")
        private final RoaringBitmap collectedTasks = new RoaringBitmap();
        private final Queue<Domain> summaryDomains = new ConcurrentLinkedQueue<>();
        private final AtomicLong summaryDomainsRetainedSizeInBytes = new AtomicLong();

        @GuardedBy("this")
        private volatile Integer expectedTaskCount;
        @GuardedBy("this")
        private int collectedTaskCount;

        private final long start = System.nanoTime();
        private final AtomicReference<Duration> collectionDuration = new AtomicReference<>();
        @GuardedBy("this")
        private volatile boolean collected;
        private final SettableFuture<Domain> collectedDomainsFuture = SettableFuture.create();

        private DynamicFilterCollectionContext(boolean replicated, long domainSizeLimitInBytes)
        {
            this.replicated = replicated;
            this.domainSizeLimitInBytes = domainSizeLimitInBytes;
        }

        public void collect(TaskId taskId, Domain domain)
        {
            if (collected) {
                return;
            }

            if (replicated) {
                collectReplicated(domain);
            }
            else {
                collectPartitioned(taskId, domain);
            }
        }

        private void collectReplicated(Domain domain)
        {
            if (domain.getRetainedSizeInBytes() > domainSizeLimitInBytes) {
                domain = domain.simplify(1);
            }
            if (domain.getRetainedSizeInBytes() > domainSizeLimitInBytes) {
                domain = Domain.all(domain.getType());
            }
            Domain result;
            synchronized (this) {
                if (collected) {
                    return;
                }
                collectedTaskCount++;
                collected = true;
                result = domain;
            }
            collectionDuration.set(Duration.succinctNanos(System.nanoTime() - start));
            collectedDomainsFuture.set(result);
        }

        private void collectPartitioned(TaskId taskId, Domain domain)
        {
            synchronized (collectedTasks) {
                if (!collectedTasks.checkedAdd(taskId.getPartitionId())) {
                    return;
                }
            }

            summaryDomainsRetainedSizeInBytes.addAndGet(domain.getRetainedSizeInBytes());
            summaryDomains.add(domain);
            unionSummaryDomainsIfNecessary(false);

            Domain result;
            synchronized (this) {
                if (collected) {
                    clearSummaryDomains();
                    return;
                }
                collectedTaskCount++;
                boolean allPartitionsCollected = expectedTaskCount != null && expectedTaskCount == collectedTaskCount;
                if (allPartitionsCollected) {
                    // run final compaction as previous concurrent compactions may have left more than a single domain
                    unionSummaryDomainsIfNecessary(true);
                }

                boolean sizeLimitExceeded = false;
                Domain allDomain = null;
                Domain summary = summaryDomains.poll();
                // summary can be null as another concurrent summary compaction may be running
                if (summary != null) {
                    long originalSize = summary.getRetainedSizeInBytes();
                    if (summary.getRetainedSizeInBytes() > domainSizeLimitInBytes) {
                        summary = summary.simplify(1);
                    }
                    if (summary.getRetainedSizeInBytes() > domainSizeLimitInBytes) {
                        sizeLimitExceeded = true;
                        allDomain = Domain.all(summary.getType());
                        summaryDomainsRetainedSizeInBytes.addAndGet(-originalSize);
                    }
                    else {
                        summaryDomainsRetainedSizeInBytes.addAndGet(summary.getRetainedSizeInBytes() - originalSize);
                        summaryDomains.add(summary);
                    }
                }

                boolean collectionFinished = sizeLimitExceeded || domain.isAll() || allPartitionsCollected;
                if (!collectionFinished) {
                    return;
                }
                collected = true;
                if (sizeLimitExceeded) {
                    result = allDomain;
                }
                else if (domain.isAll()) {
                    clearSummaryDomains();
                    result = domain;
                }
                else {
                    verify(allPartitionsCollected, "allPartitionsCollected is expected to be true");
                    int summaryDomainsCount = summaryDomains.size();
                    verify(summaryDomainsCount == 1, "summaryDomainsCount is expected to be equal to 1, got: %s", summaryDomainsCount);
                    result = summaryDomains.poll();
                    verify(result != null);
                    long currentSize = summaryDomainsRetainedSizeInBytes.addAndGet(-result.getRetainedSizeInBytes());
                    verify(currentSize == 0, "currentSize is expected to be zero: %s", currentSize);
                }
            }

            collectionDuration.set(Duration.succinctNanos(System.nanoTime() - start));
            collectedDomainsFuture.set(result);
        }

        private void unionSummaryDomainsIfNecessary(boolean force)
        {
            if (summaryDomainsRetainedSizeInBytes.get() < domainSizeLimitInBytes && !force) {
                return;
            }

            List<Domain> domains = new ArrayList<>();
            long domainsRetainedSizeInBytes = 0;
            while (true) {
                Domain domain = summaryDomains.poll();
                if (domain == null) {
                    break;
                }
                domains.add(domain);
                domainsRetainedSizeInBytes += domain.getRetainedSizeInBytes();
            }

            if (domains.isEmpty()) {
                return;
            }

            Domain union = union(domains);
            summaryDomainsRetainedSizeInBytes.addAndGet(union.getRetainedSizeInBytes() - domainsRetainedSizeInBytes);
            long currentSize = summaryDomainsRetainedSizeInBytes.get();
            verify(currentSize >= 0, "currentSize is expected to be greater than or equal to zero: %s", currentSize);
            summaryDomains.add(union);
        }

        private void clearSummaryDomains()
        {
            long domainsRetainedSizeInBytes = 0;
            while (true) {
                Domain domain = summaryDomains.poll();
                if (domain == null) {
                    break;
                }
                domainsRetainedSizeInBytes += domain.getRetainedSizeInBytes();
            }
            summaryDomainsRetainedSizeInBytes.addAndGet(-domainsRetainedSizeInBytes);
            long currentSize = summaryDomainsRetainedSizeInBytes.get();
            verify(currentSize >= 0, "currentSize is expected to be greater than or equal to zero: %s", currentSize);
        }

        public void setExpectedTaskCount(int count)
        {
            if (collected || expectedTaskCount != null) {
                return;
            }
            checkArgument(count > 0, "count is expected to be greater than zero: %s", count);

            Domain result;
            synchronized (this) {
                if (collected || expectedTaskCount != null) {
                    return;
                }
                expectedTaskCount = count;
                verify(collectedTaskCount <= expectedTaskCount,
                        "collectedTaskCount is expected to be less than or equal to %s, got: %s",
                        expectedTaskCount,
                        collectedTaskCount);
                if (collectedTaskCount != expectedTaskCount) {
                    return;
                }
                // run union one more time
                unionSummaryDomainsIfNecessary(true);

                verify(summaryDomains.size() == 1);
                result = summaryDomains.poll();
                verify(result != null);
                long currentSize = summaryDomainsRetainedSizeInBytes.addAndGet(-result.getRetainedSizeInBytes());
                verify(currentSize == 0, "currentSize is expected to be zero: %s", currentSize);
            }

            collectionDuration.set(Duration.succinctNanos(System.nanoTime() - start));
            collectedDomainsFuture.set(result);
        }

        public ListenableFuture<Domain> getCollectedDomainFuture()
        {
            return collectedDomainsFuture;
        }

        public Optional<Duration> getCollectionDuration()
        {
            return Optional.ofNullable(collectionDuration.get());
        }
    }

    private static class DynamicFilterContext
    {
        private final Session session;
        private final Set<DynamicFilterId> dynamicFilters;
        private final Set<DynamicFilterId> replicatedDynamicFilters;
        private final DataSize dynamicFilterSizeLimit;
        private final Map<DynamicFilterId, SettableFuture<Void>> lazyDynamicFilters;
        private final Map<DynamicFilterId, DynamicFilterCollectionContext> dynamicFilterCollectionContexts;

        private final Map<StageId, Set<DynamicFilterId>> stageDynamicFilters = new ConcurrentHashMap<>();
        private final Map<StageId, Integer> stageNumberOfTasks = new ConcurrentHashMap<>();

        private final int attemptId;

        private DynamicFilterContext(
                Session session,
                Set<DynamicFilterId> dynamicFilters,
                Set<DynamicFilterId> lazyDynamicFilters,
                Set<DynamicFilterId> replicatedDynamicFilters,
                DataSize dynamicFilterSizeLimit,
                int attemptId)
        {
            this.session = requireNonNull(session, "session is null");
            this.dynamicFilters = requireNonNull(dynamicFilters, "dynamicFilters is null");
            requireNonNull(lazyDynamicFilters, "lazyDynamicFilters is null");
            this.lazyDynamicFilters = lazyDynamicFilters.stream()
                    .collect(toImmutableMap(identity(), filter -> SettableFuture.create()));
            this.replicatedDynamicFilters = requireNonNull(replicatedDynamicFilters, "replicatedDynamicFilters is null");
            this.dynamicFilterSizeLimit = requireNonNull(dynamicFilterSizeLimit, "dynamicFilterSizeLimit is null");
            ImmutableMap.Builder<DynamicFilterId, DynamicFilterCollectionContext> collectionContexts = ImmutableMap.builder();
            for (DynamicFilterId dynamicFilterId : dynamicFilters) {
                DynamicFilterCollectionContext collectionContext = new DynamicFilterCollectionContext(replicatedDynamicFilters.contains(dynamicFilterId), dynamicFilterSizeLimit.toBytes());
                collectionContexts.put(dynamicFilterId, collectionContext);
                SettableFuture<Void> lazyDynamicFilterFuture = this.lazyDynamicFilters.get(dynamicFilterId);
                if (lazyDynamicFilterFuture != null) {
                    collectionContext.getCollectedDomainFuture().addListener(() -> lazyDynamicFilterFuture.set(null), directExecutor());
                }
            }
            dynamicFilterCollectionContexts = collectionContexts.buildOrThrow();
            this.attemptId = attemptId;
        }

        DynamicFilterContext createContextForQueryRetry(int attemptId)
        {
            return new DynamicFilterContext(
                    session,
                    dynamicFilters,
                    lazyDynamicFilters.keySet(),
                    replicatedDynamicFilters,
                    dynamicFilterSizeLimit,
                    attemptId);
        }

        void addDynamicFilterConsumer(Set<DynamicFilterId> dynamicFilterIds, Consumer<Map<DynamicFilterId, Domain>> consumer)
        {
            for (DynamicFilterId dynamicFilterId : dynamicFilterIds) {
                DynamicFilterCollectionContext collectionContext = dynamicFilterCollectionContexts.get(dynamicFilterId);
                verify(collectionContext != null, "collectionContext is missing for %s", dynamicFilterId);
                addSuccessCallback(collectionContext.getCollectedDomainFuture(), domain -> consumer.accept(ImmutableMap.of(dynamicFilterId, domain)));
            }
        }

        public Session getSession()
        {
            return session;
        }

        private int getTotalDynamicFilters()
        {
            return dynamicFilters.size();
        }

        private void addTaskDynamicFilters(TaskId taskId, Map<DynamicFilterId, Domain> newDynamicFilters)
        {
            newDynamicFilters.forEach((dynamicFilterId, domain) -> {
                DynamicFilterCollectionContext collectionContext = dynamicFilterCollectionContexts.get(dynamicFilterId);
                verify(collectionContext != null, "collectionContext is missing for %s", dynamicFilterId);
                collectionContext.collect(taskId, domain);
            });

            if (stageDynamicFilters.computeIfAbsent(taskId.getStageId(), key -> newConcurrentHashSet()).addAll(newDynamicFilters.keySet())) {
                updateExpectedTaskCount();
            }
        }

        private void stageCannotScheduleMoreTasks(StageId stageId, int numberOfTasks)
        {
            if (stageNumberOfTasks.put(stageId, numberOfTasks) == null) {
                updateExpectedTaskCount();
            }
        }

        private void updateExpectedTaskCount()
        {
            stageNumberOfTasks.forEach((stage, taskCount) -> {
                Set<DynamicFilterId> filtersIds = stageDynamicFilters.get(stage);
                if (filtersIds != null) {
                    for (DynamicFilterId filterId : filtersIds) {
                        DynamicFilterCollectionContext collectionContext = dynamicFilterCollectionContexts.get(filterId);
                        verify(collectionContext != null, "collectionContext is missing for %s", filterId);
                        collectionContext.setExpectedTaskCount(taskCount);
                    }
                }
            });
        }

        private Map<DynamicFilterId, Domain> getDynamicFilterSummaries()
        {
            return dynamicFilterCollectionContexts.entrySet().stream()
                    .filter(entry -> entry.getValue().getCollectedDomainFuture().isDone())
                    .collect(toImmutableMap(Map.Entry::getKey, entry -> getFutureValue(entry.getValue().getCollectedDomainFuture())));
        }

        private Optional<Domain> getDynamicFilterSummary(DynamicFilterId filterId)
        {
            DynamicFilterCollectionContext context = dynamicFilterCollectionContexts.get(filterId);
            if (context == null || !context.getCollectedDomainFuture().isDone()) {
                return Optional.empty();
            }
            return Optional.of(getFutureValue(context.getCollectedDomainFuture()));
        }

        private Map<DynamicFilterId, SettableFuture<Void>> getLazyDynamicFilters()
        {
            return lazyDynamicFilters;
        }

        private Set<DynamicFilterId> getReplicatedDynamicFilters()
        {
            return replicatedDynamicFilters;
        }

        private Optional<Duration> getDynamicFilterCollectionDuration(DynamicFilterId dynamicFilterId)
        {
            DynamicFilterCollectionContext collectionContext = dynamicFilterCollectionContexts.get(dynamicFilterId);
            verify(collectionContext != null, "collectionContext is missing for %s", dynamicFilterId);
            return collectionContext.getCollectionDuration();
        }

        private int getAttemptId()
        {
            return attemptId;
        }

        private boolean isTaskRetriesEnabled()
        {
            return getRetryPolicy(session) == RetryPolicy.TASK;
        }
    }

    private static class CurrentDynamicFilter
    {
        private final int completedDynamicFiltersCount;
        private final TupleDomain<ColumnHandle> dynamicFilter;

        private CurrentDynamicFilter(int completedDynamicFiltersCount, TupleDomain<ColumnHandle> dynamicFilter)
        {
            this.completedDynamicFiltersCount = completedDynamicFiltersCount;
            this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
        }

        private int getCompletedDynamicFiltersCount()
        {
            return completedDynamicFiltersCount;
        }

        private TupleDomain<ColumnHandle> getDynamicFilter()
        {
            return dynamicFilter;
        }
    }
}
