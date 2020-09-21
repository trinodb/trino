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
package io.prestosql.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.execution.DynamicFilterConfig;
import io.prestosql.execution.SqlQueryExecution;
import io.prestosql.execution.StageState;
import io.prestosql.operator.JoinUtils;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.DynamicFilter;
import io.prestosql.spi.predicate.DiscreteValues;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Ranges;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.sql.planner.PlanFragment;
import io.prestosql.sql.planner.SubPlan;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.optimizations.PlanNodeSearcher;
import io.prestosql.sql.planner.plan.DynamicFilterId;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.SemiJoinNode;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableListMultimap.toImmutableListMultimap;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Sets.intersection;
import static com.google.common.collect.Sets.union;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.airlift.concurrent.MoreFutures.unmodifiableFuture;
import static io.airlift.concurrent.MoreFutures.whenAnyComplete;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.spi.connector.DynamicFilter.EMPTY;
import static io.prestosql.spi.predicate.Domain.union;
import static io.prestosql.sql.DynamicFilters.extractDynamicFilters;
import static io.prestosql.sql.planner.ExpressionExtractor.extractExpressions;
import static io.prestosql.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.prestosql.util.MorePredicates.isInstanceOfAny;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;

@ThreadSafe
public class DynamicFilterService
{
    private final Duration dynamicFilteringRefreshInterval;
    private final ScheduledExecutorService collectDynamicFiltersExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("DynamicFilterService"));

    private final Map<QueryId, DynamicFilterContext> dynamicFilterContexts = new ConcurrentHashMap<>();

    @Inject
    public DynamicFilterService(DynamicFilterConfig dynamicFilterConfig)
    {
        this.dynamicFilteringRefreshInterval = requireNonNull(dynamicFilterConfig, "dynamicFilterConfig is null").getDynamicFilteringRefreshInterval();
    }

    @PostConstruct
    public void start()
    {
        collectDynamicFiltersExecutor.scheduleWithFixedDelay(this::collectDynamicFilters, 0, dynamicFilteringRefreshInterval.toMillis(), MILLISECONDS);
    }

    @PreDestroy
    public void stop()
    {
        collectDynamicFiltersExecutor.shutdownNow();
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
                    sqlQueryExecution::getStageDynamicFilters,
                    dynamicFilters,
                    lazyDynamicFilters,
                    replicatedDynamicFilters);
        }
    }

    @VisibleForTesting
    public void registerQuery(
            QueryId queryId,
            Supplier<List<StageDynamicFilters>> stageDynamicFiltersSupplier,
            Set<DynamicFilterId> dynamicFilters,
            Set<DynamicFilterId> lazyDynamicFilters,
            Set<DynamicFilterId> replicatedDynamicFilters)
    {
        Map<DynamicFilterId, SettableFuture<?>> lazyDynamicFilterFutures = lazyDynamicFilters.stream()
                .collect(toImmutableMap(filter -> filter, filter -> SettableFuture.create()));
        dynamicFilterContexts.putIfAbsent(queryId, new DynamicFilterContext(
                stageDynamicFiltersSupplier,
                dynamicFilters,
                lazyDynamicFilterFutures,
                replicatedDynamicFilters));
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

        List<DynamicFilterDomainStats> dynamicFilterDomainStats = context.getDynamicFilterSummaries().entrySet().stream()
                .map(entry -> {
                    DynamicFilterId dynamicFilterId = entry.getKey();
                    Domain domain = entry.getValue();
                    // simplify for readability
                    String simplifiedDomain = domain.simplify(1).toString(session.toConnectorSession());
                    int rangeCount = domain.getValues().getValuesProcessor().transform(
                            Ranges::getRangeCount,
                            discreteValues -> 0,
                            allOrNone -> 0);
                    int discreteValuesCount = domain.getValues().getValuesProcessor().transform(
                            ranges -> 0,
                            DiscreteValues::getValuesCount,
                            allOrNone -> 0);
                    return new DynamicFilterDomainStats(dynamicFilterId, simplifiedDomain, rangeCount, discreteValuesCount);
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
     * Dynamic filters are collected in same stage as the join operator. This can result in deadlock
     * for source stage joins and connectors that wait for dynamic filters before generating splits
     * (probe splits might be blocked on dynamic filters which require at least one probe task in order to be collected).
     * To overcome this issue an initial task is created for source stages running broadcast join operator.
     * This task allows for dynamic filters collection without any probe side splits being scheduled.
     */
    public boolean isCollectingTaskNeeded(QueryId queryId, PlanFragment plan)
    {
        DynamicFilterContext context = dynamicFilterContexts.get(queryId);
        if (context == null) {
            // query has been removed or not registered (e.g dynamic filtering is disabled)
            return false;
        }

        return !getSourceStageInnerLazyDynamicFilters(plan).isEmpty();
    }

    /**
     * Join build source tasks might become blocked waiting for join stage to collect build data.
     * In such case dynamic filters must be unblocked (and probe split generation resumed) for
     * source stage containing joins to allow build source tasks to flush data and complete.
     */
    public void unblockStageDynamicFilters(QueryId queryId, PlanFragment plan)
    {
        DynamicFilterContext context = dynamicFilterContexts.get(queryId);
        if (context == null) {
            // query has been removed or not registered (e.g dynamic filtering is disabled)
            return;
        }

        getSourceStageInnerLazyDynamicFilters(plan).forEach(filter ->
                requireNonNull(context.getLazyDynamicFilters().get(filter), "Future not found").set(null));
    }

    public DynamicFilter createDynamicFilter(QueryId queryId, List<DynamicFilters.Descriptor> dynamicFilterDescriptors, Map<Symbol, ColumnHandle> columnHandles)
    {
        Multimap<DynamicFilterId, ColumnHandle> sourceColumnHandles = extractSourceColumnHandles(dynamicFilterDescriptors, columnHandles);
        Set<DynamicFilterId> dynamicFilters = dynamicFilterDescriptors.stream()
                .map(DynamicFilters.Descriptor::getId)
                .collect(toImmutableSet());
        DynamicFilterContext context = dynamicFilterContexts.get(queryId);
        if (context == null) {
            // query has been removed
            return EMPTY;
        }

        List<ListenableFuture<?>> lazyDynamicFilterFutures = dynamicFilters.stream()
                .map(context.getLazyDynamicFilters()::get)
                .filter(Objects::nonNull)
                .collect(toImmutableList());
        AtomicReference<CurrentDynamicFilter> currentDynamicFilter = new AtomicReference<>(new CurrentDynamicFilter(0, TupleDomain.all()));

        return new DynamicFilter()
        {
            @Override
            public CompletableFuture<?> isBlocked()
            {
                // wait for any of the requested dynamic filter domains to be completed
                List<ListenableFuture<?>> undoneFutures = lazyDynamicFilterFutures.stream()
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
                        .allMatch(context.getDynamicFilterSummaries()::containsKey);
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
                Set<DynamicFilterId> completedDynamicFilters = dynamicFilters.stream()
                        .filter(filter -> context.getDynamicFilterSummaries().containsKey(filter))
                        .collect(toImmutableSet());

                CurrentDynamicFilter currentFilter = currentDynamicFilter.get();
                if (currentFilter.getCompletedDynamicFiltersCount() >= completedDynamicFilters.size()) {
                    // return current dynamic filter as it's more complete
                    return currentFilter.getDynamicFilter();
                }

                TupleDomain<ColumnHandle> dynamicFilter = completedDynamicFilters.stream()
                        .map(filter -> translateSummaryToTupleDomain(filter, context.getDynamicFilterSummaries().get(filter), sourceColumnHandles))
                        .reduce(TupleDomain.all(), TupleDomain::intersect);

                // It could happen that two threads update currentDynamicFilter concurrently.
                // In such case, currentDynamicFilter might be set to dynamic filter with less domains.
                // However, this isn't an issue since in the next getCurrentPredicate() call currentDynamicFilter
                // will be updated again with most accurate dynamic filter.
                currentDynamicFilter.set(new CurrentDynamicFilter(completedDynamicFilters.size(), dynamicFilter));
                return dynamicFilter;
            }
        };
    }

    @VisibleForTesting
    void collectDynamicFilters()
    {
        for (DynamicFilterContext context : dynamicFilterContexts.values()) {
            if (context.isCompleted()) {
                continue;
            }

            Set<DynamicFilterId> uncollectedFilters = context.getUncollectedDynamicFilters();
            ImmutableMap.Builder<DynamicFilterId, Domain> newDynamicFiltersBuilder = ImmutableMap.builder();
            for (StageDynamicFilters stageDynamicFilters : context.getDynamicFilterSupplier().get()) {
                StageState stageState = stageDynamicFilters.getStageState();
                stageDynamicFilters.getTaskDynamicFilters().stream()
                        .flatMap(taskDomains -> taskDomains.entrySet().stream())
                        .filter(domain -> uncollectedFilters.contains(domain.getKey()))
                        .collect(groupingBy(Map.Entry::getKey, mapping(Map.Entry::getValue, toImmutableList())))
                        .entrySet().stream()
                        .filter(stageDomains -> {
                            if (stageDomains.getValue().stream().anyMatch(Domain::isAll)) {
                                // if one of the domains is all, we don't need to get dynamic filters from all tasks
                                return true;
                            }

                            if (context.getReplicatedDynamicFilters().contains(stageDomains.getKey())) {
                                checkState(
                                        stageDomains.getValue().size() == 1,
                                        "Replicated dynamic filter should be collected from single task");
                                // for replicated dynamic filters it's enough to get dynamic filter from a single task
                                return true;
                            }

                            // check if all tasks of a dynamic filter source have reported dynamic filter summary
                            return !stageState.canScheduleMoreTasks() && stageDomains.getValue().size() == stageDynamicFilters.getNumberOfTasks();
                        })
                        .forEach(stageDomains -> newDynamicFiltersBuilder.put(stageDomains.getKey(), union(stageDomains.getValue())));
            }

            context.addDynamicFilters(newDynamicFiltersBuilder.build());
        }
    }

    @VisibleForTesting
    Optional<Domain> getSummary(QueryId queryId, DynamicFilterId filterId)
    {
        return Optional.ofNullable(dynamicFilterContexts.get(queryId).getDynamicFilterSummaries().get(filterId));
    }

    private static TupleDomain<ColumnHandle> translateSummaryToTupleDomain(DynamicFilterId filterId, Domain summary, Multimap<DynamicFilterId, ColumnHandle> sourceColumnHandles)
    {
        checkState(sourceColumnHandles.containsKey(filterId), "No source column handles for dynamic filter %s", filterId);
        return TupleDomain.withColumnDomains(
                sourceColumnHandles.get(filterId).stream()
                        .collect(toImmutableMap(handle -> handle, handle -> summary)));
    }

    private static Multimap<DynamicFilterId, ColumnHandle> extractSourceColumnHandles(List<DynamicFilters.Descriptor> dynamicFilters, Map<Symbol, ColumnHandle> columnHandles)
    {
        return dynamicFilters.stream()
                .collect(toImmutableListMultimap(
                        DynamicFilters.Descriptor::getId,
                        descriptor -> columnHandles.get(Symbol.from(descriptor.getInput()))));
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
                .where(isInstanceOfAny(JoinNode.class, SemiJoinNode.class))
                .findAll().stream()
                .filter((JoinUtils::isBuildSideReplicated))
                .flatMap(node -> getDynamicFiltersProducedInPlanNode(node).stream())
                .collect(toImmutableSet());
    }

    private static Set<DynamicFilterId> getProducedDynamicFilters(PlanNode planNode)
    {
        return PlanNodeSearcher.searchFrom(planNode)
                .where(isInstanceOfAny(JoinNode.class, SemiJoinNode.class))
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
        throw new IllegalStateException("getDynamicFiltersProducedInPlanNode called with neither JoinNode nor SemiJoinNode");
    }

    private static Set<DynamicFilterId> getConsumedDynamicFilters(PlanNode planNode)
    {
        return extractExpressions(planNode).stream()
                .flatMap(expression -> extractDynamicFilters(expression).getDynamicConjuncts().stream())
                .map(DynamicFilters.Descriptor::getId)
                .collect(toImmutableSet());
    }

    public static class StageDynamicFilters
    {
        private final StageState stageState;
        private final int numberOfTasks;
        private final List<Map<DynamicFilterId, Domain>> taskDynamicFilters;

        public StageDynamicFilters(StageState stageState, int numberOfTasks, List<Map<DynamicFilterId, Domain>> taskDynamicFilters)
        {
            this.stageState = requireNonNull(stageState, "stageState is null");
            this.numberOfTasks = numberOfTasks;
            this.taskDynamicFilters = ImmutableList.copyOf(requireNonNull(taskDynamicFilters, "taskDynamicFilters is null"));
        }

        private StageState getStageState()
        {
            return stageState;
        }

        private int getNumberOfTasks()
        {
            return numberOfTasks;
        }

        private List<Map<DynamicFilterId, Domain>> getTaskDynamicFilters()
        {
            return taskDynamicFilters;
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
            this.dynamicFilterDomainStats = dynamicFilterDomainStats;
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
        private final int rangeCount;
        private final int discreteValuesCount;

        @JsonCreator
        public DynamicFilterDomainStats(
                @JsonProperty("dynamicFilterId") DynamicFilterId dynamicFilterId,
                @JsonProperty("simplifiedDomain") String simplifiedDomain,
                @JsonProperty("rangeCount") int rangeCount,
                @JsonProperty("discreteValuesCount") int discreteValuesCount)
        {
            this.dynamicFilterId = dynamicFilterId;
            this.simplifiedDomain = simplifiedDomain;
            this.rangeCount = rangeCount;
            this.discreteValuesCount = discreteValuesCount;
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
        public int getRangeCount()
        {
            return rangeCount;
        }

        @JsonProperty
        public int getDiscreteValuesCount()
        {
            return discreteValuesCount;
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
            DynamicFilterDomainStats that = (DynamicFilterDomainStats) o;
            return rangeCount == that.rangeCount &&
                    discreteValuesCount == that.discreteValuesCount &&
                    Objects.equals(dynamicFilterId, that.dynamicFilterId) &&
                    Objects.equals(simplifiedDomain, that.simplifiedDomain);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(dynamicFilterId, simplifiedDomain, rangeCount, discreteValuesCount);
        }
    }

    private static class DynamicFilterContext
    {
        private final Map<DynamicFilterId, Domain> dynamicFilterSummaries = new ConcurrentHashMap<>();
        private final Supplier<List<StageDynamicFilters>> dynamicFilterSupplier;
        private final Set<DynamicFilterId> dynamicFilters;
        private final Map<DynamicFilterId, SettableFuture<?>> lazyDynamicFilters;
        private final Set<DynamicFilterId> replicatedDynamicFilters;
        private final AtomicBoolean completed = new AtomicBoolean();

        private DynamicFilterContext(
                Supplier<List<StageDynamicFilters>> dynamicFilterSupplier,
                Set<DynamicFilterId> dynamicFilters,
                Map<DynamicFilterId, SettableFuture<?>> lazyDynamicFilters,
                Set<DynamicFilterId> replicatedDynamicFilters)
        {
            this.dynamicFilterSupplier = requireNonNull(dynamicFilterSupplier, "dynamicFilterSupplier is null");
            this.dynamicFilters = requireNonNull(dynamicFilters, "dynamicFilters is null");
            this.lazyDynamicFilters = requireNonNull(lazyDynamicFilters, "lazyDynamicFilters is null");
            this.replicatedDynamicFilters = requireNonNull(replicatedDynamicFilters, "replicatedDynamicFilters is null");
        }

        private int getTotalDynamicFilters()
        {
            return dynamicFilters.size();
        }

        private Set<DynamicFilterId> getUncollectedDynamicFilters()
        {
            return dynamicFilters.stream()
                    .filter(filter -> !dynamicFilterSummaries.containsKey(filter))
                    .collect(toImmutableSet());
        }

        private void addDynamicFilters(Map<DynamicFilterId, Domain> newDynamicFilters)
        {
            newDynamicFilters.forEach((filter, domain) -> {
                dynamicFilterSummaries.put(filter, domain);
                Optional.ofNullable(lazyDynamicFilters.get(filter)).ifPresent(future -> future.set(null));
            });

            // stop collecting dynamic filters for query when all dynamic filters have been collected
            completed.set(dynamicFilters.stream().allMatch(dynamicFilterSummaries::containsKey));
        }

        private Map<DynamicFilterId, Domain> getDynamicFilterSummaries()
        {
            return dynamicFilterSummaries;
        }

        private Supplier<List<StageDynamicFilters>> getDynamicFilterSupplier()
        {
            return dynamicFilterSupplier;
        }

        private Map<DynamicFilterId, SettableFuture<?>> getLazyDynamicFilters()
        {
            return lazyDynamicFilters;
        }

        private Set<DynamicFilterId> getReplicatedDynamicFilters()
        {
            return replicatedDynamicFilters;
        }

        private boolean isCompleted()
        {
            return completed.get();
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
