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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.prestosql.execution.SqlQueryExecution;
import io.prestosql.execution.StageInfo;
import io.prestosql.execution.StageState;
import io.prestosql.execution.TaskInfo;
import io.prestosql.execution.TaskManagerConfig;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.optimizations.PlanNodeSearcher;
import io.prestosql.sql.planner.plan.JoinNode;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;

@ThreadSafe
public class DynamicFilterService
{
    private final Map<SourceDescriptor, Domain> dynamicFilterSummaries = new ConcurrentHashMap<>();

    @GuardedBy("this")
    private final Map<QueryId, Supplier<List<StageInfo>>> queries = new HashMap<>();

    private final ScheduledExecutorService collectDynamicFiltersExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("DynamicFilterService"));

    @Inject
    public DynamicFilterService(TaskManagerConfig taskConfig)
    {
        collectDynamicFiltersExecutor.scheduleWithFixedDelay(this::collectDynamicFilters, 0, taskConfig.getStatusRefreshMaxWait().toMillis(), MILLISECONDS);
    }

    @PreDestroy
    public void stop()
    {
        collectDynamicFiltersExecutor.shutdownNow();
    }

    public void registerQuery(SqlQueryExecution sqlQueryExecution)
    {
        // register query only if it contains dynamic filters
        boolean hasDynamicFilters = PlanNodeSearcher.searchFrom(sqlQueryExecution.getQueryPlan().getRoot())
                .where(node -> node instanceof JoinNode && !((JoinNode) node).getDynamicFilters().isEmpty())
                .matches();
        if (hasDynamicFilters) {
            registerQuery(sqlQueryExecution.getQueryId(), sqlQueryExecution::getAllStages);
        }
    }

    @VisibleForTesting
    public synchronized void registerQuery(QueryId queryId, Supplier<List<StageInfo>> stageInfoSupplier)
    {
        queries.putIfAbsent(queryId, stageInfoSupplier);
    }

    public synchronized void removeQuery(QueryId queryId)
    {
        dynamicFilterSummaries.keySet().removeIf(sourceDescriptor -> sourceDescriptor.getQueryId().equals(queryId));
        queries.remove(queryId);
    }

    @VisibleForTesting
    public synchronized void collectDynamicFilters()
    {
        for (Map.Entry<QueryId, Supplier<List<StageInfo>>> entry : queries.entrySet()) {
            QueryId queryId = entry.getKey();
            for (StageInfo stageInfo : entry.getValue().get()) {
                StageState stageState = stageInfo.getState();
                // wait until stage has finished scheduling tasks
                if (stageState.canScheduleMoreTasks()) {
                    continue;
                }
                List<TaskInfo> tasks = stageInfo.getTasks();
                tasks.stream()
                        .map(taskInfo -> taskInfo.getTaskStatus().getDynamicFilterDomains())
                        .flatMap(taskDomains -> taskDomains.entrySet().stream())
                        .filter(domain -> !dynamicFilterSummaries.containsKey(SourceDescriptor.of(queryId, domain.getKey())))
                        .collect(groupingBy(Map.Entry::getKey, mapping(Map.Entry::getValue, toImmutableList())))
                        .entrySet().stream()
                        // check if all tasks of a dynamic filter source have reported dynamic filter summary
                        .filter(stageDomains -> stageDomains.getValue().size() == tasks.size())
                        .forEach(stageDomains -> dynamicFilterSummaries.put(
                                SourceDescriptor.of(queryId, stageDomains.getKey()),
                                Domain.union(stageDomains.getValue())));
            }
        }
    }

    public Supplier<TupleDomain<ColumnHandle>> createDynamicFilterSupplier(QueryId queryId, List<DynamicFilters.Descriptor> dynamicFilters, Map<Symbol, ColumnHandle> columnHandles)
    {
        Map<String, ColumnHandle> sourceColumnHandles = extractSourceColumnHandles(dynamicFilters, columnHandles);

        return () -> dynamicFilters.stream()
                .map(filter -> getSummary(queryId, filter.getId())
                        .map(summary -> translateSummaryToTupleDomain(filter.getId(), summary, sourceColumnHandles)))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .reduce(TupleDomain.all(), TupleDomain::intersect);
    }

    @VisibleForTesting
    Optional<Domain> getSummary(QueryId queryId, String filterId)
    {
        return Optional.ofNullable(dynamicFilterSummaries.get(SourceDescriptor.of(queryId, filterId)));
    }

    @Immutable
    private static class SourceDescriptor
    {
        private final QueryId queryId;
        private final String filterId;

        public static SourceDescriptor of(QueryId queryId, String filterId)
        {
            return new SourceDescriptor(queryId, filterId);
        }

        private SourceDescriptor(QueryId queryId, String filterId)
        {
            this.queryId = requireNonNull(queryId, "queryId is null");
            this.filterId = requireNonNull(filterId, "filterId is null");
        }

        public QueryId getQueryId()
        {
            return queryId;
        }

        @Override
        public boolean equals(Object other)
        {
            if (other == this) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }

            SourceDescriptor sourceDescriptor = (SourceDescriptor) other;

            return Objects.equals(queryId, sourceDescriptor.queryId) &&
                    Objects.equals(filterId, sourceDescriptor.filterId);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(queryId, filterId);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("queryId", queryId)
                    .add("filterId", filterId)
                    .toString();
        }
    }

    private static TupleDomain<ColumnHandle> translateSummaryToTupleDomain(String filterId, Domain summary, Map<String, ColumnHandle> sourceColumnHandles)
    {
        ColumnHandle sourceColumnHandle = requireNonNull(sourceColumnHandles.get(filterId), () -> format("Source column handle for dynamic filter %s is null", filterId));
        return TupleDomain.withColumnDomains(ImmutableMap.of(sourceColumnHandle, summary));
    }

    private static Map<String, ColumnHandle> extractSourceColumnHandles(List<DynamicFilters.Descriptor> dynamicFilters, Map<Symbol, ColumnHandle> columnHandles)
    {
        return dynamicFilters.stream()
                .collect(toImmutableMap(
                        DynamicFilters.Descriptor::getId,
                        descriptor -> columnHandles.get(Symbol.from(descriptor.getInput()))));
    }
}
