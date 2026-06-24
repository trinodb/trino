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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.trino.Session;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.sql.DynamicFilters.Descriptor;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.plan.DynamicFilterId;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.concurrent.MoreFutures.unmodifiableFuture;
import static io.trino.sql.DynamicFilters.extractSourceSymbols;
import static io.trino.sql.planner.DomainCoercer.applySaturatedCasts;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class LocalDynamicFiltersCollector
{
    private final Session session;
    private final Map<DynamicFilterId, DynamicFilterState> dynamicFilters = new HashMap<>();

    public LocalDynamicFiltersCollector(Session session)
    {
        this.session = requireNonNull(session, "session is null");
    }

    // Called during JoinNode planning (no need to be synchronized as local planning is single threaded)
    public void register(Set<DynamicFilterId> filterIds)
    {
        register(filterIds, false);
    }

    // Called during planning (no need to be synchronized as local planning is single threaded)
    public void register(Set<DynamicFilterId> filterIds, boolean streaming)
    {
        filterIds.forEach(filterId -> verify(
                dynamicFilters.put(filterId, new DynamicFilterState(streaming)) == null,
                "LocalDynamicFiltersCollector: duplicate filter %s",
                filterId));
    }

    public Set<DynamicFilterId> getRegisteredDynamicFilterIds()
    {
        return dynamicFilters.keySet();
    }

    // Used during execution.
    // No need to be synchronized as the futures map doesn't change.
    public void collectDynamicFilterDomains(Map<DynamicFilterId, Domain> dynamicFilterDomains)
    {
        dynamicFilterDomains.forEach((key, value) -> {
            DynamicFilterState dynamicFilter = dynamicFilters.get(key);
            // Skip dynamic filters that are not applied locally.
            if (dynamicFilter != null) {
                dynamicFilter.update(value);
            }
        });
    }

    // Called during TableScan planning (no need to be synchronized as local planning is single threaded)
    public DynamicFilter createDynamicFilter(
            List<Descriptor> descriptors,
            Map<Symbol, ColumnHandle> columnsMap,
            PlannerContext plannerContext)
    {
        Multimap<DynamicFilterId, Descriptor> descriptorMap = extractSourceSymbols(descriptors);

        // Iterate over dynamic filters that are collected locally, and required for filtering.
        // It is possible that some dynamic filters are collected in a different stage - and will not available here.
        // It is also possible that not all local dynamic filters are needed for this specific table scan.
        Map<DynamicFilterId, DynamicFilterState> activeDynamicFilters = descriptorMap.keySet().stream()
                .filter(dynamicFilters.keySet()::contains)
                .collect(toImmutableMap(
                        filterId -> filterId,
                        filterId -> requireNonNull(dynamicFilters.get(filterId), () -> format("Missing dynamic filter %s", filterId))));

        List<DynamicFilterPredicate> predicates = activeDynamicFilters.keySet().stream()
                .map(filterId -> new DynamicFilterPredicate(filterId, descriptorMap.get(filterId).stream()
                        .collect(toImmutableMap(
                                descriptor -> {
                                    Symbol probeSymbol = Symbol.from(descriptor.getInput());
                                    return requireNonNull(columnsMap.get(probeSymbol), () -> format("Missing probe column for %s", probeSymbol));
                                },
                                descriptor -> descriptor,
                                (left, right) -> left))))
                .collect(toImmutableList());

        Set<ColumnHandle> columnsCovered = descriptorMap.values().stream()
                .map(Descriptor::getInput)
                .map(Symbol::from)
                .map(probeSymbol -> requireNonNull(columnsMap.get(probeSymbol), () -> "Missing probe column for " + probeSymbol))
                .collect(toImmutableSet());

        return new TableSpecificDynamicFilter(columnsCovered, activeDynamicFilters, predicates, plannerContext);
    }

    // Table-specific dynamic filter (collects all domains for a specific table scan)
    private class TableSpecificDynamicFilter
            implements DynamicFilter
    {
        private final Set<ColumnHandle> columnsCovered;
        private final Map<DynamicFilterId, DynamicFilterState> activeDynamicFilters;
        private final List<DynamicFilterPredicate> predicates;
        private final PlannerContext plannerContext;

        private TableSpecificDynamicFilter(
                Set<ColumnHandle> columnsCovered,
                Map<DynamicFilterId, DynamicFilterState> activeDynamicFilters,
                List<DynamicFilterPredicate> predicates,
                PlannerContext plannerContext)
        {
            this.columnsCovered = ImmutableSet.copyOf(requireNonNull(columnsCovered, "columnsCovered is null"));
            this.activeDynamicFilters = requireNonNull(activeDynamicFilters, "activeDynamicFilters is null");
            this.predicates = requireNonNull(predicates, "predicates is null");
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        }

        @Override
        public Set<ColumnHandle> getColumnsCovered()
        {
            return columnsCovered;
        }

        @Override
        public CompletableFuture<?> isBlocked()
        {
            List<CompletableFuture<?>> blockedFutures = activeDynamicFilters.values().stream()
                    .map(DynamicFilterState::isBlocked)
                    .filter(future -> !future.isDone())
                    .collect(toImmutableList());
            if (blockedFutures.isEmpty()) {
                return NOT_BLOCKED;
            }
            return unmodifiableFuture(CompletableFuture.anyOf(blockedFutures.toArray(CompletableFuture[]::new)));
        }

        @Override
        public boolean isComplete()
        {
            return activeDynamicFilters.values().stream()
                    .allMatch(DynamicFilterState::isComplete);
        }

        @Override
        public boolean isAwaitable()
        {
            return activeDynamicFilters.values().stream()
                    .anyMatch(dynamicFilter -> !dynamicFilter.isComplete());
        }

        @Override
        public TupleDomain<ColumnHandle> getCurrentPredicate()
        {
            return TupleDomain.intersect(predicates.stream()
                    .map(predicate -> predicate.toTupleDomain(activeDynamicFilters.get(predicate.filterId()).getCurrentDomain(), plannerContext))
                    .collect(toImmutableList()));
        }
    }

    private class DynamicFilterPredicate
    {
        private final DynamicFilterId filterId;
        private final Map<ColumnHandle, Descriptor> descriptors;

        private DynamicFilterPredicate(DynamicFilterId filterId, Map<ColumnHandle, Descriptor> descriptors)
        {
            this.filterId = requireNonNull(filterId, "filterId is null");
            this.descriptors = requireNonNull(descriptors, "descriptors is null");
        }

        private DynamicFilterId filterId()
        {
            return filterId;
        }

        private TupleDomain<ColumnHandle> toTupleDomain(Optional<Domain> domain, PlannerContext plannerContext)
        {
            if (domain.isEmpty()) {
                return TupleDomain.all();
            }
            return TupleDomain.withColumnDomains(descriptors.entrySet().stream()
                    .collect(toImmutableMap(
                            Map.Entry::getKey,
                            entry -> {
                                Descriptor descriptor = entry.getValue();
                                Symbol symbol = Symbol.from(descriptor.getInput());
                                Type targetType = symbol.type();
                                Domain updatedDomain = descriptor.applyComparison(domain.orElseThrow());
                                if (!updatedDomain.getType().equals(targetType)) {
                                    return applySaturatedCasts(
                                            plannerContext.getMetadata(),
                                            plannerContext.getFunctionManager(),
                                            plannerContext.getTypeOperators(),
                                            session,
                                            updatedDomain,
                                            targetType);
                                }
                                return updatedDomain;
                            },
                            Domain::intersect)));
        }
    }

    private static class DynamicFilterState
    {
        private final boolean streaming;

        @GuardedBy("this")
        private Optional<Domain> currentDomain = Optional.empty();

        @GuardedBy("this")
        private CompletableFuture<?> blocked = new CompletableFuture<>();

        @GuardedBy("this")
        private boolean complete;

        private DynamicFilterState(boolean streaming)
        {
            this.streaming = streaming;
        }

        private void update(Domain domain)
        {
            CompletableFuture<?> currentBlocked;
            synchronized (this) {
                currentDomain = Optional.of(currentDomain
                        .map(existing -> existing.intersect(domain))
                        .orElse(domain));
                if (!streaming) {
                    complete = true;
                }
                currentBlocked = blocked;
                blocked = complete ? DynamicFilter.NOT_BLOCKED : new CompletableFuture<>();
            }
            currentBlocked.complete(null);
        }

        private synchronized CompletableFuture<?> isBlocked()
        {
            return blocked;
        }

        private synchronized boolean isComplete()
        {
            return complete;
        }

        private synchronized Optional<Domain> getCurrentDomain()
        {
            return currentDomain;
        }
    }
}
