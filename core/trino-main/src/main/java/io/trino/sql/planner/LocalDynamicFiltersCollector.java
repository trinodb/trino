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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.trino.Session;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.plan.DynamicFilterId;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.addSuccessCallback;
import static io.airlift.concurrent.MoreFutures.unmodifiableFuture;
import static io.trino.sql.DynamicFilters.Descriptor;
import static io.trino.sql.DynamicFilters.extractSourceSymbols;
import static io.trino.sql.planner.DomainCoercer.applySaturatedCasts;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class LocalDynamicFiltersCollector
{
    private final Session session;
    // Each future blocks until its dynamic filter is collected.
    private final Map<DynamicFilterId, SettableFuture<Domain>> futures = new HashMap<>();

    public LocalDynamicFiltersCollector(Session session)
    {
        this.session = requireNonNull(session, "session is null");
    }

    // Called during JoinNode planning (no need to be synchronized as local planning is single threaded)
    public void register(Set<DynamicFilterId> filterIds)
    {
        filterIds.forEach(filterId -> verify(
                futures.put(filterId, SettableFuture.create()) == null,
                "LocalDynamicFiltersCollector: duplicate filter %s", filterId));
    }

    public Set<DynamicFilterId> getRegisteredDynamicFilterIds()
    {
        return futures.keySet();
    }

    // Used during execution (after build-side dynamic filter collection is over).
    // No need to be synchronized as the futures map doesn't change.
    public void collectDynamicFilterDomains(Map<DynamicFilterId, Domain> dynamicFilterDomains)
    {
        dynamicFilterDomains.forEach((key, value) -> {
            SettableFuture<Domain> future = futures.get(key);
            // Skip dynamic filters that are not applied locally.
            if (future != null) {
                // Coordinator may re-send dynamicFilterDomain if sendUpdate request fails
                // It's possible that the request failed after the DF was already collected here
                future.set(value);
            }
        });
    }

    // Called during TableScan planning (no need to be synchronized as local planning is single threaded)
    public DynamicFilter createDynamicFilter(
            List<Descriptor> descriptors,
            Map<Symbol, ColumnHandle> columnsMap,
            TypeProvider typeProvider,
            PlannerContext plannerContext)
    {
        Multimap<DynamicFilterId, Descriptor> descriptorMap = extractSourceSymbols(descriptors);

        // Iterate over dynamic filters that are collected (correspond to one of the futures), and required for filtering (correspond to one of the descriptors).
        // It is possible that some dynamic filters are collected in a different stage - and will not available here.
        // It is also possible that not all local dynamic filters are needed for this specific table scan.
        List<ListenableFuture<TupleDomain<ColumnHandle>>> predicateFutures = descriptorMap.keySet().stream()
                .filter(futures.keySet()::contains)
                .map(filterId -> {
                    // Probe-side columns that can be filtered with this dynamic filter resulting domain.
                    return Futures.transform(
                            requireNonNull(futures.get(filterId), () -> format("Missing dynamic filter %s", filterId)),
                            // Construct a probe-side predicate by duplicating the resulting domain over the corresponding columns.
                            domain -> TupleDomain.withColumnDomains(
                                    descriptorMap.get(filterId).stream()
                                            .collect(toImmutableMap(
                                                    descriptor -> {
                                                        Symbol probeSymbol = Symbol.from(descriptor.getInput());
                                                        return requireNonNull(columnsMap.get(probeSymbol), () -> format("Missing probe column for %s", probeSymbol));
                                                    },
                                                    descriptor -> {
                                                        Type targetType = typeProvider.get(Symbol.from(descriptor.getInput()));
                                                        Domain updatedDomain = descriptor.applyComparison(domain);
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
                                                    }))),
                            directExecutor());
                })
                .collect(toImmutableList());

        Set<ColumnHandle> columnsCovered = descriptorMap.values().stream()
                .map(Descriptor::getInput)
                .map(Symbol::from)
                .map(probeSymbol -> requireNonNull(columnsMap.get(probeSymbol), () -> "Missing probe column for " + probeSymbol))
                .collect(toImmutableSet());

        return new TableSpecificDynamicFilter(columnsCovered, predicateFutures);
    }

    // Table-specific dynamic filter (collects all domains for a specific table scan)
    private static class TableSpecificDynamicFilter
            implements DynamicFilter
    {
        private final Set<ColumnHandle> columnsCovered;
        @GuardedBy("this")
        private CompletableFuture<?> isBlocked;

        @GuardedBy("this")
        private TupleDomain<ColumnHandle> currentPredicate;

        @GuardedBy("this")
        private int futuresLeft;

        private TableSpecificDynamicFilter(Set<ColumnHandle> columnsCovered, List<ListenableFuture<TupleDomain<ColumnHandle>>> predicateFutures)
        {
            this.columnsCovered = ImmutableSet.copyOf(requireNonNull(columnsCovered, "columnsCovered is null"));
            this.futuresLeft = predicateFutures.size();
            this.isBlocked = predicateFutures.isEmpty() ? NOT_BLOCKED : new CompletableFuture<>();
            this.currentPredicate = TupleDomain.all();
            predicateFutures.forEach(future -> addSuccessCallback(future, this::update, directExecutor()));
        }

        private void update(TupleDomain<ColumnHandle> predicate)
        {
            CompletableFuture<?> currentFuture;
            synchronized (this) {
                futuresLeft -= 1;
                verify(futuresLeft >= 0);
                currentPredicate = currentPredicate.intersect(predicate);
                currentFuture = isBlocked;
                // create next blocking future (if needed)
                isBlocked = isComplete() ? NOT_BLOCKED : new CompletableFuture<>();
            }
            // notify readers outside of lock since this may result in a callback
            verify(currentFuture.complete(null));
        }

        @Override
        public Set<ColumnHandle> getColumnsCovered()
        {
            return columnsCovered;
        }

        @Override
        public synchronized CompletableFuture<?> isBlocked()
        {
            return unmodifiableFuture(isBlocked);
        }

        @Override
        public synchronized boolean isComplete()
        {
            return futuresLeft == 0;
        }

        @Override
        public synchronized boolean isAwaitable()
        {
            return futuresLeft > 0;
        }

        @Override
        public synchronized TupleDomain<ColumnHandle> getCurrentPredicate()
        {
            return currentPredicate;
        }
    }
}
