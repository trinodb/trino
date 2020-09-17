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
package io.prestosql.sql.planner;

import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.DynamicFilter;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.sql.planner.plan.DynamicFilterId;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSetMultimap.toImmutableSetMultimap;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.addSuccessCallback;
import static io.airlift.concurrent.MoreFutures.unmodifiableFuture;
import static io.prestosql.sql.DynamicFilters.Descriptor;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@ThreadSafe
class LocalDynamicFiltersCollector
{
    // Each future blocks until its dynamic filter is collected.
    private final Map<DynamicFilterId, SettableFuture<Domain>> futures = new HashMap<>();

    public LocalDynamicFiltersCollector()
    {
    }

    // Called during JoinNode planning (no need to be synchronized as local planning is single threaded)
    public void register(Set<DynamicFilterId> filterIds)
    {
        filterIds.forEach(filterId -> verify(
                futures.put(filterId, SettableFuture.create()) == null,
                "LocalDynamicFiltersCollector: duplicate filter %s", filterId));
    }

    // Used during execution (after build-side dynamic filter collection is over).
    // No need to be synchronized as the futures map doesn't change.
    public void collectDynamicFilterDomains(Map<DynamicFilterId, Domain> dynamicFilterDomains)
    {
        dynamicFilterDomains
                .entrySet()
                .forEach(entry -> {
                    SettableFuture<Domain> future = futures.get(entry.getKey());
                    // Skip dynamic filters that are not applied locally.
                    if (future != null) {
                        verify(future.set(entry.getValue()), "Dynamic filter %s already collected", entry.getKey());
                    }
                });
    }

    // Called during TableScan planning (no need to be synchronized as local planning is single threaded)
    public DynamicFilter createDynamicFilter(List<Descriptor> descriptors, Map<Symbol, ColumnHandle> columnsMap)
    {
        Multimap<DynamicFilterId, Symbol> symbolsMap = descriptors.stream()
                .collect(toImmutableSetMultimap(Descriptor::getId, descriptor -> Symbol.from(descriptor.getInput())));

        // Iterate over dynamic filters that are collected (correspond to one of the futures), and required for filtering (correspond to one of the descriptors).
        // It is possible that some dynamic filters are collected in a different stage - and will not available here.
        // It is also possible that not all local dynamic filters are needed for this specific table scan.
        List<ListenableFuture<TupleDomain<ColumnHandle>>> predicateFutures = symbolsMap.keySet().stream()
                .filter(futures.keySet()::contains)
                .map(filterId -> {
                    // Probe-side columns that can be filtered with this dynamic filter resulting domain.
                    List<ColumnHandle> probeColumns = symbolsMap.get(filterId).stream()
                            .map(probeSymbol -> requireNonNull(columnsMap.get(probeSymbol), () -> format("Missing probe column for %s", probeSymbol)))
                            .collect(toImmutableList());
                    return Futures.transform(
                            requireNonNull(futures.get(filterId), () -> format("Missing dynamic filter %s", filterId)),
                            // Construct a probe-side predicate by duplicating the resulting domain over the corresponding columns.
                            domain -> TupleDomain.withColumnDomains(
                                    probeColumns.stream()
                                            .collect(toImmutableMap(
                                                    column -> column,
                                                    column -> domain))),
                            directExecutor());
                })
                .collect(toImmutableList());
        return new TableSpecificDynamicFilter(predicateFutures);
    }

    // Table-specific dynamic filter (collects all domains for a specific table scan)
    private static class TableSpecificDynamicFilter
            implements DynamicFilter
    {
        @GuardedBy("this")
        private CompletableFuture<?> isBlocked;

        @GuardedBy("this")
        private TupleDomain<ColumnHandle> currentPredicate;

        @GuardedBy("this")
        private int futuresLeft;

        private TableSpecificDynamicFilter(List<ListenableFuture<TupleDomain<ColumnHandle>>> predicateFutures)
        {
            this.futuresLeft = predicateFutures.size();
            this.isBlocked = predicateFutures.isEmpty() ? NOT_BLOCKED : new CompletableFuture();
            this.currentPredicate = TupleDomain.all();
            predicateFutures.stream().forEach(future -> addSuccessCallback(future, this::update, directExecutor()));
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
                isBlocked = isComplete() ? NOT_BLOCKED : new CompletableFuture();
            }
            // notify readers outside of lock since this may result in a callback
            verify(currentFuture.complete(null));
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
