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
package io.trino.cache;

import com.google.common.util.concurrent.ListenableFuture;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.anyOf;

/**
 * Implementation of dynamic filter that is not awaitable.
 */
public class StaticDynamicFilter
        implements DynamicFilter
{
    private final Set<ColumnHandle> columnsCovered;
    private final boolean isComplete;
    private final TupleDomain<ColumnHandle> tupleDomain;
    private volatile int hashCode;

    /**
     * Creates a {@code Supplier<StaticDynamicFilter>} that caches the {@link StaticDynamicFilter}.
     * Cache is reset whenever any underlying dynamic filter gets updated.
     */
    public static Supplier<StaticDynamicFilter> createStaticDynamicFilterSupplier(List<DynamicFilter> disjunctiveDynamicFilters)
    {
        AtomicReference<AtomicReference<StaticDynamicFilter>> dynamicFilterCache = new AtomicReference<>();
        resetDynamicFilterCache(dynamicFilterCache, disjunctiveDynamicFilters);
        return () -> {
            AtomicReference<StaticDynamicFilter> dynamicFilterReference = requireNonNull(dynamicFilterCache.get());
            StaticDynamicFilter dynamicFilter = dynamicFilterReference.get();
            if (dynamicFilter == null) {
                dynamicFilter = createStaticDynamicFilter(disjunctiveDynamicFilters);
                if (!dynamicFilterReference.compareAndSet(null, dynamicFilter)) {
                    return dynamicFilterReference.get();
                }
            }
            return dynamicFilter;
        };
    }

    private static void resetDynamicFilterCache(
            AtomicReference<AtomicReference<StaticDynamicFilter>> dynamicFilterCache,
            List<DynamicFilter> disjunctiveDynamicFilters)
    {
        if (areAwaitable(disjunctiveDynamicFilters)) {
            // reset dynamic filter cache whenever any underlying dynamic filter gets updated
            whenAnyUpdates(disjunctiveDynamicFilters).addListener(() -> resetDynamicFilterCache(dynamicFilterCache, disjunctiveDynamicFilters), directExecutor());
        }
        dynamicFilterCache.set(new AtomicReference<>());
    }

    private static boolean areAwaitable(List<DynamicFilter> disjunctiveDynamicFilters)
    {
        return disjunctiveDynamicFilters.stream().anyMatch(DynamicFilter::isAwaitable);
    }

    private static ListenableFuture<?> whenAnyUpdates(List<DynamicFilter> disjunctiveDynamicFilters)
    {
        return toListenableFuture(anyOf(disjunctiveDynamicFilters.stream()
                .map(DynamicFilter::isBlocked)
                .toArray(CompletableFuture[]::new)));
    }

    public static StaticDynamicFilter createStaticDynamicFilter(List<DynamicFilter> disjunctiveDynamicFilters)
    {
        requireNonNull(disjunctiveDynamicFilters, "disjunctiveDynamicFilters is null");
        checkArgument(!disjunctiveDynamicFilters.isEmpty());
        return new StaticDynamicFilter(
                disjunctiveDynamicFilters.stream()
                        .flatMap(filter -> filter.getColumnsCovered().stream())
                        .collect(toImmutableSet()),
                // isComplete needs to be called before getCurrentPredicate
                disjunctiveDynamicFilters.stream().allMatch(DynamicFilter::isComplete),
                TupleDomain.columnWiseUnion(disjunctiveDynamicFilters.stream()
                        .map(DynamicFilter::getCurrentPredicate)
                        .collect(toImmutableList())));
    }

    private StaticDynamicFilter(Set<ColumnHandle> columnsCovered, boolean isComplete, TupleDomain<ColumnHandle> tupleDomain)
    {
        this.columnsCovered = requireNonNull(columnsCovered, "columnsCovered is null");
        this.isComplete = isComplete;
        this.tupleDomain = requireNonNull(tupleDomain, "tupleDomain is null");
    }

    @Override
    public Set<ColumnHandle> getColumnsCovered()
    {
        return columnsCovered;
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return NOT_BLOCKED;
    }

    @Override
    public boolean isComplete()
    {
        return isComplete;
    }

    @Override
    public boolean isAwaitable()
    {
        return false;
    }

    @Override
    public TupleDomain<ColumnHandle> getCurrentPredicate()
    {
        return tupleDomain;
    }

    @Override
    public OptionalLong getPreferredDynamicFilterTimeout()
    {
        return OptionalLong.of(0L);
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
        StaticDynamicFilter that = (StaticDynamicFilter) o;
        return isComplete == that.isComplete
                && Objects.equals(columnsCovered, that.columnsCovered)
                && Objects.equals(tupleDomain, that.tupleDomain);
    }

    @Override
    public int hashCode()
    {
        if (hashCode == 0) {
            hashCode = Objects.hash(columnsCovered, isComplete, tupleDomain);
        }
        return hashCode;
    }
}
