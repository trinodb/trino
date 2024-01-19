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

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

/**
 * Implementation of dynamic filter that is not awaitable.
 */
public class StaticDynamicFilter
        implements DynamicFilter
{
    private final Set<ColumnHandle> columnsCovered;
    private final boolean isComplete;
    private final TupleDomain<ColumnHandle> tupleDomain;

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
}
