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
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

/**
 * Dynamic filter which accepts only rows accepted by both of the filters.
 * Intersections of equal filters are equal, so that consumers caching work per dynamic filter can reuse it.
 */
public record IntersectionDynamicFilter(DynamicFilter first, DynamicFilter second)
        implements DynamicFilter
{
    public static DynamicFilter intersect(DynamicFilter first, DynamicFilter second)
    {
        if (first == DynamicFilter.EMPTY) {
            return second;
        }
        if (second == DynamicFilter.EMPTY) {
            return first;
        }
        return new IntersectionDynamicFilter(first, second);
    }

    public IntersectionDynamicFilter
    {
        requireNonNull(first, "first is null");
        requireNonNull(second, "second is null");
    }

    @Override
    public Set<ColumnHandle> getColumnsCovered()
    {
        return ImmutableSet.<ColumnHandle>builder()
                .addAll(first.getColumnsCovered())
                .addAll(second.getColumnsCovered())
                .build();
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        boolean firstAwaitable = first.isAwaitable();
        boolean secondAwaitable = second.isAwaitable();
        if (firstAwaitable && secondAwaitable) {
            return CompletableFuture.anyOf(first.isBlocked(), second.isBlocked());
        }
        if (firstAwaitable) {
            return first.isBlocked();
        }
        if (secondAwaitable) {
            return second.isBlocked();
        }
        return NOT_BLOCKED;
    }

    @Override
    public boolean isComplete()
    {
        return first.isComplete() && second.isComplete();
    }

    @Override
    public boolean isAwaitable()
    {
        return first.isAwaitable() || second.isAwaitable();
    }

    @Override
    public TupleDomain<ColumnHandle> getCurrentPredicate()
    {
        return first.getCurrentPredicate().intersect(second.getCurrentPredicate());
    }
}
