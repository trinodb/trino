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
package io.trino.operator;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import jakarta.annotation.Nullable;

import java.lang.invoke.MethodHandle;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.typeHasNaN;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Objects.requireNonNull;

/**
 * Dynamic filter on the first sort key of a TopN.
 * Algorithm is described here:
 * <a href="https://clickhouse.com/blog/clickhouse-top-n-queries-granule-level-data-skipping">...</a>
 * Once a TopN heap holds N rows, a row ranked below the lowest ranked row can be skipped.
 * The filter starts out empty and the bound becomes tighter as the TopN finds better rows.
 */
@ThreadSafe
public class TopNDynamicFilter
{
    private final ColumnHandle column;
    private final Type type;
    private final SortOrder sortOrder;
    // Only the first column of the sort order is considered in the dynamic filter.
    // If the sort order has only a single column, the bound can be exclusive otherwise must be inclusive.
    private final boolean inclusive;
    private final MethodHandle comparison;

    @Nullable
    @GuardedBy("this")
    private Object bound;
    @GuardedBy("this")
    private DynamicFilter snapshot = DynamicFilter.EMPTY;

    public TopNDynamicFilter(ColumnHandle column, Type type, SortOrder sortOrder, boolean inclusive, TypeOperators typeOperators)
    {
        this.column = requireNonNull(column, "column is null");
        this.type = requireNonNull(type, "type is null");
        this.sortOrder = requireNonNull(sortOrder, "sortOrder is null");
        this.inclusive = inclusive;
        this.comparison = typeOperators.getComparisonUnorderedLastOperator(type, simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL))
                .asType(methodType(long.class, Object.class, Object.class));
    }

    /**
     * TopNDynamicFilter requires an orderable type without the possibility of NaN; NaN ordering is complicated
     */
    public static boolean isSupportedType(Type type)
    {
        return type.isOrderable() && !containsNaN(type);
    }

    private static boolean containsNaN(Type type)
    {
        return typeHasNaN(type) || type.getTypeParameters().stream().anyMatch(TopNDynamicFilter::containsNaN);
    }

    /**
     * Tightens the filter using the sort key of the lowest ranked row held by a TopN which already holds N rows.
     */
    public void tighten(Block block, int position)
    {
        // A null bound does not exclude any non-null value
        // TODO: nulls could tighten the bound when the sort order is NULLS FIRST, but is an uncommon query pattern
        if (block.isNull(position)) {
            return;
        }
        Object value = readNativeValue(type, block, position);

        synchronized (this) {
            if (bound != null && !ranksAbove(value, bound)) {
                return;
            }
            bound = value;
            snapshot = new Snapshot(column, TupleDomain.withColumnDomains(ImmutableMap.of(column, createDomain(value))));
        }
    }

    /**
     * Returns a dynamic filter with the current bound, which does not change when the bound is tightened later.
     */
    public synchronized DynamicFilter snapshot()
    {
        return snapshot;
    }

    private boolean ranksAbove(Object value, Object other)
    {
        long result;
        try {
            result = (long) comparison.invokeExact(value, other);
        }
        catch (Throwable t) {
            throwIfUnchecked(t);
            throw new RuntimeException(t);
        }
        if (sortOrder.isAscending()) {
            return result < 0;
        }
        return result > 0;
    }

    private Domain createDomain(Object value)
    {
        Range range;
        if (sortOrder.isAscending()) {
            range = inclusive ? Range.lessThanOrEqual(type, value) : Range.lessThan(type, value);
        }
        else {
            range = inclusive ? Range.greaterThanOrEqual(type, value) : Range.greaterThan(type, value);
        }
        // nulls rank above any value when sorted first
        return Domain.create(ValueSet.ofRanges(range), sortOrder.isNullsFirst());
    }

    @Override
    public synchronized String toString()
    {
        return "TopNDynamicFilter{column=%s, sortOrder=%s, inclusive=%s, snapshot=%s}".formatted(column, sortOrder, inclusive, snapshot);
    }

    private record Snapshot(ColumnHandle column, TupleDomain<ColumnHandle> predicate)
            implements DynamicFilter
    {
        private Snapshot
        {
            requireNonNull(column, "column is null");
            requireNonNull(predicate, "predicate is null");
        }

        @Override
        public Set<ColumnHandle> getColumnsCovered()
        {
            return ImmutableSet.of(column);
        }

        @Override
        public CompletableFuture<?> isBlocked()
        {
            return NOT_BLOCKED;
        }

        @Override
        public boolean isComplete()
        {
            return true;
        }

        @Override
        public boolean isAwaitable()
        {
            return false;
        }

        @Override
        public TupleDomain<ColumnHandle> getCurrentPredicate()
        {
            return predicate;
        }
    }
}
