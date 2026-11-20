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

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.FloatingPointValueSet;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import io.trino.type.BlockTypeOperators;
import io.trino.type.BlockTypeOperators.BlockPositionOrdering;
import jakarta.annotation.Nullable;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static io.trino.spi.type.TypeUtils.isFloatingPointNaN;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.typeHasNaN;
import static java.util.Objects.requireNonNull;

public final class TopNDynamicFilter
        implements DynamicFilter
{
    private static final int MAX_BOUNDARY_BYTES = 64 * 1024;

    private final boolean canUseOrderedRange;
    private final boolean nanComesFirst;
    private final Type type;
    private final BlockPositionOrdering ordering;
    private final ColumnHandle column;
    private final int channel;
    private final SortOrder order;
    private volatile TupleDomain<ColumnHandle> predicate = TupleDomain.all();

    @Nullable
    private Block boundary;

    public TopNDynamicFilter(ColumnHandle column, int channel, Type type, SortOrder order, BlockTypeOperators blockTypeOperators)
    {
        this.type = requireNonNull(type, "type is null");
        this.ordering = blockTypeOperators.generateBlockPositionOrdering(type, order);
        this.column = requireNonNull(column, "column is null");
        this.channel = channel;
        this.order = requireNonNull(order, "order is null");
        // Range uses the unordered-last comparison. For example, a row containing a
        // null field may sort first in TopN but last in a connector's range.
        canUseOrderedRange = !order.isNullsFirst() || typeHasNaN(type) || !blockTypeOperators.hasBothUnorderedComparisons(type);
        if (typeHasNaN(type)) {
            Block nan = NullableValue.of(type, FloatingPointValueSet.nanValue(type)).asBlock();
            Object minimum = FloatingPointValueSet.allOrderedValues(type).getSpan().getLowBoundedValue();
            nanComesFirst = ordering.order(nan, 0, NullableValue.of(type, minimum).asBlock(), 0) < 0;
        }
        else {
            nanComesFirst = false;
        }
    }

    synchronized void update(Page page, int position)
    {
        Block block = page.getBlock(channel);
        if (!canUseOrderedRange && !block.isNull(position)) {
            return;
        }
        if (boundary != null && ordering.order(block, position, boundary, 0) >= 0) {
            return;
        }
        if (block.getUnderlyingValueBlock().getRegionSizeInBytes(block.getUnderlyingValuePosition(position), 1) > MAX_BOUNDARY_BYTES) {
            return;
        }
        // Keep a single detached value so the filter does not retain the input page.
        boundary = block.getSingleValueBlock(position);
        Object value = readNativeValue(type, boundary, 0);
        Domain domain;
        if (value == null) {
            domain = order.isNullsFirst() ? Domain.onlyNull(type) : Domain.all(type);
        }
        else {
            domain = Domain.create(qualifyingValues(value), order.isNullsFirst());
        }
        predicate = TupleDomain.withColumnDomains(Map.of(column, domain));
    }

    private ValueSet qualifyingValues(Object value)
    {
        if (isFloatingPointNaN(type, value)) {
            return nanComesFirst ? ValueSet.of(type, value) : ValueSet.all(type);
        }
        Range range = order.isAscending() ? Range.lessThanOrEqual(type, value) : Range.greaterThanOrEqual(type, value);
        ValueSet values = ValueSet.ofRanges(range);
        if (values instanceof FloatingPointValueSet floatingPoint && nanComesFirst) {
            return new FloatingPointValueSet(floatingPoint.getOrderedValues(), true);
        }
        return values;
    }

    @Override
    public Set<ColumnHandle> getColumnsCovered()
    {
        return Set.of(column);
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return NOT_BLOCKED;
    }

    @Override
    public boolean isComplete()
    {
        return false;
    }

    @Override
    public boolean isAwaitable()
    {
        return false;
    }

    @Override
    public boolean shouldRefreshDuringScan()
    {
        return true;
    }

    @Override
    public TupleDomain<ColumnHandle> getCurrentPredicate()
    {
        return predicate;
    }
}
