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
package io.trino.plugin.memory;

import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;

import java.lang.invoke.MethodHandle;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.typeHasNaN;
import static java.util.Objects.requireNonNull;

/**
 * Evaluates the per-channel {@link Domain}s of a dynamic filter against whole pages.
 * <p>
 * The evaluators are built once per predicate and then applied column at a time, so
 * that the per-row work does not box values, allocate or look the domain up. Domains
 * whose type or shape is not specialized are evaluated with {@link Domain#includesNullableValue(Object)},
 * so the result always equals the one of evaluating that method for every position.
 */
final class DomainPageFilter
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    private final int[] channels;
    private final ColumnFilter[] filters;
    private final boolean matchesNothing;

    private DomainPageFilter(int[] channels, ColumnFilter[] filters, boolean matchesNothing)
    {
        this.channels = channels;
        this.filters = filters;
        this.matchesNothing = matchesNothing;
    }

    /**
     * @param domains domains by channel, all of them must match for a position to be selected
     */
    static DomainPageFilter create(Map<Integer, Domain> domains)
    {
        requireNonNull(domains, "domains is null");
        int[] channels = new int[domains.size()];
        ColumnFilter[] filters = new ColumnFilter[domains.size()];
        int size = 0;
        for (Map.Entry<Integer, Domain> entry : domains.entrySet()) {
            Domain domain = entry.getValue();
            if (domain.isNone()) {
                return new DomainPageFilter(new int[0], new ColumnFilter[0], true);
            }
            if (domain.isAll()) {
                continue;
            }
            channels[size] = entry.getKey();
            filters[size] = createColumnFilter(domain);
            size++;
        }
        return new DomainPageFilter(Arrays.copyOf(channels, size), Arrays.copyOf(filters, size), false);
    }

    /**
     * Writes the positions of the page that match all domains, in ascending order, to the start
     * of the given array.
     *
     * @param positions array with at least as many elements as the page has positions, it is used as scratch space
     * @return the number of matching positions
     */
    int filter(SourcePage page, int[] positions)
    {
        if (matchesNothing) {
            return 0;
        }
        int count = page.getPositionCount();
        for (int i = 0; i < count; i++) {
            positions[i] = i;
        }
        for (int i = 0; i < channels.length && count > 0; i++) {
            count = filters[i].filter(page.getBlock(channels[i]), positions, count);
        }
        return count;
    }

    private static ColumnFilter createColumnFilter(Domain domain)
    {
        Type type = domain.getType();
        ValueSet values = domain.getValues();
        boolean nullAllowed = domain.isNullAllowed();

        if (values.isNone()) {
            return new ConstantFilter(nullAllowed, false);
        }
        if (values.isAll()) {
            return new ConstantFilter(nullAllowed, true);
        }
        if (values instanceof SortedRangeSet ranges && !typeHasNaN(type)) {
            if (isLongBasedType(type)) {
                return createLongFilter(type, ranges, nullAllowed);
            }
            if (type instanceof VarcharType && ranges.isDiscreteSet()) {
                return new SliceSetFilter(type, ranges.getDiscreteSet().stream().map(Slice.class::cast).collect(toImmutableSet()), nullAllowed);
            }
            // Nested types keep using Domain
            if (!(type instanceof ArrayType || type instanceof MapType || type instanceof RowType)) {
                return new ComparisonRangesFilter(type, ranges, nullAllowed);
            }
        }
        return new DomainFilter(domain);
    }

    // Types whose values are read as long and are ordered the same way as the long values
    private static boolean isLongBasedType(Type type)
    {
        return type instanceof BigintType ||
                type instanceof IntegerType ||
                type instanceof SmallintType ||
                type instanceof TinyintType ||
                type instanceof DateType ||
                (type instanceof DecimalType decimalType && decimalType.isShort());
    }

    private static ColumnFilter createLongFilter(Type type, SortedRangeSet ranges, boolean nullAllowed)
    {
        Block sortedRanges = ranges.getSortedRanges();
        boolean[] inclusive = ranges.getInclusive();
        int rangeCount = ranges.getRangeCount();

        if (ranges.isDiscreteSet()) {
            long[] values = new long[rangeCount];
            for (int i = 0; i < rangeCount; i++) {
                values[i] = type.getLong(sortedRanges, 2 * i);
            }
            return new LongSetFilter(type, values, nullAllowed);
        }

        long[] low = new long[rangeCount];
        long[] high = new long[rangeCount];
        boolean[] lowInclusive = new boolean[rangeCount];
        boolean[] highInclusive = new boolean[rangeCount];
        for (int i = 0; i < rangeCount; i++) {
            if (sortedRanges.isNull(2 * i)) {
                low[i] = Long.MIN_VALUE;
                lowInclusive[i] = true;
            }
            else {
                low[i] = type.getLong(sortedRanges, 2 * i);
                lowInclusive[i] = inclusive[2 * i];
            }
            if (sortedRanges.isNull(2 * i + 1)) {
                high[i] = Long.MAX_VALUE;
                highInclusive[i] = true;
            }
            else {
                high[i] = type.getLong(sortedRanges, 2 * i + 1);
                highInclusive[i] = inclusive[2 * i + 1];
            }
        }
        return new LongRangesFilter(type, low, lowInclusive, high, highInclusive, nullAllowed);
    }

    private abstract static class ColumnFilter
    {
        /**
         * Compacts the first {@code count} entries of {@code positions} to the ones that match.
         *
         * @return the number of matching positions
         */
        final int filter(Block block, int[] positions, int count)
        {
            if (block instanceof RunLengthEncodedBlock runLengthEncodedBlock) {
                return matches(runLengthEncodedBlock.getValue(), 0) ? count : 0;
            }
            if (block instanceof DictionaryBlock dictionaryBlock && dictionaryBlock.getDictionary().getPositionCount() <= count) {
                Block dictionary = dictionaryBlock.getDictionary();
                boolean[] dictionaryMatches = new boolean[dictionary.getPositionCount()];
                for (int i = 0; i < dictionaryMatches.length; i++) {
                    dictionaryMatches[i] = matches(dictionary, i);
                }
                int length = 0;
                for (int i = 0; i < count; i++) {
                    int position = positions[i];
                    if (dictionaryMatches[dictionaryBlock.getId(position)]) {
                        positions[length++] = position;
                    }
                }
                return length;
            }
            return filterPositions(block, positions, count);
        }

        int filterPositions(Block block, int[] positions, int count)
        {
            int length = 0;
            for (int i = 0; i < count; i++) {
                int position = positions[i];
                if (matches(block, position)) {
                    positions[length++] = position;
                }
            }
            return length;
        }

        abstract boolean matches(Block block, int position);
    }

    private static final class ConstantFilter
            extends ColumnFilter
    {
        private final boolean nullMatches;
        private final boolean valueMatches;

        private ConstantFilter(boolean nullMatches, boolean valueMatches)
        {
            this.nullMatches = nullMatches;
            this.valueMatches = valueMatches;
        }

        @Override
        boolean matches(Block block, int position)
        {
            return block.isNull(position) ? nullMatches : valueMatches;
        }
    }

    private static final class LongSetFilter
            extends ColumnFilter
    {
        private final Type type;
        private final long[] values;
        private final long min;
        private final long max;
        private final boolean nullAllowed;

        private LongSetFilter(Type type, long[] values, boolean nullAllowed)
        {
            this.type = type;
            this.values = values;
            this.min = values[0];
            this.max = values[values.length - 1];
            this.nullAllowed = nullAllowed;
        }

        @Override
        boolean matches(Block block, int position)
        {
            if (block.isNull(position)) {
                return nullAllowed;
            }
            return contains(type.getLong(block, position));
        }

        @Override
        int filterPositions(Block block, int[] positions, int count)
        {
            int length = 0;
            for (int i = 0; i < count; i++) {
                int position = positions[i];
                boolean match;
                if (block.isNull(position)) {
                    match = nullAllowed;
                }
                else {
                    match = contains(type.getLong(block, position));
                }
                if (match) {
                    positions[length++] = position;
                }
            }
            return length;
        }

        private boolean contains(long value)
        {
            return value >= min && value <= max && Arrays.binarySearch(values, value) >= 0;
        }
    }

    // Ranges are sorted and do not overlap, unbounded ends are represented by inclusive Long.MIN_VALUE and Long.MAX_VALUE
    private static final class LongRangesFilter
            extends ColumnFilter
    {
        private final Type type;
        private final long[] low;
        private final boolean[] lowInclusive;
        private final long[] high;
        private final boolean[] highInclusive;
        private final boolean nullAllowed;

        private LongRangesFilter(Type type, long[] low, boolean[] lowInclusive, long[] high, boolean[] highInclusive, boolean nullAllowed)
        {
            this.type = type;
            this.low = low;
            this.lowInclusive = lowInclusive;
            this.high = high;
            this.highInclusive = highInclusive;
            this.nullAllowed = nullAllowed;
        }

        @Override
        boolean matches(Block block, int position)
        {
            if (block.isNull(position)) {
                return nullAllowed;
            }
            return contains(type.getLong(block, position));
        }

        @Override
        int filterPositions(Block block, int[] positions, int count)
        {
            int length = 0;
            for (int i = 0; i < count; i++) {
                int position = positions[i];
                boolean match;
                if (block.isNull(position)) {
                    match = nullAllowed;
                }
                else {
                    match = contains(type.getLong(block, position));
                }
                if (match) {
                    positions[length++] = position;
                }
            }
            return length;
        }

        private boolean contains(long value)
        {
            // find the first range with a low bound above the value, the only candidate is the one before it
            int fromIndex = 0;
            int toIndex = low.length;
            while (fromIndex < toIndex) {
                int middle = (fromIndex + toIndex) >>> 1;
                if (low[middle] < value || (lowInclusive[middle] && low[middle] == value)) {
                    fromIndex = middle + 1;
                }
                else {
                    toIndex = middle;
                }
            }
            int candidate = fromIndex - 1;
            if (candidate < 0) {
                return false;
            }
            return value < high[candidate] || (highInclusive[candidate] && value == high[candidate]);
        }
    }

    private static final class SliceSetFilter
            extends ColumnFilter
    {
        private final Type type;
        private final Set<Slice> values;
        private final boolean nullAllowed;

        private SliceSetFilter(Type type, Set<Slice> values, boolean nullAllowed)
        {
            this.type = type;
            this.values = values;
            this.nullAllowed = nullAllowed;
        }

        @Override
        boolean matches(Block block, int position)
        {
            if (block.isNull(position)) {
                return nullAllowed;
            }
            return values.contains(type.getSlice(block, position));
        }
    }

    // Same search as SortedRangeSet#containsValue, but reading the values from the block without boxing them
    private static final class ComparisonRangesFilter
            extends ColumnFilter
    {
        private final MethodHandle comparisonOperator;
        private final Block sortedRanges;
        private final boolean[] inclusive;
        private final int rangeCount;
        private final boolean nullAllowed;

        private ComparisonRangesFilter(Type type, SortedRangeSet ranges, boolean nullAllowed)
        {
            this.comparisonOperator = TYPE_OPERATORS.getComparisonUnorderedLastOperator(type, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL, BLOCK_POSITION_NOT_NULL));
            this.sortedRanges = ranges.getSortedRanges();
            this.inclusive = ranges.getInclusive();
            this.rangeCount = ranges.getRangeCount();
            this.nullAllowed = nullAllowed;
        }

        @Override
        boolean matches(Block block, int position)
        {
            if (block.isNull(position)) {
                return nullAllowed;
            }
            Block value = block.getUnderlyingValueBlock();
            int valuePosition = block.getUnderlyingValuePosition(position);

            // find the first range with a low bound above the value, the only candidate is the one before it
            int fromIndex = 0;
            int toIndex = rangeCount;
            while (fromIndex < toIndex) {
                int middle = (fromIndex + toIndex) >>> 1;
                if (isLowBoundAtOrBelow(middle, value, valuePosition)) {
                    fromIndex = middle + 1;
                }
                else {
                    toIndex = middle;
                }
            }
            int candidate = fromIndex - 1;
            return candidate >= 0 && isHighBoundAtOrAbove(candidate, value, valuePosition);
        }

        private boolean isLowBoundAtOrBelow(int range, Block value, int valuePosition)
        {
            int boundPosition = 2 * range;
            if (sortedRanges.isNull(boundPosition)) {
                return true;
            }
            long comparison = compare(value, valuePosition, boundPosition);
            return comparison > 0 || (comparison == 0 && inclusive[boundPosition]);
        }

        private boolean isHighBoundAtOrAbove(int range, Block value, int valuePosition)
        {
            int boundPosition = 2 * range + 1;
            if (sortedRanges.isNull(boundPosition)) {
                return true;
            }
            long comparison = compare(value, valuePosition, boundPosition);
            return comparison < 0 || (comparison == 0 && inclusive[boundPosition]);
        }

        private long compare(Block value, int valuePosition, int boundPosition)
        {
            try {
                return (long) comparisonOperator.invokeExact(value, valuePosition, sortedRanges, boundPosition);
            }
            catch (Throwable throwable) {
                throwIfUnchecked(throwable);
                throw new RuntimeException(throwable);
            }
        }
    }

    // Used for everything that is not specialized, including values that can be NaN
    private static final class DomainFilter
            extends ColumnFilter
    {
        private final Domain domain;
        private final Type type;

        private DomainFilter(Domain domain)
        {
            this.domain = domain;
            this.type = domain.getType();
        }

        @Override
        boolean matches(Block block, int position)
        {
            return domain.includesNullableValue(readNativeValue(type, block, position));
        }
    }
}
