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
package io.trino.spi.predicate;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.predicate.Utils.TUPLE_DOMAIN_TYPE_OPERATORS;
import static io.trino.spi.predicate.Utils.handleThrowable;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.TypeUtils.isFloatingPointNaN;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static java.lang.Boolean.TRUE;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/**
 * A set containing zero or more Ranges of the same type over a continuous space of possible values.
 * Ranges are coalesced into the most compact representation of non-overlapping Ranges. This structure
 * allows iteration across these compacted Ranges in increasing order, as well as other common
 * set-related operation.
 */
public final class SortedRangeSet
        implements ValueSet
{
    private final Type type;
    private final MethodHandle equalOperator;
    private final MethodHandle hashCodeOperator;
    private final MethodHandle comparisonOperator;

    private final boolean[] inclusive;
    private final Block sortedRanges;

    private int lazyHash;

    private SortedRangeSet(Type type, boolean[] inclusive, Block sortedRanges)
    {
        requireNonNull(type, "type is null");
        if (!type.isOrderable()) {
            throw new IllegalArgumentException("Type is not orderable: " + type);
        }
        this.type = type;
        this.equalOperator = TUPLE_DOMAIN_TYPE_OPERATORS.getEqualOperator(type, simpleConvention(NULLABLE_RETURN, BLOCK_POSITION, BLOCK_POSITION));
        this.hashCodeOperator = TUPLE_DOMAIN_TYPE_OPERATORS.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION));
        // choice of placing unordered values first or last does not matter for this code
        this.comparisonOperator = TUPLE_DOMAIN_TYPE_OPERATORS.getComparisonUnorderedLastOperator(type, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION));

        requireNonNull(inclusive, "inclusive is null");
        requireNonNull(sortedRanges, "sortedRanges is null");
        if (inclusive.length % 2 != 0) {
            throw new IllegalArgumentException("Malformed inclusive markers");
        }
        if (inclusive.length != sortedRanges.getPositionCount()) {
            throw new IllegalArgumentException(format("Size mismatch between inclusive markers and sortedRanges block: %s, %s", inclusive.length, sortedRanges.getPositionCount()));
        }
        for (int position = 0; position < sortedRanges.getPositionCount(); position++) {
            if (sortedRanges.isNull(position)) {
                if (inclusive[position]) {
                    throw new IllegalArgumentException("Invalid inclusive marker for null value at position " + position);
                }
                if (position != 0 && position != sortedRanges.getPositionCount() - 1) {
                    throw new IllegalArgumentException(format("Invalid null value at position %s of %s", position, sortedRanges.getPositionCount()));
                }
            }
        }
        this.inclusive = inclusive;
        this.sortedRanges = sortedRanges;
    }

    static SortedRangeSet none(Type type)
    {
        return new SortedRangeSet(
                type,
                new boolean[0],
                // TODO This can perhaps use an empty block singleton
                type.createBlockBuilder(null, 0).build());
    }

    static SortedRangeSet all(Type type)
    {
        return new SortedRangeSet(
                type,
                new boolean[] {false, false},
                // TODO This can perhaps use a "block with two nulls" singleton
                type.createBlockBuilder(null, 2)
                        .appendNull()
                        .appendNull()
                        .build());
    }

    @Deprecated // For JSON deserialization only
    @JsonCreator
    public static SortedRangeSet fromJson(
            @JsonProperty("type") Type type,
            @JsonProperty("inclusive") boolean[] inclusive,
            @JsonProperty("sortedRanges") Block sortedRanges)
    {
        if (sortedRanges instanceof BlockBuilder) {
            throw new IllegalArgumentException("sortedRanges must be a block: " + sortedRanges);
        }
        return new SortedRangeSet(type, inclusive.clone(), sortedRanges);
    }

    /**
     * Provided discrete values that are unioned together to form the SortedRangeSet
     */
    static SortedRangeSet of(Type type, Object first, Object... rest)
    {
        if (rest.length == 0) {
            return of(type, first);
        }

        BlockBuilder blockBuilder = type.createBlockBuilder(null, 1 + rest.length);
        checkNotNaN(type, first);
        writeNativeValue(type, blockBuilder, first);
        for (Object value : rest) {
            checkNotNaN(type, value);
            writeNativeValue(type, blockBuilder, value);
        }
        Block block = blockBuilder.build();

        return fromUnorderedValuesBlock(type, block);
    }

    static SortedRangeSet of(Type type, Collection<?> values)
    {
        if (values.isEmpty()) {
            return none(type);
        }

        BlockBuilder blockBuilder = type.createBlockBuilder(null, values.size());
        for (Object value : values) {
            checkNotNaN(type, value);
            writeNativeValue(type, blockBuilder, value);
        }
        Block block = blockBuilder.build();

        return fromUnorderedValuesBlock(type, block);
    }

    private static SortedRangeSet fromUnorderedValuesBlock(Type type, Block block)
    {
        // choice of placing unordered values first or last does not matter for this code
        MethodHandle comparisonOperator = TUPLE_DOMAIN_TYPE_OPERATORS.getComparisonUnorderedLastOperator(type, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION));

        List<Integer> indexes = new ArrayList<>(block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            indexes.add(position);
        }
        indexes.sort((left, right) -> compareValues(comparisonOperator, block, left, block, right));

        int[] dictionary = new int[block.getPositionCount() * 2];
        dictionary[0] = indexes.get(0);
        dictionary[1] = indexes.get(0);
        int dictionaryIndex = 2;

        for (int i = 1; i < indexes.size(); i++) {
            int compare = compareValues(comparisonOperator, block, indexes.get(i - 1), block, indexes.get(i));
            if (compare > 0) {
                throw new IllegalStateException("Values not sorted");
            }
            if (compare == 0) {
                // equal, skip
                continue;
            }
            dictionary[dictionaryIndex] = indexes.get(i);
            dictionaryIndex++;
            dictionary[dictionaryIndex] = indexes.get(i);
            dictionaryIndex++;
        }

        boolean[] inclusive = new boolean[dictionaryIndex];
        Arrays.fill(inclusive, true);

        return new SortedRangeSet(
                type,
                inclusive,
                new DictionaryBlock(
                        dictionaryIndex,
                        block,
                        dictionary));
    }

    /**
     * Provided Ranges are unioned together to form the SortedRangeSet
     */
    static SortedRangeSet of(Range first, Range... rest)
    {
        if (rest.length == 0 && first.isSingleValue()) {
            return of(first.getType(), first.getSingleValue());
        }

        List<Range> rangeList = new ArrayList<>(rest.length + 1);
        rangeList.add(first);
        rangeList.addAll(asList(rest));
        return copyOf(first.getType(), rangeList);
    }

    static SortedRangeSet of(List<Range> rangeList)
    {
        if (rangeList.isEmpty()) {
            throw new IllegalArgumentException("cannot use empty rangeList");
        }
        return copyOf(rangeList.get(0).getType(), rangeList);
    }

    private static SortedRangeSet of(Type type, Object value)
    {
        checkNotNaN(type, value);
        Block block = nativeValueToBlock(type, value);
        return new SortedRangeSet(
                type,
                new boolean[] {true, true},
                new RunLengthEncodedBlock(block, 2));
    }

    static SortedRangeSet copyOf(Type type, Iterable<Range> ranges)
    {
        return new Builder(type).addAll(ranges).build();
    }

    /**
     * Provided Ranges are unioned together to form the SortedRangeSet
     */
    public static SortedRangeSet copyOf(Type type, List<Range> ranges)
    {
        return copyOf(type, (Iterable<Range>) ranges);
    }

    @Override
    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public boolean[] getInclusive()
    {
        return inclusive;
    }

    @JsonProperty
    public Block getSortedRanges()
    {
        return sortedRanges;
    }

    public List<Range> getOrderedRanges()
    {
        List<Range> ranges = new ArrayList<>(getRangeCount());
        for (int rangeIndex = 0; rangeIndex < getRangeCount(); rangeIndex++) {
            ranges.add(getRange(rangeIndex));
        }
        return unmodifiableList(ranges);
    }

    public int getRangeCount()
    {
        return inclusive.length / 2;
    }

    @Override
    public boolean isNone()
    {
        return getRangeCount() == 0;
    }

    @Override
    public boolean isAll()
    {
        if (getRangeCount() != 1) {
            return false;
        }
        RangeView onlyRange = getRangeView(0);
        return onlyRange.isLowUnbounded() && onlyRange.isHighUnbounded();
    }

    @Override
    public boolean isSingleValue()
    {
        return getRangeCount() == 1 && getRangeView(0).isSingleValue();
    }

    @Override
    public Object getSingleValue()
    {
        if (getRangeCount() == 1) {
            Optional<Object> singleValue = getRangeView(0).getSingleValue();
            if (singleValue.isPresent()) {
                return singleValue.get();
            }
        }
        throw new IllegalStateException("SortedRangeSet does not have just a single value");
    }

    @Override
    public boolean isDiscreteSet()
    {
        for (int i = 0; i < getRangeCount(); i++) {
            if (!getRangeView(i).isSingleValue()) {
                return false;
            }
        }
        return !isNone();
    }

    @Override
    public List<Object> getDiscreteSet()
    {
        List<Object> values = new ArrayList<>(getRangeCount());
        for (int rangeIndex = 0; rangeIndex < getRangeCount(); rangeIndex++) {
            RangeView range = getRangeView(rangeIndex);
            values.add(range.getSingleValue()
                    .orElseThrow(() -> new IllegalStateException("SortedRangeSet is not a discrete set")));
        }
        return unmodifiableList(values);
    }

    @Override
    public boolean containsValue(Object value)
    {
        requireNonNull(value, "value is null");
        if (isFloatingPointNaN(type, value)) {
            return isAll();
        }
        if (isNone()) {
            return false;
        }

        Block valueAsBlock = nativeValueToBlock(type, value);
        RangeView valueRange = new RangeView(
                type,
                comparisonOperator,
                true,
                valueAsBlock,
                0,
                true,
                valueAsBlock,
                0);

        // first candidate
        int lowRangeIndex = 0;
        // first non-candidate
        int highRangeIndex = getRangeCount();

        while (lowRangeIndex + 1 < highRangeIndex) {
            int midRangeIndex = (lowRangeIndex + highRangeIndex) >>> 1;
            int compare = getRangeView(midRangeIndex).compareLowBound(valueRange);
            if (compare <= 0) {
                // search value is in current range, or above
                lowRangeIndex = midRangeIndex;
            }
            else {
                // search value is less than current range min
                highRangeIndex = midRangeIndex;
            }
        }

        return getRangeView(lowRangeIndex).overlaps(valueRange);
    }

    public Range getSpan()
    {
        if (isNone()) {
            throw new IllegalStateException("Cannot get span if no ranges exist");
        }
        int lastIndex = (getRangeCount() - 1) * 2 + 1;
        return new RangeView(
                type,
                comparisonOperator,
                inclusive[0],
                sortedRanges,
                0,
                inclusive[lastIndex],
                sortedRanges,
                lastIndex)
                .toRange();
    }

    private Range getRange(int rangeIndex)
    {
        return getRangeView(rangeIndex).toRange();
    }

    private RangeView getRangeView(int rangeIndex)
    {
        int rangeLeft = 2 * rangeIndex;
        int rangeRight = 2 * rangeIndex + 1;
        return new RangeView(
                type,
                comparisonOperator,
                inclusive[rangeLeft],
                sortedRanges,
                rangeLeft,
                inclusive[rangeRight],
                sortedRanges,
                rangeRight);
    }

    @Override
    public Ranges getRanges()
    {
        return new Ranges()
        {
            @Override
            public int getRangeCount()
            {
                return SortedRangeSet.this.getRangeCount();
            }

            @Override
            public List<Range> getOrderedRanges()
            {
                return SortedRangeSet.this.getOrderedRanges();
            }

            @Override
            public Range getSpan()
            {
                return SortedRangeSet.this.getSpan();
            }
        };
    }

    @Override
    public ValuesProcessor getValuesProcessor()
    {
        return new ValuesProcessor()
        {
            @Override
            public <T> T transform(Function<Ranges, T> rangesFunction, Function<DiscreteValues, T> valuesFunction, Function<AllOrNone, T> allOrNoneFunction)
            {
                return rangesFunction.apply(getRanges());
            }

            @Override
            public void consume(Consumer<Ranges> rangesConsumer, Consumer<DiscreteValues> valuesConsumer, Consumer<AllOrNone> allOrNoneConsumer)
            {
                rangesConsumer.accept(getRanges());
            }
        };
    }

    @Override
    public SortedRangeSet intersect(ValueSet other)
    {
        SortedRangeSet that = checkCompatibility(other);

        if (this.isNone()) {
            return this;
        }
        if (that.isNone()) {
            return that;
        }

        int thisRangeCount = this.getRangeCount();
        int thatRangeCount = that.getRangeCount();

        boolean[] inclusive = new boolean[2 * (thisRangeCount + thatRangeCount)];
        BlockBuilder blockBuilder = type.createBlockBuilder(null, 2 * (thisRangeCount + thatRangeCount));
        int resultRangeIndex = 0;

        int thisNextRangeIndex = 0;
        int thatNextRangeIndex = 0;

        while (thisNextRangeIndex < thisRangeCount && thatNextRangeIndex < thatRangeCount) {
            RangeView thisCurrent = this.getRangeView(thisNextRangeIndex);
            RangeView thatCurrent = that.getRangeView(thatNextRangeIndex);

            Optional<RangeView> intersect = thisCurrent.tryIntersect(thatCurrent);
            if (intersect.isPresent()) {
                writeRange(type, blockBuilder, inclusive, resultRangeIndex, intersect.get());
                resultRangeIndex++;
            }
            int compare = thisCurrent.compareHighBound(thatCurrent);
            if (compare == 0) {
                thisNextRangeIndex++;
                thatNextRangeIndex++;
            }
            if (compare < 0) {
                thisNextRangeIndex++;
            }
            if (compare > 0) {
                thatNextRangeIndex++;
            }
        }

        if (resultRangeIndex * 2 < inclusive.length) {
            inclusive = Arrays.copyOf(inclusive, resultRangeIndex * 2);
        }

        return new SortedRangeSet(type, inclusive, blockBuilder.build());
    }

    @Override
    public boolean overlaps(ValueSet other)
    {
        SortedRangeSet that = checkCompatibility(other);

        if (this.isNone() || that.isNone()) {
            return false;
        }

        int thisRangeCount = this.getRangeCount();
        int thatRangeCount = that.getRangeCount();

        int thisNextRangeIndex = 0;
        int thatNextRangeIndex = 0;
        while (thisNextRangeIndex < thisRangeCount && thatNextRangeIndex < thatRangeCount) {
            RangeView thisCurrent = this.getRangeView(thisNextRangeIndex);
            RangeView thatCurrent = that.getRangeView(thatNextRangeIndex);
            if (thisCurrent.overlaps(thatCurrent)) {
                return true;
            }
            int compare = thisCurrent.compareTo(thatCurrent);
            if (compare < 0) {
                thisNextRangeIndex++;
            }
            if (compare > 0) {
                thatNextRangeIndex++;
            }
        }

        return false;
    }

    @Override
    public SortedRangeSet union(Collection<ValueSet> valueSets)
    {
        if (this.isAll()) {
            return this;
        }

        // Logically this organizes all value sets (this and valueSets) into a binary tree and merges them pairwise, bottom-up.
        // TODO generalize union(SortedRangeSet) to merge multiple sources at once

        List<SortedRangeSet> toUnion = new ArrayList<>(1 + valueSets.size());
        toUnion.add(this);
        for (ValueSet valueSet : valueSets) {
            SortedRangeSet other = checkCompatibility(valueSet);
            if (other.isAll()) {
                return other;
            }
            toUnion.add(other);
        }

        while (toUnion.size() > 1) {
            List<SortedRangeSet> unioned = new ArrayList<>((toUnion.size() + 1) / 2);
            for (int i = 0; i < toUnion.size() - 1; i += 2) {
                unioned.add(toUnion.get(i).union(toUnion.get(i + 1)));
            }
            if (toUnion.size() % 2 != 0) {
                unioned.add(toUnion.get(toUnion.size() - 1));
            }
            toUnion = unioned;
        }

        return toUnion.get(0);
    }

    @Override
    public SortedRangeSet union(ValueSet other)
    {
        SortedRangeSet that = checkCompatibility(other);

        if (this.isAll()) {
            return this;
        }
        if (that.isAll()) {
            return that;
        }
        if (this == that) {
            return this;
        }

        int thisRangeCount = this.getRangeCount();
        int thatRangeCount = that.getRangeCount();

        boolean[] inclusive = new boolean[2 * (thisRangeCount + thatRangeCount)];
        BlockBuilder blockBuilder = type.createBlockBuilder(null, 2 * (thisRangeCount + thatRangeCount));
        int resultRangeIndex = 0;

        int thisNextRangeIndex = 0;
        int thatNextRangeIndex = 0;
        RangeView current = null;
        while (thisNextRangeIndex < thisRangeCount || thatNextRangeIndex < thatRangeCount) {
            RangeView next;
            if (thisNextRangeIndex == thisRangeCount) {
                // this exhausted
                next = that.getRangeView(thatNextRangeIndex);
                thatNextRangeIndex++;
            }
            else if (thatNextRangeIndex == thatRangeCount) {
                // that exhausted
                next = this.getRangeView(thisNextRangeIndex);
                thisNextRangeIndex++;
            }
            else {
                // both are not exhausted yet
                RangeView thisNext = this.getRangeView(thisNextRangeIndex);
                RangeView thatNext = that.getRangeView(thatNextRangeIndex);
                if (thisNext.compareTo(thatNext) <= 0) {
                    next = thisNext;
                    thisNextRangeIndex++;
                }
                else {
                    next = thatNext;
                    thatNextRangeIndex++;
                }
            }

            if (current != null) {
                Optional<RangeView> merged = current.tryMergeWithNext(next);
                if (merged.isPresent()) {
                    current = merged.get();
                }
                else {
                    writeRange(type, blockBuilder, inclusive, resultRangeIndex, current);
                    resultRangeIndex++;
                    current = next;
                }
            }
            else {
                current = next;
            }
        }
        if (current != null) {
            writeRange(type, blockBuilder, inclusive, resultRangeIndex, current);
            resultRangeIndex++;
        }

        if (resultRangeIndex * 2 < inclusive.length) {
            inclusive = Arrays.copyOf(inclusive, resultRangeIndex * 2);
        }

        return new SortedRangeSet(type, inclusive, blockBuilder.build());
    }

    @Override
    public SortedRangeSet complement()
    {
        if (isNone()) {
            return all(type);
        }
        if (isAll()) {
            return none(type);
        }

        RangeView first = getRangeView(0);
        RangeView last = getRangeView(getRangeCount() - 1);

        int resultRanges = getRangeCount() - 1;
        if (!first.isLowUnbounded()) {
            resultRanges++;
        }
        if (!last.isHighUnbounded()) {
            resultRanges++;
        }

        boolean[] inclusive = new boolean[2 * resultRanges];
        BlockBuilder blockBuilder = type.createBlockBuilder(null, 2 * resultRanges);
        int resultRangeIndex = 0;

        if (!first.isLowUnbounded()) {
            inclusive[2 * resultRangeIndex] = false;
            inclusive[2 * resultRangeIndex + 1] = !first.lowInclusive;
            blockBuilder.appendNull();
            type.appendTo(first.lowValueBlock, first.lowValuePosition, blockBuilder);
            resultRangeIndex++;
        }

        RangeView previous = first;
        for (int rangeIndex = 1; rangeIndex < getRangeCount(); rangeIndex++) {
            RangeView current = getRangeView(rangeIndex);

            inclusive[2 * resultRangeIndex] = !previous.highInclusive;
            inclusive[2 * resultRangeIndex + 1] = !current.lowInclusive;
            type.appendTo(previous.highValueBlock, previous.highValuePosition, blockBuilder);
            type.appendTo(current.lowValueBlock, current.lowValuePosition, blockBuilder);
            resultRangeIndex++;

            previous = current;
        }

        if (!last.isHighUnbounded()) {
            inclusive[2 * resultRangeIndex] = !last.highInclusive;
            inclusive[2 * resultRangeIndex + 1] = false;
            type.appendTo(last.highValueBlock, last.highValuePosition, blockBuilder);
            blockBuilder.appendNull();
            resultRangeIndex++;
        }

        if (resultRangeIndex * 2 != inclusive.length) {
            throw new IllegalStateException("Incorrect number of ranges written");
        }

        return new SortedRangeSet(
                type,
                inclusive,
                blockBuilder.build());
    }

    private SortedRangeSet checkCompatibility(ValueSet other)
    {
        if (!getType().equals(other.getType())) {
            throw new IllegalStateException(format("Mismatched types: %s vs %s", getType(), other.getType()));
        }
        if (!(other instanceof SortedRangeSet)) {
            throw new IllegalStateException(format("ValueSet is not a SortedRangeSet: %s", other.getClass()));
        }
        return (SortedRangeSet) other;
    }

    @Override
    public int hashCode()
    {
        int hash = lazyHash;
        if (hash == 0) {
            hash = Objects.hash(type, Arrays.hashCode(inclusive));
            for (int position = 0; position < sortedRanges.getPositionCount(); position++) {
                if (sortedRanges.isNull(position)) {
                    hash = hash * 31;
                    continue;
                }
                try {
                    hash = hash * 31 + (int) (long) hashCodeOperator.invokeExact(sortedRanges, position);
                }
                catch (Throwable throwable) {
                    throw handleThrowable(throwable);
                }
            }
            if (hash == 0) {
                hash = 1;
            }
            lazyHash = hash;
        }
        return hash;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SortedRangeSet other = (SortedRangeSet) obj;
        return hashCode() == other.hashCode() && // compare hash codes because they are cached, so this is cheap and efficient
                Objects.equals(this.type, other.type) &&
                Arrays.equals(this.inclusive, other.inclusive) &&
                blocksEqual(this.sortedRanges, other.sortedRanges);
    }

    private boolean blocksEqual(Block leftBlock, Block rightBlock)
    {
        if (leftBlock.getPositionCount() != rightBlock.getPositionCount()) {
            return false;
        }
        for (int position = 0; position < leftBlock.getPositionCount(); position++) {
            if (!valuesEqual(leftBlock, position, rightBlock, position)) {
                return false;
            }
        }
        return true;
    }

    private boolean valuesEqual(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        boolean leftIsNull = leftBlock.isNull(leftPosition);
        boolean rightIsNull = rightBlock.isNull(rightPosition);
        if (leftIsNull || rightIsNull) {
            // TODO this should probably use IS NOT DISTINCT FROM
            return leftIsNull == rightIsNull;
        }
        Boolean equal;
        try {
            equal = (Boolean) equalOperator.invokeExact(leftBlock, leftPosition, rightBlock, rightPosition);
        }
        catch (Throwable throwable) {
            throw handleThrowable(throwable);
        }
        return TRUE.equals(equal);
    }

    private static int compareValues(MethodHandle comparisonOperator, Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        try {
            return (int) (long) comparisonOperator.invokeExact(leftBlock, leftPosition, rightBlock, rightPosition);
        }
        catch (Throwable throwable) {
            throw handleThrowable(throwable);
        }
    }

    @Override
    public String toString()
    {
        return toString(ToStringSession.INSTANCE);
    }

    @Override
    public String toString(ConnectorSession session)
    {
        return toString(session, 10);
    }

    @Override
    public String toString(ConnectorSession session, int limit)
    {
        return new StringJoiner(", ", SortedRangeSet.class.getSimpleName() + "[", "]")
                .add("type=" + type)
                .add("ranges=" + getRangeCount())
                .add(formatRanges(session, limit))
                .toString();
    }

    private String formatRanges(ConnectorSession session, int limit)
    {
        if (isNone()) {
            return "{}";
        }
        if (getRangeCount() == 1) {
            return "{" + getRangeView(0).formatRange(session) + "}";
        }
        if (limit < 2) {
            return format("{%s, ...}", getRangeView(0).formatRange(session));
        }
        // Print first (limit - 1) elements, followed by last element
        // to provide a readable summary of the contents
        Stream<String> prefix = Stream.concat(
                IntStream.range(0, min(getRangeCount(), limit) - 1)
                        .mapToObj(this::getRangeView)
                        .map(rangeView -> rangeView.formatRange(session)),
                limit < getRangeCount() ? Stream.of("...") : Stream.of());

        Stream<String> suffix = Stream.of(
                getRangeView(getRangeCount() - 1).formatRange(session));

        return Stream.concat(prefix, suffix)
                .collect(joining(", ", "{", "}"));
    }

    static class Builder
    {
        private final Type type;
        private final List<Range> ranges = new ArrayList<>();

        Builder(Type type)
        {
            requireNonNull(type, "type is null");

            if (!type.isOrderable()) {
                throw new IllegalArgumentException("Type is not orderable: " + type);
            }
            this.type = type;
        }

        Builder add(Range range)
        {
            if (!type.equals(range.getType())) {
                throw new IllegalArgumentException(format("Range type %s does not match builder type %s", range.getType(), type));
            }

            ranges.add(range);
            return this;
        }

        Builder addAll(Iterable<Range> ranges)
        {
            for (Range range : ranges) {
                add(range);
            }
            return this;
        }

        SortedRangeSet build()
        {
            ranges.sort(Range::compareLowBound);

            List<Range> result = new ArrayList<>(ranges.size());

            Range current = null;
            for (Range next : ranges) {
                if (current == null) {
                    current = next;
                    continue;
                }

                Optional<Range> merged = current.tryMergeWithNext(next);
                if (merged.isPresent()) {
                    current = merged.get();
                }
                else {
                    result.add(current);
                    current = next;
                }
            }

            if (current != null) {
                result.add(current);
            }

            boolean[] inclusive = new boolean[2 * result.size()];
            BlockBuilder blockBuilder = type.createBlockBuilder(null, 2 * result.size());
            for (int rangeIndex = 0; rangeIndex < result.size(); rangeIndex++) {
                Range range = result.get(rangeIndex);
                writeRange(type, blockBuilder, inclusive, rangeIndex, range);
            }

            return new SortedRangeSet(type, inclusive, blockBuilder);
        }
    }

    private static void writeRange(Type type, BlockBuilder blockBuilder, boolean[] inclusive, int rangeIndex, Range range)
    {
        inclusive[2 * rangeIndex] = range.isLowInclusive();
        inclusive[2 * rangeIndex + 1] = range.isHighInclusive();
        writeNativeValue(type, blockBuilder, range.getLowValue().orElse(null));
        writeNativeValue(type, blockBuilder, range.getHighValue().orElse(null));
    }

    private static void writeRange(Type type, BlockBuilder blockBuilder, boolean[] inclusive, int rangeIndex, RangeView range)
    {
        inclusive[2 * rangeIndex] = range.lowInclusive;
        inclusive[2 * rangeIndex + 1] = range.highInclusive;
        type.appendTo(range.lowValueBlock, range.lowValuePosition, blockBuilder);
        type.appendTo(range.highValueBlock, range.highValuePosition, blockBuilder);
    }

    private static void checkNotNaN(Type type, Object value)
    {
        if (isFloatingPointNaN(type, value)) {
            throw new IllegalArgumentException("cannot use NaN as range bound");
        }
    }

    private static class RangeView
            implements Comparable<RangeView>
    {
        private final Type type;
        private final MethodHandle comparisonOperator;

        private final boolean lowInclusive;
        private final Block lowValueBlock;
        private final int lowValuePosition;

        private final boolean highInclusive;
        private final Block highValueBlock;
        private final int highValuePosition;

        RangeView(
                Type type,
                MethodHandle comparisonOperator,
                boolean lowInclusive,
                Block lowValueBlock,
                int lowValuePosition,
                boolean highInclusive,
                Block highValueBlock,
                int highValuePosition)
        {
            this.type = type;
            this.comparisonOperator = comparisonOperator;
            this.lowInclusive = lowInclusive;
            this.lowValueBlock = lowValueBlock;
            this.lowValuePosition = lowValuePosition;
            this.highInclusive = highInclusive;
            this.highValueBlock = highValueBlock;
            this.highValuePosition = highValuePosition;
        }

        public Range toRange()
        {
            Object low = readNativeValue(type, lowValueBlock, lowValuePosition);
            Object high = readNativeValue(type, highValueBlock, highValuePosition);
            return new Range(type, lowInclusive, Optional.ofNullable(low), highInclusive, Optional.ofNullable(high));
        }

        @Override
        public int compareTo(RangeView that)
        {
            int compare;
            compare = compareLowBound(that);
            if (compare != 0) {
                return compare;
            }
            compare = compareHighBound(that);
            if (compare != 0) {
                return compare;
            }

            return 0;
        }

        private int compareLowBound(RangeView that)
        {
            if (this.isLowUnbounded() || that.isLowUnbounded()) {
                return Boolean.compare(!this.isLowUnbounded(), !that.isLowUnbounded());
            }
            int compare = compareValues(comparisonOperator, this.lowValueBlock, this.lowValuePosition, that.lowValueBlock, that.lowValuePosition);
            if (compare != 0) {
                return compare;
            }
            return Boolean.compare(!this.lowInclusive, !that.lowInclusive);
        }

        private int compareHighBound(RangeView that)
        {
            if (this.isHighUnbounded() || that.isHighUnbounded()) {
                return Boolean.compare(this.isHighUnbounded(), that.isHighUnbounded());
            }
            int compare = compareValues(comparisonOperator, this.highValueBlock, this.highValuePosition, that.highValueBlock, that.highValuePosition);
            if (compare != 0) {
                return compare;
            }
            return Boolean.compare(this.highInclusive, that.highInclusive);
        }

        /**
         * Returns unioned range if {@code this} and {@code next} overlap or are adjacent.
         * The {@code next} lower bound must not be before {@code this} lower bound.
         */
        public Optional<RangeView> tryMergeWithNext(RangeView next)
        {
            if (this.compareTo(next) > 0) {
                throw new IllegalArgumentException("next before this");
            }

            if (this.isHighUnbounded()) {
                return Optional.of(this);
            }

            boolean merge;
            if (next.isLowUnbounded()) {
                // both are low-unbounded
                merge = true;
            }
            else {
                int compare = compareValues(comparisonOperator, this.highValueBlock, this.highValuePosition, next.lowValueBlock, next.lowValuePosition);
                merge = compare > 0  // overlap
                        || compare == 0 && (this.highInclusive || next.lowInclusive); // adjacent
            }
            if (merge) {
                int compareHighBound = compareHighBound(next);
                return Optional.of(new RangeView(
                        this.type,
                        this.comparisonOperator,
                        this.lowInclusive,
                        this.lowValueBlock,
                        this.lowValuePosition,
                        // max of high bounds
                        compareHighBound <= 0 ? next.highInclusive : this.highInclusive,
                        compareHighBound <= 0 ? next.highValueBlock : this.highValueBlock,
                        compareHighBound <= 0 ? next.highValuePosition : this.highValuePosition));
            }

            return Optional.empty();
        }

        public boolean isLowUnbounded()
        {
            return lowValueBlock.isNull(lowValuePosition);
        }

        public boolean isHighUnbounded()
        {
            return highValueBlock.isNull(highValuePosition);
        }

        public boolean isSingleValue()
        {
            return lowInclusive &&
                    highInclusive &&
                    // in SQL types, comparing with comparison is guaranteed to be consistent with equals
                    compareValues(comparisonOperator, lowValueBlock, lowValuePosition, highValueBlock, highValuePosition) == 0;
        }

        public Optional<Object> getSingleValue()
        {
            if (!isSingleValue()) {
                return Optional.empty();
            }
            // The value cannot be null
            return Optional.of(readNativeValue(type, lowValueBlock, lowValuePosition));
        }

        public boolean overlaps(RangeView that)
        {
            return !this.isFullyBefore(that) && !that.isFullyBefore(this);
        }

        public Optional<RangeView> tryIntersect(RangeView that)
        {
            if (!overlaps(that)) {
                return Optional.empty();
            }

            int compareLowBound = compareLowBound(that);
            int compareHighBound = compareHighBound(that);

            return Optional.of(new RangeView(
                    type,
                    comparisonOperator,
                    // max of low bounds
                    compareLowBound <= 0 ? that.lowInclusive : this.lowInclusive,
                    compareLowBound <= 0 ? that.lowValueBlock : this.lowValueBlock,
                    compareLowBound <= 0 ? that.lowValuePosition : this.lowValuePosition,
                    // min of high bounds
                    compareHighBound <= 0 ? this.highInclusive : that.highInclusive,
                    compareHighBound <= 0 ? this.highValueBlock : that.highValueBlock,
                    compareHighBound <= 0 ? this.highValuePosition : that.highValuePosition));
        }

        private boolean isFullyBefore(RangeView that)
        {
            if (this.isHighUnbounded()) {
                return false;
            }
            if (that.isLowUnbounded()) {
                return false;
            }

            int compare = compareValues(comparisonOperator, this.highValueBlock, this.highValuePosition, that.lowValueBlock, that.lowValuePosition);
            if (compare < 0) {
                return true;
            }
            if (compare == 0) {
                return !(this.highInclusive && that.lowInclusive);
            }

            return false;
        }

        @Override
        public String toString()
        {
            return new StringJoiner(", ", RangeView.class.getSimpleName() + "[", "]")
                    .add(formatRange(ToStringSession.INSTANCE))
                    .add("type=" + type.getDisplayName())
                    .toString();
        }

        public String formatRange(ConnectorSession session)
        {
            Optional<Object> singleValue = getSingleValue();
            if (singleValue.isPresent()) {
                return format("[%s]", singleValue.get());
            }

            Object lowValue = isLowUnbounded()
                    ? "<min>"
                    : type.getObjectValue(session, lowValueBlock, lowValuePosition);
            Object highValue = isHighUnbounded()
                    ? "<max>"
                    : type.getObjectValue(session, highValueBlock, highValuePosition);

            return format(
                    "%s%s,%s%s",
                    lowInclusive ? "[" : "(",
                    lowValue,
                    highValue,
                    highInclusive ? "]" : ")");
        }
    }
}
