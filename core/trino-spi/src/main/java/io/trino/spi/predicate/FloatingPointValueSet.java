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
import io.trino.spi.type.TrinoNumber;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TypeUtils.isFloatingPointNaN;
import static io.trino.spi.type.TypeUtils.typeHasNaN;
import static java.lang.Float.floatToRawIntBits;
import static java.util.Objects.requireNonNull;

/// A set of REAL, DOUBLE, or NUMBER values. NaN membership is independent of the
/// ordered values, so union, intersection, and complement preserve complete membership.
/// NaN is never used as an ordered range endpoint.
public final class FloatingPointValueSet
        implements ValueSet
{
    private static final int INSTANCE_SIZE = instanceSize(FloatingPointValueSet.class);
    private static final int OPTIONAL_INSTANCE_SIZE = instanceSize(Optional.class);

    private final SortedRangeSet orderedValues;
    private final boolean nanAllowed;
    private final boolean allOrderedValues;
    private final Optional<SortedRangeSet> ranges;

    @JsonCreator
    public FloatingPointValueSet(@JsonProperty("orderedValues") SortedRangeSet orderedValues, @JsonProperty("nanAllowed") boolean nanAllowed)
    {
        requireNonNull(orderedValues, "orderedValues is null");
        if (!typeHasNaN(orderedValues.getType())) {
            throw new IllegalArgumentException("Type does not support NaN: " + orderedValues.getType());
        }
        SortedRangeSet orderedUniverse = allOrderedValues(orderedValues.getType());
        this.orderedValues = orderedValues.intersect(orderedUniverse);
        this.nanAllowed = nanAllowed;
        this.allOrderedValues = this.orderedValues.equals(orderedUniverse);
        this.ranges = createRanges(orderedUniverse.getSpan());
    }

    /// Preserves the membership of the range representation: only its all-values set includes NaN.
    public static FloatingPointValueSet fromRanges(SortedRangeSet ranges)
    {
        return new FloatingPointValueSet(ranges, ranges.isAll());
    }

    public static FloatingPointValueSet copyOf(Type type, Collection<?> values)
    {
        List<Object> ordered = new ArrayList<>();
        boolean nan = false;
        for (Object value : values) {
            requireNonNull(value, "value is null");
            if (isFloatingPointNaN(type, value)) {
                nan = true;
            }
            else {
                ordered.add(value);
            }
        }
        return new FloatingPointValueSet(SortedRangeSet.of(type, ordered), nan);
    }

    @Override
    public Type getType()
    {
        return orderedValues.getType();
    }

    /// Returns just the ordered members, bounded by inclusive infinities. Callers must
    /// also account for [#isNaNAllowed]. Use these bounds for ordered comparisons.
    @JsonProperty
    public SortedRangeSet getOrderedValues()
    {
        return orderedValues;
    }

    @JsonProperty("nanAllowed")
    public boolean isNaNAllowed()
    {
        return nanAllowed;
    }

    @Override
    public boolean isNone()
    {
        return orderedValues.isNone() && !nanAllowed;
    }

    @Override
    public boolean isAll()
    {
        return nanAllowed && allOrderedValues;
    }

    /// Whether every ordered value belongs to this set, independently of NaN membership.
    public boolean isAllOrderedValues()
    {
        return allOrderedValues;
    }

    @Override
    public boolean isSingleValue()
    {
        return nanAllowed ? orderedValues.isNone() : orderedValues.isSingleValue();
    }

    @Override
    public Object getSingleValue()
    {
        if (!isSingleValue()) {
            throw new IllegalStateException("Set is not a single value");
        }
        return nanAllowed ? nanValue(getType()) : orderedValues.getSingleValue();
    }

    @Override
    public boolean isDiscreteSet()
    {
        return (orderedValues.isNone() && nanAllowed) || orderedValues.isDiscreteSet();
    }

    @Override
    public List<Object> getDiscreteSet()
    {
        if (!isDiscreteSet()) {
            throw new IllegalStateException("Set is not discrete");
        }
        List<Object> values = new ArrayList<>();
        if (!orderedValues.isNone()) {
            values.addAll(orderedValues.getDiscreteSet());
        }
        if (nanAllowed) {
            values.add(nanValue(getType()));
        }
        return List.copyOf(values);
    }

    @Override
    public boolean containsValue(Object value)
    {
        requireNonNull(value, "value is null");
        return isFloatingPointNaN(getType(), value) ? nanAllowed : orderedValues.containsValue(value);
    }

    /// Returns an exact legacy range representation when one exists. Partial sets containing
    /// NaN have no such representation; use the floating-point [ValuesProcessor] callback.
    public Optional<SortedRangeSet> asRanges()
    {
        return ranges;
    }

    private Optional<SortedRangeSet> createRanges(Range full)
    {
        if (isAll()) {
            return Optional.of(SortedRangeSet.all(getType()));
        }
        if (nanAllowed) {
            return Optional.empty();
        }
        if (orderedValues.isNone() || orderedValues.isSingleValue() || allOrderedValues) {
            return Optional.of(orderedValues);
        }
        Range span = orderedValues.getSpan();
        Range minimum = Range.equal(getType(), full.getLowBoundedValue());
        Range maximum = Range.equal(getType(), full.getHighBoundedValue());
        if (!span.contains(minimum) && !span.contains(maximum)) {
            return Optional.of(orderedValues);
        }
        List<Range> ranges = new ArrayList<>();
        for (Range range : orderedValues.getOrderedRanges()) {
            boolean lowUnbounded = range.contains(minimum);
            boolean highUnbounded = range.contains(maximum);
            if (range.isSingleValue()) {
                ranges.add(range);
            }
            else if (lowUnbounded) {
                ranges.add(range.isHighInclusive() ? Range.lessThanOrEqual(getType(), range.getHighBoundedValue()) : Range.lessThan(getType(), range.getHighBoundedValue()));
            }
            else if (highUnbounded) {
                ranges.add(range.isLowInclusive() ? Range.greaterThanOrEqual(getType(), range.getLowBoundedValue()) : Range.greaterThan(getType(), range.getLowBoundedValue()));
            }
            else {
                ranges.add(range);
            }
        }
        return Optional.of(SortedRangeSet.copyOf(getType(), ranges));
    }

    @Override
    public Ranges getRanges()
    {
        return asRanges().orElseThrow(() -> new IllegalStateException("NaN-containing set cannot be represented by ranges")).getRanges();
    }

    @Override
    public ValuesProcessor getValuesProcessor()
    {
        return new ValuesProcessor()
        {
            @Override
            public <T> T transform(Function<Ranges, T> ranges, Function<DiscreteValues, T> discrete, Function<AllOrNone, T> allOrNone)
            {
                return ranges.apply(getRanges());
            }

            @Override
            public <T> T transform(Function<Ranges, T> ranges, Function<DiscreteValues, T> discrete, Function<AllOrNone, T> allOrNone, Function<FloatingPointValueSet, T> floatingPoint)
            {
                return floatingPoint.apply(FloatingPointValueSet.this);
            }

            @Override
            public void consume(Consumer<Ranges> ranges, Consumer<DiscreteValues> discrete, Consumer<AllOrNone> allOrNone)
            {
                ranges.accept(getRanges());
            }

            @Override
            public void consume(Consumer<Ranges> ranges, Consumer<DiscreteValues> discrete, Consumer<AllOrNone> allOrNone, Consumer<FloatingPointValueSet> floatingPoint)
            {
                floatingPoint.accept(FloatingPointValueSet.this);
            }
        };
    }

    private FloatingPointValueSet compatible(ValueSet other)
    {
        if (!getType().equals(other.getType())) {
            throw new IllegalArgumentException("Mismatched types: " + getType() + " and " + other.getType());
        }
        if (other instanceof FloatingPointValueSet floatingPoint) {
            return floatingPoint;
        }
        return fromRanges((SortedRangeSet) other);
    }

    @Override
    public boolean contains(ValueSet other)
    {
        FloatingPointValueSet that = compatible(other);
        return (!that.nanAllowed || nanAllowed) && (allOrderedValues || orderedValues.contains(that.orderedValues));
    }

    @Override
    public boolean overlaps(ValueSet other)
    {
        FloatingPointValueSet that = compatible(other);
        return (nanAllowed && that.nanAllowed) || orderedValues.overlaps(that.orderedValues);
    }

    @Override
    public FloatingPointValueSet intersect(ValueSet other)
    {
        FloatingPointValueSet that = compatible(other);
        return new FloatingPointValueSet(orderedValues.intersect(that.orderedValues), nanAllowed && that.nanAllowed);
    }

    @Override
    public FloatingPointValueSet union(ValueSet other)
    {
        FloatingPointValueSet that = compatible(other);
        return new FloatingPointValueSet(orderedValues.union(that.orderedValues), nanAllowed || that.nanAllowed);
    }

    @Override
    public FloatingPointValueSet union(Collection<ValueSet> others)
    {
        List<SortedRangeSet> ranges = new ArrayList<>(others.size());
        boolean includeNaN = nanAllowed;
        for (ValueSet valueSet : others) {
            FloatingPointValueSet other = compatible(valueSet);
            ranges.add(other.orderedValues);
            includeNaN |= other.nanAllowed;
        }
        return new FloatingPointValueSet(orderedValues.unionRanges(ranges), includeNaN);
    }

    @Override
    public FloatingPointValueSet subtract(ValueSet other)
    {
        return intersect(compatible(other).complement());
    }

    @Override
    public FloatingPointValueSet complement()
    {
        return new FloatingPointValueSet(orderedValues.complement(), !nanAllowed);
    }

    @Override
    public Optional<Collection<Object>> tryExpandRanges(int valuesLimit)
    {
        if (isNone()) {
            return Optional.of(List.of());
        }
        if (isDiscreteSet() && getDiscreteSet().size() <= valuesLimit) {
            return Optional.of(getDiscreteSet());
        }
        return Optional.empty();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long size = INSTANCE_SIZE + OPTIONAL_INSTANCE_SIZE + orderedValues.getRetainedSizeInBytes();
        if (ranges.isPresent() && ranges.get() != orderedValues) {
            size += ranges.get().getRetainedSizeInBytes();
        }
        return size;
    }

    @Override
    public boolean equals(Object other)
    {
        return other instanceof FloatingPointValueSet that && nanAllowed == that.nanAllowed && orderedValues.equals(that.orderedValues);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(orderedValues, nanAllowed);
    }

    @Override
    public String toString()
    {
        return toString(10);
    }

    @Override
    public String toString(int limit)
    {
        return orderedValues.toString(limit) + (nanAllowed ? " OR NaN" : "");
    }

    public static SortedRangeSet allOrderedValues(Type type)
    {
        Object low;
        Object high;
        if (type.equals(DOUBLE)) {
            low = Double.NEGATIVE_INFINITY;
            high = Double.POSITIVE_INFINITY;
        }
        else if (type.equals(REAL)) {
            low = (long) floatToRawIntBits(Float.NEGATIVE_INFINITY);
            high = (long) floatToRawIntBits(Float.POSITIVE_INFINITY);
        }
        else if (type.equals(NUMBER)) {
            low = TrinoNumber.from(new TrinoNumber.Infinity(true));
            high = TrinoNumber.from(new TrinoNumber.Infinity(false));
        }
        else {
            throw new IllegalArgumentException("Type does not support NaN: " + type);
        }
        return SortedRangeSet.of(Range.range(type, low, true, high, true));
    }

    public static Object nanValue(Type type)
    {
        if (type.equals(DOUBLE)) {
            return Double.NaN;
        }
        if (type.equals(REAL)) {
            return (long) floatToRawIntBits(Float.NaN);
        }
        if (type.equals(NUMBER)) {
            return TrinoNumber.from(new TrinoNumber.NotANumber());
        }
        throw new IllegalArgumentException("Type does not support NaN: " + type);
    }
}
