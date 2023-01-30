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

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;

import java.lang.invoke.MethodHandle;
import java.util.Objects;
import java.util.Optional;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.predicate.Utils.TUPLE_DOMAIN_TYPE_OPERATORS;
import static io.trino.spi.predicate.Utils.handleThrowable;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.TypeUtils.isFloatingPointNaN;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * A Range of values across the continuous space defined by the types of the Markers
 */
public final class Range
{
    private final Type type;
    private final boolean lowInclusive;
    private final Optional<Object> lowValue;
    private final boolean highInclusive;
    private final Optional<Object> highValue;

    private final MethodHandle comparisonOperator;
    private final boolean isSingleValue;

    Range(Type type, boolean lowInclusive, Optional<Object> lowValue, boolean highInclusive, Optional<Object> highValue, MethodHandle comparisonOperator)
    {
        requireNonNull(type, "type is null");
        this.type = type;

        requireNonNull(lowValue, "lowValue is null");
        requireNonNull(highValue, "highValue is null");
        requireNonNull(comparisonOperator, "comparisonOperator is null");

        if (lowValue.isEmpty() && lowInclusive) {
            throw new IllegalArgumentException("low bound must be exclusive for low unbounded range");
        }
        if (highValue.isEmpty() && highInclusive) {
            throw new IllegalArgumentException("high bound must be exclusive for high unbounded range");
        }
        boolean isSingleValue = false;
        if (lowValue.isPresent() && highValue.isPresent()) {
            int compare = compareValues(comparisonOperator, lowValue.get(), highValue.get());
            if (compare > 0) {
                throw new IllegalArgumentException("low must be less than or equal to high");
            }
            if (compare == 0) {
                if (!highInclusive || !lowInclusive) {
                    throw new IllegalArgumentException("invalid bounds for single value range");
                }
                isSingleValue = true;
            }
        }
        lowValue.ifPresent(value -> verifyNotNan(type, value));
        highValue.ifPresent(value -> verifyNotNan(type, value));

        this.lowInclusive = lowInclusive;
        this.lowValue = lowValue;
        this.highInclusive = highInclusive;
        this.highValue = highValue;

        this.comparisonOperator = comparisonOperator;
        this.isSingleValue = isSingleValue;
    }

    private static void verifyNotNan(Type type, Object value)
    {
        if (isFloatingPointNaN(type, value)) {
            throw new IllegalArgumentException("cannot use NaN as range bound");
        }
    }

    static MethodHandle getComparisonOperator(Type type)
    {
        // choice of placing unordered values first or last does not matter for this code
        return TUPLE_DOMAIN_TYPE_OPERATORS.getComparisonUnorderedLastOperator(type, simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL));
    }

    public static Range all(Type type)
    {
        return new Range(type, false, Optional.empty(), false, Optional.empty(), getComparisonOperator(type));
    }

    public static Range greaterThan(Type type, Object low)
    {
        requireNonNull(low, "low is null");
        return new Range(type, false, Optional.of(low), false, Optional.empty(), getComparisonOperator(type));
    }

    public static Range greaterThanOrEqual(Type type, Object low)
    {
        requireNonNull(low, "low is null");
        return new Range(type, true, Optional.of(low), false, Optional.empty(), getComparisonOperator(type));
    }

    public static Range lessThan(Type type, Object high)
    {
        requireNonNull(high, "high is null");
        return new Range(type, false, Optional.empty(), false, Optional.of(high), getComparisonOperator(type));
    }

    public static Range lessThanOrEqual(Type type, Object high)
    {
        requireNonNull(high, "high is null");
        return new Range(type, false, Optional.empty(), true, Optional.of(high), getComparisonOperator(type));
    }

    public static Range equal(Type type, Object value)
    {
        requireNonNull(value, "value is null");
        Optional<Object> valueAsOptional = Optional.of(value);
        return new Range(type, true, valueAsOptional, true, valueAsOptional, getComparisonOperator(type));
    }

    public static Range range(Type type, Object low, boolean lowInclusive, Object high, boolean highInclusive)
    {
        requireNonNull(low, "low is null");
        requireNonNull(high, "high is null");
        return new Range(type, lowInclusive, Optional.of(low), highInclusive, Optional.of(high), getComparisonOperator(type));
    }

    public Type getType()
    {
        return type;
    }

    public boolean isLowInclusive()
    {
        return lowInclusive;
    }

    public boolean isLowUnbounded()
    {
        return lowValue.isEmpty();
    }

    public Object getLowBoundedValue()
    {
        return lowValue.orElseThrow(() -> new IllegalStateException("The range is low-unbounded"));
    }

    public Optional<Object> getLowValue()
    {
        return lowValue;
    }

    public boolean isHighInclusive()
    {
        return highInclusive;
    }

    public boolean isHighUnbounded()
    {
        return highValue.isEmpty();
    }

    public Object getHighBoundedValue()
    {
        return highValue.orElseThrow(() -> new IllegalStateException("The range is high-unbounded"));
    }

    public Optional<Object> getHighValue()
    {
        return highValue;
    }

    public boolean isSingleValue()
    {
        return isSingleValue;
    }

    public Object getSingleValue()
    {
        if (!isSingleValue()) {
            throw new IllegalStateException("Range does not have just a single value");
        }
        return lowValue.orElseThrow();
    }

    public boolean isAll()
    {
        return lowValue.isEmpty() && highValue.isEmpty();
    }

    public boolean contains(Range other)
    {
        checkTypeCompatibility(other);

        return compareLowBound(other) <= 0 &&
                compareHighBound(other) >= 0;
    }

    public Range span(Range other)
    {
        checkTypeCompatibility(other);
        int compareLowBound = compareLowBound(other);
        int compareHighBound = compareHighBound(other);
        return new Range(
                type,
                compareLowBound <= 0 ? this.lowInclusive : other.lowInclusive,
                compareLowBound <= 0 ? this.lowValue : other.lowValue,
                compareHighBound >= 0 ? this.highInclusive : other.highInclusive,
                compareHighBound >= 0 ? this.highValue : other.highValue,
                comparisonOperator);
    }

    public Optional<Range> intersect(Range other)
    {
        checkTypeCompatibility(other);
        if (!this.overlaps(other)) {
            return Optional.empty();
        }

        int compareLowBound = compareLowBound(other);
        int compareHighBound = compareHighBound(other);
        return Optional.of(new Range(
                type,
                compareLowBound >= 0 ? this.lowInclusive : other.lowInclusive,
                compareLowBound >= 0 ? this.lowValue : other.lowValue,
                compareHighBound <= 0 ? this.highInclusive : other.highInclusive,
                compareHighBound <= 0 ? this.highValue : other.highValue,
                comparisonOperator));
    }

    public boolean overlaps(Range other)
    {
        checkTypeCompatibility(other);
        return !this.isFullyBefore(other) && !other.isFullyBefore(this);
    }

    /**
     * Returns unioned range if {@code this} and {@code next} overlap or are adjacent.
     * The {@code next} lower bound must not be before {@code this} lower bound.
     */
    Optional<Range> tryMergeWithNext(Range next)
    {
        if (this.compareLowBound(next) > 0) {
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
            int compare = compareValues(comparisonOperator, this.highValue.orElseThrow(), next.lowValue.orElseThrow());
            merge = compare > 0  // overlap
                    || compare == 0 && (this.highInclusive || next.lowInclusive); // adjacent
        }
        if (merge) {
            int compareHighBound = compareHighBound(next);
            return Optional.of(new Range(
                    this.type,
                    this.lowInclusive,
                    this.lowValue,
                    // max of high bounds
                    compareHighBound <= 0 ? next.highInclusive : this.highInclusive,
                    compareHighBound <= 0 ? next.highValue : this.highValue,
                    comparisonOperator));
        }

        return Optional.empty();
    }

    private boolean isFullyBefore(Range other)
    {
        if (this.isHighUnbounded()) {
            return false;
        }
        if (other.isLowUnbounded()) {
            return false;
        }

        int compare = compareValues(comparisonOperator, this.highValue.orElseThrow(), other.lowValue.orElseThrow());
        if (compare < 0) {
            return true;
        }
        if (compare == 0) {
            return !(this.highInclusive && other.lowInclusive);
        }

        return false;
    }

    private void checkTypeCompatibility(Range range)
    {
        if (!getType().equals(range.getType())) {
            throw new IllegalArgumentException(format("Mismatched Range types: %s vs %s", getType(), range.getType()));
        }
    }

    int compareLowBound(Range other)
    {
        if (this.isLowUnbounded() || other.isLowUnbounded()) {
            return Boolean.compare(!this.isLowUnbounded(), !other.isLowUnbounded());
        }
        int compare = compareValues(comparisonOperator, this.lowValue.orElseThrow(), other.lowValue.orElseThrow());
        if (compare != 0) {
            return compare;
        }
        return Boolean.compare(!this.lowInclusive, !other.lowInclusive);
    }

    private int compareHighBound(Range other)
    {
        if (this.isHighUnbounded() || other.isHighUnbounded()) {
            return Boolean.compare(this.isHighUnbounded(), other.isHighUnbounded());
        }
        int compare = compareValues(comparisonOperator, this.highValue.orElseThrow(), other.highValue.orElseThrow());
        if (compare != 0) {
            return compare;
        }
        return Boolean.compare(this.highInclusive, other.highInclusive);
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
        Range range = (Range) o;
        return lowInclusive == range.lowInclusive &&
                highInclusive == range.highInclusive &&
                type.equals(range.type) &&
                valuesEqual(lowValue, range.lowValue) &&
                valuesEqual(highValue, range.highValue);
    }

    private boolean valuesEqual(Optional<Object> a, Optional<Object> b)
    {
        if (a.isEmpty() || b.isEmpty()) {
            return a.isEmpty() == b.isEmpty();
        }
        return compareValues(comparisonOperator, a.get(), b.get()) == 0;
    }

    private static int compareValues(MethodHandle comparisonOperator, Object left, Object right)
    {
        try {
            return (int) (long) comparisonOperator.invoke(left, right);
        }
        catch (Throwable throwable) {
            throw handleThrowable(throwable);
        }
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, lowInclusive, lowValue, highInclusive, highValue);
    }

    @Override
    public String toString()
    {
        return toString(ToStringSession.INSTANCE);
    }

    public String toString(ConnectorSession session)
    {
        Object lowObject = lowValue
                .map(value -> type.getObjectValue(session, nativeValueToBlock(type, value), 0))
                .orElse("<min>");

        if (isSingleValue()) {
            return format("[%s]", lowObject);
        }
        Object highObject = highValue
                .map(value -> type.getObjectValue(session, nativeValueToBlock(type, value), 0))
                .orElse("<max>");
        return format(
                "%s%s, %s%s",
                lowInclusive ? "[" : "(",
                lowObject,
                highObject,
                highInclusive ? "]" : ")");
    }
}
