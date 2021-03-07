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

import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;

import java.lang.invoke.MethodHandle;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.predicate.Utils.TUPLE_DOMAIN_TYPE_OPERATORS;
import static io.trino.spi.predicate.Utils.blockToNativeValue;
import static io.trino.spi.predicate.Utils.handleThrowable;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.TypeUtils.isFloatingPointNaN;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * A point on the continuous space defined by the specified type.
 * Each point may be just below, exact, or just above the specified value according to the Bound.
 */
@Deprecated
public final class Marker
        implements Comparable<Marker>
{
    public enum Bound
    {
        BELOW,   // lower than the value, but infinitesimally close to the value
        EXACTLY, // exactly the value
        ABOVE    // higher than the value, but infinitesimally close to the value
    }

    private final Type type;
    private final MethodHandle comparisonOperator;
    private final Optional<Block> valueBlock;
    private final Bound bound;
    private final MethodHandle equalOperator;
    private final MethodHandle hashCodeOperator;

    /**
     * LOWER UNBOUNDED is specified with an empty value and a ABOVE bound
     * UPPER UNBOUNDED is specified with an empty value and a BELOW bound
     */
    public Marker(Type type, Optional<Block> valueBlock, Bound bound)
    {
        requireNonNull(type, "type is null");
        requireNonNull(valueBlock, "valueBlock is null");
        requireNonNull(bound, "bound is null");

        if (!type.isOrderable()) {
            throw new IllegalArgumentException("type must be orderable");
        }
        if (valueBlock.isEmpty() && bound == Bound.EXACTLY) {
            throw new IllegalArgumentException("Cannot be equal to unbounded");
        }
        if (valueBlock.isPresent() && valueBlock.get().getPositionCount() != 1) {
            throw new IllegalArgumentException("value block should only have one position");
        }
        if (valueBlock.isPresent() && isFloatingPointNaN(type, blockToNativeValue(type, valueBlock.get()))) {
            throw new IllegalArgumentException("cannot use NaN as range bound");
        }
        this.type = type;
        this.comparisonOperator = TUPLE_DOMAIN_TYPE_OPERATORS.getComparisonUnorderedLastOperator(type, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION));
        this.valueBlock = valueBlock;
        this.bound = bound;
        this.equalOperator = TUPLE_DOMAIN_TYPE_OPERATORS.getEqualOperator(type, simpleConvention(NULLABLE_RETURN, BLOCK_POSITION, BLOCK_POSITION));
        this.hashCodeOperator = TUPLE_DOMAIN_TYPE_OPERATORS.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION));
    }

    private static Marker create(Type type, Optional<Object> value, Bound bound)
    {
        return new Marker(type, value.map(object -> nativeValueToBlock(type, object)), bound);
    }

    public static Marker upperUnbounded(Type type)
    {
        requireNonNull(type, "type is null");
        return create(type, Optional.empty(), Bound.BELOW);
    }

    public static Marker lowerUnbounded(Type type)
    {
        requireNonNull(type, "type is null");
        return create(type, Optional.empty(), Bound.ABOVE);
    }

    public static Marker above(Type type, Object value)
    {
        requireNonNull(type, "type is null");
        requireNonNull(value, "value is null");
        return create(type, Optional.of(value), Bound.ABOVE);
    }

    public static Marker exactly(Type type, Object value)
    {
        requireNonNull(type, "type is null");
        requireNonNull(value, "value is null");
        return create(type, Optional.of(value), Bound.EXACTLY);
    }

    public static Marker below(Type type, Object value)
    {
        requireNonNull(type, "type is null");
        requireNonNull(value, "value is null");
        return create(type, Optional.of(value), Bound.BELOW);
    }

    public Type getType()
    {
        return type;
    }

    public Optional<Block> getValueBlock()
    {
        return valueBlock;
    }

    public Object getValue()
    {
        if (valueBlock.isEmpty()) {
            throw new IllegalStateException("No value to get");
        }
        return blockToNativeValue(type, valueBlock.get());
    }

    public Object getPrintableValue(ConnectorSession session)
    {
        if (valueBlock.isEmpty()) {
            throw new IllegalStateException("No value to get");
        }
        return type.getObjectValue(session, valueBlock.get(), 0);
    }

    public Bound getBound()
    {
        return bound;
    }

    public boolean isUpperUnbounded()
    {
        return valueBlock.isEmpty() && bound == Bound.BELOW;
    }

    public boolean isLowerUnbounded()
    {
        return valueBlock.isEmpty() && bound == Bound.ABOVE;
    }

    private void checkTypeCompatibility(Marker marker)
    {
        if (!type.equals(marker.getType())) {
            throw new IllegalArgumentException(format("Mismatched Marker types: %s vs %s", type, marker.getType()));
        }
    }

    /**
     * Adjacency is defined by two Markers being infinitesimally close to each other.
     * This means they must share the same value and have adjacent Bounds.
     */
    public boolean isAdjacent(Marker other)
    {
        checkTypeCompatibility(other);
        if (isUpperUnbounded() || isLowerUnbounded() || other.isUpperUnbounded() || other.isLowerUnbounded()) {
            return false;
        }
        if (compare(valueBlock.get(), other.valueBlock.get()) != 0) {
            return false;
        }
        return (bound == Bound.EXACTLY && other.bound != Bound.EXACTLY) ||
                (bound != Bound.EXACTLY && other.bound == Bound.EXACTLY);
    }

    public Marker greaterAdjacent()
    {
        if (valueBlock.isEmpty()) {
            throw new IllegalStateException("No marker adjacent to unbounded");
        }
        switch (bound) {
            case BELOW:
                return new Marker(type, valueBlock, Bound.EXACTLY);
            case EXACTLY:
                return new Marker(type, valueBlock, Bound.ABOVE);
            case ABOVE:
                throw new IllegalStateException("No greater marker adjacent to an ABOVE bound");
        }
        throw new AssertionError("Unsupported type: " + bound);
    }

    public Marker lesserAdjacent()
    {
        if (valueBlock.isEmpty()) {
            throw new IllegalStateException("No marker adjacent to unbounded");
        }
        switch (bound) {
            case BELOW:
                throw new IllegalStateException("No lesser marker adjacent to a BELOW bound");
            case EXACTLY:
                return new Marker(type, valueBlock, Bound.BELOW);
            case ABOVE:
                return new Marker(type, valueBlock, Bound.EXACTLY);
        }
        throw new AssertionError("Unsupported type: " + bound);
    }

    @Override
    public int compareTo(Marker o)
    {
        checkTypeCompatibility(o);
        if (isUpperUnbounded()) {
            return o.isUpperUnbounded() ? 0 : 1;
        }
        if (isLowerUnbounded()) {
            return o.isLowerUnbounded() ? 0 : -1;
        }
        if (o.isUpperUnbounded()) {
            return -1;
        }
        if (o.isLowerUnbounded()) {
            return 1;
        }
        // INVARIANT: value and o.value are present

        int compare = compare(valueBlock.get(), o.valueBlock.get());
        if (compare == 0) {
            if (bound == o.bound) {
                return 0;
            }
            if (bound == Bound.BELOW) {
                return -1;
            }
            if (bound == Bound.ABOVE) {
                return 1;
            }
            // INVARIANT: bound == EXACTLY
            return (o.bound == Bound.BELOW) ? 1 : -1;
        }
        return compare;
    }

    public int compare(Block left, Block right)
    {
        try {
            return (int) (long) comparisonOperator.invokeExact(left, 0, right, 0);
        }
        catch (RuntimeException | Error e) {
            throw e;
        }
        catch (Throwable throwable) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, throwable);
        }
    }

    public static Marker min(Marker marker1, Marker marker2)
    {
        return marker1.compareTo(marker2) <= 0 ? marker1 : marker2;
    }

    public static Marker max(Marker marker1, Marker marker2)
    {
        return marker1.compareTo(marker2) >= 0 ? marker1 : marker2;
    }

    @Override
    public int hashCode()
    {
        long hash = Objects.hash(type, bound);
        if (valueBlock.isPresent()) {
            hash = hash * 31 + valueHash();
        }
        return (int) hash;
    }

    private long valueHash()
    {
        try {
            return (long) hashCodeOperator.invokeExact(valueBlock.get(), 0);
        }
        catch (Throwable throwable) {
            throw handleThrowable(throwable);
        }
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
        Marker other = (Marker) obj;
        return Objects.equals(this.type, other.type)
                && this.bound == other.bound
                && ((this.valueBlock.isPresent()) == (other.valueBlock.isPresent()))
                && (this.valueBlock.isEmpty() || valueEqual(this.valueBlock.get(), other.valueBlock.get()));
    }

    private boolean valueEqual(Block leftBlock, Block rightBlock)
    {
        try {
            return Boolean.TRUE.equals((Boolean) equalOperator.invokeExact(leftBlock, 0, rightBlock, 0));
        }
        catch (Throwable throwable) {
            throw handleThrowable(throwable);
        }
    }

    @Override
    public String toString()
    {
        StringJoiner stringJoiner = new StringJoiner(", ", Marker.class.getSimpleName() + "[", "]");
        if (isLowerUnbounded()) {
            stringJoiner.add("lower unbounded");
        }
        else if (isUpperUnbounded()) {
            stringJoiner.add("upper unbounded");
        }
        stringJoiner.add("bound=" + bound);
        valueBlock.ifPresent(valueBlock -> stringJoiner.add("valueBlock=..."));
        return stringJoiner.toString();
    }

    public String toString(ConnectorSession session)
    {
        StringBuilder buffer = new StringBuilder("{");
        buffer.append("type=").append(type);
        buffer.append(", value=");
        if (isLowerUnbounded()) {
            buffer.append("<min>");
        }
        else if (isUpperUnbounded()) {
            buffer.append("<max>");
        }
        else {
            buffer.append(getPrintableValue(session));
        }
        buffer.append(", bound=").append(bound);
        buffer.append("}");
        return buffer.toString();
    }
}
