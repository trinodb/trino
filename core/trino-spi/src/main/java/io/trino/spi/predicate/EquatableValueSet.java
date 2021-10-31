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
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import java.lang.invoke.MethodHandle;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Stream;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.predicate.Utils.TUPLE_DOMAIN_TYPE_OPERATORS;
import static io.trino.spi.predicate.Utils.handleThrowable;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toUnmodifiableList;

/**
 * A set containing values that are uniquely identifiable.
 * Assumes an infinite number of possible values. The values may be collectively included
 * or collectively excluded.
 */
public class EquatableValueSet
        implements ValueSet
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(EquatableValueSet.class).instanceSize();

    private final Type type;
    private final boolean inclusive;
    private final Set<ValueEntry> entries;

    @JsonCreator
    public EquatableValueSet(
            @JsonProperty("type") Type type,
            @JsonProperty("inclusive") boolean inclusive,
            @JsonProperty("entries") Set<ValueEntry> entries)
    {
        requireNonNull(type, "type is null");
        requireNonNull(entries, "entries is null");

        if (!type.isComparable()) {
            throw new IllegalArgumentException("Type is not comparable: " + type);
        }
        if (type.isOrderable()) {
            throw new IllegalArgumentException("Use SortedRangeSet instead");
        }
        this.type = type;
        this.inclusive = inclusive;
        this.entries = unmodifiableSet(new LinkedHashSet<>(entries));
    }

    static EquatableValueSet none(Type type)
    {
        return new EquatableValueSet(type, true, Collections.emptySet());
    }

    static EquatableValueSet all(Type type)
    {
        return new EquatableValueSet(type, false, Collections.emptySet());
    }

    static EquatableValueSet of(Type type, Object first, Object... rest)
    {
        HashSet<ValueEntry> set = new LinkedHashSet<>(rest.length + 1);
        set.add(ValueEntry.create(type, first));
        for (Object value : rest) {
            set.add(ValueEntry.create(type, value));
        }
        return new EquatableValueSet(type, true, set);
    }

    static EquatableValueSet copyOf(Type type, Collection<?> values)
    {
        return new EquatableValueSet(type, true, values.stream()
                .map(value -> ValueEntry.create(type, value))
                .collect(toLinkedSet()));
    }

    @JsonProperty
    @Override
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public boolean inclusive()
    {
        return inclusive;
    }

    @JsonProperty
    public Set<ValueEntry> getEntries()
    {
        return entries;
    }

    public Collection<Object> getValues()
    {
        return entries.stream()
                .map(ValueEntry::getValue)
                .collect(toUnmodifiableList());
    }

    public int getValuesCount()
    {
        return entries.size();
    }

    @Override
    public boolean isNone()
    {
        return inclusive && entries.isEmpty();
    }

    @Override
    public boolean isAll()
    {
        return !inclusive && entries.isEmpty();
    }

    @Override
    public boolean isSingleValue()
    {
        return inclusive && entries.size() == 1;
    }

    @Override
    public Object getSingleValue()
    {
        if (!isSingleValue()) {
            throw new IllegalStateException("EquatableValueSet does not have just a single value");
        }
        return entries.iterator().next().getValue();
    }

    @Override
    public boolean isDiscreteSet()
    {
        return inclusive && !entries.isEmpty();
    }

    @Override
    public List<Object> getDiscreteSet()
    {
        if (!isDiscreteSet()) {
            throw new IllegalStateException("EquatableValueSet is not a discrete set");
        }
        return entries.stream()
                .map(ValueEntry::getValue)
                .collect(toUnmodifiableList());
    }

    @Override
    public boolean containsValue(Object value)
    {
        return inclusive == entries.contains(ValueEntry.create(type, value));
    }

    @Override
    public DiscreteValues getDiscreteValues()
    {
        return new DiscreteValues()
        {
            @Override
            public boolean isInclusive()
            {
                return EquatableValueSet.this.inclusive();
            }

            @Override
            public Collection<Object> getValues()
            {
                return EquatableValueSet.this.getValues();
            }

            @Override
            public int getValuesCount()
            {
                return EquatableValueSet.this.getValuesCount();
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
                return valuesFunction.apply(getDiscreteValues());
            }

            @Override
            public void consume(Consumer<Ranges> rangesConsumer, Consumer<DiscreteValues> valuesConsumer, Consumer<AllOrNone> allOrNoneConsumer)
            {
                valuesConsumer.accept(getDiscreteValues());
            }
        };
    }

    @Override
    public EquatableValueSet intersect(ValueSet other)
    {
        EquatableValueSet otherValueSet = checkCompatibility(other);

        if (inclusive && otherValueSet.inclusive()) {
            return new EquatableValueSet(type, true, intersect(entries, otherValueSet.entries));
        }
        else if (inclusive) {
            return new EquatableValueSet(type, true, subtract(entries, otherValueSet.entries));
        }
        else if (otherValueSet.inclusive()) {
            return new EquatableValueSet(type, true, subtract(otherValueSet.entries, entries));
        }
        else {
            return new EquatableValueSet(type, false, union(otherValueSet.entries, entries));
        }
    }

    @Override
    public boolean overlaps(ValueSet other)
    {
        EquatableValueSet otherValueSet = checkCompatibility(other);

        if (inclusive && otherValueSet.inclusive()) {
            return setsOverlap(entries, otherValueSet.entries);
        }
        else if (inclusive) {
            return !otherValueSet.entries.containsAll(entries);
        }
        else if (otherValueSet.inclusive()) {
            return !entries.containsAll(otherValueSet.entries);
        }
        else {
            return true;
        }
    }

    @Override
    public EquatableValueSet union(ValueSet other)
    {
        EquatableValueSet otherValueSet = checkCompatibility(other);

        if (inclusive && otherValueSet.inclusive()) {
            return new EquatableValueSet(type, true, union(entries, otherValueSet.entries));
        }
        else if (inclusive) {
            return new EquatableValueSet(type, false, subtract(otherValueSet.entries, entries));
        }
        else if (otherValueSet.inclusive()) {
            return new EquatableValueSet(type, false, subtract(entries, otherValueSet.entries));
        }
        else {
            return new EquatableValueSet(type, false, intersect(otherValueSet.entries, entries));
        }
    }

    @Override
    public boolean contains(ValueSet other)
    {
        EquatableValueSet otherValueSet = checkCompatibility(other);

        if (inclusive && otherValueSet.inclusive()) {
            return entries.containsAll(otherValueSet.entries);
        }
        if (inclusive) {
            /* Note: This isn't correct for a finite universe of values.
             * For example, for boolean universe: {true, false}
             * `this` being [inclusive, {true}] and `other` being [excluding, {false}]
             * `this.contains(other)` should return true, since the domains are equal.
             * However, we return false for consistency with `this.union(other).equals(this)`
             */
            return false;
        }
        if (otherValueSet.inclusive()) {
            return !setsOverlap(entries, otherValueSet.entries);
        }
        return otherValueSet.entries.containsAll(entries);
    }

    @Override
    public EquatableValueSet complement()
    {
        return new EquatableValueSet(type, !inclusive, entries);
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
        return new StringJoiner(", ", EquatableValueSet.class.getSimpleName() + "[", "]")
                .add("type=" + type)
                .add("values=" + getValuesCount())
                .add(formatValues(session, limit))
                .toString();
    }

    @Override
    public Optional<Collection<Object>> tryExpandRanges(int valuesLimit)
    {
        if (inclusive() && getValuesCount() <= valuesLimit) {
            return Optional.of(getValues());
        }
        return Optional.empty();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        // type is not accounted for as the instances are cached (by TypeRegistry) and shared
        return INSTANCE_SIZE + estimatedSizeOf(entries, ValueEntry::getRetainedSizeInBytes);
    }

    private String formatValues(ConnectorSession session, int limit)
    {
        return Stream.concat(
                entries.stream()
                        .map(entry -> type.getObjectValue(session, entry.getBlock(), 0).toString())
                        .limit(limit),
                limit < getValuesCount() ? Stream.of("...") : Stream.of())
                .collect(joining(", ", inclusive ? "{" : "EXCLUDES{", "}"));
    }

    private static <T> Set<T> intersect(Set<T> set1, Set<T> set2)
    {
        if (set1.size() > set2.size()) {
            return intersect(set2, set1);
        }
        return set1.stream()
                .filter(set2::contains)
                .collect(toLinkedSet());
    }

    private static <T> boolean setsOverlap(Set<T> set1, Set<T> set2)
    {
        if (set1.size() > set2.size()) {
            return setsOverlap(set2, set1);
        }
        for (T element : set1) {
            if (set2.contains(element)) {
                return true;
            }
        }
        return false;
    }

    private static <T> Set<T> union(Set<T> set1, Set<T> set2)
    {
        return Stream.concat(set1.stream(), set2.stream())
                .collect(toLinkedSet());
    }

    private static <T> Set<T> subtract(Set<T> set1, Set<T> set2)
    {
        return set1.stream()
                .filter(value -> !set2.contains(value))
                .collect(toLinkedSet());
    }

    private EquatableValueSet checkCompatibility(ValueSet other)
    {
        if (!getType().equals(other.getType())) {
            throw new IllegalStateException(format("Mismatched types: %s vs %s", getType(), other.getType()));
        }
        if (!(other instanceof EquatableValueSet)) {
            throw new IllegalStateException(format("ValueSet is not a EquatableValueSet: %s", other.getClass()));
        }
        return (EquatableValueSet) other;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, inclusive, entries);
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
        EquatableValueSet other = (EquatableValueSet) obj;
        return Objects.equals(this.type, other.type)
                && this.inclusive == other.inclusive
                && Objects.equals(this.entries, other.entries);
    }

    public static class ValueEntry
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(ValueEntry.class).instanceSize();

        private final Type type;
        private final Block block;
        private final MethodHandle equalOperator;
        private final MethodHandle hashCodeOperator;

        @JsonCreator
        public ValueEntry(
                @JsonProperty("type") Type type,
                @JsonProperty("block") Block block)
        {
            this.type = requireNonNull(type, "type is null");
            this.block = requireNonNull(block, "block is null");

            if (block.getPositionCount() != 1) {
                throw new IllegalArgumentException("Block should only have one position");
            }
            this.equalOperator = TUPLE_DOMAIN_TYPE_OPERATORS.getEqualOperator(type, simpleConvention(NULLABLE_RETURN, BLOCK_POSITION, BLOCK_POSITION));
            this.hashCodeOperator = TUPLE_DOMAIN_TYPE_OPERATORS.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION));
        }

        public static ValueEntry create(Type type, Object value)
        {
            return new ValueEntry(type, Utils.nativeValueToBlock(type, value));
        }

        @JsonProperty
        public Type getType()
        {
            return type;
        }

        @JsonProperty
        public Block getBlock()
        {
            return block;
        }

        public Object getValue()
        {
            return Utils.blockToNativeValue(type, block);
        }

        @Override
        public int hashCode()
        {
            try {
                return (int) (long) hashCodeOperator.invokeExact(block, 0);
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
            ValueEntry other = (ValueEntry) obj;
            if (!Objects.equals(this.type, other.type)) {
                return false;
            }

            Boolean result;
            try {
                result = (Boolean) equalOperator.invokeExact(this.block, 0, other.block, 0);
            }
            catch (Throwable throwable) {
                throw handleThrowable(throwable);
            }
            return Boolean.TRUE.equals(result);
        }

        public long getRetainedSizeInBytes()
        {
            // type is not accounted for as the instances are cached (by TypeRegistry) and shared
            return INSTANCE_SIZE + block.getRetainedSizeInBytes();
        }
    }

    private static <T> Collector<T, ?, Set<T>> toLinkedSet()
    {
        return toCollection(LinkedHashSet::new);
    }
}
