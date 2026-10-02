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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.type.TypeUtils.typeHasNaN;
import static java.util.Objects.requireNonNull;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        property = "@type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = AllOrNoneValueSet.class, name = "allOrNone"),
        @JsonSubTypes.Type(value = EquatableValueSet.class, name = "equatable"),
        @JsonSubTypes.Type(value = SortedRangeSet.class, name = "sortable"),
        @JsonSubTypes.Type(value = FloatingPointValueSet.class, name = "floatingPoint"),
})
public interface ValueSet
{
    static ValueSet none(Type type)
    {
        if (typeHasNaN(type)) {
            return FloatingPointValueSet.fromRanges(SortedRangeSet.none(type));
        }
        if (type.isOrderable()) {
            return SortedRangeSet.none(type);
        }
        if (type.isComparable()) {
            return EquatableValueSet.none(type);
        }
        return AllOrNoneValueSet.none(type);
    }

    static ValueSet all(Type type)
    {
        if (typeHasNaN(type)) {
            return FloatingPointValueSet.fromRanges(SortedRangeSet.all(type));
        }
        if (type.isOrderable()) {
            return SortedRangeSet.all(type);
        }
        if (type.isComparable()) {
            return EquatableValueSet.all(type);
        }
        return AllOrNoneValueSet.all(type);
    }

    static ValueSet of(Type type, Object first, Object... rest)
    {
        if (typeHasNaN(type)) {
            List<Object> values = new ArrayList<>();
            values.add(first);
            values.addAll(List.of(rest));
            return FloatingPointValueSet.copyOf(type, values);
        }
        if (type.isOrderable()) {
            return SortedRangeSet.of(type, first, rest);
        }
        if (type.isComparable()) {
            return EquatableValueSet.of(type, first, rest);
        }
        throw new IllegalArgumentException("Cannot create discrete ValueSet with non-comparable type: " + type);
    }

    static ValueSet copyOf(Type type, Collection<?> values)
    {
        if (typeHasNaN(type)) {
            return FloatingPointValueSet.copyOf(type, values);
        }
        if (type.isOrderable()) {
            return SortedRangeSet.of(type, values);
        }
        if (type.isComparable()) {
            return EquatableValueSet.copyOf(type, values);
        }
        throw new IllegalArgumentException("Cannot create discrete ValueSet with non-comparable type: " + type);
    }

    static ValueSet ofRanges(Range first, Range... rest)
    {
        if (!typeHasNaN(first.getType())) {
            return SortedRangeSet.of(first, rest);
        }
        List<Range> ranges = new ArrayList<>();
        ranges.add(first);
        ranges.addAll(List.of(rest));
        return ofRanges(ranges);
    }

    static ValueSet ofRanges(List<Range> ranges)
    {
        SortedRangeSet ordered = SortedRangeSet.of(ranges);
        return typeHasNaN(ordered.getType())
                ? new FloatingPointValueSet(ordered, ranges.stream().anyMatch(Range::isAll))
                : ordered;
    }

    static ValueSet copyOfRanges(Type type, Collection<Range> ranges)
    {
        SortedRangeSet ordered = SortedRangeSet.copyOf(type, ranges);
        return typeHasNaN(type)
                ? new FloatingPointValueSet(ordered, ranges.stream().anyMatch(Range::isAll))
                : ordered;
    }

    /// Converts a legacy floating-point range representation without changing its membership.
    static ValueSet normalize(ValueSet values)
    {
        requireNonNull(values, "values is null");
        if (values instanceof SortedRangeSet ranges && typeHasNaN(values.getType())) {
            return FloatingPointValueSet.fromRanges(ranges);
        }
        return values;
    }

    Type getType();

    boolean isNone();

    boolean isAll();

    boolean isSingleValue();

    Object getSingleValue();

    boolean isDiscreteSet();

    List<Object> getDiscreteSet();

    boolean containsValue(Object value);

    /**
     * @return value predicates for equatable Types (but not orderable)
     */
    default DiscreteValues getDiscreteValues()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * @return range predicates for orderable Types
     */
    default Ranges getRanges()
    {
        throw new UnsupportedOperationException();
    }

    ValuesProcessor getValuesProcessor();

    ValueSet intersect(ValueSet other);

    ValueSet union(ValueSet other);

    default ValueSet union(Collection<ValueSet> valueSets)
    {
        ValueSet current = this;
        for (ValueSet valueSet : valueSets) {
            current = current.union(valueSet);
        }
        return current;
    }

    ValueSet complement();

    default boolean overlaps(ValueSet other)
    {
        return !this.intersect(other).isNone();
    }

    default ValueSet subtract(ValueSet other)
    {
        return this.intersect(other.complement());
    }

    default boolean contains(ValueSet other)
    {
        return this.union(other).equals(this);
    }

    @Override
    String toString();

    String toString(int limit);

    long getRetainedSizeInBytes();

    /**
     * Try to expand {@code valueSet} into a discrete set of (at most {@code limit}) objects.
     * For example: [1, 5] can be expanded into {1, 2, 3, 4, 5}.
     * If the data type is not supported or the expansion results in too many values, {@code Optional.empty()} is returned.
     */
    Optional<Collection<Object>> tryExpandRanges(int valuesLimit);
}
