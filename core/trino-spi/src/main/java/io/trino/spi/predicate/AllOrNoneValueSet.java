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
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.airlift.slice.SizeOf.instanceSize;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Set that either includes all values, or excludes all values.
 */
public class AllOrNoneValueSet
        implements ValueSet
{
    private static final int INSTANCE_SIZE = instanceSize(AllOrNoneValueSet.class);

    private final Type type;
    private final boolean all;

    @JsonCreator
    public AllOrNoneValueSet(@JsonProperty("type") Type type, @JsonProperty("all") boolean all)
    {
        this.type = requireNonNull(type, "type is null");
        this.all = all;
    }

    static AllOrNoneValueSet all(Type type)
    {
        return new AllOrNoneValueSet(type, true);
    }

    static AllOrNoneValueSet none(Type type)
    {
        return new AllOrNoneValueSet(type, false);
    }

    @Override
    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @Override
    public boolean isNone()
    {
        return !all;
    }

    @Override
    @JsonProperty
    public boolean isAll()
    {
        return all;
    }

    @Override
    public boolean isSingleValue()
    {
        return false;
    }

    @Override
    public Object getSingleValue()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isDiscreteSet()
    {
        return false;
    }

    @Override
    public List<Object> getDiscreteSet()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsValue(Object value)
    {
        requireNonNull(value, "value is null");
        Class<?> expectedClass = Primitives.wrap(type.getJavaType());
        if (!expectedClass.isInstance(value)) {
            throw new IllegalArgumentException(format("Value class %s does not match required class %s", value.getClass().getName(), expectedClass.getName()));
        }
        return all;
    }

    public AllOrNone getAllOrNone()
    {
        return () -> all;
    }

    @Override
    public ValuesProcessor getValuesProcessor()
    {
        return new ValuesProcessor()
        {
            @Override
            public <T> T transform(Function<Ranges, T> rangesFunction, Function<DiscreteValues, T> valuesFunction, Function<AllOrNone, T> allOrNoneFunction)
            {
                return allOrNoneFunction.apply(getAllOrNone());
            }

            @Override
            public void consume(Consumer<Ranges> rangesConsumer, Consumer<DiscreteValues> valuesConsumer, Consumer<AllOrNone> allOrNoneConsumer)
            {
                allOrNoneConsumer.accept(getAllOrNone());
            }
        };
    }

    @Override
    public ValueSet intersect(ValueSet other)
    {
        AllOrNoneValueSet otherValueSet = checkCompatibility(other);
        return new AllOrNoneValueSet(type, all && otherValueSet.all);
    }

    @Override
    public ValueSet union(ValueSet other)
    {
        AllOrNoneValueSet otherValueSet = checkCompatibility(other);
        return new AllOrNoneValueSet(type, all || otherValueSet.all);
    }

    @Override
    public ValueSet complement()
    {
        return new AllOrNoneValueSet(type, !all);
    }

    @Override
    public String toString()
    {
        return "[" + (all ? "ALL" : "NONE") + "]";
    }

    @Override
    public String toString(ConnectorSession session)
    {
        return toString();
    }

    @Override
    public String toString(ConnectorSession session, int limit)
    {
        return toString();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        // type is not accounted for as the instances are cached (by TypeRegistry) and shared
        return INSTANCE_SIZE;
    }

    @Override
    public Optional<Collection<Object>> tryExpandRanges(int valuesLimit)
    {
        if (this.isNone()) {
            return Optional.of(List.of());
        }
        return Optional.empty();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, all);
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
        AllOrNoneValueSet other = (AllOrNoneValueSet) obj;
        return Objects.equals(this.type, other.type)
                && this.all == other.all;
    }

    private AllOrNoneValueSet checkCompatibility(ValueSet other)
    {
        if (!getType().equals(other.getType())) {
            throw new IllegalArgumentException(format("Mismatched types: %s vs %s", getType(), other.getType()));
        }
        if (!(other instanceof AllOrNoneValueSet allOrNoneValueSet)) {
            throw new IllegalArgumentException(format("ValueSet is not a AllOrNoneValueSet: %s", other.getClass()));
        }
        return allOrNoneValueSet;
    }
}
