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
package io.trino.plugin.thrift.api.valuesets;

import io.airlift.drift.annotations.ThriftConstructor;
import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.annotations.ThriftStruct;
import io.trino.plugin.thrift.api.TrinoThriftBlock;
import io.trino.spi.predicate.EquatableValueSet;
import io.trino.spi.predicate.EquatableValueSet.ValueEntry;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.thrift.api.TrinoThriftBlock.fromBlock;
import static java.util.Objects.requireNonNull;

/**
 * A set containing values that are uniquely identifiable.
 * Assumes an infinite number of possible values. The values may be collectively included
 * or collectively excluded.
 * This structure is used with comparable, but not orderable types like "json", "map".
 */
@ThriftStruct
public final class TrinoThriftEquatableValueSet
{
    private final boolean inclusive;
    private final List<TrinoThriftBlock> values;

    @ThriftConstructor
    public TrinoThriftEquatableValueSet(boolean inclusive, List<TrinoThriftBlock> values)
    {
        this.inclusive = inclusive;
        this.values = requireNonNull(values, "values are null");
    }

    @ThriftField(1)
    public boolean isInclusive()
    {
        return inclusive;
    }

    @ThriftField(2)
    public List<TrinoThriftBlock> getValues()
    {
        return values;
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
        TrinoThriftEquatableValueSet other = (TrinoThriftEquatableValueSet) obj;
        return this.inclusive == other.inclusive &&
                Objects.equals(this.values, other.values);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(inclusive, values);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("inclusive", inclusive)
                .add("values", values)
                .toString();
    }

    public static TrinoThriftEquatableValueSet fromEquatableValueSet(EquatableValueSet valueSet)
    {
        Type type = valueSet.getType();
        Set<ValueEntry> values = valueSet.getEntries();
        List<TrinoThriftBlock> thriftValues = new ArrayList<>(values.size());
        for (ValueEntry value : values) {
            checkState(type.equals(value.getType()), "ValueEntrySet has elements of different types: %s vs %s", type, value.getType());
            thriftValues.add(fromBlock(value.getBlock(), type));
        }
        return new TrinoThriftEquatableValueSet(valueSet.inclusive(), thriftValues);
    }
}
