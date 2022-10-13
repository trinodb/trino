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
import io.trino.spi.predicate.AllOrNoneValueSet;
import io.trino.spi.predicate.EquatableValueSet;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.ValueSet;

import javax.annotation.Nullable;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.drift.annotations.ThriftField.Requiredness.OPTIONAL;
import static io.trino.plugin.thrift.api.valuesets.TrinoThriftAllOrNoneValueSet.fromAllOrNoneValueSet;
import static io.trino.plugin.thrift.api.valuesets.TrinoThriftEquatableValueSet.fromEquatableValueSet;
import static io.trino.plugin.thrift.api.valuesets.TrinoThriftRangeValueSet.fromSortedRangeSet;

@ThriftStruct
public final class TrinoThriftValueSet
{
    private final TrinoThriftAllOrNoneValueSet allOrNoneValueSet;
    private final TrinoThriftEquatableValueSet equatableValueSet;
    private final TrinoThriftRangeValueSet rangeValueSet;

    @ThriftConstructor
    public TrinoThriftValueSet(
            @Nullable TrinoThriftAllOrNoneValueSet allOrNoneValueSet,
            @Nullable TrinoThriftEquatableValueSet equatableValueSet,
            @Nullable TrinoThriftRangeValueSet rangeValueSet)
    {
        checkArgument(isExactlyOneNonNull(allOrNoneValueSet, equatableValueSet, rangeValueSet), "exactly one value set must be present");
        this.allOrNoneValueSet = allOrNoneValueSet;
        this.equatableValueSet = equatableValueSet;
        this.rangeValueSet = rangeValueSet;
    }

    @Nullable
    @ThriftField(value = 1, requiredness = OPTIONAL)
    public TrinoThriftAllOrNoneValueSet getAllOrNoneValueSet()
    {
        return allOrNoneValueSet;
    }

    @Nullable
    @ThriftField(value = 2, requiredness = OPTIONAL)
    public TrinoThriftEquatableValueSet getEquatableValueSet()
    {
        return equatableValueSet;
    }

    @Nullable
    @ThriftField(value = 3, requiredness = OPTIONAL)
    public TrinoThriftRangeValueSet getRangeValueSet()
    {
        return rangeValueSet;
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
        TrinoThriftValueSet other = (TrinoThriftValueSet) obj;
        return Objects.equals(this.allOrNoneValueSet, other.allOrNoneValueSet) &&
                Objects.equals(this.equatableValueSet, other.equatableValueSet) &&
                Objects.equals(this.rangeValueSet, other.rangeValueSet);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(allOrNoneValueSet, equatableValueSet, rangeValueSet);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("valueSet", firstNonNull(allOrNoneValueSet, equatableValueSet, rangeValueSet))
                .toString();
    }

    public static TrinoThriftValueSet fromValueSet(ValueSet valueSet)
    {
        if (valueSet.getClass() == AllOrNoneValueSet.class) {
            return new TrinoThriftValueSet(
                    fromAllOrNoneValueSet((AllOrNoneValueSet) valueSet),
                    null,
                    null);
        }
        if (valueSet.getClass() == EquatableValueSet.class) {
            return new TrinoThriftValueSet(
                    null,
                    fromEquatableValueSet((EquatableValueSet) valueSet),
                    null);
        }
        if (valueSet.getClass() == SortedRangeSet.class) {
            return new TrinoThriftValueSet(
                    null,
                    null,
                    fromSortedRangeSet((SortedRangeSet) valueSet));
        }
        throw new IllegalArgumentException("Unknown implementation of a value set: " + valueSet.getClass());
    }

    private static boolean isExactlyOneNonNull(Object a, Object b, Object c)
    {
        return a != null && b == null && c == null ||
                a == null && b != null && c == null ||
                a == null && b == null && c != null;
    }

    private static Object firstNonNull(Object a, Object b, Object c)
    {
        if (a != null) {
            return a;
        }
        if (b != null) {
            return b;
        }
        if (c != null) {
            return c;
        }
        throw new IllegalArgumentException("All arguments are null");
    }
}
