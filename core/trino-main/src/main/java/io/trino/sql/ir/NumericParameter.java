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
package io.trino.sql.ir;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

@Immutable
public class NumericParameter
        extends DataTypeParameter
{
    private final String value;

    @JsonCreator
    public NumericParameter(
            @JsonProperty("value") String value)
    {
        this.value = requireNonNull(value, "value is null");
    }

    @JsonProperty
    public String getValue()
    {
        return value;
    }

    @Override
    public String toString()
    {
        return value;
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    protected <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitNumericTypeParameter(this, context);
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
        NumericParameter that = (NumericParameter) o;
        return value.equals(that.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        return Objects.equals(value, ((NumericParameter) other).value);
    }
}
