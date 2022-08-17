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
import io.trino.sql.tree.CurrentTime.Function;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

@Immutable
public class CurrentTime
        extends Expression
{
    private final Function function;
    private final Integer precision;

    public CurrentTime(Function function)
    {
        this(function, null);
    }

    @JsonCreator
    public CurrentTime(
            @JsonProperty("function") Function function,
            @JsonProperty("precision") Integer precision)
    {
        requireNonNull(function, "function is null");
        this.function = function;
        this.precision = precision;
    }

    @JsonProperty
    public Function getFunction()
    {
        return function;
    }

    @JsonProperty
    public Integer getPrecision()
    {
        return precision;
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitCurrentTime(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if ((o == null) || (getClass() != o.getClass())) {
            return false;
        }
        CurrentTime that = (CurrentTime) o;
        return (function == that.function) &&
                Objects.equals(precision, that.precision);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(function, precision);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        CurrentTime otherNode = (CurrentTime) other;
        return (function == otherNode.function) &&
                Objects.equals(precision, otherNode.precision);
    }
}
