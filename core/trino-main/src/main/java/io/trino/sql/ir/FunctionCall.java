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
import io.trino.metadata.ResolvedFunction;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public final class FunctionCall
        extends Expression
{
    private final ResolvedFunction function;
    private final List<Expression> arguments;

    @JsonCreator
    public FunctionCall(ResolvedFunction function, List<Expression> arguments)
    {
        this.function = requireNonNull(function, "function is null");
        this.arguments = ImmutableList.copyOf(arguments);
    }

    @JsonProperty
    public ResolvedFunction getFunction()
    {
        return function;
    }

    @JsonProperty
    public List<Expression> getArguments()
    {
        return arguments;
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitFunctionCall(this, context);
    }

    @Override
    public List<? extends Expression> getChildren()
    {
        return arguments;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        FunctionCall o = (FunctionCall) obj;
        return Objects.equals(function, o.function) &&
                Objects.equals(arguments, o.arguments);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(function, arguments);
    }

    @Override
    public String toString()
    {
        return "%s(%s)".formatted(
                function.getName(),
                arguments.stream()
                        .map(Expression::toString)
                        .collect(Collectors.joining(", ")));
    }
}
