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
package io.trino.sql.relational;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.type.Type;
import io.trino.sql.planner.Symbol;
import io.trino.type.FunctionType;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public final class LambdaDefinitionExpression
        extends RowExpression
{
    private final List<Symbol> arguments;
    private final RowExpression body;

    @JsonCreator
    public LambdaDefinitionExpression(
            @JsonProperty List<Symbol> arguments,
            @JsonProperty RowExpression body)
    {
        this.arguments = ImmutableList.copyOf(requireNonNull(arguments, "arguments is null"));
        this.body = requireNonNull(body, "body is null");
    }

    @JsonProperty
    public List<Symbol> getArguments()
    {
        return arguments;
    }

    @JsonProperty
    public RowExpression getBody()
    {
        return body;
    }

    @Override
    public Type getType()
    {
        return new FunctionType(
                arguments.stream()
                        .map(Symbol::type)
                        .toList(),
                body.getType());
    }

    @Override
    public String toString()
    {
        return "(" +
                arguments.stream()
                        .map(Symbol::name)
                        .collect(Collectors.joining(", ")) +
                ") -> " + body;
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
        LambdaDefinitionExpression that = (LambdaDefinitionExpression) o;
        return Objects.equals(arguments, that.arguments) &&
                Objects.equals(body, that.body);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(arguments, body);
    }

    @Override
    public <R, C> R accept(RowExpressionVisitor<R, C> visitor, C context)
    {
        return visitor.visitLambda(this, context);
    }
}
