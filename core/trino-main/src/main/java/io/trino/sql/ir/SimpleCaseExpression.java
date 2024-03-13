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

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public final class SimpleCaseExpression
        extends Expression
{
    private final Expression operand;
    private final List<WhenClause> whenClauses;
    private final Optional<Expression> defaultValue;

    @JsonCreator
    public SimpleCaseExpression(Expression operand, List<WhenClause> whenClauses, Optional<Expression> defaultValue)
    {
        requireNonNull(operand, "operand is null");
        requireNonNull(whenClauses, "whenClauses is null");

        this.operand = operand;
        this.whenClauses = ImmutableList.copyOf(whenClauses);
        this.defaultValue = defaultValue;
    }

    @JsonProperty
    public Expression getOperand()
    {
        return operand;
    }

    @JsonProperty
    public List<WhenClause> getWhenClauses()
    {
        return whenClauses;
    }

    @JsonProperty
    public Optional<Expression> getDefaultValue()
    {
        return defaultValue;
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitSimpleCaseExpression(this, context);
    }

    @Override
    public List<? extends Expression> getChildren()
    {
        ImmutableList.Builder<Expression> builder = ImmutableList.<Expression>builder()
                .add(operand)
                .addAll(whenClauses);

        defaultValue.ifPresent(builder::add);

        return builder.build();
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

        SimpleCaseExpression that = (SimpleCaseExpression) o;
        return Objects.equals(operand, that.operand) &&
                Objects.equals(whenClauses, that.whenClauses) &&
                Objects.equals(defaultValue, that.defaultValue);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(operand, whenClauses, defaultValue);
    }

    @Override
    public String toString()
    {
        return "SimpleCase(%s, %s, %s)".formatted(
                operand,
                whenClauses.stream()
                        .map(WhenClause::toString)
                        .collect(Collectors.joining(", ")),
                defaultValue.map(Expression::toString).orElse("null"));
    }
}
