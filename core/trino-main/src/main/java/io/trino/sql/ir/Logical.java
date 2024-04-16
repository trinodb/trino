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

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableList;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.IrUtils.validateType;
import static java.util.Objects.requireNonNull;

@JsonSerialize
public record Logical(Operator operator, List<Expression> terms)
        implements Expression
{
    public enum Operator
    {
        AND, OR;

        public Operator flip()
        {
            return switch (this) {
                case AND -> OR;
                case OR -> AND;
            };
        }
    }

    public Logical
    {
        requireNonNull(operator, "operator is null");
        checkArgument(terms.size() >= 2, "Expected at least 2 terms");

        for (Expression term : terms) {
            validateType(BOOLEAN, term);
        }

        terms = ImmutableList.copyOf(terms);
    }

    @Override
    public Type type()
    {
        return BOOLEAN;
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitLogical(this, context);
    }

    @Override
    public List<? extends Expression> children()
    {
        return terms;
    }

    public static Logical and(Expression left, Expression right)
    {
        return new Logical(Operator.AND, ImmutableList.of(left, right));
    }

    public static Logical or(Expression left, Expression right)
    {
        return new Logical(Operator.OR, ImmutableList.of(left, right));
    }

    @Override
    public String toString()
    {
        return "%s(%s)".formatted(
                switch (operator) {
                    case AND -> "$and";
                    case OR -> "$or";
                },
                terms.stream()
                        .map(Expression::toString)
                        .collect(Collectors.joining(", ")));
    }
}
