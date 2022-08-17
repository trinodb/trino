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
import io.trino.sql.tree.LogicalExpression.Operator;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class LogicalExpression
        extends Expression
{
    private final Operator operator;
    private final List<Expression> terms;

    @JsonCreator
    public LogicalExpression(
            @JsonProperty("operator") Operator operator,
            @JsonProperty("terms") List<Expression> terms)
    {
        requireNonNull(operator, "operator is null");
        checkArgument(terms.size() >= 2, "Expected at least 2 terms");

        this.operator = operator;
        this.terms = ImmutableList.copyOf(terms);
    }

    @JsonProperty
    public Operator getOperator()
    {
        return operator;
    }

    @JsonProperty
    public List<Expression> getTerms()
    {
        return terms;
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitLogicalExpression(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return terms;
    }

    public static LogicalExpression and(Expression left, Expression right)
    {
        return new LogicalExpression(Operator.AND, ImmutableList.of(left, right));
    }

    public static LogicalExpression or(Expression left, Expression right)
    {
        return new LogicalExpression(Operator.OR, ImmutableList.of(left, right));
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
        LogicalExpression that = (LogicalExpression) o;
        return operator == that.operator && Objects.equals(terms, that.terms);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(operator, terms);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        return operator == ((LogicalExpression) other).operator;
    }
}
