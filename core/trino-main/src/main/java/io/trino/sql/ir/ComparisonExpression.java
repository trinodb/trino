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
import io.trino.sql.tree.ComparisonExpression.Operator;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

@Immutable
public class ComparisonExpression
        extends Expression
{
    private final Operator operator;
    private final Expression left;
    private final Expression right;

    @JsonCreator
    public ComparisonExpression(
            @JsonProperty("operator") Operator operator,
            @JsonProperty("left") Expression left,
            @JsonProperty("right") Expression right)
    {
        requireNonNull(operator, "operator is null");
        requireNonNull(left, "left is null");
        requireNonNull(right, "right is null");

        this.operator = operator;
        this.left = left;
        this.right = right;
    }

    @JsonProperty
    public Operator getOperator()
    {
        return operator;
    }

    @JsonProperty
    public Expression getLeft()
    {
        return left;
    }

    @JsonProperty
    public Expression getRight()
    {
        return right;
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitComparisonExpression(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(left, right);
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

        ComparisonExpression that = (ComparisonExpression) o;
        return (operator == that.operator) &&
                Objects.equals(left, that.left) &&
                Objects.equals(right, that.right);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(operator, left, right);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        return operator == ((ComparisonExpression) other).operator;
    }
}
