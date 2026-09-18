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
package io.trino.sql.tree;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/// SQL spec `<comparison predicate part 2> ::= <comp op> <row value predicand>`.
/// The `IS [NOT] DISTINCT FROM` forms live on [DistinctFromPredicate].
public final class ComparisonPredicate
        extends Predicate
{
    public enum Operator
    {
        EQUAL("="),
        NOT_EQUAL("<>"),
        LESS_THAN("<"),
        LESS_THAN_OR_EQUAL("<="),
        GREATER_THAN(">"),
        GREATER_THAN_OR_EQUAL(">=");

        private final String value;

        Operator(String value)
        {
            this.value = value;
        }

        public String getValue()
        {
            return value;
        }

        public Operator flip()
        {
            return switch (this) {
                case EQUAL -> EQUAL;
                case NOT_EQUAL -> NOT_EQUAL;
                case LESS_THAN -> GREATER_THAN;
                case LESS_THAN_OR_EQUAL -> GREATER_THAN_OR_EQUAL;
                case GREATER_THAN -> LESS_THAN;
                case GREATER_THAN_OR_EQUAL -> LESS_THAN_OR_EQUAL;
            };
        }

        public Operator negate()
        {
            return switch (this) {
                case EQUAL -> NOT_EQUAL;
                case NOT_EQUAL -> EQUAL;
                case LESS_THAN -> GREATER_THAN_OR_EQUAL;
                case LESS_THAN_OR_EQUAL -> GREATER_THAN;
                case GREATER_THAN -> LESS_THAN_OR_EQUAL;
                case GREATER_THAN_OR_EQUAL -> LESS_THAN;
            };
        }
    }

    private final Operator operator;
    private final Expression right;

    public ComparisonPredicate(NodeLocation location, Operator operator, Expression right)
    {
        super(location);
        this.operator = requireNonNull(operator, "operator is null");
        this.right = requireNonNull(right, "right is null");
    }

    public Operator getOperator()
    {
        return operator;
    }

    public Expression getRight()
    {
        return right;
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return List.of(right);
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitComparisonPredicate(this, context);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        return sameClass(this, other) && operator == ((ComparisonPredicate) other).operator;
    }

    @Override
    public boolean equals(Object o)
    {
        return o instanceof ComparisonPredicate that
                && operator == that.operator
                && right.equals(that.right);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(operator, right);
    }

    @Override
    public String toString()
    {
        return operator.getValue() + " " + right;
    }
}
