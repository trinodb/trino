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

/// SQL spec `<between predicate part 2> ::= [NOT] BETWEEN <row value predicand> AND <row value predicand>`.
/// The `negated` flag captures the in-place `NOT BETWEEN` form; outer
/// `NOT (x BETWEEN ...)` stays as a [NotExpression] wrapping a non-negated
/// `BetweenPredicate`.
public final class BetweenPredicate
        extends Predicate
{
    private final boolean negated;
    private final Expression min;
    private final Expression max;

    public BetweenPredicate(NodeLocation location, boolean negated, Expression min, Expression max)
    {
        super(location);
        this.negated = negated;
        this.min = requireNonNull(min, "min is null");
        this.max = requireNonNull(max, "max is null");
    }

    public boolean isNegated()
    {
        return negated;
    }

    public Expression getMin()
    {
        return min;
    }

    public Expression getMax()
    {
        return max;
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return List.of(min, max);
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitBetweenPredicate(this, context);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        return sameClass(this, other) && negated == ((BetweenPredicate) other).negated;
    }

    @Override
    public boolean equals(Object o)
    {
        return o instanceof BetweenPredicate that
                && negated == that.negated
                && min.equals(that.min)
                && max.equals(that.max);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(negated, min, max);
    }

    @Override
    public String toString()
    {
        return (negated ? "NOT BETWEEN " : "BETWEEN ") + min + " AND " + max;
    }
}
