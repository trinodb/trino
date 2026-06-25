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

/// SQL spec `<distinct predicate part 2> ::= IS [NOT] DISTINCT FROM <row value predicand>`.
/// The in-place `NOT` (i.e. `IS NOT DISTINCT FROM`) is recorded via [#isNegated()];
/// an outer `NOT (x IS DISTINCT FROM y)` is a [NotExpression] around a non-negated
/// `DistinctFromPredicate`.
public final class DistinctFromPredicate
        extends Predicate
{
    private final boolean negated;
    private final Expression right;

    public DistinctFromPredicate(NodeLocation location, boolean negated, Expression right)
    {
        super(location);
        this.negated = negated;
        this.right = requireNonNull(right, "right is null");
    }

    public boolean isNegated()
    {
        return negated;
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
        return visitor.visitDistinctFromPredicate(this, context);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        return sameClass(this, other) && negated == ((DistinctFromPredicate) other).negated;
    }

    @Override
    public boolean equals(Object o)
    {
        return o instanceof DistinctFromPredicate that
                && negated == that.negated
                && right.equals(that.right);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(negated, right);
    }

    @Override
    public String toString()
    {
        return (negated ? "IS NOT DISTINCT FROM " : "IS DISTINCT FROM ") + right;
    }
}
