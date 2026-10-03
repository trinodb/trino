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

/// SQL spec `<member predicate part 2> ::= [NOT] MEMBER [OF] <row value predicand>`. The candidate
/// value is held by the enclosing [Predicated]; this fragment carries only the right-hand multiset.
/// `x MEMBER OF y` is true when the value equals at least one element of the multiset `right`.
/// Three-valued like `IN`: unknown when the value or `right` is null, or when the value matches no
/// element but `right` contains a null. `negated` captures the in-place `NOT MEMBER` form.
public final class MemberPredicate
        extends Predicate
{
    private final boolean negated;
    private final Expression right;

    public MemberPredicate(NodeLocation location, boolean negated, Expression right)
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
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitMemberPredicate(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return List.of(right);
    }

    @Override
    public boolean equals(Object o)
    {
        return o instanceof MemberPredicate that
                && negated == that.negated
                && right.equals(that.right);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(negated, right);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        return sameClass(this, other)
                && negated == ((MemberPredicate) other).negated;
    }

    @Override
    public String toString()
    {
        return (negated ? "NOT MEMBER OF " : "MEMBER OF ") + right;
    }
}
