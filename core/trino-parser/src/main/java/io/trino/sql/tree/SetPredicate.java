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

/// SQL spec `<set predicate part 2> ::= IS [NOT] A SET`. The multiset operand is held by the
/// enclosing [Predicated]; this fragment carries only the operator. `x IS A SET` is true when the
/// multiset contains no duplicate elements; `negated` captures the `IS NOT A SET` form.
public final class SetPredicate
        extends Predicate
{
    private final boolean negated;

    public SetPredicate(NodeLocation location, boolean negated)
    {
        super(location);
        this.negated = negated;
    }

    public boolean isNegated()
    {
        return negated;
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitSetPredicate(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return List.of();
    }

    @Override
    public boolean equals(Object o)
    {
        return o instanceof SetPredicate that
                && negated == that.negated;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(negated);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        return sameClass(this, other)
                && negated == ((SetPredicate) other).negated;
    }

    @Override
    public String toString()
    {
        return negated ? "IS NOT A SET" : "IS A SET";
    }
}
