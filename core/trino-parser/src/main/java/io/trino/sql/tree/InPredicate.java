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

/// SQL spec `<in predicate part 2> ::= [NOT] IN <in predicate value>` where the value is
/// either an [InListExpression] or a [SubqueryExpression]. The in-place `NOT IN`
/// is recorded via [#isNegated()]; an outer `NOT (x IN (...))` stays as a
/// [NotExpression] wrapping a non-negated `InPredicate`.
public final class InPredicate
        extends Predicate
{
    private final boolean negated;
    private final Expression valueList;

    public InPredicate(NodeLocation location, boolean negated, Expression valueList)
    {
        super(location);
        this.negated = negated;
        this.valueList = requireNonNull(valueList, "valueList is null");
    }

    public boolean isNegated()
    {
        return negated;
    }

    public Expression getValueList()
    {
        return valueList;
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return List.of(valueList);
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitInPredicate(this, context);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        return sameClass(this, other) && negated == ((InPredicate) other).negated;
    }

    @Override
    public boolean equals(Object o)
    {
        return o instanceof InPredicate that && negated == that.negated && valueList.equals(that.valueList);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(negated, valueList);
    }

    @Override
    public String toString()
    {
        return (negated ? "NOT IN " : "IN ") + valueList;
    }
}
