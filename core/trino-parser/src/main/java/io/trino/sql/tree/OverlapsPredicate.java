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

/// SQL spec `<overlaps predicate part 2> ::= OVERLAPS <row value predicand>`.
public final class OverlapsPredicate
        extends Predicate
{
    private final Expression right;

    public OverlapsPredicate(NodeLocation location, Expression right)
    {
        super(location);
        this.right = requireNonNull(right, "right is null");
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
        return visitor.visitOverlapsPredicate(this, context);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        return sameClass(this, other);
    }

    @Override
    public boolean equals(Object o)
    {
        return o instanceof OverlapsPredicate that && right.equals(that.right);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(right);
    }

    @Override
    public String toString()
    {
        return "OVERLAPS " + right;
    }
}
