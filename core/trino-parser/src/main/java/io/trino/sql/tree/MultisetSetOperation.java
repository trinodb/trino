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

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/// A `MULTISET UNION`, `MULTISET INTERSECT` or `MULTISET EXCEPT` operation. The `distinct` flag
/// selects the DISTINCT form (set semantics); when false the ALL form (multiplicity semantics) is
/// used, which is also the default when neither ALL nor DISTINCT is written.
public class MultisetSetOperation
        extends Expression
{
    public enum Operator
    {
        UNION,
        INTERSECT,
        EXCEPT,
    }

    private final Operator operator;
    private final boolean distinct;
    private final Expression left;
    private final Expression right;

    public MultisetSetOperation(NodeLocation location, Operator operator, boolean distinct, Expression left, Expression right)
    {
        super(location);
        this.operator = requireNonNull(operator, "operator is null");
        this.distinct = distinct;
        this.left = requireNonNull(left, "left is null");
        this.right = requireNonNull(right, "right is null");
    }

    public Operator getOperator()
    {
        return operator;
    }

    public boolean isDistinct()
    {
        return distinct;
    }

    public Expression getLeft()
    {
        return left;
    }

    public Expression getRight()
    {
        return right;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitMultisetSetOperation(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
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

        MultisetSetOperation that = (MultisetSetOperation) o;
        return distinct == that.distinct &&
                operator == that.operator &&
                Objects.equals(left, that.left) &&
                Objects.equals(right, that.right);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(operator, distinct, left, right);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        MultisetSetOperation that = (MultisetSetOperation) other;
        return operator == that.operator && distinct == that.distinct;
    }
}
