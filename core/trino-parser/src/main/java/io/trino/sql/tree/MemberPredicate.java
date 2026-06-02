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

/// The `x MEMBER OF y` predicate: true when the value `value` is equal to at least one element
/// of the multiset `right`. Three-valued like `IN`: unknown when `value` or `right`
/// is null, or when `value` matches no element but `right` contains a null.
public class MemberPredicate
        extends Expression
{
    private final Expression value;
    private final Expression right;

    public MemberPredicate(NodeLocation location, Expression value, Expression right)
    {
        super(location);
        this.value = requireNonNull(value, "value is null");
        this.right = requireNonNull(right, "right is null");
    }

    public Expression getValue()
    {
        return value;
    }

    public Expression getRight()
    {
        return right;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitMemberPredicate(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of(value, right);
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

        MemberPredicate that = (MemberPredicate) o;
        return Objects.equals(value, that.value) &&
                Objects.equals(right, that.right);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value, right);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        return sameClass(this, other);
    }
}
