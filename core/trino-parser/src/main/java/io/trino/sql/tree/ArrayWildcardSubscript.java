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
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/// SQL:2023 §6.36 T864 array wildcard accessor: `base[*]` selects all
/// elements of a JSON array. Parallels [DereferenceExpression] with an empty
/// field for the `base.*` member-wildcard form.
public class ArrayWildcardSubscript
        extends Expression
{
    private final Expression base;

    public ArrayWildcardSubscript(NodeLocation location, Expression base)
    {
        this(Optional.of(location), base);
    }

    public ArrayWildcardSubscript(Optional<NodeLocation> location, Expression base)
    {
        super(location);
        this.base = requireNonNull(base, "base is null");
    }

    public Expression getBase()
    {
        return base;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitArrayWildcardSubscript(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(base);
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
        ArrayWildcardSubscript that = (ArrayWildcardSubscript) o;
        return Objects.equals(base, that.base);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(base);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        return sameClass(this, other);
    }
}
