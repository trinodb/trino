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
package io.prestosql.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

public class Offset
        extends Node
{
    private final Expression rowCount;

    public Offset(Expression rowCount)
    {
        this(Optional.empty(), rowCount);
    }

    public Offset(NodeLocation location, Expression rowCount)
    {
        this(Optional.of(location), rowCount);
    }

    public Offset(Optional<NodeLocation> location, Expression rowCount)
    {
        super(location);
        checkArgument(rowCount instanceof LongLiteral || rowCount instanceof Parameter,
                "unexpected rowCount class: %s",
                rowCount.getClass().getSimpleName());
        this.rowCount = rowCount;
    }

    public Expression getRowCount()
    {
        return rowCount;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitOffset(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of(rowCount);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        Offset o = (Offset) obj;
        return Objects.equals(rowCount, o.rowCount);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(rowCount);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("rowCount", rowCount)
                .toString();
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        return sameClass(this, other);
    }
}
