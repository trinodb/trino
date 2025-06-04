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

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class AddColumn
        extends Statement
{
    private final QualifiedName name;
    private final ColumnDefinition column;
    private final Optional<ColumnPosition> position;
    private final boolean tableExists;
    private final boolean columnNotExists;

    public AddColumn(NodeLocation location, QualifiedName name, ColumnDefinition column, Optional<ColumnPosition> position, boolean tableExists, boolean columnNotExists)
    {
        super(location);
        this.name = requireNonNull(name, "name is null");
        this.column = requireNonNull(column, "column is null");
        this.position = requireNonNull(position, "position is null");
        this.tableExists = tableExists;
        this.columnNotExists = columnNotExists;
    }

    public QualifiedName getName()
    {
        return name;
    }

    public ColumnDefinition getColumn()
    {
        return column;
    }

    public Optional<ColumnPosition> getPosition()
    {
        return position;
    }

    public boolean isTableExists()
    {
        return tableExists;
    }

    public boolean isColumnNotExists()
    {
        return columnNotExists;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitAddColumn(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        nodes.add(column);
        if (position.isPresent() && position.get() instanceof ColumnPosition.After after) {
            nodes.add(after.column());
        }
        return nodes.build();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, column, position);
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
        AddColumn o = (AddColumn) obj;
        return Objects.equals(name, o.name) &&
                Objects.equals(column, o.column) &&
                Objects.equals(position, o.position);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("column", column)
                .add("position", position)
                .toString();
    }
}
