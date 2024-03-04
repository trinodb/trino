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
    private final ColumnPosition position;
    private final Optional<Identifier> after;
    private final boolean tableExists;
    private final boolean columnNotExists;

    public AddColumn(QualifiedName name, ColumnDefinition column, ColumnPosition position, Optional<Identifier> after, boolean tableExists, boolean columnNotExists)
    {
        this(Optional.empty(), name, column, position, after, tableExists, columnNotExists);
    }

    public AddColumn(NodeLocation location, QualifiedName name, ColumnDefinition column, ColumnPosition position, Optional<Identifier> after, boolean tableExists, boolean columnNotExists)
    {
        this(Optional.of(location), name, column, position, after, tableExists, columnNotExists);
    }

    private AddColumn(Optional<NodeLocation> location, QualifiedName name, ColumnDefinition column, ColumnPosition position, Optional<Identifier> after, boolean tableExists, boolean columnNotExists)
    {
        super(location);
        this.name = requireNonNull(name, "name is null");
        this.column = requireNonNull(column, "column is null");
        this.position = requireNonNull(position, "position is null");
        this.after = requireNonNull(after, "after is null");
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

    public ColumnPosition getPosition()
    {
        return position;
    }

    public Optional<Identifier> getAfter()
    {
        return after;
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
        after.ifPresent(nodes::add);
        return nodes.build();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, column, position, after);
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
                position == o.position &&
                Objects.equals(after, o.after);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("column", column)
                .add("position", position)
                .add("after", after)
                .toString();
    }
}
