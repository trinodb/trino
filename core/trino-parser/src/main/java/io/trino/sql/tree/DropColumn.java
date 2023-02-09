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

public class DropColumn
        extends Statement
{
    private final QualifiedName table;
    private final QualifiedName field;
    private final boolean tableExists;
    private final boolean columnExists;

    public DropColumn(QualifiedName table, QualifiedName field, boolean tableExists, boolean columnExists)
    {
        this(Optional.empty(), table, field, tableExists, columnExists);
    }

    public DropColumn(NodeLocation location, QualifiedName table, QualifiedName field, boolean tableExists, boolean columnExists)
    {
        this(Optional.of(location), table, field, tableExists, columnExists);
    }

    private DropColumn(Optional<NodeLocation> location, QualifiedName table, QualifiedName field, boolean tableExists, boolean columnExists)
    {
        super(location);
        this.table = requireNonNull(table, "table is null");
        this.field = requireNonNull(field, "field is null");
        this.tableExists = tableExists;
        this.columnExists = columnExists;
    }

    public QualifiedName getTable()
    {
        return table;
    }

    public QualifiedName getField()
    {
        return field;
    }

    public boolean isTableExists()
    {
        return tableExists;
    }

    public boolean isColumnExists()
    {
        return columnExists;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDropColumn(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
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
        DropColumn that = (DropColumn) o;
        return Objects.equals(table, that.table) &&
                Objects.equals(field, that.field);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, field);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .add("field", field)
                .toString();
    }
}
