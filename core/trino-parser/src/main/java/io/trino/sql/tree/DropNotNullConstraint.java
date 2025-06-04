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

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class DropNotNullConstraint
        extends Statement
{
    private final QualifiedName table;
    private final Identifier column;
    private final boolean tableExists;

    public DropNotNullConstraint(NodeLocation location, QualifiedName table, Identifier column, boolean tableExists)
    {
        super(location);
        this.table = requireNonNull(table, "table is null");
        this.column = requireNonNull(column, "field is null");
        this.tableExists = tableExists;
    }

    public QualifiedName getTable()
    {
        return table;
    }

    public Identifier getColumn()
    {
        return column;
    }

    public boolean isTableExists()
    {
        return tableExists;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDropNotNullConstraint(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(column);
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
        DropNotNullConstraint that = (DropNotNullConstraint) o;
        return Objects.equals(table, that.table) &&
                Objects.equals(column, that.column);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, column);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .add("column", column)
                .toString();
    }
}
