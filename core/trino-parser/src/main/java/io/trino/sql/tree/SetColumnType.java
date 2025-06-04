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

public class SetColumnType
        extends Statement
{
    private final QualifiedName tableName;
    private final QualifiedName columnName;
    private final DataType type;
    private final boolean tableExists;

    public SetColumnType(NodeLocation location, QualifiedName tableName, QualifiedName columnName, DataType type, boolean tableExists)
    {
        super(location);
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.type = requireNonNull(type, "type is null");
        this.tableExists = tableExists;
    }

    public QualifiedName getTableName()
    {
        return tableName;
    }

    public QualifiedName getColumnName()
    {
        return columnName;
    }

    public DataType getType()
    {
        return type;
    }

    public boolean isTableExists()
    {
        return tableExists;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitSetColumnType(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(type);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableName, columnName, type, tableExists);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SetColumnType o = (SetColumnType) obj;
        return Objects.equals(tableName, o.tableName) &&
                Objects.equals(columnName, o.columnName) &&
                Objects.equals(type, o.type) &&
                tableExists == o.tableExists;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", tableName)
                .add("column", columnName)
                .add("type", type)
                .add("tableExists", tableExists)
                .toString();
    }
}
