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

public class TableExecute
        extends Statement
{
    private final Table table;
    private final Identifier procedureName;
    private final List<Property> properties;
    private final Optional<Expression> where;
    private final Optional<OrderBy> orderBy;

    public TableExecute(
            Table table,
            Identifier procedureName,
            List<Property> properties,
            Optional<Expression> where,
            Optional<OrderBy> orderBy)
    {
        this(Optional.empty(), table, procedureName, properties, where, orderBy);
    }

    public TableExecute(
            NodeLocation location,
            Table table,
            Identifier procedureName,
            List<Property> properties,
            Optional<Expression> where,
            Optional<OrderBy> orderBy)
    {
        this(Optional.of(location), table, procedureName, properties, where, orderBy);
    }

    private TableExecute(
            Optional<NodeLocation> location,
            Table table,
            Identifier procedureName,
            List<Property> properties,
            Optional<Expression> where,
            Optional<OrderBy> orderBy)
    {
        super(location);
        this.table = requireNonNull(table, "table is null");
        this.procedureName = requireNonNull(procedureName, "procedureName is null");
        this.properties = requireNonNull(properties, "properties is null");
        this.where = requireNonNull(where, "where is null");
        this.orderBy = requireNonNull(orderBy, "orderBy is null");
    }

    public Table getTable()
    {
        return table;
    }

    public Identifier getProcedureName()
    {
        return procedureName;
    }

    public List<Property> getProperties()
    {
        return properties;
    }

    public Optional<Expression> getWhere()
    {
        return where;
    }

    public Optional<OrderBy> getOrderBy()
    {
        return orderBy;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitTableExecute(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        nodes.addAll(properties);
        where.ifPresent(nodes::add);
        orderBy.ifPresent(nodes::add);
        return nodes.build();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, procedureName, properties, where, orderBy);
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
        TableExecute that = (TableExecute) o;
        return Objects.equals(table, that.table) &&
                Objects.equals(procedureName, that.procedureName) &&
                Objects.equals(properties, that.properties) &&
                Objects.equals(where, that.where) &&
                Objects.equals(orderBy, that.orderBy);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .add("procedureNaem", procedureName)
                .add("properties", properties)
                .add("where", where)
                .add("orderBy", orderBy)
                .toString();
    }
}
