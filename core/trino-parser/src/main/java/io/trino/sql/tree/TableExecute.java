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
    private final List<CallArgument> arguments;
    private final Optional<Expression> where;

    public TableExecute(
            Table table,
            Identifier procedureName,
            List<CallArgument> properties,
            Optional<Expression> where)
    {
        this(Optional.empty(), table, procedureName, properties, where);
    }

    public TableExecute(
            NodeLocation location,
            Table table,
            Identifier procedureName,
            List<CallArgument> arguments,
            Optional<Expression> where)
    {
        this(Optional.of(location), table, procedureName, arguments, where);
    }

    private TableExecute(
            Optional<NodeLocation> location,
            Table table,
            Identifier procedureName,
            List<CallArgument> arguments,
            Optional<Expression> where)
    {
        super(location);
        this.table = requireNonNull(table, "table is null");
        this.procedureName = requireNonNull(procedureName, "procedureName is null");
        this.arguments = requireNonNull(arguments, "arguments is null");
        this.where = requireNonNull(where, "where is null");
    }

    public Table getTable()
    {
        return table;
    }

    public Identifier getProcedureName()
    {
        return procedureName;
    }

    public List<CallArgument> getArguments()
    {
        return arguments;
    }

    public Optional<Expression> getWhere()
    {
        return where;
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
        nodes.addAll(arguments);
        where.ifPresent(nodes::add);
        return nodes.build();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, procedureName, arguments, where);
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
                Objects.equals(arguments, that.arguments) &&
                Objects.equals(where, that.where);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .add("procedureNaem", procedureName)
                .add("arguments", arguments)
                .add("where", where)
                .toString();
    }
}
