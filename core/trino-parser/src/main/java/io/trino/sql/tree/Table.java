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

public class Table
        extends QueryBody
{
    private final QualifiedName name;
    private final Optional<QueryPeriod> queryPeriod;
    private final List<Property> properties;

    public Table(QualifiedName name)
    {
        this(Optional.empty(), name, Optional.empty(), ImmutableList.of());
    }

    public Table(NodeLocation location, QualifiedName name)
    {
        this(Optional.of(location), name, Optional.empty(), ImmutableList.of());
    }

    public Table(NodeLocation location, QualifiedName name, List<Property> properties)
    {
        this(Optional.of(location), name, Optional.empty(), properties);
    }

    public Table(NodeLocation location, QualifiedName name, QueryPeriod queryPeriod, List<Property> properties)
    {
        this(Optional.of(location), name, Optional.of(queryPeriod), properties);
    }

    private Table(Optional<NodeLocation> location, QualifiedName name, Optional<QueryPeriod> queryPeriod, List<Property> properties)
    {
        super(location);
        this.name = name;
        this.queryPeriod = queryPeriod;
        this.properties = ImmutableList.copyOf(properties);
    }

    public QualifiedName getName()
    {
        return name;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitTable(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        queryPeriod.ifPresent(nodes::add);
        nodes.addAll(properties);
        return nodes.build();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(name)
                .addValue(queryPeriod)
                .addValue(properties)
                .toString();
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

        Table table = (Table) o;
        return Objects.equals(name, table.name) &&
                Objects.equals(queryPeriod, table.getQueryPeriod()) &&
                Objects.equals(properties, table.getProperties());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, queryPeriod, properties);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        Table otherTable = (Table) other;
        return name.equals(otherTable.name);
    }

    public Optional<QueryPeriod> getQueryPeriod()
    {
        return queryPeriod;
    }

    public List<Property> getProperties()
    {
        return properties;
    }
}
