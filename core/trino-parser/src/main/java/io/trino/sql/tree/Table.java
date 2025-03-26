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
    private final Optional<String> branch;

    public Table(QualifiedName name)
    {
        this(Optional.empty(), name, Optional.empty(), Optional.empty());
    }

    public Table(NodeLocation location, QualifiedName name)
    {
        this(Optional.of(location), name, Optional.empty(), Optional.empty());
    }

    public Table(NodeLocation location, QualifiedName name, Optional<String> properties)
    {
        this(Optional.of(location), name, Optional.empty(), properties);
    }

    public Table(NodeLocation location, QualifiedName name, QueryPeriod queryPeriod, Optional<String> branch)
    {
        this(Optional.of(location), name, Optional.of(queryPeriod), branch);
    }

    private Table(Optional<NodeLocation> location, QualifiedName name, Optional<QueryPeriod> queryPeriod, Optional<String> branch)
    {
        super(location);
        this.name = name;
        this.queryPeriod = queryPeriod;
        this.branch = branch;
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
        if (queryPeriod.isPresent()) {
            return ImmutableList.of(queryPeriod.get());
        }
        return ImmutableList.of();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(name)
                .addValue(queryPeriod)
                .addValue(branch)
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
                Objects.equals(branch, table.branch);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, queryPeriod, branch);
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

    public Optional<String> getBranch()
    {
        return branch;
    }
}
