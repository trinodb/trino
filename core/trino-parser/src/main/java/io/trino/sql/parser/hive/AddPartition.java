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
package io.trino.sql.parser.hive;

import com.google.common.collect.ImmutableList;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.ColumnDefinition;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Statement;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class AddPartition
        extends Statement
{
    private final QualifiedName name;
    private final List<Map<Object, Object>> partitions;

    public AddPartition(QualifiedName name, ColumnDefinition column, List<Map<Object, Object>> partitions)
    {
        this(Optional.empty(), name, column, partitions);
    }

    public AddPartition(NodeLocation location, QualifiedName name, ColumnDefinition column, List<Map<Object, Object>> partitions)
    {
        this(Optional.of(location), name, column, partitions);
    }

    private AddPartition(Optional<NodeLocation> location, QualifiedName name, ColumnDefinition column, List<Map<Object, Object>> partitions)
    {
        super(location);
        this.name = requireNonNull(name, "table is null");
        this.partitions = requireNonNull(partitions, "partitionColumnNames is null");
    }

    public QualifiedName getName()

    {
        return name;
    }

    public List<Map<Object, Object>> getPartitions()
    {
        return partitions;
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitAddPartition(this, context);
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
        AddPartition that = (AddPartition) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(partitions, that.partitions);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, partitions);
    }

    @Override
    public String toString()
    {
        return "AddPartition{" +
                "name=" + name +
                ", partitions=" + partitions +
                '}';
    }
}
