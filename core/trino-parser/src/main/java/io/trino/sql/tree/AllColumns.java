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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class AllColumns
        extends SelectItem
{
    private final List<Identifier> aliases;
    private final Optional<Expression> target;
    private final List<Identifier> excludedColumns;

    @Deprecated
    public AllColumns()
    {
        this(Optional.empty(), Optional.empty(), ImmutableList.of(), ImmutableList.of());
    }

    @Deprecated
    public AllColumns(Expression target, List<Identifier> aliases)
    {
        this(Optional.empty(), Optional.of(target), aliases, ImmutableList.of());
    }

    @Deprecated
    public AllColumns(Optional<NodeLocation> location, Optional<Expression> target, List<Identifier> aliases)
    {
        this(location, target, aliases, ImmutableList.of());
    }

    @Deprecated
    public AllColumns(Optional<NodeLocation> location, Optional<Expression> target, List<Identifier> aliases, List<Identifier> excludedColumns)
    {
        super(location);
        this.aliases = ImmutableList.copyOf(requireNonNull(aliases, "aliases is null"));
        this.target = requireNonNull(target, "target is null");
        this.excludedColumns = ImmutableList.copyOf(excludedColumns);
    }

    public AllColumns(NodeLocation location)
    {
        this(location, Optional.empty(), ImmutableList.of(), ImmutableList.of());
    }

    public AllColumns(NodeLocation location, Optional<Expression> target, List<Identifier> aliases)
    {
        this(location, target, aliases, ImmutableList.of());
    }

    public AllColumns(NodeLocation location, Optional<Expression> target, List<Identifier> aliases, List<Identifier> excludedColumns)
    {
        super(location);
        this.aliases = ImmutableList.copyOf(aliases);
        this.target = requireNonNull(target, "target is null");
        this.excludedColumns = ImmutableList.copyOf(excludedColumns);
    }

    public List<Identifier> getAliases()
    {
        return aliases;
    }

    public Optional<Expression> getTarget()
    {
        return target;
    }

    public List<Identifier> getExcludedColumns()
    {
        return excludedColumns;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitAllColumns(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return target.map(ImmutableList::<Node>of)
                .orElse(ImmutableList.of());
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

        AllColumns other = (AllColumns) o;
        return Objects.equals(aliases, other.aliases) &&
                Objects.equals(target, other.target) &&
                Objects.equals(excludedColumns, other.excludedColumns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(aliases, target, excludedColumns);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();

        target.ifPresent(value -> builder.append(value).append("."));
        builder.append("*");

        if (!excludedColumns.isEmpty()) {
            builder.append(" (EXCLUDE (");
            Joiner.on(", ").appendTo(builder, excludedColumns);
            builder.append("))");
        }

        if (!aliases.isEmpty()) {
            builder.append(" (");
            Joiner.on(", ").appendTo(builder, aliases);
            builder.append(")");
        }

        return builder.toString();
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        AllColumns otherAllColumns = (AllColumns) other;
        return aliases.equals(otherAllColumns.aliases) &&
                excludedColumns.equals(otherAllColumns.excludedColumns);
    }
}
