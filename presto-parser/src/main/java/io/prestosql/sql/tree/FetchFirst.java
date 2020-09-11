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
package io.prestosql.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

public class FetchFirst
        extends Node
{
    private final Optional<Expression> rowCount;
    private final boolean withTies;

    public FetchFirst(Expression rowCount)
    {
        this(Optional.empty(), Optional.of(rowCount), false);
    }

    public FetchFirst(Expression rowCount, boolean withTies)
    {
        this(Optional.empty(), Optional.of(rowCount), withTies);
    }

    public FetchFirst(Optional<Expression> rowCount)
    {
        this(Optional.empty(), rowCount, false);
    }

    public FetchFirst(Optional<Expression> rowCount, boolean withTies)
    {
        this(Optional.empty(), rowCount, withTies);
    }

    public FetchFirst(Optional<NodeLocation> location, Optional<Expression> rowCount, boolean withTies)
    {
        super(location);
        rowCount.ifPresent(count -> checkArgument(
                count instanceof LongLiteral || count instanceof Parameter,
                "unexpected rowCount class: %s",
                rowCount.getClass().getSimpleName()));
        this.rowCount = rowCount;
        this.withTies = withTies;
    }

    public Optional<Expression> getRowCount()
    {
        return rowCount;
    }

    public boolean isWithTies()
    {
        return withTies;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitFetchFirst(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return rowCount.map(ImmutableList::of).orElse(ImmutableList.of());
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if ((o == null) || (getClass() != o.getClass())) {
            return false;
        }
        FetchFirst that = (FetchFirst) o;
        return withTies == that.withTies &&
                Objects.equals(rowCount, that.rowCount);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(rowCount, withTies);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("rowCount", rowCount.orElse(null))
                .add("withTies", withTies)
                .omitNullValues()
                .toString();
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        FetchFirst otherNode = (FetchFirst) other;

        return withTies == otherNode.withTies;
    }
}
