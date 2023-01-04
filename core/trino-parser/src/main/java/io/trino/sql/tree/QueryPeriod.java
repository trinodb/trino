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

import static java.util.Objects.requireNonNull;

public class QueryPeriod
        extends Node
{
    private final Optional<Expression> start;
    private final Optional<Expression> end;
    private final RangeType rangeType;

    public enum RangeType {
        TIMESTAMP,
        VERSION
    }

    public QueryPeriod(NodeLocation location, RangeType rangeType, Expression end)
    {
        this(location, rangeType, Optional.empty(), Optional.of(end));
    }

    private QueryPeriod(NodeLocation location, RangeType rangeType, Optional<Expression> start, Optional<Expression> end)
    {
        super(Optional.of(location));
        this.rangeType = requireNonNull(rangeType, "rangeType is null");
        this.start = start;
        this.end = end;
    }

    @Override
    public List<Node> getChildren()
    {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        start.ifPresent(nodes::add);
        end.ifPresent(nodes::add);
        return nodes.build();
    }

    public Optional<Expression> getStart()
    {
        return start;
    }

    public Optional<Expression> getEnd()
    {
        return end;
    }

    public RangeType getRangeType()
    {
        return rangeType;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitQueryPeriod(this, context);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        QueryPeriod o = (QueryPeriod) obj;
        return Objects.equals(rangeType, o.rangeType) &&
                Objects.equals(start, o.start) &&
                Objects.equals(end, o.end);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(rangeType, start, end);
    }

    @Override
    public String toString()
    {
        return "FOR " + rangeType.toString() + " AS OF " + end.get().toString();
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        return rangeType.equals(((QueryPeriod) other).rangeType);
    }
}
