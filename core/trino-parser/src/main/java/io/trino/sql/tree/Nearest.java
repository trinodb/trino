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

public final class Nearest
        extends Relation
{
    private final Relation relation;
    private final Optional<Expression> where;
    private final Expression match;

    public Nearest(NodeLocation location, Relation relation, Optional<Expression> where, Expression match)
    {
        super(location);
        this.relation = requireNonNull(relation, "relation is null");
        this.where = requireNonNull(where, "where is null");
        this.match = requireNonNull(match, "match is null");
    }

    public Relation getRelation()
    {
        return relation;
    }

    public Optional<Expression> getWhere()
    {
        return where;
    }

    public Expression getMatch()
    {
        return match;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitNearest(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        ImmutableList.Builder<Node> children = ImmutableList.builder();
        children.add(relation);
        where.ifPresent(children::add);
        children.add(match);
        return children.build();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("relation", relation)
                .add("where", where.orElse(null))
                .add("match", match)
                .omitNullValues()
                .toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(relation, where, match);
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
        Nearest other = (Nearest) obj;
        return Objects.equals(relation, other.relation) &&
                Objects.equals(where, other.where) &&
                Objects.equals(match, other.match);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        return sameClass(this, other);
    }
}
