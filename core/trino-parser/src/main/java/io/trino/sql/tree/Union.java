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

public class Union
        extends SetOperation
{
    private final List<Relation> relations;

    public Union(List<Relation> relations, boolean distinct, boolean corresponding)
    {
        this(Optional.empty(), relations, distinct, corresponding);
    }

    public Union(NodeLocation location, List<Relation> relations, boolean distinct, boolean corresponding)
    {
        this(Optional.of(location), relations, distinct, corresponding);
    }

    private Union(Optional<NodeLocation> location, List<Relation> relations, boolean distinct, boolean corresponding)
    {
        super(location, distinct, corresponding);
        requireNonNull(relations, "relations is null");

        this.relations = ImmutableList.copyOf(relations);
    }

    @Override
    public List<Relation> getRelations()
    {
        return relations;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitUnion(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return relations;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("relations", relations)
                .add("distinct", isDistinct())
                .add("corresponding", isCorresponding())
                .toString();
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
        Union o = (Union) obj;
        return Objects.equals(relations, o.relations) &&
               isDistinct() == o.isDistinct() &&
               isCorresponding() == o.isCorresponding();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(relations, isDistinct(), isCorresponding());
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        Union otherUnion = (Union) other;
        return this.isDistinct() == otherUnion.isDistinct() &&
               this.isCorresponding() == otherUnion.isCorresponding();
    }
}
