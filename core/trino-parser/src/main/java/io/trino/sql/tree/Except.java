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

public class Except
        extends SetOperation
{
    private final QueryBody left;
    private final QueryBody right;

    public Except(NodeLocation location, QueryBody left, QueryBody right, boolean distinct, Optional<Corresponding> corresponding)
    {
        super(Optional.of(location), distinct, corresponding);
        requireNonNull(left, "left is null");
        requireNonNull(right, "right is null");

        this.left = left;
        this.right = right;
    }

    public Relation getLeft()
    {
        return left;
    }

    public Relation getRight()
    {
        return right;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitExcept(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(left, right);
    }

    @Override
    public List<Relation> getRelations()
    {
        return ImmutableList.of(left, right);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("left", left)
                .add("right", right)
                .add("distinct", isDistinct())
                .add("corresponding", getCorresponding())
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
        Except o = (Except) obj;
        return Objects.equals(left, o.left) &&
                Objects.equals(right, o.right) &&
                isDistinct() == o.isDistinct() &&
                Objects.equals(getCorresponding(), o.getCorresponding());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(left, right, isDistinct(), getCorresponding());
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        Except otherExcept = (Except) other;
        return this.isDistinct() == otherExcept.isDistinct() &&
                Objects.equals(getCorresponding(), otherExcept.getCorresponding());
    }
}
