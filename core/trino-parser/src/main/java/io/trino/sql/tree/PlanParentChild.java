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

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class PlanParentChild
        extends JsonTableSpecificPlan
{
    private final ParentChildPlanType type;
    private final PlanLeaf parent;
    private final JsonTableSpecificPlan child;

    public PlanParentChild(NodeLocation location, ParentChildPlanType type, PlanLeaf parent, JsonTableSpecificPlan child)
    {
        super(location);
        this.type = requireNonNull(type, "type is null");
        this.parent = requireNonNull(parent, "parent is null");
        this.child = requireNonNull(child, "child is null");
    }

    public ParentChildPlanType getType()
    {
        return type;
    }

    public PlanLeaf getParent()
    {
        return parent;
    }

    public JsonTableSpecificPlan getChild()
    {
        return child;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitPlanParentChild(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of(child);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", type)
                .add("parent", parent)
                .add("child", child)
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

        PlanParentChild that = (PlanParentChild) o;
        return type == that.type &&
                Objects.equals(parent, that.parent) &&
                Objects.equals(child, that.child);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, parent, child);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        PlanParentChild otherPlan = (PlanParentChild) other;

        return type == otherPlan.type &&
                parent.equals(otherPlan.parent);
    }
}
