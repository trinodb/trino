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

public class JsonTableDefaultPlan
        extends JsonTablePlan
{
    private final ParentChildPlanType parentChild;
    private final SiblingsPlanType siblings;

    public JsonTableDefaultPlan(NodeLocation location, ParentChildPlanType parentChildPlanType, SiblingsPlanType siblingsPlanType)
    {
        super(location);
        this.parentChild = requireNonNull(parentChildPlanType, "parentChild is null");
        this.siblings = requireNonNull(siblingsPlanType, "siblings is null");
    }

    public ParentChildPlanType getParentChild()
    {
        return parentChild;
    }

    public SiblingsPlanType getSiblings()
    {
        return siblings;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitJsonTableDefaultPlan(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("parentChild", parentChild)
                .add("siblings", siblings)
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

        JsonTableDefaultPlan that = (JsonTableDefaultPlan) o;
        return parentChild == that.parentChild &&
                siblings == that.siblings;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(parentChild, siblings);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        JsonTableDefaultPlan otherPlan = (JsonTableDefaultPlan) other;

        return parentChild == otherPlan.parentChild &&
                siblings == otherPlan.siblings;
    }
}
