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
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class PlanSiblings
        extends JsonTableSpecificPlan
{
    private final SiblingsPlanType type;
    private final List<JsonTableSpecificPlan> siblings;

    public PlanSiblings(NodeLocation location, SiblingsPlanType type, List<JsonTableSpecificPlan> siblings)
    {
        super(location);
        this.type = requireNonNull(type, "type is null");
        this.siblings = ImmutableList.copyOf(siblings);
        checkArgument(siblings.size() >= 2, "sibling plan must contain at least two siblings, actual: " + siblings.size());
    }

    public SiblingsPlanType getType()
    {
        return type;
    }

    public List<JsonTableSpecificPlan> getSiblings()
    {
        return siblings;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitPlanSiblings(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return siblings;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", type)
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

        PlanSiblings that = (PlanSiblings) o;
        return type == that.type &&
                Objects.equals(siblings, that.siblings);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, siblings);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        return type == ((PlanSiblings) other).type;
    }
}
