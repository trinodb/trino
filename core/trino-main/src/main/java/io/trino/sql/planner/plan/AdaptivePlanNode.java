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
package io.trino.sql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.trino.sql.planner.Symbol;

import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class AdaptivePlanNode
        extends PlanNode
{
    private final PlanNode initialPlan;
    // We do not store the initial plan types in PlanFragment#types since initial plan is only stored for
    // printing purposes. Therefore, we need to store the types separately here to be able to print the
    // initial plan.
    private final Set<Symbol> initialSymbols;
    private final PlanNode currentPlan;

    @JsonCreator
    public AdaptivePlanNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("initialPlan") PlanNode initialPlan,
            @JsonProperty("initialSymbols") Set<Symbol> initialSymbols,
            @JsonProperty("currentPlan") PlanNode currentPlan)
    {
        super(id);

        this.initialPlan = requireNonNull(initialPlan, "initialPlan is null");
        this.initialSymbols = ImmutableSet.copyOf(initialSymbols);
        this.currentPlan = requireNonNull(currentPlan, "currentPlan is null");
    }

    @JsonProperty
    public PlanNode getInitialPlan()
    {
        return initialPlan;
    }

    @JsonProperty
    public Set<Symbol> getInitialSymbols()
    {
        return initialSymbols;
    }

    @JsonProperty
    public PlanNode getCurrentPlan()
    {
        return currentPlan;
    }

    @Override
    public List<PlanNode> getSources()
    {
        // The initial plan is not used in the execution, so it is not a source of the adaptive plan.
        return ImmutableList.of(currentPlan);
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return currentPlan.getOutputSymbols();
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new AdaptivePlanNode(getId(), initialPlan, initialSymbols, Iterables.getOnlyElement(newChildren));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitAdaptivePlanNode(this, context);
    }
}
