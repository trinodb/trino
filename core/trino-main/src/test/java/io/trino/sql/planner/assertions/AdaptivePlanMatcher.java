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
package io.trino.sql.planner.assertions;

import io.trino.Session;
import io.trino.cost.StatsProvider;
import io.trino.metadata.Metadata;
import io.trino.sql.planner.plan.AdaptivePlanNode;
import io.trino.sql.planner.plan.PlanNode;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.sql.planner.iterative.Lookup.noLookup;
import static java.util.Objects.requireNonNull;

public class AdaptivePlanMatcher
        implements Matcher
{
    PlanMatchPattern initialPlan;

    public AdaptivePlanMatcher(PlanMatchPattern initialPlan)
    {
        this.initialPlan = requireNonNull(initialPlan, "initialPlanPattern is null");
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof AdaptivePlanNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider statsProvider, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());
        AdaptivePlanNode adaptivePlanNode = (AdaptivePlanNode) node;
        return adaptivePlanNode.getInitialPlan().accept(new PlanMatchingVisitor(session, metadata, statsProvider, noLookup()), initialPlan);
    }

    @Override
    public String toString()
    {
        return "AdaptivePlanMatcher\n" +
                "initialPlan: " + "\n" + initialPlan;
    }
}
