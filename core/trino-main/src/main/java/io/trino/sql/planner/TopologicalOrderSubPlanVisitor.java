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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.graph.SuccessorsFunction;
import com.google.common.graph.Traverser;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.RemoteSourceNode;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public final class TopologicalOrderSubPlanVisitor
{
    private TopologicalOrderSubPlanVisitor() {}

    public static List<SubPlan> sortPlanInTopologicalOrder(SubPlan subPlan)
    {
        return ImmutableList.copyOf(Traverser.forTree(getChildren).depthFirstPostOrder(subPlan));
    }

    private static final SuccessorsFunction<SubPlan> getChildren = subPlan -> {
        Visitor visitor = new Visitor(subPlan);
        subPlan.getFragment().getRoot().accept(visitor, null);
        checkState(visitor.getSourceSubPlans().isEmpty(),
                "Some SubNode sources have not been visited: %s",
                visitor.getSourceSubPlans());
        return visitor.getChildren();
    };

    private static class Visitor
            extends BuildSideJoinPlanVisitor<Void>
    {
        private final SubPlan subPlan;
        private final Map<PlanFragmentId, SubPlan> sourceSubPlans;
        private final ImmutableList.Builder<SubPlan> children = ImmutableList.builder();

        public Visitor(SubPlan subPlan)
        {
            this.subPlan = subPlan;
            this.sourceSubPlans = subPlan.getChildren().stream()
                    .collect(toMap(plan -> plan.getFragment().getId(), plan -> plan));
        }

        public Map<PlanFragmentId, SubPlan> getSourceSubPlans()
        {
            return sourceSubPlans;
        }

        public List<SubPlan> getChildren()
        {
            return children.build();
        }

        @Override
        public Void visitRemoteSource(RemoteSourceNode node, Void context)
        {
            for (PlanFragmentId fragmentId : node.getSourceFragmentIds()) {
                SubPlan child = sourceSubPlans.remove(fragmentId);
                requireNonNull(child, "PlanFragmentId %s does not appear in sources of %s".formatted(fragmentId, subPlan));
                children.add(child);
            }
            return null;
        }
    }
}
