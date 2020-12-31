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
package io.trino.sql.planner.optimizations;

import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.DistinctLimitNode;
import io.trino.sql.planner.plan.EnforceSingleRowNode;
import io.trino.sql.planner.plan.ExceptNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.IntersectNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.ValuesNode;

import java.util.function.Function;

import static java.util.function.Function.identity;

public final class DistinctOutputQueryUtil
{
    private DistinctOutputQueryUtil() {}

    public static boolean isDistinct(PlanNode node)
    {
        return node.accept(new IsDistinctPlanVisitor(identity()), null);
    }

    public static boolean isDistinct(PlanNode node, Function<PlanNode, PlanNode> lookupFunction)
    {
        return node.accept(new IsDistinctPlanVisitor(lookupFunction), null);
    }

    private static final class IsDistinctPlanVisitor
            extends PlanVisitor<Boolean, Void>
    {
        /*
        With the iterative optimizer, plan nodes are replaced with
        GroupReference nodes. This requires the rules that look deeper
        in the tree than the rewritten node and its immediate sources
        to use Lookup for resolving the nodes corresponding to the
        GroupReferences.
        */
        private final Function<PlanNode, PlanNode> lookupFunction;

        private IsDistinctPlanVisitor(Function<PlanNode, PlanNode> lookupFunction)
        {
            this.lookupFunction = lookupFunction;
        }

        @Override
        protected Boolean visitPlan(PlanNode node, Void context)
        {
            return false;
        }

        @Override
        public Boolean visitAggregation(AggregationNode node, Void context)
        {
            return true;
        }

        @Override
        public Boolean visitAssignUniqueId(AssignUniqueId node, Void context)
        {
            return true;
        }

        @Override
        public Boolean visitDistinctLimit(DistinctLimitNode node, Void context)
        {
            return true;
        }

        @Override
        public Boolean visitEnforceSingleRow(EnforceSingleRowNode node, Void context)
        {
            return true;
        }

        @Override
        public Boolean visitExcept(ExceptNode node, Void context)
        {
            return node.isDistinct() || lookupFunction.apply(node.getSources().get(0)).accept(this, null);
        }

        @Override
        public Boolean visitFilter(FilterNode node, Void context)
        {
            return lookupFunction.apply(node.getSource()).accept(this, null);
        }

        @Override
        public Boolean visitIntersect(IntersectNode node, Void context)
        {
            return node.isDistinct() || node.getSources().stream().allMatch(source -> lookupFunction.apply(source).accept(this, null));
        }

        @Override
        public Boolean visitValues(ValuesNode node, Void context)
        {
            return node.getRowCount() <= 1;
        }

        @Override
        public Boolean visitLimit(LimitNode node, Void context)
        {
            return node.getCount() <= 1;
        }

        @Override
        public Boolean visitTopN(TopNNode node, Void context)
        {
            return node.getCount() <= 1;
        }
    }
}
