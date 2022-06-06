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

import io.trino.Session;
import io.trino.cost.TableStatsProvider;
import io.trino.execution.warnings.WarningCollector;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.plan.DeleteNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.plan.SimplePlanRewriter;

import static io.trino.sql.planner.plan.SemiJoinNode.DistributionType.REPLICATED;
import static java.util.Objects.requireNonNull;

public class ReplicateSemiJoinInDelete
        implements PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector, TableStatsProvider tableStatsProvider)
    {
        requireNonNull(plan, "plan is null");
        return SimplePlanRewriter.rewriteWith(new Rewriter(), plan);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private boolean isDeleteQuery;

        @Override
        public PlanNode visitSemiJoin(SemiJoinNode node, RewriteContext<Void> context)
        {
            PlanNode sourceRewritten = context.rewrite(node.getSource(), context.get());
            PlanNode filteringSourceRewritten = context.rewrite(node.getFilteringSource(), context.get());

            SemiJoinNode rewrittenNode = new SemiJoinNode(
                    node.getId(),
                    sourceRewritten,
                    filteringSourceRewritten,
                    node.getSourceJoinSymbol(),
                    node.getFilteringSourceJoinSymbol(),
                    node.getSemiJoinOutput(),
                    node.getSourceHashSymbol(),
                    node.getFilteringSourceHashSymbol(),
                    node.getDistributionType(),
                    node.getDynamicFilterId());

            if (isDeleteQuery) {
                return rewrittenNode.withDistributionType(REPLICATED);
            }

            return rewrittenNode;
        }

        @Override
        public PlanNode visitDelete(DeleteNode node, RewriteContext<Void> context)
        {
            // For delete queries, the TableScan node that corresponds to the table being deleted must be collocated with the Delete node,
            // so you can't do a distributed semi-join
            isDeleteQuery = true;
            PlanNode rewrittenSource = context.rewrite(node.getSource());
            return new DeleteNode(
                    node.getId(),
                    rewrittenSource,
                    node.getTarget(),
                    node.getRowId(),
                    node.getOutputSymbols());
        }
    }
}
