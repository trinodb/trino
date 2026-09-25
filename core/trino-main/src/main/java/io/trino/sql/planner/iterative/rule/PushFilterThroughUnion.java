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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.UnionNode;

import static io.trino.sql.planner.ExpressionSymbolInliner.inlineSymbols;
import static io.trino.sql.planner.plan.Patterns.union;

public final class PushFilterThroughUnion
        extends FilterPushdownRule<UnionNode>
{
    public PushFilterThroughUnion(PlannerContext plannerContext, boolean useTableProperties, boolean dynamicFiltering)
    {
        super(plannerContext, useTableProperties, dynamicFiltering, union());
    }

    @Override
    protected PlanNode pushDown(Pushdown pushdown, UnionNode node, Expression inheritedPredicate)
    {
        boolean modified = false;
        ImmutableList.Builder<PlanNode> builder = ImmutableList.builder();
        for (int i = 0; i < node.getSources().size(); i++) {
            Expression sourcePredicate = inlineSymbols(node.sourceSymbolMap(i), inheritedPredicate);
            PlanNode source = node.getSources().get(i);
            PlanNode rewrittenSource = pushdown.filter(source, sourcePredicate);
            if (rewrittenSource != source) {
                modified = true;
            }
            builder.add(rewrittenSource);
        }

        if (modified) {
            return new UnionNode(node.getId(), builder.build(), node.getSymbolMapping(), node.getOutputSymbols());
        }

        return node;
    }
}
