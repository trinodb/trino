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
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.PlanNode;

import java.util.HashMap;
import java.util.Map;

import static io.trino.sql.planner.ExpressionSymbolInliner.inlineSymbols;
import static io.trino.sql.planner.plan.Patterns.exchange;

public final class PushFilterThroughExchange
        extends FilterPushdownRule<ExchangeNode>
{
    public PushFilterThroughExchange(PlannerContext plannerContext, boolean useTableProperties, boolean dynamicFiltering)
    {
        super(plannerContext, useTableProperties, dynamicFiltering, exchange());
    }

    @Override
    protected PlanNode pushDown(Pushdown pushdown, ExchangeNode node, Expression inheritedPredicate)
    {
        boolean modified = false;
        ImmutableList.Builder<PlanNode> builder = ImmutableList.builder();
        for (int i = 0; i < node.getSources().size(); i++) {
            Map<Symbol, Reference> outputsToInputs = new HashMap<>();
            for (int index = 0; index < node.getInputs().get(i).size(); index++) {
                outputsToInputs.put(
                        node.getOutputSymbols().get(index),
                        node.getInputs().get(i).get(index).toSymbolReference());
            }

            Expression sourcePredicate = inlineSymbols(outputsToInputs, inheritedPredicate);
            PlanNode source = node.getSources().get(i);
            PlanNode rewrittenSource = pushdown.filter(source, sourcePredicate);
            if (rewrittenSource != source) {
                modified = true;
            }
            builder.add(rewrittenSource);
        }

        if (modified) {
            return new ExchangeNode(
                    node.getId(),
                    node.getType(),
                    node.getScope(),
                    node.getPartitioningScheme(),
                    builder.build(),
                    node.getInputs(),
                    node.getOrderingScheme());
        }

        return node;
    }
}
