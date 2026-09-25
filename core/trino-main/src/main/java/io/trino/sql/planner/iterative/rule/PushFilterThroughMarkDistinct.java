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

import com.google.common.collect.ImmutableSet;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.PlanNode;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.trino.sql.ir.IrUtils.combineConjuncts;
import static io.trino.sql.ir.IrUtils.extractConjuncts;
import static io.trino.sql.planner.SymbolsExtractor.extractUnique;
import static io.trino.sql.planner.plan.Patterns.markDistinct;

public final class PushFilterThroughMarkDistinct
        extends FilterPushdownRule<MarkDistinctNode>
{
    public PushFilterThroughMarkDistinct(PlannerContext plannerContext, boolean useTableProperties, boolean dynamicFiltering)
    {
        super(plannerContext, useTableProperties, dynamicFiltering, markDistinct());
    }

    @Override
    protected PlanNode pushDown(Pushdown pushdown, MarkDistinctNode node, Expression inheritedPredicate)
    {
        Set<Symbol> pushDownableSymbols = ImmutableSet.copyOf(node.getDistinctSymbols());
        Map<Boolean, List<Expression>> conjuncts = extractConjuncts(inheritedPredicate).stream()
                .collect(Collectors.partitioningBy(conjunct -> pushDownableSymbols.containsAll(extractUnique(conjunct))));

        PlanNode rewrittenNode = pushdown.filterChildren(node, combineConjuncts(conjuncts.get(true)));

        if (!conjuncts.get(false).isEmpty()) {
            rewrittenNode = new FilterNode(pushdown.idAllocator.getNextId(), rewrittenNode, combineConjuncts(conjuncts.get(false)));
        }
        return rewrittenNode;
    }
}
