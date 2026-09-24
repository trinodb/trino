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

import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.GroupIdNode;
import io.trino.sql.planner.plan.PlanNode;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.sql.ir.IrUtils.combineConjuncts;
import static io.trino.sql.ir.IrUtils.extractConjuncts;
import static io.trino.sql.planner.ExpressionSymbolInliner.inlineSymbols;
import static io.trino.sql.planner.SymbolsExtractor.extractUnique;
import static io.trino.sql.planner.plan.Patterns.groupId;

public final class PushFilterThroughGroupId
        extends FilterPushdownRule<GroupIdNode>
{
    public PushFilterThroughGroupId(PlannerContext plannerContext, boolean useTableProperties, boolean dynamicFiltering)
    {
        super(plannerContext, useTableProperties, dynamicFiltering, groupId());
    }

    @Override
    protected PlanNode pushDown(Pushdown pushdown, GroupIdNode node, Expression inheritedPredicate)
    {
        Map<Symbol, Reference> commonGroupingSymbolMapping = node.getGroupingColumns().entrySet().stream()
                .filter(entry -> node.getCommonGroupingColumns().contains(entry.getKey()))
                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().toSymbolReference()));

        Predicate<Expression> pushdownEligiblePredicate = conjunct -> commonGroupingSymbolMapping.keySet().containsAll(extractUnique(conjunct));

        Map<Boolean, List<Expression>> conjuncts = extractConjuncts(inheritedPredicate).stream().collect(Collectors.partitioningBy(pushdownEligiblePredicate));

        // Push down conjuncts from the inherited predicate that apply to common grouping symbols
        PlanNode rewrittenNode = pushdown.filterChildren(node, inlineSymbols(commonGroupingSymbolMapping, combineConjuncts(conjuncts.get(true))));

        // All other conjuncts, if any, will be in the filter node.
        if (!conjuncts.get(false).isEmpty()) {
            rewrittenNode = new FilterNode(pushdown.idAllocator.getNextId(), rewrittenNode, combineConjuncts(conjuncts.get(false)));
        }

        return rewrittenNode;
    }
}
