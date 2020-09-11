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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.SymbolsExtractor;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.ApplyNode;
import io.prestosql.sql.planner.plan.PlanNode;

import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.sql.planner.iterative.rule.Util.restrictOutputs;
import static io.prestosql.sql.planner.plan.Patterns.applyNode;

/**
 * This rule restricts outputs of ApplyNode's subquery to include only the symbols
 * needed for subqueryAssignments. Symbols from the subquery are not produced at
 * ApplyNode's output. They are only used for the assignments.
 * Transforms:
 * <pre>
 * - Apply
 *      correlation: [corr_symbol]
 *      assignments:
 *          result_1 -> a in subquery_symbol_1,
 *          result_2 -> b > ALL subquery_symbol_2
 *    - Input (a, b, corr_symbol)
 *    - Subquery (subquery_symbol_1, subquery_symbol_2, subquery_symbol_3)
 * </pre>
 * Into:
 * <pre>
 * - Apply
 *      correlation: [corr_symbol]
 *      assignments:
 *          result_1 -> a in subquery_symbol_1,
 *          result_2 -> b > ALL subquery_symbol_2
 *    - Input (a, b, corr_symbol)
 *    - Project
 *          subquery_symbol_1 -> subquery_symbol_1
 *          subquery_symbol_2 -> subquery_symbol_2
 *        - Subquery (subquery_symbol_1, subquery_symbol_2, subquery_symbol_3)
 * </pre>
 * Note: ApplyNode's input symbols are produced on ApplyNode's output.
 * They cannot be pruned without outer context.
 */
public class PruneApplySourceColumns
        implements Rule<ApplyNode>
{
    private static final Pattern<ApplyNode> PATTERN = applyNode();

    @Override
    public Pattern<ApplyNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(ApplyNode applyNode, Captures captures, Context context)
    {
        Set<Symbol> subqueryAssignmentsSymbols = applyNode.getSubqueryAssignments().getExpressions().stream()
                .flatMap(expression -> SymbolsExtractor.extractUnique(expression).stream())
                .collect(toImmutableSet());

        Optional<PlanNode> prunedSubquery = restrictOutputs(context.getIdAllocator(), applyNode.getSubquery(), subqueryAssignmentsSymbols);
        return prunedSubquery.map(subquery -> applyNode.replaceChildren(ImmutableList.of(applyNode.getInput(), subquery)))
                .map(Result::ofPlanNode)
                .orElse(Result.empty());
    }
}
