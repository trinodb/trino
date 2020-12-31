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

import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.ApplyNode;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.sql.planner.SymbolsExtractor.extractUnique;
import static io.prestosql.sql.planner.plan.Patterns.applyNode;

/**
 * This rule updates ApplyNode's correlation list.
 * A symbol can be removed from the correlation list if it is not referenced by the subquery node.
 * Note: This rule does not restrict ApplyNode's children outputs. It requires additional information
 * about context (symbols required by the outer plan) and is done in PruneApplyColumns rule.
 */
public class PruneApplyCorrelation
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
        Set<Symbol> subquerySymbols = extractUnique(applyNode.getSubquery(), context.getLookup());
        List<Symbol> newCorrelation = applyNode.getCorrelation().stream()
                .filter(subquerySymbols::contains)
                .collect(toImmutableList());

        if (newCorrelation.size() < applyNode.getCorrelation().size()) {
            return Result.ofPlanNode(new ApplyNode(
                    applyNode.getId(),
                    applyNode.getInput(),
                    applyNode.getSubquery(),
                    applyNode.getSubqueryAssignments(),
                    newCorrelation,
                    applyNode.getOriginSubquery()));
        }

        return Result.empty();
    }
}
