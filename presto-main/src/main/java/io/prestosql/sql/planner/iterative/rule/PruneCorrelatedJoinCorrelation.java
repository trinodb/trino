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
import io.prestosql.sql.planner.plan.CorrelatedJoinNode;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.sql.planner.SymbolsExtractor.extractUnique;
import static io.prestosql.sql.planner.plan.Patterns.correlatedJoin;

/**
 * This rule updates CorrelatedJoinNode's correlation list.
 * A symbol can be removed from the correlation list if it is not referenced by the subquery node.
 * Note: This rule does not restrict CorrelatedJoinNode's children outputs. It requires additional information
 * about context (symbols required by the outer plan) and is done in PruneCorrelatedJoinColumns rule.
 */

public class PruneCorrelatedJoinCorrelation
        implements Rule<CorrelatedJoinNode>
{
    private static final Pattern<CorrelatedJoinNode> PATTERN = correlatedJoin();

    @Override
    public Pattern<CorrelatedJoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(CorrelatedJoinNode correlatedJoinNode, Captures captures, Context context)
    {
        Set<Symbol> subquerySymbols = extractUnique(correlatedJoinNode.getSubquery(), context.getLookup());
        List<Symbol> newCorrelation = correlatedJoinNode.getCorrelation().stream()
                .filter(subquerySymbols::contains)
                .collect(toImmutableList());

        if (newCorrelation.size() < correlatedJoinNode.getCorrelation().size()) {
            return Result.ofPlanNode(new CorrelatedJoinNode(
                    correlatedJoinNode.getId(),
                    correlatedJoinNode.getInput(),
                    correlatedJoinNode.getSubquery(),
                    newCorrelation,
                    correlatedJoinNode.getType(),
                    correlatedJoinNode.getFilter(),
                    correlatedJoinNode.getOriginSubquery()));
        }

        return Result.empty();
    }
}
