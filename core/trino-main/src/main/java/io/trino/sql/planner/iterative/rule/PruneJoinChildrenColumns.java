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
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.JoinNode;

import java.util.Set;

import static io.trino.sql.planner.iterative.rule.Util.restrictChildOutputs;
import static io.trino.sql.planner.plan.Patterns.join;

/**
 * Joins support output symbol selection, so make any project-off of child columns explicit in project nodes.
 */
public class PruneJoinChildrenColumns
        implements Rule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = join();

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(JoinNode joinNode, Captures captures, Context context)
    {
        Set<Symbol> globallyUsableInputs = ImmutableSet.<Symbol>builder()
                .addAll(joinNode.getOutputSymbols())
                .addAll(
                        joinNode.getFilter()
                                .map(SymbolsExtractor::extractUnique)
                                .orElse(ImmutableSet.of()))
                .build();

        Set<Symbol> leftUsableInputs = ImmutableSet.<Symbol>builder()
                .addAll(globallyUsableInputs)
                .addAll(
                        joinNode.getCriteria().stream()
                                .map(JoinNode.EquiJoinClause::getLeft)
                                .iterator())
                .build();

        Set<Symbol> rightUsableInputs = ImmutableSet.<Symbol>builder()
                .addAll(globallyUsableInputs)
                .addAll(
                        joinNode.getCriteria().stream()
                                .map(JoinNode.EquiJoinClause::getRight)
                                .iterator())
                .build();

        return restrictChildOutputs(context.getIdAllocator(), joinNode, leftUsableInputs, rightUsableInputs)
                .map(Result::ofPlanNode)
                .orElse(Result.empty());
    }
}
