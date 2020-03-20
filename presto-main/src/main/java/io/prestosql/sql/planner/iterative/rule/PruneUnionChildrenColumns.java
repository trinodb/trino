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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.UnionNode;

import java.util.Collection;
import java.util.Set;

import static io.prestosql.sql.planner.iterative.rule.Util.restrictChildOutputs;
import static io.prestosql.sql.planner.plan.Patterns.union;

public class PruneUnionChildrenColumns
        implements Rule<UnionNode>
{
    private static final Pattern<UnionNode> PATTERN = union();

    @Override
    public Pattern<UnionNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(UnionNode unionNode, Captures captures, Context context)
    {
        ImmutableList.Builder<Set<Symbol>> expectedOutputs = ImmutableList.builder();
        for (int i = 0; i < unionNode.getSources().size(); i++) {
            ImmutableSet.Builder<Symbol> expectedInputSymbols = ImmutableSet.builder();
            for (Collection<Symbol> symbols : unionNode.getSymbolMapping().asMap().values()) {
                expectedInputSymbols.add(Iterables.get(symbols, i));
            }
            expectedOutputs.add(expectedInputSymbols.build());
        }
        return restrictChildOutputs(context.getIdAllocator(), unionNode, expectedOutputs.build())
                .map(Result::ofPlanNode)
                .orElse(Result.empty());
    }
}
