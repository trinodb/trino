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

import com.google.common.collect.ImmutableSet;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.SymbolsExtractor;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.SpatialJoinNode;

import java.util.Set;

import static io.prestosql.sql.planner.iterative.rule.Util.restrictChildOutputs;
import static io.prestosql.sql.planner.plan.Patterns.spatialJoin;

public class PruneSpatialJoinChildrenColumns
        implements Rule<SpatialJoinNode>
{
    private static final Pattern<SpatialJoinNode> PATTERN = spatialJoin();

    @Override
    public Pattern<SpatialJoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(SpatialJoinNode spatialJoinNode, Captures captures, Context context)
    {
        Set<Symbol> requiredOutputAndFilterSymbols = ImmutableSet.<Symbol>builder()
                .addAll(spatialJoinNode.getOutputSymbols())
                .addAll(SymbolsExtractor.extractUnique(spatialJoinNode.getFilter()))
                .build();

        ImmutableSet.Builder<Symbol> leftInputs = ImmutableSet.<Symbol>builder()
                .addAll(requiredOutputAndFilterSymbols);
        spatialJoinNode.getLeftPartitionSymbol()
                .ifPresent(leftInputs::add);

        ImmutableSet.Builder<Symbol> rightInputs = ImmutableSet.<Symbol>builder()
                .addAll(requiredOutputAndFilterSymbols);
        spatialJoinNode.getRightPartitionSymbol()
                .ifPresent(rightInputs::add);

        return restrictChildOutputs(context.getIdAllocator(), spatialJoinNode, leftInputs.build(), rightInputs.build())
                .map(Result::ofPlanNode)
                .orElse(Result.empty());
    }
}
