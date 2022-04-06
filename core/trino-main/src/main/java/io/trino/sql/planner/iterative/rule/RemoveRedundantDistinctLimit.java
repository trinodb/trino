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
import com.google.common.collect.ImmutableMap;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.DistinctLimitNode;
import io.trino.sql.planner.plan.ValuesNode;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.sql.planner.optimizations.QueryCardinalityUtil.isAtMost;
import static io.trino.sql.planner.optimizations.QueryCardinalityUtil.isScalar;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.trino.sql.planner.plan.Patterns.distinctLimit;

/**
 * Replace DistinctLimit node
 * 1. With a empty ValuesNode when count is 0
 * 2. With a Distinct node when the subplan is guaranteed to produce fewer rows than count
 * 3. With its source when the subplan produces only one row
 */
public class RemoveRedundantDistinctLimit
        implements Rule<DistinctLimitNode>
{
    private static final Pattern<DistinctLimitNode> PATTERN = distinctLimit();

    @Override
    public Pattern<DistinctLimitNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(DistinctLimitNode node, Captures captures, Context context)
    {
        checkArgument(node.getHashSymbol().isEmpty(), "HashSymbol should be empty");
        if (node.getLimit() == 0) {
            return Result.ofPlanNode(new ValuesNode(node.getId(), node.getOutputSymbols(), ImmutableList.of()));
        }
        if (isScalar(node.getSource(), context.getLookup())) {
            return Result.ofPlanNode(node.getSource());
        }
        if (isAtMost(node.getSource(), context.getLookup(), node.getLimit())) {
            return Result.ofPlanNode(new AggregationNode(
                    node.getId(),
                    node.getSource(),
                    ImmutableMap.of(),
                    singleGroupingSet(node.getDistinctSymbols()),
                    ImmutableList.of(),
                    SINGLE,
                    node.getHashSymbol(),
                    Optional.empty(),
                    Optional.empty()));
        }
        return Result.empty();
    }
}
