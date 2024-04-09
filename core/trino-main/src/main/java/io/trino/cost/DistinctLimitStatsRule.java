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
package io.trino.cost;

import com.google.common.collect.ImmutableMap;
import io.trino.cost.StatsCalculator.Context;
import io.trino.matching.Pattern;
import io.trino.sql.planner.plan.DistinctLimitNode;

import java.util.Optional;

import static io.trino.sql.planner.plan.Patterns.distinctLimit;
import static java.lang.Math.min;

public class DistinctLimitStatsRule
        extends SimpleStatsRule<DistinctLimitNode>
{
    private static final Pattern<DistinctLimitNode> PATTERN = distinctLimit();

    public DistinctLimitStatsRule(StatsNormalizer normalizer)
    {
        super(normalizer);
    }

    @Override
    public Pattern<DistinctLimitNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    protected Optional<PlanNodeStatsEstimate> doCalculate(DistinctLimitNode node, Context context)
    {
        if (node.isPartial()) {
            return Optional.empty();
        }

        PlanNodeStatsEstimate distinctStats = AggregationStatsRule.groupBy(
                context.statsProvider().getStats(node.getSource()),
                node.getDistinctSymbols(),
                ImmutableMap.of());
        PlanNodeStatsEstimate distinctLimitStats = distinctStats.mapOutputRowCount(rowCount -> min(rowCount, node.getLimit()));
        return Optional.of(distinctLimitStats);
    }
}
