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

import io.trino.cost.ComposableStatsCalculator.Rule;
import io.trino.cost.StatsCalculator.Context;
import io.trino.matching.Pattern;
import io.trino.sql.planner.plan.SemiJoinNode;

import java.util.Optional;

import static io.trino.sql.planner.plan.Patterns.semiJoin;

public class SemiJoinStatsRule
        implements Rule<SemiJoinNode>
{
    private static final Pattern<SemiJoinNode> PATTERN = semiJoin();

    @Override
    public Pattern<SemiJoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<PlanNodeStatsEstimate> calculate(SemiJoinNode node, Context context)
    {
        PlanNodeStatsEstimate sourceStats = context.statsProvider().getStats(node.getSource());

        // For now we just propagate statistics for source symbols.
        // Handling semiJoinOutput symbols requires support for correlation for boolean columns.

        return Optional.of(sourceStats);
    }
}
