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
import io.trino.Session;
import io.trino.matching.Pattern;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.iterative.Lookup;
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
    protected Optional<PlanNodeStatsEstimate> doCalculate(DistinctLimitNode node, StatsProvider statsProvider, Lookup lookup, Session session, TypeProvider types)
    {
        if (node.isPartial()) {
            return Optional.empty();
        }

        PlanNodeStatsEstimate distinctStats = AggregationStatsRule.groupBy(
                statsProvider.getStats(node.getSource()),
                node.getDistinctSymbols(),
                ImmutableMap.of());
        PlanNodeStatsEstimate distinctLimitStats = distinctStats.mapOutputRowCount(rowCount -> min(rowCount, node.getLimit()));
        return Optional.of(distinctLimitStats);
    }
}
