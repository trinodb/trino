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

import io.trino.Session;
import io.trino.matching.Pattern;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.plan.FilterNode;

import java.util.Optional;

import static io.trino.SystemSessionProperties.isDefaultFilterFactorEnabled;
import static io.trino.cost.FilterStatsCalculator.UNKNOWN_FILTER_COEFFICIENT;
import static io.trino.sql.planner.plan.Patterns.filter;

public class FilterStatsRule
        extends SimpleStatsRule<FilterNode>
{
    private static final Pattern<FilterNode> PATTERN = filter();

    private final FilterStatsCalculator filterStatsCalculator;

    public FilterStatsRule(StatsNormalizer normalizer, FilterStatsCalculator filterStatsCalculator)
    {
        super(normalizer);
        this.filterStatsCalculator = filterStatsCalculator;
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<PlanNodeStatsEstimate> doCalculate(FilterNode node, StatsProvider statsProvider, Lookup lookup, Session session, TypeProvider types, TableStatsProvider tableStatsProvider)
    {
        PlanNodeStatsEstimate sourceStats = statsProvider.getStats(node.getSource());
        PlanNodeStatsEstimate estimate = filterStatsCalculator.filterStats(sourceStats, node.getPredicate(), session, types);
        if (isDefaultFilterFactorEnabled(session) && estimate.isOutputRowCountUnknown()) {
            estimate = sourceStats.mapOutputRowCount(sourceRowCount -> sourceStats.getOutputRowCount() * UNKNOWN_FILTER_COEFFICIENT);
        }
        return Optional.of(estimate);
    }
}
