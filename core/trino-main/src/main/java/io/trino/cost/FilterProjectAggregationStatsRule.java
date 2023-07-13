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
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;

import java.util.Optional;

import static io.trino.SystemSessionProperties.isNonEstimatablePredicateApproximationEnabled;
import static io.trino.cost.FilterStatsCalculator.UNKNOWN_FILTER_COEFFICIENT;
import static io.trino.sql.planner.plan.Patterns.filter;
import static java.util.Objects.requireNonNull;

/**
 * AggregationStatsRule does not have sufficient information to provide accurate estimates on aggregated symbols.
 * This rule finds unestimated filters on top of aggregations whose output row count was estimated and returns
 * estimated row count of (UNKNOWN_FILTER_COEFFICIENT * aggregation output row count) for the filter.
 */
public class FilterProjectAggregationStatsRule
        extends SimpleStatsRule<FilterNode>
{
    private static final Pattern<FilterNode> PATTERN = filter();

    private final FilterStatsCalculator filterStatsCalculator;

    public FilterProjectAggregationStatsRule(StatsNormalizer normalizer, FilterStatsCalculator filterStatsCalculator)
    {
        super(normalizer);
        this.filterStatsCalculator = requireNonNull(filterStatsCalculator, "filterStatsCalculator cannot be null");
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    protected Optional<PlanNodeStatsEstimate> doCalculate(FilterNode node, StatsProvider sourceStats, Lookup lookup, Session session, TypeProvider types, TableStatsProvider tableStatsProvider)
    {
        if (!isNonEstimatablePredicateApproximationEnabled(session)) {
            return Optional.empty();
        }
        PlanNode nodeSource = lookup.resolve(node.getSource());
        AggregationNode aggregationNode;
        // TODO match the required source nodes through separate patterns when
        //  ComposableStatsCalculator allows patterns other than TypeOfPattern
        if (nodeSource instanceof ProjectNode projectNode) {
            if (!projectNode.isIdentity()) {
                return Optional.empty();
            }
            PlanNode projectNodeSource = lookup.resolve(projectNode.getSource());
            if (!(projectNodeSource instanceof AggregationNode)) {
                return Optional.empty();
            }
            aggregationNode = (AggregationNode) projectNodeSource;
        }
        else if (nodeSource instanceof AggregationNode) {
            aggregationNode = (AggregationNode) nodeSource;
        }
        else {
            return Optional.empty();
        }

        return calculate(node, aggregationNode, sourceStats, session, types);
    }

    private Optional<PlanNodeStatsEstimate> calculate(FilterNode filterNode, AggregationNode aggregationNode, StatsProvider statsProvider, Session session, TypeProvider types)
    {
        // We assume here that due to predicate pushdown all the filters left are on the aggregation result
        PlanNodeStatsEstimate filteredStats = filterStatsCalculator.filterStats(statsProvider.getStats(filterNode.getSource()), filterNode.getPredicate(), session, types);
        if (filteredStats.isOutputRowCountUnknown()) {
            PlanNodeStatsEstimate sourceStats = statsProvider.getStats(aggregationNode);
            if (sourceStats.isOutputRowCountUnknown()) {
                return Optional.of(filteredStats);
            }
            return Optional.of(sourceStats.mapOutputRowCount(rowCount -> rowCount * UNKNOWN_FILTER_COEFFICIENT));
        }
        return Optional.of(filteredStats);
    }
}
