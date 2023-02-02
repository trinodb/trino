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
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import static io.trino.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class AggregationStatsRule
        extends SimpleStatsRule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = aggregation();

    public AggregationStatsRule(StatsNormalizer normalizer)
    {
        super(normalizer);
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    protected Optional<PlanNodeStatsEstimate> doCalculate(AggregationNode node, StatsProvider statsProvider, Lookup lookup, Session session, TypeProvider types, TableStatsProvider tableStatsProvider)
    {
        if (node.getGroupingSetCount() != 1) {
            return Optional.empty();
        }

        if (node.getStep() != SINGLE && node.getStep() != FINAL) {
            return Optional.empty();
        }

        return Optional.of(groupBy(
                statsProvider.getStats(node.getSource()),
                node.getGroupingKeys(),
                node.getAggregations()));
    }

    public static PlanNodeStatsEstimate groupBy(PlanNodeStatsEstimate sourceStats, Collection<Symbol> groupBySymbols, Map<Symbol, Aggregation> aggregations)
    {
        PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.builder();
        if (groupBySymbols.isEmpty()) {
            result.setOutputRowCount(1);
        }
        else {
            for (Symbol groupBySymbol : groupBySymbols) {
                SymbolStatsEstimate symbolStatistics = sourceStats.getSymbolStatistics(groupBySymbol);
                result.addSymbolStatistics(groupBySymbol, symbolStatistics.mapNullsFraction(nullsFraction -> {
                    if (nullsFraction == 0.0) {
                        return 0.0;
                    }
                    return 1.0 / (symbolStatistics.getDistinctValuesCount() + 1);
                }));
            }
            double rowsCount = getRowsCount(sourceStats, groupBySymbols);
            result.setOutputRowCount(min(rowsCount, sourceStats.getOutputRowCount()));
        }
        for (Map.Entry<Symbol, Aggregation> aggregationEntry : aggregations.entrySet()) {
            result.addSymbolStatistics(aggregationEntry.getKey(), estimateAggregationStats(aggregationEntry.getValue(), sourceStats));
        }

        return result.build();
    }

    public static double getRowsCount(PlanNodeStatsEstimate sourceStats, Collection<Symbol> groupBySymbols)
    {
        double rowsCount = 1;
        for (Symbol groupBySymbol : groupBySymbols) {
            SymbolStatsEstimate symbolStatistics = sourceStats.getSymbolStatistics(groupBySymbol);
            int nullRow = (symbolStatistics.getNullsFraction() == 0.0) ? 0 : 1;
            rowsCount *= symbolStatistics.getDistinctValuesCount() + nullRow;
        }
        return rowsCount;
    }

    private static SymbolStatsEstimate estimateAggregationStats(Aggregation aggregation, PlanNodeStatsEstimate sourceStats)
    {
        requireNonNull(aggregation, "aggregation is null");
        requireNonNull(sourceStats, "sourceStats is null");

        // TODO implement simple aggregations like: min, max, count, sum
        return SymbolStatsEstimate.unknown();
    }
}
