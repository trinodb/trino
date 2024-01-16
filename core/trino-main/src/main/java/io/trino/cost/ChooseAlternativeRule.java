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
import io.trino.sql.planner.plan.ChooseAlternativeNode;
import io.trino.sql.planner.plan.PlanNode;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.trino.sql.planner.plan.Patterns.chooseAlternative;
import static io.trino.util.MoreMath.maxExcludeNaN;
import static io.trino.util.MoreMath.minExcludeNaN;
import static java.lang.Double.NaN;

public class ChooseAlternativeRule
        extends SimpleStatsRule<ChooseAlternativeNode>
{
    private static final Pattern<ChooseAlternativeNode> PATTERN = chooseAlternative();

    public ChooseAlternativeRule(StatsNormalizer normalizer)
    {
        super(normalizer);
    }

    @Override
    public Pattern<ChooseAlternativeNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    protected Optional<PlanNodeStatsEstimate> doCalculate(ChooseAlternativeNode node, StatsProvider statsProvider, Lookup lookup, Session session, TypeProvider types, TableStatsProvider tableStatsProvider)
    {
        // For each symbol, peek up the narrowest estimate out of the alternatives.
        // That's because all the alternatives describe the same dataset,
        // so the narrowest stats probably represent the best knowledge about the data.
        double lowestOutputRowCount = NaN;
        Map<Symbol, SymbolStatsEstimate> narrowestSymbolStatistics = new HashMap<>();
        for (PlanNode alternative : node.getSources()) {
            PlanNodeStatsEstimate alternativeStats = statsProvider.getStats(alternative);
            lowestOutputRowCount = minExcludeNaN(lowestOutputRowCount, alternativeStats.getOutputRowCount());
            for (Map.Entry<Symbol, SymbolStatsEstimate> stats : alternativeStats.getSymbolStatistics().entrySet()) {
                if (narrowestSymbolStatistics.containsKey(stats.getKey())) {
                    SymbolStatsEstimate currentEstimates = narrowestSymbolStatistics.get(stats.getKey());
                    SymbolStatsEstimate alternativeEstimates = stats.getValue();
                    double lowValue = maxExcludeNaN(currentEstimates.getLowValue(), alternativeEstimates.getLowValue());
                    double highValue = minExcludeNaN(currentEstimates.getHighValue(), alternativeEstimates.getHighValue());
                    double nullsFraction = minExcludeNaN(currentEstimates.getNullsFraction(), alternativeEstimates.getNullsFraction());
                    double averageRowSize = minExcludeNaN(currentEstimates.getAverageRowSize(), alternativeEstimates.getAverageRowSize());
                    double distinctValuesCount = minExcludeNaN(currentEstimates.getDistinctValuesCount(), alternativeEstimates.getDistinctValuesCount());
                    narrowestSymbolStatistics.put(stats.getKey(), new SymbolStatsEstimate(lowValue, highValue, nullsFraction, averageRowSize, distinctValuesCount));
                }
                else {
                    narrowestSymbolStatistics.put(stats.getKey(), stats.getValue());
                }
            }
        }
        return Optional.of(new PlanNodeStatsEstimate(lowestOutputRowCount, narrowestSymbolStatistics));
    }
}
