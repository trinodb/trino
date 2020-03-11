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
package io.prestosql.cost;

import io.prestosql.Session;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.planner.plan.LimitNode;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.prestosql.sql.planner.plan.Patterns.limit;
import static java.lang.Double.min;
import static java.util.Map.Entry;

public class LimitStatsRule
        extends SimpleStatsRule<LimitNode>
{
    private static final Pattern<LimitNode> PATTERN = limit();

    public LimitStatsRule(StatsNormalizer normalizer)
    {
        super(normalizer);
    }

    @Override
    public Pattern<LimitNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    protected Optional<PlanNodeStatsEstimate> doCalculate(LimitNode node, StatsProvider statsProvider, Lookup lookup, Session session, TypeProvider types)
    {
        PlanNodeStatsEstimate sourceStats = statsProvider.getStats(node.getSource());
        if (sourceStats.getOutputRowCount() <= node.getCount()) {
            return Optional.of(sourceStats);
        }

        // LIMIT actually limits (or when there was no row count estimated for source)
        Map<Symbol, SymbolStatsEstimate> symbolStatsEstimate = statsProvider.getStats(node.getSource()).getSymbolStatistics()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().mapDistinctValuesCount(distinctValueCount -> min(distinctValueCount, node.getCount()))));
        return Optional.of(new PlanNodeStatsEstimate(node.getCount(), symbolStatsEstimate));
    }
}
