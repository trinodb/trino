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
import io.prestosql.sql.planner.plan.EnforceSingleRowNode;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.prestosql.sql.planner.plan.Patterns.enforceSingleRow;
import static java.lang.Double.min;
import static java.util.Map.Entry;

public class EnforceSingleRowStatsRule
        extends SimpleStatsRule<EnforceSingleRowNode>
{
    private static final Pattern<EnforceSingleRowNode> PATTERN = enforceSingleRow();

    public EnforceSingleRowStatsRule(StatsNormalizer normalizer)
    {
        super(normalizer);
    }

    @Override
    public Pattern<EnforceSingleRowNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    protected Optional<PlanNodeStatsEstimate> doCalculate(EnforceSingleRowNode node, StatsProvider sourceStats, Lookup lookup, Session session, TypeProvider types)
    {
        Map<Symbol, SymbolStatsEstimate> symbolStatsEstimate = sourceStats.getStats(node.getSource()).getSymbolStatistics()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().mapDistinctValuesCount(distinctValueCount -> min(distinctValueCount, 1))));
        return Optional.of(new PlanNodeStatsEstimate(1, symbolStatsEstimate));
    }
}
