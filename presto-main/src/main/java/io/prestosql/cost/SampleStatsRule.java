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
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.planner.plan.SampleNode;

import java.util.Optional;

import static io.prestosql.sql.planner.plan.Patterns.sample;

public class SampleStatsRule
        extends SimpleStatsRule<SampleNode>
{
    private static final Pattern<SampleNode> PATTERN = sample();

    public SampleStatsRule(StatsNormalizer normalizer)
    {
        super(normalizer);
    }

    @Override
    public Pattern<SampleNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    protected Optional<PlanNodeStatsEstimate> doCalculate(SampleNode node, StatsProvider statsProvider, Lookup lookup, Session session, TypeProvider types)
    {
        PlanNodeStatsEstimate sourceStats = statsProvider.getStats(node.getSource());
        double estimatedOutput = sourceStats.getOutputRowCount() * node.getSampleRatio();

        return Optional.of(PlanNodeStatsEstimate.buildFrom(sourceStats)
                .setOutputRowCount(estimatedOutput)
                .build());
    }
}
