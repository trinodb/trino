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

import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.matching.Pattern;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.plan.TopNRankingNode;

import java.util.Optional;

import static io.trino.SystemSessionProperties.isEnhanceUnknownStatsCalculation;
import static io.trino.sql.planner.plan.Patterns.topNRanking;

public class TopNRankingStatsRule
        extends SimpleStatsRule<TopNRankingNode>
{
    private static final Logger log = Logger.get(TopNRankingNode.class);

    private static final Pattern<TopNRankingNode> PATTERN = topNRanking();

    public TopNRankingStatsRule(StatsNormalizer normalizer)
    {
        super(normalizer);
    }

    @Override
    public Pattern<TopNRankingNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    protected Optional<PlanNodeStatsEstimate> doCalculate(TopNRankingNode node, StatsProvider statsProvider, Lookup lookup, Session session, TypeProvider types, TableStatsProvider tableStatsProvider)
    {
        if (isEnhanceUnknownStatsCalculation(session)) {
            return Optional.of(statsProvider.getStats(node.getSource()));
        }

        return Optional.of(PlanNodeStatsEstimate.unknown());
    }
}
