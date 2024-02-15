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

import io.trino.matching.Pattern;
import io.trino.sql.planner.plan.ChooseAlternativeNode;

import java.util.Optional;

import static io.trino.sql.planner.plan.Patterns.chooseAlternative;

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
    protected Optional<PlanNodeStatsEstimate> doCalculate(ChooseAlternativeNode node, StatsCalculator.Context context)
    {
        // All alternatives describe the same dataset, therefore it would be wasteful to calculate stats for each alternative.
        // Instead, we calculate only for the first alternative which is the most pessimistic and therefore probably contains
        // the broadest filter (so it's most informative)
        return Optional.of(context.statsProvider().getStats(node.getSources().get(0)));
    }
}
