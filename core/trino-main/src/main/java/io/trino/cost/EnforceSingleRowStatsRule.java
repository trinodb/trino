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
import io.trino.sql.planner.plan.EnforceSingleRowNode;

import java.util.Optional;

import static io.trino.sql.planner.plan.Patterns.enforceSingleRow;

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
    protected Optional<PlanNodeStatsEstimate> doCalculate(EnforceSingleRowNode node, StatsProvider sourceStats, Lookup lookup, Session session, TypeProvider types, TableStatsProvider tableStatsProvider)
    {
        return Optional.of(PlanNodeStatsEstimate.buildFrom(sourceStats.getStats(node.getSource()))
                .setOutputRowCount(1)
                .build());
    }
}
