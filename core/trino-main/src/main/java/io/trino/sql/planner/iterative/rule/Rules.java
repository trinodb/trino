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
package io.trino.sql.planner.iterative.rule;

import io.trino.Session;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.StatsProvider;
import io.trino.sql.planner.plan.PlanNode;

import java.util.Optional;

import static io.trino.SystemSessionProperties.isStatisticsPrecalculationForPushdownEnabled;

public final class Rules
{
    private Rules() {}

    public static Optional<PlanNodeStatsEstimate> deriveTableStatisticsForPushdown(
            StatsProvider statsProvider,
            Session session,
            boolean precalculateStatistics,
            PlanNode oldTableScanParent)
    {
        if (!precalculateStatistics || !isStatisticsPrecalculationForPushdownEnabled(session)) {
            return Optional.empty();
        }
        PlanNodeStatsEstimate statistics = statsProvider.getStats(oldTableScanParent);
        return Optional.of(statistics);
    }
}
