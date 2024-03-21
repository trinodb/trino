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
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.plan.PlanNode;

import static java.util.Objects.requireNonNull;

public interface StatsCalculator
{
    /**
     * Calculate stats for the {@code node}.
     *
     * @param node The node to compute stats for.
     * @param context The context required to calculate stats.
     */
    PlanNodeStatsEstimate calculateStats(PlanNode node, Context context);

    static StatsCalculator noopStatsCalculator()
    {
        return (node, context) -> PlanNodeStatsEstimate.unknown();
    }

    record Context(
            StatsProvider statsProvider,
            Lookup lookup,
            Session session,
            TableStatsProvider tableStatsProvider,
            RuntimeInfoProvider runtimeInfoProvider)
    {
        public Context
        {
            requireNonNull(statsProvider, "statsProvider is null");
            requireNonNull(lookup, "lookup is null");
            requireNonNull(session, "session is null");
            requireNonNull(tableStatsProvider, "tableStatsProvider is null");
            requireNonNull(runtimeInfoProvider, "runtimeInfoProvider is null");
        }
    }
}
