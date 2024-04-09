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
package io.trino.sql.planner.optimizations;

import io.trino.Session;
import io.trino.cost.RuntimeInfoProvider;
import io.trino.cost.TableStatsProvider;
import io.trino.execution.querystats.PlanOptimizersStatsCollector;
import io.trino.execution.warnings.WarningCollector;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.plan.PlanNode;

import static java.util.Objects.requireNonNull;

public interface PlanOptimizer
{
    PlanNode optimize(PlanNode plan, Context context);

    record Context(
            Session session,
            SymbolAllocator symbolAllocator,
            PlanNodeIdAllocator idAllocator,
            WarningCollector warningCollector,
            PlanOptimizersStatsCollector planOptimizersStatsCollector,
            TableStatsProvider tableStatsProvider,
            RuntimeInfoProvider runtimeInfoProvider)
    {
        public Context(
                Session session,
                SymbolAllocator symbolAllocator,
                PlanNodeIdAllocator idAllocator,
                WarningCollector warningCollector,
                PlanOptimizersStatsCollector planOptimizersStatsCollector,
                TableStatsProvider tableStatsProvider,
                RuntimeInfoProvider runtimeInfoProvider)
        {
            this.session = requireNonNull(session, "session is null");
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
            this.tableStatsProvider = requireNonNull(tableStatsProvider, "tableStatsProvider is null");
            this.planOptimizersStatsCollector = requireNonNull(planOptimizersStatsCollector, "planOptimizersStatsCollector is null");
            this.runtimeInfoProvider = requireNonNull(runtimeInfoProvider, "runtimeInfoProvider is null");
        }
    }
}
