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
package io.trino.sql.planner.iterative;

import io.trino.Session;
import io.trino.cost.CostProvider;
import io.trino.cost.StatsProvider;
import io.trino.execution.warnings.WarningCollector;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.plan.PlanNode;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public interface Rule<T>
{
    /**
     * Returns a pattern to which plan nodes this rule applies.
     */
    Pattern<T> getPattern();

    default boolean isEnabled(Session session)
    {
        return true;
    }

    Result apply(T node, Captures captures, Context context);

    interface Context
    {
        Lookup getLookup();

        PlanNodeIdAllocator getIdAllocator();

        SymbolAllocator getSymbolAllocator();

        Session getSession();

        StatsProvider getStatsProvider();

        CostProvider getCostProvider();

        void checkTimeoutNotExhausted();

        WarningCollector getWarningCollector();
    }

    record Result(Optional<PlanNode> transformedPlan)
    {
        private static final Result EMPTY = new Result(Optional.empty());

        public static Result empty()
        {
            return EMPTY;
        }

        public static Result ofPlanNode(PlanNode transformedPlan)
        {
            return new Result(Optional.of(transformedPlan));
        }

        public Result
        {
            requireNonNull(transformedPlan, "transformedPlan is null");
        }

        public boolean isEmpty()
        {
            return transformedPlan.isEmpty();
        }
    }
}
