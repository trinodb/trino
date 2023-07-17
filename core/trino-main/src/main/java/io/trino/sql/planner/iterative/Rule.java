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

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.cost.CostProvider;
import io.trino.cost.StatsProvider;
import io.trino.execution.warnings.WarningCollector;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.plan.PlanNode;

import java.util.List;
import java.util.Optional;

import static java.util.Collections.emptyList;
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

    final class Result
    {
        private static final Result EMPTY = new Result(Optional.empty(), emptyList());

        public static Result empty()
        {
            return EMPTY;
        }

        /**
         * @param mainAlternative an alternative that won't be pruned by the engine, one that can be used for all splits,
         * or empty if the original {@code PlanNode} is to be retained.
         * @param additionalAlternatives the rest of the alternatives or an empty list if there are no more alternatives.
         * The engine might prune elements off the end of the list or even ignore it completely.
         */
        public static Result ofNodeAlternatives(Optional<PlanNode> mainAlternative, Iterable<PlanNode> additionalAlternatives)
        {
            return new Result(mainAlternative, ImmutableList.copyOf(additionalAlternatives));
        }

        public static Result ofPlanNode(PlanNode transformedPlan)
        {
            return new Result(Optional.of(transformedPlan), emptyList());
        }

        private final Optional<PlanNode> mainAlternative;
        private final List<PlanNode> additionalAlternatives;

        private Result(Optional<PlanNode> mainAlternative, List<PlanNode> additionalAlternatives)
        {
            this.mainAlternative = requireNonNull(mainAlternative, "mainAlternative is null");
            this.additionalAlternatives = ImmutableList.copyOf(requireNonNull(additionalAlternatives, "additionalAlternatives is null"));
        }

        public Optional<PlanNode> getMainAlternative()
        {
            return mainAlternative;
        }

        public List<PlanNode> getAdditionalAlternatives()
        {
            return additionalAlternatives;
        }

        public boolean isEmpty()
        {
            return mainAlternative.isEmpty() && additionalAlternatives.isEmpty();
        }
    }
}
