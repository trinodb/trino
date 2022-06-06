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
import io.trino.cost.ComposableStatsCalculator.Rule;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.plan.PlanNode;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public abstract class SimpleStatsRule<T extends PlanNode>
        implements Rule<T>
{
    private final StatsNormalizer normalizer;

    protected SimpleStatsRule(StatsNormalizer normalizer)
    {
        this.normalizer = requireNonNull(normalizer, "normalizer is null");
    }

    @Override
    public final Optional<PlanNodeStatsEstimate> calculate(T node, StatsProvider sourceStats, Lookup lookup, Session session, TypeProvider types, TableStatsProvider tableStatsProvider)
    {
        return doCalculate(node, sourceStats, lookup, session, types, tableStatsProvider)
                .map(estimate -> normalizer.normalize(estimate, node.getOutputSymbols(), types));
    }

    protected abstract Optional<PlanNodeStatsEstimate> doCalculate(T node, StatsProvider sourceStats, Lookup lookup, Session session, TypeProvider types, TableStatsProvider tableStatsProvider);
}
