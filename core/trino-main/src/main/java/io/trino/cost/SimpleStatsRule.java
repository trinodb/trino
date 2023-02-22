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

import io.trino.cost.ComposableStatsCalculator.Rule;
import io.trino.cost.StatsCalculator.Context;
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
    public final Optional<PlanNodeStatsEstimate> calculate(T node, Context context)
    {
        return doCalculate(node, context)
                .map(estimate -> normalizer.normalize(estimate, node.getOutputSymbols(), context.types()));
    }

    protected abstract Optional<PlanNodeStatsEstimate> doCalculate(T node, Context context);
}
