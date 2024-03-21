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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.cost.CostCalculator;
import io.trino.cost.CostCalculator.EstimatedExchanges;
import io.trino.cost.StatsCalculator;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.iterative.IterativeOptimizer;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.iterative.rule.PruneFilterColumns;
import io.trino.sql.planner.iterative.rule.PruneTableScanColumns;
import io.trino.sql.planner.iterative.rule.PushPredicateIntoTableScan;
import io.trino.sql.planner.iterative.rule.RemoveRedundantIdentityProjectionsInOrder;
import io.trino.sql.planner.optimizations.AdaptivePlanOptimizer;
import io.trino.sql.planner.optimizations.PlanOptimizer;

import java.util.List;

public class AlternativesOptimizers
        implements PlanOptimizersFactory
{
    private final List<PlanOptimizer> optimizers;

    @Inject
    public AlternativesOptimizers(
            PlannerContext plannerContext,
            StatsCalculator statsCalculator,
            @EstimatedExchanges CostCalculator costCalculatorWithEstimatedExchanges,
            RuleStatsRecorder ruleStats)
    {
        CostCalculator costCalculator = costCalculatorWithEstimatedExchanges;

        AlternativesOptimizer alternativesOptimizer = new AlternativesOptimizer(
                plannerContext,
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.<Rule<?>>builder()
                        .add(new PushPredicateIntoTableScan(plannerContext, false))
                        .build());

        IterativeOptimizer iterativeOptimizer = new IterativeOptimizer(
                plannerContext,
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.<Rule<?>>builder()
                        // more rules will be added in the future
                        .add(new PruneTableScanColumns(plannerContext.getMetadata()))
                        .add(new RemoveRedundantIdentityProjectionsInOrder())   // output symbols of all alternatives must be in the same order
                        .add(new PruneFilterColumns())
                        .build());

        optimizers = List.of(alternativesOptimizer, iterativeOptimizer);
    }

    @Override
    public List<PlanOptimizer> getPlanOptimizers()
    {
        return optimizers;
    }

    @Override
    public List<AdaptivePlanOptimizer> getAdaptivePlanOptimizers()
    {
        return ImmutableList.of();
    }
}
