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

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ValuesNode;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.SystemSessionProperties.LOW_CONFIDENCE_COST_MARGIN;
import static io.trino.cost.EstimateConfidence.HIGH;
import static io.trino.cost.EstimateConfidence.LOW;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCachingCostProvider
{
    @Test
    public void testCostPicksUpStatsConfidence()
    {
        PlanNode guessed = new ValuesNode(new PlanNodeId("guessed"), 1);
        PlanNode reported = new ValuesNode(new PlanNodeId("reported"), 1);

        StatsProvider statsProvider = new FixedStatsProvider(ImmutableMap.of(
                guessed, statsWithConfidence(LOW),
                reported, statsWithConfidence(HIGH)));
        CostCalculator costCalculator = new FixedCostCalculator(ImmutableMap.of(
                guessed, new PlanCostEstimate(100, 100, 100, 100),
                reported, new PlanCostEstimate(100, 100, 100, 100)));

        Session session = testSessionBuilder().build();
        CachingCostProvider costProvider = new CachingCostProvider(costCalculator, statsProvider, session);

        // the cost calculator returns HIGH confidence, but the provider degrades it to the confidence of the node's stats
        assertThat(costProvider.getCost(guessed).getConfidence()).isEqualTo(LOW);
        assertThat(costProvider.getCost(reported).getConfidence()).isEqualTo(HIGH);
    }

    @Test
    public void testLowConfidenceCostMarginFlipsChosenPlan()
    {
        PlanNode guessed = new ValuesNode(new PlanNodeId("guessed"), 1);
        PlanNode reported = new ValuesNode(new PlanNodeId("reported"), 1);

        StatsProvider statsProvider = new FixedStatsProvider(ImmutableMap.of(
                guessed, statsWithConfidence(LOW),
                reported, statsWithConfidence(HIGH)));
        // the plan resting on guessed stats looks slightly cheaper on paper
        CostCalculator costCalculator = new FixedCostCalculator(ImmutableMap.of(
                guessed, new PlanCostEstimate(100, 100, 100, 100),
                reported, new PlanCostEstimate(120, 120, 120, 120)));
        CostComparator costComparator = new CostComparator(1.0, 1.0, 1.0);

        Session noMargin = testSessionBuilder()
                .setSystemProperty(LOW_CONFIDENCE_COST_MARGIN, "1.0")
                .build();
        CachingCostProvider withoutMargin = new CachingCostProvider(costCalculator, statsProvider, noMargin);
        // without a margin the cheaper plan wins even though its cost rests on guessed stats
        assertThat(costComparator.compare(noMargin, withoutMargin.getCost(guessed), withoutMargin.getCost(reported)))
                .isLessThan(0);

        Session withMargin = testSessionBuilder()
                .setSystemProperty(LOW_CONFIDENCE_COST_MARGIN, "2.0")
                .build();
        CachingCostProvider withMarginProvider = new CachingCostProvider(costCalculator, statsProvider, withMargin);
        // a margin above 1 makes the guessed plan look dearer, so the reported plan is chosen instead
        assertThat(costComparator.compare(withMargin, withMarginProvider.getCost(guessed), withMarginProvider.getCost(reported)))
                .isGreaterThan(0);
    }

    private static PlanNodeStatsEstimate statsWithConfidence(EstimateConfidence confidence)
    {
        return PlanNodeStatsEstimate.builder()
                .setOutputRowCount(1)
                .setConfidence(confidence)
                .build();
    }

    private static final class FixedStatsProvider
            implements StatsProvider
    {
        private final Map<PlanNode, PlanNodeStatsEstimate> stats;

        private FixedStatsProvider(Map<PlanNode, PlanNodeStatsEstimate> stats)
        {
            this.stats = ImmutableMap.copyOf(requireNonNull(stats, "stats is null"));
        }

        @Override
        public PlanNodeStatsEstimate getStats(PlanNode node)
        {
            PlanNodeStatsEstimate estimate = stats.get(node);
            requireNonNull(estimate, "no stats for node");
            return estimate;
        }
    }

    private static final class FixedCostCalculator
            implements CostCalculator
    {
        private final Map<PlanNode, PlanCostEstimate> costs;

        private FixedCostCalculator(Map<PlanNode, PlanCostEstimate> costs)
        {
            this.costs = ImmutableMap.copyOf(requireNonNull(costs, "costs is null"));
        }

        @Override
        public PlanCostEstimate calculateCost(PlanNode node, StatsProvider stats, CostProvider sourcesCosts, Session session)
        {
            PlanCostEstimate cost = costs.get(node);
            requireNonNull(cost, "no cost for node");
            return cost;
        }
    }
}
