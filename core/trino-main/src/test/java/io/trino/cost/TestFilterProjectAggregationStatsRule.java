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

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import org.testng.annotations.Test;

import java.util.function.Function;

import static io.trino.SystemSessionProperties.NON_ESTIMATABLE_PREDICATE_APPROXIMATION_ENABLED;
import static io.trino.cost.FilterStatsCalculator.UNKNOWN_FILTER_COEFFICIENT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestFilterProjectAggregationStatsRule
        extends BaseStatsCalculatorTest
{
    private static final SymbolStatsEstimate SYMBOL_STATS_ESTIMATE_X = SymbolStatsEstimate.builder()
            .setLowValue(0)
            .setHighValue(100)
            .setDistinctValuesCount(10)
            .setNullsFraction(0.1)
            .build();

    private static final SymbolStatsEstimate SYMBOL_STATS_ESTIMATE_Y = SymbolStatsEstimate.builder()
            .setLowValue(0)
            .setHighValue(10)
            .setDistinctValuesCount(10)
            .setNullsFraction(0)
            .build();

    private static final Session APPROXIMATION_ENABLED = testSessionBuilder()
            .setSystemProperty(NON_ESTIMATABLE_PREDICATE_APPROXIMATION_ENABLED, "true")
            .build();

    private static final Session APPROXIMATION_DISABLED = testSessionBuilder()
            .setSystemProperty(NON_ESTIMATABLE_PREDICATE_APPROXIMATION_ENABLED, "false")
            .build();

    @Test
    public void testFilterOverAggregationStats()
    {
        Function<PlanBuilder, PlanNode> planProvider = pb -> pb.filter(
                expression("count_on_x > 0"),
                pb.aggregation(ab -> ab
                        .addAggregation(pb.symbol("count_on_x", BIGINT), expression("count(x)"), ImmutableList.of(BIGINT))
                        .singleGroupingSet(pb.symbol("y", BIGINT))
                        .source(pb.values(pb.symbol("x", BIGINT), pb.symbol("y", BIGINT)))));

        PlanNodeStatsEstimate sourceStats = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(100)
                .addSymbolStatistics(new Symbol("y"), SYMBOL_STATS_ESTIMATE_Y)
                .build();

        tester().assertStatsFor(APPROXIMATION_ENABLED, planProvider)
                .withSourceStats(sourceStats)
                .check(check -> check.outputRowsCount(100 * UNKNOWN_FILTER_COEFFICIENT)
                        .symbolStatsUnknown("count_on_x"));

        tester().assertStatsFor(APPROXIMATION_DISABLED, planProvider)
                .withSourceStats(sourceStats)
                .check(PlanNodeStatsAssertion::outputRowsCountUnknown);

        // No estimate when source row count is not provided
        tester().assertStatsFor(APPROXIMATION_ENABLED, planProvider)
                .withSourceStats(PlanNodeStatsEstimate.builder()
                        .addSymbolStatistics(new Symbol("y"), SymbolStatsEstimate.builder().setDistinctValuesCount(50).build())
                        .build())
                .check(PlanNodeStatsAssertion::outputRowsCountUnknown);

        // If filter estimate is known, approximation should not be applied
        tester().assertStatsFor(APPROXIMATION_ENABLED, pb -> pb.filter(
                        expression("y = 1"),
                        pb.aggregation(ab -> ab
                                .addAggregation(pb.symbol("count_on_x", BIGINT), expression("count(x)"), ImmutableList.of(BIGINT))
                                .singleGroupingSet(pb.symbol("y", BIGINT))
                                .source(pb.values(pb.symbol("x", BIGINT), pb.symbol("y", BIGINT))))))
                .withSourceStats(sourceStats)
                .check(check -> check.outputRowsCount(100 * (1.0 / 10)));
    }

    @Test
    public void testFilterAndProjectOverAggregationStats()
    {
        PlanNodeId aggregationId = new PlanNodeId("aggregation");
        PlanNodeStatsEstimate sourceStats = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(100)
                .addSymbolStatistics(new Symbol("x"), SYMBOL_STATS_ESTIMATE_X)
                .addSymbolStatistics(new Symbol("y"), SYMBOL_STATS_ESTIMATE_Y)
                .build();
        tester().assertStatsFor(
                        APPROXIMATION_ENABLED,
                        pb -> {
                            Symbol aggregatedOutput = pb.symbol("count_on_x", BIGINT);
                            return pb.filter(
                                    expression("count_on_x > 0"),
                                    // Narrowing identity projection
                                    pb.project(Assignments.identity(aggregatedOutput),
                                            pb.aggregation(ab -> ab
                                                    .addAggregation(aggregatedOutput, expression("count(x)"), ImmutableList.of(BIGINT))
                                                    .singleGroupingSet(pb.symbol("y", BIGINT))
                                                    .source(pb.values(pb.symbol("x", BIGINT), pb.symbol("y", BIGINT)))
                                                    .nodeId(aggregationId))));
                        })
                .withSourceStats(sourceStats)
                .withSourceStats(aggregationId, PlanNodeStatsEstimate.builder().setOutputRowCount(50).build())
                .check(check -> check.outputRowsCount(50 * UNKNOWN_FILTER_COEFFICIENT));

        tester().assertStatsFor(
                        APPROXIMATION_ENABLED,
                        pb -> {
                            Symbol aggregatedOutput = pb.symbol("count_on_x", BIGINT);
                            return pb.filter(
                                    expression("count_on_x > 0"),
                                    // Non-narrowing projection
                                    pb.project(Assignments.of(pb.symbol("x_1"), PlanBuilder.expression("x + 1"), aggregatedOutput, aggregatedOutput.toSymbolReference()),
                                            pb.aggregation(ab -> ab
                                                    .addAggregation(aggregatedOutput, expression("count(x)"), ImmutableList.of(BIGINT))
                                                    .singleGroupingSet(pb.symbol("y", BIGINT))
                                                    .source(pb.values(pb.symbol("x", BIGINT), pb.symbol("y", BIGINT)))
                                                    .nodeId(aggregationId))));
                        })
                .withSourceStats(sourceStats)
                .withSourceStats(aggregationId, PlanNodeStatsEstimate.builder().setOutputRowCount(50).build())
                .check(PlanNodeStatsAssertion::outputRowsCountUnknown);
    }
}
