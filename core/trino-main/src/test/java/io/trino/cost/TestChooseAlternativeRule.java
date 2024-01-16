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

import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.ChooseAlternativeNode.FilteredTableScan;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Optional;

import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static java.lang.Double.NaN;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
class TestChooseAlternativeRule
        extends BaseStatsCalculatorTest
{
    @Test
    public void testStatsForChooseAlternative()
    {
        tester().assertStatsFor(pb -> pb
                        .chooseAlternative(
                                List.of(
                                        pb.filter(expression("i1 = 5"),
                                                pb.values(pb.symbol("i1"), pb.symbol("i2"), pb.symbol("i3"))),
                                        pb.filter(expression("i1 = 10"),
                                                pb.values(pb.symbol("i1"), pb.symbol("i2"), pb.symbol("i3")))),
                                new FilteredTableScan(pb.tableScan(List.of(pb.symbol("i1"), pb.symbol("i2"), pb.symbol("i3")), false), Optional.empty())))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10)
                        .addSymbolStatistics(new Symbol("i1"), SymbolStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(10)
                                .setAverageRowSize(NaN)
                                .setDistinctValuesCount(5)
                                .setNullsFraction(0)
                                .build())
                        .addSymbolStatistics(new Symbol("i2"), SymbolStatsEstimate.builder()
                                .setLowValue(0)
                                .setHighValue(3)
                                .setAverageRowSize(25)
                                .setDistinctValuesCount(4)
                                .setNullsFraction(0)
                                .build())
                        .addSymbolStatistics(new Symbol("i3"), SymbolStatsEstimate.builder()
                                .setLowValue(10)
                                .setHighValue(15)
                                .setDistinctValuesCount(4)
                                .setNullsFraction(0.1)
                                .build())
                        .build())
                .withSourceStats(1, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(5)
                        .addSymbolStatistics(new Symbol("i1"), SymbolStatsEstimate.builder()
                                .setLowValue(7)
                                .setHighValue(9)
                                .setAverageRowSize(NaN)
                                .setDistinctValuesCount(3)
                                .setNullsFraction(0.2)
                                .build())
                        .addSymbolStatistics(new Symbol("i2"), SymbolStatsEstimate.builder()
                                .setLowValue(NaN)
                                .setHighValue(NaN)
                                .setAverageRowSize(NaN)
                                .setDistinctValuesCount(NaN)
                                .setNullsFraction(NaN)
                                .build())
                        .addSymbolStatistics(new Symbol("i3"), SymbolStatsEstimate.builder()
                                .setLowValue(6)
                                .setHighValue(14)
                                .setDistinctValuesCount(5)
                                .setNullsFraction(0.2)
                                .build())
                        .build())
                .check(check -> check
                        .outputRowsCount(5)
                        .symbolStats("i1", assertion -> assertion
                                .lowValue(7)
                                .highValue(9)
                                .distinctValuesCount(3)
                                .dataSizeUnknown()
                                .nullsFraction(0))
                        .symbolStats("i2", assertion -> assertion
                                .lowValue(0)
                                .highValue(3)
                                .averageRowSize(25)
                                .distinctValuesCount(4)
                                .nullsFraction(0))
                        .symbolStats("i3", assertion -> assertion
                                .lowValue(10)
                                .highValue(14)
                                .dataSizeUnknown()
                                .distinctValuesCount(4)
                                .nullsFraction(0.1)));
    }
}
