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

import io.trino.sql.ir.ComparisonExpression;
import io.trino.sql.ir.LongLiteral;
import io.trino.sql.ir.SymbolReference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.ChooseAlternativeNode.FilteredTableScan;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Optional;

import static io.trino.sql.ir.ComparisonExpression.Operator.EQUAL;
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
                                        pb.filter(
                                                new ComparisonExpression(EQUAL, new SymbolReference("i1"), new LongLiteral("5")),
                                                pb.values(pb.symbol("i1"), pb.symbol("i2"))),
                                        pb.filter(
                                                new ComparisonExpression(EQUAL, new SymbolReference("i1"), new LongLiteral("10")),
                                                pb.values(pb.symbol("i1"), pb.symbol("i2")))),
                                new FilteredTableScan(pb.tableScan(List.of(pb.symbol("i1"), pb.symbol("i2")), false), Optional.empty())))
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
                                .setNullsFraction(0.5)
                                .build())
                        .build())
                .withSourceStats(1, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(5)
                        .addSymbolStatistics(new Symbol("i1"), SymbolStatsEstimate.builder()
                                .setLowValue(7)
                                .setHighValue(9)
                                .setAverageRowSize(3)
                                .setDistinctValuesCount(NaN)
                                .setNullsFraction(0.2)
                                .build())
                        .addSymbolStatistics(new Symbol("i2"), SymbolStatsEstimate.builder()
                                .setLowValue(-5)
                                .setHighValue(12)
                                .setAverageRowSize(NaN)
                                .setDistinctValuesCount(NaN)
                                .setNullsFraction(0.1)
                                .build())
                        .build())
                .check(check -> check
                        .outputRowsCount(10)
                        .symbolStats("i1", assertion -> assertion
                                .lowValue(1)
                                .highValue(10)
                                .averageRowSize(NaN)
                                .distinctValuesCount(5)
                                .nullsFraction(0))
                        .symbolStats("i2", assertion -> assertion
                                .lowValue(0)
                                .highValue(3)
                                .averageRowSize(25)
                                .distinctValuesCount(4)
                                .nullsFraction(0.5)));
    }
}
