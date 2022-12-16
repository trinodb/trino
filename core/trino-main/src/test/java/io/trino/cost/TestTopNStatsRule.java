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
import io.trino.spi.connector.SortOrder;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.TopNNode;
import org.testng.annotations.Test;

public class TestTopNStatsRule
        extends BaseStatsCalculatorTest
{
    @Test
    public void testTopNWithLongInput()
    {
        // Test case with more rows in data than in topN SINGLE step
        tester().assertStatsFor(pb -> pb
                .topN(10, ImmutableList.of(pb.symbol("i1")), pb.values(pb.symbol("i1"), pb.symbol("i2"))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .addSymbolStatistics(new Symbol("i1"), SymbolStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(10)
                                .setDistinctValuesCount(5)
                                .setNullsFraction(0)
                                .build())
                        .addSymbolStatistics(new Symbol("i2"), SymbolStatsEstimate.builder()
                                .setLowValue(0)
                                .setHighValue(3)
                                .setDistinctValuesCount(4)
                                .setNullsFraction(0)
                                .build())
                        .build())
                .check(check -> check
                        .outputRowsCount(10) //Expect TopN to limit
                        .symbolStats("i1", assertion -> assertion
                                .lowValue(1)
                                .highValue(10)
                                .distinctValuesCount(5)
                                .dataSizeUnknown()
                                .nullsFraction(0))
                        .symbolStats("i2", assertion -> assertion
                                .lowValue(0)
                                .highValue(3)
                                .dataSizeUnknown()
                                .distinctValuesCount(4)
                                .nullsFraction(0)));

        // Test case with more rows in data than in topN PARTIAL step
        PlanNodeStatsEstimate sourceStats = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(100)
                .addSymbolStatistics(new Symbol("i1"), SymbolStatsEstimate.builder()
                        .setLowValue(1)
                        .setHighValue(10)
                        .setDistinctValuesCount(5)
                        .setNullsFraction(0)
                        .build())
                .addSymbolStatistics(new Symbol("i2"), SymbolStatsEstimate.builder()
                        .setLowValue(0)
                        .setHighValue(3)
                        .setDistinctValuesCount(4)
                        .setNullsFraction(0)
                        .build())
                .build();

        tester().assertStatsFor(pb -> pb
                .topN(10, ImmutableList.of(pb.symbol("i1")), TopNNode.Step.PARTIAL, pb.values(pb.symbol("i1"), pb.symbol("i2"))))
                .withSourceStats(0, sourceStats)
                .check(check -> check.equalTo(sourceStats.mapOutputRowCount(ignore -> 10.0)));

        tester().assertStatsFor(pb -> pb
                        .topN(10, ImmutableList.of(pb.symbol("i1")), TopNNode.Step.PARTIAL, pb.values(pb.symbol("i1"), pb.symbol("i2"))))
                .withSourceStats(0, sourceStats.mapOutputRowCount(ignore -> 5000000.0))
                .check(check -> check.equalTo(sourceStats.mapOutputRowCount(ignore -> 50.0)));

        tester().assertStatsFor(pb -> pb
                        .topN(
                                10,
                                ImmutableList.of(pb.symbol("i1")),
                                TopNNode.Step.FINAL,
                                pb.topN(10, ImmutableList.of(pb.symbol("i1")), TopNNode.Step.PARTIAL, pb.values(pb.symbol("i1"), pb.symbol("i2")))))
                .withSourceStats(0, sourceStats)
                .check(check -> check.equalTo(sourceStats.mapOutputRowCount(ignore -> 10.0)));
    }

    @Test
    public void testTopNWithSmallInput()
    {
        // Test case with less rows in data than in topN
        PlanNodeStatsEstimate sourceStats = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(100)
                .addSymbolStatistics(new Symbol("i1"), SymbolStatsEstimate.builder()
                        .setLowValue(1)
                        .setHighValue(10)
                        .setDistinctValuesCount(5)
                        .setNullsFraction(0)
                        .build())
                .addSymbolStatistics(new Symbol("i2"), SymbolStatsEstimate.builder()
                        .setLowValue(0)
                        .setHighValue(3)
                        .setDistinctValuesCount(4)
                        .setNullsFraction(0)
                        .build())
                .build();

        tester().assertStatsFor(pb -> pb
                        .topN(1000, ImmutableList.of(pb.symbol("i1")), pb.values(pb.symbol("i1"), pb.symbol("i2"))))
                .withSourceStats(0, sourceStats)
                .check(check -> check
                        .outputRowsCount(100) //Expect TopN not to limit
                        .symbolStats("i1", assertion -> assertion
                                .lowValue(1)
                                .highValue(10)
                                .distinctValuesCount(5)
                                .dataSizeUnknown()
                                .nullsFraction(0))
                        .symbolStats("i2", assertion -> assertion
                                .lowValue(0)
                                .highValue(3)
                                .dataSizeUnknown()
                                .distinctValuesCount(4)
                                .nullsFraction(0)));

        tester().assertStatsFor(pb -> pb
                        .topN(1000, ImmutableList.of(pb.symbol("i1")), TopNNode.Step.PARTIAL, pb.values(pb.symbol("i1"), pb.symbol("i2"))))
                .withSourceStats(0, sourceStats)
                .check(check -> check.equalTo(sourceStats));
    }

    @Test
    public void testTopNWithNulls()
    {
        // Test no nulls case
        tester().assertStatsFor(pb -> pb
                .topN(10, ImmutableList.of(pb.symbol("i1")), TopNNode.Step.SINGLE, SortOrder.ASC_NULLS_LAST, pb.values(pb.symbol("i1"), pb.symbol("i2"))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .addSymbolStatistics(new Symbol("i1"), SymbolStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(10)
                                .setDistinctValuesCount(5)
                                .setNullsFraction(0.3)
                                .build())
                        .addSymbolStatistics(new Symbol("i2"), SymbolStatsEstimate.builder()
                                .setLowValue(0)
                                .setHighValue(3)
                                .setDistinctValuesCount(4)
                                .setNullsFraction(0.5)
                                .build())
                        .build())
                .check(check -> check
                        .outputRowsCount(10) //Expect TopN to limit
                        .symbolStats("i1", assertion -> assertion
                                .lowValue(1)
                                .highValue(10)
                                .distinctValuesCount(5)
                                .dataSizeUnknown()
                                .nullsFraction(0))
                        .symbolStats("i2", assertion -> assertion
                                .lowValue(0)
                                .highValue(3)
                                .dataSizeUnknown()
                                .distinctValuesCount(4)
                                .nullsFraction(0.5)));

        // test Reducing the nullFraction
        tester().assertStatsFor(pb -> pb
                .topN(50, ImmutableList.of(pb.symbol("i1")), TopNNode.Step.SINGLE, SortOrder.ASC_NULLS_LAST, pb.values(pb.symbol("i1"), pb.symbol("i2"))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .addSymbolStatistics(new Symbol("i1"), SymbolStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(10)
                                .setDistinctValuesCount(5)
                                .setNullsFraction(0.6)
                                .build())
                        .addSymbolStatistics(new Symbol("i2"), SymbolStatsEstimate.builder()
                                .setLowValue(0)
                                .setHighValue(3)
                                .setDistinctValuesCount(4)
                                .setNullsFraction(0.5)
                                .build())
                        .build())
                .check(check -> check
                        .outputRowsCount(50) //Expect TopN to limit
                        .symbolStats("i1", assertion -> assertion
                                .lowValue(1)
                                .highValue(10)
                                .distinctValuesCount(5)
                                .dataSizeUnknown()
                                .nullsFraction(0.2))
                        .symbolStats("i2", assertion -> assertion
                                .lowValue(0)
                                .highValue(3)
                                .dataSizeUnknown()
                                .distinctValuesCount(4)
                                .nullsFraction(0.5)));

        // test nulls first
        tester().assertStatsFor(pb -> pb
                .topN(50, ImmutableList.of(pb.symbol("i1")), pb.values(pb.symbol("i1"), pb.symbol("i2"))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .addSymbolStatistics(new Symbol("i1"), SymbolStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(10)
                                .setDistinctValuesCount(5)
                                .setNullsFraction(0.6)
                                .build())
                        .addSymbolStatistics(new Symbol("i2"), SymbolStatsEstimate.builder()
                                .setLowValue(0)
                                .setHighValue(3)
                                .setDistinctValuesCount(4)
                                .setNullsFraction(0.5)
                                .build())
                        .build())
                .check(check -> check
                        .outputRowsCount(50) //Expect TopN to limit
                        .symbolStats("i1", assertion -> assertion
                                .lowValue(1)
                                .highValue(10)
                                .distinctValuesCount(2.5)
                                .dataSizeUnknown()
                                .nullsFraction(0.95))
                        .symbolStats("i2", assertion -> assertion
                                .lowValue(0)
                                .highValue(3)
                                .dataSizeUnknown()
                                .distinctValuesCount(4)
                                .nullsFraction(0.5)));
    }
}
