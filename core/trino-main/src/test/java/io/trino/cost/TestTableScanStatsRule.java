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
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.TestingColumnHandle;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.DoubleRange;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.sql.planner.Symbol;
import org.testng.annotations.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;

public class TestTableScanStatsRule
        extends BaseStatsCalculatorTest
{
    @Test
    public void testStatsForTableScan()
    {
        ColumnHandle columnA = new TestingColumnHandle("a");
        ColumnHandle columnB = new TestingColumnHandle("b");
        ColumnHandle columnC = new TestingColumnHandle("c");
        ColumnHandle columnD = new TestingColumnHandle("d");
        ColumnHandle columnE = new TestingColumnHandle("e");
        ColumnHandle unknownColumn = new TestingColumnHandle("unknown");
        tester()
                .assertStatsFor(pb -> {
                    Symbol a = pb.symbol("a", BIGINT);
                    Symbol b = pb.symbol("b", DOUBLE);
                    Symbol c = pb.symbol("c", DOUBLE);
                    Symbol d = pb.symbol("d", DOUBLE);
                    Symbol e = pb.symbol("e", INTEGER);
                    Symbol unknown = pb.symbol("unknown", INTEGER);
                    return pb.tableScan(
                            ImmutableList.of(a, b, c, d, e, unknown),
                            ImmutableMap.of(a, columnA, b, columnB, c, columnC, d, columnD, e, columnE, unknown, unknownColumn));
                })
                .withTableStatisticsProvider(tableHandle -> TableStatistics.builder()
                        .setRowCount(Estimate.of(33))
                        .setColumnStatistics(
                                columnA,
                                ColumnStatistics.builder().setDistinctValuesCount(Estimate.of(20)).build())
                        .setColumnStatistics(
                                columnB,
                                ColumnStatistics.builder().setNullsFraction(Estimate.of(0.3)).setDistinctValuesCount(Estimate.of(23.1)).build())
                        .setColumnStatistics(
                                columnC,
                                ColumnStatistics.builder().setRange(new DoubleRange(15, 20)).build())
                        .setColumnStatistics(
                                columnD,
                                ColumnStatistics.builder().setDistinctValuesCount(Estimate.of(33)).build())
                        .setColumnStatistics(
                                columnE,
                                ColumnStatistics.builder().setDistinctValuesCount(Estimate.of(31)).build())
                        .setColumnStatistics(unknownColumn, ColumnStatistics.empty())
                        .build())
                .check(check -> check
                        .outputRowsCount(33)
                        .symbolStats("a", assertion -> assertion
                                .distinctValuesCount(20)
                                // UNKNOWN_NULLS_FRACTION populated to allow CBO to use NDV for estimation
                                .nullsFraction(0.1))
                        .symbolStats("b", assertion -> assertion
                                .distinctValuesCount(23.1)
                                .nullsFraction(0.3))
                        .symbolStats("c", assertion -> assertion
                                .distinctValuesCountUnknown()
                                .nullsFractionUnknown()
                                .lowValue(15)
                                .highValue(20))
                        .symbolStats("d", assertion -> assertion
                                .distinctValuesCount(33)
                                .nullsFraction(0))
                        .symbolStats("e", assertion -> assertion
                                .distinctValuesCount(31)
                                .nullsFraction(0.06060606))
                        .symbolStats("unknown", assertion -> assertion
                                .unknownRange()
                                .distinctValuesCountUnknown()
                                .nullsFractionUnknown()
                                .dataSizeUnknown()));
    }

    @Test
    public void testZeroStatsForTableScan()
    {
        ColumnHandle columnHandle = new TestingColumnHandle("zero");
        tester()
                .assertStatsFor(pb -> {
                    Symbol column = pb.symbol("zero", INTEGER);
                    return pb.tableScan(
                            ImmutableList.of(column),
                            ImmutableMap.of(column, columnHandle));
                })
                .withTableStatisticsProvider(tableHandle -> TableStatistics.builder()
                        .setRowCount(Estimate.zero())
                        .setColumnStatistics(
                                columnHandle,
                                ColumnStatistics.builder().setDistinctValuesCount(Estimate.zero()).build())
                        .build())
                .check(check -> check
                        .outputRowsCount(0)
                        .symbolStats("zero", assertion -> assertion.isEqualTo(SymbolStatsEstimate.zero())));
    }
}
