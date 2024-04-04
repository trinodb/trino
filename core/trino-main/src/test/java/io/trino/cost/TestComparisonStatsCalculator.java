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

import io.airlift.slice.Slices;
import io.trino.Session;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import static io.trino.cost.ComparisonStatsCalculator.OVERLAPPING_RANGE_INEQUALITY_FILTER_COEFFICIENT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.Comparison.Operator.NOT_EQUAL;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.Double.isNaN;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public class TestComparisonStatsCalculator
{
    private final FilterStatsCalculator filterStatsCalculator = new FilterStatsCalculator(PLANNER_CONTEXT, new ScalarStatsCalculator(PLANNER_CONTEXT), new StatsNormalizer());
    private final Session session = testSessionBuilder().build();
    private final SymbolStatsEstimate uStats = SymbolStatsEstimate.builder()
            .setAverageRowSize(8.0)
            .setDistinctValuesCount(300)
            .setLowValue(0)
            .setHighValue(20)
            .setNullsFraction(0.1)
            .build();
    private final SymbolStatsEstimate wStats = SymbolStatsEstimate.builder()
            .setAverageRowSize(8.0)
            .setDistinctValuesCount(30)
            .setLowValue(0)
            .setHighValue(20)
            .setNullsFraction(0.1)
            .build();
    private final SymbolStatsEstimate xStats = SymbolStatsEstimate.builder()
            .setAverageRowSize(4.0)
            .setDistinctValuesCount(40.0)
            .setLowValue(-10.0)
            .setHighValue(10.0)
            .setNullsFraction(0.25)
            .build();
    private final SymbolStatsEstimate yStats = SymbolStatsEstimate.builder()
            .setAverageRowSize(4.0)
            .setDistinctValuesCount(20.0)
            .setLowValue(0.0)
            .setHighValue(5.0)
            .setNullsFraction(0.5)
            .build();
    private final SymbolStatsEstimate zStats = SymbolStatsEstimate.builder()
            .setAverageRowSize(4.0)
            .setDistinctValuesCount(5.0)
            .setLowValue(-100.0)
            .setHighValue(100.0)
            .setNullsFraction(0.1)
            .build();
    private final SymbolStatsEstimate leftOpenStats = SymbolStatsEstimate.builder()
            .setAverageRowSize(4.0)
            .setDistinctValuesCount(50.0)
            .setLowValue(NEGATIVE_INFINITY)
            .setHighValue(15.0)
            .setNullsFraction(0.1)
            .build();
    private final SymbolStatsEstimate rightOpenStats = SymbolStatsEstimate.builder()
            .setAverageRowSize(4.0)
            .setDistinctValuesCount(50.0)
            .setLowValue(-15.0)
            .setHighValue(POSITIVE_INFINITY)
            .setNullsFraction(0.1)
            .build();
    private final SymbolStatsEstimate unknownRangeStats = SymbolStatsEstimate.builder()
            .setAverageRowSize(4.0)
            .setDistinctValuesCount(50.0)
            .setLowValue(NEGATIVE_INFINITY)
            .setHighValue(POSITIVE_INFINITY)
            .setNullsFraction(0.1)
            .build();
    private final SymbolStatsEstimate emptyRangeStats = SymbolStatsEstimate.builder()
            .setAverageRowSize(0.0)
            .setDistinctValuesCount(0.0)
            .setLowValue(NaN)
            .setHighValue(NaN)
            .setNullsFraction(1.0)
            .build();
    private final SymbolStatsEstimate unknownNdvRangeStats = SymbolStatsEstimate.builder()
            .setAverageRowSize(4.0)
            .setDistinctValuesCount(NaN)
            .setLowValue(0)
            .setHighValue(10)
            .setNullsFraction(0.1)
            .build();
    private final SymbolStatsEstimate varcharStats = SymbolStatsEstimate.builder()
            .setAverageRowSize(4.0)
            .setDistinctValuesCount(50.0)
            .setLowValue(NEGATIVE_INFINITY)
            .setHighValue(POSITIVE_INFINITY)
            .setNullsFraction(0.1)
            .build();

    private final PlanNodeStatsEstimate standardInputStatistics = PlanNodeStatsEstimate.builder()
            .addSymbolStatistics(new Symbol(DOUBLE, "u"), uStats)
            .addSymbolStatistics(new Symbol(DOUBLE, "w"), wStats)
            .addSymbolStatistics(new Symbol(DOUBLE, "x"), xStats)
            .addSymbolStatistics(new Symbol(DOUBLE, "y"), yStats)
            .addSymbolStatistics(new Symbol(DOUBLE, "z"), zStats)
            .addSymbolStatistics(new Symbol(DOUBLE, "leftOpen"), leftOpenStats)
            .addSymbolStatistics(new Symbol(DOUBLE, "rightOpen"), rightOpenStats)
            .addSymbolStatistics(new Symbol(DOUBLE, "unknownRange"), unknownRangeStats)
            .addSymbolStatistics(new Symbol(DOUBLE, "emptyRange"), emptyRangeStats)
            .addSymbolStatistics(new Symbol(DOUBLE, "unknownNdvRange"), unknownNdvRangeStats)
            .addSymbolStatistics(new Symbol(createVarcharType(10), "varchar"), varcharStats)
            .setOutputRowCount(1000.0)
            .build();

    private Consumer<SymbolStatsAssertion> equalTo(SymbolStatsEstimate estimate)
    {
        return symbolAssert -> {
            symbolAssert
                    .lowValue(estimate.getLowValue())
                    .highValue(estimate.getHighValue())
                    .distinctValuesCount(estimate.getDistinctValuesCount())
                    .nullsFraction(estimate.getNullsFraction());
        };
    }

    private SymbolStatsEstimate updateNDV(SymbolStatsEstimate symbolStats, double delta)
    {
        return symbolStats.mapDistinctValuesCount(ndv -> ndv + delta);
    }

    private SymbolStatsEstimate capNDV(SymbolStatsEstimate symbolStats, double rowCount)
    {
        double ndv = symbolStats.getDistinctValuesCount();
        double nulls = symbolStats.getNullsFraction();
        if (isNaN(ndv) || isNaN(rowCount) || isNaN(nulls)) {
            return symbolStats;
        }
        if (ndv <= rowCount * (1 - nulls)) {
            return symbolStats;
        }
        return symbolStats
                .mapDistinctValuesCount(n -> (min(ndv, rowCount) + rowCount * (1 - nulls)) / 2)
                .mapNullsFraction(n -> nulls / 2);
    }

    private SymbolStatsEstimate zeroNullsFraction(SymbolStatsEstimate symbolStats)
    {
        return symbolStats.mapNullsFraction(fraction -> 0.0);
    }

    private PlanNodeStatsAssertion assertCalculate(Expression comparisonExpression)
    {
        return PlanNodeStatsAssertion.assertThat(filterStatsCalculator.filterStats(standardInputStatistics, comparisonExpression, session));
    }

    @Test
    public void verifyTestInputConsistent()
    {
        // if tests' input is not normalized, other tests don't make sense
        checkConsistent(
                new StatsNormalizer(),
                "standardInputStatistics",
                standardInputStatistics,
                standardInputStatistics.getSymbolsWithKnownStatistics());
    }

    @Test
    public void symbolToLiteralEqualStats()
    {
        // Simple case
        assertCalculate(new Comparison(EQUAL, new Reference(DOUBLE, "y"), new Constant(DOUBLE, 2.5)))
                .outputRowsCount(25.0) // all rows minus nulls divided by distinct values count
                .symbolStats("y", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(1.0)
                            .lowValue(2.5)
                            .highValue(2.5)
                            .nullsFraction(0.0);
                });

        // Literal on the edge of symbol range
        assertCalculate(new Comparison(EQUAL, new Reference(DOUBLE, "x"), new Constant(DOUBLE, 10.0)))
                .outputRowsCount(18.75) // all rows minus nulls divided by distinct values count
                .symbolStats("x", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(1.0)
                            .lowValue(10.0)
                            .highValue(10.0)
                            .nullsFraction(0.0);
                });

        // Literal out of symbol range
        assertCalculate(new Comparison(EQUAL, new Reference(DOUBLE, "y"), new Constant(DOUBLE, 10.0)))
                .outputRowsCount(0.0) // all rows minus nulls divided by distinct values count
                .symbolStats("y", symbolAssert -> {
                    symbolAssert.averageRowSize(0.0)
                            .distinctValuesCount(0.0)
                            .emptyRange()
                            .nullsFraction(1.0);
                });

        // Literal in left open range
        assertCalculate(new Comparison(EQUAL, new Reference(DOUBLE, "leftOpen"), new Constant(DOUBLE, 2.5)))
                .outputRowsCount(18.0) // all rows minus nulls divided by distinct values count
                .symbolStats("leftOpen", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(1.0)
                            .lowValue(2.5)
                            .highValue(2.5)
                            .nullsFraction(0.0);
                });

        // Literal in right open range
        assertCalculate(new Comparison(EQUAL, new Reference(DOUBLE, "rightOpen"), new Constant(DOUBLE, -2.5)))
                .outputRowsCount(18.0) // all rows minus nulls divided by distinct values count
                .symbolStats("rightOpen", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(1.0)
                            .lowValue(-2.5)
                            .highValue(-2.5)
                            .nullsFraction(0.0);
                });

        // Literal in unknown range
        assertCalculate(new Comparison(EQUAL, new Reference(DOUBLE, "unknownRange"), new Constant(DOUBLE, 0.0)))
                .outputRowsCount(18.0) // all rows minus nulls divided by distinct values count
                .symbolStats("unknownRange", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(1.0)
                            .lowValue(0.0)
                            .highValue(0.0)
                            .nullsFraction(0.0);
                });

        // Literal in empty range
        assertCalculate(new Comparison(EQUAL, new Reference(DOUBLE, "emptyRange"), new Constant(DOUBLE, 0.0)))
                .outputRowsCount(0.0)
                .symbolStats("emptyRange", equalTo(emptyRangeStats));

        // Column with values not representable as double (unknown range)
        assertCalculate(new Comparison(EQUAL, new Reference(createVarcharType(10), "varchar"), new Constant(createVarcharType(10), Slices.utf8Slice("blah"))))
                .outputRowsCount(18.0) // all rows minus nulls divided by distinct values count
                .symbolStats("varchar", createVarcharType(10), symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(1.0)
                            .lowValue(NEGATIVE_INFINITY)
                            .highValue(POSITIVE_INFINITY)
                            .nullsFraction(0.0);
                });
    }

    @Test
    public void symbolToLiteralNotEqualStats()
    {
        // Simple case
        assertCalculate(new Comparison(NOT_EQUAL, new Reference(DOUBLE, "y"), new Constant(DOUBLE, 2.5)))
                .outputRowsCount(475.0) // all rows minus nulls multiplied by ((distinct values - 1) / distinct values)
                .symbolStats("y", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(19.0)
                            .lowValue(0.0)
                            .highValue(5.0)
                            .nullsFraction(0.0);
                });

        // Literal on the edge of symbol range
        assertCalculate(new Comparison(NOT_EQUAL, new Reference(DOUBLE, "x"), new Constant(DOUBLE, 10.0)))
                .outputRowsCount(731.25) // all rows minus nulls multiplied by ((distinct values - 1) / distinct values)
                .symbolStats("x", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(39.0)
                            .lowValue(-10.0)
                            .highValue(10.0)
                            .nullsFraction(0.0);
                });

        // Literal out of symbol range
        assertCalculate(new Comparison(NOT_EQUAL, new Reference(DOUBLE, "y"), new Constant(DOUBLE, 10.0)))
                .outputRowsCount(500.0) // all rows minus nulls
                .symbolStats("y", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(19.0)
                            .lowValue(0.0)
                            .highValue(5.0)
                            .nullsFraction(0.0);
                });

        // Literal in left open range
        assertCalculate(new Comparison(NOT_EQUAL, new Reference(DOUBLE, "leftOpen"), new Constant(DOUBLE, 2.5)))
                .outputRowsCount(882.0) // all rows minus nulls multiplied by ((distinct values - 1) / distinct values)
                .symbolStats("leftOpen", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(49.0)
                            .lowValueUnknown()
                            .highValue(15.0)
                            .nullsFraction(0.0);
                });

        // Literal in right open range
        assertCalculate(new Comparison(NOT_EQUAL, new Reference(DOUBLE, "rightOpen"), new Constant(DOUBLE, -2.5)))
                .outputRowsCount(882.0) // all rows minus nulls divided by distinct values count
                .symbolStats("rightOpen", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(49.0)
                            .lowValue(-15.0)
                            .highValueUnknown()
                            .nullsFraction(0.0);
                });

        // Literal in unknown range
        assertCalculate(new Comparison(NOT_EQUAL, new Reference(DOUBLE, "unknownRange"), new Constant(DOUBLE, 0.0)))
                .outputRowsCount(882.0) // all rows minus nulls divided by distinct values count
                .symbolStats("unknownRange", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(49.0)
                            .lowValueUnknown()
                            .highValueUnknown()
                            .nullsFraction(0.0);
                });

        // Literal in empty range
        assertCalculate(new Comparison(NOT_EQUAL, new Reference(DOUBLE, "emptyRange"), new Constant(DOUBLE, 0.0)))
                .outputRowsCount(0.0)
                .symbolStats("emptyRange", equalTo(emptyRangeStats));

        // Column with values not representable as double (unknown range)
        assertCalculate(new Comparison(NOT_EQUAL, new Reference(createVarcharType(10), "varchar"), new Constant(createVarcharType(10), Slices.utf8Slice("blah"))))
                .outputRowsCount(882.0) // all rows minus nulls divided by distinct values count
                .symbolStats("varchar", createVarcharType(10), symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(49.0)
                            .lowValueUnknown()
                            .highValueUnknown()
                            .nullsFraction(0.0);
                });
    }

    @Test
    public void symbolToLiteralLessThanStats()
    {
        // Simple case
        assertCalculate(new Comparison(LESS_THAN, new Reference(DOUBLE, "y"), new Constant(DOUBLE, 2.5)))
                .outputRowsCount(250.0) // all rows minus nulls times range coverage (50%)
                .symbolStats("y", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(10.0)
                            .lowValue(0.0)
                            .highValue(2.5)
                            .nullsFraction(0.0);
                });

        // Literal on the edge of symbol range (whole range included)
        assertCalculate(new Comparison(LESS_THAN, new Reference(DOUBLE, "x"), new Constant(DOUBLE, 10.0)))
                .outputRowsCount(750.0) // all rows minus nulls times range coverage (100%)
                .symbolStats("x", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(40.0)
                            .lowValue(-10.0)
                            .highValue(10.0)
                            .nullsFraction(0.0);
                });

        // Literal on the edge of symbol range (whole range excluded)
        assertCalculate(new Comparison(LESS_THAN, new Reference(DOUBLE, "x"), new Constant(DOUBLE, -10.0)))
                .outputRowsCount(18.75) // all rows minus nulls divided by NDV (one value from edge is included as approximation)
                .symbolStats("x", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(1.0)
                            .lowValue(-10.0)
                            .highValue(-10.0)
                            .nullsFraction(0.0);
                });

        // Literal range out of symbol range
        assertCalculate(new Comparison(LESS_THAN, new Reference(DOUBLE, "y"), new Constant(DOUBLE, -10.0)))
                .outputRowsCount(0.0) // all rows minus nulls times range coverage (0%)
                .symbolStats("y", symbolAssert -> {
                    symbolAssert.averageRowSize(0.0)
                            .distinctValuesCount(0.0)
                            .emptyRange()
                            .nullsFraction(1.0);
                });

        // Literal in left open range
        assertCalculate(new Comparison(LESS_THAN, new Reference(DOUBLE, "leftOpen"), new Constant(DOUBLE, 0.0)))
                .outputRowsCount(450.0) // all rows minus nulls times range coverage (50% - heuristic)
                .symbolStats("leftOpen", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(25.0) //(50% heuristic)
                            .lowValueUnknown()
                            .highValue(0.0)
                            .nullsFraction(0.0);
                });

        // Literal in right open range
        assertCalculate(new Comparison(LESS_THAN, new Reference(DOUBLE, "rightOpen"), new Constant(DOUBLE, 0.0)))
                .outputRowsCount(225.0) // all rows minus nulls times range coverage (25% - heuristic)
                .symbolStats("rightOpen", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(12.5) //(25% heuristic)
                            .lowValue(-15.0)
                            .highValue(0.0)
                            .nullsFraction(0.0);
                });

        // Literal in unknown range
        assertCalculate(new Comparison(LESS_THAN, new Reference(DOUBLE, "unknownRange"), new Constant(DOUBLE, 0.0)))
                .outputRowsCount(450.0) // all rows minus nulls times range coverage (50% - heuristic)
                .symbolStats("unknownRange", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(25.0) // (50% heuristic)
                            .lowValueUnknown()
                            .highValue(0.0)
                            .nullsFraction(0.0);
                });

        // Literal in empty range
        assertCalculate(new Comparison(LESS_THAN, new Reference(DOUBLE, "emptyRange"), new Constant(DOUBLE, 0.0)))
                .outputRowsCount(0.0)
                .symbolStats("emptyRange", equalTo(emptyRangeStats));
    }

    @Test
    public void symbolToLiteralGreaterThanStats()
    {
        // Simple case
        assertCalculate(new Comparison(GREATER_THAN, new Reference(DOUBLE, "y"), new Constant(DOUBLE, 2.5)))
                .outputRowsCount(250.0) // all rows minus nulls times range coverage (50%)
                .symbolStats("y", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(10.0)
                            .lowValue(2.5)
                            .highValue(5.0)
                            .nullsFraction(0.0);
                });

        // Literal on the edge of symbol range (whole range included)
        assertCalculate(new Comparison(GREATER_THAN, new Reference(DOUBLE, "x"), new Constant(DOUBLE, -10.0)))
                .outputRowsCount(750.0) // all rows minus nulls times range coverage (100%)
                .symbolStats("x", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(40.0)
                            .lowValue(-10.0)
                            .highValue(10.0)
                            .nullsFraction(0.0);
                });

        // Literal on the edge of symbol range (whole range excluded)
        assertCalculate(new Comparison(GREATER_THAN, new Reference(DOUBLE, "x"), new Constant(DOUBLE, 10.0)))
                .outputRowsCount(18.75) // all rows minus nulls divided by NDV (one value from edge is included as approximation)
                .symbolStats("x", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(1.0)
                            .lowValue(10.0)
                            .highValue(10.0)
                            .nullsFraction(0.0);
                });

        // Literal range out of symbol range
        assertCalculate(new Comparison(GREATER_THAN, new Reference(DOUBLE, "y"), new Constant(DOUBLE, 10.0)))
                .outputRowsCount(0.0) // all rows minus nulls times range coverage (0%)
                .symbolStats("y", symbolAssert -> {
                    symbolAssert.averageRowSize(0.0)
                            .distinctValuesCount(0.0)
                            .emptyRange()
                            .nullsFraction(1.0);
                });

        // Literal in left open range
        assertCalculate(new Comparison(GREATER_THAN, new Reference(DOUBLE, "leftOpen"), new Constant(DOUBLE, 0.0)))
                .outputRowsCount(225.0) // all rows minus nulls times range coverage (25% - heuristic)
                .symbolStats("leftOpen", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(12.5) //(25% heuristic)
                            .lowValue(0.0)
                            .highValue(15.0)
                            .nullsFraction(0.0);
                });

        // Literal in right open range
        assertCalculate(new Comparison(GREATER_THAN, new Reference(DOUBLE, "rightOpen"), new Constant(DOUBLE, 0.0)))
                .outputRowsCount(450.0) // all rows minus nulls times range coverage (50% - heuristic)
                .symbolStats("rightOpen", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(25.0) //(50% heuristic)
                            .lowValue(0.0)
                            .highValueUnknown()
                            .nullsFraction(0.0);
                });

        // Literal in unknown range
        assertCalculate(new Comparison(GREATER_THAN, new Reference(DOUBLE, "unknownRange"), new Constant(DOUBLE, 0.0)))
                .outputRowsCount(450.0) // all rows minus nulls times range coverage (50% - heuristic)
                .symbolStats("unknownRange", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(25.0) // (50% heuristic)
                            .lowValue(0.0)
                            .highValueUnknown()
                            .nullsFraction(0.0);
                });

        // Literal in empty range
        assertCalculate(new Comparison(GREATER_THAN, new Reference(DOUBLE, "emptyRange"), new Constant(DOUBLE, 0.0)))
                .outputRowsCount(0.0)
                .symbolStats("emptyRange", equalTo(emptyRangeStats));
    }

    @Test
    public void symbolToSymbolEqualStats()
    {
        // z's stats should be unchanged when not involved, except NDV capping to row count
        // Equal ranges
        double rowCount = 2.7;
        assertCalculate(new Comparison(EQUAL, new Reference(DOUBLE, "u"), new Reference(DOUBLE, "w")))
                .outputRowsCount(rowCount)
                .symbolStats("u", DOUBLE, equalTo(capNDV(zeroNullsFraction(uStats), rowCount)))
                .symbolStats("w", DOUBLE, equalTo(capNDV(zeroNullsFraction(wStats), rowCount)))
                .symbolStats("z", DOUBLE, equalTo(capNDV(zStats, rowCount)));

        // One symbol's range is within the other's
        rowCount = 9.375;
        assertCalculate(new Comparison(EQUAL, new Reference(DOUBLE, "x"), new Reference(DOUBLE, "y")))
                .outputRowsCount(rowCount)
                .symbolStats("x", symbolAssert -> {
                    symbolAssert.averageRowSize(4)
                            .lowValue(0)
                            .highValue(5)
                            .distinctValuesCount(9.375 /* min(rowCount, ndv in intersection */)
                            .nullsFraction(0);
                })
                .symbolStats("y", symbolAssert -> {
                    symbolAssert.averageRowSize(4)
                            .lowValue(0)
                            .highValue(5)
                            .distinctValuesCount(9.375 /* min(rowCount, ndv in intersection */)
                            .nullsFraction(0);
                })
                .symbolStats("z", equalTo(capNDV(zStats, rowCount)));

        // Partially overlapping ranges
        rowCount = 16.875;
        assertCalculate(new Comparison(EQUAL, new Reference(DOUBLE, "x"), new Reference(DOUBLE, "w")))
                .outputRowsCount(rowCount)
                .symbolStats("x", symbolAssert -> {
                    symbolAssert.averageRowSize(6)
                            .lowValue(0)
                            .highValue(10)
                            .distinctValuesCount(16.875 /* min(rowCount, ndv in intersection */)
                            .nullsFraction(0);
                })
                .symbolStats("w", symbolAssert -> {
                    symbolAssert.averageRowSize(6)
                            .lowValue(0)
                            .highValue(10)
                            .distinctValuesCount(16.875 /* min(rowCount, ndv in intersection */)
                            .nullsFraction(0);
                })
                .symbolStats("z", equalTo(capNDV(zStats, rowCount)));

        // None of the ranges is included in the other, and one symbol has much higher cardinality, so that it has bigger NDV in intersect than the other in total
        rowCount = 2.25;
        assertCalculate(new Comparison(EQUAL, new Reference(DOUBLE, "x"), new Reference(DOUBLE, "u")))
                .outputRowsCount(rowCount)
                .symbolStats("x", symbolAssert -> {
                    symbolAssert.averageRowSize(6)
                            .lowValue(0)
                            .highValue(10)
                            .distinctValuesCount(2.25 /* min(rowCount, ndv in intersection */)
                            .nullsFraction(0);
                })
                .symbolStats("u", symbolAssert -> {
                    symbolAssert.averageRowSize(6)
                            .lowValue(0)
                            .highValue(10)
                            .distinctValuesCount(2.25 /* min(rowCount, ndv in intersection */)
                            .nullsFraction(0);
                })
                .symbolStats("z", equalTo(capNDV(zStats, rowCount)));
    }

    @Test
    public void symbolToSymbolNotEqual()
    {
        // Equal ranges
        double rowCount = 807.3;
        assertCalculate(new Comparison(NOT_EQUAL, new Reference(DOUBLE, "u"), new Reference(DOUBLE, "w")))
                .outputRowsCount(rowCount)
                .symbolStats("u", equalTo(capNDV(zeroNullsFraction(uStats), rowCount)))
                .symbolStats("w", equalTo(capNDV(zeroNullsFraction(wStats), rowCount)))
                .symbolStats("z", equalTo(capNDV(zStats, rowCount)));

        // One symbol's range is within the other's
        rowCount = 365.625;
        assertCalculate(new Comparison(NOT_EQUAL, new Reference(DOUBLE, "x"), new Reference(DOUBLE, "y")))
                .outputRowsCount(rowCount)
                .symbolStats("x", equalTo(capNDV(zeroNullsFraction(xStats), rowCount)))
                .symbolStats("y", equalTo(capNDV(zeroNullsFraction(yStats), rowCount)))
                .symbolStats("z", equalTo(capNDV(zStats, rowCount)));

        // Partially overlapping ranges
        rowCount = 658.125;
        assertCalculate(new Comparison(NOT_EQUAL, new Reference(DOUBLE, "x"), new Reference(DOUBLE, "w")))
                .outputRowsCount(rowCount)
                .symbolStats("x", equalTo(capNDV(zeroNullsFraction(xStats), rowCount)))
                .symbolStats("w", equalTo(capNDV(zeroNullsFraction(wStats), rowCount)))
                .symbolStats("z", equalTo(capNDV(zStats, rowCount)));

        // None of the ranges is included in the other, and one symbol has much higher cardinality, so that it has bigger NDV in intersect than the other in total
        rowCount = 672.75;
        assertCalculate(new Comparison(NOT_EQUAL, new Reference(DOUBLE, "x"), new Reference(DOUBLE, "u")))
                .outputRowsCount(rowCount)
                .symbolStats("x", equalTo(capNDV(zeroNullsFraction(xStats), rowCount)))
                .symbolStats("u", equalTo(capNDV(zeroNullsFraction(uStats), rowCount)))
                .symbolStats("z", equalTo(capNDV(zStats, rowCount)));
    }

    @Test
    public void symbolToCastExpressionNotEqual()
    {
        double rowCount = 897.0;
        assertCalculate(new Comparison(NOT_EQUAL, new Reference(DOUBLE, "u"), new Constant(DOUBLE, 10.0)))
                .outputRowsCount(rowCount)
                .symbolStats("u", equalTo(capNDV(updateNDV(zeroNullsFraction(uStats), -1), rowCount)))
                .symbolStats("z", equalTo(capNDV(zStats, rowCount)));
    }

    @Test
    public void symbolToSymbolInequalityStats()
    {
        double inputRowCount = standardInputStatistics.getOutputRowCount();
        // z's stats should be unchanged when not involved, except NDV capping to row count

        double nullsFractionX = 0.25;
        double rowCount = inputRowCount * (1 - nullsFractionX);
        // Same symbol on both sides of inequality, gets simplified to x IS NOT NULL
        assertCalculate(new Comparison(LESS_THAN, new Reference(DOUBLE, "x"), new Reference(DOUBLE, "x")))
                .outputRowsCount(rowCount)
                .symbolStats("x", equalTo(capNDV(zeroNullsFraction(xStats), rowCount)));

        double nullsFractionU = 0.1;
        double nonNullRowCount = inputRowCount * (1 - nullsFractionU);
        rowCount = nonNullRowCount * OVERLAPPING_RANGE_INEQUALITY_FILTER_COEFFICIENT;
        // Equal ranges
        assertCalculate(new Comparison(LESS_THAN, new Reference(DOUBLE, "u"), new Reference(DOUBLE, "w")))
                .outputRowsCount(rowCount)
                .symbolStats("u", equalTo(capNDV(zeroNullsFraction(uStats), rowCount)))
                .symbolStats("w", equalTo(capNDV(zeroNullsFraction(wStats), rowCount)))
                .symbolStats("z", equalTo(capNDV(zStats, rowCount)));

        double overlappingFractionX = 0.25;
        double alwaysLesserFractionX = 0.5;
        double nullsFractionY = 0.5;
        nonNullRowCount = inputRowCount * (1 - nullsFractionY);
        rowCount = nonNullRowCount * (alwaysLesserFractionX + (overlappingFractionX * OVERLAPPING_RANGE_INEQUALITY_FILTER_COEFFICIENT));
        // One symbol's range is within the other's
        assertCalculate(new Comparison(LESS_THAN, new Reference(DOUBLE, "x"), new Reference(DOUBLE, "y")))
                .outputRowsCount(rowCount)
                .symbolStats("x", symbolAssert -> symbolAssert.averageRowSize(4)
                        .lowValue(-10)
                        .highValue(5)
                        .distinctValuesCount(30)
                        .nullsFraction(0))
                .symbolStats("y", symbolAssert -> symbolAssert.averageRowSize(4)
                        .lowValue(0)
                        .highValue(5)
                        .distinctValuesCount(20)
                        .nullsFraction(0))
                .symbolStats("z", equalTo(capNDV(zStats, rowCount)));
        assertCalculate(new Comparison(LESS_THAN_OR_EQUAL, new Reference(DOUBLE, "x"), new Reference(DOUBLE, "y")))
                .outputRowsCount(rowCount)
                .symbolStats("x", symbolAssert -> symbolAssert.averageRowSize(4)
                        .lowValue(-10)
                        .highValue(5)
                        .distinctValuesCount(30)
                        .nullsFraction(0))
                .symbolStats("y", symbolAssert -> symbolAssert.averageRowSize(4)
                        .lowValue(0)
                        .highValue(5)
                        .distinctValuesCount(20)
                        .nullsFraction(0))
                .symbolStats("z", equalTo(capNDV(zStats, rowCount)));
        // Flip symbols to be on opposite sides
        assertCalculate(new Comparison(GREATER_THAN, new Reference(DOUBLE, "y"), new Reference(DOUBLE, "x")))
                .outputRowsCount(rowCount)
                .symbolStats("x", symbolAssert -> symbolAssert.averageRowSize(4)
                        .lowValue(-10)
                        .highValue(5)
                        .distinctValuesCount(30)
                        .nullsFraction(0))
                .symbolStats("y", symbolAssert -> symbolAssert.averageRowSize(4)
                        .lowValue(0)
                        .highValue(5)
                        .distinctValuesCount(20)
                        .nullsFraction(0))
                .symbolStats("z", equalTo(capNDV(zStats, rowCount)));

        double alwaysGreaterFractionX = 0.25;
        rowCount = nonNullRowCount * (alwaysGreaterFractionX + overlappingFractionX * OVERLAPPING_RANGE_INEQUALITY_FILTER_COEFFICIENT);
        assertCalculate(new Comparison(GREATER_THAN, new Reference(DOUBLE, "x"), new Reference(DOUBLE, "y")))
                .outputRowsCount(rowCount)
                .symbolStats("x", symbolAssert -> symbolAssert.averageRowSize(4)
                        .lowValue(0)
                        .highValue(10)
                        .distinctValuesCount(20)
                        .nullsFraction(0))
                .symbolStats("y", symbolAssert -> symbolAssert.averageRowSize(4)
                        .lowValue(0)
                        .highValue(5)
                        .distinctValuesCount(20)
                        .nullsFraction(0))
                .symbolStats("z", equalTo(capNDV(zStats, rowCount)));
        assertCalculate(new Comparison(GREATER_THAN_OR_EQUAL, new Reference(DOUBLE, "x"), new Reference(DOUBLE, "y")))
                .outputRowsCount(rowCount)
                .symbolStats("x", symbolAssert -> symbolAssert.averageRowSize(4)
                        .lowValue(0)
                        .highValue(10)
                        .distinctValuesCount(20)
                        .nullsFraction(0))
                .symbolStats("y", symbolAssert -> symbolAssert.averageRowSize(4)
                        .lowValue(0)
                        .highValue(5)
                        .distinctValuesCount(20)
                        .nullsFraction(0))
                .symbolStats("z", equalTo(capNDV(zStats, rowCount)));
        // Flip symbols to be on opposite sides
        assertCalculate(new Comparison(LESS_THAN, new Reference(DOUBLE, "y"), new Reference(DOUBLE, "x")))
                .outputRowsCount(rowCount)
                .symbolStats("x", symbolAssert -> symbolAssert.averageRowSize(4)
                        .lowValue(0)
                        .highValue(10)
                        .distinctValuesCount(20)
                        .nullsFraction(0))
                .symbolStats("y", symbolAssert -> symbolAssert.averageRowSize(4)
                        .lowValue(0)
                        .highValue(5)
                        .distinctValuesCount(20)
                        .nullsFraction(0))
                .symbolStats("z", equalTo(capNDV(zStats, rowCount)));

        // Partially overlapping ranges
        overlappingFractionX = 0.5;
        nonNullRowCount = inputRowCount * (1 - nullsFractionX);
        double overlappingFractionW = 0.5;
        double alwaysGreaterFractionW = 0.5;
        rowCount = nonNullRowCount * (alwaysLesserFractionX +
                overlappingFractionX * (overlappingFractionW * OVERLAPPING_RANGE_INEQUALITY_FILTER_COEFFICIENT + alwaysGreaterFractionW));
        assertCalculate(new Comparison(LESS_THAN, new Reference(DOUBLE, "x"), new Reference(DOUBLE, "w")))
                .outputRowsCount(rowCount)
                .symbolStats("x", symbolAssert -> symbolAssert.averageRowSize(4)
                        .lowValue(-10)
                        .highValue(10)
                        .distinctValuesCount(40)
                        .nullsFraction(0))
                .symbolStats("w", symbolAssert -> symbolAssert.averageRowSize(8)
                        .lowValue(0)
                        .highValue(20)
                        .distinctValuesCount(30)
                        .nullsFraction(0))
                .symbolStats("z", equalTo(capNDV(zStats, rowCount)));
        // Flip symbols to be on opposite sides
        assertCalculate(new Comparison(GREATER_THAN, new Reference(DOUBLE, "w"), new Reference(DOUBLE, "x")))
                .outputRowsCount(rowCount)
                .symbolStats("x", DOUBLE, symbolAssert -> symbolAssert.averageRowSize(4)
                        .lowValue(-10)
                        .highValue(10)
                        .distinctValuesCount(40)
                        .nullsFraction(0))
                .symbolStats("w", symbolAssert -> symbolAssert.averageRowSize(8)
                        .lowValue(0)
                        .highValue(20)
                        .distinctValuesCount(30)
                        .nullsFraction(0))
                .symbolStats("z", DOUBLE, equalTo(capNDV(zStats, rowCount)));

        rowCount = nonNullRowCount * (overlappingFractionX * overlappingFractionW * OVERLAPPING_RANGE_INEQUALITY_FILTER_COEFFICIENT);
        assertCalculate(new Comparison(GREATER_THAN, new Reference(DOUBLE, "x"), new Reference(DOUBLE, "w")))
                .outputRowsCount(rowCount)
                .symbolStats("x", DOUBLE, symbolAssert -> symbolAssert.averageRowSize(4)
                        .lowValue(0)
                        .highValue(10)
                        .distinctValuesCount(20)
                        .nullsFraction(0))
                .symbolStats("w", DOUBLE, symbolAssert -> symbolAssert.averageRowSize(8)
                        .lowValue(0)
                        .highValue(10)
                        .distinctValuesCount(15)
                        .nullsFraction(0))
                .symbolStats("z", DOUBLE, equalTo(capNDV(zStats, rowCount)));
        // Flip symbols to be on opposite sides
        assertCalculate(new Comparison(LESS_THAN, new Reference(DOUBLE, "w"), new Reference(DOUBLE, "x")))
                .outputRowsCount(rowCount)
                .symbolStats("x", DOUBLE, symbolAssert -> symbolAssert.averageRowSize(4)
                        .lowValue(0)
                        .highValue(10)
                        .distinctValuesCount(20)
                        .nullsFraction(0))
                .symbolStats("w", DOUBLE, symbolAssert -> symbolAssert.averageRowSize(8)
                        .lowValue(0)
                        .highValue(10)
                        .distinctValuesCount(15)
                        .nullsFraction(0))
                .symbolStats("z", DOUBLE, equalTo(capNDV(zStats, rowCount)));

        // Open ranges
        double nullsFractionLeft = 0.1;
        nonNullRowCount = inputRowCount * (1 - nullsFractionLeft);
        double overlappingFractionLeft = 0.25;
        double alwaysLesserFractionLeft = 0.5;
        double overlappingFractionRight = 0.25;
        double alwaysGreaterFractionRight = 0.5;
        rowCount = nonNullRowCount * (alwaysLesserFractionLeft + overlappingFractionLeft
                * (overlappingFractionRight * OVERLAPPING_RANGE_INEQUALITY_FILTER_COEFFICIENT + alwaysGreaterFractionRight));
        assertCalculate(new Comparison(LESS_THAN, new Reference(DOUBLE, "leftOpen"), new Reference(DOUBLE, "rightOpen")))
                .outputRowsCount(rowCount)
                .symbolStats("leftOpen", DOUBLE, symbolAssert -> symbolAssert.averageRowSize(4)
                        .lowValue(NEGATIVE_INFINITY)
                        .highValue(15)
                        .distinctValuesCount(37.5)
                        .nullsFraction(0))
                .symbolStats("rightOpen", DOUBLE, symbolAssert -> symbolAssert.averageRowSize(4)
                        .lowValue(-15)
                        .highValue(POSITIVE_INFINITY)
                        .distinctValuesCount(37.5)
                        .nullsFraction(0))
                .symbolStats("z", DOUBLE, equalTo(capNDV(zStats, rowCount)));

        rowCount = nonNullRowCount * (alwaysLesserFractionLeft + overlappingFractionLeft * OVERLAPPING_RANGE_INEQUALITY_FILTER_COEFFICIENT);
        assertCalculate(new Comparison(LESS_THAN, new Reference(DOUBLE, "leftOpen"), new Reference(DOUBLE, "unknownNdvRange")))
                .outputRowsCount(rowCount)
                .symbolStats("leftOpen", DOUBLE, symbolAssert -> symbolAssert.averageRowSize(4)
                        .lowValue(NEGATIVE_INFINITY)
                        .highValue(10)
                        .distinctValuesCount(37.5)
                        .nullsFraction(0))
                .symbolStats("unknownNdvRange", DOUBLE, symbolAssert -> symbolAssert.averageRowSize(4)
                        .lowValue(0)
                        .highValue(10)
                        .distinctValuesCount(NaN)
                        .nullsFraction(0))
                .symbolStats("z", DOUBLE, equalTo(capNDV(zStats, rowCount)));

        rowCount = nonNullRowCount * OVERLAPPING_RANGE_INEQUALITY_FILTER_COEFFICIENT;
        assertCalculate(new Comparison(LESS_THAN, new Reference(DOUBLE, "leftOpen"), new Reference(DOUBLE, "unknownRange")))
                .outputRowsCount(rowCount)
                .symbolStats("leftOpen", DOUBLE, symbolAssert -> symbolAssert.averageRowSize(4)
                        .lowValue(NEGATIVE_INFINITY)
                        .highValue(15)
                        .distinctValuesCount(50)
                        .nullsFraction(0))
                .symbolStats("unknownRange", DOUBLE, symbolAssert -> symbolAssert.averageRowSize(4)
                        .lowValue(NEGATIVE_INFINITY)
                        .highValue(POSITIVE_INFINITY)
                        .distinctValuesCount(50)
                        .nullsFraction(0))
                .symbolStats("z", DOUBLE, equalTo(capNDV(zStats, rowCount)));
    }

    private static void checkConsistent(StatsNormalizer normalizer, String source, PlanNodeStatsEstimate stats, Collection<Symbol> outputSymbols)
    {
        PlanNodeStatsEstimate normalized = normalizer.normalize(stats, outputSymbols);
        if (Objects.equals(stats, normalized)) {
            return;
        }

        List<String> problems = new ArrayList<>();

        if (Double.compare(stats.getOutputRowCount(), normalized.getOutputRowCount()) != 0) {
            problems.add(format(
                    "Output row count is %s, should be normalized to %s",
                    stats.getOutputRowCount(),
                    normalized.getOutputRowCount()));
        }

        for (Symbol symbol : stats.getSymbolsWithKnownStatistics()) {
            if (!Objects.equals(stats.getSymbolStatistics(symbol), normalized.getSymbolStatistics(symbol))) {
                problems.add(format(
                        "Symbol stats for '%s' are \n\t\t\t\t\t%s, should be normalized to \n\t\t\t\t\t%s",
                        symbol,
                        stats.getSymbolStatistics(symbol),
                        normalized.getSymbolStatistics(symbol)));
            }
        }

        if (problems.isEmpty()) {
            problems.add(stats.toString());
        }
        throw new IllegalStateException(format(
                "Rule %s returned inconsistent stats: %s",
                source,
                problems.stream().collect(joining("\n\t\t\t", "\n\t\t\t", ""))));
    }
}
