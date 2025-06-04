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

import io.trino.sql.ir.Comparison;
import io.trino.sql.planner.Symbol;

import java.util.Optional;
import java.util.OptionalDouble;

import static io.trino.cost.SymbolStatsEstimate.buildFrom;
import static io.trino.util.MoreMath.averageExcludingNaNs;
import static io.trino.util.MoreMath.max;
import static io.trino.util.MoreMath.maxExcludeNaN;
import static io.trino.util.MoreMath.min;
import static io.trino.util.MoreMath.minExcludeNaN;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.Double.isFinite;
import static java.lang.Double.isNaN;

public final class ComparisonStatsCalculator
{
    // We assume uniform distribution of values within each range.
    // Within the overlapping range, we assume that all pairs of distinct values from both ranges exist.
    // Based on the above, we estimate that half of the pairs of values will match inequality predicate on average.
    public static final double OVERLAPPING_RANGE_INEQUALITY_FILTER_COEFFICIENT = 0.5;

    private ComparisonStatsCalculator() {}

    public static PlanNodeStatsEstimate estimateExpressionToLiteralComparison(
            PlanNodeStatsEstimate inputStatistics,
            SymbolStatsEstimate expressionStatistics,
            Optional<Symbol> expressionSymbol,
            OptionalDouble literalValue,
            Comparison.Operator operator)
    {
        return switch (operator) {
            case EQUAL -> estimateExpressionEqualToLiteral(inputStatistics, expressionStatistics, expressionSymbol, literalValue);
            case NOT_EQUAL -> estimateExpressionNotEqualToLiteral(inputStatistics, expressionStatistics, expressionSymbol, literalValue);
            case LESS_THAN, LESS_THAN_OR_EQUAL -> estimateExpressionLessThanLiteral(inputStatistics, expressionStatistics, expressionSymbol, literalValue);
            case GREATER_THAN, GREATER_THAN_OR_EQUAL -> estimateExpressionGreaterThanLiteral(inputStatistics, expressionStatistics, expressionSymbol, literalValue);
            case IDENTICAL -> PlanNodeStatsEstimate.unknown();
        };
    }

    private static PlanNodeStatsEstimate estimateExpressionEqualToLiteral(
            PlanNodeStatsEstimate inputStatistics,
            SymbolStatsEstimate expressionStatistics,
            Optional<Symbol> expressionSymbol,
            OptionalDouble literalValue)
    {
        StatisticRange filterRange;
        if (literalValue.isPresent()) {
            filterRange = new StatisticRange(literalValue.getAsDouble(), literalValue.getAsDouble(), 1);
        }
        else {
            filterRange = new StatisticRange(NEGATIVE_INFINITY, POSITIVE_INFINITY, 1);
        }
        return estimateFilterRange(inputStatistics, expressionStatistics, expressionSymbol, filterRange);
    }

    private static PlanNodeStatsEstimate estimateExpressionNotEqualToLiteral(
            PlanNodeStatsEstimate inputStatistics,
            SymbolStatsEstimate expressionStatistics,
            Optional<Symbol> expressionSymbol,
            OptionalDouble literalValue)
    {
        StatisticRange expressionRange = StatisticRange.from(expressionStatistics);

        StatisticRange filterRange;
        if (literalValue.isPresent()) {
            filterRange = new StatisticRange(literalValue.getAsDouble(), literalValue.getAsDouble(), 1);
        }
        else {
            filterRange = new StatisticRange(NEGATIVE_INFINITY, POSITIVE_INFINITY, 1);
        }
        StatisticRange intersectRange = expressionRange.intersect(filterRange);
        double filterFactor = 1 - expressionRange.overlapPercentWith(intersectRange);

        PlanNodeStatsEstimate.Builder estimate = PlanNodeStatsEstimate.buildFrom(inputStatistics);
        estimate.setOutputRowCount(filterFactor * (1 - expressionStatistics.getNullsFraction()) * inputStatistics.getOutputRowCount());
        if (expressionSymbol.isPresent()) {
            SymbolStatsEstimate symbolNewEstimate = buildFrom(expressionStatistics)
                    .setNullsFraction(0.0)
                    .setDistinctValuesCount(max(expressionStatistics.getDistinctValuesCount() - 1, 0))
                    .build();
            estimate.addSymbolStatistics(expressionSymbol.get(), symbolNewEstimate);
        }
        return estimate.build();
    }

    private static PlanNodeStatsEstimate estimateExpressionLessThanLiteral(
            PlanNodeStatsEstimate inputStatistics,
            SymbolStatsEstimate expressionStatistics,
            Optional<Symbol> expressionSymbol,
            OptionalDouble literalValue)
    {
        StatisticRange filterRange = new StatisticRange(NEGATIVE_INFINITY, literalValue.orElse(POSITIVE_INFINITY), NaN);
        return estimateFilterRange(inputStatistics, expressionStatistics, expressionSymbol, filterRange);
    }

    private static PlanNodeStatsEstimate estimateExpressionGreaterThanLiteral(
            PlanNodeStatsEstimate inputStatistics,
            SymbolStatsEstimate expressionStatistics,
            Optional<Symbol> expressionSymbol,
            OptionalDouble literalValue)
    {
        StatisticRange filterRange = new StatisticRange(literalValue.orElse(NEGATIVE_INFINITY), POSITIVE_INFINITY, NaN);
        return estimateFilterRange(inputStatistics, expressionStatistics, expressionSymbol, filterRange);
    }

    private static PlanNodeStatsEstimate estimateFilterRange(
            PlanNodeStatsEstimate inputStatistics,
            SymbolStatsEstimate expressionStatistics,
            Optional<Symbol> expressionSymbol,
            StatisticRange filterRange)
    {
        StatisticRange expressionRange = StatisticRange.from(expressionStatistics);
        StatisticRange intersectRange = expressionRange.intersect(filterRange);

        double filterFactor = expressionRange.overlapPercentWith(intersectRange);

        PlanNodeStatsEstimate estimate = inputStatistics.mapOutputRowCount(rowCount -> filterFactor * (1 - expressionStatistics.getNullsFraction()) * rowCount);
        if (expressionSymbol.isPresent()) {
            SymbolStatsEstimate symbolNewEstimate =
                    SymbolStatsEstimate.builder()
                            .setAverageRowSize(expressionStatistics.getAverageRowSize())
                            .setStatisticsRange(intersectRange)
                            .setNullsFraction(0.0)
                            .build();
            estimate = estimate.mapSymbolColumnStatistics(expressionSymbol.get(), oldStats -> symbolNewEstimate);
        }
        return estimate;
    }

    public static PlanNodeStatsEstimate estimateExpressionToExpressionComparison(
            PlanNodeStatsEstimate inputStatistics,
            SymbolStatsEstimate leftExpressionStatistics,
            Optional<Symbol> leftExpressionSymbol,
            SymbolStatsEstimate rightExpressionStatistics,
            Optional<Symbol> rightExpressionSymbol,
            Comparison.Operator operator)
    {
        return switch (operator) {
            case EQUAL -> estimateExpressionEqualToExpression(inputStatistics, leftExpressionStatistics, leftExpressionSymbol, rightExpressionStatistics, rightExpressionSymbol);
            case NOT_EQUAL -> estimateExpressionNotEqualToExpression(inputStatistics, leftExpressionStatistics, leftExpressionSymbol, rightExpressionStatistics, rightExpressionSymbol);
            case LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL -> estimateExpressionToExpressionInequality(
                    operator,
                    inputStatistics,
                    leftExpressionStatistics,
                    leftExpressionSymbol,
                    rightExpressionStatistics,
                    rightExpressionSymbol);
            case IDENTICAL -> PlanNodeStatsEstimate.unknown();
        };
    }

    private static PlanNodeStatsEstimate estimateExpressionEqualToExpression(
            PlanNodeStatsEstimate inputStatistics,
            SymbolStatsEstimate leftExpressionStatistics,
            Optional<Symbol> leftExpressionSymbol,
            SymbolStatsEstimate rightExpressionStatistics,
            Optional<Symbol> rightExpressionSymbol)
    {
        if (isNaN(leftExpressionStatistics.getDistinctValuesCount()) || isNaN(rightExpressionStatistics.getDistinctValuesCount())) {
            return PlanNodeStatsEstimate.unknown();
        }

        StatisticRange leftExpressionRange = StatisticRange.from(leftExpressionStatistics);
        StatisticRange rightExpressionRange = StatisticRange.from(rightExpressionStatistics);

        StatisticRange intersect = leftExpressionRange.intersect(rightExpressionRange);

        double nullsFilterFactor = (1 - leftExpressionStatistics.getNullsFraction()) * (1 - rightExpressionStatistics.getNullsFraction());
        double leftNdv = leftExpressionRange.getDistinctValuesCount();
        double rightNdv = rightExpressionRange.getDistinctValuesCount();
        double filterFactor = 1.0 / max(leftNdv, rightNdv, 1);
        double retainedNdv = min(leftNdv, rightNdv);

        PlanNodeStatsEstimate.Builder estimate = PlanNodeStatsEstimate.buildFrom(inputStatistics)
                .setOutputRowCount(inputStatistics.getOutputRowCount() * nullsFilterFactor * filterFactor);

        SymbolStatsEstimate equalityStats = SymbolStatsEstimate.builder()
                .setAverageRowSize(averageExcludingNaNs(leftExpressionStatistics.getAverageRowSize(), rightExpressionStatistics.getAverageRowSize()))
                .setNullsFraction(0)
                .setStatisticsRange(intersect)
                .setDistinctValuesCount(retainedNdv)
                .build();

        leftExpressionSymbol.ifPresent(symbol -> estimate.addSymbolStatistics(symbol, equalityStats));
        rightExpressionSymbol.ifPresent(symbol -> estimate.addSymbolStatistics(symbol, equalityStats));

        return estimate.build();
    }

    private static PlanNodeStatsEstimate estimateExpressionNotEqualToExpression(
            PlanNodeStatsEstimate inputStatistics,
            SymbolStatsEstimate leftExpressionStatistics,
            Optional<Symbol> leftExpressionSymbol,
            SymbolStatsEstimate rightExpressionStatistics,
            Optional<Symbol> rightExpressionSymbol)
    {
        double nullsFilterFactor = (1 - leftExpressionStatistics.getNullsFraction()) * (1 - rightExpressionStatistics.getNullsFraction());
        PlanNodeStatsEstimate inputNullsFiltered = inputStatistics.mapOutputRowCount(size -> size * nullsFilterFactor);
        SymbolStatsEstimate leftNullsFiltered = leftExpressionStatistics.mapNullsFraction(nullsFraction -> 0.0);
        SymbolStatsEstimate rightNullsFiltered = rightExpressionStatistics.mapNullsFraction(nullsFraction -> 0.0);
        PlanNodeStatsEstimate equalityStats = estimateExpressionEqualToExpression(
                inputNullsFiltered,
                leftNullsFiltered,
                leftExpressionSymbol,
                rightNullsFiltered,
                rightExpressionSymbol);
        if (equalityStats.isOutputRowCountUnknown()) {
            return PlanNodeStatsEstimate.unknown();
        }

        PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.buildFrom(inputNullsFiltered);
        double equalityFilterFactor = equalityStats.getOutputRowCount() / inputNullsFiltered.getOutputRowCount();
        if (!isFinite(equalityFilterFactor)) {
            equalityFilterFactor = 0.0;
        }
        result.setOutputRowCount(inputNullsFiltered.getOutputRowCount() * (1 - equalityFilterFactor));
        leftExpressionSymbol.ifPresent(symbol -> result.addSymbolStatistics(symbol, leftNullsFiltered));
        rightExpressionSymbol.ifPresent(symbol -> result.addSymbolStatistics(symbol, rightNullsFiltered));
        return result.build();
    }

    private static PlanNodeStatsEstimate estimateExpressionToExpressionInequality(
            Comparison.Operator operator,
            PlanNodeStatsEstimate inputStatistics,
            SymbolStatsEstimate leftExpressionStatistics,
            Optional<Symbol> leftExpressionSymbol,
            SymbolStatsEstimate rightExpressionStatistics,
            Optional<Symbol> rightExpressionSymbol)
    {
        if (leftExpressionStatistics.isUnknown() || rightExpressionStatistics.isUnknown()) {
            return PlanNodeStatsEstimate.unknown();
        }
        if (isNaN(leftExpressionStatistics.getNullsFraction()) && isNaN(rightExpressionStatistics.getNullsFraction())) {
            return PlanNodeStatsEstimate.unknown();
        }
        if (leftExpressionStatistics.statisticRange().isEmpty() || rightExpressionStatistics.statisticRange().isEmpty()) {
            return inputStatistics.mapOutputRowCount(rowCount -> 0.0);
        }

        // We don't know the correlation between NULLs, so we take the max nullsFraction from the expression statistics
        // to make a conservative estimate (nulls are fully correlated) for the NULLs filter factor
        double nullsFilterFactor = 1 - maxExcludeNaN(leftExpressionStatistics.getNullsFraction(), rightExpressionStatistics.getNullsFraction());
        return switch (operator) {
            case LESS_THAN, LESS_THAN_OR_EQUAL -> estimateExpressionLessThanOrEqualToExpression(
                    inputStatistics,
                    leftExpressionStatistics,
                    leftExpressionSymbol,
                    rightExpressionStatistics,
                    rightExpressionSymbol,
                    nullsFilterFactor);
            case GREATER_THAN, GREATER_THAN_OR_EQUAL -> estimateExpressionLessThanOrEqualToExpression(
                    inputStatistics,
                    rightExpressionStatistics,
                    rightExpressionSymbol,
                    leftExpressionStatistics,
                    leftExpressionSymbol,
                    nullsFilterFactor);
            default -> throw new IllegalArgumentException("Unsupported inequality operator " + operator);
        };
    }

    private static PlanNodeStatsEstimate estimateExpressionLessThanOrEqualToExpression(
            PlanNodeStatsEstimate inputStatistics,
            SymbolStatsEstimate leftExpressionStatistics,
            Optional<Symbol> leftExpressionSymbol,
            SymbolStatsEstimate rightExpressionStatistics,
            Optional<Symbol> rightExpressionSymbol,
            double nullsFilterFactor)
    {
        StatisticRange leftRange = StatisticRange.from(leftExpressionStatistics);
        StatisticRange rightRange = StatisticRange.from(rightExpressionStatistics);
        // left is always greater than right, no overlap
        if (leftRange.getLow() > rightRange.getHigh()) {
            return inputStatistics.mapOutputRowCount(rowCount -> 0.0);
        }
        // left is always lesser than right
        if (leftRange.getHigh() < rightRange.getLow()) {
            PlanNodeStatsEstimate.Builder estimate = PlanNodeStatsEstimate.buildFrom(inputStatistics);
            leftExpressionSymbol.ifPresent(symbol -> estimate.addSymbolStatistics(
                    symbol,
                    leftExpressionStatistics.mapNullsFraction(nullsFraction -> 0.0)));
            rightExpressionSymbol.ifPresent(symbol -> estimate.addSymbolStatistics(
                    symbol,
                    rightExpressionStatistics.mapNullsFraction(nullsFraction -> 0.0)));
            return estimate.setOutputRowCount(inputStatistics.getOutputRowCount() * nullsFilterFactor)
                    .build();
        }

        PlanNodeStatsEstimate.Builder estimate = PlanNodeStatsEstimate.buildFrom(inputStatistics);
        double leftOverlappingRangeFraction = leftRange.overlapPercentWith(rightRange);
        double leftAlwaysLessRangeFraction;
        if (leftRange.getLow() < rightRange.getLow()) {
            leftAlwaysLessRangeFraction = min(
                    leftRange.overlapPercentWith(new StatisticRange(leftRange.getLow(), rightRange.getLow(), NaN)),
                    // Prevents expanding NDVs in case range fractions addition goes beyond 1 for infinite ranges
                    1 - leftOverlappingRangeFraction);
        }
        else {
            leftAlwaysLessRangeFraction = 0;
        }
        leftExpressionSymbol.ifPresent(symbol -> estimate.addSymbolStatistics(
                symbol,
                SymbolStatsEstimate.builder()
                        .setLowValue(leftRange.getLow())
                        .setHighValue(minExcludeNaN(leftRange.getHigh(), rightRange.getHigh()))
                        .setAverageRowSize(leftExpressionStatistics.getAverageRowSize())
                        .setDistinctValuesCount(leftExpressionStatistics.getDistinctValuesCount() * (leftAlwaysLessRangeFraction + leftOverlappingRangeFraction))
                        .setNullsFraction(0)
                        .build()));

        double rightOverlappingRangeFraction = rightRange.overlapPercentWith(leftRange);
        double rightAlwaysGreaterRangeFraction;
        if (leftRange.getHigh() < rightRange.getHigh()) {
            rightAlwaysGreaterRangeFraction = min(
                    rightRange.overlapPercentWith(new StatisticRange(leftRange.getHigh(), rightRange.getHigh(), NaN)),
                    // Prevents expanding NDVs in case range fractions addition goes beyond 1 for infinite ranges
                    1 - rightOverlappingRangeFraction);
        }
        else {
            rightAlwaysGreaterRangeFraction = 0;
        }
        rightExpressionSymbol.ifPresent(symbol -> estimate.addSymbolStatistics(
                symbol,
                SymbolStatsEstimate.builder()
                        .setLowValue(maxExcludeNaN(leftRange.getLow(), rightRange.getLow()))
                        .setHighValue(rightRange.getHigh())
                        .setAverageRowSize(rightExpressionStatistics.getAverageRowSize())
                        .setDistinctValuesCount(rightExpressionStatistics.getDistinctValuesCount() * (rightOverlappingRangeFraction + rightAlwaysGreaterRangeFraction))
                        .setNullsFraction(0)
                        .build()));
        double filterFactor =
                // all left range values which are below right range are selected
                leftAlwaysLessRangeFraction +
                        // for pairs in overlapping range, only half of pairs are selected
                        leftOverlappingRangeFraction * rightOverlappingRangeFraction * OVERLAPPING_RANGE_INEQUALITY_FILTER_COEFFICIENT +
                        // all pairs where left value is in overlapping range and right value is above left range are selected
                        leftOverlappingRangeFraction * rightAlwaysGreaterRangeFraction;
        return estimate.setOutputRowCount(inputStatistics.getOutputRowCount() * nullsFilterFactor * filterFactor).build();
    }
}
