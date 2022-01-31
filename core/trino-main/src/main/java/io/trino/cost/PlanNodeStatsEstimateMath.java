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
import io.trino.util.MoreMath;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.cost.FilterStatsCalculator.UNKNOWN_FILTER_COEFFICIENT;
import static java.lang.Double.NaN;
import static java.lang.Double.isNaN;
import static java.lang.Double.max;
import static java.lang.Double.min;
import static java.util.Comparator.comparingDouble;
import static java.util.stream.Stream.concat;

public final class PlanNodeStatsEstimateMath
{
    private PlanNodeStatsEstimateMath() {}

    /**
     * Subtracts subset stats from supersets stats.
     * It is assumed that each NDV from subset has a matching NDV in superset.
     */
    public static PlanNodeStatsEstimate subtractSubsetStats(PlanNodeStatsEstimate superset, PlanNodeStatsEstimate subset)
    {
        if (superset.isOutputRowCountUnknown() || subset.isOutputRowCountUnknown()) {
            return PlanNodeStatsEstimate.unknown();
        }

        double supersetRowCount = superset.getOutputRowCount();
        double subsetRowCount = subset.getOutputRowCount();
        double outputRowCount = max(supersetRowCount - subsetRowCount, 0);

        // everything will be filtered out after applying negation
        if (outputRowCount == 0) {
            return createZeroStats(superset);
        }

        PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.builder();
        result.setOutputRowCount(outputRowCount);

        superset.getSymbolsWithKnownStatistics().forEach(symbol -> {
            SymbolStatsEstimate supersetSymbolStats = superset.getSymbolStatistics(symbol);
            SymbolStatsEstimate subsetSymbolStats = subset.getSymbolStatistics(symbol);

            SymbolStatsEstimate.Builder newSymbolStats = SymbolStatsEstimate.builder();

            // for simplicity keep the average row size the same as in the input
            // in most cases the average row size doesn't change after applying filters
            newSymbolStats.setAverageRowSize(supersetSymbolStats.getAverageRowSize());

            // nullsCount
            double supersetNullsCount = supersetSymbolStats.getNullsFraction() * supersetRowCount;
            double subsetNullsCount = subsetSymbolStats.getNullsFraction() * subsetRowCount;
            double newNullsCount = max(supersetNullsCount - subsetNullsCount, 0);
            newSymbolStats.setNullsFraction(min(newNullsCount, outputRowCount) / outputRowCount);

            // distinctValuesCount
            double supersetDistinctValues = supersetSymbolStats.getDistinctValuesCount();
            double subsetDistinctValues = subsetSymbolStats.getDistinctValuesCount();
            double newDistinctValuesCount;
            if (isNaN(supersetDistinctValues) || isNaN(subsetDistinctValues)) {
                newDistinctValuesCount = NaN;
            }
            else if (supersetDistinctValues == 0) {
                newDistinctValuesCount = 0;
            }
            else if (subsetDistinctValues == 0) {
                newDistinctValuesCount = supersetDistinctValues;
            }
            else {
                double supersetNonNullsCount = supersetRowCount - supersetNullsCount;
                double subsetNonNullsCount = subsetRowCount - subsetNullsCount;
                double supersetValuesPerDistinctValue = supersetNonNullsCount / supersetDistinctValues;
                double subsetValuesPerDistinctValue = subsetNonNullsCount / subsetDistinctValues;
                if (supersetValuesPerDistinctValue <= subsetValuesPerDistinctValue) {
                    newDistinctValuesCount = max(supersetDistinctValues - subsetDistinctValues, 0);
                }
                else {
                    newDistinctValuesCount = supersetDistinctValues;
                }
            }
            newSymbolStats.setDistinctValuesCount(newDistinctValuesCount);

            // range
            newSymbolStats.setLowValue(supersetSymbolStats.getLowValue());
            newSymbolStats.setHighValue(supersetSymbolStats.getHighValue());

            result.addSymbolStatistics(symbol, newSymbolStats.build());
        });

        return result.build();
    }

    public static PlanNodeStatsEstimate capStats(PlanNodeStatsEstimate stats, PlanNodeStatsEstimate cap)
    {
        if (stats.isOutputRowCountUnknown() || cap.isOutputRowCountUnknown()) {
            return PlanNodeStatsEstimate.unknown();
        }

        PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.builder();
        double cappedRowCount = min(stats.getOutputRowCount(), cap.getOutputRowCount());
        result.setOutputRowCount(cappedRowCount);

        stats.getSymbolsWithKnownStatistics().forEach(symbol -> {
            SymbolStatsEstimate symbolStats = stats.getSymbolStatistics(symbol);
            SymbolStatsEstimate capSymbolStats = cap.getSymbolStatistics(symbol);

            SymbolStatsEstimate.Builder newSymbolStats = SymbolStatsEstimate.builder();

            // for simplicity keep the average row size the same as in the input
            // in most cases the average row size doesn't change after applying filters
            newSymbolStats.setAverageRowSize(symbolStats.getAverageRowSize());

            newSymbolStats.setDistinctValuesCount(min(symbolStats.getDistinctValuesCount(), capSymbolStats.getDistinctValuesCount()));
            newSymbolStats.setLowValue(max(symbolStats.getLowValue(), capSymbolStats.getLowValue()));
            newSymbolStats.setHighValue(min(symbolStats.getHighValue(), capSymbolStats.getHighValue()));

            double numberOfNulls = stats.getOutputRowCount() * symbolStats.getNullsFraction();
            double capNumberOfNulls = cap.getOutputRowCount() * capSymbolStats.getNullsFraction();
            double cappedNumberOfNulls = min(numberOfNulls, capNumberOfNulls);
            double cappedNullsFraction = cappedRowCount == 0 ? 1 : cappedNumberOfNulls / cappedRowCount;
            newSymbolStats.setNullsFraction(cappedNullsFraction);

            result.addSymbolStatistics(symbol, newSymbolStats.build());
        });

        return result.build();
    }

    public static Map<Symbol, SymbolStatsEstimate> intersectCorrelatedStats(List<PlanNodeStatsEstimate> estimates)
    {
        checkArgument(!estimates.isEmpty(), "estimates is empty");
        if (estimates.size() == 1) {
            return estimates.get(0).getSymbolStatistics();
        }
        PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.builder();
        // Update statistic range for symbols
        estimates.stream().flatMap(estimate -> estimate.getSymbolsWithKnownStatistics().stream())
                .distinct()
                .forEach(symbol -> {
                    List<SymbolStatsEstimate> symbolStatsEstimates = estimates.stream()
                            .map(estimate -> estimate.getSymbolStatistics(symbol))
                            .collect(toImmutableList());

                    StatisticRange intersect = symbolStatsEstimates.stream()
                            .map(StatisticRange::from)
                            .reduce(StatisticRange::intersect)
                            .orElseThrow();

                    // intersectCorrelatedStats should try produce stats as if filters are applied in sequence.
                    // Using min works for filters like (a > 10 AND b < 10), but won't work for
                    // (a > 10 AND b IS NULL). However, former case is more common.
                    double nullsFraction = symbolStatsEstimates.stream()
                            .map(SymbolStatsEstimate::getNullsFraction)
                            .reduce(MoreMath::minExcludeNaN)
                            .orElseThrow();

                    double averageRowSize = symbolStatsEstimates.stream()
                            .map(SymbolStatsEstimate::getAverageRowSize)
                            .reduce(MoreMath::averageExcludingNaNs)
                            .orElseThrow();

                    result.addSymbolStatistics(symbol, SymbolStatsEstimate.builder()
                            .setStatisticsRange(intersect)
                            .setNullsFraction(nullsFraction)
                            .setAverageRowSize(averageRowSize)
                            .build());
                });
        return result.build().getSymbolStatistics();
    }

    public static double estimateCorrelatedConjunctionRowCount(
            PlanNodeStatsEstimate input,
            List<PlanNodeStatsEstimate> estimates,
            double independenceFactor)
    {
        checkArgument(!estimates.isEmpty(), "estimates is empty");
        if (input.isOutputRowCountUnknown() || input.getOutputRowCount() == 0) {
            return input.getOutputRowCount();
        }
        List<PlanNodeStatsEstimate> knownSortedEstimates = estimates.stream()
                .filter(estimateInfo -> !estimateInfo.isOutputRowCountUnknown())
                .sorted(comparingDouble(PlanNodeStatsEstimate::getOutputRowCount))
                .collect(toImmutableList());
        if (knownSortedEstimates.isEmpty()) {
            return NaN;
        }

        PlanNodeStatsEstimate combinedEstimate = knownSortedEstimates.get(0);
        double combinedSelectivity = combinedEstimate.getOutputRowCount() / input.getOutputRowCount();
        double combinedIndependenceFactor = 1.0;
        // For independenceFactor = 0.75 and terms t1, t2, t3
        // Combined selectivity = (t1 selectivity) * ((t2 selectivity) ^ 0.75) * ((t3 selectivity) ^ (0.75 * 0.75))
        // independenceFactor = 1 implies the terms are assumed to have no correlation and their selectivities are multiplied without scaling.
        // independenceFactor = 0 implies the terms are assumed to be fully correlated and only the most selective term drives the selectivity.
        for (int i = 1; i < knownSortedEstimates.size(); i++) {
            PlanNodeStatsEstimate term = knownSortedEstimates.get(i);
            combinedIndependenceFactor *= independenceFactor;
            combinedSelectivity *= Math.pow(term.getOutputRowCount() / input.getOutputRowCount(), combinedIndependenceFactor);
        }
        double outputRowCount = input.getOutputRowCount() * combinedSelectivity;
        // TODO use UNKNOWN_FILTER_COEFFICIENT only when default-filter-factor is enabled
        boolean hasUnestimatedTerm = estimates.stream().anyMatch(PlanNodeStatsEstimate::isOutputRowCountUnknown);
        return hasUnestimatedTerm ? outputRowCount * UNKNOWN_FILTER_COEFFICIENT : outputRowCount;
    }

    private static PlanNodeStatsEstimate createZeroStats(PlanNodeStatsEstimate stats)
    {
        PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.builder();
        result.setOutputRowCount(0);
        stats.getSymbolsWithKnownStatistics().forEach(symbol -> result.addSymbolStatistics(symbol, SymbolStatsEstimate.zero()));
        return result.build();
    }

    @FunctionalInterface
    private interface RangeAdditionStrategy
    {
        StatisticRange add(StatisticRange leftRange, StatisticRange rightRange);
    }

    public static PlanNodeStatsEstimate addStatsAndSumDistinctValues(PlanNodeStatsEstimate left, PlanNodeStatsEstimate right)
    {
        return addStats(left, right, StatisticRange::addAndSumDistinctValues);
    }

    public static PlanNodeStatsEstimate addStatsAndMaxDistinctValues(PlanNodeStatsEstimate left, PlanNodeStatsEstimate right)
    {
        return addStats(left, right, StatisticRange::addAndMaxDistinctValues);
    }

    public static PlanNodeStatsEstimate addStatsAndCollapseDistinctValues(PlanNodeStatsEstimate left, PlanNodeStatsEstimate right)
    {
        return addStats(left, right, StatisticRange::addAndCollapseDistinctValues);
    }

    private static PlanNodeStatsEstimate addStats(PlanNodeStatsEstimate left, PlanNodeStatsEstimate right, RangeAdditionStrategy strategy)
    {
        if (left.isOutputRowCountUnknown() || right.isOutputRowCountUnknown()) {
            return PlanNodeStatsEstimate.unknown();
        }

        PlanNodeStatsEstimate.Builder statsBuilder = PlanNodeStatsEstimate.builder();
        double newRowCount = left.getOutputRowCount() + right.getOutputRowCount();

        concat(left.getSymbolsWithKnownStatistics().stream(), right.getSymbolsWithKnownStatistics().stream())
                .distinct()
                .forEach(symbol -> {
                    SymbolStatsEstimate symbolStats = SymbolStatsEstimate.zero();
                    if (newRowCount > 0) {
                        symbolStats = addColumnStats(
                                left.getSymbolStatistics(symbol),
                                left.getOutputRowCount(),
                                right.getSymbolStatistics(symbol),
                                right.getOutputRowCount(),
                                newRowCount,
                                strategy);
                    }
                    statsBuilder.addSymbolStatistics(symbol, symbolStats);
                });

        return statsBuilder.setOutputRowCount(newRowCount).build();
    }

    private static SymbolStatsEstimate addColumnStats(SymbolStatsEstimate leftStats, double leftRows, SymbolStatsEstimate rightStats, double rightRows, double newRowCount, RangeAdditionStrategy strategy)
    {
        checkArgument(newRowCount > 0, "newRowCount must be greater than zero");

        StatisticRange leftRange = StatisticRange.from(leftStats);
        StatisticRange rightRange = StatisticRange.from(rightStats);

        StatisticRange sum = strategy.add(leftRange, rightRange);
        double nullsCountRight = rightStats.getNullsFraction() * rightRows;
        double nullsCountLeft = leftStats.getNullsFraction() * leftRows;
        double totalSizeLeft = (leftRows - nullsCountLeft) * leftStats.getAverageRowSize();
        double totalSizeRight = (rightRows - nullsCountRight) * rightStats.getAverageRowSize();
        double newNullsFraction = (nullsCountLeft + nullsCountRight) / newRowCount;
        double newNonNullsRowCount = newRowCount * (1.0 - newNullsFraction);

        // FIXME, weights to average. left and right should be equal in most cases anyway
        double newAverageRowSize = newNonNullsRowCount == 0 ? 0 : ((totalSizeLeft + totalSizeRight) / newNonNullsRowCount);

        return SymbolStatsEstimate.builder()
                .setStatisticsRange(sum)
                .setAverageRowSize(newAverageRowSize)
                .setNullsFraction(newNullsFraction)
                .build();
    }
}
