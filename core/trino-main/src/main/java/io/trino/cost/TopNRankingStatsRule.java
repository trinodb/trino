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

import io.trino.matching.Pattern;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.TopNRankingNode;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.plan.Patterns.topNRanking;
import static io.trino.sql.planner.plan.TopNRankingNode.RankingType.RANK;
import static io.trino.sql.planner.plan.TopNRankingNode.RankingType.ROW_NUMBER;
import static java.lang.Double.isNaN;
import static java.lang.Math.ceil;
import static java.lang.Math.min;
import static java.lang.Math.pow;

/**
 * Stats rule for {@link TopNRankingNode}.
 */
public class TopNRankingStatsRule
        extends SimpleStatsRule<TopNRankingNode>
{
    /**
     * The independence factor is used to decrease the distinct count of second and subsequent symbols. The
     * lower it is, the more correlated the symbols are. The value of 0.9 is chosen arbitrarily.
     */
    private static final double INDEPENDENCE_FACTOR = 0.9;
    private static final Pattern<TopNRankingNode> PATTERN = topNRanking();

    public TopNRankingStatsRule(StatsNormalizer normalizer)
    {
        super(normalizer);
    }

    @Override
    public Pattern<TopNRankingNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<PlanNodeStatsEstimate> doCalculate(TopNRankingNode node, StatsCalculator.Context context)
    {
        PlanNodeStatsEstimate sourceStats = context.statsProvider().getStats(node.getSource());
        if (sourceStats.isOutputRowCountUnknown()) {
            return Optional.empty();
        }

        if (node.isPartial()) {
            // Pessimistic assumption of no reduction from PARTIAL TopNRanking, forwarding of the source
            // statistics. This makes the CBO estimates in the EXPLAIN plan output easier to understand,
            // even though partial aggregations are added after the CBO rules have been run.            r
            return Optional.of(sourceStats);
        }

        double sourceRowCount = sourceStats.getOutputRowCount();
        // If sourceRowCount is 0, then the outputRowCount will also be 0.
        if (sourceRowCount == 0) {
            return Optional.of(PlanNodeStatsEstimate.buildFrom(sourceStats)
                    .addSymbolStatistics(node.getRankingSymbol(), SymbolStatsEstimate.zero())
                    .build());
        }

        double partitionCount = estimateCorrelatedPartitionCount(node.getPartitionBy(), sourceStats);
        partitionCount = min(sourceRowCount, partitionCount);
        if (isNaN(partitionCount)) {
            // if partition count is NaN, we can't estimate anything, so we will return the source stats which is
            // better than nothing.
            return Optional.of(sourceStats);
        }

        // assuming no skew
        double averageRowsPerPartition = sourceRowCount / partitionCount;

        double topRowsPerPartition;
        double rankDistinctCount;
        if (node.getRankingType() == ROW_NUMBER) {
            // In case of ROW_NUMBER, each row gets a unique incremental rank. Therefore, the number of rows per
            // partition can be at most the max rank per partition.
            topRowsPerPartition = min(averageRowsPerPartition, node.getMaxRankingPerPartition());
            // The distinct count for ROW_NUMBER symbol should be equal to the number of rows per partition.
            rankDistinctCount = topRowsPerPartition;
        }
        else if (node.getRankingType() == RANK) {
            // In case of RANK, each unique row gets a unique rank. However, unlike DENSE_RANK, the rank is not
            // incremental.
            // For example, ordering by ROW1, ROW2:
            // ROW1   ROW2   RANK
            // A      A      1
            // A      A      1
            // B      B      3
            // B      B      3
            // C      C      5
            // C      C      5
            // In the above example, the rank is not incremental. Therefore, the maximum number of rows per partition
            // should be calculated using distinct count (assuming uniform distribution). In the above case
            // rowsPerDistinctValue is 2 based on distinct count. Hence, the number of rows per partition for max rank
            // 3 can be calculated using
            // ceil(maxRank / rowsPerDistinctValue) * rowsPerDistinctValue = 4 => ceil(3/2) * 2 = 4 rows.
            // Whereas, the distinct count for rank symbol should be 2 i.e. ceil(maxRank / rowsPerDistinctValue).

            // We assume order by columns are uncorrelated to partition columns, hence order by
            // distinct rows should be equally spread among partitions. Additionally, this ensures that the
            // rowsPerDistinctValue is always >= 1.0, otherwise we will underestimate the rows per partition.
            double distinctCount = min(
                    estimateCorrelatedPartitionCount(node.getOrderingScheme().orderBy(), sourceStats),
                    averageRowsPerPartition);
            double rowsPerDistinctValue = averageRowsPerPartition / distinctCount;
            rankDistinctCount = ceil(node.getMaxRankingPerPartition() / rowsPerDistinctValue);
            topRowsPerPartition = min(averageRowsPerPartition, rankDistinctCount * rowsPerDistinctValue);
        }
        else {
            // In case of DENSE_RANK, each unique row gets a unique rank. The rank is incremental.
            // For example, ordering by ROW1, ROW2:
            // ROW1   ROW2   DENSE_RANK
            // A      A      1
            // A      A      1
            // B      B      2
            // B      B      2
            // C      C      3
            // C      C      3
            // In the above example, the rank is incremental. Therefore, the maximum number of rows per partition can be
            // at most (maxRank * rowsPerDistinctValue). In the above case rowsPerDistinctValue is 2 based on distinct
            // count. Hence, the number of rows per partition for max rank 2 should be 4. Whereas, the distinct count
            // for rank symbol should be 2 i.e. maxRank.

            // We assume order by columns are uncorrelated to partition columns, hence order by
            // distinct rows should be equally spread among partitions. Additionally, this ensures that the
            // rowsPerDistinctValue is always >= 1.0, otherwise we will underestimate the rows per partition.
            double distinctCount = min(
                    estimateCorrelatedPartitionCount(node.getOrderingScheme().orderBy(), sourceStats),
                    averageRowsPerPartition);
            double rowsPerDistinctValue = averageRowsPerPartition / distinctCount;
            topRowsPerPartition = min(averageRowsPerPartition, node.getMaxRankingPerPartition() * rowsPerDistinctValue);
            rankDistinctCount = min(distinctCount, node.getMaxRankingPerPartition());
        }

        if (isNaN(topRowsPerPartition)) {
            topRowsPerPartition = averageRowsPerPartition;
        }

        if (isNaN(rankDistinctCount)) {
            rankDistinctCount = topRowsPerPartition;
        }

        PlanNodeStatsEstimate.Builder estimateBuilder = PlanNodeStatsEstimate.buildFrom(sourceStats);
        adjustOrderBySymbolDistinctCount(
                node.getOrderingScheme().orderBy(),
                partitionCount,
                topRowsPerPartition,
                averageRowsPerPartition,
                sourceStats,
                estimateBuilder);

        double outputRowsCount = partitionCount * topRowsPerPartition;
        return Optional.of(estimateBuilder
                .setOutputRowCount(outputRowsCount)
                .addSymbolStatistics(node.getRankingSymbol(), SymbolStatsEstimate.builder()
                        .setLowValue(1)
                        // maxRankingPerPartition is the maximum rank per partition. Therefore, in the worse case
                        // scenario the rank symbol can have maxRankingPerPartition as the high value.
                        .setHighValue(node.getMaxRankingPerPartition())
                        .setDistinctValuesCount(rankDistinctCount)
                        .setNullsFraction(0.0)
                        .setAverageRowSize(BIGINT.getFixedSize())
                        .build())
                .build());
    }

    private static void adjustOrderBySymbolDistinctCount(
            List<Symbol> orderBy,
            double partitionCount,
            double topRowsPerPartition,
            double averageRowsPerPartition,
            PlanNodeStatsEstimate sourceStats,
            PlanNodeStatsEstimate.Builder estimateBuilder)
    {
        verify(!orderBy.isEmpty(), "Order by symbols should not be empty for TopNRankingNode.");
        Symbol firstSortSymbol = orderBy.getFirst();
        SymbolStatsEstimate symbolStats = sourceStats.getSymbolStatistics(firstSortSymbol);
        // We assume that the order by columns are uncorrelated to partition columns, hence order by distinct rows
        // should be equally spread among partitions.
        double distinctCountPerPartition = min(symbolStats.getDistinctValuesCount(), averageRowsPerPartition);
        // Consider that order by columns are COL1, COL2 for the below single partition:
        // COL1   COL2
        // A      1
        // A      2
        // A      3
        // A      4
        // B      5
        // B      6
        // B      7
        // B      8
        // The max rows per partition is 8, and for rank 4, the top rows per partition is 4.
        // So, in that case, the new distinct count for COL1 should be 1. We can calculate the new distinct count
        // per partition using the formula:
        // (symbolStats.getDistinctValuesCount() / averageRowsPerPartition) * topRowsPerPartition => (2 / 8) * 4 = 1
        // Additionally, it's quite hard to argue about COL2, thus we will not change the distinct count for COL2.
        // It'll automatically be handled by the StatsNormalizer based on outputRowCount.
        double adjustedDistinctCountPerPartition = ceil((distinctCountPerPartition / averageRowsPerPartition) * topRowsPerPartition);
        if (isNaN(adjustedDistinctCountPerPartition)) {
            return;
        }
        // It is not accurate to simply set the distinct count to adjustedDistinctCountPerPartition, because in practice the
        // distinct values are not uniformly distributed across partitions. Therefore, we set the distinct
        // count to the minimum of the current distinct count and the adjustedDistinctCountPerPartition * partitionCount
        // (i.e. the worst case scenario).
        double newDistinctCount = min(symbolStats.getDistinctValuesCount(), adjustedDistinctCountPerPartition * partitionCount);
        SymbolStatsEstimate newFirstSortSymbolStats = SymbolStatsEstimate.buildFrom(symbolStats)
                .setDistinctValuesCount(newDistinctCount)
                .build();
        estimateBuilder.addSymbolStatistics(firstSortSymbol, newFirstSortSymbolStats);
    }

    private static double estimateCorrelatedPartitionCount(List<Symbol> partitionBy, PlanNodeStatsEstimate sourceStats)
    {
        double distinctCount = 1;
        double combinedIndependenceFactor = 1;
        for (Symbol partitionSymbol : partitionBy) {
            SymbolStatsEstimate symbolStatistics = sourceStats.getSymbolStatistics(partitionSymbol);
            int nullRow = (symbolStatistics.getNullsFraction() == 0.0) ? 0 : 1;
            // Decrease the distinct count of second and subsequent symbols with exponential factor to
            // account for correlation between symbols. However, currently this exponential factor is constant
            // for all symbols. Maybe this can be improved in the future.
            distinctCount *= pow(symbolStatistics.getDistinctValuesCount() + nullRow, combinedIndependenceFactor);
            // independence factor should be applied only for the second and subsequent symbols
            combinedIndependenceFactor *= INDEPENDENCE_FACTOR;
        }
        return distinctCount;
    }
}
