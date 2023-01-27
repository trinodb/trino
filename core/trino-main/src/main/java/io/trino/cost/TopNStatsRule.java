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

import io.trino.Session;
import io.trino.matching.Pattern;
import io.trino.spi.connector.SortOrder;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.plan.TopNNode;

import java.util.Optional;

import static io.trino.sql.planner.plan.Patterns.topN;

public class TopNStatsRule
        extends SimpleStatsRule<TopNNode>
{
    private static final int ESTIMATED_PARTIAL_TOPN_INPUT_PER_DRIVER = 1_000_000;
    private static final Pattern<TopNNode> PATTERN = topN();

    public TopNStatsRule(StatsNormalizer normalizer)
    {
        super(normalizer);
    }

    @Override
    public Pattern<TopNNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    protected Optional<PlanNodeStatsEstimate> doCalculate(TopNNode node, StatsProvider statsProvider, Lookup lookup, Session session, TypeProvider types, TableStatsProvider tableStatsProvider)
    {
        PlanNodeStatsEstimate sourceStats = statsProvider.getStats(node.getSource());
        double rowCount = sourceStats.getOutputRowCount();

        /* CreatePartialTopN rule runs after ReorderJoins but before DetermineJoinDistributionType and DetermineSemiJoinDistributionType.
         * Therefore, it is useful to provide estimates for partial and final topN nodes.
         * If, for example, the partial topN is part of a source stage, then it's overall output will depend on the number of splits that get created at runtime,
         * and that's not known in advance. We populate a rough estimate for partial topN by assuming an input of 1 million rows per driver.
         */
        if (node.getStep() == TopNNode.Step.PARTIAL) {
            double estimatedOutputRowCount = Math.max(rowCount / ESTIMATED_PARTIAL_TOPN_INPUT_PER_DRIVER, 1) * node.getCount();
            return Optional.of(PlanNodeStatsEstimate.buildFrom(sourceStats)
                    .setOutputRowCount(Math.min(estimatedOutputRowCount, rowCount))
                    .build());
        }

        if (rowCount <= node.getCount()) {
            return Optional.of(sourceStats);
        }

        long limitCount = node.getCount();

        PlanNodeStatsEstimate resultStats = PlanNodeStatsEstimate.buildFrom(sourceStats)
                .setOutputRowCount(limitCount)
                .build();
        if (limitCount == 0) {
            return Optional.of(resultStats);
        }
        // augment null fraction estimation for first ORDER BY symbol
        Symbol firstOrderSymbol = node.getOrderingScheme().getOrderBy().get(0); // Assuming not empty list
        SortOrder sortOrder = node.getOrderingScheme().getOrdering(firstOrderSymbol);

        resultStats = resultStats.mapSymbolColumnStatistics(firstOrderSymbol, symbolStats -> {
            SymbolStatsEstimate.Builder newStats = SymbolStatsEstimate.buildFrom(symbolStats);
            double nullCount = rowCount * symbolStats.getNullsFraction();

            if (sortOrder.isNullsFirst()) {
                if (nullCount > limitCount) {
                    newStats.setNullsFraction(1.0);
                }
                else {
                    newStats.setNullsFraction(nullCount / limitCount);
                }
            }
            else {
                double nonNullCount = (rowCount - nullCount);
                if (nonNullCount > limitCount) {
                    newStats.setNullsFraction(0.0);
                }
                else {
                    newStats.setNullsFraction((limitCount - nonNullCount) / limitCount);
                }
            }
            return newStats.build();
        });

        // TopN actually limits (or when there was no row count estimated for source)
        return Optional.of(resultStats);
    }
}
