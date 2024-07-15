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

import io.trino.cost.StatsCalculator.Context;
import io.trino.execution.scheduler.OutputDataSizeEstimate;
import io.trino.matching.Pattern;
import io.trino.spi.type.FixedWidthType;
import io.trino.spi.type.Type;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.RemoteSourceNode;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.cost.PlanNodeStatsEstimateMath.addStatsAndMaxDistinctValues;
import static io.trino.execution.scheduler.faulttolerant.OutputStatsEstimator.OutputStatsEstimateResult;
import static io.trino.sql.planner.plan.Patterns.remoteSourceNode;
import static io.trino.util.MoreMath.firstNonNaN;
import static java.lang.Double.NaN;
import static java.lang.Double.isNaN;

public class RemoteSourceStatsRule
        extends SimpleStatsRule<RemoteSourceNode>
{
    private static final Pattern<RemoteSourceNode> PATTERN = remoteSourceNode();

    public RemoteSourceStatsRule(StatsNormalizer normalizer)
    {
        super(normalizer);
    }

    @Override
    public Pattern<RemoteSourceNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    protected Optional<PlanNodeStatsEstimate> doCalculate(RemoteSourceNode node, Context context)
    {
        Optional<PlanNodeStatsEstimate> estimate = Optional.empty();
        RuntimeInfoProvider runtimeInfoProvider = context.runtimeInfoProvider();

        for (int i = 0; i < node.getSourceFragmentIds().size(); i++) {
            PlanFragmentId planFragmentId = node.getSourceFragmentIds().get(i);
            OutputStatsEstimateResult stageRuntimeStats = runtimeInfoProvider.getRuntimeOutputStats(planFragmentId);

            PlanNodeStatsEstimate stageEstimatedStats = getEstimatedStats(runtimeInfoProvider, context.statsProvider(), planFragmentId);
            PlanNodeStatsEstimate adjustedStageStats = adjustStats(
                    node.getOutputSymbols(),
                    stageRuntimeStats,
                    stageEstimatedStats);

            estimate = estimate
                    .map(planNodeStatsEstimate -> addStatsAndMaxDistinctValues(planNodeStatsEstimate, adjustedStageStats))
                    .or(() -> Optional.of(adjustedStageStats));
        }

        verify(estimate.isPresent());
        return estimate;
    }

    private PlanNodeStatsEstimate getEstimatedStats(
            RuntimeInfoProvider runtimeInfoProvider,
            StatsProvider statsProvider,
            PlanFragmentId fragmentId)
    {
        PlanFragment fragment = runtimeInfoProvider.getPlanFragment(fragmentId);
        PlanNode fragmentRoot = fragment.getRoot();
        PlanNodeStatsEstimate estimate = fragment.getStatsAndCosts().getStats().get(fragmentRoot.getId());
        // We will not have stats for the root node in a PlanFragment if collect_plan_statistics_for_all_queries
        // is disabled and query isn't an explain analyze.
        if (estimate != null && !estimate.isOutputRowCountUnknown()) {
            return estimate;
        }
        return statsProvider.getStats(fragmentRoot);
    }

    private PlanNodeStatsEstimate adjustStats(
            List<Symbol> outputs,
            OutputStatsEstimateResult runtimeStats,
            PlanNodeStatsEstimate estimateStats)
    {
        if (runtimeStats.isUnknown()) {
            return estimateStats;
        }

        // We prefer runtime stats over estimated stats, because runtime stats are more accurate.
        OutputDataSizeEstimate outputDataSizeEstimate = runtimeStats.outputDataSizeEstimate();
        PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(runtimeStats.outputRowCountEstimate());

        double fixedWidthTypeSize = 0;
        double variableTypeValuesCount = 0;

        for (Symbol outputSymbol : outputs) {
            Type type = outputSymbol.type();
            SymbolStatsEstimate symbolStatistics = estimateStats.getSymbolStatistics(outputSymbol);
            double nullsFraction = firstNonNaN(symbolStatistics.getNullsFraction(), 0d);
            double numberOfNonNullRows = runtimeStats.outputRowCountEstimate() * (1.0 - nullsFraction);

            if (type instanceof FixedWidthType fixedType) {
                fixedWidthTypeSize += numberOfNonNullRows * fixedType.getFixedSize();
            }
            else {
                variableTypeValuesCount += numberOfNonNullRows;
            }
        }

        double runtimeOutputDataSize = outputDataSizeEstimate.getTotalSizeInBytes();
        double variableTypeValueAverageSize = NaN;
        if (variableTypeValuesCount > 0 && runtimeOutputDataSize > fixedWidthTypeSize) {
            variableTypeValueAverageSize = (runtimeOutputDataSize - fixedWidthTypeSize) / variableTypeValuesCount;
        }

        for (Symbol outputSymbol : outputs) {
            SymbolStatsEstimate symbolStatistics = estimateStats.getSymbolStatistics(outputSymbol);
            Type type = outputSymbol.type();
            if (!(isNaN(variableTypeValueAverageSize) || type instanceof FixedWidthType)) {
                symbolStatistics = SymbolStatsEstimate.buildFrom(symbolStatistics)
                        .setAverageRowSize(variableTypeValueAverageSize)
                        .build();
            }
            result.addSymbolStatistics(outputSymbol, symbolStatistics);
        }

        return result.build();
    }
}
