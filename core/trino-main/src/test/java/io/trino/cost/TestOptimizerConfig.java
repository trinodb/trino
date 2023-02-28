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

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.sql.planner.OptimizerConfig;
import io.trino.sql.planner.OptimizerConfig.JoinDistributionType;
import io.trino.sql.planner.OptimizerConfig.JoinReorderingStrategy;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.sql.planner.OptimizerConfig.JoinDistributionType.BROADCAST;
import static io.trino.sql.planner.OptimizerConfig.JoinReorderingStrategy.NONE;
import static io.trino.sql.planner.OptimizerConfig.MarkDistinctStrategy.ALWAYS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestOptimizerConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(OptimizerConfig.class)
                .setCpuCostWeight(75)
                .setMemoryCostWeight(10)
                .setNetworkCostWeight(15)
                .setJoinMaxBroadcastTableSize(DataSize.of(100, MEGABYTE))
                .setJoinDistributionType(JoinDistributionType.AUTOMATIC)
                .setJoinMultiClauseIndependenceFactor(0.25)
                .setJoinReorderingStrategy(JoinReorderingStrategy.AUTOMATIC)
                .setMaxReorderedJoins(9)
                .setColocatedJoinsEnabled(false)
                .setSpatialJoinsEnabled(true)
                .setUsePreferredWritePartitioning(true)
                .setPreferredWritePartitioningMinNumberOfPartitions(50)
                .setEnableStatsCalculator(true)
                .setStatisticsPrecalculationForPushdownEnabled(true)
                .setCollectPlanStatisticsForAllQueries(false)
                .setIgnoreStatsCalculatorFailures(true)
                .setDefaultFilterFactorEnabled(false)
                .setFilterConjunctionIndependenceFactor(0.75)
                .setNonEstimatablePredicateApproximationEnabled(true)
                .setOptimizeMetadataQueries(false)
                .setOptimizeHashGeneration(true)
                .setPushTableWriteThroughUnion(true)
                .setDictionaryAggregation(false)
                .setOptimizeMixedDistinctAggregations(false)
                .setIterativeOptimizerTimeout(new Duration(3, MINUTES))
                .setEnableForcedExchangeBelowGroupId(true)
                .setEnableIntermediateAggregations(false)
                .setPushAggregationThroughOuterJoin(true)
                .setPushPartialAggregationThoughJoin(false)
                .setPreAggregateCaseAggregationsEnabled(true)
                .setMarkDistinctStrategy(null)
                .setPreferPartialAggregation(true)
                .setOptimizeTopNRanking(true)
                .setDistributedSortEnabled(true)
                .setSkipRedundantSort(true)
                .setComplexExpressionPushdownEnabled(true)
                .setPredicatePushdownUseTableProperties(true)
                .setIgnoreDownstreamPreferences(false)
                .setRewriteFilteringSemiJoinToInnerJoin(true)
                .setOptimizeDuplicateInsensitiveJoins(true)
                .setUseLegacyWindowFilterPushdown(false)
                .setUseTableScanNodePartitioning(true)
                .setTableScanNodePartitioningMinBucketToTaskRatio(0.5)
                .setMergeProjectWithValues(true)
                .setForceSingleNodeOutput(false)
                .setAdaptivePartialAggregationEnabled(true)
                .setAdaptivePartialAggregationMinRows(100_000)
                .setAdaptivePartialAggregationUniqueRowsRatioThreshold(0.8)
                .setJoinPartitionedBuildMinRowCount(1_000_000)
                .setMinInputSizePerTask(DataSize.of(5, GIGABYTE))
                .setMinInputRowsPerTask(10_000_000L)
                .setUseExactPartitioning(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("cpu-cost-weight", "0.4")
                .put("memory-cost-weight", "0.3")
                .put("network-cost-weight", "0.2")
                .put("enable-stats-calculator", "false")
                .put("statistics-precalculation-for-pushdown.enabled", "false")
                .put("collect-plan-statistics-for-all-queries", "true")
                .put("optimizer.ignore-stats-calculator-failures", "false")
                .put("optimizer.default-filter-factor-enabled", "true")
                .put("optimizer.filter-conjunction-independence-factor", "1.0")
                .put("optimizer.non-estimatable-predicate-approximation.enabled", "false")
                .put("join-distribution-type", "BROADCAST")
                .put("join-max-broadcast-table-size", "42GB")
                .put("optimizer.join-multi-clause-independence-factor", "0.75")
                .put("optimizer.join-reordering-strategy", "NONE")
                .put("optimizer.max-reordered-joins", "5")
                .put("iterative-optimizer-timeout", "10s")
                .put("enable-forced-exchange-below-group-id", "false")
                .put("colocated-joins-enabled", "true")
                .put("spatial-joins-enabled", "false")
                .put("distributed-sort", "false")
                .put("use-preferred-write-partitioning", "false")
                .put("preferred-write-partitioning-min-number-of-partitions", "10")
                .put("optimizer.optimize-metadata-queries", "true")
                .put("optimizer.optimize-hash-generation", "false")
                .put("optimizer.optimize-mixed-distinct-aggregations", "true")
                .put("optimizer.push-table-write-through-union", "false")
                .put("optimizer.dictionary-aggregation", "true")
                .put("optimizer.push-aggregation-through-outer-join", "false")
                .put("optimizer.push-partial-aggregation-through-join", "true")
                .put("optimizer.pre-aggregate-case-aggregations.enabled", "false")
                .put("optimizer.enable-intermediate-aggregations", "true")
                .put("optimizer.force-single-node-output", "true")
                .put("optimizer.mark-distinct-strategy", "always")
                .put("optimizer.prefer-partial-aggregation", "false")
                .put("optimizer.optimize-top-n-ranking", "false")
                .put("optimizer.skip-redundant-sort", "false")
                .put("optimizer.complex-expression-pushdown.enabled", "false")
                .put("optimizer.predicate-pushdown-use-table-properties", "false")
                .put("optimizer.ignore-downstream-preferences", "true")
                .put("optimizer.rewrite-filtering-semi-join-to-inner-join", "false")
                .put("optimizer.optimize-duplicate-insensitive-joins", "false")
                .put("optimizer.use-legacy-window-filter-pushdown", "true")
                .put("optimizer.use-table-scan-node-partitioning", "false")
                .put("optimizer.table-scan-node-partitioning-min-bucket-to-task-ratio", "0.0")
                .put("optimizer.merge-project-with-values", "false")
                .put("adaptive-partial-aggregation.enabled", "false")
                .put("adaptive-partial-aggregation.min-rows", "1")
                .put("adaptive-partial-aggregation.unique-rows-ratio-threshold", "0.99")
                .put("optimizer.join-partitioned-build-min-row-count", "1")
                .put("optimizer.min-input-size-per-task", "1MB")
                .put("optimizer.min-input-rows-per-task", "1000000")
                .put("optimizer.use-exact-partitioning", "true")
                .buildOrThrow();

        OptimizerConfig expected = new OptimizerConfig()
                .setCpuCostWeight(0.4)
                .setMemoryCostWeight(0.3)
                .setNetworkCostWeight(0.2)
                .setEnableStatsCalculator(false)
                .setStatisticsPrecalculationForPushdownEnabled(false)
                .setCollectPlanStatisticsForAllQueries(true)
                .setIgnoreStatsCalculatorFailures(false)
                .setJoinDistributionType(BROADCAST)
                .setJoinMaxBroadcastTableSize(DataSize.of(42, GIGABYTE))
                .setJoinMultiClauseIndependenceFactor(0.75)
                .setJoinReorderingStrategy(NONE)
                .setMaxReorderedJoins(5)
                .setIterativeOptimizerTimeout(new Duration(10, SECONDS))
                .setEnableForcedExchangeBelowGroupId(false)
                .setColocatedJoinsEnabled(true)
                .setSpatialJoinsEnabled(false)
                .setUsePreferredWritePartitioning(false)
                .setPreferredWritePartitioningMinNumberOfPartitions(10)
                .setDefaultFilterFactorEnabled(true)
                .setFilterConjunctionIndependenceFactor(1.0)
                .setNonEstimatablePredicateApproximationEnabled(false)
                .setOptimizeMetadataQueries(true)
                .setOptimizeHashGeneration(false)
                .setOptimizeMixedDistinctAggregations(true)
                .setPushTableWriteThroughUnion(false)
                .setDictionaryAggregation(true)
                .setPushAggregationThroughOuterJoin(false)
                .setPushPartialAggregationThoughJoin(true)
                .setPreAggregateCaseAggregationsEnabled(false)
                .setEnableIntermediateAggregations(true)
                .setMarkDistinctStrategy(ALWAYS)
                .setPreferPartialAggregation(false)
                .setOptimizeTopNRanking(false)
                .setDistributedSortEnabled(false)
                .setSkipRedundantSort(false)
                .setComplexExpressionPushdownEnabled(false)
                .setPredicatePushdownUseTableProperties(false)
                .setIgnoreDownstreamPreferences(true)
                .setRewriteFilteringSemiJoinToInnerJoin(false)
                .setOptimizeDuplicateInsensitiveJoins(false)
                .setUseLegacyWindowFilterPushdown(true)
                .setUseTableScanNodePartitioning(false)
                .setTableScanNodePartitioningMinBucketToTaskRatio(0.0)
                .setMergeProjectWithValues(false)
                .setForceSingleNodeOutput(true)
                .setAdaptivePartialAggregationEnabled(false)
                .setAdaptivePartialAggregationMinRows(1)
                .setAdaptivePartialAggregationUniqueRowsRatioThreshold(0.99)
                .setJoinPartitionedBuildMinRowCount(1)
                .setMinInputSizePerTask(DataSize.of(1, MEGABYTE))
                .setMinInputRowsPerTask(1_000_000L)
                .setUseExactPartitioning(true);
        assertFullMapping(properties, expected);
    }
}
