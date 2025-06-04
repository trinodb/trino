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
package io.trino.sql.planner;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigHidden;
import io.airlift.configuration.DefunctConfig;
import io.airlift.configuration.LegacyConfig;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;

@DefunctConfig({
        "adaptive-partial-aggregation.min-rows",
        "preferred-write-partitioning-min-number-of-partitions",
        "optimizer.use-mark-distinct",
        "optimizer.optimize-mixed-distinct-aggregations",
        "optimizer.optimize-hash-generation",
})
public class OptimizerConfig
{
    private double cpuCostWeight = 75;
    private double memoryCostWeight = 10;
    private double networkCostWeight = 15;

    private DataSize joinMaxBroadcastTableSize = DataSize.of(100, MEGABYTE);
    private JoinDistributionType joinDistributionType = JoinDistributionType.AUTOMATIC;
    private double joinMultiClauseIndependenceFactor = 0.25;

    private JoinReorderingStrategy joinReorderingStrategy = JoinReorderingStrategy.AUTOMATIC;
    private int maxReorderedJoins = 8;
    private int maxPrefetchedInformationSchemaPrefixes = 100;

    private boolean enableStatsCalculator = true;
    private boolean statisticsPrecalculationForPushdownEnabled = true;
    private boolean collectPlanStatisticsForAllQueries;
    private boolean ignoreStatsCalculatorFailures = true;
    private boolean defaultFilterFactorEnabled;
    private double filterConjunctionIndependenceFactor = 0.75;
    private boolean nonEstimatablePredicateApproximationEnabled = true;

    private boolean colocatedJoinsEnabled = true;
    private boolean spatialJoinsEnabled = true;
    private boolean distributedSort = true;

    private boolean usePreferredWritePartitioning = true;

    private Duration iterativeOptimizerTimeout = new Duration(3, MINUTES); // by default let optimizer wait a long time in case it retrieves some data from ConnectorMetadata

    private boolean optimizeMetadataQueries;
    private boolean pushTableWriteThroughUnion = true;
    private boolean dictionaryAggregation;
    private MarkDistinctStrategy markDistinctStrategy;
    private DistinctAggregationsStrategy distinctAggregationsStrategy;
    private boolean preferPartialAggregation = true;
    private boolean pushAggregationThroughOuterJoin = true;
    private boolean enableIntermediateAggregations;
    private boolean pushPartialAggregationThroughJoin = true;
    private boolean preAggregateCaseAggregationsEnabled = true;
    private boolean enableForcedExchangeBelowGroupId = true;
    private boolean optimizeTopNRanking = true;
    private boolean skipRedundantSort = true;
    private boolean complexExpressionPushdownEnabled = true;
    private boolean predicatePushdownUseTableProperties = true;
    private boolean ignoreDownstreamPreferences;
    private boolean rewriteFilteringSemiJoinToInnerJoin = true;
    private boolean optimizeDuplicateInsensitiveJoins = true;
    private boolean useLegacyWindowFilterPushdown;
    private boolean useTableScanNodePartitioning = true;
    private double tableScanNodePartitioningMinBucketToTaskRatio = 0.5;
    private boolean mergeProjectWithValues = true;
    private boolean forceSingleNodeOutput;
    private boolean useExactPartitioning;
    private boolean useCostBasedPartitioning = true;
    private int pushFilterIntoValuesMaxRowCount = 100;
    // adaptive partial aggregation
    private boolean adaptivePartialAggregationEnabled = true;
    private double adaptivePartialAggregationUniqueRowsRatioThreshold = 0.8;
    private long joinPartitionedBuildMinRowCount = 1_000_000L;
    private DataSize minInputSizePerTask = DataSize.of(5, GIGABYTE);
    private long minInputRowsPerTask = 10_000_000L;

    private boolean allowUnsafePushdown; // TODO: remove once https://github.com/trinodb/trino/issues/22268 is fixed

    public enum JoinReorderingStrategy
    {
        NONE,
        ELIMINATE_CROSS_JOINS,
        AUTOMATIC,
    }

    public enum JoinDistributionType
    {
        BROADCAST,
        PARTITIONED,
        AUTOMATIC;

        public boolean canPartition()
        {
            return this == PARTITIONED || this == AUTOMATIC;
        }

        public boolean canReplicate()
        {
            return this == BROADCAST || this == AUTOMATIC;
        }
    }

    public enum MarkDistinctStrategy
    {
        NONE,
        ALWAYS,
        AUTOMATIC,
    }

    public enum DistinctAggregationsStrategy
    {
        SINGLE_STEP,
        MARK_DISTINCT,
        PRE_AGGREGATE,
        SPLIT_TO_SUBQUERIES,
        AUTOMATIC,
    }

    public double getCpuCostWeight()
    {
        return cpuCostWeight;
    }

    @Config("cpu-cost-weight")
    public OptimizerConfig setCpuCostWeight(double cpuCostWeight)
    {
        this.cpuCostWeight = cpuCostWeight;
        return this;
    }

    public double getMemoryCostWeight()
    {
        return memoryCostWeight;
    }

    @Config("memory-cost-weight")
    public OptimizerConfig setMemoryCostWeight(double memoryCostWeight)
    {
        this.memoryCostWeight = memoryCostWeight;
        return this;
    }

    public double getNetworkCostWeight()
    {
        return networkCostWeight;
    }

    @Config("network-cost-weight")
    public OptimizerConfig setNetworkCostWeight(double networkCostWeight)
    {
        this.networkCostWeight = networkCostWeight;
        return this;
    }

    public JoinDistributionType getJoinDistributionType()
    {
        return joinDistributionType;
    }

    @Config("join-distribution-type")
    public OptimizerConfig setJoinDistributionType(JoinDistributionType joinDistributionType)
    {
        this.joinDistributionType = requireNonNull(joinDistributionType, "joinDistributionType is null");
        return this;
    }

    @NotNull
    public DataSize getJoinMaxBroadcastTableSize()
    {
        return joinMaxBroadcastTableSize;
    }

    @Config("join-max-broadcast-table-size")
    @ConfigDescription("Maximum estimated size of a table that can be broadcast when using automatic join type selection")
    public OptimizerConfig setJoinMaxBroadcastTableSize(DataSize joinMaxBroadcastTableSize)
    {
        this.joinMaxBroadcastTableSize = joinMaxBroadcastTableSize;
        return this;
    }

    @Min(0)
    @Max(1)
    public double getJoinMultiClauseIndependenceFactor()
    {
        return joinMultiClauseIndependenceFactor;
    }

    @Config("optimizer.join-multi-clause-independence-factor")
    @ConfigDescription("Scales the strength of independence assumption for selectivity estimates of multi-clause joins")
    public OptimizerConfig setJoinMultiClauseIndependenceFactor(double joinMultiClauseIndependenceFactor)
    {
        this.joinMultiClauseIndependenceFactor = joinMultiClauseIndependenceFactor;
        return this;
    }

    public JoinReorderingStrategy getJoinReorderingStrategy()
    {
        return joinReorderingStrategy;
    }

    @Config("optimizer.join-reordering-strategy")
    @ConfigDescription("The strategy to use for reordering joins")
    public OptimizerConfig setJoinReorderingStrategy(JoinReorderingStrategy joinReorderingStrategy)
    {
        this.joinReorderingStrategy = joinReorderingStrategy;
        return this;
    }

    @Min(2)
    public int getMaxReorderedJoins()
    {
        return maxReorderedJoins;
    }

    @Config("optimizer.max-reordered-joins")
    @ConfigDescription("The maximum number of tables to reorder in cost-based join reordering")
    public OptimizerConfig setMaxReorderedJoins(int maxReorderedJoins)
    {
        this.maxReorderedJoins = maxReorderedJoins;
        return this;
    }

    @Min(1)
    public int getMaxPrefetchedInformationSchemaPrefixes()
    {
        return maxPrefetchedInformationSchemaPrefixes;
    }

    @Config("optimizer.experimental-max-prefetched-information-schema-prefixes")
    @ConfigHidden
    @ConfigDescription("Experimental: maximum number of internal \"prefixes\" to be prefetched when optimizing information_schema queries")
    public OptimizerConfig setMaxPrefetchedInformationSchemaPrefixes(int maxPrefetchedInformationSchemaPrefixes)
    {
        this.maxPrefetchedInformationSchemaPrefixes = maxPrefetchedInformationSchemaPrefixes;
        return this;
    }

    public boolean isEnableStatsCalculator()
    {
        return enableStatsCalculator;
    }

    @Config("enable-stats-calculator")
    @LegacyConfig("experimental.enable-stats-calculator")
    public OptimizerConfig setEnableStatsCalculator(boolean enableStatsCalculator)
    {
        this.enableStatsCalculator = enableStatsCalculator;
        return this;
    }

    public boolean isStatisticsPrecalculationForPushdownEnabled()
    {
        return statisticsPrecalculationForPushdownEnabled;
    }

    @Config("statistics-precalculation-for-pushdown.enabled")
    public OptimizerConfig setStatisticsPrecalculationForPushdownEnabled(boolean statisticsPrecalculationForPushdownEnabled)
    {
        this.statisticsPrecalculationForPushdownEnabled = statisticsPrecalculationForPushdownEnabled;
        return this;
    }

    public boolean isCollectPlanStatisticsForAllQueries()
    {
        return collectPlanStatisticsForAllQueries;
    }

    @Config("collect-plan-statistics-for-all-queries")
    @ConfigDescription("Collect plan statistics for non-EXPLAIN queries")
    public OptimizerConfig setCollectPlanStatisticsForAllQueries(boolean collectPlanStatisticsForAllQueries)
    {
        this.collectPlanStatisticsForAllQueries = collectPlanStatisticsForAllQueries;
        return this;
    }

    public boolean isIgnoreStatsCalculatorFailures()
    {
        return ignoreStatsCalculatorFailures;
    }

    @Config("optimizer.ignore-stats-calculator-failures")
    @ConfigDescription("Ignore statistics calculator failures")
    public OptimizerConfig setIgnoreStatsCalculatorFailures(boolean ignoreStatsCalculatorFailures)
    {
        this.ignoreStatsCalculatorFailures = ignoreStatsCalculatorFailures;
        return this;
    }

    public boolean isDefaultFilterFactorEnabled()
    {
        return defaultFilterFactorEnabled;
    }

    @Config("optimizer.default-filter-factor-enabled")
    public OptimizerConfig setDefaultFilterFactorEnabled(boolean defaultFilterFactorEnabled)
    {
        this.defaultFilterFactorEnabled = defaultFilterFactorEnabled;
        return this;
    }

    @Min(0)
    @Max(1)
    public double getFilterConjunctionIndependenceFactor()
    {
        return filterConjunctionIndependenceFactor;
    }

    @Config("optimizer.filter-conjunction-independence-factor")
    @ConfigDescription("Scales the strength of independence assumption for selectivity estimates of the conjunction of multiple filters")
    public OptimizerConfig setFilterConjunctionIndependenceFactor(double filterConjunctionIndependenceFactor)
    {
        this.filterConjunctionIndependenceFactor = filterConjunctionIndependenceFactor;
        return this;
    }

    public boolean isNonEstimatablePredicateApproximationEnabled()
    {
        return nonEstimatablePredicateApproximationEnabled;
    }

    @Config("optimizer.non-estimatable-predicate-approximation.enabled")
    @ConfigDescription("Approximate the cost of filters which cannot be accurately estimated even with complete statistics")
    public OptimizerConfig setNonEstimatablePredicateApproximationEnabled(boolean nonEstimatablePredicateApproximationEnabled)
    {
        this.nonEstimatablePredicateApproximationEnabled = nonEstimatablePredicateApproximationEnabled;
        return this;
    }

    public boolean isColocatedJoinsEnabled()
    {
        return colocatedJoinsEnabled;
    }

    @Config("colocated-joins-enabled")
    @ConfigDescription("Use a colocated join when possible")
    public OptimizerConfig setColocatedJoinsEnabled(boolean colocatedJoinsEnabled)
    {
        this.colocatedJoinsEnabled = colocatedJoinsEnabled;
        return this;
    }

    public boolean isSpatialJoinsEnabled()
    {
        return spatialJoinsEnabled;
    }

    @Config("spatial-joins-enabled")
    @ConfigDescription("Use spatial index for spatial joins when possible")
    public OptimizerConfig setSpatialJoinsEnabled(boolean spatialJoinsEnabled)
    {
        this.spatialJoinsEnabled = spatialJoinsEnabled;
        return this;
    }

    public boolean isDistributedSortEnabled()
    {
        return distributedSort;
    }

    @Config("distributed-sort")
    public OptimizerConfig setDistributedSortEnabled(boolean enabled)
    {
        distributedSort = enabled;
        return this;
    }

    public boolean isUsePreferredWritePartitioning()
    {
        return usePreferredWritePartitioning;
    }

    @Config("use-preferred-write-partitioning")
    public OptimizerConfig setUsePreferredWritePartitioning(boolean usePreferredWritePartitioning)
    {
        this.usePreferredWritePartitioning = usePreferredWritePartitioning;
        return this;
    }

    public Duration getIterativeOptimizerTimeout()
    {
        return iterativeOptimizerTimeout;
    }

    @Config("iterative-optimizer-timeout")
    @LegacyConfig("experimental.iterative-optimizer-timeout")
    public OptimizerConfig setIterativeOptimizerTimeout(Duration timeout)
    {
        this.iterativeOptimizerTimeout = timeout;
        return this;
    }

    public boolean isEnableIntermediateAggregations()
    {
        return enableIntermediateAggregations;
    }

    @Config("optimizer.enable-intermediate-aggregations")
    public OptimizerConfig setEnableIntermediateAggregations(boolean enableIntermediateAggregations)
    {
        this.enableIntermediateAggregations = enableIntermediateAggregations;
        return this;
    }

    public boolean isPushAggregationThroughOuterJoin()
    {
        return pushAggregationThroughOuterJoin;
    }

    @Config("optimizer.push-aggregation-through-outer-join")
    @LegacyConfig("optimizer.push-aggregation-through-join")
    public OptimizerConfig setPushAggregationThroughOuterJoin(boolean pushAggregationThroughOuterJoin)
    {
        this.pushAggregationThroughOuterJoin = pushAggregationThroughOuterJoin;
        return this;
    }

    public boolean isPushPartialAggregationThroughJoin()
    {
        return pushPartialAggregationThroughJoin;
    }

    @Config("optimizer.push-partial-aggregation-through-join")
    public OptimizerConfig setPushPartialAggregationThroughJoin(boolean pushPartialAggregationThroughJoin)
    {
        this.pushPartialAggregationThroughJoin = pushPartialAggregationThroughJoin;
        return this;
    }

    public boolean isPreAggregateCaseAggregationsEnabled()
    {
        return preAggregateCaseAggregationsEnabled;
    }

    @Config("optimizer.pre-aggregate-case-aggregations.enabled")
    @ConfigDescription("Pre-aggregate rows before GROUP BY with multiple CASE aggregations on same column")
    public OptimizerConfig setPreAggregateCaseAggregationsEnabled(boolean preAggregateCaseAggregationsEnabled)
    {
        this.preAggregateCaseAggregationsEnabled = preAggregateCaseAggregationsEnabled;
        return this;
    }

    public boolean isOptimizeMetadataQueries()
    {
        return optimizeMetadataQueries;
    }

    @Config("optimizer.optimize-metadata-queries")
    public OptimizerConfig setOptimizeMetadataQueries(boolean optimizeMetadataQueries)
    {
        this.optimizeMetadataQueries = optimizeMetadataQueries;
        return this;
    }

    @Deprecated
    @Nullable
    public MarkDistinctStrategy getMarkDistinctStrategy()
    {
        return markDistinctStrategy;
    }

    @Deprecated
    @LegacyConfig(value = "optimizer.mark-distinct-strategy", replacedBy = "optimizer.distinct-aggregations-strategy")
    @ConfigDescription("Strategy to use for distinct aggregations")
    public OptimizerConfig setMarkDistinctStrategy(MarkDistinctStrategy markDistinctStrategy)
    {
        this.markDistinctStrategy = markDistinctStrategy;
        return this;
    }

    @Nullable
    public DistinctAggregationsStrategy getDistinctAggregationsStrategy()
    {
        return distinctAggregationsStrategy;
    }

    @Config("optimizer.distinct-aggregations-strategy")
    @ConfigDescription("Strategy to use for distinct aggregations")
    public OptimizerConfig setDistinctAggregationsStrategy(DistinctAggregationsStrategy distinctAggregationsStrategy)
    {
        this.distinctAggregationsStrategy = distinctAggregationsStrategy;
        return this;
    }

    public boolean isPreferPartialAggregation()
    {
        return preferPartialAggregation;
    }

    @Config("optimizer.prefer-partial-aggregation")
    public OptimizerConfig setPreferPartialAggregation(boolean value)
    {
        this.preferPartialAggregation = value;
        return this;
    }

    public boolean isEnableForcedExchangeBelowGroupId()
    {
        return enableForcedExchangeBelowGroupId;
    }

    @Config("enable-forced-exchange-below-group-id")
    public OptimizerConfig setEnableForcedExchangeBelowGroupId(boolean enableForcedExchangeBelowGroupId)
    {
        this.enableForcedExchangeBelowGroupId = enableForcedExchangeBelowGroupId;
        return this;
    }

    public boolean isOptimizeTopNRanking()
    {
        return optimizeTopNRanking;
    }

    @Config("optimizer.optimize-top-n-ranking")
    @LegacyConfig("optimizer.optimize-top-n-row-number")
    public OptimizerConfig setOptimizeTopNRanking(boolean optimizeTopNRanking)
    {
        this.optimizeTopNRanking = optimizeTopNRanking;
        return this;
    }

    public boolean isPushTableWriteThroughUnion()
    {
        return pushTableWriteThroughUnion;
    }

    @Config("optimizer.push-table-write-through-union")
    public OptimizerConfig setPushTableWriteThroughUnion(boolean pushTableWriteThroughUnion)
    {
        this.pushTableWriteThroughUnion = pushTableWriteThroughUnion;
        return this;
    }

    public boolean isDictionaryAggregation()
    {
        return dictionaryAggregation;
    }

    @Config("optimizer.dictionary-aggregation")
    public OptimizerConfig setDictionaryAggregation(boolean dictionaryAggregation)
    {
        this.dictionaryAggregation = dictionaryAggregation;
        return this;
    }

    public boolean isSkipRedundantSort()
    {
        return skipRedundantSort;
    }

    @Config("optimizer.skip-redundant-sort")
    public OptimizerConfig setSkipRedundantSort(boolean value)
    {
        this.skipRedundantSort = value;
        return this;
    }

    public boolean isComplexExpressionPushdownEnabled()
    {
        return complexExpressionPushdownEnabled;
    }

    @Config("optimizer.complex-expression-pushdown.enabled")
    public OptimizerConfig setComplexExpressionPushdownEnabled(boolean complexExpressionPushdownEnabled)
    {
        this.complexExpressionPushdownEnabled = complexExpressionPushdownEnabled;
        return this;
    }

    public boolean isPredicatePushdownUseTableProperties()
    {
        return predicatePushdownUseTableProperties;
    }

    @Config("optimizer.predicate-pushdown-use-table-properties")
    public OptimizerConfig setPredicatePushdownUseTableProperties(boolean predicatePushdownUseTableProperties)
    {
        this.predicatePushdownUseTableProperties = predicatePushdownUseTableProperties;
        return this;
    }

    public boolean isIgnoreDownstreamPreferences()
    {
        return ignoreDownstreamPreferences;
    }

    @Config("optimizer.ignore-downstream-preferences")
    public OptimizerConfig setIgnoreDownstreamPreferences(boolean ignoreDownstreamPreferences)
    {
        this.ignoreDownstreamPreferences = ignoreDownstreamPreferences;
        return this;
    }

    public boolean isRewriteFilteringSemiJoinToInnerJoin()
    {
        return rewriteFilteringSemiJoinToInnerJoin;
    }

    @Config("optimizer.rewrite-filtering-semi-join-to-inner-join")
    public OptimizerConfig setRewriteFilteringSemiJoinToInnerJoin(boolean rewriteFilteringSemiJoinToInnerJoin)
    {
        this.rewriteFilteringSemiJoinToInnerJoin = rewriteFilteringSemiJoinToInnerJoin;
        return this;
    }

    public boolean isOptimizeDuplicateInsensitiveJoins()
    {
        return optimizeDuplicateInsensitiveJoins;
    }

    @Config("optimizer.optimize-duplicate-insensitive-joins")
    public OptimizerConfig setOptimizeDuplicateInsensitiveJoins(boolean optimizeDuplicateInsensitiveJoins)
    {
        this.optimizeDuplicateInsensitiveJoins = optimizeDuplicateInsensitiveJoins;
        return this;
    }

    public boolean isUseLegacyWindowFilterPushdown()
    {
        return useLegacyWindowFilterPushdown;
    }

    @Config("optimizer.use-legacy-window-filter-pushdown")
    public OptimizerConfig setUseLegacyWindowFilterPushdown(boolean useLegacyWindowFilterPushdown)
    {
        this.useLegacyWindowFilterPushdown = useLegacyWindowFilterPushdown;
        return this;
    }

    public boolean isUseTableScanNodePartitioning()
    {
        return useTableScanNodePartitioning;
    }

    @Config("optimizer.use-table-scan-node-partitioning")
    @LegacyConfig("optimizer.plan-with-table-node-partitioning")
    @ConfigDescription("Adapt plan to node pre-partitioned tables")
    public OptimizerConfig setUseTableScanNodePartitioning(boolean useTableScanNodePartitioning)
    {
        this.useTableScanNodePartitioning = useTableScanNodePartitioning;
        return this;
    }

    @Min(0)
    public double getTableScanNodePartitioningMinBucketToTaskRatio()
    {
        return tableScanNodePartitioningMinBucketToTaskRatio;
    }

    @Config("optimizer.table-scan-node-partitioning-min-bucket-to-task-ratio")
    @ConfigDescription("Min table scan bucket to task ratio for which plan will be adopted to node pre-partitioned tables")
    public OptimizerConfig setTableScanNodePartitioningMinBucketToTaskRatio(double tableScanNodePartitioningMinBucketToTaskRatio)
    {
        this.tableScanNodePartitioningMinBucketToTaskRatio = tableScanNodePartitioningMinBucketToTaskRatio;
        return this;
    }

    public boolean isMergeProjectWithValues()
    {
        return mergeProjectWithValues;
    }

    @Config("optimizer.merge-project-with-values")
    public OptimizerConfig setMergeProjectWithValues(boolean mergeProjectWithValues)
    {
        this.mergeProjectWithValues = mergeProjectWithValues;
        return this;
    }

    public boolean isForceSingleNodeOutput()
    {
        return forceSingleNodeOutput;
    }

    @Config("optimizer.force-single-node-output")
    public OptimizerConfig setForceSingleNodeOutput(boolean value)
    {
        this.forceSingleNodeOutput = value;
        return this;
    }

    public boolean isAdaptivePartialAggregationEnabled()
    {
        return adaptivePartialAggregationEnabled;
    }

    @Config("adaptive-partial-aggregation.enabled")
    public OptimizerConfig setAdaptivePartialAggregationEnabled(boolean adaptivePartialAggregationEnabled)
    {
        this.adaptivePartialAggregationEnabled = adaptivePartialAggregationEnabled;
        return this;
    }

    public double getAdaptivePartialAggregationUniqueRowsRatioThreshold()
    {
        return adaptivePartialAggregationUniqueRowsRatioThreshold;
    }

    @Config("adaptive-partial-aggregation.unique-rows-ratio-threshold")
    @ConfigDescription("Ratio between aggregation output and input rows above which partial aggregation might be adaptively turned off")
    public OptimizerConfig setAdaptivePartialAggregationUniqueRowsRatioThreshold(double adaptivePartialAggregationUniqueRowsRatioThreshold)
    {
        this.adaptivePartialAggregationUniqueRowsRatioThreshold = adaptivePartialAggregationUniqueRowsRatioThreshold;
        return this;
    }

    @Min(0)
    public long getJoinPartitionedBuildMinRowCount()
    {
        return joinPartitionedBuildMinRowCount;
    }

    @Config("optimizer.join-partitioned-build-min-row-count")
    @ConfigDescription("Minimum number of join build side rows required to use partitioned join lookup")
    public OptimizerConfig setJoinPartitionedBuildMinRowCount(long joinPartitionedBuildMinRowCount)
    {
        this.joinPartitionedBuildMinRowCount = joinPartitionedBuildMinRowCount;
        return this;
    }

    @NotNull
    public DataSize getMinInputSizePerTask()
    {
        return minInputSizePerTask;
    }

    @Config("optimizer.min-input-size-per-task")
    @ConfigDescription("Minimum input data size required per task. This will help optimizer determine hash partition count for joins and aggregations")
    public OptimizerConfig setMinInputSizePerTask(DataSize minInputSizePerTask)
    {
        this.minInputSizePerTask = minInputSizePerTask;
        return this;
    }

    @Min(0)
    public long getMinInputRowsPerTask()
    {
        return minInputRowsPerTask;
    }

    @Config("optimizer.min-input-rows-per-task")
    @ConfigDescription("Minimum input rows required per task. This will help optimizer determine hash partition count for joins and aggregations")
    public OptimizerConfig setMinInputRowsPerTask(long minInputRowsPerTask)
    {
        this.minInputRowsPerTask = minInputRowsPerTask;
        return this;
    }

    public boolean isUseExactPartitioning()
    {
        return useExactPartitioning;
    }

    @Config("optimizer.use-exact-partitioning")
    @ConfigDescription("When enabled this forces data repartitioning unless the partitioning of upstream stage matches exactly what downstream stage expects")
    public OptimizerConfig setUseExactPartitioning(boolean useExactPartitioning)
    {
        this.useExactPartitioning = useExactPartitioning;
        return this;
    }

    public boolean isUseCostBasedPartitioning()
    {
        return useCostBasedPartitioning;
    }

    @Config("optimizer.use-cost-based-partitioning")
    @ConfigDescription("When enabled the cost based optimizer is used to determine if repartitioning the output of an already partitioned stage is necessary")
    public OptimizerConfig setUseCostBasedPartitioning(boolean useCostBasedPartitioning)
    {
        this.useCostBasedPartitioning = useCostBasedPartitioning;
        return this;
    }

    @Min(0)
    public int getPushFilterIntoValuesMaxRowCount()
    {
        return pushFilterIntoValuesMaxRowCount;
    }

    @Config("optimizer.push-filter-into-values-max-row-count")
    @ConfigDescription("Maximum number of rows in values for which filter is pushed down into values")
    public OptimizerConfig setPushFilterIntoValuesMaxRowCount(int pushFilterIntoValuesMaxRowCount)
    {
        this.pushFilterIntoValuesMaxRowCount = pushFilterIntoValuesMaxRowCount;
        return this;
    }

    public boolean isUnsafePushdownAllowed()
    {
        return allowUnsafePushdown;
    }

    @Config("optimizer.allow-unsafe-pushdown")
    @ConfigDescription("Allow pushing down expressions that mail fail for some inputs")
    public OptimizerConfig setUnsafePushdownAllowed(boolean value)
    {
        this.allowUnsafePushdown = value;
        return this;
    }
}
