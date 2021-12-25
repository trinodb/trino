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
package io.trino;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import io.airlift.configuration.LegacyConfig;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MaxDataSize;
import io.trino.sql.analyzer.RegexLibrary;

import javax.validation.constraints.DecimalMax;
import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.sql.analyzer.RegexLibrary.JONI;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;

@DefunctConfig({
        "analyzer.experimental-syntax-enabled",
        "arrayagg.implementation",
        "deprecated.group-by-uses-equal",
        "deprecated.legacy-char-to-varchar-coercion",
        "deprecated.legacy-join-using",
        "deprecated.legacy-map-subscript",
        "deprecated.legacy-order-by",
        "deprecated.legacy-row-field-ordinal-access",
        "deprecated.legacy-timestamp",
        "deprecated.legacy-unnest-array-rows",
        "experimental-syntax-enabled",
        "experimental.resource-groups-enabled",
        "fast-inequality-joins",
        "histogram.implementation",
        "multimapagg.implementation",
        "optimizer.iterative-rule-based-column-pruning",
        "optimizer.processing-optimization",
        "resource-group-manager",
})
public class FeaturesConfig
{
    @VisibleForTesting
    static final String SPILL_ENABLED = "spill-enabled";
    public static final String SPILLER_SPILL_PATH = "spiller-spill-path";

    private double cpuCostWeight = 75;
    private double memoryCostWeight = 10;
    private double networkCostWeight = 15;
    private boolean distributedIndexJoinsEnabled;
    private DataSize joinMaxBroadcastTableSize = DataSize.of(100, MEGABYTE);
    private JoinDistributionType joinDistributionType = JoinDistributionType.AUTOMATIC;
    private boolean colocatedJoinsEnabled;
    private boolean groupedExecutionEnabled;
    private boolean dynamicScheduleForGroupedExecution;
    private int concurrentLifespansPerTask;
    private boolean spatialJoinsEnabled = true;
    private JoinReorderingStrategy joinReorderingStrategy = JoinReorderingStrategy.AUTOMATIC;
    private int maxReorderedJoins = 9;
    private boolean redistributeWrites = true;
    private boolean usePreferredWritePartitioning = true;
    private int preferredWritePartitioningMinNumberOfPartitions = 50;
    private boolean scaleWriters;
    private DataSize writerMinSize = DataSize.of(32, DataSize.Unit.MEGABYTE);
    private boolean optimizeMetadataQueries;
    private boolean optimizeHashGeneration = true;
    private boolean enableIntermediateAggregations;
    private boolean pushTableWriteThroughUnion = true;
    private DataIntegrityVerification exchangeDataIntegrityVerification = DataIntegrityVerification.ABORT;
    private boolean exchangeCompressionEnabled;
    private boolean legacyRowToJsonCast;
    private boolean optimizeMixedDistinctAggregations;
    private boolean forceSingleNodeOutput = true;
    private boolean pagesIndexEagerCompactionEnabled;
    private boolean distributedSort = true;
    private boolean omitDateTimeTypePrecision;
    private int maxRecursionDepth = 10;

    private boolean dictionaryAggregation;

    private int re2JDfaStatesLimit = Integer.MAX_VALUE;
    private int re2JDfaRetries = 5;
    private RegexLibrary regexLibrary = JONI;
    private boolean spillEnabled;
    private boolean spillOrderBy = true;
    private boolean spillWindowOperator = true;
    private DataSize aggregationOperatorUnspillMemoryLimit = DataSize.of(4, DataSize.Unit.MEGABYTE);
    private List<Path> spillerSpillPaths = ImmutableList.of();
    private int spillerThreads = 4;
    private double spillMaxUsedSpaceThreshold = 0.9;
    private boolean enableStatsCalculator = true;
    private boolean statisticsPrecalculationForPushdownEnabled;
    private boolean collectPlanStatisticsForAllQueries;
    private boolean ignoreStatsCalculatorFailures = true;
    private boolean defaultFilterFactorEnabled;
    private boolean enableForcedExchangeBelowGroupId = true;
    private boolean pushAggregationThroughOuterJoin = true;
    private boolean pushPartialAggregationThoughJoin;
    private double memoryRevokingTarget = 0.5;
    private double memoryRevokingThreshold = 0.9;
    private boolean parseDecimalLiteralsAsDouble;
    private boolean useMarkDistinct = true;
    private boolean preferPartialAggregation = true;
    private boolean optimizeTopNRanking = true;
    private boolean lateMaterializationEnabled;
    private boolean skipRedundantSort = true;
    private boolean predicatePushdownUseTableProperties = true;
    private boolean ignoreDownstreamPreferences;
    private boolean rewriteFilteringSemiJoinToInnerJoin = true;
    private boolean optimizeDuplicateInsensitiveJoins = true;
    private boolean useLegacyWindowFilterPushdown;
    private boolean useTableScanNodePartitioning = true;
    private double tableScanNodePartitioningMinBucketToTaskRatio = 0.5;
    private boolean mergeProjectWithValues = true;

    private Duration iterativeOptimizerTimeout = new Duration(3, MINUTES); // by default let optimizer wait a long time in case it retrieves some data from ConnectorMetadata
    private DataSize filterAndProjectMinOutputPageSize = DataSize.of(500, KILOBYTE);
    private int filterAndProjectMinOutputPageRowCount = 256;
    private int maxGroupingSets = 2048;

    private boolean legacyCatalogRoles;
    private boolean disableSetPropertiesSecurityCheckForCreateDdl;
    private boolean incrementalHashArrayLoadFactorEnabled = true;
    private boolean allowSetViewAuthorization;

    private boolean hideInaccesibleColumns;

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

    public enum DataIntegrityVerification
    {
        NONE,
        ABORT,
        RETRY,
        /**/;
    }

    public double getCpuCostWeight()
    {
        return cpuCostWeight;
    }

    @Config("cpu-cost-weight")
    public FeaturesConfig setCpuCostWeight(double cpuCostWeight)
    {
        this.cpuCostWeight = cpuCostWeight;
        return this;
    }

    public double getMemoryCostWeight()
    {
        return memoryCostWeight;
    }

    @Config("memory-cost-weight")
    public FeaturesConfig setMemoryCostWeight(double memoryCostWeight)
    {
        this.memoryCostWeight = memoryCostWeight;
        return this;
    }

    public double getNetworkCostWeight()
    {
        return networkCostWeight;
    }

    @Config("network-cost-weight")
    public FeaturesConfig setNetworkCostWeight(double networkCostWeight)
    {
        this.networkCostWeight = networkCostWeight;
        return this;
    }

    public boolean isDistributedIndexJoinsEnabled()
    {
        return distributedIndexJoinsEnabled;
    }

    @Config("distributed-index-joins-enabled")
    public FeaturesConfig setDistributedIndexJoinsEnabled(boolean distributedIndexJoinsEnabled)
    {
        this.distributedIndexJoinsEnabled = distributedIndexJoinsEnabled;
        return this;
    }

    public boolean isOmitDateTimeTypePrecision()
    {
        return omitDateTimeTypePrecision;
    }

    @Config("deprecated.omit-datetime-type-precision")
    @ConfigDescription("Enable compatibility mode for legacy clients when rendering datetime type names with default precision")
    public FeaturesConfig setOmitDateTimeTypePrecision(boolean value)
    {
        this.omitDateTimeTypePrecision = value;
        return this;
    }

    public boolean isLegacyRowToJsonCast()
    {
        return legacyRowToJsonCast;
    }

    @Config("deprecated.legacy-row-to-json-cast")
    public FeaturesConfig setLegacyRowToJsonCast(boolean legacyRowToJsonCast)
    {
        this.legacyRowToJsonCast = legacyRowToJsonCast;
        return this;
    }

    public JoinDistributionType getJoinDistributionType()
    {
        return joinDistributionType;
    }

    @Config("join-distribution-type")
    public FeaturesConfig setJoinDistributionType(JoinDistributionType joinDistributionType)
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
    public FeaturesConfig setJoinMaxBroadcastTableSize(DataSize joinMaxBroadcastTableSize)
    {
        this.joinMaxBroadcastTableSize = joinMaxBroadcastTableSize;
        return this;
    }

    public boolean isGroupedExecutionEnabled()
    {
        return groupedExecutionEnabled;
    }

    @Config("grouped-execution-enabled")
    @ConfigDescription("Experimental: Use grouped execution when possible")
    public FeaturesConfig setGroupedExecutionEnabled(boolean groupedExecutionEnabled)
    {
        this.groupedExecutionEnabled = groupedExecutionEnabled;
        return this;
    }

    public boolean isDynamicScheduleForGroupedExecutionEnabled()
    {
        return dynamicScheduleForGroupedExecution;
    }

    @Config("dynamic-schedule-for-grouped-execution")
    @ConfigDescription("Experimental: Use dynamic schedule for grouped execution when possible")
    public FeaturesConfig setDynamicScheduleForGroupedExecutionEnabled(boolean dynamicScheduleForGroupedExecution)
    {
        this.dynamicScheduleForGroupedExecution = dynamicScheduleForGroupedExecution;
        return this;
    }

    @Min(0)
    public int getConcurrentLifespansPerTask()
    {
        return concurrentLifespansPerTask;
    }

    @Config("concurrent-lifespans-per-task")
    @ConfigDescription("Experimental: Default number of lifespans that run in parallel on each task when grouped execution is enabled")
    // When set to zero, a limit is not imposed on the number of lifespans that run in parallel
    public FeaturesConfig setConcurrentLifespansPerTask(int concurrentLifespansPerTask)
    {
        this.concurrentLifespansPerTask = concurrentLifespansPerTask;
        return this;
    }

    public boolean isColocatedJoinsEnabled()
    {
        return colocatedJoinsEnabled;
    }

    @Config("colocated-joins-enabled")
    @ConfigDescription("Experimental: Use a colocated join when possible")
    public FeaturesConfig setColocatedJoinsEnabled(boolean colocatedJoinsEnabled)
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
    public FeaturesConfig setSpatialJoinsEnabled(boolean spatialJoinsEnabled)
    {
        this.spatialJoinsEnabled = spatialJoinsEnabled;
        return this;
    }

    public JoinReorderingStrategy getJoinReorderingStrategy()
    {
        return joinReorderingStrategy;
    }

    @Config("optimizer.join-reordering-strategy")
    @ConfigDescription("The strategy to use for reordering joins")
    public FeaturesConfig setJoinReorderingStrategy(JoinReorderingStrategy joinReorderingStrategy)
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
    public FeaturesConfig setMaxReorderedJoins(int maxReorderedJoins)
    {
        this.maxReorderedJoins = maxReorderedJoins;
        return this;
    }

    public boolean isRedistributeWrites()
    {
        return redistributeWrites;
    }

    @Config("redistribute-writes")
    public FeaturesConfig setRedistributeWrites(boolean redistributeWrites)
    {
        this.redistributeWrites = redistributeWrites;
        return this;
    }

    public boolean isUsePreferredWritePartitioning()
    {
        return usePreferredWritePartitioning;
    }

    @Config("use-preferred-write-partitioning")
    public FeaturesConfig setUsePreferredWritePartitioning(boolean usePreferredWritePartitioning)
    {
        this.usePreferredWritePartitioning = usePreferredWritePartitioning;
        return this;
    }

    @Min(1)
    public int getPreferredWritePartitioningMinNumberOfPartitions()
    {
        return preferredWritePartitioningMinNumberOfPartitions;
    }

    @Config("preferred-write-partitioning-min-number-of-partitions")
    @ConfigDescription("Use preferred write partitioning when the number of written partitions exceeds the configured threshold")
    public FeaturesConfig setPreferredWritePartitioningMinNumberOfPartitions(int preferredWritePartitioningMinNumberOfPartitions)
    {
        this.preferredWritePartitioningMinNumberOfPartitions = preferredWritePartitioningMinNumberOfPartitions;
        return this;
    }

    public boolean isScaleWriters()
    {
        return scaleWriters;
    }

    @Config("scale-writers")
    public FeaturesConfig setScaleWriters(boolean scaleWriters)
    {
        this.scaleWriters = scaleWriters;
        return this;
    }

    @NotNull
    public DataSize getWriterMinSize()
    {
        return writerMinSize;
    }

    @Config("writer-min-size")
    @ConfigDescription("Target minimum size of writer output when scaling writers")
    public FeaturesConfig setWriterMinSize(DataSize writerMinSize)
    {
        this.writerMinSize = writerMinSize;
        return this;
    }

    public boolean isOptimizeMetadataQueries()
    {
        return optimizeMetadataQueries;
    }

    @Config("optimizer.optimize-metadata-queries")
    public FeaturesConfig setOptimizeMetadataQueries(boolean optimizeMetadataQueries)
    {
        this.optimizeMetadataQueries = optimizeMetadataQueries;
        return this;
    }

    public boolean isUseMarkDistinct()
    {
        return useMarkDistinct;
    }

    @Config("optimizer.use-mark-distinct")
    public FeaturesConfig setUseMarkDistinct(boolean value)
    {
        this.useMarkDistinct = value;
        return this;
    }

    public boolean isPreferPartialAggregation()
    {
        return preferPartialAggregation;
    }

    @Config("optimizer.prefer-partial-aggregation")
    public FeaturesConfig setPreferPartialAggregation(boolean value)
    {
        this.preferPartialAggregation = value;
        return this;
    }

    public boolean isOptimizeTopNRanking()
    {
        return optimizeTopNRanking;
    }

    @Config("optimizer.optimize-top-n-ranking")
    @LegacyConfig("optimizer.optimize-top-n-row-number")
    public FeaturesConfig setOptimizeTopNRanking(boolean optimizeTopNRanking)
    {
        this.optimizeTopNRanking = optimizeTopNRanking;
        return this;
    }

    public boolean isOptimizeHashGeneration()
    {
        return optimizeHashGeneration;
    }

    @Config("optimizer.optimize-hash-generation")
    public FeaturesConfig setOptimizeHashGeneration(boolean optimizeHashGeneration)
    {
        this.optimizeHashGeneration = optimizeHashGeneration;
        return this;
    }

    public boolean isPushTableWriteThroughUnion()
    {
        return pushTableWriteThroughUnion;
    }

    @Config("optimizer.push-table-write-through-union")
    public FeaturesConfig setPushTableWriteThroughUnion(boolean pushTableWriteThroughUnion)
    {
        this.pushTableWriteThroughUnion = pushTableWriteThroughUnion;
        return this;
    }

    public boolean isDictionaryAggregation()
    {
        return dictionaryAggregation;
    }

    @Config("optimizer.dictionary-aggregation")
    public FeaturesConfig setDictionaryAggregation(boolean dictionaryAggregation)
    {
        this.dictionaryAggregation = dictionaryAggregation;
        return this;
    }

    @Min(2)
    public int getRe2JDfaStatesLimit()
    {
        return re2JDfaStatesLimit;
    }

    @Config("re2j.dfa-states-limit")
    public FeaturesConfig setRe2JDfaStatesLimit(int re2JDfaStatesLimit)
    {
        this.re2JDfaStatesLimit = re2JDfaStatesLimit;
        return this;
    }

    @Min(0)
    public int getRe2JDfaRetries()
    {
        return re2JDfaRetries;
    }

    @Config("re2j.dfa-retries")
    public FeaturesConfig setRe2JDfaRetries(int re2JDfaRetries)
    {
        this.re2JDfaRetries = re2JDfaRetries;
        return this;
    }

    public RegexLibrary getRegexLibrary()
    {
        return regexLibrary;
    }

    @Config("regex-library")
    public FeaturesConfig setRegexLibrary(RegexLibrary regexLibrary)
    {
        this.regexLibrary = regexLibrary;
        return this;
    }

    public boolean isSpillEnabled()
    {
        return spillEnabled;
    }

    @Config(SPILL_ENABLED)
    @LegacyConfig("experimental.spill-enabled")
    public FeaturesConfig setSpillEnabled(boolean spillEnabled)
    {
        this.spillEnabled = spillEnabled;
        return this;
    }

    public boolean isSpillOrderBy()
    {
        return spillOrderBy;
    }

    @Config("spill-order-by")
    @LegacyConfig("experimental.spill-order-by")
    public FeaturesConfig setSpillOrderBy(boolean spillOrderBy)
    {
        this.spillOrderBy = spillOrderBy;
        return this;
    }

    public boolean isSpillWindowOperator()
    {
        return spillWindowOperator;
    }

    @Config("spill-window-operator")
    @LegacyConfig("experimental.spill-window-operator")
    public FeaturesConfig setSpillWindowOperator(boolean spillWindowOperator)
    {
        this.spillWindowOperator = spillWindowOperator;
        return this;
    }

    public Duration getIterativeOptimizerTimeout()
    {
        return iterativeOptimizerTimeout;
    }

    @Config("iterative-optimizer-timeout")
    @LegacyConfig("experimental.iterative-optimizer-timeout")
    public FeaturesConfig setIterativeOptimizerTimeout(Duration timeout)
    {
        this.iterativeOptimizerTimeout = timeout;
        return this;
    }

    public boolean isEnableStatsCalculator()
    {
        return enableStatsCalculator;
    }

    @Config("enable-stats-calculator")
    @LegacyConfig("experimental.enable-stats-calculator")
    public FeaturesConfig setEnableStatsCalculator(boolean enableStatsCalculator)
    {
        this.enableStatsCalculator = enableStatsCalculator;
        return this;
    }

    public boolean isStatisticsPrecalculationForPushdownEnabled()
    {
        return statisticsPrecalculationForPushdownEnabled;
    }

    @Config("statistics-precalculation-for-pushdown.enabled")
    public FeaturesConfig setStatisticsPrecalculationForPushdownEnabled(boolean statisticsPrecalculationForPushdownEnabled)
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
    public FeaturesConfig setCollectPlanStatisticsForAllQueries(boolean collectPlanStatisticsForAllQueries)
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
    public FeaturesConfig setIgnoreStatsCalculatorFailures(boolean ignoreStatsCalculatorFailures)
    {
        this.ignoreStatsCalculatorFailures = ignoreStatsCalculatorFailures;
        return this;
    }

    @Config("optimizer.default-filter-factor-enabled")
    public FeaturesConfig setDefaultFilterFactorEnabled(boolean defaultFilterFactorEnabled)
    {
        this.defaultFilterFactorEnabled = defaultFilterFactorEnabled;
        return this;
    }

    public boolean isDefaultFilterFactorEnabled()
    {
        return defaultFilterFactorEnabled;
    }

    public boolean isEnableForcedExchangeBelowGroupId()
    {
        return enableForcedExchangeBelowGroupId;
    }

    @Config("enable-forced-exchange-below-group-id")
    public FeaturesConfig setEnableForcedExchangeBelowGroupId(boolean enableForcedExchangeBelowGroupId)
    {
        this.enableForcedExchangeBelowGroupId = enableForcedExchangeBelowGroupId;
        return this;
    }

    public DataSize getAggregationOperatorUnspillMemoryLimit()
    {
        return aggregationOperatorUnspillMemoryLimit;
    }

    @Config("aggregation-operator-unspill-memory-limit")
    @LegacyConfig("experimental.aggregation-operator-unspill-memory-limit")
    public FeaturesConfig setAggregationOperatorUnspillMemoryLimit(DataSize aggregationOperatorUnspillMemoryLimit)
    {
        this.aggregationOperatorUnspillMemoryLimit = aggregationOperatorUnspillMemoryLimit;
        return this;
    }

    public List<Path> getSpillerSpillPaths()
    {
        return spillerSpillPaths;
    }

    @Config(SPILLER_SPILL_PATH)
    @LegacyConfig("experimental.spiller-spill-path")
    public FeaturesConfig setSpillerSpillPaths(String spillPaths)
    {
        List<String> spillPathsSplit = ImmutableList.copyOf(Splitter.on(",").trimResults().omitEmptyStrings().split(spillPaths));
        this.spillerSpillPaths = spillPathsSplit.stream().map(Paths::get).collect(toImmutableList());
        return this;
    }

    @Min(1)
    public int getSpillerThreads()
    {
        return spillerThreads;
    }

    @Config("spiller-threads")
    @LegacyConfig("experimental.spiller-threads")
    public FeaturesConfig setSpillerThreads(int spillerThreads)
    {
        this.spillerThreads = spillerThreads;
        return this;
    }

    @DecimalMin("0.0")
    @DecimalMax("1.0")
    public double getMemoryRevokingThreshold()
    {
        return memoryRevokingThreshold;
    }

    @Config("memory-revoking-threshold")
    @LegacyConfig("experimental.memory-revoking-threshold")
    @ConfigDescription("Revoke memory when memory pool is filled over threshold")
    public FeaturesConfig setMemoryRevokingThreshold(double memoryRevokingThreshold)
    {
        this.memoryRevokingThreshold = memoryRevokingThreshold;
        return this;
    }

    @DecimalMin("0.0")
    @DecimalMax("1.0")
    public double getMemoryRevokingTarget()
    {
        return memoryRevokingTarget;
    }

    @Config("memory-revoking-target")
    @LegacyConfig("experimental.memory-revoking-target")
    @ConfigDescription("When revoking memory, try to revoke so much that pool is filled below target at the end")
    public FeaturesConfig setMemoryRevokingTarget(double memoryRevokingTarget)
    {
        this.memoryRevokingTarget = memoryRevokingTarget;
        return this;
    }

    public double getSpillMaxUsedSpaceThreshold()
    {
        return spillMaxUsedSpaceThreshold;
    }

    @Config("spiller-max-used-space-threshold")
    @LegacyConfig("experimental.spiller-max-used-space-threshold")
    public FeaturesConfig setSpillMaxUsedSpaceThreshold(double spillMaxUsedSpaceThreshold)
    {
        this.spillMaxUsedSpaceThreshold = spillMaxUsedSpaceThreshold;
        return this;
    }

    public boolean isOptimizeMixedDistinctAggregations()
    {
        return optimizeMixedDistinctAggregations;
    }

    @Config("optimizer.optimize-mixed-distinct-aggregations")
    public FeaturesConfig setOptimizeMixedDistinctAggregations(boolean value)
    {
        this.optimizeMixedDistinctAggregations = value;
        return this;
    }

    public boolean isExchangeCompressionEnabled()
    {
        return exchangeCompressionEnabled;
    }

    @Config("exchange.compression-enabled")
    public FeaturesConfig setExchangeCompressionEnabled(boolean exchangeCompressionEnabled)
    {
        this.exchangeCompressionEnabled = exchangeCompressionEnabled;
        return this;
    }

    public DataIntegrityVerification getExchangeDataIntegrityVerification()
    {
        return exchangeDataIntegrityVerification;
    }

    @Config("exchange.data-integrity-verification")
    public FeaturesConfig setExchangeDataIntegrityVerification(DataIntegrityVerification exchangeDataIntegrityVerification)
    {
        this.exchangeDataIntegrityVerification = exchangeDataIntegrityVerification;
        return this;
    }

    public boolean isEnableIntermediateAggregations()
    {
        return enableIntermediateAggregations;
    }

    @Config("optimizer.enable-intermediate-aggregations")
    public FeaturesConfig setEnableIntermediateAggregations(boolean enableIntermediateAggregations)
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
    public FeaturesConfig setPushAggregationThroughOuterJoin(boolean pushAggregationThroughOuterJoin)
    {
        this.pushAggregationThroughOuterJoin = pushAggregationThroughOuterJoin;
        return this;
    }

    public boolean isPushPartialAggregationThoughJoin()
    {
        return pushPartialAggregationThoughJoin;
    }

    @Config("optimizer.push-partial-aggregation-through-join")
    public FeaturesConfig setPushPartialAggregationThoughJoin(boolean pushPartialAggregationThoughJoin)
    {
        this.pushPartialAggregationThoughJoin = pushPartialAggregationThoughJoin;
        return this;
    }

    public boolean isParseDecimalLiteralsAsDouble()
    {
        return parseDecimalLiteralsAsDouble;
    }

    @Config("parse-decimal-literals-as-double")
    public FeaturesConfig setParseDecimalLiteralsAsDouble(boolean parseDecimalLiteralsAsDouble)
    {
        this.parseDecimalLiteralsAsDouble = parseDecimalLiteralsAsDouble;
        return this;
    }

    public boolean isForceSingleNodeOutput()
    {
        return forceSingleNodeOutput;
    }

    @Config("optimizer.force-single-node-output")
    public FeaturesConfig setForceSingleNodeOutput(boolean value)
    {
        this.forceSingleNodeOutput = value;
        return this;
    }

    public boolean isPagesIndexEagerCompactionEnabled()
    {
        return pagesIndexEagerCompactionEnabled;
    }

    @Config("pages-index.eager-compaction-enabled")
    public FeaturesConfig setPagesIndexEagerCompactionEnabled(boolean pagesIndexEagerCompactionEnabled)
    {
        this.pagesIndexEagerCompactionEnabled = pagesIndexEagerCompactionEnabled;
        return this;
    }

    @MaxDataSize("1MB")
    public DataSize getFilterAndProjectMinOutputPageSize()
    {
        return filterAndProjectMinOutputPageSize;
    }

    @Config("filter-and-project-min-output-page-size")
    @LegacyConfig("experimental.filter-and-project-min-output-page-size")
    public FeaturesConfig setFilterAndProjectMinOutputPageSize(DataSize filterAndProjectMinOutputPageSize)
    {
        this.filterAndProjectMinOutputPageSize = filterAndProjectMinOutputPageSize;
        return this;
    }

    @Min(0)
    public int getFilterAndProjectMinOutputPageRowCount()
    {
        return filterAndProjectMinOutputPageRowCount;
    }

    @Config("filter-and-project-min-output-page-row-count")
    @LegacyConfig("experimental.filter-and-project-min-output-page-row-count")
    public FeaturesConfig setFilterAndProjectMinOutputPageRowCount(int filterAndProjectMinOutputPageRowCount)
    {
        this.filterAndProjectMinOutputPageRowCount = filterAndProjectMinOutputPageRowCount;
        return this;
    }

    public boolean isDistributedSortEnabled()
    {
        return distributedSort;
    }

    @Config("distributed-sort")
    public FeaturesConfig setDistributedSortEnabled(boolean enabled)
    {
        distributedSort = enabled;
        return this;
    }

    public int getMaxRecursionDepth()
    {
        return maxRecursionDepth;
    }

    @Config("max-recursion-depth")
    @ConfigDescription("Maximum recursion depth for recursive common table expression")
    public FeaturesConfig setMaxRecursionDepth(int maxRecursionDepth)
    {
        this.maxRecursionDepth = maxRecursionDepth;
        return this;
    }

    public int getMaxGroupingSets()
    {
        return maxGroupingSets;
    }

    @Config("analyzer.max-grouping-sets")
    public FeaturesConfig setMaxGroupingSets(int maxGroupingSets)
    {
        this.maxGroupingSets = maxGroupingSets;
        return this;
    }

    public boolean isLateMaterializationEnabled()
    {
        return lateMaterializationEnabled;
    }

    @Config("experimental.late-materialization.enabled")
    @LegacyConfig("experimental.work-processor-pipelines")
    public FeaturesConfig setLateMaterializationEnabled(boolean lateMaterializationEnabled)
    {
        this.lateMaterializationEnabled = lateMaterializationEnabled;
        return this;
    }

    public boolean isSkipRedundantSort()
    {
        return skipRedundantSort;
    }

    @Config("optimizer.skip-redundant-sort")
    public FeaturesConfig setSkipRedundantSort(boolean value)
    {
        this.skipRedundantSort = value;
        return this;
    }

    public boolean isPredicatePushdownUseTableProperties()
    {
        return predicatePushdownUseTableProperties;
    }

    @Config("optimizer.predicate-pushdown-use-table-properties")
    public FeaturesConfig setPredicatePushdownUseTableProperties(boolean predicatePushdownUseTableProperties)
    {
        this.predicatePushdownUseTableProperties = predicatePushdownUseTableProperties;
        return this;
    }

    public boolean isIgnoreDownstreamPreferences()
    {
        return ignoreDownstreamPreferences;
    }

    @Config("optimizer.ignore-downstream-preferences")
    public FeaturesConfig setIgnoreDownstreamPreferences(boolean ignoreDownstreamPreferences)
    {
        this.ignoreDownstreamPreferences = ignoreDownstreamPreferences;
        return this;
    }

    public boolean isRewriteFilteringSemiJoinToInnerJoin()
    {
        return rewriteFilteringSemiJoinToInnerJoin;
    }

    @Config("optimizer.rewrite-filtering-semi-join-to-inner-join")
    public FeaturesConfig setRewriteFilteringSemiJoinToInnerJoin(boolean rewriteFilteringSemiJoinToInnerJoin)
    {
        this.rewriteFilteringSemiJoinToInnerJoin = rewriteFilteringSemiJoinToInnerJoin;
        return this;
    }

    public boolean isOptimizeDuplicateInsensitiveJoins()
    {
        return optimizeDuplicateInsensitiveJoins;
    }

    @Config("optimizer.optimize-duplicate-insensitive-joins")
    public FeaturesConfig setOptimizeDuplicateInsensitiveJoins(boolean optimizeDuplicateInsensitiveJoins)
    {
        this.optimizeDuplicateInsensitiveJoins = optimizeDuplicateInsensitiveJoins;
        return this;
    }

    public boolean isUseLegacyWindowFilterPushdown()
    {
        return useLegacyWindowFilterPushdown;
    }

    @Config("optimizer.use-legacy-window-filter-pushdown")
    public FeaturesConfig setUseLegacyWindowFilterPushdown(boolean useLegacyWindowFilterPushdown)
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
    public FeaturesConfig setUseTableScanNodePartitioning(boolean useTableScanNodePartitioning)
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
    public FeaturesConfig setTableScanNodePartitioningMinBucketToTaskRatio(double tableScanNodePartitioningMinBucketToTaskRatio)
    {
        this.tableScanNodePartitioningMinBucketToTaskRatio = tableScanNodePartitioningMinBucketToTaskRatio;
        return this;
    }

    public boolean isMergeProjectWithValues()
    {
        return mergeProjectWithValues;
    }

    @Config("optimizer.merge-project-with-values")
    public FeaturesConfig setMergeProjectWithValues(boolean mergeProjectWithValues)
    {
        this.mergeProjectWithValues = mergeProjectWithValues;
        return this;
    }

    public boolean isLegacyCatalogRoles()
    {
        return legacyCatalogRoles;
    }

    @Config("deprecated.legacy-catalog-roles")
    @ConfigDescription("Enable legacy role management syntax that assumed all roles are catalog scoped")
    public FeaturesConfig setLegacyCatalogRoles(boolean legacyCatalogRoles)
    {
        this.legacyCatalogRoles = legacyCatalogRoles;
        return this;
    }

    public boolean isDisableSetPropertiesSecurityCheckForCreateDdl()
    {
        return disableSetPropertiesSecurityCheckForCreateDdl;
    }

    @Config("deprecated.disable-set-properties-security-check-for-create-ddl")
    public FeaturesConfig setDisableSetPropertiesSecurityCheckForCreateDdl(boolean disableSetPropertiesSecurityCheckForCreateDdl)
    {
        this.disableSetPropertiesSecurityCheckForCreateDdl = disableSetPropertiesSecurityCheckForCreateDdl;
        return this;
    }

    @Deprecated
    public boolean isIncrementalHashArrayLoadFactorEnabled()
    {
        return incrementalHashArrayLoadFactorEnabled;
    }

    @Deprecated
    @Config("incremental-hash-array-load-factor.enabled")
    @ConfigDescription("Use smaller load factor for small hash arrays in order to improve performance")
    public FeaturesConfig setIncrementalHashArrayLoadFactorEnabled(boolean incrementalHashArrayLoadFactorEnabled)
    {
        this.incrementalHashArrayLoadFactorEnabled = incrementalHashArrayLoadFactorEnabled;
        return this;
    }

    public boolean isHideInaccesibleColumns()
    {
        return hideInaccesibleColumns;
    }

    @Config("hide-inaccessible-columns")
    @ConfigDescription("When enabled non-accessible columns are silently filtered from results from SELECT * statements")
    public FeaturesConfig setHideInaccesibleColumns(boolean hideInaccesibleColumns)
    {
        this.hideInaccesibleColumns = hideInaccesibleColumns;
        return this;
    }

    public boolean isAllowSetViewAuthorization()
    {
        return allowSetViewAuthorization;
    }

    @Config("legacy.allow-set-view-authorization")
    @ConfigDescription("For security reasons ALTER VIEW SET AUTHORIZATION is disabled for SECURITY DEFINER; " +
            "setting this option to true will re-enable this functionality")
    public FeaturesConfig setAllowSetViewAuthorization(boolean allowSetViewAuthorization)
    {
        this.allowSetViewAuthorization = allowSetViewAuthorization;
        return this;
    }
}
