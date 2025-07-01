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
package io.trino.plugin.hudi;

import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;

@DefunctConfig({
        "hudi.min-partition-batch-size",
        "hudi.max-partition-batch-size"
})
public class HudiConfig
{
    private List<String> columnsToHide = ImmutableList.of();
    private boolean tableStatisticsEnabled = true;
    private int tableStatisticsExecutorParallelism = 4;
    private boolean metadataEnabled;
    private boolean shouldUseParquetColumnNames = true;
    private boolean shouldUseParquetColumnIndex;
    private boolean sizeBasedSplitWeightsEnabled = true;
    private DataSize standardSplitWeightSize = DataSize.of(128, MEGABYTE);
    private double minimumAssignedSplitWeight = 0.05;
    private DataSize targetSplitSize = DataSize.of(128, MEGABYTE);
    private int maxSplitsPerSecond = Integer.MAX_VALUE;
    private int maxOutstandingSplits = 10000;
    private int splitLoaderParallelism = 10;
    private int splitGeneratorParallelism = 4;
    private long perTransactionMetastoreCacheMaximumSize = 2000;
    private boolean queryPartitionFilterRequired;
    private boolean ignoreAbsentPartitions;
    private Duration dynamicFilteringWaitTimeout = new Duration(1, SECONDS);

    // Internal configuration for debugging and testing
    private boolean isRecordLevelIndexEnabled = true;
    private boolean isSecondaryIndexEnabled = true;
    private boolean isColumnStatsIndexEnabled = true;
    private boolean isPartitionStatsIndexEnabled = true;
    private Duration columnStatsWaitTimeout = new Duration(1, SECONDS);
    private Duration recordIndexWaitTimeout = new Duration(2, SECONDS);
    private Duration secondaryIndexWaitTimeout = new Duration(2, SECONDS);

    public List<String> getColumnsToHide()
    {
        return columnsToHide;
    }

    @Config("hudi.columns-to-hide")
    @ConfigDescription("List of column names that will be hidden from the query output. " +
            "It can be used to hide Hudi meta fields. By default, no fields are hidden.")
    public HudiConfig setColumnsToHide(List<String> columnsToHide)
    {
        this.columnsToHide = columnsToHide.stream()
                .map(s -> s.toLowerCase(ENGLISH))
                .collect(toImmutableList());
        return this;
    }

    @Config("hudi.table-statistics-enabled")
    @ConfigDescription("Enable table statistics for query planning.")
    public HudiConfig setTableStatisticsEnabled(boolean tableStatisticsEnabled)
    {
        this.tableStatisticsEnabled = tableStatisticsEnabled;
        return this;
    }

    public boolean isTableStatisticsEnabled()
    {
        return this.tableStatisticsEnabled;
    }

    @Min(1)
    public int getTableStatisticsExecutorParallelism()
    {
        return tableStatisticsExecutorParallelism;
    }

    @Config("hudi.table-statistics-executor-parallelism")
    @ConfigDescription("Number of threads to asynchronously generate table statistics.")
    public HudiConfig setTableStatisticsExecutorParallelism(int parallelism)
    {
        this.tableStatisticsExecutorParallelism = parallelism;
        return this;
    }

    @Config("hudi.metadata-enabled")
    @ConfigDescription("Fetch the list of file names and sizes from Hudi metadata table rather than storage.")
    public HudiConfig setMetadataEnabled(boolean metadataEnabled)
    {
        this.metadataEnabled = metadataEnabled;
        return this;
    }

    public boolean isMetadataEnabled()
    {
        return this.metadataEnabled;
    }

    @Config("hudi.parquet.use-column-names")
    @ConfigDescription("Access Parquet columns using names from the file. If disabled, then columns are accessed using index."
            + "Only applicable to Parquet file format.")
    public HudiConfig setUseParquetColumnNames(boolean shouldUseParquetColumnNames)
    {
        this.shouldUseParquetColumnNames = shouldUseParquetColumnNames;
        return this;
    }

    public boolean getUseParquetColumnNames()
    {
        return this.shouldUseParquetColumnNames;
    }

    @Config("hudi.parquet.use-column-index")
    @ConfigDescription("Enable using Parquet column indexes")
    public HudiConfig setUseParquetColumnIndex(boolean shouldUseParquetColumnIndex)
    {
        this.shouldUseParquetColumnIndex = shouldUseParquetColumnIndex;
        return this;
    }

    public boolean isUseParquetColumnIndex()
    {
        return this.shouldUseParquetColumnIndex;
    }

    @Config("hudi.size-based-split-weights-enabled")
    @ConfigDescription("Unlike uniform splitting, size-based splitting ensures that each batch of splits has enough data to process. " +
            "By default, it is enabled to improve performance.")
    public HudiConfig setSizeBasedSplitWeightsEnabled(boolean sizeBasedSplitWeightsEnabled)
    {
        this.sizeBasedSplitWeightsEnabled = sizeBasedSplitWeightsEnabled;
        return this;
    }

    public boolean isSizeBasedSplitWeightsEnabled()
    {
        return sizeBasedSplitWeightsEnabled;
    }

    @Config("hudi.standard-split-weight-size")
    @ConfigDescription("The split size corresponding to the standard weight (1.0) "
            + "when size based split weights are enabled.")
    public HudiConfig setStandardSplitWeightSize(DataSize standardSplitWeightSize)
    {
        this.standardSplitWeightSize = standardSplitWeightSize;
        return this;
    }

    @NotNull
    public DataSize getStandardSplitWeightSize()
    {
        return standardSplitWeightSize;
    }

    @Config("hudi.minimum-assigned-split-weight")
    @ConfigDescription("Minimum weight that a split can be assigned when size based split weights are enabled.")
    public HudiConfig setMinimumAssignedSplitWeight(double minimumAssignedSplitWeight)
    {
        this.minimumAssignedSplitWeight = minimumAssignedSplitWeight;
        return this;
    }

    @DecimalMax("1")
    @DecimalMin(value = "0", inclusive = false)
    public double getMinimumAssignedSplitWeight()
    {
        return minimumAssignedSplitWeight;
    }

    @Config("hudi.target-split-size")
    @ConfigDescription("The target split size")
    public HudiConfig setTargetSplitSize(DataSize targetSplitSize)
    {
        this.targetSplitSize = targetSplitSize;
        return this;
    }

    @NotNull
    public DataSize getTargetSplitSize()
    {
        return targetSplitSize;
    }

    @Min(1)
    public int getMaxSplitsPerSecond()
    {
        return maxSplitsPerSecond;
    }

    @Config("hudi.max-splits-per-second")
    @ConfigDescription("Rate at which splits are enqueued for processing. The queue will throttle if this rate limit is breached.")
    public HudiConfig setMaxSplitsPerSecond(int maxSplitsPerSecond)
    {
        this.maxSplitsPerSecond = maxSplitsPerSecond;
        return this;
    }

    @Min(1)
    public int getMaxOutstandingSplits()
    {
        return maxOutstandingSplits;
    }

    @Config("hudi.max-outstanding-splits")
    @ConfigDescription("Maximum outstanding splits in a batch enqueued for processing.")
    public HudiConfig setMaxOutstandingSplits(int maxOutstandingSplits)
    {
        this.maxOutstandingSplits = maxOutstandingSplits;
        return this;
    }

    @Min(1)
    public int getSplitGeneratorParallelism()
    {
        return splitGeneratorParallelism;
    }

    @Config("hudi.split-generator-parallelism")
    @ConfigDescription("Number of threads to generate splits from partitions.")
    public HudiConfig setSplitGeneratorParallelism(int splitGeneratorParallelism)
    {
        this.splitGeneratorParallelism = splitGeneratorParallelism;
        return this;
    }

    @Min(1)
    public int getSplitLoaderParallelism()
    {
        return splitLoaderParallelism;
    }

    @Config("hudi.split-loader-parallelism")
    @ConfigDescription("Number of threads to run background split loader. A single background split loader is needed per query.")
    public HudiConfig setSplitLoaderParallelism(int splitLoaderParallelism)
    {
        this.splitLoaderParallelism = splitLoaderParallelism;
        return this;
    }

    @Min(1)
    public long getPerTransactionMetastoreCacheMaximumSize()
    {
        return perTransactionMetastoreCacheMaximumSize;
    }

    @Config("hudi.per-transaction-metastore-cache-maximum-size")
    public HudiConfig setPerTransactionMetastoreCacheMaximumSize(long perTransactionMetastoreCacheMaximumSize)
    {
        this.perTransactionMetastoreCacheMaximumSize = perTransactionMetastoreCacheMaximumSize;
        return this;
    }

    @Config("hudi.query-partition-filter-required")
    @ConfigDescription("Require a filter on at least one partition column")
    public HudiConfig setQueryPartitionFilterRequired(boolean queryPartitionFilterRequired)
    {
        this.queryPartitionFilterRequired = queryPartitionFilterRequired;
        return this;
    }

    public boolean isQueryPartitionFilterRequired()
    {
        return queryPartitionFilterRequired;
    }

    @Config("hudi.ignore-absent-partitions")
    public HudiConfig setIgnoreAbsentPartitions(boolean ignoreAbsentPartitions)
    {
        this.ignoreAbsentPartitions = ignoreAbsentPartitions;
        return this;
    }

    public boolean isIgnoreAbsentPartitions()
    {
        return ignoreAbsentPartitions;
    }

    @Config("hudi.index.record-level-index-enabled")
    @ConfigDescription("Internal configuration to control whether record level index is enabled for debugging/testing.")
    public HudiConfig setRecordLevelIndexEnabled(boolean isRecordLevelIndexEnabled)
    {
        this.isRecordLevelIndexEnabled = isRecordLevelIndexEnabled;
        return this;
    }

    public boolean isRecordLevelIndexEnabled()
    {
        return isRecordLevelIndexEnabled;
    }

    @Config("hudi.index.secondary-index-enabled")
    @ConfigDescription("Internal configuration to control whether secondary index is enabled for debugging/testing.")
    public HudiConfig setSecondaryIndexEnabled(boolean isSecondaryIndexEnabled)
    {
        this.isSecondaryIndexEnabled = isSecondaryIndexEnabled;
        return this;
    }

    public boolean isSecondaryIndexEnabled()
    {
        return isSecondaryIndexEnabled;
    }

    @Config("hudi.index.column-stats-index-enabled")
    @ConfigDescription("Internal configuration to control whether column stats index is enabled for debugging/testing.")
    public HudiConfig setColumnStatsIndexEnabled(boolean isColumnStatsIndexEnabled)
    {
        this.isColumnStatsIndexEnabled = isColumnStatsIndexEnabled;
        return this;
    }

    public boolean isColumnStatsIndexEnabled()
    {
        return isColumnStatsIndexEnabled;
    }

    @Config("hudi.index.partition-stats-index-enabled")
    @ConfigDescription("Internal configuration to control whether partition stats index is enabled for debugging/testing.")
    public HudiConfig setPartitionStatsIndexEnabled(boolean isPartitionStatsIndexEnabled)
    {
        this.isPartitionStatsIndexEnabled = isPartitionStatsIndexEnabled;
        return this;
    }

    public boolean isPartitionStatsIndexEnabled()
    {
        return isPartitionStatsIndexEnabled;
    }

    @Config("hudi.dynamic-filtering.wait-timeout")
    @ConfigDescription("Maximum timeout to wait for dynamic filtering, e.g. 1000ms, 20s, 2m, 1h")
    public HudiConfig setDynamicFilteringWaitTimeout(Duration dynamicFilteringWaitTimeout)
    {
        this.dynamicFilteringWaitTimeout = dynamicFilteringWaitTimeout;
        return this;
    }

    @NotNull
    public Duration getDynamicFilteringWaitTimeout()
    {
        return dynamicFilteringWaitTimeout;
    }

    @Config("hudi.index.column-stats.wait-timeout")
    @ConfigDescription("Maximum timeout to wait for loading column stats, e.g. 1000ms, 20s")
    public HudiConfig setColumnStatsWaitTimeout(Duration columnStatusWaitTimeout)
    {
        this.columnStatsWaitTimeout = columnStatusWaitTimeout;
        return this;
    }

    @NotNull
    public Duration getColumnStatsWaitTimeout()
    {
        return columnStatsWaitTimeout;
    }

    @Config("hudi.index.record-index.wait-timeout")
    @ConfigDescription("Maximum timeout to wait for loading record index, e.g. 1000ms, 20s")
    public HudiConfig setRecordIndexWaitTimeout(Duration recordIndexWaitTimeout)
    {
        this.recordIndexWaitTimeout = recordIndexWaitTimeout;
        return this;
    }

    @NotNull
    public Duration getRecordIndexWaitTimeout()
    {
        return recordIndexWaitTimeout;
    }

    @Config("hudi.index.secondary-index.wait-timeout")
    @ConfigDescription("Maximum timeout to wait for loading secondary index, e.g. 1000ms, 20s")
    public HudiConfig setSecondaryIndexWaitTimeout(Duration secondaryIndexWaitTimeout)
    {
        this.secondaryIndexWaitTimeout = secondaryIndexWaitTimeout;
        return this;
    }

    @NotNull
    public Duration getSecondaryIndexWaitTimeout()
    {
        return secondaryIndexWaitTimeout;
    }
}
