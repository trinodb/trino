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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;

import javax.validation.constraints.DecimalMax;
import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.apache.hudi.common.model.HoodieFileFormat.PARQUET;

public class HudiConfig
{
    private String baseFileFormat = PARQUET.name();
    private boolean metadataEnabled;
    private boolean shouldSkipMetaStoreForPartition;
    private boolean shouldUseParquetColumnNames = true;
    private int partitionScannerParallelism = 16;
    private int splitGeneratorParallelism = 16;
    private int minPartitionBatchSize = 10;
    private int maxPartitionBatchSize = 100;
    private boolean sizeBasedSplitWeightsEnabled = true;
    private DataSize standardSplitWeightSize = DataSize.of(128, MEGABYTE);
    private double minimumAssignedSplitWeight = 0.05;

    @NotNull
    public String getBaseFileFormat()
    {
        return baseFileFormat;
    }

    @Config("hudi.base-file-format")
    public HudiConfig setBaseFileFormat(String baseFileFormat)
    {
        this.baseFileFormat = baseFileFormat;
        return this;
    }

    @Config("hudi.metadata-enabled")
    @ConfigDescription("Fetch the list of file names and sizes from metadata rather than storage")
    public HudiConfig setMetadataEnabled(boolean metadataEnabled)
    {
        this.metadataEnabled = metadataEnabled;
        return this;
    }

    @NotNull
    public boolean isMetadataEnabled()
    {
        return this.metadataEnabled;
    }

    @Config("hudi.skip-metastore-for-partition")
    @ConfigDescription("By default, partition info is fetched from the metastore. " +
            "When this config is enabled, then the partition info is fetched using Hudi's partition extractor and relative partition path.")
    public HudiConfig setSkipMetaStoreForPartition(boolean shouldSkipMetaStoreForPartition)
    {
        this.shouldSkipMetaStoreForPartition = shouldSkipMetaStoreForPartition;
        return this;
    }

    @NotNull
    public boolean getSkipMetaStoreForPartition()
    {
        return this.shouldSkipMetaStoreForPartition;
    }

    @Config("hudi.use-parquet-column-names")
    @ConfigDescription("Access parquet columns using names from the file. If disabled, then columns are accessed using index."
            + "Only applicable to parquet file format.")
    public HudiConfig setUseParquetColumnNames(boolean shouldUseParquetColumnNames)
    {
        this.shouldUseParquetColumnNames = shouldUseParquetColumnNames;
        return this;
    }

    @NotNull
    public boolean getUseParquetColumnNames()
    {
        return this.shouldUseParquetColumnNames;
    }

    @Config("hudi.partition-scanner-parallelism")
    @ConfigDescription("Number of threads to use for partition scanners")
    public HudiConfig setPartitionScannerParallelism(int partitionScannerParallelism)
    {
        this.partitionScannerParallelism = partitionScannerParallelism;
        return this;
    }

    @NotNull
    public int getPartitionScannerParallelism()
    {
        return this.partitionScannerParallelism;
    }

    @Config("hudi.split-generator-parallelism")
    @ConfigDescription("Number of threads to use for split generators")
    public HudiConfig setSplitGeneratorParallelism(int splitGeneratorParallelism)
    {
        this.splitGeneratorParallelism = splitGeneratorParallelism;
        return this;
    }

    @NotNull
    public int getSplitGeneratorParallelism()
    {
        return this.splitGeneratorParallelism;
    }

    @Config("hudi.min-partition-batch-size")
    public HudiConfig setMinPartitionBatchSize(int minPartitionBatchSize)
    {
        this.minPartitionBatchSize = minPartitionBatchSize;
        return this;
    }

    @Min(1)
    public int getMinPartitionBatchSize()
    {
        return minPartitionBatchSize;
    }

    @Config("hudi.max-partition-batch-size")
    public HudiConfig setMaxPartitionBatchSize(int maxPartitionBatchSize)
    {
        this.maxPartitionBatchSize = maxPartitionBatchSize;
        return this;
    }

    @Min(1)
    public int getMaxPartitionBatchSize()
    {
        return maxPartitionBatchSize;
    }

    @Config("hudi.size-based-split-weights-enabled")
    @ConfigDescription("Unlike uniform splitting, size-based splitting ensures that each batch of splits has enough data to process. " +
            "By default, it is enabled to improve performance")
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
            + "when size based split weights are enabled")
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
    @ConfigDescription("Minimum weight that a split can be assigned when size based split weights are enabled")
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
}
