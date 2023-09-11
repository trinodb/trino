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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import io.airlift.units.DataSize;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.util.List;

import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Locale.ENGLISH;

@DefunctConfig({
        "hudi.min-partition-batch-size",
        "hudi.max-partition-batch-size",
        "hudi.metadata-enabled",
})
public class HudiConfig
{
    private static final Splitter COMMA_SPLITTER = Splitter.on(",").omitEmptyStrings().trimResults();

    private List<String> columnsToHide = ImmutableList.of();
    private boolean shouldUseParquetColumnNames = true;
    private boolean sizeBasedSplitWeightsEnabled = true;
    private DataSize standardSplitWeightSize = DataSize.of(128, MEGABYTE);
    private double minimumAssignedSplitWeight = 0.05;
    private int maxSplitsPerSecond = Integer.MAX_VALUE;
    private int maxOutstandingSplits = 1000;
    private int splitLoaderParallelism = 4;
    private int splitGeneratorParallelism = 4;
    private long perTransactionMetastoreCacheMaximumSize = 2000;

    public List<String> getColumnsToHide()
    {
        return columnsToHide;
    }

    @Config("hudi.columns-to-hide")
    @ConfigDescription("List of column names that will be hidden from the query output. " +
            "It can be used to hide Hudi meta fields. By default, no fields are hidden.")
    public HudiConfig setColumnsToHide(String columnsToHide)
    {
        this.columnsToHide = COMMA_SPLITTER.splitToStream(nullToEmpty(columnsToHide))
                .map(s -> s.toLowerCase(ENGLISH))
                .collect(toImmutableList());
        return this;
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
}
