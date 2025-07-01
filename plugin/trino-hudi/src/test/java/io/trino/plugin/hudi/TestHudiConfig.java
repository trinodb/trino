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
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class TestHudiConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(HudiConfig.class)
                .setColumnsToHide(ImmutableList.of())
                .setTableStatisticsEnabled(true)
                .setMetadataEnabled(false)
                .setUseParquetColumnNames(true)
                .setUseParquetColumnIndex(false)
                .setTableStatisticsExecutorParallelism(4)
                .setSizeBasedSplitWeightsEnabled(true)
                .setStandardSplitWeightSize(DataSize.of(128, MEGABYTE))
                .setMinimumAssignedSplitWeight(0.05)
                .setTargetSplitSize(DataSize.of(128, MEGABYTE))
                .setMaxSplitsPerSecond(Integer.MAX_VALUE)
                .setMaxOutstandingSplits(10000)
                .setSplitLoaderParallelism(10)
                .setSplitGeneratorParallelism(4)
                .setPerTransactionMetastoreCacheMaximumSize(2000)
                .setQueryPartitionFilterRequired(false)
                .setIgnoreAbsentPartitions(false)
                .setRecordLevelIndexEnabled(true)
                .setSecondaryIndexEnabled(true)
                .setColumnStatsIndexEnabled(true)
                .setPartitionStatsIndexEnabled(true)
                .setDynamicFilteringWaitTimeout(Duration.valueOf("1s"))
                .setColumnStatsWaitTimeout(Duration.valueOf("1s"))
                .setRecordIndexWaitTimeout(Duration.valueOf("2s"))
                .setSecondaryIndexWaitTimeout(Duration.valueOf("2s")));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hudi.columns-to-hide", "_hoodie_record_key")
                .put("hudi.table-statistics-enabled", "false")
                .put("hudi.metadata-enabled", "true")
                .put("hudi.parquet.use-column-names", "false")
                .put("hudi.parquet.use-column-index", "true")
                .put("hudi.table-statistics-executor-parallelism", "16")
                .put("hudi.size-based-split-weights-enabled", "false")
                .put("hudi.standard-split-weight-size", "64MB")
                .put("hudi.minimum-assigned-split-weight", "0.1")
                .put("hudi.target-split-size", "32MB")
                .put("hudi.max-splits-per-second", "100")
                .put("hudi.max-outstanding-splits", "100")
                .put("hudi.split-loader-parallelism", "16")
                .put("hudi.split-generator-parallelism", "32")
                .put("hudi.per-transaction-metastore-cache-maximum-size", "1000")
                .put("hudi.query-partition-filter-required", "true")
                .put("hudi.ignore-absent-partitions", "true")
                .put("hudi.index.record-level-index-enabled", "false")
                .put("hudi.index.secondary-index-enabled", "false")
                .put("hudi.index.column-stats-index-enabled", "false")
                .put("hudi.index.partition-stats-index-enabled", "false")
                .put("hudi.dynamic-filtering.wait-timeout", "2s")
                .put("hudi.index.column-stats.wait-timeout", "2s")
                .put("hudi.index.record-index.wait-timeout", "1s")
                .put("hudi.index.secondary-index.wait-timeout", "1s")
                .buildOrThrow();

        HudiConfig expected = new HudiConfig()
                .setColumnsToHide(ImmutableList.of("_hoodie_record_key"))
                .setTableStatisticsEnabled(false)
                .setMetadataEnabled(true)
                .setUseParquetColumnNames(false)
                .setUseParquetColumnIndex(true)
                .setTableStatisticsExecutorParallelism(16)
                .setSizeBasedSplitWeightsEnabled(false)
                .setStandardSplitWeightSize(DataSize.of(64, MEGABYTE))
                .setMinimumAssignedSplitWeight(0.1)
                .setTargetSplitSize(DataSize.of(32, MEGABYTE))
                .setMaxSplitsPerSecond(100)
                .setMaxOutstandingSplits(100)
                .setSplitLoaderParallelism(16)
                .setSplitGeneratorParallelism(32)
                .setPerTransactionMetastoreCacheMaximumSize(1000)
                .setQueryPartitionFilterRequired(true)
                .setIgnoreAbsentPartitions(true)
                .setRecordLevelIndexEnabled(false)
                .setSecondaryIndexEnabled(false)
                .setColumnStatsIndexEnabled(false)
                .setPartitionStatsIndexEnabled(false)
                .setDynamicFilteringWaitTimeout(Duration.valueOf("2s"))
                .setColumnStatsWaitTimeout(Duration.valueOf("2s"))
                .setRecordIndexWaitTimeout(Duration.valueOf("1s"))
                .setSecondaryIndexWaitTimeout(Duration.valueOf("1s"));

        assertFullMapping(properties, expected);
    }
}
