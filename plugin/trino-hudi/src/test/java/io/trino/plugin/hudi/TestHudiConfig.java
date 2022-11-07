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

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

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
                .setColumnsToHide(null)
                .setMetadataEnabled(false)
                .setUseParquetColumnNames(true)
                .setMinPartitionBatchSize(10)
                .setMaxPartitionBatchSize(100)
                .setSizeBasedSplitWeightsEnabled(true)
                .setStandardSplitWeightSize(DataSize.of(128, MEGABYTE))
                .setMinimumAssignedSplitWeight(0.05)
                .setMaxSplitsPerSecond(Integer.MAX_VALUE)
                .setMaxOutstandingSplits(1000));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hudi.columns-to-hide", "_hoodie_record_key")
                .put("hudi.metadata-enabled", "true")
                .put("hudi.parquet.use-column-names", "false")
                .put("hudi.min-partition-batch-size", "5")
                .put("hudi.max-partition-batch-size", "50")
                .put("hudi.size-based-split-weights-enabled", "false")
                .put("hudi.standard-split-weight-size", "64MB")
                .put("hudi.minimum-assigned-split-weight", "0.1")
                .put("hudi.max-splits-per-second", "100")
                .put("hudi.max-outstanding-splits", "100")
                .buildOrThrow();

        HudiConfig expected = new HudiConfig()
                .setColumnsToHide("_hoodie_record_key")
                .setMetadataEnabled(true)
                .setUseParquetColumnNames(false)
                .setMinPartitionBatchSize(5)
                .setMaxPartitionBatchSize(50)
                .setSizeBasedSplitWeightsEnabled(false)
                .setStandardSplitWeightSize(DataSize.of(64, MEGABYTE))
                .setMinimumAssignedSplitWeight(0.1)
                .setMaxSplitsPerSecond(100)
                .setMaxOutstandingSplits(100);

        assertFullMapping(properties, expected);
    }
}
