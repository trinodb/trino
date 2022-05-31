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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.plugin.hive.HiveCompressionCodec;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestDeltaLakeConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(DeltaLakeConfig.class)
                .setDataFileCacheSize(DeltaLakeConfig.DEFAULT_DATA_FILE_CACHE_SIZE)
                .setMetadataCacheTtl(new Duration(5, TimeUnit.MINUTES))
                .setDomainCompactionThreshold(100)
                .setMaxSplitsPerSecond(Integer.MAX_VALUE)
                .setMaxOutstandingSplits(1_000)
                .setMaxInitialSplits(200)
                .setMaxInitialSplitSize(DataSize.of(32, DataSize.Unit.MEGABYTE))
                .setMaxSplitSize(DataSize.of(64, DataSize.Unit.MEGABYTE))
                .setMaxPartitionsPerWriter(100)
                .setUnsafeWritesEnabled(false)
                .setDefaultCheckpointWritingInterval(10)
                .setIgnoreCheckpointWriteFailures(false)
                .setCheckpointRowStatisticsWritingEnabled(true)
                .setVacuumMinRetention(new Duration(7, DAYS))
                .setHiveCatalogName(null)
                .setDynamicFilteringWaitTimeout(new Duration(0, SECONDS))
                .setTableStatisticsEnabled(true)
                .setExtendedStatisticsEnabled(true)
                .setCompressionCodec(HiveCompressionCodec.SNAPPY));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("delta.metadata.cache-ttl", "10m")
                .put("delta.metadata.live-files.cache-size", "0 MB")
                .put("delta.domain-compaction-threshold", "500")
                .put("delta.max-outstanding-splits", "200")
                .put("delta.max-splits-per-second", "10")
                .put("delta.max-initial-splits", "5")
                .put("delta.max-initial-split-size", "1 GB")
                .put("delta.max-split-size", "10 MB")
                .put("delta.max-partitions-per-writer", "200")
                .put("delta.enable-non-concurrent-writes", "true")
                .put("delta.default-checkpoint-writing-interval", "15")
                .put("delta.experimental.ignore-checkpoint-write-failures", "true")
                .put("delta.checkpoint-row-statistics-writing.enabled", "false")
                .put("delta.vacuum.min-retention", "13h")
                .put("delta.hive-catalog-name", "hive")
                .put("delta.dynamic-filtering.wait-timeout", "30m")
                .put("delta.table-statistics-enabled", "false")
                .put("delta.extended-statistics.enabled", "false")
                .put("delta.compression-codec", "GZIP")
                .buildOrThrow();

        DeltaLakeConfig expected = new DeltaLakeConfig()
                .setDataFileCacheSize(DataSize.succinctBytes(0))
                .setMetadataCacheTtl(new Duration(10, TimeUnit.MINUTES))
                .setDomainCompactionThreshold(500)
                .setMaxOutstandingSplits(200)
                .setMaxSplitsPerSecond(10)
                .setMaxInitialSplits(5)
                .setMaxInitialSplitSize(DataSize.of(1, DataSize.Unit.GIGABYTE))
                .setMaxSplitSize(DataSize.of(10, DataSize.Unit.MEGABYTE))
                .setMaxPartitionsPerWriter(200)
                .setUnsafeWritesEnabled(true)
                .setDefaultCheckpointWritingInterval(15)
                .setIgnoreCheckpointWriteFailures(true)
                .setCheckpointRowStatisticsWritingEnabled(false)
                .setVacuumMinRetention(new Duration(13, HOURS))
                .setHiveCatalogName("hive")
                .setDynamicFilteringWaitTimeout(new Duration(30, MINUTES))
                .setTableStatisticsEnabled(false)
                .setExtendedStatisticsEnabled(false)
                .setCompressionCodec(HiveCompressionCodec.GZIP);

        assertFullMapping(properties, expected);
    }
}
