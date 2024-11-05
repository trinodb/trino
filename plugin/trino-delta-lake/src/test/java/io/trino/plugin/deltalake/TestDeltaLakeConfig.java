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
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.plugin.hive.util.TestHiveUtil.nonDefaultTimeZone;
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
                .setDataFileCacheTtl(new Duration(30, MINUTES))
                .setMetadataCacheTtl(new Duration(5, TimeUnit.MINUTES))
                .setMetadataCacheMaxSize(1000)
                .setDomainCompactionThreshold(1000)
                .setMaxSplitsPerSecond(Integer.MAX_VALUE)
                .setMaxOutstandingSplits(1_000)
                .setMaxSplitSize(DataSize.of(64, DataSize.Unit.MEGABYTE))
                .setMinimumAssignedSplitWeight(0.05)
                .setMaxPartitionsPerWriter(100)
                .setUnsafeWritesEnabled(false)
                .setDefaultCheckpointWritingInterval(10)
                .setCheckpointFilteringEnabled(true)
                .setCheckpointRowStatisticsWritingEnabled(true)
                .setVacuumMinRetention(new Duration(7, DAYS))
                .setHiveCatalogName(null)
                .setDynamicFilteringWaitTimeout(new Duration(0, SECONDS))
                .setTableStatisticsEnabled(true)
                .setExtendedStatisticsEnabled(true)
                .setCollectExtendedStatisticsOnWrite(true)
                .setCompressionCodec(HiveCompressionCodec.SNAPPY)
                .setDeleteSchemaLocationsFallback(false)
                .setParquetTimeZone(TimeZone.getDefault().getID())
                .setPerTransactionMetastoreCacheMaximumSize(1000)
                .setStoreTableMetadataEnabled(false)
                .setStoreTableMetadataThreads(5)
                .setStoreTableMetadataInterval(new Duration(1, SECONDS))
                .setTargetMaxFileSize(DataSize.of(1, GIGABYTE))
                .setIdleWriterMinFileSize(DataSize.of(16, MEGABYTE))
                .setUniqueTableLocation(true)
                .setRegisterTableProcedureEnabled(false)
                .setProjectionPushdownEnabled(true)
                .setQueryPartitionFilterRequired(false)
                .setDeletionVectorsEnabled(false)
                .setDeltaLogFileSystemCacheDisabled(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("delta.metadata.cache-ttl", "10m")
                .put("delta.metadata.cache-size", "10")
                .put("delta.metadata.live-files.cache-size", "0 MB")
                .put("delta.metadata.live-files.cache-ttl", "60m")
                .put("delta.domain-compaction-threshold", "500")
                .put("delta.max-outstanding-splits", "200")
                .put("delta.max-splits-per-second", "10")
                .put("delta.max-split-size", "10 MB")
                .put("delta.minimum-assigned-split-weight", "0.01")
                .put("delta.max-partitions-per-writer", "200")
                .put("delta.enable-non-concurrent-writes", "true")
                .put("delta.default-checkpoint-writing-interval", "15")
                .put("delta.checkpoint-filtering.enabled", "false")
                .put("delta.checkpoint-row-statistics-writing.enabled", "false")
                .put("delta.vacuum.min-retention", "13h")
                .put("delta.hive-catalog-name", "hive")
                .put("delta.dynamic-filtering.wait-timeout", "30m")
                .put("delta.table-statistics-enabled", "false")
                .put("delta.extended-statistics.enabled", "false")
                .put("delta.extended-statistics.collect-on-write", "false")
                .put("delta.compression-codec", "GZIP")
                .put("delta.per-transaction-metastore-cache-maximum-size", "500")
                .put("delta.delete-schema-locations-fallback", "true")
                .put("delta.metastore.store-table-metadata", "true")
                .put("delta.metastore.store-table-metadata-threads", "1")
                .put("delta.metastore.store-table-metadata-interval", "30m")
                .put("delta.parquet.time-zone", nonDefaultTimeZone().getID())
                .put("delta.target-max-file-size", "2 GB")
                .put("delta.idle-writer-min-file-size", "1MB")
                .put("delta.unique-table-location", "false")
                .put("delta.register-table-procedure.enabled", "true")
                .put("delta.projection-pushdown-enabled", "false")
                .put("delta.query-partition-filter-required", "true")
                .put("delta.deletion-vectors-enabled", "true")
                .put("delta.fs.cache.disable-transaction-log-caching", "true")
                .buildOrThrow();

        DeltaLakeConfig expected = new DeltaLakeConfig()
                .setDataFileCacheSize(DataSize.succinctBytes(0))
                .setDataFileCacheTtl(new Duration(60, MINUTES))
                .setMetadataCacheTtl(new Duration(10, TimeUnit.MINUTES))
                .setMetadataCacheMaxSize(10)
                .setDomainCompactionThreshold(500)
                .setMaxOutstandingSplits(200)
                .setMaxSplitsPerSecond(10)
                .setMaxSplitSize(DataSize.of(10, DataSize.Unit.MEGABYTE))
                .setMinimumAssignedSplitWeight(0.01)
                .setMaxPartitionsPerWriter(200)
                .setUnsafeWritesEnabled(true)
                .setDefaultCheckpointWritingInterval(15)
                .setCheckpointRowStatisticsWritingEnabled(false)
                .setCheckpointFilteringEnabled(false)
                .setVacuumMinRetention(new Duration(13, HOURS))
                .setHiveCatalogName("hive")
                .setDynamicFilteringWaitTimeout(new Duration(30, MINUTES))
                .setTableStatisticsEnabled(false)
                .setExtendedStatisticsEnabled(false)
                .setCollectExtendedStatisticsOnWrite(false)
                .setCompressionCodec(HiveCompressionCodec.GZIP)
                .setDeleteSchemaLocationsFallback(true)
                .setParquetTimeZone(nonDefaultTimeZone().getID())
                .setPerTransactionMetastoreCacheMaximumSize(500)
                .setStoreTableMetadataEnabled(true)
                .setStoreTableMetadataThreads(1)
                .setStoreTableMetadataInterval(new Duration(30, MINUTES))
                .setTargetMaxFileSize(DataSize.of(2, GIGABYTE))
                .setIdleWriterMinFileSize(DataSize.of(1, MEGABYTE))
                .setUniqueTableLocation(false)
                .setRegisterTableProcedureEnabled(true)
                .setProjectionPushdownEnabled(false)
                .setQueryPartitionFilterRequired(true)
                .setDeletionVectorsEnabled(true)
                .setDeltaLogFileSystemCacheDisabled(true);

        assertFullMapping(properties, expected);
    }
}
