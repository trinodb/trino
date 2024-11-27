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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.plugin.hive.HiveCompressionCodec;
import jakarta.validation.constraints.AssertFalse;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.plugin.hive.HiveCompressionCodec.ZSTD;
import static io.trino.plugin.iceberg.CatalogType.GLUE;
import static io.trino.plugin.iceberg.CatalogType.HIVE_METASTORE;
import static io.trino.plugin.iceberg.IcebergFileFormat.ORC;
import static io.trino.plugin.iceberg.IcebergFileFormat.PARQUET;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestIcebergConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(IcebergConfig.class)
                .setFileFormat(PARQUET)
                .setCompressionCodec(ZSTD)
                .setUseFileSizeFromMetadata(true)
                .setMaxPartitionsPerWriter(100)
                .setUniqueTableLocation(true)
                .setCatalogType(HIVE_METASTORE)
                .setDynamicFilteringWaitTimeout(new Duration(1, SECONDS))
                .setTableStatisticsEnabled(true)
                .setExtendedStatisticsEnabled(true)
                .setCollectExtendedStatisticsOnWrite(true)
                .setProjectionPushdownEnabled(true)
                .setHiveCatalogName(null)
                .setFormatVersion(2)
                .setExpireSnapshotsMinRetention(new Duration(7, DAYS))
                .setRemoveOrphanFilesMinRetention(new Duration(7, DAYS))
                .setDeleteSchemaLocationsFallback(false)
                .setTargetMaxFileSize(DataSize.of(1, GIGABYTE))
                .setIdleWriterMinFileSize(DataSize.of(16, MEGABYTE))
                .setMinimumAssignedSplitWeight(0.05)
                .setHideMaterializedViewStorageTable(true)
                .setMaterializedViewsStorageSchema(null)
                .setRegisterTableProcedureEnabled(false)
                .setAddFilesProcedureEnabled(false)
                .setSortedWritingEnabled(true)
                .setQueryPartitionFilterRequired(false)
                .setQueryPartitionFilterRequiredSchemas(ImmutableSet.of())
                .setSplitManagerThreads(Runtime.getRuntime().availableProcessors() * 2)
                .setAllowedExtraProperties(ImmutableList.of())
                .setIncrementalRefreshEnabled(true)
                .setMetadataCacheEnabled(true)
                .setIncrementalRefreshEnabled(true)
                .setObjectStoreLayoutEnabled(false)
                .setMetadataParallelism(8));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("iceberg.file-format", "ORC")
                .put("iceberg.compression-codec", "NONE")
                .put("iceberg.use-file-size-from-metadata", "false")
                .put("iceberg.max-partitions-per-writer", "222")
                .put("iceberg.unique-table-location", "false")
                .put("iceberg.catalog.type", "GLUE")
                .put("iceberg.dynamic-filtering.wait-timeout", "1h")
                .put("iceberg.table-statistics-enabled", "false")
                .put("iceberg.extended-statistics.enabled", "false")
                .put("iceberg.extended-statistics.collect-on-write", "false")
                .put("iceberg.projection-pushdown-enabled", "false")
                .put("iceberg.hive-catalog-name", "hive")
                .put("iceberg.format-version", "1")
                .put("iceberg.expire-snapshots.min-retention", "13h")
                .put("iceberg.remove-orphan-files.min-retention", "14h")
                .put("iceberg.delete-schema-locations-fallback", "true")
                .put("iceberg.target-max-file-size", "1MB")
                .put("iceberg.idle-writer-min-file-size", "1MB")
                .put("iceberg.minimum-assigned-split-weight", "0.01")
                .put("iceberg.materialized-views.hide-storage-table", "false")
                .put("iceberg.materialized-views.storage-schema", "mv_storage_schema")
                .put("iceberg.register-table-procedure.enabled", "true")
                .put("iceberg.add-files-procedure.enabled", "true")
                .put("iceberg.sorted-writing-enabled", "false")
                .put("iceberg.query-partition-filter-required", "true")
                .put("iceberg.query-partition-filter-required-schemas", "bronze,silver")
                .put("iceberg.split-manager-threads", "42")
                .put("iceberg.allowed-extra-properties", "propX,propY")
                .put("iceberg.incremental-refresh-enabled", "false")
                .put("iceberg.metadata-cache.enabled", "false")
                .put("iceberg.object-store-layout.enabled", "true")
                .put("iceberg.metadata.parallelism", "10")
                .buildOrThrow();

        IcebergConfig expected = new IcebergConfig()
                .setFileFormat(ORC)
                .setCompressionCodec(HiveCompressionCodec.NONE)
                .setUseFileSizeFromMetadata(false)
                .setMaxPartitionsPerWriter(222)
                .setUniqueTableLocation(false)
                .setCatalogType(GLUE)
                .setDynamicFilteringWaitTimeout(Duration.valueOf("1h"))
                .setTableStatisticsEnabled(false)
                .setExtendedStatisticsEnabled(false)
                .setCollectExtendedStatisticsOnWrite(false)
                .setProjectionPushdownEnabled(false)
                .setHiveCatalogName("hive")
                .setFormatVersion(1)
                .setExpireSnapshotsMinRetention(new Duration(13, HOURS))
                .setRemoveOrphanFilesMinRetention(new Duration(14, HOURS))
                .setDeleteSchemaLocationsFallback(true)
                .setTargetMaxFileSize(DataSize.of(1, MEGABYTE))
                .setIdleWriterMinFileSize(DataSize.of(1, MEGABYTE))
                .setMinimumAssignedSplitWeight(0.01)
                .setHideMaterializedViewStorageTable(false)
                .setMaterializedViewsStorageSchema("mv_storage_schema")
                .setRegisterTableProcedureEnabled(true)
                .setAddFilesProcedureEnabled(true)
                .setSortedWritingEnabled(false)
                .setQueryPartitionFilterRequired(true)
                .setQueryPartitionFilterRequiredSchemas(ImmutableSet.of("bronze", "silver"))
                .setSplitManagerThreads(42)
                .setAllowedExtraProperties(ImmutableList.of("propX", "propY"))
                .setIncrementalRefreshEnabled(false)
                .setMetadataCacheEnabled(false)
                .setIncrementalRefreshEnabled(false)
                .setObjectStoreLayoutEnabled(true)
                .setMetadataParallelism(10);

        assertFullMapping(properties, expected);
    }

    @Test
    public void testValidation()
    {
        assertFailsValidation(
                new IcebergConfig()
                        .setHideMaterializedViewStorageTable(true)
                        .setMaterializedViewsStorageSchema("storage_schema"),
                "storageSchemaSetWhenHidingIsEnabled",
                "iceberg.materialized-views.storage-schema may only be set when iceberg.materialized-views.hide-storage-table is set to false",
                AssertFalse.class);
    }
}
