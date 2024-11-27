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
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import io.airlift.configuration.LegacyConfig;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.plugin.hive.HiveCompressionCodec;
import jakarta.validation.constraints.AssertFalse;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.plugin.hive.HiveCompressionCodec.ZSTD;
import static io.trino.plugin.iceberg.CatalogType.HIVE_METASTORE;
import static io.trino.plugin.iceberg.IcebergFileFormat.PARQUET;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.SECONDS;

@DefunctConfig({
        "iceberg.allow-legacy-snapshot-syntax",
        "iceberg.experimental.extended-statistics.enabled",
})
public class IcebergConfig
{
    public static final int FORMAT_VERSION_SUPPORT_MIN = 1;
    public static final int FORMAT_VERSION_SUPPORT_MAX = 2;
    public static final String EXTENDED_STATISTICS_CONFIG = "iceberg.extended-statistics.enabled";
    public static final String EXTENDED_STATISTICS_DESCRIPTION = "Enable collection (ANALYZE) and use of extended statistics.";
    public static final String COLLECT_EXTENDED_STATISTICS_ON_WRITE_DESCRIPTION = "Collect extended statistics during writes";
    public static final String EXPIRE_SNAPSHOTS_MIN_RETENTION = "iceberg.expire-snapshots.min-retention";
    public static final String REMOVE_ORPHAN_FILES_MIN_RETENTION = "iceberg.remove-orphan-files.min-retention";

    private IcebergFileFormat fileFormat = PARQUET;
    private HiveCompressionCodec compressionCodec = ZSTD;
    private boolean useFileSizeFromMetadata = true;
    private int maxPartitionsPerWriter = 100;
    private boolean uniqueTableLocation = true;
    private CatalogType catalogType = HIVE_METASTORE;
    private Duration dynamicFilteringWaitTimeout = new Duration(1, SECONDS);
    private boolean tableStatisticsEnabled = true;
    private boolean extendedStatisticsEnabled = true;
    private boolean collectExtendedStatisticsOnWrite = true;
    private boolean projectionPushdownEnabled = true;
    private boolean registerTableProcedureEnabled;
    private boolean addFilesProcedureEnabled;
    private Optional<String> hiveCatalogName = Optional.empty();
    private int formatVersion = FORMAT_VERSION_SUPPORT_MAX;
    private Duration expireSnapshotsMinRetention = new Duration(7, DAYS);
    private Duration removeOrphanFilesMinRetention = new Duration(7, DAYS);
    private DataSize targetMaxFileSize = DataSize.of(1, GIGABYTE);
    private DataSize idleWriterMinFileSize = DataSize.of(16, MEGABYTE);
    // This is meant to protect users who are misusing schema locations (by
    // putting schemas in locations with extraneous files), so default to false
    // to avoid deleting those files if Trino is unable to check.
    private boolean deleteSchemaLocationsFallback;
    private double minimumAssignedSplitWeight = 0.05;
    private boolean hideMaterializedViewStorageTable = true;
    private Optional<String> materializedViewsStorageSchema = Optional.empty();
    private boolean sortedWritingEnabled = true;
    private boolean queryPartitionFilterRequired;
    private Set<String> queryPartitionFilterRequiredSchemas = ImmutableSet.of();
    private int splitManagerThreads = Runtime.getRuntime().availableProcessors() * 2;
    private List<String> allowedExtraProperties = ImmutableList.of();
    private boolean incrementalRefreshEnabled = true;
    private boolean metadataCacheEnabled = true;
    private boolean objectStoreLayoutEnabled;
    private int metadataParallelism = 8;

    public CatalogType getCatalogType()
    {
        return catalogType;
    }

    @Config("iceberg.catalog.type")
    public IcebergConfig setCatalogType(CatalogType catalogType)
    {
        this.catalogType = catalogType;
        return this;
    }

    @NotNull
    public IcebergFileFormat getFileFormat()
    {
        return fileFormat;
    }

    @Config("iceberg.file-format")
    public IcebergConfig setFileFormat(IcebergFileFormat fileFormat)
    {
        this.fileFormat = fileFormat;
        return this;
    }

    @NotNull
    public HiveCompressionCodec getCompressionCodec()
    {
        return compressionCodec;
    }

    @Config("iceberg.compression-codec")
    public IcebergConfig setCompressionCodec(HiveCompressionCodec compressionCodec)
    {
        this.compressionCodec = compressionCodec;
        return this;
    }

    @Deprecated
    public boolean isUseFileSizeFromMetadata()
    {
        return useFileSizeFromMetadata;
    }

    /**
     * Some Iceberg writers populate incorrect file sizes in the metadata. When
     * this property is set to false, Trino ignores the stored values and fetches
     * them with a getFileStatus call. This means an additional call per split,
     * so it is recommended for a Trino admin to fix the metadata, rather than
     * relying on this property for too long.
     */
    @Deprecated
    @Config("iceberg.use-file-size-from-metadata")
    public IcebergConfig setUseFileSizeFromMetadata(boolean useFileSizeFromMetadata)
    {
        this.useFileSizeFromMetadata = useFileSizeFromMetadata;
        return this;
    }

    @Min(1)
    public int getMaxPartitionsPerWriter()
    {
        return maxPartitionsPerWriter;
    }

    @Config("iceberg.max-partitions-per-writer")
    @ConfigDescription("Maximum number of partitions per writer")
    public IcebergConfig setMaxPartitionsPerWriter(int maxPartitionsPerWriter)
    {
        this.maxPartitionsPerWriter = maxPartitionsPerWriter;
        return this;
    }

    public boolean isUniqueTableLocation()
    {
        return uniqueTableLocation;
    }

    @Config("iceberg.unique-table-location")
    @ConfigDescription("Use randomized, unique table locations")
    public IcebergConfig setUniqueTableLocation(boolean uniqueTableLocation)
    {
        this.uniqueTableLocation = uniqueTableLocation;
        return this;
    }

    @NotNull
    public Duration getDynamicFilteringWaitTimeout()
    {
        return dynamicFilteringWaitTimeout;
    }

    @Config("iceberg.dynamic-filtering.wait-timeout")
    @ConfigDescription("Duration to wait for completion of dynamic filters during split generation")
    public IcebergConfig setDynamicFilteringWaitTimeout(Duration dynamicFilteringWaitTimeout)
    {
        this.dynamicFilteringWaitTimeout = dynamicFilteringWaitTimeout;
        return this;
    }

    public boolean isTableStatisticsEnabled()
    {
        return tableStatisticsEnabled;
    }

    // In case of some queries / tables, retrieving table statistics from Iceberg
    // can take 20+ seconds. This config allows the user / operator the option
    // to opt out of retrieving table statistics in those cases to speed up query planning.
    @Config("iceberg.table-statistics-enabled")
    @ConfigDescription("Enable use of table statistics")
    public IcebergConfig setTableStatisticsEnabled(boolean tableStatisticsEnabled)
    {
        this.tableStatisticsEnabled = tableStatisticsEnabled;
        return this;
    }

    public boolean isExtendedStatisticsEnabled()
    {
        return extendedStatisticsEnabled;
    }

    @Config(EXTENDED_STATISTICS_CONFIG)
    @ConfigDescription(EXTENDED_STATISTICS_DESCRIPTION)
    public IcebergConfig setExtendedStatisticsEnabled(boolean extendedStatisticsEnabled)
    {
        this.extendedStatisticsEnabled = extendedStatisticsEnabled;
        return this;
    }

    public boolean isCollectExtendedStatisticsOnWrite()
    {
        return collectExtendedStatisticsOnWrite;
    }

    @Config("iceberg.extended-statistics.collect-on-write")
    @ConfigDescription(COLLECT_EXTENDED_STATISTICS_ON_WRITE_DESCRIPTION)
    public IcebergConfig setCollectExtendedStatisticsOnWrite(boolean collectExtendedStatisticsOnWrite)
    {
        this.collectExtendedStatisticsOnWrite = collectExtendedStatisticsOnWrite;
        return this;
    }

    public boolean isProjectionPushdownEnabled()
    {
        return projectionPushdownEnabled;
    }

    @Config("iceberg.projection-pushdown-enabled")
    @ConfigDescription("Read only required fields from a row type")
    public IcebergConfig setProjectionPushdownEnabled(boolean projectionPushdownEnabled)
    {
        this.projectionPushdownEnabled = projectionPushdownEnabled;
        return this;
    }

    public boolean isRegisterTableProcedureEnabled()
    {
        return registerTableProcedureEnabled;
    }

    @Config("iceberg.register-table-procedure.enabled")
    @ConfigDescription("Allow users to call the register_table procedure")
    public IcebergConfig setRegisterTableProcedureEnabled(boolean registerTableProcedureEnabled)
    {
        this.registerTableProcedureEnabled = registerTableProcedureEnabled;
        return this;
    }

    public boolean isAddFilesProcedureEnabled()
    {
        return addFilesProcedureEnabled;
    }

    @Config("iceberg.add-files-procedure.enabled")
    @LegacyConfig("iceberg.add_files-procedure.enabled")
    @ConfigDescription("Allow users to call the add_files procedure")
    public IcebergConfig setAddFilesProcedureEnabled(boolean addFilesProcedureEnabled)
    {
        this.addFilesProcedureEnabled = addFilesProcedureEnabled;
        return this;
    }

    public Optional<String> getHiveCatalogName()
    {
        return hiveCatalogName;
    }

    @Config("iceberg.hive-catalog-name")
    @ConfigDescription("Catalog to redirect to when a Hive table is referenced")
    public IcebergConfig setHiveCatalogName(String hiveCatalogName)
    {
        this.hiveCatalogName = Optional.ofNullable(hiveCatalogName);
        return this;
    }

    @Min(FORMAT_VERSION_SUPPORT_MIN)
    @Max(FORMAT_VERSION_SUPPORT_MAX)
    public int getFormatVersion()
    {
        return formatVersion;
    }

    @Config("iceberg.format-version")
    @ConfigDescription("Default Iceberg table format version")
    public IcebergConfig setFormatVersion(int formatVersion)
    {
        this.formatVersion = formatVersion;
        return this;
    }

    @NotNull
    public Duration getExpireSnapshotsMinRetention()
    {
        return expireSnapshotsMinRetention;
    }

    @Config(EXPIRE_SNAPSHOTS_MIN_RETENTION)
    @LegacyConfig("iceberg.expire_snapshots.min-retention")
    @ConfigDescription("Minimal retention period for expire_snapshot procedure")
    public IcebergConfig setExpireSnapshotsMinRetention(Duration expireSnapshotsMinRetention)
    {
        this.expireSnapshotsMinRetention = expireSnapshotsMinRetention;
        return this;
    }

    @NotNull
    public Duration getRemoveOrphanFilesMinRetention()
    {
        return removeOrphanFilesMinRetention;
    }

    @Config(REMOVE_ORPHAN_FILES_MIN_RETENTION)
    @LegacyConfig("iceberg.remove_orphan_files.min-retention")
    @ConfigDescription("Minimal retention period for remove_orphan_files procedure")
    public IcebergConfig setRemoveOrphanFilesMinRetention(Duration removeOrphanFilesMinRetention)
    {
        this.removeOrphanFilesMinRetention = removeOrphanFilesMinRetention;
        return this;
    }

    public DataSize getTargetMaxFileSize()
    {
        return targetMaxFileSize;
    }

    @LegacyConfig("hive.target-max-file-size")
    @Config("iceberg.target-max-file-size")
    @ConfigDescription("Target maximum size of written files; the actual size may be larger")
    public IcebergConfig setTargetMaxFileSize(DataSize targetMaxFileSize)
    {
        this.targetMaxFileSize = targetMaxFileSize;
        return this;
    }

    @NotNull
    public DataSize getIdleWriterMinFileSize()
    {
        return idleWriterMinFileSize;
    }

    @Config("iceberg.idle-writer-min-file-size")
    @ConfigDescription("Minimum data written by a single partition writer before it can be consider as 'idle' and could be closed by the engine")
    public IcebergConfig setIdleWriterMinFileSize(DataSize idleWriterMinFileSize)
    {
        this.idleWriterMinFileSize = idleWriterMinFileSize;
        return this;
    }

    public boolean isDeleteSchemaLocationsFallback()
    {
        return this.deleteSchemaLocationsFallback;
    }

    @LegacyConfig("hive.delete-schema-locations-fallback")
    @Config("iceberg.delete-schema-locations-fallback")
    @ConfigDescription("Whether schema locations should be deleted when Trino can't determine whether they contain external files.")
    public IcebergConfig setDeleteSchemaLocationsFallback(boolean deleteSchemaLocationsFallback)
    {
        this.deleteSchemaLocationsFallback = deleteSchemaLocationsFallback;
        return this;
    }

    @Config("iceberg.minimum-assigned-split-weight")
    @ConfigDescription("Minimum weight that a split can be assigned")
    public IcebergConfig setMinimumAssignedSplitWeight(double minimumAssignedSplitWeight)
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

    @Deprecated
    public boolean isHideMaterializedViewStorageTable()
    {
        return hideMaterializedViewStorageTable;
    }

    @Deprecated
    @Config("iceberg.materialized-views.hide-storage-table")
    @ConfigDescription("Hide materialized view storage tables in metastore")
    public IcebergConfig setHideMaterializedViewStorageTable(boolean hideMaterializedViewStorageTable)
    {
        this.hideMaterializedViewStorageTable = hideMaterializedViewStorageTable;
        return this;
    }

    @NotNull
    public Optional<String> getMaterializedViewsStorageSchema()
    {
        return materializedViewsStorageSchema;
    }

    @Config("iceberg.materialized-views.storage-schema")
    @ConfigDescription("Schema for creating materialized views storage tables")
    public IcebergConfig setMaterializedViewsStorageSchema(String materializedViewsStorageSchema)
    {
        this.materializedViewsStorageSchema = Optional.ofNullable(materializedViewsStorageSchema);
        return this;
    }

    public boolean isSortedWritingEnabled()
    {
        return sortedWritingEnabled;
    }

    @Config("iceberg.sorted-writing-enabled")
    @ConfigDescription("Enable sorted writing to tables with a specified sort order")
    public IcebergConfig setSortedWritingEnabled(boolean sortedWritingEnabled)
    {
        this.sortedWritingEnabled = sortedWritingEnabled;
        return this;
    }

    @Config("iceberg.query-partition-filter-required")
    @ConfigDescription("Require a filter on at least one partition column")
    public IcebergConfig setQueryPartitionFilterRequired(boolean queryPartitionFilterRequired)
    {
        this.queryPartitionFilterRequired = queryPartitionFilterRequired;
        return this;
    }

    public boolean isQueryPartitionFilterRequired()
    {
        return queryPartitionFilterRequired;
    }

    public Set<String> getQueryPartitionFilterRequiredSchemas()
    {
        return queryPartitionFilterRequiredSchemas;
    }

    @Config("iceberg.query-partition-filter-required-schemas")
    @ConfigDescription("List of schemas for which filter on partition column is enforced")
    public IcebergConfig setQueryPartitionFilterRequiredSchemas(Set<String> queryPartitionFilterRequiredSchemas)
    {
        this.queryPartitionFilterRequiredSchemas = queryPartitionFilterRequiredSchemas.stream()
                .map(value -> value.toLowerCase(ENGLISH))
                .collect(toImmutableSet());
        return this;
    }

    @Min(0)
    public int getSplitManagerThreads()
    {
        return splitManagerThreads;
    }

    @Config("iceberg.split-manager-threads")
    @ConfigDescription("Number of threads to use for generating splits")
    public IcebergConfig setSplitManagerThreads(int splitManagerThreads)
    {
        this.splitManagerThreads = splitManagerThreads;
        return this;
    }

    public List<String> getAllowedExtraProperties()
    {
        return allowedExtraProperties;
    }

    @Config("iceberg.allowed-extra-properties")
    @ConfigDescription("List of extra properties that are allowed to be set on Iceberg tables")
    public IcebergConfig setAllowedExtraProperties(List<String> allowedExtraProperties)
    {
        this.allowedExtraProperties = ImmutableList.copyOf(allowedExtraProperties);
        checkArgument(!allowedExtraProperties.contains("*") || allowedExtraProperties.size() == 1,
                "Wildcard * should be the only element in the list");
        return this;
    }

    public boolean isIncrementalRefreshEnabled()
    {
        return incrementalRefreshEnabled;
    }

    @Config("iceberg.incremental-refresh-enabled")
    @ConfigDescription("Enable Incremental refresh for MVs backed by Iceberg tables, when possible")
    public IcebergConfig setIncrementalRefreshEnabled(boolean incrementalRefreshEnabled)
    {
        this.incrementalRefreshEnabled = incrementalRefreshEnabled;
        return this;
    }

    @AssertFalse(message = "iceberg.materialized-views.storage-schema may only be set when iceberg.materialized-views.hide-storage-table is set to false")
    public boolean isStorageSchemaSetWhenHidingIsEnabled()
    {
        return hideMaterializedViewStorageTable && materializedViewsStorageSchema.isPresent();
    }

    public boolean isMetadataCacheEnabled()
    {
        return metadataCacheEnabled;
    }

    @Config("iceberg.metadata-cache.enabled")
    @ConfigDescription("Enables in-memory caching of metadata files on coordinator if fs.cache.enabled is not set to true")
    public IcebergConfig setMetadataCacheEnabled(boolean metadataCacheEnabled)
    {
        this.metadataCacheEnabled = metadataCacheEnabled;
        return this;
    }

    public boolean isObjectStoreLayoutEnabled()
    {
        return objectStoreLayoutEnabled;
    }

    @Config("iceberg.object-store-layout.enabled")
    @ConfigDescription("Enable the Iceberg object store file layout")
    public IcebergConfig setObjectStoreLayoutEnabled(boolean objectStoreLayoutEnabled)
    {
        this.objectStoreLayoutEnabled = objectStoreLayoutEnabled;
        return this;
    }

    @Min(1)
    public int getMetadataParallelism()
    {
        return metadataParallelism;
    }

    @ConfigDescription("Limits metadata enumeration calls parallelism")
    @Config("iceberg.metadata.parallelism")
    public IcebergConfig setMetadataParallelism(int metadataParallelism)
    {
        this.metadataParallelism = metadataParallelism;
        return this;
    }
}
