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

import com.google.common.annotations.VisibleForTesting;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigHidden;
import io.airlift.configuration.DefunctConfig;
import io.airlift.configuration.LegacyConfig;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MaxDuration;
import io.airlift.units.MinDuration;
import io.trino.plugin.hive.HiveCompressionCodec;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import org.joda.time.DateTimeZone;

import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.SECONDS;

@DefunctConfig({
        "delta.experimental.ignore-checkpoint-write-failures",
        "delta.legacy-create-table-with-existing-location.enabled",
        "delta.max-initial-splits",
        "delta.max-initial-split-size",
        "delta.metadata.cache-size",
})
public class DeltaLakeConfig
{
    public static final String EXTENDED_STATISTICS_ENABLED = "delta.extended-statistics.enabled";
    public static final String VACUUM_MIN_RETENTION = "delta.vacuum.min-retention";
    public static final DataSize DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE = DataSize.of(16, MEGABYTE);

    // Runtime.getRuntime().maxMemory() is not 100% stable and may return slightly different value over JVM lifetime. We use
    // constant so default configuration for cache size is stable.
    @VisibleForTesting
    static final DataSize DEFAULT_DATA_FILE_CACHE_SIZE = DataSize.succinctBytes(Math.floorDiv(Runtime.getRuntime().maxMemory(), 10L));
    @VisibleForTesting
    static final DataSize DEFAULT_METADATA_CACHE_MAX_RETAINED_SIZE = DataSize.succinctBytes(Math.floorDiv(Runtime.getRuntime().maxMemory(), 20L));

    private Duration metadataCacheTtl = new Duration(30, TimeUnit.MINUTES);
    private DataSize metadataCacheMaxRetainedSize = DEFAULT_METADATA_CACHE_MAX_RETAINED_SIZE;
    private DataSize transactionLogMaxCachedFileSize = DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE;
    private DataSize dataFileCacheSize = DEFAULT_DATA_FILE_CACHE_SIZE;
    private Duration dataFileCacheTtl = new Duration(30, TimeUnit.MINUTES);
    private int domainCompactionThreshold = 1000;
    private int maxOutstandingSplits = 1_000;
    private int maxSplitsPerSecond = Integer.MAX_VALUE;
    private DataSize maxSplitSize = DataSize.of(64, MEGABYTE);
    private double minimumAssignedSplitWeight = 0.05;
    private int maxPartitionsPerWriter = 100;
    private boolean unsafeWritesEnabled;
    private boolean checkpointRowStatisticsWritingEnabled = true;
    private long defaultCheckpointWritingInterval = 10;
    private boolean checkpointFilteringEnabled = true;
    private Duration vacuumMinRetention = new Duration(7, DAYS);
    private Optional<String> hiveCatalogName = Optional.empty();
    private Duration dynamicFilteringWaitTimeout = new Duration(0, SECONDS);
    private boolean tableStatisticsEnabled = true;
    private boolean extendedStatisticsEnabled = true;
    private boolean collectExtendedStatisticsOnWrite = true;
    private HiveCompressionCodec compressionCodec = HiveCompressionCodec.ZSTD;
    private long perTransactionMetastoreCacheMaximumSize = 1000;
    private boolean storeTableMetadataEnabled;
    private int storeTableMetadataThreads = 5;
    private Duration storeTableMetadataInterval = new Duration(1, SECONDS);
    private boolean deleteSchemaLocationsFallback;
    private String parquetTimeZone = TimeZone.getDefault().getID();
    private DataSize targetMaxFileSize = DataSize.of(1, GIGABYTE);
    private DataSize idleWriterMinFileSize = DataSize.of(16, MEGABYTE);
    private boolean uniqueTableLocation = true;
    private boolean registerTableProcedureEnabled;
    private boolean projectionPushdownEnabled = true;
    private boolean queryPartitionFilterRequired;
    private boolean deletionVectorsEnabled;
    private boolean deltaLogFileSystemCacheDisabled;
    private int metadataParallelism = 8;

    public Duration getMetadataCacheTtl()
    {
        return metadataCacheTtl;
    }

    @Config("delta.metadata.cache-ttl")
    @ConfigDescription("Caching duration for Delta table metadata (e.g. table schema, partition info)")
    public DeltaLakeConfig setMetadataCacheTtl(Duration metadataCacheTtl)
    {
        this.metadataCacheTtl = metadataCacheTtl;
        return this;
    }

    public DataSize getMetadataCacheMaxRetainedSize()
    {
        return metadataCacheMaxRetainedSize;
    }

    @Config("delta.metadata.cache-max-retained-size")
    @ConfigDescription("Maximum retained size of Delta table metadata stored in cache")
    public DeltaLakeConfig setMetadataCacheMaxRetainedSize(DataSize metadataCacheMaxRetainedSize)
    {
        this.metadataCacheMaxRetainedSize = metadataCacheMaxRetainedSize;
        return this;
    }

    public DataSize getTransactionLogMaxCachedFileSize()
    {
        return transactionLogMaxCachedFileSize;
    }

    @Config("delta.transaction-log.max-cached-file-size")
    @ConfigDescription("Maximum size of delta transaction log file that will be cached in memory")
    public DeltaLakeConfig setTransactionLogMaxCachedFileSize(DataSize transactionLogMaxCachedFileSize)
    {
        this.transactionLogMaxCachedFileSize = transactionLogMaxCachedFileSize;
        return this;
    }

    public DataSize getDataFileCacheSize()
    {
        return dataFileCacheSize;
    }

    @Config("delta.metadata.live-files.cache-size")
    @ConfigDescription("Maximum in memory cache size for Delta data file metadata (e.g. file path, statistics, partition values). Defaults to 10% of the available heap size.")
    public DeltaLakeConfig setDataFileCacheSize(DataSize dataFileCacheSize)
    {
        this.dataFileCacheSize = dataFileCacheSize;
        return this;
    }

    @NotNull
    public Duration getDataFileCacheTtl()
    {
        return dataFileCacheTtl;
    }

    @Config("delta.metadata.live-files.cache-ttl")
    @ConfigDescription("Caching duration for Delta data file metadata (e.g. table schema, partition info)")
    public DeltaLakeConfig setDataFileCacheTtl(Duration dataFileCacheTtl)
    {
        this.dataFileCacheTtl = dataFileCacheTtl;
        return this;
    }

    @Min(1)
    public int getDomainCompactionThreshold()
    {
        return domainCompactionThreshold;
    }

    @Config("delta.domain-compaction-threshold")
    @ConfigDescription("Maximum ranges to allow in a tuple domain without compacting it")
    public DeltaLakeConfig setDomainCompactionThreshold(int domainCompactionThreshold)
    {
        this.domainCompactionThreshold = domainCompactionThreshold;
        return this;
    }

    public int getMaxOutstandingSplits()
    {
        return maxOutstandingSplits;
    }

    @Config("delta.max-outstanding-splits")
    @ConfigDescription("Target number of buffered splits for each table scan in a query, before the scheduler tries to pause itself")
    public DeltaLakeConfig setMaxOutstandingSplits(int maxOutstandingSplits)
    {
        this.maxOutstandingSplits = maxOutstandingSplits;
        return this;
    }

    @Min(1)
    public int getMaxSplitsPerSecond()
    {
        return maxSplitsPerSecond;
    }

    @Config("delta.max-splits-per-second")
    @ConfigDescription("Throttles the maximum number of splits that can be assigned to tasks per second")
    public DeltaLakeConfig setMaxSplitsPerSecond(int maxSplitsPerSecond)
    {
        this.maxSplitsPerSecond = maxSplitsPerSecond;
        return this;
    }

    @NotNull
    public DataSize getMaxSplitSize()
    {
        return maxSplitSize;
    }

    @Config("delta.max-split-size")
    public DeltaLakeConfig setMaxSplitSize(DataSize maxSplitSize)
    {
        this.maxSplitSize = maxSplitSize;
        return this;
    }

    @Config("delta.minimum-assigned-split-weight")
    @ConfigDescription("Minimum weight that a split can be assigned")
    public DeltaLakeConfig setMinimumAssignedSplitWeight(double minimumAssignedSplitWeight)
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
    public int getMaxPartitionsPerWriter()
    {
        return maxPartitionsPerWriter;
    }

    @Config("delta.max-partitions-per-writer")
    @ConfigDescription("Maximum number of partitions per writer")
    public DeltaLakeConfig setMaxPartitionsPerWriter(int maxPartitionsPerWriter)
    {
        this.maxPartitionsPerWriter = maxPartitionsPerWriter;
        return this;
    }

    public boolean getUnsafeWritesEnabled()
    {
        return unsafeWritesEnabled;
    }

    @Config("delta.enable-non-concurrent-writes")
    public DeltaLakeConfig setUnsafeWritesEnabled(boolean unsafeWritesEnabled)
    {
        this.unsafeWritesEnabled = unsafeWritesEnabled;
        return this;
    }

    @Config("delta.default-checkpoint-writing-interval")
    @ConfigDescription("How often (in number of transactions) to write checkpoint of transaction log")
    public DeltaLakeConfig setDefaultCheckpointWritingInterval(long defaultCheckpointWritingInterval)
    {
        this.defaultCheckpointWritingInterval = defaultCheckpointWritingInterval;
        return this;
    }

    @Min(1)
    public long getDefaultCheckpointWritingInterval()
    {
        return defaultCheckpointWritingInterval;
    }

    public boolean isCheckpointFilteringEnabled()
    {
        return checkpointFilteringEnabled;
    }

    @Config("delta.checkpoint-filtering.enabled")
    public DeltaLakeConfig setCheckpointFilteringEnabled(boolean checkpointFilteringEnabled)
    {
        this.checkpointFilteringEnabled = checkpointFilteringEnabled;
        return this;
    }

    @NotNull
    public Duration getVacuumMinRetention()
    {
        return vacuumMinRetention;
    }

    @Config(VACUUM_MIN_RETENTION)
    @ConfigDescription("Minimal retention period for vacuum procedure")
    public DeltaLakeConfig setVacuumMinRetention(Duration vacuumMinRetention)
    {
        this.vacuumMinRetention = vacuumMinRetention;
        return this;
    }

    public Optional<String> getHiveCatalogName()
    {
        return hiveCatalogName;
    }

    @Config("delta.hive-catalog-name")
    @ConfigDescription("Catalog to redirect to when a Hive table is referenced")
    public DeltaLakeConfig setHiveCatalogName(String hiveCatalogName)
    {
        this.hiveCatalogName = Optional.ofNullable(hiveCatalogName);
        return this;
    }

    public boolean isCheckpointRowStatisticsWritingEnabled()
    {
        return checkpointRowStatisticsWritingEnabled;
    }

    @Config("delta.checkpoint-row-statistics-writing.enabled")
    public DeltaLakeConfig setCheckpointRowStatisticsWritingEnabled(boolean checkpointRowStatisticsWritingEnabled)
    {
        this.checkpointRowStatisticsWritingEnabled = checkpointRowStatisticsWritingEnabled;
        return this;
    }

    @NotNull
    public Duration getDynamicFilteringWaitTimeout()
    {
        return dynamicFilteringWaitTimeout;
    }

    @Config("delta.dynamic-filtering.wait-timeout")
    @ConfigDescription("Duration to wait for completion of dynamic filters during split generation")
    public DeltaLakeConfig setDynamicFilteringWaitTimeout(Duration dynamicFilteringWaitTimeout)
    {
        this.dynamicFilteringWaitTimeout = dynamicFilteringWaitTimeout;
        return this;
    }

    public boolean isTableStatisticsEnabled()
    {
        return tableStatisticsEnabled;
    }

    @Config("delta.table-statistics-enabled")
    @ConfigDescription("Expose table statistics")
    public DeltaLakeConfig setTableStatisticsEnabled(boolean tableStatisticsEnabled)
    {
        this.tableStatisticsEnabled = tableStatisticsEnabled;
        return this;
    }

    public boolean isExtendedStatisticsEnabled()
    {
        return extendedStatisticsEnabled;
    }

    @Config(EXTENDED_STATISTICS_ENABLED)
    @ConfigDescription("Enable collection (ANALYZE) and use of extended statistics.")
    public DeltaLakeConfig setExtendedStatisticsEnabled(boolean extendedStatisticsEnabled)
    {
        this.extendedStatisticsEnabled = extendedStatisticsEnabled;
        return this;
    }

    public boolean isCollectExtendedStatisticsOnWrite()
    {
        return collectExtendedStatisticsOnWrite;
    }

    @Config("delta.extended-statistics.collect-on-write")
    @ConfigDescription("Enables automatic column level extended statistics collection on write")
    public DeltaLakeConfig setCollectExtendedStatisticsOnWrite(boolean collectExtendedStatisticsOnWrite)
    {
        this.collectExtendedStatisticsOnWrite = collectExtendedStatisticsOnWrite;
        return this;
    }

    @NotNull
    public HiveCompressionCodec getCompressionCodec()
    {
        return compressionCodec;
    }

    @Config("delta.compression-codec")
    @ConfigDescription("Compression codec to use when writing new data files")
    public DeltaLakeConfig setCompressionCodec(HiveCompressionCodec compressionCodec)
    {
        this.compressionCodec = compressionCodec;
        return this;
    }

    @Min(1)
    public long getPerTransactionMetastoreCacheMaximumSize()
    {
        return perTransactionMetastoreCacheMaximumSize;
    }

    @LegacyConfig("hive.per-transaction-metastore-cache-maximum-size")
    @Config("delta.per-transaction-metastore-cache-maximum-size")
    public DeltaLakeConfig setPerTransactionMetastoreCacheMaximumSize(long perTransactionMetastoreCacheMaximumSize)
    {
        this.perTransactionMetastoreCacheMaximumSize = perTransactionMetastoreCacheMaximumSize;
        return this;
    }

    public boolean isStoreTableMetadataEnabled()
    {
        return storeTableMetadataEnabled;
    }

    @Config("delta.metastore.store-table-metadata")
    @ConfigDescription("Store table metadata in metastore")
    public DeltaLakeConfig setStoreTableMetadataEnabled(boolean storeTableMetadataEnabled)
    {
        this.storeTableMetadataEnabled = storeTableMetadataEnabled;
        return this;
    }

    @Min(0) // Allow 0 to use the same thread for testing purpose
    public int getStoreTableMetadataThreads()
    {
        return storeTableMetadataThreads;
    }

    @Config("delta.metastore.store-table-metadata-threads")
    @ConfigDescription("Number of threads used for storing table metadata in metastore")
    public DeltaLakeConfig setStoreTableMetadataThreads(int storeTableMetadataThreads)
    {
        this.storeTableMetadataThreads = storeTableMetadataThreads;
        return this;
    }

    @MinDuration("0ms")
    @MaxDuration("1h")
    public Duration getStoreTableMetadataInterval()
    {
        return storeTableMetadataInterval;
    }

    @ConfigHidden
    @Config("delta.metastore.store-table-metadata-interval")
    @ConfigDescription("Interval to store table metadata in metastore")
    public DeltaLakeConfig setStoreTableMetadataInterval(Duration storeTableMetadataInterval)
    {
        this.storeTableMetadataInterval = storeTableMetadataInterval;
        return this;
    }

    public boolean isDeleteSchemaLocationsFallback()
    {
        return this.deleteSchemaLocationsFallback;
    }

    @Config("delta.delete-schema-locations-fallback")
    @ConfigDescription("Whether schema locations should be deleted when Trino can't determine whether they contain external files.")
    public DeltaLakeConfig setDeleteSchemaLocationsFallback(boolean deleteSchemaLocationsFallback)
    {
        this.deleteSchemaLocationsFallback = deleteSchemaLocationsFallback;
        return this;
    }

    public DateTimeZone getParquetDateTimeZone()
    {
        return DateTimeZone.forID(parquetTimeZone);
    }

    @NotNull
    public String getParquetTimeZone()
    {
        return parquetTimeZone;
    }

    @LegacyConfig("hive.parquet.time-zone")
    @Config("delta.parquet.time-zone")
    @ConfigDescription("Time zone for Parquet read and write")
    public DeltaLakeConfig setParquetTimeZone(String parquetTimeZone)
    {
        this.parquetTimeZone = parquetTimeZone;
        return this;
    }

    @NotNull
    public DataSize getTargetMaxFileSize()
    {
        return targetMaxFileSize;
    }

    @Config("delta.target-max-file-size")
    @ConfigDescription("Target maximum size of written files; the actual size may be larger")
    public DeltaLakeConfig setTargetMaxFileSize(DataSize targetMaxFileSize)
    {
        this.targetMaxFileSize = targetMaxFileSize;
        return this;
    }

    @NotNull
    public DataSize getIdleWriterMinFileSize()
    {
        return idleWriterMinFileSize;
    }

    @Config("delta.idle-writer-min-file-size")
    @ConfigDescription("Minimum data written by a single partition writer before it can be consider as 'idle' and could be closed by the engine")
    public DeltaLakeConfig setIdleWriterMinFileSize(DataSize idleWriterMinFileSize)
    {
        this.idleWriterMinFileSize = idleWriterMinFileSize;
        return this;
    }

    public boolean isUniqueTableLocation()
    {
        return uniqueTableLocation;
    }

    @Config("delta.unique-table-location")
    @ConfigDescription("Use randomized, unique table locations")
    public DeltaLakeConfig setUniqueTableLocation(boolean uniqueTableLocation)
    {
        this.uniqueTableLocation = uniqueTableLocation;
        return this;
    }

    public boolean isRegisterTableProcedureEnabled()
    {
        return registerTableProcedureEnabled;
    }

    @Config("delta.register-table-procedure.enabled")
    @ConfigDescription("Allow users to call the register_table procedure")
    public DeltaLakeConfig setRegisterTableProcedureEnabled(boolean registerTableProcedureEnabled)
    {
        this.registerTableProcedureEnabled = registerTableProcedureEnabled;
        return this;
    }

    public boolean isProjectionPushdownEnabled()
    {
        return projectionPushdownEnabled;
    }

    @Config("delta.projection-pushdown-enabled")
    @ConfigDescription("Read only required fields from a row type")
    public DeltaLakeConfig setProjectionPushdownEnabled(boolean projectionPushdownEnabled)
    {
        this.projectionPushdownEnabled = projectionPushdownEnabled;
        return this;
    }

    public boolean isQueryPartitionFilterRequired()
    {
        return queryPartitionFilterRequired;
    }

    @Config("delta.query-partition-filter-required")
    @ConfigDescription("Require filter on at least one partition column")
    public DeltaLakeConfig setQueryPartitionFilterRequired(boolean queryPartitionFilterRequired)
    {
        this.queryPartitionFilterRequired = queryPartitionFilterRequired;
        return this;
    }

    public boolean isDeletionVectorsEnabled()
    {
        return deletionVectorsEnabled;
    }

    @Config("delta.deletion-vectors-enabled")
    @ConfigDescription("Enable deletion vectors by default for new tables")
    public DeltaLakeConfig setDeletionVectorsEnabled(boolean deletionVectorsEnabled)
    {
        this.deletionVectorsEnabled = deletionVectorsEnabled;
        return this;
    }

    public boolean isDeltaLogFileSystemCacheDisabled()
    {
        return deltaLogFileSystemCacheDisabled;
    }

    @Config("delta.fs.cache.disable-transaction-log-caching")
    @ConfigDescription("Disable filesystem caching of the _delta_log directory (effective only when fs.cache.enabled=true)")
    public DeltaLakeConfig setDeltaLogFileSystemCacheDisabled(boolean deltaLogFileSystemCacheDisabled)
    {
        this.deltaLogFileSystemCacheDisabled = deltaLogFileSystemCacheDisabled;
        return this;
    }

    @Min(1)
    public int getMetadataParallelism()
    {
        return metadataParallelism;
    }

    @ConfigDescription("Limits metadata enumeration calls parallelism")
    @Config("delta.metadata.parallelism")
    public DeltaLakeConfig setMetadataParallelism(int metadataParallelism)
    {
        this.metadataParallelism = metadataParallelism;
        return this;
    }
}
