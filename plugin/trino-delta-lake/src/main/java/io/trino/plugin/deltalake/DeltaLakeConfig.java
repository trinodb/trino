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
import io.airlift.configuration.DefunctConfig;
import io.airlift.configuration.LegacyConfig;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
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

@DefunctConfig("delta.experimental.ignore-checkpoint-write-failures")
public class DeltaLakeConfig
{
    public static final String EXTENDED_STATISTICS_ENABLED = "delta.extended-statistics.enabled";
    public static final String VACUUM_MIN_RETENTION = "delta.vacuum.min-retention";

    // Runtime.getRuntime().maxMemory() is not 100% stable and may return slightly different value over JVM lifetime. We use
    // constant so default configuration for cache size is stable.
    @VisibleForTesting
    static final DataSize DEFAULT_DATA_FILE_CACHE_SIZE = DataSize.succinctBytes(Math.floorDiv(Runtime.getRuntime().maxMemory(), 10L));

    private Duration metadataCacheTtl = new Duration(5, TimeUnit.MINUTES);
    private long metadataCacheMaxSize = 1000;
    private DataSize dataFileCacheSize = DEFAULT_DATA_FILE_CACHE_SIZE;
    private Duration dataFileCacheTtl = new Duration(30, TimeUnit.MINUTES);
    private int domainCompactionThreshold = 1000;
    private int maxOutstandingSplits = 1_000;
    private int maxSplitsPerSecond = Integer.MAX_VALUE;
    private int maxInitialSplits = 200;
    private DataSize maxInitialSplitSize;
    private DataSize maxSplitSize = DataSize.of(64, MEGABYTE);
    private double minimumAssignedSplitWeight = 0.05;
    private int maxPartitionsPerWriter = 100;
    private boolean unsafeWritesEnabled;
    private boolean checkpointRowStatisticsWritingEnabled = true;
    private long defaultCheckpointWritingInterval = 10;
    private Duration vacuumMinRetention = new Duration(7, DAYS);
    private Optional<String> hiveCatalogName = Optional.empty();
    private Duration dynamicFilteringWaitTimeout = new Duration(0, SECONDS);
    private boolean tableStatisticsEnabled = true;
    private boolean extendedStatisticsEnabled = true;
    private boolean collectExtendedStatisticsOnWrite = true;
    private HiveCompressionCodec compressionCodec = HiveCompressionCodec.SNAPPY;
    private long perTransactionMetastoreCacheMaximumSize = 1000;
    private boolean deleteSchemaLocationsFallback;
    private String parquetTimeZone = TimeZone.getDefault().getID();
    private DataSize targetMaxFileSize = DataSize.of(1, GIGABYTE);
    private boolean uniqueTableLocation = true;
    private boolean legacyCreateTableWithExistingLocationEnabled;
    private boolean registerTableProcedureEnabled;
    private boolean projectionPushdownEnabled = true;
    private boolean queryPartitionFilterRequired;

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

    public long getMetadataCacheMaxSize()
    {
        return metadataCacheMaxSize;
    }

    @Config("delta.metadata.cache-size")
    @ConfigDescription("Maximum number of Delta table metadata entries to cache")
    public DeltaLakeConfig setMetadataCacheMaxSize(long metadataCacheMaxSize)
    {
        this.metadataCacheMaxSize = metadataCacheMaxSize;
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

    public int getMaxInitialSplits()
    {
        return maxInitialSplits;
    }

    @Config("delta.max-initial-splits")
    public DeltaLakeConfig setMaxInitialSplits(int maxInitialSplits)
    {
        this.maxInitialSplits = maxInitialSplits;
        return this;
    }

    @NotNull
    public DataSize getMaxInitialSplitSize()
    {
        if (maxInitialSplitSize == null) {
            return DataSize.ofBytes(maxSplitSize.toBytes() / 2).to(maxSplitSize.getUnit());
        }
        return maxInitialSplitSize;
    }

    @Config("delta.max-initial-split-size")
    public DeltaLakeConfig setMaxInitialSplitSize(DataSize maxInitialSplitSize)
    {
        this.maxInitialSplitSize = maxInitialSplitSize;
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

    @Deprecated
    public boolean isLegacyCreateTableWithExistingLocationEnabled()
    {
        return legacyCreateTableWithExistingLocationEnabled;
    }

    @Deprecated
    @Config("delta.legacy-create-table-with-existing-location.enabled")
    @ConfigDescription("Enable using the CREATE TABLE statement to register an existing table")
    public DeltaLakeConfig setLegacyCreateTableWithExistingLocationEnabled(boolean legacyCreateTableWithExistingLocationEnabled)
    {
        this.legacyCreateTableWithExistingLocationEnabled = legacyCreateTableWithExistingLocationEnabled;
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
}
