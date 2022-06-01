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
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.plugin.hive.HiveCompressionCodec;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class DeltaLakeConfig
{
    public static final String VACUUM_MIN_RETENTION = "delta.vacuum.min-retention";

    // Runtime.getRuntime().maxMemory() is not 100% stable and may return slightly different value over JVM lifetime. We use
    // constant so default configuration for cache size is stable.
    @VisibleForTesting
    static final DataSize DEFAULT_DATA_FILE_CACHE_SIZE = DataSize.succinctBytes(Math.floorDiv(Runtime.getRuntime().maxMemory(), 10L));

    private Duration metadataCacheTtl = new Duration(5, TimeUnit.MINUTES);
    private DataSize dataFileCacheSize = DEFAULT_DATA_FILE_CACHE_SIZE;
    private int domainCompactionThreshold = 100;
    private int maxOutstandingSplits = 1_000;
    private int maxSplitsPerSecond = Integer.MAX_VALUE;
    private int maxInitialSplits = 200;
    private DataSize maxInitialSplitSize;
    private DataSize maxSplitSize = DataSize.of(64, MEGABYTE);
    private int maxPartitionsPerWriter = 100;
    private boolean unsafeWritesEnabled;
    private boolean checkpointRowStatisticsWritingEnabled = true;
    private long defaultCheckpointWritingInterval = 10;
    private boolean ignoreCheckpointWriteFailures;
    private Duration vacuumMinRetention = new Duration(7, DAYS);
    private Optional<String> hiveCatalogName = Optional.empty();
    private Duration dynamicFilteringWaitTimeout = new Duration(0, SECONDS);
    private boolean tableStatisticsEnabled = true;
    private boolean extendedStatisticsEnabled = true;
    private HiveCompressionCodec compressionCodec = HiveCompressionCodec.SNAPPY;

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

    @Config("delta.experimental.ignore-checkpoint-write-failures")
    public DeltaLakeConfig setIgnoreCheckpointWriteFailures(boolean ignoreCheckpointWriteFailures)
    {
        this.ignoreCheckpointWriteFailures = ignoreCheckpointWriteFailures;
        return this;
    }

    public boolean isIgnoreCheckpointWriteFailures()
    {
        return ignoreCheckpointWriteFailures;
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

    @Config("delta.extended-statistics.enabled")
    @ConfigDescription("Use extended statistics collected by ANALYZE")
    public DeltaLakeConfig setExtendedStatisticsEnabled(boolean extendedStatisticsEnabled)
    {
        this.extendedStatisticsEnabled = extendedStatisticsEnabled;
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
}
