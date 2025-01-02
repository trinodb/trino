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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import io.airlift.configuration.LegacyConfig;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MaxDataSize;
import io.airlift.units.MinDataSize;
import io.trino.plugin.hive.HiveSessionProperties.InsertExistingPartitionsBehavior;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import org.joda.time.DateTimeZone;

import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.plugin.hive.HiveSessionProperties.InsertExistingPartitionsBehavior.APPEND;
import static io.trino.plugin.hive.HiveSessionProperties.InsertExistingPartitionsBehavior.ERROR;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.MINUTES;

@DefunctConfig({
        "dfs.domain-socket-path",
        "hive.file-system-cache-ttl",
        "hive.max-global-split-iterator-threads",
        "hive.max-sort-files-per-bucket",
        "hive.bucket-writing",
        "hive.optimized-reader.enabled",
        "hive.rcfile-optimized-writer.enabled",
        "hive.time-zone",
        "hive.assume-canonical-partition-keys",
        "hive.partition-use-column-names",
        "hive.allow-corrupt-writes-for-testing",
        "hive.optimize-symlink-listing",
        "hive.s3select-pushdown.enabled",
        "hive.s3select-pushdown.experimental-textfile-pushdown-enabled",
        "hive.s3select-pushdown.max-connections",
})
public class HiveConfig
{
    private boolean singleStatementWritesOnly;

    private DataSize maxSplitSize = DataSize.of(64, MEGABYTE);
    private int maxPartitionsPerScan = 1_000_000;
    private int maxPartitionsForEagerLoad = 100_000;
    private int maxOutstandingSplits = 3_000;
    private DataSize maxOutstandingSplitsSize = DataSize.of(256, MEGABYTE);
    private int maxSplitIteratorThreads = 1_000;
    private int minPartitionBatchSize = 10;
    private int maxPartitionBatchSize = 100;
    private int maxInitialSplits = 200;
    private int splitLoaderConcurrency = 64;
    private Integer maxSplitsPerSecond;
    private DataSize maxInitialSplitSize;
    private int domainCompactionThreshold = 1000;
    private boolean forceLocalScheduling;
    private boolean recursiveDirWalkerEnabled;
    private boolean ignoreAbsentPartitions;

    private int maxConcurrentFileSystemOperations = 20;
    private int maxConcurrentMetastoreDrops = 20;
    private int maxConcurrentMetastoreUpdates = 20;
    private int maxPartitionDropsPerQuery = 100_000;

    private long perTransactionMetastoreCacheMaximumSize = 1000;

    private HiveStorageFormat hiveStorageFormat = HiveStorageFormat.ORC;
    private HiveCompressionOption hiveCompressionCodec = HiveCompressionOption.GZIP;
    private boolean respectTableFormat = true;
    private boolean immutablePartitions;
    private Optional<InsertExistingPartitionsBehavior> insertExistingPartitionsBehavior = Optional.empty();
    private boolean createEmptyBucketFiles;
    // This is meant to protect users who are misusing schema locations (by
    // putting schemas in locations with extraneous files), so default to false
    // to avoid deleting those files if Trino is unable to check.
    private boolean deleteSchemaLocationsFallback;
    private int maxPartitionsPerWriter = 100;
    private int writeValidationThreads = 16;
    private boolean validateBucketing = true;
    private boolean parallelPartitionedBucketedWrites = true;

    private DataSize textMaxLineLength = DataSize.of(100, MEGABYTE);

    private String orcLegacyTimeZone = TimeZone.getDefault().getID();

    private String parquetTimeZone = TimeZone.getDefault().getID();
    private boolean useParquetColumnNames = true;

    private String rcfileTimeZone = TimeZone.getDefault().getID();
    private boolean rcfileWriterValidate;

    private boolean skipDeletionForAlter;
    private boolean skipTargetCleanupOnRollback;

    private boolean bucketExecutionEnabled = true;
    private boolean sortedWritingEnabled = true;
    private boolean propagateTableScanSortingProperties;

    private boolean optimizeMismatchedBucketCount = true;
    private boolean writesToNonManagedTablesEnabled;
    private boolean createsOfNonManagedTablesEnabled = true;

    private boolean tableStatisticsEnabled = true;
    private int partitionStatisticsSampleSize = 100;
    private boolean ignoreCorruptedStatistics;
    private boolean collectColumnStatisticsOnWrite = true;

    private boolean isTemporaryStagingDirectoryEnabled = true;
    private String temporaryStagingDirectoryPath = "/tmp/presto-${USER}";
    private boolean delegateTransactionalManagedTableLocationToMetastore;

    private Duration fileStatusCacheExpireAfterWrite = new Duration(1, MINUTES);
    private DataSize fileStatusCacheMaxRetainedSize = DataSize.of(1, GIGABYTE);
    private List<String> fileStatusCacheTables = ImmutableList.of();
    private DataSize perTransactionFileStatusCacheMaxRetainedSize = DataSize.of(100, MEGABYTE);

    private boolean translateHiveViews;
    private boolean legacyHiveViewTranslation;
    private boolean hiveViewsRunAsInvoker;

    private Optional<Duration> hiveTransactionHeartbeatInterval = Optional.empty();
    private int hiveTransactionHeartbeatThreads = 5;

    private boolean allowRegisterPartition;
    private boolean queryPartitionFilterRequired;
    private Set<String> queryPartitionFilterRequiredSchemas = ImmutableSet.of();

    private boolean projectionPushdownEnabled = true;

    private Duration dynamicFilteringWaitTimeout = new Duration(0, MINUTES);

    private HiveTimestampPrecision timestampPrecision = HiveTimestampPrecision.DEFAULT_PRECISION;

    private Optional<String> icebergCatalogName = Optional.empty();
    private Optional<String> deltaLakeCatalogName = Optional.empty();
    private Optional<String> hudiCatalogName = Optional.empty();

    private DataSize targetMaxFileSize = DataSize.of(1, GIGABYTE);
    private DataSize idleWriterMinFileSize = DataSize.of(16, MEGABYTE);

    private boolean sizeBasedSplitWeightsEnabled = true;
    private double minimumAssignedSplitWeight = 0.05;
    private boolean autoPurge;

    private boolean partitionProjectionEnabled;

    private S3StorageClassFilter s3StorageClassFilter = S3StorageClassFilter.READ_ALL;

    private int metadataParallelism = 8;

    public boolean isSingleStatementWritesOnly()
    {
        return singleStatementWritesOnly;
    }

    @Config("hive.single-statement-writes")
    @ConfigDescription("Require transaction to be in auto-commit mode for writes")
    public HiveConfig setSingleStatementWritesOnly(boolean singleStatementWritesOnly)
    {
        this.singleStatementWritesOnly = singleStatementWritesOnly;
        return this;
    }

    public int getMaxInitialSplits()
    {
        return maxInitialSplits;
    }

    @Config("hive.max-initial-splits")
    public HiveConfig setMaxInitialSplits(int maxInitialSplits)
    {
        this.maxInitialSplits = maxInitialSplits;
        return this;
    }

    public DataSize getMaxInitialSplitSize()
    {
        if (maxInitialSplitSize == null) {
            return DataSize.ofBytes(maxSplitSize.toBytes() / 2).to(maxSplitSize.getUnit());
        }
        return maxInitialSplitSize;
    }

    @Config("hive.max-initial-split-size")
    public HiveConfig setMaxInitialSplitSize(DataSize maxInitialSplitSize)
    {
        this.maxInitialSplitSize = maxInitialSplitSize;
        return this;
    }

    @Min(1)
    public int getSplitLoaderConcurrency()
    {
        return splitLoaderConcurrency;
    }

    @Config("hive.split-loader-concurrency")
    public HiveConfig setSplitLoaderConcurrency(int splitLoaderConcurrency)
    {
        this.splitLoaderConcurrency = splitLoaderConcurrency;
        return this;
    }

    @Min(1)
    @Nullable
    public Integer getMaxSplitsPerSecond()
    {
        return maxSplitsPerSecond;
    }

    @Config("hive.max-splits-per-second")
    @ConfigDescription("Throttles the maximum number of splits that can be assigned to tasks per second")
    public HiveConfig setMaxSplitsPerSecond(Integer maxSplitsPerSecond)
    {
        this.maxSplitsPerSecond = maxSplitsPerSecond;
        return this;
    }

    @Min(1)
    public int getDomainCompactionThreshold()
    {
        return domainCompactionThreshold;
    }

    @Config("hive.domain-compaction-threshold")
    @ConfigDescription("Maximum ranges to allow in a tuple domain without compacting it")
    public HiveConfig setDomainCompactionThreshold(int domainCompactionThreshold)
    {
        this.domainCompactionThreshold = domainCompactionThreshold;
        return this;
    }

    public DataSize getTargetMaxFileSize()
    {
        return targetMaxFileSize;
    }

    @Config("hive.target-max-file-size")
    @ConfigDescription("Target maximum size of written files; the actual size may be larger")
    public HiveConfig setTargetMaxFileSize(DataSize targetMaxFileSize)
    {
        this.targetMaxFileSize = targetMaxFileSize;
        return this;
    }

    public DataSize getIdleWriterMinFileSize()
    {
        return idleWriterMinFileSize;
    }

    @Config("hive.idle-writer-min-file-size")
    @ConfigDescription("Minimum data written by a single partition writer before it can be consider as 'idle' and could be closed by the engine")
    public HiveConfig setIdleWriterMinFileSize(DataSize idleWriterMinFileSize)
    {
        this.idleWriterMinFileSize = idleWriterMinFileSize;
        return this;
    }

    public boolean isForceLocalScheduling()
    {
        return forceLocalScheduling;
    }

    @Config("hive.force-local-scheduling")
    public HiveConfig setForceLocalScheduling(boolean forceLocalScheduling)
    {
        this.forceLocalScheduling = forceLocalScheduling;
        return this;
    }

    @Min(1)
    public int getMaxConcurrentFileSystemOperations()
    {
        return maxConcurrentFileSystemOperations;
    }

    @LegacyConfig("hive.max-concurrent-file-renames")
    @Config("hive.max-concurrent-file-system-operations")
    public HiveConfig setMaxConcurrentFileSystemOperations(int maxConcurrentFileSystemOperations)
    {
        this.maxConcurrentFileSystemOperations = maxConcurrentFileSystemOperations;
        return this;
    }

    @Min(1)
    public int getMaxConcurrentMetastoreDrops()
    {
        return maxConcurrentMetastoreDrops;
    }

    @Config("hive.max-concurrent-metastore-drops")
    public HiveConfig setMaxConcurrentMetastoreDrops(int maxConcurrentMetastoreDeletes)
    {
        this.maxConcurrentMetastoreDrops = maxConcurrentMetastoreDeletes;
        return this;
    }

    @Min(1)
    public int getMaxConcurrentMetastoreUpdates()
    {
        return maxConcurrentMetastoreUpdates;
    }

    @Config("hive.max-concurrent-metastore-updates")
    public HiveConfig setMaxConcurrentMetastoreUpdates(int maxConcurrentMetastoreUpdates)
    {
        this.maxConcurrentMetastoreUpdates = maxConcurrentMetastoreUpdates;
        return this;
    }

    @Min(1)
    public int getMaxPartitionDropsPerQuery()
    {
        return maxPartitionDropsPerQuery;
    }

    @Config("hive.max-partition-drops-per-query")
    public HiveConfig setMaxPartitionDropsPerQuery(int maxPartitionDropsPerQuery)
    {
        this.maxPartitionDropsPerQuery = maxPartitionDropsPerQuery;
        return this;
    }

    @Config("hive.recursive-directories")
    public HiveConfig setRecursiveDirWalkerEnabled(boolean recursiveDirWalkerEnabled)
    {
        this.recursiveDirWalkerEnabled = recursiveDirWalkerEnabled;
        return this;
    }

    public boolean getRecursiveDirWalkerEnabled()
    {
        return recursiveDirWalkerEnabled;
    }

    public boolean isIgnoreAbsentPartitions()
    {
        return ignoreAbsentPartitions;
    }

    @Config("hive.ignore-absent-partitions")
    public HiveConfig setIgnoreAbsentPartitions(boolean ignoreAbsentPartitions)
    {
        this.ignoreAbsentPartitions = ignoreAbsentPartitions;
        return this;
    }

    @NotNull
    public DataSize getMaxSplitSize()
    {
        return maxSplitSize;
    }

    @Config("hive.max-split-size")
    public HiveConfig setMaxSplitSize(DataSize maxSplitSize)
    {
        this.maxSplitSize = maxSplitSize;
        return this;
    }

    @Min(1)
    public int getMaxPartitionsPerScan()
    {
        return maxPartitionsPerScan;
    }

    @Config("hive.max-partitions-per-scan")
    @ConfigDescription("Maximum allowed partitions for a single table scan")
    public HiveConfig setMaxPartitionsPerScan(int maxPartitionsPerScan)
    {
        this.maxPartitionsPerScan = maxPartitionsPerScan;
        return this;
    }

    @Min(1)
    public int getMaxPartitionsForEagerLoad()
    {
        return maxPartitionsForEagerLoad;
    }

    @Config("hive.max-partitions-for-eager-load")
    @ConfigDescription("Maximum allowed partitions for a single table scan to be loaded eagerly on coordinator. Certain optimizations are not possible without eager loading.")
    public HiveConfig setMaxPartitionsForEagerLoad(int maxPartitionsForEagerLoad)
    {
        this.maxPartitionsForEagerLoad = maxPartitionsForEagerLoad;
        return this;
    }

    @Min(1)
    public int getMaxOutstandingSplits()
    {
        return maxOutstandingSplits;
    }

    @Config("hive.max-outstanding-splits")
    @ConfigDescription("Target number of buffered splits for each table scan in a query, before the scheduler tries to pause itself")
    public HiveConfig setMaxOutstandingSplits(int maxOutstandingSplits)
    {
        this.maxOutstandingSplits = maxOutstandingSplits;
        return this;
    }

    @MinDataSize("1MB")
    public DataSize getMaxOutstandingSplitsSize()
    {
        return maxOutstandingSplitsSize;
    }

    @Config("hive.max-outstanding-splits-size")
    @ConfigDescription("Maximum amount of memory allowed for split buffering for each table scan in a query, before the query is failed")
    public HiveConfig setMaxOutstandingSplitsSize(DataSize maxOutstandingSplits)
    {
        this.maxOutstandingSplitsSize = maxOutstandingSplits;
        return this;
    }

    @Min(1)
    public int getMaxSplitIteratorThreads()
    {
        return maxSplitIteratorThreads;
    }

    @Config("hive.max-split-iterator-threads")
    public HiveConfig setMaxSplitIteratorThreads(int maxSplitIteratorThreads)
    {
        this.maxSplitIteratorThreads = maxSplitIteratorThreads;
        return this;
    }

    @Min(1)
    public long getPerTransactionMetastoreCacheMaximumSize()
    {
        return perTransactionMetastoreCacheMaximumSize;
    }

    @Config("hive.per-transaction-metastore-cache-maximum-size")
    public HiveConfig setPerTransactionMetastoreCacheMaximumSize(long perTransactionMetastoreCacheMaximumSize)
    {
        this.perTransactionMetastoreCacheMaximumSize = perTransactionMetastoreCacheMaximumSize;
        return this;
    }

    @Min(1)
    public int getMinPartitionBatchSize()
    {
        return minPartitionBatchSize;
    }

    @Config("hive.metastore.partition-batch-size.min")
    public HiveConfig setMinPartitionBatchSize(int minPartitionBatchSize)
    {
        this.minPartitionBatchSize = minPartitionBatchSize;
        return this;
    }

    @Min(1)
    public int getMaxPartitionBatchSize()
    {
        return maxPartitionBatchSize;
    }

    @Config("hive.metastore.partition-batch-size.max")
    public HiveConfig setMaxPartitionBatchSize(int maxPartitionBatchSize)
    {
        this.maxPartitionBatchSize = maxPartitionBatchSize;
        return this;
    }

    public HiveStorageFormat getHiveStorageFormat()
    {
        return hiveStorageFormat;
    }

    @Config("hive.storage-format")
    public HiveConfig setHiveStorageFormat(HiveStorageFormat hiveStorageFormat)
    {
        this.hiveStorageFormat = hiveStorageFormat;
        return this;
    }

    public HiveCompressionOption getHiveCompressionCodec()
    {
        return hiveCompressionCodec;
    }

    @Config("hive.compression-codec")
    public HiveConfig setHiveCompressionCodec(HiveCompressionOption hiveCompressionCodec)
    {
        this.hiveCompressionCodec = hiveCompressionCodec;
        return this;
    }

    public boolean isRespectTableFormat()
    {
        return respectTableFormat;
    }

    @Config("hive.respect-table-format")
    @ConfigDescription("Should new partitions be written using the existing table format or the default Trino format")
    public HiveConfig setRespectTableFormat(boolean respectTableFormat)
    {
        this.respectTableFormat = respectTableFormat;
        return this;
    }

    public boolean isImmutablePartitions()
    {
        return immutablePartitions;
    }

    @Config("hive.immutable-partitions")
    @ConfigDescription("Can new data be inserted into existing partitions or existing unpartitioned tables")
    public HiveConfig setImmutablePartitions(boolean immutablePartitions)
    {
        this.immutablePartitions = immutablePartitions;
        return this;
    }

    public InsertExistingPartitionsBehavior getInsertExistingPartitionsBehavior()
    {
        return insertExistingPartitionsBehavior.orElse(immutablePartitions ? ERROR : APPEND);
    }

    @Config("hive.insert-existing-partitions-behavior")
    @ConfigDescription("Default value for insert existing partitions behavior")
    public HiveConfig setInsertExistingPartitionsBehavior(InsertExistingPartitionsBehavior insertExistingPartitionsBehavior)
    {
        this.insertExistingPartitionsBehavior = Optional.ofNullable(insertExistingPartitionsBehavior);
        return this;
    }

    @AssertTrue(message = "insert-existing-partitions-behavior cannot be APPEND when immutable-partitions is true")
    public boolean isInsertExistingPartitionsBehaviorValid()
    {
        return insertExistingPartitionsBehavior
                .map(v -> InsertExistingPartitionsBehavior.isValid(v, immutablePartitions))
                .orElse(true);
    }

    public boolean isCreateEmptyBucketFiles()
    {
        return createEmptyBucketFiles;
    }

    @Config("hive.create-empty-bucket-files")
    @ConfigDescription("Create empty files for buckets that have no data")
    public HiveConfig setCreateEmptyBucketFiles(boolean createEmptyBucketFiles)
    {
        this.createEmptyBucketFiles = createEmptyBucketFiles;
        return this;
    }

    public boolean isDeleteSchemaLocationsFallback()
    {
        return this.deleteSchemaLocationsFallback;
    }

    @Config("hive.delete-schema-locations-fallback")
    @ConfigDescription("Whether schema locations should be deleted when Trino can't determine whether they contain external files.")
    public HiveConfig setDeleteSchemaLocationsFallback(boolean deleteSchemaLocationsFallback)
    {
        this.deleteSchemaLocationsFallback = deleteSchemaLocationsFallback;
        return this;
    }

    @Min(1)
    public int getMaxPartitionsPerWriter()
    {
        return maxPartitionsPerWriter;
    }

    @Config("hive.max-partitions-per-writers")
    @ConfigDescription("Maximum number of partitions per writer")
    public HiveConfig setMaxPartitionsPerWriter(int maxPartitionsPerWriter)
    {
        this.maxPartitionsPerWriter = maxPartitionsPerWriter;
        return this;
    }

    public int getWriteValidationThreads()
    {
        return writeValidationThreads;
    }

    @Config("hive.write-validation-threads")
    @ConfigDescription("Number of threads used for verifying data after a write")
    public HiveConfig setWriteValidationThreads(int writeValidationThreads)
    {
        this.writeValidationThreads = writeValidationThreads;
        return this;
    }

    public boolean isValidateBucketing()
    {
        return validateBucketing;
    }

    @Config("hive.validate-bucketing")
    @ConfigDescription("Verify that data is bucketed correctly when reading")
    public HiveConfig setValidateBucketing(boolean validateBucketing)
    {
        this.validateBucketing = validateBucketing;
        return this;
    }

    public boolean isParallelPartitionedBucketedWrites()
    {
        return parallelPartitionedBucketedWrites;
    }

    @Config("hive.parallel-partitioned-bucketed-writes")
    @LegacyConfig("hive.parallel-partitioned-bucketed-inserts")
    @ConfigDescription("Improve parallelism of partitioned and bucketed table writes")
    public HiveConfig setParallelPartitionedBucketedWrites(boolean parallelPartitionedBucketedWrites)
    {
        this.parallelPartitionedBucketedWrites = parallelPartitionedBucketedWrites;
        return this;
    }

    public DateTimeZone getRcfileDateTimeZone()
    {
        TimeZone timeZone = TimeZone.getTimeZone(ZoneId.of(rcfileTimeZone));
        return DateTimeZone.forTimeZone(timeZone);
    }

    @NotNull
    public String getRcfileTimeZone()
    {
        return rcfileTimeZone;
    }

    @Config("hive.rcfile.time-zone")
    @ConfigDescription("Time zone for RCFile binary read and write")
    public HiveConfig setRcfileTimeZone(String rcfileTimeZone)
    {
        this.rcfileTimeZone = rcfileTimeZone;
        return this;
    }

    public boolean isRcfileWriterValidate()
    {
        return rcfileWriterValidate;
    }

    @Config("hive.rcfile.writer.validate")
    @ConfigDescription("Validate RCFile after write by re-reading the whole file")
    public HiveConfig setRcfileWriterValidate(boolean rcfileWriterValidate)
    {
        this.rcfileWriterValidate = rcfileWriterValidate;
        return this;
    }

    @MinDataSize("1B")
    @MaxDataSize("1GB")
    @NotNull
    public DataSize getTextMaxLineLength()
    {
        return textMaxLineLength;
    }

    @Config("hive.text.max-line-length")
    @ConfigDescription("Maximum line length for text files")
    public HiveConfig setTextMaxLineLength(DataSize textMaxLineLength)
    {
        this.textMaxLineLength = textMaxLineLength;
        return this;
    }

    public DateTimeZone getOrcLegacyDateTimeZone()
    {
        TimeZone timeZone = TimeZone.getTimeZone(ZoneId.of(orcLegacyTimeZone));
        return DateTimeZone.forTimeZone(timeZone);
    }

    @NotNull
    public String getOrcLegacyTimeZone()
    {
        return orcLegacyTimeZone;
    }

    @Config("hive.orc.time-zone")
    @ConfigDescription("Time zone for legacy ORC files that do not contain a time zone")
    public HiveConfig setOrcLegacyTimeZone(String orcLegacyTimeZone)
    {
        this.orcLegacyTimeZone = orcLegacyTimeZone;
        return this;
    }

    public DateTimeZone getParquetDateTimeZone()
    {
        TimeZone timeZone = TimeZone.getTimeZone(ZoneId.of(parquetTimeZone));
        return DateTimeZone.forTimeZone(timeZone);
    }

    @NotNull
    public String getParquetTimeZone()
    {
        return parquetTimeZone;
    }

    @Config("hive.parquet.time-zone")
    @ConfigDescription("Time zone for Parquet read and write")
    public HiveConfig setParquetTimeZone(String parquetTimeZone)
    {
        this.parquetTimeZone = parquetTimeZone;
        return this;
    }

    public boolean isUseParquetColumnNames()
    {
        return useParquetColumnNames;
    }

    @Config("hive.parquet.use-column-names")
    @ConfigDescription("Access Parquet columns using names from the file")
    public HiveConfig setUseParquetColumnNames(boolean useParquetColumnNames)
    {
        this.useParquetColumnNames = useParquetColumnNames;
        return this;
    }

    public boolean isOptimizeMismatchedBucketCount()
    {
        return optimizeMismatchedBucketCount;
    }

    @Config("hive.optimize-mismatched-bucket-count")
    public HiveConfig setOptimizeMismatchedBucketCount(boolean optimizeMismatchedBucketCount)
    {
        this.optimizeMismatchedBucketCount = optimizeMismatchedBucketCount;
        return this;
    }

    public List<String> getFileStatusCacheTables()
    {
        return fileStatusCacheTables;
    }

    @Config("hive.file-status-cache-tables")
    public HiveConfig setFileStatusCacheTables(List<String> fileStatusCacheTables)
    {
        this.fileStatusCacheTables = ImmutableList.copyOf(fileStatusCacheTables);
        return this;
    }

    @MinDataSize("0MB")
    @NotNull
    public DataSize getPerTransactionFileStatusCacheMaxRetainedSize()
    {
        return perTransactionFileStatusCacheMaxRetainedSize;
    }

    @Config("hive.per-transaction-file-status-cache.max-retained-size")
    @ConfigDescription("Maximum retained size of file statuses cached by transactional file status cache")
    public HiveConfig setPerTransactionFileStatusCacheMaxRetainedSize(DataSize perTransactionFileStatusCacheMaxRetainedSize)
    {
        this.perTransactionFileStatusCacheMaxRetainedSize = perTransactionFileStatusCacheMaxRetainedSize;
        return this;
    }

    @Deprecated
    @LegacyConfig(value = "hive.per-transaction-file-status-cache-maximum-size", replacedBy = "hive.per-transaction-file-status-cache.max-retained-size")
    @ConfigDescription("Maximum number of file statuses cached by transactional file status cache")
    public HiveConfig setPerTransactionFileStatusCacheMaximumSize(long perTransactionFileStatusCacheMaximumSize)
    {
        // assume some fixed size per entry in order to keep the deprecated property for backward compatibility
        this.perTransactionFileStatusCacheMaxRetainedSize = DataSize.of(perTransactionFileStatusCacheMaximumSize, KILOBYTE);
        return this;
    }

    public boolean isTranslateHiveViews()
    {
        return translateHiveViews;
    }

    @LegacyConfig({"hive.views-execution.enabled", "hive.translate-hive-views"})
    @Config("hive.hive-views.enabled")
    @ConfigDescription("Experimental: Allow translation of Hive views into Trino views")
    public HiveConfig setTranslateHiveViews(boolean translateHiveViews)
    {
        this.translateHiveViews = translateHiveViews;
        return this;
    }

    public boolean isLegacyHiveViewTranslation()
    {
        return this.legacyHiveViewTranslation;
    }

    @LegacyConfig("hive.legacy-hive-view-translation")
    @Config("hive.hive-views.legacy-translation")
    @ConfigDescription("Use legacy Hive view translation mechanism")
    public HiveConfig setLegacyHiveViewTranslation(boolean legacyHiveViewTranslation)
    {
        this.legacyHiveViewTranslation = legacyHiveViewTranslation;
        return this;
    }

    public boolean isHiveViewsRunAsInvoker()
    {
        return hiveViewsRunAsInvoker;
    }

    @Config("hive.hive-views.run-as-invoker")
    @ConfigDescription("Execute Hive views with permissions of invoker")
    public HiveConfig setHiveViewsRunAsInvoker(boolean hiveViewsRunAsInvoker)
    {
        this.hiveViewsRunAsInvoker = hiveViewsRunAsInvoker;
        return this;
    }

    @MinDataSize("0MB")
    @NotNull
    public DataSize getFileStatusCacheMaxRetainedSize()
    {
        return fileStatusCacheMaxRetainedSize;
    }

    @Config("hive.file-status-cache.max-retained-size")
    public HiveConfig setFileStatusCacheMaxRetainedSize(DataSize fileStatusCacheMaxRetainedSize)
    {
        this.fileStatusCacheMaxRetainedSize = fileStatusCacheMaxRetainedSize;
        return this;
    }

    @Deprecated
    @LegacyConfig(value = "hive.file-status-cache-size", replacedBy = "hive.file-status-cache.max-retained-size")
    public HiveConfig setFileStatusCacheMaxSize(long fileStatusCacheMaxSize)
    {
        // assume some fixed size per entry in order to keep the deprecated property for backward compatibility
        this.fileStatusCacheMaxRetainedSize = DataSize.of(fileStatusCacheMaxSize, KILOBYTE);
        return this;
    }

    public Duration getFileStatusCacheExpireAfterWrite()
    {
        return fileStatusCacheExpireAfterWrite;
    }

    @Config("hive.file-status-cache-expire-time")
    public HiveConfig setFileStatusCacheExpireAfterWrite(Duration fileStatusCacheExpireAfterWrite)
    {
        this.fileStatusCacheExpireAfterWrite = fileStatusCacheExpireAfterWrite;
        return this;
    }

    public boolean isSkipDeletionForAlter()
    {
        return skipDeletionForAlter;
    }

    @Config("hive.skip-deletion-for-alter")
    @ConfigDescription("Skip deletion of old partition data when a partition is deleted and then inserted in the same transaction")
    public HiveConfig setSkipDeletionForAlter(boolean skipDeletionForAlter)
    {
        this.skipDeletionForAlter = skipDeletionForAlter;
        return this;
    }

    public boolean isSkipTargetCleanupOnRollback()
    {
        return skipTargetCleanupOnRollback;
    }

    @Config("hive.skip-target-cleanup-on-rollback")
    @ConfigDescription("Skip deletion of target directories when a metastore operation fails")
    public HiveConfig setSkipTargetCleanupOnRollback(boolean skipTargetCleanupOnRollback)
    {
        this.skipTargetCleanupOnRollback = skipTargetCleanupOnRollback;
        return this;
    }

    public boolean isBucketExecutionEnabled()
    {
        return bucketExecutionEnabled;
    }

    @Config("hive.bucket-execution")
    @ConfigDescription("Enable bucket-aware execution: use physical bucketing information to optimize queries")
    public HiveConfig setBucketExecutionEnabled(boolean bucketExecutionEnabled)
    {
        this.bucketExecutionEnabled = bucketExecutionEnabled;
        return this;
    }

    public boolean isSortedWritingEnabled()
    {
        return sortedWritingEnabled;
    }

    @Config("hive.sorted-writing")
    @ConfigDescription("Enable writing to bucketed sorted tables")
    public HiveConfig setSortedWritingEnabled(boolean sortedWritingEnabled)
    {
        this.sortedWritingEnabled = sortedWritingEnabled;
        return this;
    }

    public boolean isPropagateTableScanSortingProperties()
    {
        return propagateTableScanSortingProperties;
    }

    @Config("hive.propagate-table-scan-sorting-properties")
    @ConfigDescription("Use sorted table layout to generate more efficient execution plans. May lead to incorrect results if files are not sorted as per table definition.")
    public HiveConfig setPropagateTableScanSortingProperties(boolean propagateTableScanSortingProperties)
    {
        this.propagateTableScanSortingProperties = propagateTableScanSortingProperties;
        return this;
    }

    @Config("hive.non-managed-table-writes-enabled")
    @ConfigDescription("Enable writes to non-managed (external) tables")
    public HiveConfig setWritesToNonManagedTablesEnabled(boolean writesToNonManagedTablesEnabled)
    {
        this.writesToNonManagedTablesEnabled = writesToNonManagedTablesEnabled;
        return this;
    }

    public boolean getWritesToNonManagedTablesEnabled()
    {
        return writesToNonManagedTablesEnabled;
    }

    @Config("hive.non-managed-table-creates-enabled")
    @ConfigDescription("Enable non-managed (external) table creates")
    public HiveConfig setCreatesOfNonManagedTablesEnabled(boolean createsOfNonManagedTablesEnabled)
    {
        this.createsOfNonManagedTablesEnabled = createsOfNonManagedTablesEnabled;
        return this;
    }

    public boolean getCreatesOfNonManagedTablesEnabled()
    {
        return createsOfNonManagedTablesEnabled;
    }

    @Config("hive.table-statistics-enabled")
    @ConfigDescription("Enable use of table statistics")
    public HiveConfig setTableStatisticsEnabled(boolean tableStatisticsEnabled)
    {
        this.tableStatisticsEnabled = tableStatisticsEnabled;
        return this;
    }

    public boolean isTableStatisticsEnabled()
    {
        return tableStatisticsEnabled;
    }

    @Min(1)
    public int getPartitionStatisticsSampleSize()
    {
        return partitionStatisticsSampleSize;
    }

    @Config("hive.partition-statistics-sample-size")
    @ConfigDescription("Maximum sample size of the partitions column statistics")
    public HiveConfig setPartitionStatisticsSampleSize(int partitionStatisticsSampleSize)
    {
        this.partitionStatisticsSampleSize = partitionStatisticsSampleSize;
        return this;
    }

    public boolean isIgnoreCorruptedStatistics()
    {
        return ignoreCorruptedStatistics;
    }

    @Config("hive.ignore-corrupted-statistics")
    @ConfigDescription("Ignore corrupted statistics rather than failing")
    public HiveConfig setIgnoreCorruptedStatistics(boolean ignoreCorruptedStatistics)
    {
        this.ignoreCorruptedStatistics = ignoreCorruptedStatistics;
        return this;
    }

    public boolean isCollectColumnStatisticsOnWrite()
    {
        return collectColumnStatisticsOnWrite;
    }

    @Config("hive.collect-column-statistics-on-write")
    @ConfigDescription("Enables automatic column level statistics collection on write")
    public HiveConfig setCollectColumnStatisticsOnWrite(boolean collectColumnStatisticsOnWrite)
    {
        this.collectColumnStatisticsOnWrite = collectColumnStatisticsOnWrite;
        return this;
    }

    @Config("hive.temporary-staging-directory-enabled")
    @ConfigDescription("Should use (if possible) temporary staging directory for write operations")
    public HiveConfig setTemporaryStagingDirectoryEnabled(boolean temporaryStagingDirectoryEnabled)
    {
        this.isTemporaryStagingDirectoryEnabled = temporaryStagingDirectoryEnabled;
        return this;
    }

    public boolean isTemporaryStagingDirectoryEnabled()
    {
        return isTemporaryStagingDirectoryEnabled;
    }

    @Config("hive.temporary-staging-directory-path")
    @ConfigDescription("Location of temporary staging directory for write operations. Use ${USER} placeholder to use different location for each user.")
    public HiveConfig setTemporaryStagingDirectoryPath(String temporaryStagingDirectoryPath)
    {
        this.temporaryStagingDirectoryPath = temporaryStagingDirectoryPath;
        return this;
    }

    @NotNull
    public String getTemporaryStagingDirectoryPath()
    {
        return temporaryStagingDirectoryPath;
    }

    @Config("hive.delegate-transactional-managed-table-location-to-metastore")
    @ConfigDescription("When transactional, managed table is created via Trino the location will not be set in request sent to HMS and location will be determined by metastore; if this value is set to true, CREATE TABLE AS queries are not supported.")
    public HiveConfig setDelegateTransactionalManagedTableLocationToMetastore(boolean delegateTransactionalManagedTableLocationToMetastore)
    {
        this.delegateTransactionalManagedTableLocationToMetastore = delegateTransactionalManagedTableLocationToMetastore;
        return this;
    }

    public boolean isDelegateTransactionalManagedTableLocationToMetastore()
    {
        return delegateTransactionalManagedTableLocationToMetastore;
    }

    @Config("hive.transaction-heartbeat-interval")
    @ConfigDescription("Interval after which heartbeat is sent for open Hive transaction")
    public HiveConfig setHiveTransactionHeartbeatInterval(Duration interval)
    {
        this.hiveTransactionHeartbeatInterval = Optional.ofNullable(interval);
        return this;
    }

    @NotNull
    public Optional<Duration> getHiveTransactionHeartbeatInterval()
    {
        return hiveTransactionHeartbeatInterval;
    }

    public int getHiveTransactionHeartbeatThreads()
    {
        return hiveTransactionHeartbeatThreads;
    }

    @Config("hive.transaction-heartbeat-threads")
    @ConfigDescription("Number of threads to run in the Hive transaction heartbeat service")
    public HiveConfig setHiveTransactionHeartbeatThreads(int hiveTransactionHeartbeatThreads)
    {
        this.hiveTransactionHeartbeatThreads = hiveTransactionHeartbeatThreads;
        return this;
    }

    public boolean isAllowRegisterPartition()
    {
        return allowRegisterPartition;
    }

    @Config("hive.allow-register-partition-procedure")
    public HiveConfig setAllowRegisterPartition(boolean allowRegisterPartition)
    {
        this.allowRegisterPartition = allowRegisterPartition;
        return this;
    }

    public boolean isQueryPartitionFilterRequired()
    {
        return queryPartitionFilterRequired;
    }

    @Config("hive.query-partition-filter-required")
    @ConfigDescription("Require filter on at least one partition column")
    public HiveConfig setQueryPartitionFilterRequired(boolean queryPartitionFilterRequired)
    {
        this.queryPartitionFilterRequired = queryPartitionFilterRequired;
        return this;
    }

    public Set<String> getQueryPartitionFilterRequiredSchemas()
    {
        return queryPartitionFilterRequiredSchemas;
    }

    @Config("hive.query-partition-filter-required-schemas")
    @ConfigDescription("List of schemas for which filter on partition column is enforced")
    public HiveConfig setQueryPartitionFilterRequiredSchemas(List<String> queryPartitionFilterRequiredSchemas)
    {
        this.queryPartitionFilterRequiredSchemas = queryPartitionFilterRequiredSchemas.stream()
                .map(value -> value.toLowerCase(ENGLISH))
                .collect(toImmutableSet());
        return this;
    }

    public boolean isProjectionPushdownEnabled()
    {
        return projectionPushdownEnabled;
    }

    @Config("hive.projection-pushdown-enabled")
    @ConfigDescription("Projection pushdown into hive is enabled through applyProjection")
    public HiveConfig setProjectionPushdownEnabled(boolean projectionPushdownEnabled)
    {
        this.projectionPushdownEnabled = projectionPushdownEnabled;
        return this;
    }

    @NotNull
    public Duration getDynamicFilteringWaitTimeout()
    {
        return dynamicFilteringWaitTimeout;
    }

    @Config("hive.dynamic-filtering.wait-timeout")
    @LegacyConfig("hive.dynamic-filtering-probe-blocking-timeout")
    @ConfigDescription("Duration to wait for completion of dynamic filters during split generation")
    public HiveConfig setDynamicFilteringWaitTimeout(Duration dynamicFilteringWaitTimeout)
    {
        this.dynamicFilteringWaitTimeout = dynamicFilteringWaitTimeout;
        return this;
    }

    public HiveTimestampPrecision getTimestampPrecision()
    {
        return timestampPrecision;
    }

    @Config("hive.timestamp-precision")
    @ConfigDescription("Precision used to represent timestamps")
    public HiveConfig setTimestampPrecision(HiveTimestampPrecision timestampPrecision)
    {
        this.timestampPrecision = timestampPrecision;
        return this;
    }

    public Optional<String> getIcebergCatalogName()
    {
        return icebergCatalogName;
    }

    @Config("hive.iceberg-catalog-name")
    @ConfigDescription("The catalog to redirect iceberg tables to")
    public HiveConfig setIcebergCatalogName(String icebergCatalogName)
    {
        this.icebergCatalogName = Optional.ofNullable(icebergCatalogName);
        return this;
    }

    @Config("hive.size-based-split-weights-enabled")
    public HiveConfig setSizeBasedSplitWeightsEnabled(boolean sizeBasedSplitWeightsEnabled)
    {
        this.sizeBasedSplitWeightsEnabled = sizeBasedSplitWeightsEnabled;
        return this;
    }

    public boolean isSizeBasedSplitWeightsEnabled()
    {
        return sizeBasedSplitWeightsEnabled;
    }

    @Config("hive.minimum-assigned-split-weight")
    @ConfigDescription("Minimum weight that a split can be assigned when size based split weights are enabled")
    public HiveConfig setMinimumAssignedSplitWeight(double minimumAssignedSplitWeight)
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

    public Optional<String> getDeltaLakeCatalogName()
    {
        return deltaLakeCatalogName;
    }

    @Config("hive.delta-lake-catalog-name")
    @ConfigDescription("Catalog to redirect to when a Delta Lake table is referenced")
    public HiveConfig setDeltaLakeCatalogName(String deltaLakeCatalogName)
    {
        this.deltaLakeCatalogName = Optional.ofNullable(deltaLakeCatalogName);
        return this;
    }

    public Optional<String> getHudiCatalogName()
    {
        return hudiCatalogName;
    }

    @Config("hive.hudi-catalog-name")
    @ConfigDescription("Catalog to redirect to when a Hudi table is referenced")
    public HiveConfig setHudiCatalogName(String hudiCatalogName)
    {
        this.hudiCatalogName = Optional.ofNullable(hudiCatalogName);
        return this;
    }

    public boolean isAutoPurge()
    {
        return this.autoPurge;
    }

    @Config("hive.auto-purge")
    public HiveConfig setAutoPurge(boolean autoPurge)
    {
        this.autoPurge = autoPurge;
        return this;
    }

    public boolean isPartitionProjectionEnabled()
    {
        return partitionProjectionEnabled;
    }

    @Config("hive.partition-projection-enabled")
    @ConfigDescription("Enables AWS Athena partition projection")
    public HiveConfig setPartitionProjectionEnabled(boolean enabledAthenaPartitionProjection)
    {
        this.partitionProjectionEnabled = enabledAthenaPartitionProjection;
        return this;
    }

    public S3StorageClassFilter getS3StorageClassFilter()
    {
        return s3StorageClassFilter;
    }

    @Config("hive.s3.storage-class-filter")
    @ConfigDescription("Filter based on storage class of S3 object")
    public HiveConfig setS3StorageClassFilter(S3StorageClassFilter s3StorageClassFilter)
    {
        this.s3StorageClassFilter = s3StorageClassFilter;
        return this;
    }

    @Min(1)
    public int getMetadataParallelism()
    {
        return metadataParallelism;
    }

    @ConfigDescription("Limits metadata enumeration calls parallelism")
    @Config("hive.metadata.parallelism")
    public HiveConfig setMetadataParallelism(int metadataParallelism)
    {
        this.metadataParallelism = metadataParallelism;
        return this;
    }
}
