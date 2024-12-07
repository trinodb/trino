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
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.orc.OrcWriteValidation.OrcWriteValidationMode;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.hive.HiveCompressionCodec;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.base.session.PropertyMetadataUtil.dataSizeProperty;
import static io.trino.plugin.base.session.PropertyMetadataUtil.durationProperty;
import static io.trino.plugin.base.session.PropertyMetadataUtil.validateMaxDataSize;
import static io.trino.plugin.base.session.PropertyMetadataUtil.validateMinDataSize;
import static io.trino.plugin.hive.parquet.ParquetReaderConfig.PARQUET_READER_MAX_SMALL_FILE_THRESHOLD;
import static io.trino.plugin.hive.parquet.ParquetWriterConfig.PARQUET_WRITER_MAX_BLOCK_SIZE;
import static io.trino.plugin.hive.parquet.ParquetWriterConfig.PARQUET_WRITER_MAX_PAGE_SIZE;
import static io.trino.plugin.hive.parquet.ParquetWriterConfig.PARQUET_WRITER_MAX_PAGE_VALUE_COUNT;
import static io.trino.plugin.hive.parquet.ParquetWriterConfig.PARQUET_WRITER_MIN_PAGE_SIZE;
import static io.trino.plugin.hive.parquet.ParquetWriterConfig.PARQUET_WRITER_MIN_PAGE_VALUE_COUNT;
import static io.trino.plugin.iceberg.IcebergConfig.COLLECT_EXTENDED_STATISTICS_ON_WRITE_DESCRIPTION;
import static io.trino.plugin.iceberg.IcebergConfig.EXTENDED_STATISTICS_DESCRIPTION;
import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.doubleProperty;
import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class IcebergSessionProperties
        implements SessionPropertiesProvider
{
    public static final String SPLIT_SIZE = "experimental_split_size";
    private static final String COMPRESSION_CODEC = "compression_codec";
    private static final String USE_FILE_SIZE_FROM_METADATA = "use_file_size_from_metadata";
    private static final String ORC_BLOOM_FILTERS_ENABLED = "orc_bloom_filters_enabled";
    private static final String ORC_MAX_MERGE_DISTANCE = "orc_max_merge_distance";
    private static final String ORC_MAX_BUFFER_SIZE = "orc_max_buffer_size";
    private static final String ORC_STREAM_BUFFER_SIZE = "orc_stream_buffer_size";
    private static final String ORC_TINY_STRIPE_THRESHOLD = "orc_tiny_stripe_threshold";
    private static final String ORC_MAX_READ_BLOCK_SIZE = "orc_max_read_block_size";
    private static final String ORC_LAZY_READ_SMALL_RANGES = "orc_lazy_read_small_ranges";
    private static final String ORC_NESTED_LAZY_ENABLED = "orc_nested_lazy_enabled";
    private static final String ORC_STRING_STATISTICS_LIMIT = "orc_string_statistics_limit";
    private static final String ORC_WRITER_VALIDATE_PERCENTAGE = "orc_writer_validate_percentage";
    private static final String ORC_WRITER_VALIDATE_MODE = "orc_writer_validate_mode";
    private static final String ORC_WRITER_MIN_STRIPE_SIZE = "orc_writer_min_stripe_size";
    private static final String ORC_WRITER_MAX_STRIPE_SIZE = "orc_writer_max_stripe_size";
    private static final String ORC_WRITER_MAX_STRIPE_ROWS = "orc_writer_max_stripe_rows";
    private static final String ORC_WRITER_MAX_ROW_GROUP_ROWS = "orc_writer_max_row_group_rows";
    private static final String ORC_WRITER_MAX_DICTIONARY_MEMORY = "orc_writer_max_dictionary_memory";
    private static final String PARQUET_MAX_READ_BLOCK_SIZE = "parquet_max_read_block_size";
    private static final String PARQUET_USE_BLOOM_FILTER = "parquet_use_bloom_filter";
    private static final String PARQUET_MAX_READ_BLOCK_ROW_COUNT = "parquet_max_read_block_row_count";
    private static final String PARQUET_SMALL_FILE_THRESHOLD = "parquet_small_file_threshold";
    private static final String PARQUET_IGNORE_STATISTICS = "parquet_ignore_statistics";
    private static final String PARQUET_VECTORIZED_DECODING_ENABLED = "parquet_vectorized_decoding_enabled";
    private static final String PARQUET_WRITER_BLOCK_SIZE = "parquet_writer_block_size";
    private static final String PARQUET_WRITER_PAGE_SIZE = "parquet_writer_page_size";
    private static final String PARQUET_WRITER_PAGE_VALUE_COUNT = "parquet_writer_page_value_count";
    private static final String PARQUET_WRITER_BATCH_SIZE = "parquet_writer_batch_size";
    public static final String DYNAMIC_FILTERING_WAIT_TIMEOUT = "dynamic_filtering_wait_timeout";
    private static final String STATISTICS_ENABLED = "statistics_enabled";
    public static final String EXTENDED_STATISTICS_ENABLED = "extended_statistics_enabled";
    private static final String PROJECTION_PUSHDOWN_ENABLED = "projection_pushdown_enabled";
    private static final String TARGET_MAX_FILE_SIZE = "target_max_file_size";
    private static final String IDLE_WRITER_MIN_FILE_SIZE = "idle_writer_min_file_size";
    public static final String COLLECT_EXTENDED_STATISTICS_ON_WRITE = "collect_extended_statistics_on_write";
    private static final String HIVE_CATALOG_NAME = "hive_catalog_name";
    private static final String MINIMUM_ASSIGNED_SPLIT_WEIGHT = "minimum_assigned_split_weight";
    public static final String EXPIRE_SNAPSHOTS_MIN_RETENTION = "expire_snapshots_min_retention";
    public static final String REMOVE_ORPHAN_FILES_MIN_RETENTION = "remove_orphan_files_min_retention";
    private static final String MERGE_MANIFESTS_ON_WRITE = "merge_manifests_on_write";
    private static final String SORTED_WRITING_ENABLED = "sorted_writing_enabled";
    private static final String QUERY_PARTITION_FILTER_REQUIRED = "query_partition_filter_required";
    private static final String QUERY_PARTITION_FILTER_REQUIRED_SCHEMAS = "query_partition_filter_required_schemas";
    private static final String INCREMENTAL_REFRESH_ENABLED = "incremental_refresh_enabled";
    public static final String BUCKET_EXECUTION_ENABLED = "bucket_execution_enabled";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public IcebergSessionProperties(
            IcebergConfig icebergConfig,
            OrcReaderConfig orcReaderConfig,
            OrcWriterConfig orcWriterConfig,
            ParquetReaderConfig parquetReaderConfig,
            ParquetWriterConfig parquetWriterConfig)
    {
        sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(dataSizeProperty(
                        SPLIT_SIZE,
                        "Target split size",
                        // Note: this is null by default & hidden, currently mainly for tests.
                        // See https://github.com/trinodb/trino/issues/9018#issuecomment-1752929193 for further discussion.
                        null,
                        true))
                .add(enumProperty(
                        COMPRESSION_CODEC,
                        "Compression codec to use when writing files",
                        HiveCompressionCodec.class,
                        icebergConfig.getCompressionCodec(),
                        false))
                .add(booleanProperty(
                        USE_FILE_SIZE_FROM_METADATA,
                        "Use file size stored in Iceberg metadata",
                        icebergConfig.isUseFileSizeFromMetadata(),
                        false))
                .add(booleanProperty(
                        ORC_BLOOM_FILTERS_ENABLED,
                        "ORC: Enable bloom filters for predicate pushdown",
                        orcReaderConfig.isBloomFiltersEnabled(),
                        false))
                .add(dataSizeProperty(
                        ORC_MAX_MERGE_DISTANCE,
                        "ORC: Maximum size of gap between two reads to merge into a single read",
                        orcReaderConfig.getMaxMergeDistance(),
                        false))
                .add(dataSizeProperty(
                        ORC_MAX_BUFFER_SIZE,
                        "ORC: Maximum size of a single read",
                        orcReaderConfig.getMaxBufferSize(),
                        false))
                .add(dataSizeProperty(
                        ORC_STREAM_BUFFER_SIZE,
                        "ORC: Size of buffer for streaming reads",
                        orcReaderConfig.getStreamBufferSize(),
                        false))
                .add(dataSizeProperty(
                        ORC_TINY_STRIPE_THRESHOLD,
                        "ORC: Threshold below which an ORC stripe or file will read in its entirety",
                        orcReaderConfig.getTinyStripeThreshold(),
                        false))
                .add(dataSizeProperty(
                        ORC_MAX_READ_BLOCK_SIZE,
                        "ORC: Soft max size of Trino blocks produced by ORC reader",
                        orcReaderConfig.getMaxBlockSize(),
                        false))
                .add(booleanProperty(
                        ORC_LAZY_READ_SMALL_RANGES,
                        "Experimental: ORC: Read small file segments lazily",
                        orcReaderConfig.isLazyReadSmallRanges(),
                        false))
                .add(booleanProperty(
                        ORC_NESTED_LAZY_ENABLED,
                        "Experimental: ORC: Lazily read nested data",
                        orcReaderConfig.isNestedLazy(),
                        false))
                .add(dataSizeProperty(
                        ORC_STRING_STATISTICS_LIMIT,
                        "ORC: Maximum size of string statistics; drop if exceeding",
                        orcWriterConfig.getStringStatisticsLimit(),
                        false))
                .add(doubleProperty(
                        ORC_WRITER_VALIDATE_PERCENTAGE,
                        "ORC: Percentage of written files to validate by re-reading them",
                        orcWriterConfig.getValidationPercentage(),
                        doubleValue -> {
                            if (doubleValue < 0.0 || doubleValue > 100.0) {
                                throw new TrinoException(INVALID_SESSION_PROPERTY, format(
                                        "%s must be between 0.0 and 100.0 inclusive: %s",
                                        ORC_WRITER_VALIDATE_PERCENTAGE,
                                        doubleValue));
                            }
                        },
                        false))
                .add(enumProperty(
                        ORC_WRITER_VALIDATE_MODE,
                        "ORC: Level of detail in ORC validation",
                        OrcWriteValidationMode.class,
                        orcWriterConfig.getValidationMode(),
                        false))
                .add(dataSizeProperty(
                        ORC_WRITER_MIN_STRIPE_SIZE,
                        "ORC: Min stripe size",
                        orcWriterConfig.getStripeMinSize(),
                        false))
                .add(dataSizeProperty(
                        ORC_WRITER_MAX_STRIPE_SIZE,
                        "ORC: Max stripe size",
                        orcWriterConfig.getStripeMaxSize(),
                        false))
                .add(integerProperty(
                        ORC_WRITER_MAX_STRIPE_ROWS,
                        "ORC: Max stripe row count",
                        orcWriterConfig.getStripeMaxRowCount(),
                        false))
                .add(integerProperty(
                        ORC_WRITER_MAX_ROW_GROUP_ROWS,
                        "ORC: Max number of rows in a row group",
                        orcWriterConfig.getRowGroupMaxRowCount(),
                        false))
                .add(dataSizeProperty(
                        ORC_WRITER_MAX_DICTIONARY_MEMORY,
                        "ORC: Max dictionary memory",
                        orcWriterConfig.getDictionaryMaxMemory(),
                        false))
                .add(dataSizeProperty(
                        PARQUET_MAX_READ_BLOCK_SIZE,
                        "Parquet: Maximum size of a block to read",
                        parquetReaderConfig.getMaxReadBlockSize(),
                        false))
                .add(booleanProperty(
                        PARQUET_USE_BLOOM_FILTER,
                        "Use Parquet Bloom filters",
                        parquetReaderConfig.isUseBloomFilter(),
                        false))
                .add(integerProperty(
                        PARQUET_MAX_READ_BLOCK_ROW_COUNT,
                        "Parquet: Maximum number of rows read in a batch",
                        parquetReaderConfig.getMaxReadBlockRowCount(),
                        value -> {
                            if (value < 128 || value > 65536) {
                                throw new TrinoException(
                                        INVALID_SESSION_PROPERTY,
                                        format("%s must be between 128 and 65536: %s", PARQUET_MAX_READ_BLOCK_ROW_COUNT, value));
                            }
                        },
                        false))
                .add(dataSizeProperty(
                        PARQUET_SMALL_FILE_THRESHOLD,
                        "Parquet: Size below which a parquet file will be read entirely",
                        parquetReaderConfig.getSmallFileThreshold(),
                        value -> validateMaxDataSize(PARQUET_SMALL_FILE_THRESHOLD, value, DataSize.valueOf(PARQUET_READER_MAX_SMALL_FILE_THRESHOLD)),
                        false))
                .add(booleanProperty(
                        PARQUET_IGNORE_STATISTICS,
                        "Ignore statistics from Parquet to allow querying files with corrupted or incorrect statistics",
                        parquetReaderConfig.isIgnoreStatistics(),
                        false))
                .add(booleanProperty(
                        PARQUET_VECTORIZED_DECODING_ENABLED,
                        "Enable using Java Vector API for faster decoding of parquet files",
                        parquetReaderConfig.isVectorizedDecodingEnabled(),
                        false))
                .add(dataSizeProperty(
                        PARQUET_WRITER_BLOCK_SIZE,
                        "Parquet: Writer block size",
                        parquetWriterConfig.getBlockSize(),
                        value -> validateMaxDataSize(PARQUET_WRITER_BLOCK_SIZE, value, DataSize.valueOf(PARQUET_WRITER_MAX_BLOCK_SIZE)),
                        false))
                .add(dataSizeProperty(
                        PARQUET_WRITER_PAGE_SIZE,
                        "Parquet: Writer page size",
                        parquetWriterConfig.getPageSize(),
                        value -> {
                            validateMinDataSize(PARQUET_WRITER_PAGE_SIZE, value, DataSize.valueOf(PARQUET_WRITER_MIN_PAGE_SIZE));
                            validateMaxDataSize(PARQUET_WRITER_PAGE_SIZE, value, DataSize.valueOf(PARQUET_WRITER_MAX_PAGE_SIZE));
                        },
                        false))
                .add(integerProperty(
                        PARQUET_WRITER_PAGE_VALUE_COUNT,
                        "Parquet: Writer page row count",
                        parquetWriterConfig.getPageValueCount(),
                        value -> {
                            if (value < PARQUET_WRITER_MIN_PAGE_VALUE_COUNT || value > PARQUET_WRITER_MAX_PAGE_VALUE_COUNT) {
                                throw new TrinoException(
                                        INVALID_SESSION_PROPERTY,
                                        format("%s must be between %s and %s: %s", PARQUET_WRITER_PAGE_VALUE_COUNT, PARQUET_WRITER_MIN_PAGE_VALUE_COUNT, PARQUET_WRITER_MAX_PAGE_VALUE_COUNT, value));
                            }
                        },
                        false))
                .add(integerProperty(
                        PARQUET_WRITER_BATCH_SIZE,
                        "Parquet: Maximum number of rows passed to the writer in each batch",
                        parquetWriterConfig.getBatchSize(),
                        false))
                .add(durationProperty(
                        DYNAMIC_FILTERING_WAIT_TIMEOUT,
                        "Duration to wait for completion of dynamic filters during split generation",
                        icebergConfig.getDynamicFilteringWaitTimeout(),
                        false))
                .add(booleanProperty(
                        STATISTICS_ENABLED,
                        "Expose table statistics",
                        icebergConfig.isTableStatisticsEnabled(),
                        false))
                .add(booleanProperty(
                        EXTENDED_STATISTICS_ENABLED,
                        EXTENDED_STATISTICS_DESCRIPTION,
                        icebergConfig.isExtendedStatisticsEnabled(),
                        false))
                .add(booleanProperty(
                        PROJECTION_PUSHDOWN_ENABLED,
                        "Read only required fields from a row type",
                        icebergConfig.isProjectionPushdownEnabled(),
                        false))
                .add(dataSizeProperty(
                        TARGET_MAX_FILE_SIZE,
                        "Target maximum size of written files; the actual size may be larger",
                        icebergConfig.getTargetMaxFileSize(),
                        false))
                .add(dataSizeProperty(
                        IDLE_WRITER_MIN_FILE_SIZE,
                        "Minimum data written by a single partition writer before it can be consider as 'idle' and could be closed by the engine",
                        icebergConfig.getIdleWriterMinFileSize(),
                        false))
                .add(booleanProperty(
                        COLLECT_EXTENDED_STATISTICS_ON_WRITE,
                        COLLECT_EXTENDED_STATISTICS_ON_WRITE_DESCRIPTION,
                        icebergConfig.isCollectExtendedStatisticsOnWrite(),
                        false))
                .add(stringProperty(
                        HIVE_CATALOG_NAME,
                        "Catalog to redirect to when a Hive table is referenced",
                        icebergConfig.getHiveCatalogName().orElse(null),
                        // Session-level redirections configuration does not work well with views, as view body is analyzed in context
                        // of a session with properties stripped off. Thus, this property is more of a test-only, or at most POC usefulness.
                        true))
                .add(doubleProperty(
                        MINIMUM_ASSIGNED_SPLIT_WEIGHT,
                        "Minimum assigned split weight",
                        icebergConfig.getMinimumAssignedSplitWeight(),
                        false))
                .add(durationProperty(
                        EXPIRE_SNAPSHOTS_MIN_RETENTION,
                        "Minimal retention period for expire_snapshot procedure",
                        icebergConfig.getExpireSnapshotsMinRetention(),
                        false))
                .add(durationProperty(
                        REMOVE_ORPHAN_FILES_MIN_RETENTION,
                        "Minimal retention period for remove_orphan_files procedure",
                        icebergConfig.getRemoveOrphanFilesMinRetention(),
                        false))
                .add(booleanProperty(
                        MERGE_MANIFESTS_ON_WRITE,
                        "Compact manifest files when performing write operations",
                        true,
                        false))
                .add(booleanProperty(
                        SORTED_WRITING_ENABLED,
                        "Enable sorted writing to tables with a specified sort order",
                        icebergConfig.isSortedWritingEnabled(),
                        false))
                .add(booleanProperty(
                        QUERY_PARTITION_FILTER_REQUIRED,
                        "Require filter on partition column",
                        icebergConfig.isQueryPartitionFilterRequired(),
                        false))
                .add(new PropertyMetadata<>(
                        QUERY_PARTITION_FILTER_REQUIRED_SCHEMAS,
                        "List of schemas for which filter on partition column is enforced.",
                        new ArrayType(VARCHAR),
                        Set.class,
                        icebergConfig.getQueryPartitionFilterRequiredSchemas(),
                        false,
                        object -> ((Collection<?>) object).stream()
                                .map(String.class::cast)
                                .peek(property -> {
                                    if (isNullOrEmpty(property)) {
                                        throw new TrinoException(INVALID_SESSION_PROPERTY, format("Invalid null or empty value in %s property", QUERY_PARTITION_FILTER_REQUIRED_SCHEMAS));
                                    }
                                })
                                .map(schema -> schema.toLowerCase(ENGLISH))
                                .collect(toImmutableSet()),
                        value -> value))
                .add(booleanProperty(
                        INCREMENTAL_REFRESH_ENABLED,
                        "Enable Incremental refresh for MVs backed by Iceberg tables, when possible.",
                        icebergConfig.isIncrementalRefreshEnabled(),
                        false))
                .add(booleanProperty(
                        BUCKET_EXECUTION_ENABLED,
                        "Enable bucket-aware execution: use physical bucketing information to optimize queries",
                        icebergConfig.isBucketExecutionEnabled(),
                        false))
                .build();
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isOrcBloomFiltersEnabled(ConnectorSession session)
    {
        return session.getProperty(ORC_BLOOM_FILTERS_ENABLED, Boolean.class);
    }

    public static DataSize getOrcMaxMergeDistance(ConnectorSession session)
    {
        return session.getProperty(ORC_MAX_MERGE_DISTANCE, DataSize.class);
    }

    public static DataSize getOrcMaxBufferSize(ConnectorSession session)
    {
        return session.getProperty(ORC_MAX_BUFFER_SIZE, DataSize.class);
    }

    public static DataSize getOrcStreamBufferSize(ConnectorSession session)
    {
        return session.getProperty(ORC_STREAM_BUFFER_SIZE, DataSize.class);
    }

    public static DataSize getOrcTinyStripeThreshold(ConnectorSession session)
    {
        return session.getProperty(ORC_TINY_STRIPE_THRESHOLD, DataSize.class);
    }

    public static DataSize getOrcMaxReadBlockSize(ConnectorSession session)
    {
        return session.getProperty(ORC_MAX_READ_BLOCK_SIZE, DataSize.class);
    }

    public static boolean getOrcLazyReadSmallRanges(ConnectorSession session)
    {
        return session.getProperty(ORC_LAZY_READ_SMALL_RANGES, Boolean.class);
    }

    public static boolean isOrcNestedLazy(ConnectorSession session)
    {
        return session.getProperty(ORC_NESTED_LAZY_ENABLED, Boolean.class);
    }

    public static DataSize getOrcStringStatisticsLimit(ConnectorSession session)
    {
        return session.getProperty(ORC_STRING_STATISTICS_LIMIT, DataSize.class);
    }

    public static boolean isOrcWriterValidate(ConnectorSession session)
    {
        double percentage = session.getProperty(ORC_WRITER_VALIDATE_PERCENTAGE, Double.class);
        if (percentage == 0.0) {
            return false;
        }

        checkArgument(percentage > 0.0 && percentage <= 100.0);

        return ThreadLocalRandom.current().nextDouble(100) < percentage;
    }

    public static OrcWriteValidationMode getOrcWriterValidateMode(ConnectorSession session)
    {
        return session.getProperty(ORC_WRITER_VALIDATE_MODE, OrcWriteValidationMode.class);
    }

    public static DataSize getOrcWriterMinStripeSize(ConnectorSession session)
    {
        return session.getProperty(ORC_WRITER_MIN_STRIPE_SIZE, DataSize.class);
    }

    public static DataSize getOrcWriterMaxStripeSize(ConnectorSession session)
    {
        return session.getProperty(ORC_WRITER_MAX_STRIPE_SIZE, DataSize.class);
    }

    public static int getOrcWriterMaxStripeRows(ConnectorSession session)
    {
        return session.getProperty(ORC_WRITER_MAX_STRIPE_ROWS, Integer.class);
    }

    public static int getOrcWriterMaxRowGroupRows(ConnectorSession session)
    {
        return session.getProperty(ORC_WRITER_MAX_ROW_GROUP_ROWS, Integer.class);
    }

    public static DataSize getOrcWriterMaxDictionaryMemory(ConnectorSession session)
    {
        return session.getProperty(ORC_WRITER_MAX_DICTIONARY_MEMORY, DataSize.class);
    }

    public static Optional<DataSize> getSplitSize(ConnectorSession session)
    {
        return Optional.ofNullable(session.getProperty(SPLIT_SIZE, DataSize.class));
    }

    public static HiveCompressionCodec getCompressionCodec(ConnectorSession session)
    {
        return session.getProperty(COMPRESSION_CODEC, HiveCompressionCodec.class);
    }

    public static boolean isUseFileSizeFromMetadata(ConnectorSession session)
    {
        return session.getProperty(USE_FILE_SIZE_FROM_METADATA, Boolean.class);
    }

    public static DataSize getParquetMaxReadBlockSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_MAX_READ_BLOCK_SIZE, DataSize.class);
    }

    public static int getParquetMaxReadBlockRowCount(ConnectorSession session)
    {
        return session.getProperty(PARQUET_MAX_READ_BLOCK_ROW_COUNT, Integer.class);
    }

    public static DataSize getParquetSmallFileThreshold(ConnectorSession session)
    {
        return session.getProperty(PARQUET_SMALL_FILE_THRESHOLD, DataSize.class);
    }

    public static boolean isParquetIgnoreStatistics(ConnectorSession session)
    {
        return session.getProperty(PARQUET_IGNORE_STATISTICS, Boolean.class);
    }

    public static boolean isParquetVectorizedDecodingEnabled(ConnectorSession session)
    {
        return session.getProperty(PARQUET_VECTORIZED_DECODING_ENABLED, Boolean.class);
    }

    public static DataSize getParquetWriterPageSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_WRITER_PAGE_SIZE, DataSize.class);
    }

    public static int getParquetWriterPageValueCount(ConnectorSession session)
    {
        return session.getProperty(PARQUET_WRITER_PAGE_VALUE_COUNT, Integer.class);
    }

    public static DataSize getParquetWriterBlockSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_WRITER_BLOCK_SIZE, DataSize.class);
    }

    public static int getParquetWriterBatchSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_WRITER_BATCH_SIZE, Integer.class);
    }

    public static boolean useParquetBloomFilter(ConnectorSession session)
    {
        return session.getProperty(PARQUET_USE_BLOOM_FILTER, Boolean.class);
    }

    public static Duration getDynamicFilteringWaitTimeout(ConnectorSession session)
    {
        return session.getProperty(DYNAMIC_FILTERING_WAIT_TIMEOUT, Duration.class);
    }

    public static boolean isStatisticsEnabled(ConnectorSession session)
    {
        return session.getProperty(STATISTICS_ENABLED, Boolean.class);
    }

    public static boolean isExtendedStatisticsEnabled(ConnectorSession session)
    {
        return session.getProperty(EXTENDED_STATISTICS_ENABLED, Boolean.class);
    }

    public static boolean isCollectExtendedStatisticsOnWrite(ConnectorSession session)
    {
        return session.getProperty(COLLECT_EXTENDED_STATISTICS_ON_WRITE, Boolean.class);
    }

    public static boolean isProjectionPushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(PROJECTION_PUSHDOWN_ENABLED, Boolean.class);
    }

    public static long getTargetMaxFileSize(ConnectorSession session)
    {
        return session.getProperty(TARGET_MAX_FILE_SIZE, DataSize.class).toBytes();
    }

    public static long getIdleWriterMinFileSize(ConnectorSession session)
    {
        return session.getProperty(IDLE_WRITER_MIN_FILE_SIZE, DataSize.class).toBytes();
    }

    public static Optional<String> getHiveCatalogName(ConnectorSession session)
    {
        return Optional.ofNullable(session.getProperty(HIVE_CATALOG_NAME, String.class));
    }

    public static Duration getExpireSnapshotMinRetention(ConnectorSession session)
    {
        return session.getProperty(EXPIRE_SNAPSHOTS_MIN_RETENTION, Duration.class);
    }

    public static Duration getRemoveOrphanFilesMinRetention(ConnectorSession session)
    {
        return session.getProperty(REMOVE_ORPHAN_FILES_MIN_RETENTION, Duration.class);
    }

    public static double getMinimumAssignedSplitWeight(ConnectorSession session)
    {
        return session.getProperty(MINIMUM_ASSIGNED_SPLIT_WEIGHT, Double.class);
    }

    public static boolean isMergeManifestsOnWrite(ConnectorSession session)
    {
        return session.getProperty(MERGE_MANIFESTS_ON_WRITE, Boolean.class);
    }

    public static boolean isSortedWritingEnabled(ConnectorSession session)
    {
        return session.getProperty(SORTED_WRITING_ENABLED, Boolean.class);
    }

    public static boolean isQueryPartitionFilterRequired(ConnectorSession session)
    {
        return session.getProperty(QUERY_PARTITION_FILTER_REQUIRED, Boolean.class);
    }

    @SuppressWarnings("unchecked cast")
    public static Set<String> getQueryPartitionFilterRequiredSchemas(ConnectorSession session)
    {
        Set<String> queryPartitionFilterRequiredSchemas = (Set<String>) session.getProperty(QUERY_PARTITION_FILTER_REQUIRED_SCHEMAS, Set.class);
        requireNonNull(queryPartitionFilterRequiredSchemas, "queryPartitionFilterRequiredSchemas is null");
        return queryPartitionFilterRequiredSchemas;
    }

    public static boolean isIncrementalRefreshEnabled(ConnectorSession session)
    {
        return session.getProperty(INCREMENTAL_REFRESH_ENABLED, Boolean.class);
    }

    public static boolean isBucketExecutionEnabled(ConnectorSession session)
    {
        return session.getProperty(BUCKET_EXECUTION_ENABLED, Boolean.class);
    }
}
