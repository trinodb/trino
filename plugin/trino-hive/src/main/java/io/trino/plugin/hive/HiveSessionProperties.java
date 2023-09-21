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
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.orc.OrcWriteValidation.OrcWriteValidationMode;
import io.trino.plugin.base.session.SessionPropertiesProvider;
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
import static io.trino.plugin.hive.parquet.ParquetWriterConfig.PARQUET_WRITER_MAX_BLOCK_SIZE;
import static io.trino.plugin.hive.parquet.ParquetWriterConfig.PARQUET_WRITER_MAX_PAGE_SIZE;
import static io.trino.plugin.hive.parquet.ParquetWriterConfig.PARQUET_WRITER_MIN_PAGE_SIZE;
import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.doubleProperty;
import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class HiveSessionProperties
        implements SessionPropertiesProvider
{
    private static final String BUCKET_EXECUTION_ENABLED = "bucket_execution_enabled";
    private static final String VALIDATE_BUCKETING = "validate_bucketing";
    private static final String TARGET_MAX_FILE_SIZE = "target_max_file_size";
    private static final String PARALLEL_PARTITIONED_BUCKETED_WRITES = "parallel_partitioned_bucketed_writes";
    private static final String FORCE_LOCAL_SCHEDULING = "force_local_scheduling";
    private static final String INSERT_EXISTING_PARTITIONS_BEHAVIOR = "insert_existing_partitions_behavior";
    private static final String AVRO_NATIVE_READER_ENABLED = "avro_native_reader_enabled";
    private static final String AVRO_NATIVE_WRITER_ENABLED = "avro_native_writer_enabled";
    private static final String CSV_NATIVE_READER_ENABLED = "csv_native_reader_enabled";
    private static final String CSV_NATIVE_WRITER_ENABLED = "csv_native_writer_enabled";
    private static final String JSON_NATIVE_READER_ENABLED = "json_native_reader_enabled";
    private static final String JSON_NATIVE_WRITER_ENABLED = "json_native_writer_enabled";
    private static final String OPENX_JSON_NATIVE_READER_ENABLED = "openx_json_native_reader_enabled";
    private static final String OPENX_JSON_NATIVE_WRITER_ENABLED = "openx_json_native_writer_enabled";
    private static final String REGEX_NATIVE_READER_ENABLED = "regex_native_reader_enabled";
    private static final String TEXT_FILE_NATIVE_READER_ENABLED = "text_file_native_reader_enabled";
    private static final String TEXT_FILE_NATIVE_WRITER_ENABLED = "text_file_native_writer_enabled";
    private static final String SEQUENCE_FILE_NATIVE_READER_ENABLED = "sequence_file_native_reader_enabled";
    private static final String SEQUENCE_FILE_NATIVE_WRITER_ENABLED = "sequence_file_native_writer_enabled";
    private static final String ORC_BLOOM_FILTERS_ENABLED = "orc_bloom_filters_enabled";
    private static final String ORC_MAX_MERGE_DISTANCE = "orc_max_merge_distance";
    private static final String ORC_MAX_BUFFER_SIZE = "orc_max_buffer_size";
    private static final String ORC_STREAM_BUFFER_SIZE = "orc_stream_buffer_size";
    private static final String ORC_TINY_STRIPE_THRESHOLD = "orc_tiny_stripe_threshold";
    private static final String ORC_MAX_READ_BLOCK_SIZE = "orc_max_read_block_size";
    private static final String ORC_LAZY_READ_SMALL_RANGES = "orc_lazy_read_small_ranges";
    private static final String ORC_NESTED_LAZY_ENABLED = "orc_nested_lazy_enabled";
    private static final String ORC_STRING_STATISTICS_LIMIT = "orc_string_statistics_limit";
    private static final String ORC_OPTIMIZED_WRITER_VALIDATE = "orc_optimized_writer_validate";
    private static final String ORC_OPTIMIZED_WRITER_VALIDATE_PERCENTAGE = "orc_optimized_writer_validate_percentage";
    private static final String ORC_OPTIMIZED_WRITER_VALIDATE_MODE = "orc_optimized_writer_validate_mode";
    private static final String ORC_OPTIMIZED_WRITER_MIN_STRIPE_SIZE = "orc_optimized_writer_min_stripe_size";
    private static final String ORC_OPTIMIZED_WRITER_MAX_STRIPE_SIZE = "orc_optimized_writer_max_stripe_size";
    private static final String ORC_OPTIMIZED_WRITER_MAX_STRIPE_ROWS = "orc_optimized_writer_max_stripe_rows";
    private static final String ORC_OPTIMIZED_WRITER_MAX_DICTIONARY_MEMORY = "orc_optimized_writer_max_dictionary_memory";
    private static final String ORC_USE_COLUMN_NAME = "orc_use_column_names";
    private static final String HIVE_STORAGE_FORMAT = "hive_storage_format";
    private static final String COMPRESSION_CODEC = "compression_codec";
    private static final String RESPECT_TABLE_FORMAT = "respect_table_format";
    private static final String CREATE_EMPTY_BUCKET_FILES = "create_empty_bucket_files";
    private static final String PARQUET_USE_COLUMN_NAME = "parquet_use_column_names";
    private static final String PARQUET_IGNORE_STATISTICS = "parquet_ignore_statistics";
    private static final String PARQUET_USE_COLUMN_INDEX = "parquet_use_column_index";
    private static final String PARQUET_USE_BLOOM_FILTER = "parquet_use_bloom_filter";
    private static final String PARQUET_MAX_READ_BLOCK_SIZE = "parquet_max_read_block_size";
    private static final String PARQUET_MAX_READ_BLOCK_ROW_COUNT = "parquet_max_read_block_row_count";
    private static final String PARQUET_WRITER_BLOCK_SIZE = "parquet_writer_block_size";
    private static final String PARQUET_WRITER_PAGE_SIZE = "parquet_writer_page_size";
    private static final String PARQUET_WRITER_BATCH_SIZE = "parquet_writer_batch_size";
    private static final String PARQUET_OPTIMIZED_WRITER_VALIDATION_PERCENTAGE = "parquet_optimized_writer_validation_percentage";
    private static final String MAX_SPLIT_SIZE = "max_split_size";
    private static final String MAX_INITIAL_SPLIT_SIZE = "max_initial_split_size";
    private static final String RCFILE_OPTIMIZED_WRITER_VALIDATE = "rcfile_optimized_writer_validate";
    private static final String SORTED_WRITING_ENABLED = "sorted_writing_enabled";
    private static final String PROPAGATE_TABLE_SCAN_SORTING_PROPERTIES = "propagate_table_scan_sorting_properties";
    private static final String STATISTICS_ENABLED = "statistics_enabled";
    private static final String PARTITION_STATISTICS_SAMPLE_SIZE = "partition_statistics_sample_size";
    private static final String IGNORE_CORRUPTED_STATISTICS = "ignore_corrupted_statistics";
    private static final String COLLECT_COLUMN_STATISTICS_ON_WRITE = "collect_column_statistics_on_write";
    private static final String OPTIMIZE_MISMATCHED_BUCKET_COUNT = "optimize_mismatched_bucket_count";
    private static final String S3_SELECT_PUSHDOWN_ENABLED = "s3_select_pushdown_enabled";
    private static final String DELEGATE_TRANSACTIONAL_MANAGED_TABLE_LOCATION_TO_METASTORE = "delegate_transactional_managed_table_location_to_metastore";
    private static final String IGNORE_ABSENT_PARTITIONS = "ignore_absent_partitions";
    private static final String QUERY_PARTITION_FILTER_REQUIRED = "query_partition_filter_required";
    private static final String QUERY_PARTITION_FILTER_REQUIRED_SCHEMAS = "query_partition_filter_required_schemas";
    private static final String PROJECTION_PUSHDOWN_ENABLED = "projection_pushdown_enabled";
    private static final String TIMESTAMP_PRECISION = "timestamp_precision";
    private static final String DYNAMIC_FILTERING_WAIT_TIMEOUT = "dynamic_filtering_wait_timeout";
    private static final String OPTIMIZE_SYMLINK_LISTING = "optimize_symlink_listing";
    private static final String HIVE_VIEWS_LEGACY_TRANSLATION = "hive_views_legacy_translation";
    private static final String ICEBERG_CATALOG_NAME = "iceberg_catalog_name";
    public static final String DELTA_LAKE_CATALOG_NAME = "delta_lake_catalog_name";
    public static final String HUDI_CATALOG_NAME = "hudi_catalog_name";
    public static final String SIZE_BASED_SPLIT_WEIGHTS_ENABLED = "size_based_split_weights_enabled";
    public static final String MINIMUM_ASSIGNED_SPLIT_WEIGHT = "minimum_assigned_split_weight";
    public static final String NON_TRANSACTIONAL_OPTIMIZE_ENABLED = "non_transactional_optimize_enabled";

    private final List<PropertyMetadata<?>> sessionProperties;

    public enum InsertExistingPartitionsBehavior
    {
        ERROR,
        APPEND,
        OVERWRITE,
        /**/;

        public static InsertExistingPartitionsBehavior valueOf(String value, boolean immutablePartitions)
        {
            InsertExistingPartitionsBehavior enumValue = valueOf(value.toUpperCase(ENGLISH));
            checkArgument(isValid(enumValue, immutablePartitions), "Trino is configured to treat Hive partitions as immutable. %s is not allowed to be set to %s", INSERT_EXISTING_PARTITIONS_BEHAVIOR, APPEND);
            return enumValue;
        }

        static boolean isValid(InsertExistingPartitionsBehavior value, boolean immutable)
        {
            return !(immutable && value == APPEND);
        }
    }

    @Inject
    public HiveSessionProperties(
            HiveConfig hiveConfig,
            HiveFormatsConfig hiveFormatsConfig,
            OrcReaderConfig orcReaderConfig,
            OrcWriterConfig orcWriterConfig,
            ParquetReaderConfig parquetReaderConfig,
            ParquetWriterConfig parquetWriterConfig)
    {
        sessionProperties = ImmutableList.of(
                booleanProperty(
                        BUCKET_EXECUTION_ENABLED,
                        "Enable bucket-aware execution: only use a single worker per bucket",
                        hiveConfig.isBucketExecutionEnabled(),
                        false),
                booleanProperty(
                        VALIDATE_BUCKETING,
                        "Verify that data is bucketed correctly when reading",
                        hiveConfig.isValidateBucketing(),
                        false),
                dataSizeProperty(
                        TARGET_MAX_FILE_SIZE,
                        "Target maximum size of written files; the actual size may be larger",
                        hiveConfig.getTargetMaxFileSize(),
                        false),
                booleanProperty(
                        PARALLEL_PARTITIONED_BUCKETED_WRITES,
                        "Improve parallelism of partitioned and bucketed table writes",
                        hiveConfig.isParallelPartitionedBucketedWrites(),
                        false),
                booleanProperty(
                        FORCE_LOCAL_SCHEDULING,
                        "Only schedule splits on workers colocated with data node",
                        hiveConfig.isForceLocalScheduling(),
                        false),
                new PropertyMetadata<>(
                        INSERT_EXISTING_PARTITIONS_BEHAVIOR,
                        "Behavior on insert existing partitions; this session property doesn't control behavior on insert existing unpartitioned table",
                        VARCHAR,
                        InsertExistingPartitionsBehavior.class,
                        hiveConfig.getInsertExistingPartitionsBehavior(),
                        false,
                        value -> InsertExistingPartitionsBehavior.valueOf((String) value, hiveConfig.isImmutablePartitions()),
                        InsertExistingPartitionsBehavior::toString),
                booleanProperty(
                        AVRO_NATIVE_READER_ENABLED,
                        "Use native Avro file reader",
                        hiveFormatsConfig.isAvroFileNativeReaderEnabled(),
                        false),
                booleanProperty(
                        AVRO_NATIVE_WRITER_ENABLED,
                        "Use native Avro file writer",
                        hiveFormatsConfig.isAvroFileNativeWriterEnabled(),
                        false),
                booleanProperty(
                        CSV_NATIVE_READER_ENABLED,
                        "Use native CSV reader",
                        hiveFormatsConfig.isCsvNativeReaderEnabled(),
                        false),
                booleanProperty(
                        CSV_NATIVE_WRITER_ENABLED,
                        "Use native CSV writer",
                        hiveFormatsConfig.isCsvNativeWriterEnabled(),
                        false),
                booleanProperty(
                        JSON_NATIVE_READER_ENABLED,
                        "Use native JSON reader",
                        hiveFormatsConfig.isJsonNativeReaderEnabled(),
                        false),
                booleanProperty(
                        JSON_NATIVE_WRITER_ENABLED,
                        "Use native JSON writer",
                        hiveFormatsConfig.isJsonNativeWriterEnabled(),
                        false),
                booleanProperty(
                        OPENX_JSON_NATIVE_READER_ENABLED,
                        "Use native OpenX JSON reader",
                        hiveFormatsConfig.isOpenXJsonNativeReaderEnabled(),
                        false),
                booleanProperty(
                        OPENX_JSON_NATIVE_WRITER_ENABLED,
                        "Use native OpenX JSON writer",
                        hiveFormatsConfig.isOpenXJsonNativeWriterEnabled(),
                        false),
                booleanProperty(
                        REGEX_NATIVE_READER_ENABLED,
                        "Use native REGEX reader",
                        hiveFormatsConfig.isRegexNativeReaderEnabled(),
                        false),
                booleanProperty(
                        TEXT_FILE_NATIVE_READER_ENABLED,
                        "Use native text file reader",
                        hiveFormatsConfig.isTextFileNativeReaderEnabled(),
                        false),
                booleanProperty(
                        TEXT_FILE_NATIVE_WRITER_ENABLED,
                        "Use native text file writer",
                        hiveFormatsConfig.isTextFileNativeWriterEnabled(),
                        false),
                booleanProperty(
                        SEQUENCE_FILE_NATIVE_READER_ENABLED,
                        "Use native sequence file reader",
                        hiveFormatsConfig.isSequenceFileNativeReaderEnabled(),
                        false),
                booleanProperty(
                        SEQUENCE_FILE_NATIVE_WRITER_ENABLED,
                        "Use native sequence file writer",
                        hiveFormatsConfig.isSequenceFileNativeWriterEnabled(),
                        false),
                booleanProperty(
                        ORC_BLOOM_FILTERS_ENABLED,
                        "ORC: Enable bloom filters for predicate pushdown",
                        orcReaderConfig.isBloomFiltersEnabled(),
                        false),
                dataSizeProperty(
                        ORC_MAX_MERGE_DISTANCE,
                        "ORC: Maximum size of gap between two reads to merge into a single read",
                        orcReaderConfig.getMaxMergeDistance(),
                        false),
                dataSizeProperty(
                        ORC_MAX_BUFFER_SIZE,
                        "ORC: Maximum size of a single read",
                        orcReaderConfig.getMaxBufferSize(),
                        false),
                dataSizeProperty(
                        ORC_STREAM_BUFFER_SIZE,
                        "ORC: Size of buffer for streaming reads",
                        orcReaderConfig.getStreamBufferSize(),
                        false),
                dataSizeProperty(
                        ORC_TINY_STRIPE_THRESHOLD,
                        "ORC: Threshold below which an ORC stripe or file will read in its entirety",
                        orcReaderConfig.getTinyStripeThreshold(),
                        false),
                dataSizeProperty(
                        ORC_MAX_READ_BLOCK_SIZE,
                        "ORC: Soft max size of Trino blocks produced by ORC reader",
                        orcReaderConfig.getMaxBlockSize(),
                        false),
                booleanProperty(
                        ORC_LAZY_READ_SMALL_RANGES,
                        "Experimental: ORC: Read small file segments lazily",
                        orcReaderConfig.isLazyReadSmallRanges(),
                        false),
                booleanProperty(
                        ORC_NESTED_LAZY_ENABLED,
                        "Experimental: ORC: Lazily read nested data",
                        orcReaderConfig.isNestedLazy(),
                        false),
                dataSizeProperty(
                        ORC_STRING_STATISTICS_LIMIT,
                        "ORC: Maximum size of string statistics; drop if exceeding",
                        orcWriterConfig.getStringStatisticsLimit(),
                        false),
                booleanProperty(
                        ORC_OPTIMIZED_WRITER_VALIDATE,
                        "ORC: Force all validation for files",
                        orcWriterConfig.getValidationPercentage() > 0.0,
                        false),
                new PropertyMetadata<>(
                        ORC_OPTIMIZED_WRITER_VALIDATE_PERCENTAGE,
                        "ORC: sample percentage for validation for files",
                        DOUBLE,
                        Double.class,
                        orcWriterConfig.getValidationPercentage(),
                        false,
                        value -> {
                            double doubleValue = (double) value;
                            if (doubleValue < 0.0 || doubleValue > 100.0) {
                                throw new TrinoException(
                                        INVALID_SESSION_PROPERTY,
                                        format("%s must be between 0.0 and 100.0 inclusive: %s", ORC_OPTIMIZED_WRITER_VALIDATE_PERCENTAGE, doubleValue));
                            }
                            return doubleValue;
                        },
                        value -> value),
                enumProperty(
                        ORC_OPTIMIZED_WRITER_VALIDATE_MODE,
                        "ORC: Level of detail in ORC validation",
                        OrcWriteValidationMode.class,
                        orcWriterConfig.getValidationMode(),
                        false),
                dataSizeProperty(
                        ORC_OPTIMIZED_WRITER_MIN_STRIPE_SIZE,
                        "ORC: Min stripe size",
                        orcWriterConfig.getStripeMinSize(),
                        false),
                dataSizeProperty(
                        ORC_OPTIMIZED_WRITER_MAX_STRIPE_SIZE,
                        "ORC: Max stripe size",
                        orcWriterConfig.getStripeMaxSize(),
                        false),
                integerProperty(
                        ORC_OPTIMIZED_WRITER_MAX_STRIPE_ROWS,
                        "ORC: Max stripe row count",
                        orcWriterConfig.getStripeMaxRowCount(),
                        false),
                dataSizeProperty(
                        ORC_OPTIMIZED_WRITER_MAX_DICTIONARY_MEMORY,
                        "ORC: Max dictionary memory",
                        orcWriterConfig.getDictionaryMaxMemory(),
                        false),
                booleanProperty(
                        ORC_USE_COLUMN_NAME,
                        "ORC: Access ORC columns using names from the file",
                        orcReaderConfig.isUseColumnNames(),
                        false),
                enumProperty(
                        HIVE_STORAGE_FORMAT,
                        "Default storage format for new tables or partitions",
                        HiveStorageFormat.class,
                        hiveConfig.getHiveStorageFormat(),
                        false),
                enumProperty(
                        COMPRESSION_CODEC,
                        "Compression codec to use when writing files",
                        HiveCompressionOption.class,
                        hiveConfig.getHiveCompressionCodec(),
                        false),
                booleanProperty(
                        RESPECT_TABLE_FORMAT,
                        "Write new partitions using table format rather than default storage format",
                        hiveConfig.isRespectTableFormat(),
                        false),
                booleanProperty(
                        CREATE_EMPTY_BUCKET_FILES,
                        "Create empty files for buckets that have no data",
                        hiveConfig.isCreateEmptyBucketFiles(),
                        false),
                booleanProperty(
                        PARQUET_USE_COLUMN_NAME,
                        "Parquet: Access Parquet columns using names from the file",
                        hiveConfig.isUseParquetColumnNames(),
                        false),
                booleanProperty(
                        PARQUET_IGNORE_STATISTICS,
                        "Ignore statistics from Parquet to allow querying files with corrupted or incorrect statistics",
                        parquetReaderConfig.isIgnoreStatistics(),
                        false),
                booleanProperty(
                        PARQUET_USE_COLUMN_INDEX,
                        "Use Parquet column index",
                        parquetReaderConfig.isUseColumnIndex(),
                        false),
                booleanProperty(
                        PARQUET_USE_BLOOM_FILTER,
                        "Use Parquet Bloom filters",
                        parquetReaderConfig.isUseBloomFilter(),
                        false),
                dataSizeProperty(
                        PARQUET_MAX_READ_BLOCK_SIZE,
                        "Parquet: Maximum size of a block to read",
                        parquetReaderConfig.getMaxReadBlockSize(),
                        false),
                integerProperty(
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
                        false),
                dataSizeProperty(
                        PARQUET_WRITER_BLOCK_SIZE,
                        "Parquet: Writer block size",
                        parquetWriterConfig.getBlockSize(),
                        value -> validateMaxDataSize(PARQUET_WRITER_BLOCK_SIZE, value, DataSize.valueOf(PARQUET_WRITER_MAX_BLOCK_SIZE)),
                        false),
                dataSizeProperty(
                        PARQUET_WRITER_PAGE_SIZE,
                        "Parquet: Writer page size",
                        parquetWriterConfig.getPageSize(),
                        value -> {
                            validateMinDataSize(PARQUET_WRITER_PAGE_SIZE, value, DataSize.valueOf(PARQUET_WRITER_MIN_PAGE_SIZE));
                            validateMaxDataSize(PARQUET_WRITER_PAGE_SIZE, value, DataSize.valueOf(PARQUET_WRITER_MAX_PAGE_SIZE));
                        },
                        false),
                integerProperty(
                        PARQUET_WRITER_BATCH_SIZE,
                        "Parquet: Maximum number of rows passed to the writer in each batch",
                        parquetWriterConfig.getBatchSize(),
                        false),
                new PropertyMetadata<>(
                        PARQUET_OPTIMIZED_WRITER_VALIDATION_PERCENTAGE,
                        "Parquet: sample percentage for validation of written files",
                        DOUBLE,
                        Double.class,
                        parquetWriterConfig.getValidationPercentage(),
                        false,
                        value -> {
                            double doubleValue = (double) value;
                            if (doubleValue < 0.0 || doubleValue > 100.0) {
                                throw new TrinoException(
                                        INVALID_SESSION_PROPERTY,
                                        format("%s must be between 0.0 and 100.0 inclusive: %s", PARQUET_OPTIMIZED_WRITER_VALIDATION_PERCENTAGE, doubleValue));
                            }
                            return doubleValue;
                        },
                        value -> value),
                dataSizeProperty(
                        MAX_SPLIT_SIZE,
                        "Max split size",
                        hiveConfig.getMaxSplitSize(),
                        true),
                dataSizeProperty(
                        MAX_INITIAL_SPLIT_SIZE,
                        "Max initial split size",
                        hiveConfig.getMaxInitialSplitSize(),
                        true),
                booleanProperty(
                        RCFILE_OPTIMIZED_WRITER_VALIDATE,
                        "RCFile: Validate writer files",
                        hiveConfig.isRcfileWriterValidate(),
                        false),
                booleanProperty(
                        SORTED_WRITING_ENABLED,
                        "Enable writing to bucketed sorted tables",
                        hiveConfig.isSortedWritingEnabled(),
                        false),
                booleanProperty(
                        PROPAGATE_TABLE_SCAN_SORTING_PROPERTIES,
                        "Use sorted table layout to generate more efficient execution plans. May lead to incorrect results if files are not sorted as per table definition.",
                        hiveConfig.isPropagateTableScanSortingProperties(),
                        false),
                booleanProperty(
                        STATISTICS_ENABLED,
                        "Expose table statistics",
                        hiveConfig.isTableStatisticsEnabled(),
                        false),
                integerProperty(
                        PARTITION_STATISTICS_SAMPLE_SIZE,
                        "Maximum sample size of the partitions column statistics",
                        hiveConfig.getPartitionStatisticsSampleSize(),
                        false),
                booleanProperty(
                        IGNORE_CORRUPTED_STATISTICS,
                        "Experimental: Ignore corrupted statistics rather than failing",
                        hiveConfig.isIgnoreCorruptedStatistics(),
                        false),
                booleanProperty(
                        COLLECT_COLUMN_STATISTICS_ON_WRITE,
                        "Enables automatic column level statistics collection on write",
                        hiveConfig.isCollectColumnStatisticsOnWrite(),
                        false),
                booleanProperty(
                        OPTIMIZE_MISMATCHED_BUCKET_COUNT,
                        "Experimental: Enable optimization to avoid shuffle when bucket count is compatible but not the same",
                        hiveConfig.isOptimizeMismatchedBucketCount(),
                        false),
                booleanProperty(
                        S3_SELECT_PUSHDOWN_ENABLED,
                        "S3 Select pushdown enabled",
                        hiveConfig.isS3SelectPushdownEnabled(),
                        false),
                booleanProperty(
                        DELEGATE_TRANSACTIONAL_MANAGED_TABLE_LOCATION_TO_METASTORE,
                        "When transactional managed table is created via Trino the location will not be set in request sent to HMS and location will be determined by metastore; if this property is set to true CREATE TABLE AS queries are not supported.",
                        hiveConfig.isDelegateTransactionalManagedTableLocationToMetastore(),
                        true),
                booleanProperty(
                        IGNORE_ABSENT_PARTITIONS,
                        "Ignore partitions when the file system location does not exist rather than failing the query.",
                        hiveConfig.isIgnoreAbsentPartitions(),
                        false),
                booleanProperty(
                        QUERY_PARTITION_FILTER_REQUIRED,
                        "Require filter on partition column",
                        hiveConfig.isQueryPartitionFilterRequired(),
                        false),
                new PropertyMetadata<>(
                        QUERY_PARTITION_FILTER_REQUIRED_SCHEMAS,
                        "List of schemas for which filter on partition column is enforced.",
                        new ArrayType(VARCHAR),
                        Set.class,
                        hiveConfig.getQueryPartitionFilterRequiredSchemas(),
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
                        value -> value),
                booleanProperty(
                        PROJECTION_PUSHDOWN_ENABLED,
                        "Projection push down enabled for hive",
                        hiveConfig.isProjectionPushdownEnabled(),
                        false),
                enumProperty(
                        TIMESTAMP_PRECISION,
                        "Precision for timestamp columns in Hive tables",
                        HiveTimestampPrecision.class,
                        hiveConfig.getTimestampPrecision(),
                        false),
                durationProperty(
                        DYNAMIC_FILTERING_WAIT_TIMEOUT,
                        "Duration to wait for completion of dynamic filters during split generation",
                        hiveConfig.getDynamicFilteringWaitTimeout(),
                        false),
                booleanProperty(
                        OPTIMIZE_SYMLINK_LISTING,
                        "Optimize listing for SymlinkTextFormat tables with files in a single directory",
                        hiveConfig.isOptimizeSymlinkListing(),
                        false),
                booleanProperty(
                        HIVE_VIEWS_LEGACY_TRANSLATION,
                        "Use legacy Hive view translation mechanism",
                        hiveConfig.isLegacyHiveViewTranslation(),
                        false),
                stringProperty(
                        ICEBERG_CATALOG_NAME,
                        "Catalog to redirect to when an Iceberg table is referenced",
                        hiveConfig.getIcebergCatalogName().orElse(null),
                        // Session-level redirections configuration does not work well with views, as view body is analyzed in context
                        // of a session with properties stripped off. Thus, this property is more of a test-only, or at most POC usefulness.
                        true),
                booleanProperty(
                        SIZE_BASED_SPLIT_WEIGHTS_ENABLED,
                        "Enable estimating split weights based on size in bytes",
                        hiveConfig.isSizeBasedSplitWeightsEnabled(),
                        false),
                doubleProperty(
                        MINIMUM_ASSIGNED_SPLIT_WEIGHT,
                        "Minimum assigned split weight when size based split weighting is enabled",
                        hiveConfig.getMinimumAssignedSplitWeight(),
                        value -> {
                            if (!Double.isFinite(value) || value <= 0 || value > 1) {
                                throw new TrinoException(INVALID_SESSION_PROPERTY, format("%s must be > 0 and <= 1.0: %s", MINIMUM_ASSIGNED_SPLIT_WEIGHT, value));
                            }
                        },
                        false),
                booleanProperty(
                        NON_TRANSACTIONAL_OPTIMIZE_ENABLED,
                        "Enable OPTIMIZE table procedure",
                        false,
                        false),
                stringProperty(
                        DELTA_LAKE_CATALOG_NAME,
                        "Catalog to redirect to when a Delta Lake table is referenced",
                        hiveConfig.getDeltaLakeCatalogName().orElse(null),
                        // Session-level redirections configuration does not work well with views, as view body is analyzed in context
                        // of a session with properties stripped off. Thus, this property is more of a test-only, or at most POC usefulness.
                        true),
                stringProperty(
                        HUDI_CATALOG_NAME,
                        "Catalog to redirect to when a Hudi table is referenced",
                        hiveConfig.getHudiCatalogName().orElse(null),
                        // Session-level redirections configuration does not work well with views, as view body is analyzed in context
                        // of a session with properties stripped off. Thus, this property is more of a test-only, or at most POC usefulness.
                        true));
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isBucketExecutionEnabled(ConnectorSession session)
    {
        return session.getProperty(BUCKET_EXECUTION_ENABLED, Boolean.class);
    }

    public static boolean isValidateBucketing(ConnectorSession session)
    {
        return session.getProperty(VALIDATE_BUCKETING, Boolean.class);
    }

    public static DataSize getTargetMaxFileSize(ConnectorSession session)
    {
        return session.getProperty(TARGET_MAX_FILE_SIZE, DataSize.class);
    }

    public static boolean isParallelPartitionedBucketedWrites(ConnectorSession session)
    {
        return session.getProperty(PARALLEL_PARTITIONED_BUCKETED_WRITES, Boolean.class);
    }

    public static boolean isForceLocalScheduling(ConnectorSession session)
    {
        return session.getProperty(FORCE_LOCAL_SCHEDULING, Boolean.class);
    }

    public static InsertExistingPartitionsBehavior getInsertExistingPartitionsBehavior(ConnectorSession session)
    {
        return session.getProperty(INSERT_EXISTING_PARTITIONS_BEHAVIOR, InsertExistingPartitionsBehavior.class);
    }

    public static boolean isAvroNativeReaderEnabled(ConnectorSession session)
    {
        return session.getProperty(AVRO_NATIVE_READER_ENABLED, Boolean.class);
    }

    public static boolean isAvroNativeWriterEnabled(ConnectorSession session)
    {
        return session.getProperty(AVRO_NATIVE_WRITER_ENABLED, Boolean.class);
    }

    public static boolean isCsvNativeReaderEnabled(ConnectorSession session)
    {
        return session.getProperty(CSV_NATIVE_READER_ENABLED, Boolean.class);
    }

    public static boolean isCsvNativeWriterEnabled(ConnectorSession session)
    {
        return session.getProperty(CSV_NATIVE_WRITER_ENABLED, Boolean.class);
    }

    public static boolean isJsonNativeReaderEnabled(ConnectorSession session)
    {
        return session.getProperty(JSON_NATIVE_READER_ENABLED, Boolean.class);
    }

    public static boolean isJsonNativeWriterEnabled(ConnectorSession session)
    {
        return session.getProperty(JSON_NATIVE_WRITER_ENABLED, Boolean.class);
    }

    public static boolean isOpenXJsonNativeReaderEnabled(ConnectorSession session)
    {
        return session.getProperty(OPENX_JSON_NATIVE_READER_ENABLED, Boolean.class);
    }

    public static boolean isOpenXJsonNativeWriterEnabled(ConnectorSession session)
    {
        return session.getProperty(OPENX_JSON_NATIVE_WRITER_ENABLED, Boolean.class);
    }

    public static boolean isRegexNativeReaderEnabled(ConnectorSession session)
    {
        return session.getProperty(REGEX_NATIVE_READER_ENABLED, Boolean.class);
    }

    public static boolean isTextFileNativeReaderEnabled(ConnectorSession session)
    {
        return session.getProperty(TEXT_FILE_NATIVE_READER_ENABLED, Boolean.class);
    }

    public static boolean isTextFileNativeWriterEnabled(ConnectorSession session)
    {
        return session.getProperty(TEXT_FILE_NATIVE_WRITER_ENABLED, Boolean.class);
    }

    public static boolean isSequenceFileNativeReaderEnabled(ConnectorSession session)
    {
        return session.getProperty(SEQUENCE_FILE_NATIVE_READER_ENABLED, Boolean.class);
    }

    public static boolean isSequenceFileNativeWriterEnabled(ConnectorSession session)
    {
        return session.getProperty(SEQUENCE_FILE_NATIVE_WRITER_ENABLED, Boolean.class);
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

    public static boolean isOrcOptimizedWriterValidate(ConnectorSession session)
    {
        boolean validate = session.getProperty(ORC_OPTIMIZED_WRITER_VALIDATE, Boolean.class);
        double percentage = session.getProperty(ORC_OPTIMIZED_WRITER_VALIDATE_PERCENTAGE, Double.class);

        checkArgument(percentage >= 0.0 && percentage <= 100.0);

        // session property can disabled validation
        if (!validate) {
            return false;
        }

        // session property cannot force validation when sampling is enabled
        // todo change this if session properties support null
        return ThreadLocalRandom.current().nextDouble(100) < percentage;
    }

    public static OrcWriteValidationMode getOrcOptimizedWriterValidateMode(ConnectorSession session)
    {
        return session.getProperty(ORC_OPTIMIZED_WRITER_VALIDATE_MODE, OrcWriteValidationMode.class);
    }

    public static DataSize getOrcOptimizedWriterMinStripeSize(ConnectorSession session)
    {
        return session.getProperty(ORC_OPTIMIZED_WRITER_MIN_STRIPE_SIZE, DataSize.class);
    }

    public static DataSize getOrcOptimizedWriterMaxStripeSize(ConnectorSession session)
    {
        return session.getProperty(ORC_OPTIMIZED_WRITER_MAX_STRIPE_SIZE, DataSize.class);
    }

    public static int getOrcOptimizedWriterMaxStripeRows(ConnectorSession session)
    {
        return session.getProperty(ORC_OPTIMIZED_WRITER_MAX_STRIPE_ROWS, Integer.class);
    }

    public static DataSize getOrcOptimizedWriterMaxDictionaryMemory(ConnectorSession session)
    {
        return session.getProperty(ORC_OPTIMIZED_WRITER_MAX_DICTIONARY_MEMORY, DataSize.class);
    }

    public static boolean isUseOrcColumnNames(ConnectorSession session)
    {
        return session.getProperty(ORC_USE_COLUMN_NAME, Boolean.class);
    }

    public static HiveStorageFormat getHiveStorageFormat(ConnectorSession session)
    {
        return session.getProperty(HIVE_STORAGE_FORMAT, HiveStorageFormat.class);
    }

    public static HiveCompressionOption getCompressionCodec(ConnectorSession session)
    {
        return session.getProperty(COMPRESSION_CODEC, HiveCompressionOption.class);
    }

    public static boolean isRespectTableFormat(ConnectorSession session)
    {
        return session.getProperty(RESPECT_TABLE_FORMAT, Boolean.class);
    }

    public static boolean isCreateEmptyBucketFiles(ConnectorSession session)
    {
        return session.getProperty(CREATE_EMPTY_BUCKET_FILES, Boolean.class);
    }

    public static boolean isUseParquetColumnNames(ConnectorSession session)
    {
        return session.getProperty(PARQUET_USE_COLUMN_NAME, Boolean.class);
    }

    public static boolean isParquetIgnoreStatistics(ConnectorSession session)
    {
        return session.getProperty(PARQUET_IGNORE_STATISTICS, Boolean.class);
    }

    public static boolean isParquetUseColumnIndex(ConnectorSession session)
    {
        return session.getProperty(PARQUET_USE_COLUMN_INDEX, Boolean.class);
    }

    public static boolean useParquetBloomFilter(ConnectorSession session)
    {
        return session.getProperty(PARQUET_USE_BLOOM_FILTER, Boolean.class);
    }

    public static DataSize getParquetMaxReadBlockSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_MAX_READ_BLOCK_SIZE, DataSize.class);
    }

    public static int getParquetMaxReadBlockRowCount(ConnectorSession session)
    {
        return session.getProperty(PARQUET_MAX_READ_BLOCK_ROW_COUNT, Integer.class);
    }

    public static DataSize getParquetWriterBlockSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_WRITER_BLOCK_SIZE, DataSize.class);
    }

    public static DataSize getParquetWriterPageSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_WRITER_PAGE_SIZE, DataSize.class);
    }

    public static int getParquetBatchSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_WRITER_BATCH_SIZE, Integer.class);
    }

    public static boolean isParquetOptimizedWriterValidate(ConnectorSession session)
    {
        double percentage = session.getProperty(PARQUET_OPTIMIZED_WRITER_VALIDATION_PERCENTAGE, Double.class);
        checkArgument(percentage >= 0.0 && percentage <= 100.0);
        return ThreadLocalRandom.current().nextDouble(100) < percentage;
    }

    public static DataSize getMaxSplitSize(ConnectorSession session)
    {
        return session.getProperty(MAX_SPLIT_SIZE, DataSize.class);
    }

    public static DataSize getMaxInitialSplitSize(ConnectorSession session)
    {
        return session.getProperty(MAX_INITIAL_SPLIT_SIZE, DataSize.class);
    }

    public static boolean isRcfileOptimizedWriterValidate(ConnectorSession session)
    {
        return session.getProperty(RCFILE_OPTIMIZED_WRITER_VALIDATE, Boolean.class);
    }

    public static boolean isSortedWritingEnabled(ConnectorSession session)
    {
        return session.getProperty(SORTED_WRITING_ENABLED, Boolean.class);
    }

    public static boolean isPropagateTableScanSortingProperties(ConnectorSession session)
    {
        return session.getProperty(PROPAGATE_TABLE_SCAN_SORTING_PROPERTIES, Boolean.class);
    }

    public static boolean isS3SelectPushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(S3_SELECT_PUSHDOWN_ENABLED, Boolean.class);
    }

    public static boolean isStatisticsEnabled(ConnectorSession session)
    {
        return session.getProperty(STATISTICS_ENABLED, Boolean.class);
    }

    public static int getPartitionStatisticsSampleSize(ConnectorSession session)
    {
        int size = session.getProperty(PARTITION_STATISTICS_SAMPLE_SIZE, Integer.class);
        if (size < 1) {
            throw new TrinoException(INVALID_SESSION_PROPERTY, format("%s must be greater than 0: %s", PARTITION_STATISTICS_SAMPLE_SIZE, size));
        }
        return size;
    }

    public static boolean isIgnoreCorruptedStatistics(ConnectorSession session)
    {
        return session.getProperty(IGNORE_CORRUPTED_STATISTICS, Boolean.class);
    }

    public static boolean isCollectColumnStatisticsOnWrite(ConnectorSession session)
    {
        return session.getProperty(COLLECT_COLUMN_STATISTICS_ON_WRITE, Boolean.class);
    }

    public static boolean isOptimizedMismatchedBucketCount(ConnectorSession session)
    {
        return session.getProperty(OPTIMIZE_MISMATCHED_BUCKET_COUNT, Boolean.class);
    }

    public static boolean isDelegateTransactionalManagedTableLocationToMetastore(ConnectorSession session)
    {
        return session.getProperty(DELEGATE_TRANSACTIONAL_MANAGED_TABLE_LOCATION_TO_METASTORE, Boolean.class);
    }

    public static boolean isIgnoreAbsentPartitions(ConnectorSession session)
    {
        return session.getProperty(IGNORE_ABSENT_PARTITIONS, Boolean.class);
    }

    public static boolean isQueryPartitionFilterRequired(ConnectorSession session)
    {
        return session.getProperty(QUERY_PARTITION_FILTER_REQUIRED, Boolean.class);
    }

    @SuppressWarnings("unchecked cast")
    public static Set<String> getQueryPartitionFilterRequiredSchemas(ConnectorSession session)
    {
        Set<String> schemas = (Set<String>) session.getProperty(QUERY_PARTITION_FILTER_REQUIRED_SCHEMAS, Set.class);
        requireNonNull(schemas, "queryPartitionFilterRequiredSchemas is null");
        return schemas;
    }

    public static boolean isProjectionPushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(PROJECTION_PUSHDOWN_ENABLED, Boolean.class);
    }

    public static HiveTimestampPrecision getTimestampPrecision(ConnectorSession session)
    {
        return session.getProperty(TIMESTAMP_PRECISION, HiveTimestampPrecision.class);
    }

    public static Duration getDynamicFilteringWaitTimeout(ConnectorSession session)
    {
        return session.getProperty(DYNAMIC_FILTERING_WAIT_TIMEOUT, Duration.class);
    }

    public static boolean isOptimizeSymlinkListing(ConnectorSession session)
    {
        return session.getProperty(OPTIMIZE_SYMLINK_LISTING, Boolean.class);
    }

    public static boolean isHiveViewsLegacyTranslation(ConnectorSession session)
    {
        return session.getProperty(HIVE_VIEWS_LEGACY_TRANSLATION, Boolean.class);
    }

    public static Optional<String> getIcebergCatalogName(ConnectorSession session)
    {
        return Optional.ofNullable(session.getProperty(ICEBERG_CATALOG_NAME, String.class));
    }

    public static boolean isSizeBasedSplitWeightsEnabled(ConnectorSession session)
    {
        return session.getProperty(SIZE_BASED_SPLIT_WEIGHTS_ENABLED, Boolean.class);
    }

    public static double getMinimumAssignedSplitWeight(ConnectorSession session)
    {
        return session.getProperty(MINIMUM_ASSIGNED_SPLIT_WEIGHT, Double.class);
    }

    public static boolean isNonTransactionalOptimizeEnabled(ConnectorSession session)
    {
        return session.getProperty(NON_TRANSACTIONAL_OPTIMIZE_ENABLED, Boolean.class);
    }

    public static Optional<String> getDeltaLakeCatalogName(ConnectorSession session)
    {
        return Optional.ofNullable(session.getProperty(DELTA_LAKE_CATALOG_NAME, String.class));
    }

    public static Optional<String> getHudiCatalogName(ConnectorSession session)
    {
        return Optional.ofNullable(session.getProperty(HUDI_CATALOG_NAME, String.class));
    }
}
