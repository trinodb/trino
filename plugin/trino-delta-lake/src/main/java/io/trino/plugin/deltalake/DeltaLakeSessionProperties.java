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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.hive.HiveCompressionOption;
import io.trino.plugin.hive.HiveTimestampPrecision;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.base.session.PropertyMetadataUtil.dataSizeProperty;
import static io.trino.plugin.base.session.PropertyMetadataUtil.durationProperty;
import static io.trino.plugin.base.session.PropertyMetadataUtil.validateMaxDataSize;
import static io.trino.plugin.base.session.PropertyMetadataUtil.validateMinDataSize;
import static io.trino.plugin.hive.HiveTimestampPrecision.MILLISECONDS;
import static io.trino.plugin.hive.parquet.ParquetReaderConfig.PARQUET_READER_MAX_SMALL_FILE_THRESHOLD;
import static io.trino.plugin.hive.parquet.ParquetWriterConfig.PARQUET_WRITER_MAX_BLOCK_SIZE;
import static io.trino.plugin.hive.parquet.ParquetWriterConfig.PARQUET_WRITER_MAX_PAGE_SIZE;
import static io.trino.plugin.hive.parquet.ParquetWriterConfig.PARQUET_WRITER_MAX_PAGE_VALUE_COUNT;
import static io.trino.plugin.hive.parquet.ParquetWriterConfig.PARQUET_WRITER_MIN_PAGE_SIZE;
import static io.trino.plugin.hive.parquet.ParquetWriterConfig.PARQUET_WRITER_MIN_PAGE_VALUE_COUNT;
import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static java.lang.String.format;

public final class DeltaLakeSessionProperties
        implements SessionPropertiesProvider
{
    public static final String MAX_SPLIT_SIZE = "max_split_size";
    public static final String VACUUM_MIN_RETENTION = "vacuum_min_retention";
    private static final String HIVE_CATALOG_NAME = "hive_catalog_name";
    private static final String PARQUET_MAX_READ_BLOCK_SIZE = "parquet_max_read_block_size";
    private static final String PARQUET_MAX_READ_BLOCK_ROW_COUNT = "parquet_max_read_block_row_count";
    private static final String PARQUET_SMALL_FILE_THRESHOLD = "parquet_small_file_threshold";
    private static final String PARQUET_USE_COLUMN_INDEX = "parquet_use_column_index";
    private static final String PARQUET_IGNORE_STATISTICS = "parquet_ignore_statistics";
    private static final String PARQUET_VECTORIZED_DECODING_ENABLED = "parquet_vectorized_decoding_enabled";
    private static final String PARQUET_WRITER_BLOCK_SIZE = "parquet_writer_block_size";
    private static final String PARQUET_WRITER_PAGE_SIZE = "parquet_writer_page_size";
    private static final String PARQUET_WRITER_PAGE_VALUE_COUNT = "parquet_writer_page_value_count";
    private static final String TARGET_MAX_FILE_SIZE = "target_max_file_size";
    private static final String IDLE_WRITER_MIN_FILE_SIZE = "idle_writer_min_file_size";
    private static final String COMPRESSION_CODEC = "compression_codec";
    // This property is not supported by Delta Lake and exists solely for technical reasons.
    @Deprecated
    private static final String TIMESTAMP_PRECISION = "timestamp_precision";
    private static final String DYNAMIC_FILTERING_WAIT_TIMEOUT = "dynamic_filtering_wait_timeout";
    private static final String TABLE_STATISTICS_ENABLED = "statistics_enabled";
    public static final String EXTENDED_STATISTICS_ENABLED = "extended_statistics_enabled";
    public static final String EXTENDED_STATISTICS_COLLECT_ON_WRITE = "extended_statistics_collect_on_write";
    private static final String PROJECTION_PUSHDOWN_ENABLED = "projection_pushdown_enabled";
    private static final String QUERY_PARTITION_FILTER_REQUIRED = "query_partition_filter_required";
    private static final String CHECKPOINT_FILTERING_ENABLED = "checkpoint_filtering_enabled";
    private static final String STORE_TABLE_METADATA = "store_table_metadata";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public DeltaLakeSessionProperties(
            DeltaLakeConfig deltaLakeConfig,
            ParquetReaderConfig parquetReaderConfig,
            ParquetWriterConfig parquetWriterConfig)
    {
        sessionProperties = ImmutableList.of(
                dataSizeProperty(
                        MAX_SPLIT_SIZE,
                        "Max split size",
                        deltaLakeConfig.getMaxSplitSize(),
                        true),
                durationProperty(
                        VACUUM_MIN_RETENTION,
                        "Minimal retention period for vacuum procedure",
                        deltaLakeConfig.getVacuumMinRetention(),
                        false),
                stringProperty(
                        HIVE_CATALOG_NAME,
                        "Catalog to redirect to when a Hive table is referenced",
                        deltaLakeConfig.getHiveCatalogName().orElse(null),
                        // Session-level redirections configuration does not work well with views, as view body is analyzed in context
                        // of a session with properties stripped off. Thus, this property is more of a test-only, or at most POC usefulness.
                        true),
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
                        PARQUET_SMALL_FILE_THRESHOLD,
                        "Parquet: Size below which a parquet file will be read entirely",
                        parquetReaderConfig.getSmallFileThreshold(),
                        value -> validateMaxDataSize(PARQUET_SMALL_FILE_THRESHOLD, value, DataSize.valueOf(PARQUET_READER_MAX_SMALL_FILE_THRESHOLD)),
                        false),
                booleanProperty(
                        PARQUET_USE_COLUMN_INDEX,
                        "Use Parquet column index",
                        parquetReaderConfig.isUseColumnIndex(),
                        false),
                booleanProperty(
                        PARQUET_IGNORE_STATISTICS,
                        "Ignore statistics from Parquet to allow querying files with corrupted or incorrect statistics",
                        parquetReaderConfig.isIgnoreStatistics(),
                        false),
                booleanProperty(
                        PARQUET_VECTORIZED_DECODING_ENABLED,
                        "Enable using Java Vector API for faster decoding of parquet files",
                        parquetReaderConfig.isVectorizedDecodingEnabled(),
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
                        false),
                dataSizeProperty(
                        TARGET_MAX_FILE_SIZE,
                        "Target maximum size of written files; the actual size may be larger",
                        deltaLakeConfig.getTargetMaxFileSize(),
                        false),
                dataSizeProperty(
                        IDLE_WRITER_MIN_FILE_SIZE,
                        "Minimum data written by a single partition writer before it can be consider as 'idle' and could be closed by the engine",
                        deltaLakeConfig.getIdleWriterMinFileSize(),
                        false),
                enumProperty(
                        TIMESTAMP_PRECISION,
                        "Internal Delta Lake connector property",
                        HiveTimestampPrecision.class,
                        MILLISECONDS,
                        value -> { throw new IllegalStateException("The property cannot be set"); },
                        true),
                durationProperty(
                        DYNAMIC_FILTERING_WAIT_TIMEOUT,
                        "Duration to wait for completion of dynamic filters during split generation",
                        deltaLakeConfig.getDynamicFilteringWaitTimeout(),
                        false),
                booleanProperty(
                        TABLE_STATISTICS_ENABLED,
                        "Expose table statistics",
                        deltaLakeConfig.isTableStatisticsEnabled(),
                        false),
                booleanProperty(
                        EXTENDED_STATISTICS_ENABLED,
                        "Enable collection (ANALYZE) and use of extended statistics.",
                        deltaLakeConfig.isExtendedStatisticsEnabled(),
                        false),
                booleanProperty(
                        EXTENDED_STATISTICS_COLLECT_ON_WRITE,
                        "Enables automatic column level extended statistics collection on write",
                        deltaLakeConfig.isCollectExtendedStatisticsOnWrite(),
                        false),
                enumProperty(
                        COMPRESSION_CODEC,
                        "Compression codec to use when writing new data files",
                        HiveCompressionOption.class,
                        deltaLakeConfig.getCompressionCodec(),
                        value -> {
                            if (value == HiveCompressionOption.LZ4) {
                                // TODO (https://github.com/trinodb/trino/issues/9142) Support LZ4 compression with native Parquet writer
                                throw new TrinoException(INVALID_SESSION_PROPERTY, "Unsupported codec: LZ4");
                            }
                        },
                        false),
                booleanProperty(
                        PROJECTION_PUSHDOWN_ENABLED,
                        "Read only required fields from a row type",
                        deltaLakeConfig.isProjectionPushdownEnabled(),
                        false),
                booleanProperty(
                        QUERY_PARTITION_FILTER_REQUIRED,
                        "Require filter on partition column",
                        deltaLakeConfig.isQueryPartitionFilterRequired(),
                        false),
                booleanProperty(
                        CHECKPOINT_FILTERING_ENABLED,
                        "Use filter in checkpoint reader",
                        deltaLakeConfig.isCheckpointFilteringEnabled(),
                        false),
                booleanProperty(
                        STORE_TABLE_METADATA,
                        "Store table metadata in metastore",
                        deltaLakeConfig.isStoreTableMetadataEnabled(),
                        true));
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static DataSize getMaxSplitSize(ConnectorSession session)
    {
        return session.getProperty(MAX_SPLIT_SIZE, DataSize.class);
    }

    public static Duration getVacuumMinRetention(ConnectorSession session)
    {
        return session.getProperty(VACUUM_MIN_RETENTION, Duration.class);
    }

    public static Optional<String> getHiveCatalogName(ConnectorSession session)
    {
        return Optional.ofNullable(session.getProperty(HIVE_CATALOG_NAME, String.class));
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

    public static boolean isParquetUseColumnIndex(ConnectorSession session)
    {
        return session.getProperty(PARQUET_USE_COLUMN_INDEX, Boolean.class);
    }

    public static boolean isParquetIgnoreStatistics(ConnectorSession session)
    {
        return session.getProperty(PARQUET_IGNORE_STATISTICS, Boolean.class);
    }

    public static boolean isParquetVectorizedDecodingEnabled(ConnectorSession session)
    {
        return session.getProperty(PARQUET_VECTORIZED_DECODING_ENABLED, Boolean.class);
    }

    public static DataSize getParquetWriterBlockSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_WRITER_BLOCK_SIZE, DataSize.class);
    }

    public static DataSize getParquetWriterPageSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_WRITER_PAGE_SIZE, DataSize.class);
    }

    public static int getParquetWriterPageValueCount(ConnectorSession session)
    {
        return session.getProperty(PARQUET_WRITER_PAGE_VALUE_COUNT, Integer.class);
    }

    public static long getTargetMaxFileSize(ConnectorSession session)
    {
        return session.getProperty(TARGET_MAX_FILE_SIZE, DataSize.class).toBytes();
    }

    public static long getIdleWriterMinFileSize(ConnectorSession session)
    {
        return session.getProperty(IDLE_WRITER_MIN_FILE_SIZE, DataSize.class).toBytes();
    }

    public static Duration getDynamicFilteringWaitTimeout(ConnectorSession session)
    {
        return session.getProperty(DYNAMIC_FILTERING_WAIT_TIMEOUT, Duration.class);
    }

    public static boolean isTableStatisticsEnabled(ConnectorSession session)
    {
        return session.getProperty(TABLE_STATISTICS_ENABLED, Boolean.class);
    }

    public static boolean isExtendedStatisticsEnabled(ConnectorSession session)
    {
        return session.getProperty(EXTENDED_STATISTICS_ENABLED, Boolean.class);
    }

    public static boolean isCollectExtendedStatisticsColumnStatisticsOnWrite(ConnectorSession session)
    {
        return session.getProperty(EXTENDED_STATISTICS_COLLECT_ON_WRITE, Boolean.class);
    }

    public static HiveCompressionOption getCompressionCodec(ConnectorSession session)
    {
        return session.getProperty(COMPRESSION_CODEC, HiveCompressionOption.class);
    }

    public static boolean isProjectionPushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(PROJECTION_PUSHDOWN_ENABLED, Boolean.class);
    }

    public static boolean isQueryPartitionFilterRequired(ConnectorSession session)
    {
        return session.getProperty(QUERY_PARTITION_FILTER_REQUIRED, Boolean.class);
    }

    public static boolean isCheckpointFilteringEnabled(ConnectorSession session)
    {
        return session.getProperty(CHECKPOINT_FILTERING_ENABLED, Boolean.class);
    }

    public static boolean isStoreTableMetadataInMetastoreEnabled(ConnectorSession session)
    {
        return session.getProperty(STORE_TABLE_METADATA, Boolean.class);
    }
}
