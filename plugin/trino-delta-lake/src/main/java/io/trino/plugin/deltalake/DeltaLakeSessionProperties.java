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
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.hive.HiveCompressionCodec;
import io.trino.plugin.hive.HiveTimestampPrecision;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.base.session.PropertyMetadataUtil.dataSizeProperty;
import static io.trino.plugin.base.session.PropertyMetadataUtil.durationProperty;
import static io.trino.plugin.hive.HiveTimestampPrecision.MILLISECONDS;
import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static java.lang.String.format;

public final class DeltaLakeSessionProperties
        implements SessionPropertiesProvider
{
    private static final String MAX_SPLIT_SIZE = "max_split_size";
    private static final String MAX_INITIAL_SPLIT_SIZE = "max_initial_split_size";
    public static final String VACUUM_MIN_RETENTION = "vacuum_min_retention";
    private static final String HIVE_CATALOG_NAME = "hive_catalog_name";
    private static final String PARQUET_MAX_READ_BLOCK_SIZE = "parquet_max_read_block_size";
    private static final String PARQUET_MAX_READ_BLOCK_ROW_COUNT = "parquet.max_read_block_row_count";
    private static final String PARQUET_USE_COLUMN_INDEX = "parquet_use_column_index";
    private static final String PARQUET_OPTIMIZED_READER_ENABLED = "parquet_optimized_reader_enabled";
    private static final String PARQUET_OPTIMIZED_NESTED_READER_ENABLED = "parquet_optimized_nested_reader_enabled";
    private static final String PARQUET_WRITER_BLOCK_SIZE = "parquet_writer_block_size";
    private static final String PARQUET_WRITER_PAGE_SIZE = "parquet_writer_page_size";
    private static final String TARGET_MAX_FILE_SIZE = "target_max_file_size";
    private static final String COMPRESSION_CODEC = "compression_codec";
    // This property is not supported by Delta Lake and exists solely for technical reasons.
    @Deprecated
    private static final String TIMESTAMP_PRECISION = "timestamp_precision";
    private static final String DYNAMIC_FILTERING_WAIT_TIMEOUT = "dynamic_filtering_wait_timeout";
    private static final String TABLE_STATISTICS_ENABLED = "statistics_enabled";
    public static final String EXTENDED_STATISTICS_ENABLED = "extended_statistics_enabled";
    public static final String EXTENDED_STATISTICS_COLLECT_ON_WRITE = "extended_statistics_collect_on_write";
    public static final String LEGACY_CREATE_TABLE_WITH_EXISTING_LOCATION_ENABLED = "legacy_create_table_with_existing_location_enabled";

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
                dataSizeProperty(
                        MAX_INITIAL_SPLIT_SIZE,
                        "Max initial split size",
                        deltaLakeConfig.getMaxInitialSplitSize(),
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
                booleanProperty(
                        PARQUET_USE_COLUMN_INDEX,
                        "Use Parquet column index",
                        parquetReaderConfig.isUseColumnIndex(),
                        false),
                booleanProperty(
                        PARQUET_OPTIMIZED_READER_ENABLED,
                        "Use optimized Parquet reader",
                        parquetReaderConfig.isOptimizedReaderEnabled(),
                        false),
                booleanProperty(
                        PARQUET_OPTIMIZED_NESTED_READER_ENABLED,
                        "Use optimized Parquet reader for nested columns",
                        parquetReaderConfig.isOptimizedNestedReaderEnabled(),
                        false),
                dataSizeProperty(
                        PARQUET_WRITER_BLOCK_SIZE,
                        "Parquet: Writer block size",
                        parquetWriterConfig.getBlockSize(),
                        false),
                dataSizeProperty(
                        PARQUET_WRITER_PAGE_SIZE,
                        "Parquet: Writer page size",
                        parquetWriterConfig.getPageSize(),
                        false),
                dataSizeProperty(
                        TARGET_MAX_FILE_SIZE,
                        "Target maximum size of written files; the actual size may be larger",
                        deltaLakeConfig.getTargetMaxFileSize(),
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
                        LEGACY_CREATE_TABLE_WITH_EXISTING_LOCATION_ENABLED,
                        "Enable using the CREATE TABLE statement to register an existing table",
                        deltaLakeConfig.isLegacyCreateTableWithExistingLocationEnabled(),
                        false),
                booleanProperty(
                        EXTENDED_STATISTICS_COLLECT_ON_WRITE,
                        "Enables automatic column level extended statistics collection on write",
                        deltaLakeConfig.isCollectExtendedStatisticsOnWrite(),
                        false),
                enumProperty(
                        COMPRESSION_CODEC,
                        "Compression codec to use when writing new data files",
                        HiveCompressionCodec.class,
                        deltaLakeConfig.getCompressionCodec(),
                        value -> {
                            if (value == HiveCompressionCodec.LZ4) {
                                throw new TrinoException(INVALID_SESSION_PROPERTY, "Unsupported codec: LZ4");
                            }
                        },
                        false));
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

    public static DataSize getMaxInitialSplitSize(ConnectorSession session)
    {
        return session.getProperty(MAX_INITIAL_SPLIT_SIZE, DataSize.class);
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

    public static boolean isParquetUseColumnIndex(ConnectorSession session)
    {
        return session.getProperty(PARQUET_USE_COLUMN_INDEX, Boolean.class);
    }

    public static boolean isParquetOptimizedReaderEnabled(ConnectorSession session)
    {
        return session.getProperty(PARQUET_OPTIMIZED_READER_ENABLED, Boolean.class);
    }

    public static boolean isParquetOptimizedNestedReaderEnabled(ConnectorSession session)
    {
        return session.getProperty(PARQUET_OPTIMIZED_NESTED_READER_ENABLED, Boolean.class);
    }

    public static DataSize getParquetWriterBlockSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_WRITER_BLOCK_SIZE, DataSize.class);
    }

    public static DataSize getParquetWriterPageSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_WRITER_PAGE_SIZE, DataSize.class);
    }

    public static long getTargetMaxFileSize(ConnectorSession session)
    {
        return session.getProperty(TARGET_MAX_FILE_SIZE, DataSize.class).toBytes();
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

    @Deprecated
    public static boolean isLegacyCreateTableWithExistingLocationEnabled(ConnectorSession session)
    {
        return session.getProperty(LEGACY_CREATE_TABLE_WITH_EXISTING_LOCATION_ENABLED, Boolean.class);
    }

    public static boolean isCollectExtendedStatisticsColumnStatisticsOnWrite(ConnectorSession session)
    {
        return session.getProperty(EXTENDED_STATISTICS_COLLECT_ON_WRITE, Boolean.class);
    }

    public static HiveCompressionCodec getCompressionCodec(ConnectorSession session)
    {
        return session.getProperty(COMPRESSION_CODEC, HiveCompressionCodec.class);
    }
}
