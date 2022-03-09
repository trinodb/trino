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
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.base.session.PropertyMetadataUtil.dataSizeProperty;
import static io.trino.plugin.base.session.PropertyMetadataUtil.durationProperty;
import static io.trino.plugin.hive.HiveTimestampPrecision.MILLISECONDS;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;

public final class DeltaLakeSessionProperties
        implements SessionPropertiesProvider
{
    private static final String MAX_SPLIT_SIZE = "max_split_size";
    private static final String MAX_INITIAL_SPLIT_SIZE = "max_initial_split_size";
    public static final String VACUUM_MIN_RETENTION = "vacuum_min_retention";
    private static final String HIVE_CATALOG_NAME = "hive_catalog_name";
    private static final String PARQUET_MAX_READ_BLOCK_SIZE = "parquet_max_read_block_size";
    private static final String PARQUET_USE_COLUMN_INDEX = "parquet_use_column_index";
    private static final String PARQUET_WRITER_BLOCK_SIZE = "parquet_writer_block_size";
    private static final String PARQUET_WRITER_PAGE_SIZE = "parquet_writer_page_size";
    private static final String PARQUET_OPTIMIZED_WRITER_ENABLED = "experimental_parquet_optimized_writer_enabled"; // = HiveSessionProperties#PARQUET_OPTIMIZED_WRITER_ENABLED
    private static final String COMPRESSION_CODEC = "compression_codec";
    // This property is not supported by Delta Lake and exists solely for technical reasons.
    @Deprecated
    private static final String TIMESTAMP_PRECISION = "timestamp_precision";
    private static final String TABLE_STATISTICS_ENABLED = "statistics_enabled";
    private static final String EXTENDED_STATISTICS_ENABLED = "extended_statistics_enabled";

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
                booleanProperty(
                        PARQUET_USE_COLUMN_INDEX,
                        "Use Parquet column index",
                        parquetReaderConfig.isUseColumnIndex(),
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
                booleanProperty(
                        PARQUET_OPTIMIZED_WRITER_ENABLED,
                        "Experimental: Enable optimized writer",
                        parquetWriterConfig.isParquetOptimizedWriterEnabled(),
                        false),
                enumProperty(
                        TIMESTAMP_PRECISION,
                        "Internal Delta Lake connector property",
                        HiveTimestampPrecision.class,
                        MILLISECONDS,
                        value -> { throw new IllegalStateException("The property cannot be set"); },
                        true),
                booleanProperty(
                        TABLE_STATISTICS_ENABLED,
                        "Expose table statistics",
                        deltaLakeConfig.isTableStatisticsEnabled(),
                        false),
                booleanProperty(
                        EXTENDED_STATISTICS_ENABLED,
                        "Use extended statistics collected by ANALYZE",
                        deltaLakeConfig.isExtendedStatisticsEnabled(),
                        false),
                enumProperty(
                        COMPRESSION_CODEC,
                        "Compression codec to use when writing new data files",
                        HiveCompressionCodec.class,
                        deltaLakeConfig.getCompressionCodec(),
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

    public static boolean isParquetUseColumnIndex(ConnectorSession session)
    {
        return session.getProperty(PARQUET_USE_COLUMN_INDEX, Boolean.class);
    }

    public static boolean isParquetOptimizedWriterEnabled(ConnectorSession session)
    {
        return session.getProperty(PARQUET_OPTIMIZED_WRITER_ENABLED, Boolean.class);
    }

    public static DataSize getParquetWriterBlockSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_WRITER_BLOCK_SIZE, DataSize.class);
    }

    public static DataSize getParquetWriterPageSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_WRITER_PAGE_SIZE, DataSize.class);
    }

    public static boolean isTableStatisticsEnabled(ConnectorSession session)
    {
        return session.getProperty(TABLE_STATISTICS_ENABLED, Boolean.class);
    }

    public static boolean isExtendedStatisticsEnabled(ConnectorSession session)
    {
        return session.getProperty(EXTENDED_STATISTICS_ENABLED, Boolean.class);
    }

    public static HiveCompressionCodec getCompressionCodec(ConnectorSession session)
    {
        return session.getProperty(COMPRESSION_CODEC, HiveCompressionCodec.class);
    }
}
