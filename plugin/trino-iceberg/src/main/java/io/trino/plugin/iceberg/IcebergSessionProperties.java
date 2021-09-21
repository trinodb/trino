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
import io.airlift.units.DataSize;
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

import javax.inject.Inject;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.base.session.PropertyMetadataUtil.dataSizeProperty;
import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.doubleProperty;
import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static java.lang.String.format;

public final class IcebergSessionProperties
        implements SessionPropertiesProvider
{
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
    private static final String ORC_WRITER_MAX_DICTIONARY_MEMORY = "orc_writer_max_dictionary_memory";
    private static final String PARQUET_MAX_READ_BLOCK_SIZE = "parquet_max_read_block_size";
    private static final String PARQUET_WRITER_BLOCK_SIZE = "parquet_writer_block_size";
    private static final String PARQUET_WRITER_PAGE_SIZE = "parquet_writer_page_size";
    private static final String PARQUET_WRITER_BATCH_SIZE = "parquet_writer_batch_size";
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
                .add(dataSizeProperty(
                        PARQUET_WRITER_BLOCK_SIZE,
                        "Parquet: Writer block size",
                        parquetWriterConfig.getBlockSize(),
                        false))
                .add(dataSizeProperty(
                        PARQUET_WRITER_PAGE_SIZE,
                        "Parquet: Writer page size",
                        parquetWriterConfig.getPageSize(),
                        false))
                .add(integerProperty(
                        PARQUET_WRITER_BATCH_SIZE,
                        "Parquet: Maximum number of rows passed to the writer in each batch",
                        parquetWriterConfig.getBatchSize(),
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

    public static DataSize getOrcWriterMaxDictionaryMemory(ConnectorSession session)
    {
        return session.getProperty(ORC_WRITER_MAX_DICTIONARY_MEMORY, DataSize.class);
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

    public static DataSize getParquetWriterPageSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_WRITER_PAGE_SIZE, DataSize.class);
    }

    public static DataSize getParquetWriterBlockSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_WRITER_BLOCK_SIZE, DataSize.class);
    }

    public static int getParquetWriterBatchSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_WRITER_BATCH_SIZE, Integer.class);
    }
}
