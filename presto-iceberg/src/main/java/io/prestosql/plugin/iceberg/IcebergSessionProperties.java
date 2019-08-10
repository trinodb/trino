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
package io.prestosql.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.OrcFileWriterConfig;
import io.prestosql.plugin.hive.ParquetFileWriterConfig;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.session.PropertyMetadata;

import javax.inject.Inject;

import java.util.List;

import static io.prestosql.spi.session.PropertyMetadata.booleanProperty;
import static io.prestosql.spi.session.PropertyMetadata.dataSizeProperty;
import static io.prestosql.spi.session.PropertyMetadata.integerProperty;
import static io.prestosql.spi.session.PropertyMetadata.stringProperty;

public final class IcebergSessionProperties
{
    private static final String PARQUET_FAIL_WITH_CORRUPTED_STATISTICS = "parquet_fail_with_corrupted_statistics";
    private static final String PARQUET_MAX_READ_BLOCK_SIZE = "parquet_max_read_block_size";
    private static final String PARQUET_WRITER_BLOCK_SIZE = "parquet_writer_block_size";
    private static final String PARQUET_WRITER_PAGE_SIZE = "parquet_writer_page_size";
    private static final String ORC_OPTIMIZED_WRITER_MIN_STRIPE_SIZE = "orc_optimized_writer_min_stripe_size";
    private static final String ORC_OPTIMIZED_WRITER_MAX_STRIPE_SIZE = "orc_optimized_writer_max_stripe_size";
    private static final String ORC_OPTIMIZED_WRITER_MAX_STRIPE_ROWS = "orc_optimized_writer_max_stripe_rows";
    private static final String ORC_OPTIMIZED_WRITER_MAX_DICTIONARY_MEMORY = "orc_optimized_writer_max_dictionary_memory";
    private static final String ORC_STRING_STATISTICS_LIMIT = "orc_string_statistics_limit";
    private static final String ORC_OPTIMIZED_WRITER_VALIDATE_MODE = "orc_optimized_writer_validate_mode";
    private static final String ORC_MAX_MERGE_DISTANCE = "orc_max_merge_distance";
    private static final String ORC_MAX_BUFFER_SIZE = "orc_max_buffer_size";
    private static final String ORC_STREAM_BUFFER_SIZE = "orc_stream_buffer_size";
    private static final String ORC_TINY_STRIPE_THRESHOLD = "orc_tiny_stripe_threshold";
    private static final String ORC_MAX_READ_BLOCK_SIZE = "orc_max_read_block_size";
    private static final String ORC_LAZY_READ_SMALL_RANGES = "orc_lazy_read_small_ranges";
    private static final String ORC_BLOOM_FILTERS_ENABLED = "orc_bloom_filters_enabled";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public IcebergSessionProperties(HiveConfig hiveConfig, OrcFileWriterConfig orcFileWriterConfig, ParquetFileWriterConfig parquetFileWriterConfig)
    {
        sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(booleanProperty(
                        PARQUET_FAIL_WITH_CORRUPTED_STATISTICS,
                        "Parquet: Fail when scanning Parquet files with corrupted statistics",
                        hiveConfig.isFailOnCorruptedParquetStatistics(),
                        false))
                .add(dataSizeProperty(
                        PARQUET_MAX_READ_BLOCK_SIZE,
                        "Parquet: Maximum size of a block to read",
                        hiveConfig.getParquetMaxReadBlockSize(),
                        false))
                .add(dataSizeProperty(
                        PARQUET_WRITER_BLOCK_SIZE,
                        "Parquet: Writer block size",
                        parquetFileWriterConfig.getBlockSize(),
                        false))
                .add(dataSizeProperty(
                        PARQUET_WRITER_PAGE_SIZE,
                        "Parquet: Writer page size",
                        parquetFileWriterConfig.getPageSize(),
                        false))
                .add(dataSizeProperty(
                        ORC_OPTIMIZED_WRITER_MIN_STRIPE_SIZE,
                        "Experimental: ORC: Min stripe size",
                        orcFileWriterConfig.getStripeMinSize(),
                        false))
                .add(dataSizeProperty(
                        ORC_OPTIMIZED_WRITER_MAX_STRIPE_SIZE,
                        "Experimental: ORC: Max stripe size",
                        orcFileWriterConfig.getStripeMaxSize(),
                        false))
                .add(integerProperty(
                        ORC_OPTIMIZED_WRITER_MAX_STRIPE_ROWS,
                        "Experimental: ORC: Max stripe row count",
                        orcFileWriterConfig.getStripeMaxRowCount(),
                        false))
                .add(dataSizeProperty(
                        ORC_OPTIMIZED_WRITER_MAX_DICTIONARY_MEMORY,
                        "Experimental: ORC: Max dictionary memory",
                        orcFileWriterConfig.getDictionaryMaxMemory(),
                        false))
                .add(dataSizeProperty(
                        ORC_STRING_STATISTICS_LIMIT,
                        "ORC: Maximum size of string statistics; drop if exceeding",
                        orcFileWriterConfig.getStringStatisticsLimit(),
                        false))
                .add(stringProperty(
                        ORC_OPTIMIZED_WRITER_VALIDATE_MODE,
                        "Experimental: ORC: Level of detail in ORC validation",
                        hiveConfig.getOrcWriterValidationMode().toString(),
                        false))
                .add(dataSizeProperty(
                        ORC_MAX_MERGE_DISTANCE,
                        "ORC: Maximum size of gap between two reads to merge into a single read",
                        hiveConfig.getOrcMaxMergeDistance(),
                        false))
                .add(dataSizeProperty(
                        ORC_MAX_BUFFER_SIZE,
                        "ORC: Maximum size of a single read",
                        hiveConfig.getOrcMaxBufferSize(),
                        false))
                .add(dataSizeProperty(
                        ORC_STREAM_BUFFER_SIZE,
                        "ORC: Size of buffer for streaming reads",
                        hiveConfig.getOrcStreamBufferSize(),
                        false))
                .add(dataSizeProperty(
                        ORC_TINY_STRIPE_THRESHOLD,
                        "ORC: Threshold below which an ORC stripe or file will read in its entirety",
                        hiveConfig.getOrcTinyStripeThreshold(),
                        false))
                .add(dataSizeProperty(
                        ORC_MAX_READ_BLOCK_SIZE,
                        "ORC: Soft max size of Presto blocks produced by ORC reader",
                        hiveConfig.getOrcMaxReadBlockSize(),
                        false))
                .add(booleanProperty(
                        ORC_LAZY_READ_SMALL_RANGES,
                        "Experimental: ORC: Read small file segments lazily",
                        hiveConfig.isOrcLazyReadSmallRanges(),
                        false))
                .add(booleanProperty(
                        ORC_BLOOM_FILTERS_ENABLED,
                        "ORC: Enable bloom filters for predicate pushdown",
                        hiveConfig.isOrcBloomFiltersEnabled(),
                        false))
                .build();
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isFailOnCorruptedParquetStatistics(ConnectorSession session)
    {
        return session.getProperty(PARQUET_FAIL_WITH_CORRUPTED_STATISTICS, Boolean.class);
    }

    public static DataSize getParquetMaxReadBlockSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_MAX_READ_BLOCK_SIZE, DataSize.class);
    }
}
