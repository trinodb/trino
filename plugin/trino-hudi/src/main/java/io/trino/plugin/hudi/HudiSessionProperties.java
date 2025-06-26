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
package io.trino.plugin.hudi;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;

import java.util.Collection;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.base.session.PropertyMetadataUtil.dataSizeProperty;
import static io.trino.plugin.base.session.PropertyMetadataUtil.durationProperty;
import static io.trino.plugin.base.session.PropertyMetadataUtil.validateMaxDataSize;
import static io.trino.plugin.hive.parquet.ParquetReaderConfig.PARQUET_READER_MAX_SMALL_FILE_THRESHOLD;
import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.doubleProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public class HudiSessionProperties
        implements SessionPropertiesProvider
{
    private static final String COLUMNS_TO_HIDE = "columns_to_hide";
    static final String TABLE_STATISTICS_ENABLED = "table_statistics_enabled";
    static final String METADATA_TABLE_ENABLED = "metadata_enabled";
    private static final String USE_PARQUET_COLUMN_NAMES = "use_parquet_column_names";
    private static final String PARQUET_IGNORE_STATISTICS = "parquet_ignore_statistics";
    private static final String PARQUET_USE_COLUMN_INDEX = "parquet_use_column_index";
    private static final String PARQUET_USE_BLOOM_FILTER = "parquet_use_bloom_filter";
    private static final String PARQUET_MAX_READ_BLOCK_SIZE = "parquet_max_read_block_size";
    private static final String PARQUET_MAX_READ_BLOCK_ROW_COUNT = "parquet_max_read_block_row_count";
    private static final String PARQUET_SMALL_FILE_THRESHOLD = "parquet_small_file_threshold";
    private static final String PARQUET_VECTORIZED_DECODING_ENABLED = "parquet_vectorized_decoding_enabled";
    private static final String SIZE_BASED_SPLIT_WEIGHTS_ENABLED = "size_based_split_weights_enabled";
    private static final String STANDARD_SPLIT_WEIGHT_SIZE = "standard_split_weight_size";
    private static final String MINIMUM_ASSIGNED_SPLIT_WEIGHT = "minimum_assigned_split_weight";
    private static final String TARGET_SPLIT_SIZE = "target_split_size";
    private static final String MAX_SPLITS_PER_SECOND = "max_splits_per_second";
    private static final String MAX_OUTSTANDING_SPLITS = "max_outstanding_splits";
    private static final String SPLIT_GENERATOR_PARALLELISM = "split_generator_parallelism";
    static final String QUERY_PARTITION_FILTER_REQUIRED = "query_partition_filter_required";
    private static final String IGNORE_ABSENT_PARTITIONS = "ignore_absent_partitions";
    static final String DYNAMIC_FILTERING_WAIT_TIMEOUT = "dynamic_filtering_wait_timeout";

    // Internal configuration for debugging and testing
    static final String RECORD_LEVEL_INDEX_ENABLED = "record_level_index_enabled";
    static final String SECONDARY_INDEX_ENABLED = "secondary_index_enabled";
    static final String COLUMN_STATS_INDEX_ENABLED = "column_stats_index_enabled";
    static final String PARTITION_STATS_INDEX_ENABLED = "partition_stats_index_enabled";
    static final String COLUMN_STATS_WAIT_TIMEOUT = "column_stats_wait_timeout";
    static final String RECORD_INDEX_WAIT_TIMEOUT = "record_index_wait_timeout";
    static final String SECONDARY_INDEX_WAIT_TIMEOUT = "secondary_index_wait_timeout";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public HudiSessionProperties(HudiConfig hudiConfig, ParquetReaderConfig parquetReaderConfig)
    {
        sessionProperties = ImmutableList.of(
                new PropertyMetadata<>(
                        COLUMNS_TO_HIDE,
                        "List of column names that will be hidden",
                        new ArrayType(VARCHAR),
                        List.class,
                        hudiConfig.getColumnsToHide(),
                        false,
                        value -> ((Collection<?>) value).stream()
                                .map(name -> ((String) name).toLowerCase(ENGLISH))
                                .collect(toImmutableList()),
                        value -> value),
                booleanProperty(
                        TABLE_STATISTICS_ENABLED,
                        "Expose table statistics",
                        hudiConfig.isTableStatisticsEnabled(),
                        false),
                booleanProperty(
                        METADATA_TABLE_ENABLED,
                        "For Hudi tables prefer to fetch the list of files from its metadata table",
                        hudiConfig.isMetadataEnabled(),
                        false),
                booleanProperty(
                        USE_PARQUET_COLUMN_NAMES,
                        "Access parquet columns using names from the file. If disabled, then columns are accessed using index.",
                        hudiConfig.getUseParquetColumnNames(),
                        false),
                booleanProperty(
                        PARQUET_IGNORE_STATISTICS,
                        "Ignore statistics from Parquet to allow querying files with corrupted or incorrect statistics",
                        parquetReaderConfig.isIgnoreStatistics(),
                        false),
                booleanProperty(
                        PARQUET_USE_COLUMN_INDEX,
                        "Use Parquet column index",
                        hudiConfig.isUseParquetColumnIndex(),
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
                        PARQUET_SMALL_FILE_THRESHOLD,
                        "Parquet: Size below which a parquet file will be read entirely",
                        parquetReaderConfig.getSmallFileThreshold(),
                        value -> validateMaxDataSize(PARQUET_SMALL_FILE_THRESHOLD, value, DataSize.valueOf(PARQUET_READER_MAX_SMALL_FILE_THRESHOLD)),
                        false),
                booleanProperty(
                        PARQUET_VECTORIZED_DECODING_ENABLED,
                        "Enable using Java Vector API for faster decoding of parquet files",
                        parquetReaderConfig.isVectorizedDecodingEnabled(),
                        false),
                booleanProperty(
                        SIZE_BASED_SPLIT_WEIGHTS_ENABLED,
                        format("If enabled, size-based splitting ensures that each batch of splits has enough data to process as defined by %s", STANDARD_SPLIT_WEIGHT_SIZE),
                        hudiConfig.isSizeBasedSplitWeightsEnabled(),
                        false),
                dataSizeProperty(
                        STANDARD_SPLIT_WEIGHT_SIZE,
                        "The split size corresponding to the standard weight (1.0) when size-based split weights are enabled",
                        hudiConfig.getStandardSplitWeightSize(),
                        false),
                doubleProperty(
                        MINIMUM_ASSIGNED_SPLIT_WEIGHT,
                        "Minimum assigned split weight when size-based split weights are enabled",
                        hudiConfig.getMinimumAssignedSplitWeight(),
                        value -> {
                            if (!Double.isFinite(value) || value <= 0 || value > 1) {
                                throw new TrinoException(INVALID_SESSION_PROPERTY, format("%s must be > 0 and <= 1.0: %s", MINIMUM_ASSIGNED_SPLIT_WEIGHT, value));
                            }
                        },
                        false),
                dataSizeProperty(
                        TARGET_SPLIT_SIZE,
                        "The target split size",
                        hudiConfig.getTargetSplitSize(),
                        false),
                integerProperty(
                        MAX_SPLITS_PER_SECOND,
                        "Rate at which splits are enqueued for processing. The queue will throttle if this rate limit is breached.",
                        hudiConfig.getMaxSplitsPerSecond(),
                        false),
                integerProperty(
                        MAX_OUTSTANDING_SPLITS,
                        "Maximum outstanding splits in a batch enqueued for processing",
                        hudiConfig.getMaxOutstandingSplits(),
                        false),
                integerProperty(
                        SPLIT_GENERATOR_PARALLELISM,
                        "Number of threads to generate splits from partitions",
                        hudiConfig.getSplitGeneratorParallelism(),
                        false),
                booleanProperty(
                        QUERY_PARTITION_FILTER_REQUIRED,
                        "Require a filter on at least one partition column",
                        hudiConfig.isQueryPartitionFilterRequired(),
                        false),
                booleanProperty(
                        IGNORE_ABSENT_PARTITIONS,
                        "Ignore absent partitions",
                        hudiConfig.isIgnoreAbsentPartitions(),
                        false),
                booleanProperty(
                        RECORD_LEVEL_INDEX_ENABLED,
                        "Enable record level index for file skipping",
                        hudiConfig.isRecordLevelIndexEnabled(),
                        true),
                booleanProperty(
                        SECONDARY_INDEX_ENABLED,
                        "Enable secondary index for file skipping",
                        hudiConfig.isSecondaryIndexEnabled(),
                        true),
                booleanProperty(
                        COLUMN_STATS_INDEX_ENABLED,
                        "Enable column stats index for file skipping",
                        hudiConfig.isColumnStatsIndexEnabled(),
                        true),
                booleanProperty(
                        PARTITION_STATS_INDEX_ENABLED,
                        "Enable partition stats index for file skipping",
                        hudiConfig.isPartitionStatsIndexEnabled(),
                        true),
                durationProperty(
                        COLUMN_STATS_WAIT_TIMEOUT,
                        "Maximum timeout to wait for loading column stats",
                        hudiConfig.getColumnStatsWaitTimeout(),
                        false),
                durationProperty(
                        RECORD_INDEX_WAIT_TIMEOUT,
                        "Maximum timeout to wait for loading record index",
                        hudiConfig.getRecordIndexWaitTimeout(),
                        false),
                durationProperty(
                        SECONDARY_INDEX_WAIT_TIMEOUT,
                        "Maximum timeout to wait for loading secondary index",
                        hudiConfig.getSecondaryIndexWaitTimeout(),
                        false),
                durationProperty(
                        DYNAMIC_FILTERING_WAIT_TIMEOUT,
                        "Duration to wait for completion of dynamic filters during split generation",
                        hudiConfig.getDynamicFilteringWaitTimeout(),
                        false));
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    @SuppressWarnings("unchecked")
    public static List<String> getColumnsToHide(ConnectorSession session)
    {
        return (List<String>) session.getProperty(COLUMNS_TO_HIDE, List.class);
    }

    public static boolean isTableStatisticsEnabled(ConnectorSession session)
    {
        return session.getProperty(TABLE_STATISTICS_ENABLED, Boolean.class);
    }

    public static boolean isHudiMetadataTableEnabled(ConnectorSession session)
    {
        return session.getProperty(METADATA_TABLE_ENABLED, Boolean.class);
    }

    public static boolean shouldUseParquetColumnNames(ConnectorSession session)
    {
        return session.getProperty(USE_PARQUET_COLUMN_NAMES, Boolean.class);
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

    public static DataSize getParquetSmallFileThreshold(ConnectorSession session)
    {
        return session.getProperty(PARQUET_SMALL_FILE_THRESHOLD, DataSize.class);
    }

    public static boolean isParquetVectorizedDecodingEnabled(ConnectorSession session)
    {
        return session.getProperty(PARQUET_VECTORIZED_DECODING_ENABLED, Boolean.class);
    }

    public static boolean isSizeBasedSplitWeightsEnabled(ConnectorSession session)
    {
        return session.getProperty(SIZE_BASED_SPLIT_WEIGHTS_ENABLED, Boolean.class);
    }

    public static DataSize getStandardSplitWeightSize(ConnectorSession session)
    {
        return session.getProperty(STANDARD_SPLIT_WEIGHT_SIZE, DataSize.class);
    }

    public static double getMinimumAssignedSplitWeight(ConnectorSession session)
    {
        return session.getProperty(MINIMUM_ASSIGNED_SPLIT_WEIGHT, Double.class);
    }

    public static DataSize getTargetSplitSize(ConnectorSession session)
    {
        return session.getProperty(TARGET_SPLIT_SIZE, DataSize.class);
    }

    public static int getMaxSplitsPerSecond(ConnectorSession session)
    {
        return session.getProperty(MAX_SPLITS_PER_SECOND, Integer.class);
    }

    public static int getMaxOutstandingSplits(ConnectorSession session)
    {
        return session.getProperty(MAX_OUTSTANDING_SPLITS, Integer.class);
    }

    public static int getSplitGeneratorParallelism(ConnectorSession session)
    {
        return session.getProperty(SPLIT_GENERATOR_PARALLELISM, Integer.class);
    }

    public static boolean isQueryPartitionFilterRequired(ConnectorSession session)
    {
        return session.getProperty(QUERY_PARTITION_FILTER_REQUIRED, Boolean.class);
    }

    public static boolean isIgnoreAbsentPartitions(ConnectorSession session)
    {
        return session.getProperty(IGNORE_ABSENT_PARTITIONS, Boolean.class);
    }

    public static boolean isRecordLevelIndexEnabled(ConnectorSession session)
    {
        return session.getProperty(RECORD_LEVEL_INDEX_ENABLED, Boolean.class);
    }

    public static boolean isSecondaryIndexEnabled(ConnectorSession session)
    {
        return session.getProperty(SECONDARY_INDEX_ENABLED, Boolean.class);
    }

    public static boolean isColumnStatsIndexEnabled(ConnectorSession session)
    {
        return session.getProperty(COLUMN_STATS_INDEX_ENABLED, Boolean.class);
    }

    public static boolean isPartitionStatsIndexEnabled(ConnectorSession session)
    {
        return session.getProperty(PARTITION_STATS_INDEX_ENABLED, Boolean.class);
    }

    public static boolean isNoOpIndexEnabled(ConnectorSession session)
    {
        return !isRecordLevelIndexEnabled(session) && !isSecondaryIndexEnabled(session) && !isColumnStatsIndexEnabled(session);
    }

    public static Duration getDynamicFilteringWaitTimeout(ConnectorSession session)
    {
        return session.getProperty(DYNAMIC_FILTERING_WAIT_TIMEOUT, Duration.class);
    }

    public static Duration getColumnStatsWaitTimeout(ConnectorSession session)
    {
        return session.getProperty(COLUMN_STATS_WAIT_TIMEOUT, Duration.class);
    }

    public static Duration getRecordIndexWaitTimeout(ConnectorSession session)
    {
        return session.getProperty(RECORD_INDEX_WAIT_TIMEOUT, Duration.class);
    }

    public static Duration getSecondaryIndexWaitTimeout(ConnectorSession session)
    {
        return session.getProperty(SECONDARY_INDEX_WAIT_TIMEOUT, Duration.class);
    }
}
