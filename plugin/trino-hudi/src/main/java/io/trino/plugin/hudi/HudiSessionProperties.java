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
import io.airlift.units.DataSize;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;

import javax.inject.Inject;

import java.util.Collection;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.base.session.PropertyMetadataUtil.dataSizeProperty;
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
    private static final String METADATA_ENABLED = "metadata_enabled";
    private static final String USE_PARQUET_COLUMN_NAMES = "use_parquet_column_names";
    private static final String PARQUET_OPTIMIZED_READER_ENABLED = "parquet_optimized_reader_enabled";
    private static final String PARQUET_OPTIMIZED_NESTED_READER_ENABLED = "parquet_optimized_nested_reader_enabled";
    private static final String MIN_PARTITION_BATCH_SIZE = "min_partition_batch_size";
    private static final String MAX_PARTITION_BATCH_SIZE = "max_partition_batch_size";
    private static final String SIZE_BASED_SPLIT_WEIGHTS_ENABLED = "size_based_split_weights_enabled";
    private static final String STANDARD_SPLIT_WEIGHT_SIZE = "standard_split_weight_size";
    private static final String MINIMUM_ASSIGNED_SPLIT_WEIGHT = "minimum_assigned_split_weight";

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
                        METADATA_ENABLED,
                        "For Hudi tables prefer to fetch the list of files from its metadata",
                        hudiConfig.isMetadataEnabled(),
                        false),
                booleanProperty(
                        USE_PARQUET_COLUMN_NAMES,
                        "Access parquet columns using names from the file. If disabled, then columns are accessed using index.",
                        hudiConfig.getUseParquetColumnNames(),
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
                integerProperty(
                        MIN_PARTITION_BATCH_SIZE,
                        "Minimum number of partitions returned in a single batch.",
                        hudiConfig.getMinPartitionBatchSize(),
                        false),
                integerProperty(
                        MAX_PARTITION_BATCH_SIZE,
                        "Maximum number of partitions returned in a single batch.",
                        hudiConfig.getMaxPartitionBatchSize(),
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

    public static boolean isHudiMetadataEnabled(ConnectorSession session)
    {
        return session.getProperty(METADATA_ENABLED, Boolean.class);
    }

    public static boolean shouldUseParquetColumnNames(ConnectorSession session)
    {
        return session.getProperty(USE_PARQUET_COLUMN_NAMES, Boolean.class);
    }

    public static boolean isParquetOptimizedReaderEnabled(ConnectorSession session)
    {
        return session.getProperty(PARQUET_OPTIMIZED_READER_ENABLED, Boolean.class);
    }

    public static boolean isParquetOptimizedNestedReaderEnabled(ConnectorSession session)
    {
        return session.getProperty(PARQUET_OPTIMIZED_NESTED_READER_ENABLED, Boolean.class);
    }

    public static int getMinPartitionBatchSize(ConnectorSession session)
    {
        return session.getProperty(MIN_PARTITION_BATCH_SIZE, Integer.class);
    }

    public static int getMaxPartitionBatchSize(ConnectorSession session)
    {
        return session.getProperty(MAX_PARTITION_BATCH_SIZE, Integer.class);
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
}
