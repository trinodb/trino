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
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;

import javax.inject.Inject;

import java.util.List;

import static io.trino.plugin.base.session.PropertyMetadataUtil.dataSizeProperty;
import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.doubleProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static java.lang.String.format;

public class HudiSessionProperties
        implements SessionPropertiesProvider
{
    private static final String BASE_FILE_FORMAT = "file_format";
    private static final String METADATA_ENABLED = "metadata_enabled";
    private static final String SKIP_METASTORE_FOR_PARTITION = "skip_metastore_for_partition";
    private static final String USE_PARQUET_COLUMN_NAMES = "use_parquet_column_names";
    private static final String PARTITION_SCANNER_PARALLELISM = "partition_scanner_parallelism";
    private static final String SPLIT_GENERATOR_PARALLELISM = "split_generator_parallelism";
    private static final String MIN_PARTITION_BATCH_SIZE = "min_partition_batch_size";
    private static final String MAX_PARTITION_BATCH_SIZE = "max_partition_batch_size";
    private static final String SIZE_BASED_SPLIT_WEIGHTS_ENABLED = "size_based_split_weights_enabled";
    private static final String STANDARD_SPLIT_WEIGHT_SIZE = "standard_split_weight_size";
    private static final String MINIMUM_ASSIGNED_SPLIT_WEIGHT = "minimum_assigned_split_weight";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public HudiSessionProperties(HudiConfig hudiConfig)
    {
        sessionProperties = ImmutableList.of(
                stringProperty(
                        BASE_FILE_FORMAT,
                        "Currently, only Parquet is supported",
                        hudiConfig.getBaseFileFormat(),
                        false),
                booleanProperty(
                        METADATA_ENABLED,
                        "For Hudi tables prefer to fetch the list of files from its metadata",
                        hudiConfig.isMetadataEnabled(),
                        false),
                booleanProperty(
                        SKIP_METASTORE_FOR_PARTITION,
                        "Whether to skip metastore for partition info. " +
                                "If enabled, then the partition info is fetched using Hudi's partition extractor and relative partition path.",
                        hudiConfig.getSkipMetaStoreForPartition(),
                        false),
                booleanProperty(
                        USE_PARQUET_COLUMN_NAMES,
                        "Access parquet columns using names from the file. If disabled, then columns are accessed using index.",
                        hudiConfig.getUseParquetColumnNames(),
                        false),
                integerProperty(
                        PARTITION_SCANNER_PARALLELISM,
                        "Number of threads to use for partition scanners",
                        hudiConfig.getPartitionScannerParallelism(),
                        false),
                integerProperty(
                        SPLIT_GENERATOR_PARALLELISM,
                        "Number of threads to use for split generators",
                        hudiConfig.getSplitGeneratorParallelism(),
                        false),
                integerProperty(
                        MIN_PARTITION_BATCH_SIZE,
                        "Minimum partition batch size",
                        hudiConfig.getMinPartitionBatchSize(),
                        false),
                integerProperty(
                        MAX_PARTITION_BATCH_SIZE,
                        "Maximum partition batch size",
                        hudiConfig.getMaxPartitionBatchSize(),
                        false),
                booleanProperty(
                        SIZE_BASED_SPLIT_WEIGHTS_ENABLED,
                        "Size-based splitting ensures that each batch of splits has enough data to process. Enabled by default.",
                        hudiConfig.isSizeBasedSplitWeightsEnabled(),
                        false),
                dataSizeProperty(
                        STANDARD_SPLIT_WEIGHT_SIZE,
                        "The split size corresponding to the standard weight (1.0) "
                                + "when size based split weights are enabled",
                        hudiConfig.getStandardSplitWeightSize(),
                        false),
                doubleProperty(
                        MINIMUM_ASSIGNED_SPLIT_WEIGHT,
                        "Minimum assigned split weight when size based split weights are enabled",
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

    public static HoodieFileFormat getBaseFileFormat(ConnectorSession session)
    {
        return session.getProperty(BASE_FILE_FORMAT, HoodieFileFormat.class);
    }

    public static boolean isHudiMetadataEnabled(ConnectorSession session)
    {
        return session.getProperty(METADATA_ENABLED, Boolean.class);
    }

    public static boolean shouldSkipMetaStoreForPartition(ConnectorSession session)
    {
        return session.getProperty(SKIP_METASTORE_FOR_PARTITION, Boolean.class);
    }

    public static boolean shouldUseParquetColumnNames(ConnectorSession session)
    {
        return session.getProperty(USE_PARQUET_COLUMN_NAMES, Boolean.class);
    }

    public static int getPartitionScannerParallelism(ConnectorSession session)
    {
        return session.getProperty(PARTITION_SCANNER_PARALLELISM, Integer.class);
    }

    public static int getSplitGeneratorParallelism(ConnectorSession session)
    {
        return session.getProperty(SPLIT_GENERATOR_PARALLELISM, Integer.class);
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
