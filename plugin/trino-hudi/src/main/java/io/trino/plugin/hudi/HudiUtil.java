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
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartition;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hive.HivePartitionManager;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hudi.split.HudiSplitWeightProvider;
import io.trino.plugin.hudi.split.SizeBasedSplitWeightProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.trino.plugin.hive.util.HiveUtil.checkCondition;
import static io.trino.plugin.hive.util.HiveUtil.parsePartitionValue;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_UNKNOWN_TABLE_TYPE;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_UNSUPPORTED_FILE_FORMAT;
import static io.trino.plugin.hudi.HudiSessionProperties.getMinimumAssignedSplitWeight;
import static io.trino.plugin.hudi.HudiSessionProperties.getStandardSplitWeightSize;
import static io.trino.plugin.hudi.HudiSessionProperties.isSizeBasedSplitWeightsEnabled;
import static java.util.stream.Collectors.toList;
import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;
import static org.apache.hudi.common.model.HoodieTableType.MERGE_ON_READ;

public final class HudiUtil
{
    private HudiUtil() {}

    public static boolean isHudiParquetInputFormat(InputFormat<?, ?> inputFormat)
    {
        return inputFormat instanceof HoodieParquetInputFormat;
    }

    public static HoodieFileFormat getHudiFileFormat(String path)
    {
        final String extension = FSUtils.getFileExtension(path);
        if (extension.equals(HoodieFileFormat.PARQUET.getFileExtension())) {
            return HoodieFileFormat.PARQUET;
        }
        if (extension.equals(HoodieFileFormat.HOODIE_LOG.getFileExtension())) {
            return HoodieFileFormat.HOODIE_LOG;
        }
        if (extension.equals(HoodieFileFormat.ORC.getFileExtension())) {
            return HoodieFileFormat.ORC;
        }
        if (extension.equals(HoodieFileFormat.HFILE.getFileExtension())) {
            return HoodieFileFormat.HFILE;
        }
        throw new TrinoException(HUDI_UNSUPPORTED_FILE_FORMAT, "Hoodie InputFormat not implemented for base file of type " + extension);
    }

    public static boolean partitionMatchesPredicates(
            SchemaTableName tableName,
            String hivePartitionName,
            List<HiveColumnHandle> partitionColumnHandles,
            TupleDomain<HiveColumnHandle> constraintSummary)
    {
        List<Type> partitionColumnTypes = partitionColumnHandles.stream()
                .map(HiveColumnHandle::getType)
                .collect(toList());
        HivePartition partition = HivePartitionManager.parsePartition(
                tableName, hivePartitionName, partitionColumnHandles, partitionColumnTypes);

        return partitionMatches(partitionColumnHandles, constraintSummary, partition);
    }

    public static boolean partitionMatchesPredicates(
            SchemaTableName tableName,
            String relativePartitionPath,
            List<String> partitionValues,
            List<HiveColumnHandle> partitionColumnHandles,
            TupleDomain<HiveColumnHandle> constraintSummary)
    {
        List<Type> partitionColumnTypes = partitionColumnHandles.stream()
                .map(HiveColumnHandle::getType)
                .collect(toList());
        HivePartition partition = parsePartition(
                tableName, relativePartitionPath, partitionValues, partitionColumnHandles, partitionColumnTypes);

        return partitionMatches(partitionColumnHandles, constraintSummary, partition);
    }

    private static HivePartition parsePartition(
            SchemaTableName tableName,
            String partitionName,
            List<String> partitionValues,
            List<HiveColumnHandle> partitionColumns,
            List<Type> partitionColumnTypes)
    {
        ImmutableMap.Builder<ColumnHandle, NullableValue> builder = ImmutableMap.builder();
        for (int i = 0; i < partitionColumns.size(); i++) {
            HiveColumnHandle column = partitionColumns.get(i);
            NullableValue parsedValue = parsePartitionValue(
                    partitionName, partitionValues.get(i), partitionColumnTypes.get(i));
            builder.put(column, parsedValue);
        }
        Map<ColumnHandle, NullableValue> values = builder.buildOrThrow();
        return new HivePartition(tableName, partitionName, values);
    }

    public static boolean partitionMatches(List<HiveColumnHandle> partitionColumns, TupleDomain<HiveColumnHandle> constraintSummary, HivePartition partition)
    {
        if (constraintSummary.isNone()) {
            return false;
        }
        Map<HiveColumnHandle, Domain> domains = constraintSummary.getDomains().orElseGet(ImmutableMap::of);
        for (HiveColumnHandle column : partitionColumns) {
            NullableValue value = partition.getKeys().get(column);
            Domain allowedDomain = domains.get(column);
            if (allowedDomain != null && !allowedDomain.includesNullableValue(value.getValue())) {
                return false;
            }
        }
        return true;
    }

    public static List<HivePartitionKey> buildPartitionKeys(List<Column> keys, List<String> values)
    {
        checkCondition(keys.size() == values.size(), HIVE_INVALID_METADATA,
                "Expected %s partition key values, but got %s. Keys: %s, Values: %s.",
                keys.size(), values.size(), keys, values);
        ImmutableList.Builder<HivePartitionKey> partitionKeys = ImmutableList.builder();
        for (int i = 0; i < keys.size(); i++) {
            String name = keys.get(i).getName();
            String value = values.get(i);
            partitionKeys.add(new HivePartitionKey(name, value));
        }
        return partitionKeys.build();
    }

    public static FileStatus getFileStatus(HoodieBaseFile baseFile)
    {
        try {
            return HoodieInputFormatUtils.getFileStatus(baseFile);
        }
        catch (IOException e) {
            throw new TrinoException(HUDI_CANNOT_OPEN_SPLIT, "Error getting file status of " + baseFile.getPath(), e);
        }
    }

    public static HoodieTableType getTableType(String inputFormatName)
    {
        return switch (inputFormatName) {
            case "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "org.apache.hudi.hadoop.HoodieParquetInputFormat",
                    "com.uber.hoodie.hadoop.HoodieInputFormat" -> COPY_ON_WRITE;
            case "org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat",
                    "com.uber.hoodie.hadoop.realtime.HoodieRealtimeInputFormat" -> MERGE_ON_READ;
            default -> throw new TrinoException(HUDI_UNKNOWN_TABLE_TYPE, "Unknown table type for input format: " + inputFormatName);
        };
    }

    public static HudiFile getHudiBaseFile(HudiSplit hudiSplit)
    {
        // use first log file as base file for MOR table if it hasn't base file
        return hudiSplit.getBaseFile().orElse(hudiSplit.getLogFiles().get(0));
    }

    public static HoodieTableMetaClient buildTableMetaClient(Configuration configuration, String basePath)
    {
        HoodieTableMetaClient client = HoodieTableMetaClient.builder().setConf(configuration).setBasePath(basePath).build();
        client.getTableConfig().setValue("hoodie.bootstrap.index.enable", "false");
        return client;
    }

    public static HudiSplitWeightProvider createSplitWeightProvider(ConnectorSession session)
    {
        if (isSizeBasedSplitWeightsEnabled(session)) {
            DataSize standardSplitWeightSize = getStandardSplitWeightSize(session);
            double minimumAssignedSplitWeight = getMinimumAssignedSplitWeight(session);
            return new SizeBasedSplitWeightProvider(minimumAssignedSplitWeight, standardSplitWeightSize);
        }
        return HudiSplitWeightProvider.uniformStandardWeightProvider();
    }
}
