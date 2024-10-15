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
package io.trino.plugin.hive.metastore.glue.v1.converter;

import com.amazonaws.services.glue.model.DatabaseInput;
import com.amazonaws.services.glue.model.Order;
import com.amazonaws.services.glue.model.PartitionInput;
import com.amazonaws.services.glue.model.PrincipalType;
import com.amazonaws.services.glue.model.ResourceType;
import com.amazonaws.services.glue.model.ResourceUri;
import com.amazonaws.services.glue.model.SerDeInfo;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.TableInput;
import com.amazonaws.services.glue.model.UserDefinedFunctionInput;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.trino.metastore.Column;
import io.trino.metastore.Database;
import io.trino.metastore.HiveBucketProperty;
import io.trino.metastore.Partition;
import io.trino.metastore.PartitionStatistics;
import io.trino.metastore.PartitionWithStatistics;
import io.trino.metastore.Storage;
import io.trino.metastore.Table;
import io.trino.spi.function.LanguageFunction;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.metastore.Table.TABLE_COMMENT;
import static io.trino.plugin.hive.ViewReaderUtil.isTrinoMaterializedView;
import static io.trino.plugin.hive.ViewReaderUtil.isTrinoView;
import static io.trino.plugin.hive.metastore.MetastoreUtil.metastoreFunctionName;
import static io.trino.plugin.hive.metastore.MetastoreUtil.toResourceUris;
import static io.trino.plugin.hive.metastore.MetastoreUtil.updateStatisticsParameters;
import static io.trino.plugin.hive.metastore.glue.v1.converter.GlueToTrinoConverter.getTableParameters;
import static io.trino.plugin.hive.metastore.glue.v1.converter.GlueToTrinoConverter.getTableTypeNullable;

public final class GlueInputConverter
{
    static final JsonCodec<LanguageFunction> LANGUAGE_FUNCTION_CODEC = JsonCodec.jsonCodec(LanguageFunction.class);

    private GlueInputConverter() {}

    public static DatabaseInput convertDatabase(Database database)
    {
        DatabaseInput input = new DatabaseInput();
        input.setName(database.getDatabaseName());
        input.setParameters(database.getParameters());
        database.getComment().ifPresent(input::setDescription);
        database.getLocation().ifPresent(input::setLocationUri);
        return input;
    }

    public static TableInput convertTable(Table table)
    {
        Map<String, String> tableParameters = table.getParameters();
        Optional<String> comment = Optional.empty();
        if (!isTrinoView(table) && !isTrinoMaterializedView(table)) {
            comment = Optional.ofNullable(tableParameters.get(TABLE_COMMENT));
            tableParameters = tableParameters.entrySet().stream()
                    .filter(entry -> !entry.getKey().equals(TABLE_COMMENT))
                    .collect(toImmutableMap(Entry::getKey, Entry::getValue));
        }

        TableInput input = new TableInput();
        input.setName(table.getTableName());
        input.setOwner(table.getOwner().orElse(null));
        input.setTableType(table.getTableType());
        input.setStorageDescriptor(convertStorage(table.getStorage(), table.getDataColumns()));
        input.setPartitionKeys(table.getPartitionColumns().stream().map(GlueInputConverter::convertColumn).collect(toImmutableList()));
        input.setParameters(tableParameters);
        table.getViewOriginalText().ifPresent(input::setViewOriginalText);
        table.getViewExpandedText().ifPresent(input::setViewExpandedText);
        comment.ifPresent(input::setDescription);
        return input;
    }

    public static TableInput convertGlueTableToTableInput(com.amazonaws.services.glue.model.Table glueTable)
    {
        return new TableInput()
                .withName(glueTable.getName())
                .withDescription(glueTable.getDescription())
                .withOwner(glueTable.getOwner())
                .withLastAccessTime(glueTable.getLastAccessTime())
                .withLastAnalyzedTime(glueTable.getLastAnalyzedTime())
                .withRetention(glueTable.getRetention())
                .withStorageDescriptor(glueTable.getStorageDescriptor())
                .withPartitionKeys(glueTable.getPartitionKeys())
                .withViewOriginalText(glueTable.getViewOriginalText())
                .withViewExpandedText(glueTable.getViewExpandedText())
                .withTableType(getTableTypeNullable(glueTable))
                .withTargetTable(glueTable.getTargetTable())
                .withParameters(getTableParameters(glueTable));
    }

    public static PartitionInput convertPartition(PartitionWithStatistics partitionWithStatistics)
    {
        PartitionInput input = convertPartition(partitionWithStatistics.getPartition());
        PartitionStatistics statistics = partitionWithStatistics.getStatistics();
        input.setParameters(updateStatisticsParameters(input.getParameters(), statistics.basicStatistics()));
        return input;
    }

    public static PartitionInput convertPartition(Partition partition)
    {
        PartitionInput input = new PartitionInput();
        input.setValues(partition.getValues());
        input.setStorageDescriptor(convertStorage(partition.getStorage(), partition.getColumns()));
        input.setParameters(partition.getParameters());
        return input;
    }

    private static StorageDescriptor convertStorage(Storage storage, List<Column> columns)
    {
        if (storage.isSkewed()) {
            throw new IllegalArgumentException("Writing to skewed table/partition is not supported");
        }
        SerDeInfo serdeInfo = new SerDeInfo()
                .withSerializationLibrary(storage.getStorageFormat().getSerDeNullable())
                .withParameters(storage.getSerdeParameters());

        StorageDescriptor sd = new StorageDescriptor();
        sd.setLocation(storage.getLocation());
        sd.setColumns(columns.stream().map(GlueInputConverter::convertColumn).collect(toImmutableList()));
        sd.setSerdeInfo(serdeInfo);
        sd.setInputFormat(storage.getStorageFormat().getInputFormatNullable());
        sd.setOutputFormat(storage.getStorageFormat().getOutputFormatNullable());
        sd.setParameters(ImmutableMap.of());

        Optional<HiveBucketProperty> bucketProperty = storage.getBucketProperty();
        if (bucketProperty.isPresent()) {
            sd.setNumberOfBuckets(bucketProperty.get().bucketCount());
            sd.setBucketColumns(bucketProperty.get().bucketedBy());
            if (!bucketProperty.get().sortedBy().isEmpty()) {
                sd.setSortColumns(bucketProperty.get().sortedBy().stream()
                        .map(column -> new Order().withColumn(column.columnName()).withSortOrder(column.order().getHiveOrder()))
                        .collect(toImmutableList()));
            }
        }

        return sd;
    }

    private static com.amazonaws.services.glue.model.Column convertColumn(Column trinoColumn)
    {
        return new com.amazonaws.services.glue.model.Column()
                .withName(trinoColumn.getName())
                .withType(trinoColumn.getType().toString())
                .withComment(trinoColumn.getComment().orElse(null))
                .withParameters(trinoColumn.getProperties());
    }

    public static UserDefinedFunctionInput convertFunction(String functionName, LanguageFunction function)
    {
        return new UserDefinedFunctionInput()
                .withFunctionName(metastoreFunctionName(functionName, function.signatureToken()))
                .withClassName("TrinoFunction")
                .withOwnerType(PrincipalType.USER)
                .withOwnerName(function.owner().orElse(null))
                .withResourceUris(toResourceUris(LANGUAGE_FUNCTION_CODEC.toJsonBytes(function)).stream()
                        .map(uri -> new ResourceUri()
                                .withResourceType(ResourceType.FILE)
                                .withUri(uri.getUri()))
                        .toList());
    }
}
