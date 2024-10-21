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
package io.trino.plugin.hive.metastore.glue.v2.converter;

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
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.Order;
import software.amazon.awssdk.services.glue.model.PartitionInput;
import software.amazon.awssdk.services.glue.model.PrincipalType;
import software.amazon.awssdk.services.glue.model.ResourceType;
import software.amazon.awssdk.services.glue.model.ResourceUri;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UserDefinedFunctionInput;

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
import static io.trino.plugin.hive.metastore.glue.v2.converter.GlueToTrinoConverter.getTableParameters;
import static io.trino.plugin.hive.metastore.glue.v2.converter.GlueToTrinoConverter.getTableTypeNullable;

public final class GlueInputConverter
{
    static final JsonCodec<LanguageFunction> LANGUAGE_FUNCTION_CODEC = JsonCodec.jsonCodec(LanguageFunction.class);

    private GlueInputConverter() {}

    public static DatabaseInput convertDatabase(Database database)
    {
        DatabaseInput.Builder builder = DatabaseInput.builder();
        builder.name(database.getDatabaseName());
        builder.parameters(database.getParameters());
        database.getComment().ifPresent(builder::description);
        database.getLocation().ifPresent(builder::locationUri);
        return builder.build();
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

        TableInput.Builder builder = TableInput.builder();
        builder.name(table.getTableName());
        builder.owner(table.getOwner().orElse(null));
        builder.tableType(table.getTableType());
        builder.storageDescriptor(convertStorage(table.getStorage(), table.getDataColumns()));
        builder.partitionKeys(table.getPartitionColumns().stream().map(GlueInputConverter::convertColumn).collect(toImmutableList()));
        builder.parameters(tableParameters);
        table.getViewOriginalText().ifPresent(builder::viewOriginalText);
        table.getViewExpandedText().ifPresent(builder::viewExpandedText);
        comment.ifPresent(builder::description);
        return builder.build();
    }

    public static TableInput convertGlueTableToTableInput(software.amazon.awssdk.services.glue.model.Table glueTable)
    {
        return TableInput.builder()
                .name(glueTable.name())
                .description(glueTable.description())
                .owner(glueTable.owner())
                .lastAccessTime(glueTable.lastAccessTime())
                .lastAnalyzedTime(glueTable.lastAnalyzedTime())
                .retention(glueTable.retention())
                .storageDescriptor(glueTable.storageDescriptor())
                .partitionKeys(glueTable.partitionKeys())
                .viewOriginalText(glueTable.viewOriginalText())
                .viewExpandedText(glueTable.viewExpandedText())
                .tableType(getTableTypeNullable(glueTable))
                .targetTable(glueTable.targetTable())
                .parameters(getTableParameters(glueTable))
                .build();
    }

    public static PartitionInput convertPartition(PartitionWithStatistics partitionWithStatistics)
    {
        PartitionInput input = convertPartition(partitionWithStatistics.getPartition());
        PartitionStatistics statistics = partitionWithStatistics.getStatistics();
        input.toBuilder().parameters(updateStatisticsParameters(input.parameters(), statistics.basicStatistics())).build();
        return input;
    }

    public static PartitionInput convertPartition(Partition partition)
    {
        PartitionInput.Builder builder = PartitionInput.builder();
        builder.values(partition.getValues());
        builder.storageDescriptor(convertStorage(partition.getStorage(), partition.getColumns()));
        builder.parameters(partition.getParameters());
        return builder.build();
    }

    private static StorageDescriptor convertStorage(Storage storage, List<Column> columns)
    {
        if (storage.isSkewed()) {
            throw new IllegalArgumentException("Writing to skewed table/partition is not supported");
        }
        SerDeInfo serdeInfo = SerDeInfo.builder()
                .serializationLibrary(storage.getStorageFormat().getSerDeNullable())
                .parameters(storage.getSerdeParameters())
                .build();

        StorageDescriptor.Builder sd = StorageDescriptor.builder();
        sd.location(storage.getLocation());
        sd.columns(columns.stream().map(GlueInputConverter::convertColumn).collect(toImmutableList()));
        sd.serdeInfo(serdeInfo);
        sd.inputFormat(storage.getStorageFormat().getInputFormatNullable());
        sd.outputFormat(storage.getStorageFormat().getOutputFormatNullable());
        sd.parameters(ImmutableMap.of());

        Optional<HiveBucketProperty> bucketProperty = storage.getBucketProperty();
        if (bucketProperty.isPresent()) {
            sd.numberOfBuckets(bucketProperty.get().bucketCount());
            sd.bucketColumns(bucketProperty.get().bucketedBy());
            if (!bucketProperty.get().sortedBy().isEmpty()) {
                sd.sortColumns(bucketProperty.get().sortedBy().stream()
                        .map(column -> Order.builder().column(column.columnName()).sortOrder(column.order().getHiveOrder()).build())
                        .collect(toImmutableList()));
            }
        }

        return sd.build();
    }

    private static software.amazon.awssdk.services.glue.model.Column convertColumn(Column trinoColumn)
    {
        return software.amazon.awssdk.services.glue.model.Column.builder()
                .name(trinoColumn.getName())
                .type(trinoColumn.getType().toString())
                .comment(trinoColumn.getComment().orElse(null))
                .parameters(trinoColumn.getProperties())
                .build();
    }

    public static UserDefinedFunctionInput convertFunction(String functionName, LanguageFunction function)
    {
        return UserDefinedFunctionInput.builder()
                .functionName(metastoreFunctionName(functionName, function.signatureToken()))
                .className("TrinoFunction")
                .ownerType(PrincipalType.USER)
                .ownerName(function.owner().orElse(null))
                .resourceUris(toResourceUris(LANGUAGE_FUNCTION_CODEC.toJsonBytes(function)).stream()
                        .map(uri -> ResourceUri.builder()
                                .resourceType(ResourceType.FILE)
                                .uri(uri.getUri())
                                .build())
                        .toList())
                .build();
    }
}
