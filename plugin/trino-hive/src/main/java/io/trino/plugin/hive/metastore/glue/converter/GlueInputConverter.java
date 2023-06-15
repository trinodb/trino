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
package io.trino.plugin.hive.metastore.glue.converter;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.HiveBucketProperty;
import io.trino.plugin.hive.PartitionStatistics;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.PartitionWithStatistics;
import io.trino.plugin.hive.metastore.Storage;
import io.trino.plugin.hive.metastore.Table;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.Order;
import software.amazon.awssdk.services.glue.model.PartitionInput;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.TableInput;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.updateStatisticsParameters;

public final class GlueInputConverter
{
    private GlueInputConverter() {}

    public static DatabaseInput convertDatabase(Database database)
    {
        return DatabaseInput.builder()
                .name(database.getDatabaseName())
                .parameters(database.getParameters())
                .applyMutation(builder -> database.getComment().ifPresent(builder::description))
                .applyMutation(builder -> database.getLocation().ifPresent(builder::locationUri))
                .build();
    }

    public static TableInput convertTable(Table table)
    {
        return TableInput.builder()
                .name(table.getTableName())
                .owner(table.getOwner().orElse(null))
                .tableType(table.getTableType())
                .storageDescriptor(convertStorage(table.getStorage(), table.getDataColumns()))
                .partitionKeys(table.getPartitionColumns().stream().map(GlueInputConverter::convertColumn).collect(toImmutableList()))
                .parameters(table.getParameters())
                .applyMutation(builder -> table.getViewOriginalText().ifPresent(builder::viewOriginalText))
                .applyMutation(builder -> table.getViewExpandedText().ifPresent(builder::viewExpandedText))
                .build();
    }

    public static PartitionInput convertPartition(PartitionWithStatistics partitionWithStatistics)
    {
        PartitionInput input = convertPartition(partitionWithStatistics.getPartition());
        PartitionStatistics statistics = partitionWithStatistics.getStatistics();
        return input.toBuilder()
                .parameters(updateStatisticsParameters(input.parameters(), statistics.getBasicStatistics()))
                .build();
    }

    public static PartitionInput convertPartition(Partition partition)
    {
        return PartitionInput.builder()
                .values(partition.getValues())
                .storageDescriptor(convertStorage(partition.getStorage(), partition.getColumns()))
                .parameters(partition.getParameters())
                .build();
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

        StorageDescriptor.Builder sd = StorageDescriptor.builder()
                .location(storage.getLocation())
                .columns(columns.stream().map(GlueInputConverter::convertColumn).collect(toImmutableList()))
                .serdeInfo(serdeInfo)
                .inputFormat(storage.getStorageFormat().getInputFormatNullable())
                .outputFormat(storage.getStorageFormat().getOutputFormatNullable())
                .parameters(ImmutableMap.of());

        Optional<HiveBucketProperty> bucketProperty = storage.getBucketProperty();
        if (bucketProperty.isPresent()) {
            sd.numberOfBuckets(bucketProperty.get().getBucketCount());
            sd.bucketColumns(bucketProperty.get().getBucketedBy());
            if (!bucketProperty.get().getSortedBy().isEmpty()) {
                sd.sortColumns(bucketProperty.get().getSortedBy().stream()
                        .map(column -> Order.builder().column(column.getColumnName()).sortOrder(column.getOrder().getHiveOrder()).build())
                        .collect(toImmutableList()));
            }
        }

        return sd.build();
    }

    private static software.amazon.awssdk.services.glue.model.Column convertColumn(Column prestoColumn)
    {
        return software.amazon.awssdk.services.glue.model.Column.builder()
                .name(prestoColumn.getName())
                .type(prestoColumn.getType().toString())
                .comment(prestoColumn.getComment().orElse(null))
                .build();
    }
}
