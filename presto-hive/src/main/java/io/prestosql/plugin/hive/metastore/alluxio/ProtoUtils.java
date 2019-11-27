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
package io.prestosql.plugin.hive.metastore.alluxio;

import alluxio.grpc.table.FieldSchema;
import alluxio.grpc.table.Layout;
import alluxio.grpc.table.layout.hive.PartitionInfo;
import alluxio.shaded.client.com.google.protobuf.InvalidProtocolBufferException;
import com.google.common.collect.Lists;
import io.prestosql.plugin.hive.HiveBucketProperty;
import io.prestosql.plugin.hive.HiveType;
import io.prestosql.plugin.hive.metastore.Column;
import io.prestosql.plugin.hive.metastore.Database;
import io.prestosql.plugin.hive.metastore.Partition;
import io.prestosql.plugin.hive.metastore.SortingColumn;
import io.prestosql.plugin.hive.metastore.StorageFormat;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.plugin.hive.util.HiveBucketing;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class ProtoUtils
{
    private ProtoUtils() {}

    public static Database fromProto(alluxio.grpc.table.Database db)
    {
        return Database.builder()
                .setDatabaseName(db.getDbName())
                .setLocation(db.hasLocation() ? Optional.of(db.getLocation()) : Optional.empty())
                .build();
    }

    public static Table fromProto(alluxio.grpc.table.TableInfo table)
    {
        if (!table.hasLayout()) {
            throw new UnsupportedOperationException("Unsupported table metadata. missing layout.");
        }
        Layout layout = table.getLayout();
        if (!alluxio.table.ProtoUtils.isHiveLayout(layout)) {
            throw new UnsupportedOperationException("Unsupported table layout: " + layout);
        }
        try {
            PartitionInfo partitionInfo = alluxio.table.ProtoUtils.toHiveLayout(layout);

            // compute the data columns
            Set<String> partitionColumns = table.getPartitionColsList().stream().map(FieldSchema::getName).collect(Collectors.toSet());
            List<FieldSchema> dataColumns = table.getSchema().getColsList().stream().filter((f) -> !partitionColumns.contains(f.getName())).collect(Collectors.toList());

            Table.Builder builder = Table.builder()
                    .setDatabaseName(table.getDbName())
                    .setTableName(table.getTableName())
                    .setOwner(table.getOwner())
                    .setTableType(table.getType().toString())
                    .setDataColumns(dataColumns.stream().map(ProtoUtils::fromProto).collect(Collectors.toList()))
                    .setPartitionColumns(table.getPartitionColsList().stream().map(ProtoUtils::fromProto).collect(Collectors.toList()))
                    .setParameters(table.getParametersMap())
                    .setViewOriginalText(Optional.empty())
                    .setViewExpandedText(Optional.empty());
            alluxio.grpc.table.layout.hive.Storage storage = partitionInfo.getStorage();
            builder.getStorageBuilder()
                    .setSkewed(storage.getSkewed())
                    .setStorageFormat(fromProto(storage.getStorageFormat()))
                    .setLocation(storage.getLocation())
                    .setBucketProperty(storage.hasBucketProperty() ? fromProto(storage.getBucketProperty()) : Optional.empty())
                    .setSerdeParameters(storage.getStorageFormat().getSerdelibParametersMap());
            return builder.build();
        }
        catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Failed to extract PartitionInfo from TableInfo", e);
        }
    }

    private static SortingColumn fromProto(alluxio.grpc.table.layout.hive.SortingColumn column)
    {
        if (column.getOrder().equals(alluxio.grpc.table.layout.hive.SortingColumn.SortingOrder.ASCENDING)) {
            return new SortingColumn(column.getColumnName(), SortingColumn.Order.ASCENDING);
        }
        if (column.getOrder().equals(alluxio.grpc.table.layout.hive.SortingColumn.SortingOrder.DESCENDING)) {
            return new SortingColumn(column.getColumnName(), SortingColumn.Order.DESCENDING);
        }
        throw new IllegalArgumentException("Invalid sort order: " + column.getOrder());
    }

    private static Optional<HiveBucketProperty> fromProto(alluxio.grpc.table.layout.hive.HiveBucketProperty property)
    {
        // must return empty if buckets <= 0
        if (!property.hasBucketCount() || property.getBucketCount() <= 0) {
            return Optional.empty();
        }
        List<SortingColumn> sortedBy = property.getSortedByList().stream().map(ProtoUtils::fromProto).collect(Collectors.toList());
        return Optional.of(new HiveBucketProperty(property.getBucketedByList(), HiveBucketing.BucketingVersion.BUCKETING_V1,
            (int) property.getBucketCount(), sortedBy));
    }

    private static StorageFormat fromProto(alluxio.grpc.table.layout.hive.StorageFormat format)
    {
        return StorageFormat.create(format.getSerde(), format.getInputFormat(), format.getOutputFormat());
    }

    private static Column fromProto(alluxio.grpc.table.FieldSchema column)
    {
        Optional<String> comment = column.hasComment() ? Optional.of(column.getComment()) : Optional.empty();
        return new Column(column.getName(), HiveType.valueOf(column.getType()), comment);
    }

    public static Partition fromProto(alluxio.grpc.table.layout.hive.PartitionInfo info)
    {
        Partition.Builder builder = Partition.builder()
                .setColumns(info.getDataColsList().stream().map(ProtoUtils::fromProto).collect(Collectors.toList()))
                .setDatabaseName(info.getDbName())
                .setParameters(info.getParametersMap())
                .setValues(Lists.newArrayList(info.getValuesList()))
                .setTableName(info.getTableName());

        builder.getStorageBuilder()
                .setSkewed(info.getStorage().getSkewed())
                .setStorageFormat(fromProto(info.getStorage().getStorageFormat()))
                .setLocation(info.getStorage().getLocation())
                .setBucketProperty(info.getStorage().hasBucketProperty()
                    ? fromProto(info.getStorage().getBucketProperty()) : Optional.empty())
                .setSerdeParameters(info.getStorage().getStorageFormat().getSerdelibParametersMap());

        return builder.build();
    }

    public static alluxio.grpc.table.layout.hive.PartitionInfo toPartitionInfo(alluxio.grpc.table.Partition part)
    {
        try {
            return alluxio.table.ProtoUtils.extractHiveLayout(part);
        }
        catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Failed to extract PartitionInfo", e);
        }
    }

    public static List<alluxio.grpc.table.layout.hive.PartitionInfo> toPartitionInfoList(List<alluxio.grpc.table.Partition> parts)
    {
        return parts.stream().map(ProtoUtils::toPartitionInfo).collect(Collectors.toList());
    }
}
