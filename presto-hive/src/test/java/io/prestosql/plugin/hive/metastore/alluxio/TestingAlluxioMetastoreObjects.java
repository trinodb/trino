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

import alluxio.grpc.table.Database;
import alluxio.grpc.table.FieldSchema;
import alluxio.grpc.table.Layout;
import alluxio.grpc.table.LayoutSpec;
import alluxio.grpc.table.Partition;
import alluxio.grpc.table.PartitionSpec;
import alluxio.grpc.table.Schema;
import alluxio.grpc.table.TableInfo;
import alluxio.grpc.table.layout.hive.HiveBucketProperty;
import alluxio.grpc.table.layout.hive.PartitionInfo;
import alluxio.grpc.table.layout.hive.SortingColumn;
import alluxio.grpc.table.layout.hive.Storage;
import alluxio.grpc.table.layout.hive.StorageFormat;
import alluxio.shaded.client.com.google.protobuf.ByteString;

public final class TestingAlluxioMetastoreObjects
{
    private TestingAlluxioMetastoreObjects() {}

    public static final String DATABASE_NAME = "test_db";
    public static final String OWNER_NAME = "test_owner";
    public static final String COLUMN_NAME = "test_owner";
    public static final String TABLE_NAME = "test_table";
    public static final String LOCATION = "alluxio:///";
    public static final String SPEC_NAME = "spec";

    public static Database.Builder getTestingDatabase()
    {
        return alluxio.grpc.table.Database.newBuilder()
                .setDbName(DATABASE_NAME)
                .setDescription("test")
                .setLocation(LOCATION);
    }

    public static FieldSchema.Builder getTestingFieldSchema()
    {
        return FieldSchema.newBuilder()
                .setId(0)
                .setName(COLUMN_NAME)
                .setType("int")
                .setComment("");
    }

    public static PartitionInfo.Builder getTestingPartitionInfo()
    {
        return PartitionInfo.newBuilder()
                .setDbName(DATABASE_NAME)
                .setTableName(TABLE_NAME)
                .addValues("1")
                .setPartitionName(String.format("%s=1", COLUMN_NAME))
                .setStorage(getTestingStorage())
                .addDataCols(getTestingFieldSchema());
    }

    public static Layout.Builder getTestingHiveLayout()
    {
        return Layout.newBuilder()
                .setLayoutSpec(LayoutSpec.newBuilder().setSpec(SPEC_NAME).build())
                .setLayoutData(getTestingPartitionInfo().build().toByteString())
                .setLayoutType("hive");
    }

    public static Layout.Builder getTestingNonHiveLayout()
    {
        return Layout.newBuilder()
                .setLayoutData(ByteString.EMPTY)
                .setLayoutSpec(LayoutSpec.newBuilder().setSpec(SPEC_NAME).build())
                .setLayoutType("not-hive");
    }

    public static TableInfo.Builder getTestingTableInfo()
    {
        return TableInfo.newBuilder()
                .setLayout(getTestingHiveLayout())
                .setTableName(TABLE_NAME)
                .setOwner(OWNER_NAME)
                .setType(TableInfo.TableType.IMPORTED)
                // Single column partition, no data columns
                .addPartitionCols(getTestingFieldSchema())
                .setSchema(getTestingSchema())
                .putParameters("table", "parameter");
    }

    public static Schema.Builder getTestingSchema()
    {
        return Schema.newBuilder()
                .addCols(getTestingFieldSchema());
    }

    public static Storage.Builder getTestingStorage()
    {
        return Storage.newBuilder()
                .setStorageFormat(getTestingStorageFormat())
                .setLocation(LOCATION)
                .setBucketProperty(getTestingHiveBucketProperty())
                .setSkewed(false)
                .putSerdeParameters("serde_param_key", "serde_param_value");
    }

    public static StorageFormat.Builder getTestingStorageFormat()
    {
        return StorageFormat.newBuilder()
                .setSerde("serde")
                .setInputFormat("TextFile")
                .setOutputFormat("TextFile")
                .putSerdelibParameters("serdelib_key", "serdelib_value");
    }

    public static HiveBucketProperty.Builder getTestingHiveBucketProperty()
    {
        return HiveBucketProperty.newBuilder()
                .addBucketedBy(COLUMN_NAME)
                .setBucketCount(1)
                .addSortedBy(getTestingSortingColumn());
    }

    public static SortingColumn.Builder getTestingSortingColumn()
    {
        return SortingColumn.newBuilder()
                .setColumnName(COLUMN_NAME)
                .setOrder(SortingColumn.SortingOrder.ASCENDING);
    }

    public static Partition.Builder getTestingPartition()
    {
        return Partition.newBuilder()
                .setBaseLayout(getTestingHiveLayout())
                .setPartitionSpec(getTestingPartitionSpec());
    }

    public static PartitionSpec.Builder getTestingPartitionSpec()
    {
        return PartitionSpec.newBuilder()
                .setSpec(SPEC_NAME);
    }
}
