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
package io.trino.metastore.lakehouse;

import com.google.cloud.biglake.hive.v1.FieldSchema;
import com.google.cloud.biglake.hive.v1.HiveDatabase;
import com.google.cloud.biglake.hive.v1.HiveTable;
import com.google.cloud.biglake.hive.v1.NamespaceName;
import com.google.cloud.biglake.hive.v1.Partition;
import com.google.cloud.biglake.hive.v1.SerdeInfo;
import com.google.cloud.biglake.hive.v1.StorageDescriptor;
import com.google.cloud.biglake.hive.v1.TableName;

import java.util.List;

/**
 * Builders for the Lakehouse messages a fake client hands back.
 */
public final class TestingLakehouseObjects
{
    public static final String PROJECT = "example-project";
    public static final String CATALOG = "example-catalog";

    public static final String PARQUET_SERDE = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";
    public static final String PARQUET_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
    public static final String PARQUET_OUTPUT_FORMAT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat";

    private TestingLakehouseObjects() {}

    public static String databaseName(HiveDatabase database)
    {
        return NamespaceName.parse(database.getName()).getDatabase();
    }

    public static String tableName(HiveTable table)
    {
        return TableName.parse(table.getName()).getTable();
    }

    public static HiveDatabase database(String databaseName)
    {
        return HiveDatabase.newBuilder()
                .setName(NamespaceName.format(PROJECT, CATALOG, databaseName))
                .setLocationUri("gs://warehouse/" + databaseName + ".db")
                .build();
    }

    public static FieldSchema field(String name, String type)
    {
        return FieldSchema.newBuilder().setName(name).setType(type).build();
    }

    public static StorageDescriptor storage(String location, List<FieldSchema> columns)
    {
        return StorageDescriptor.newBuilder()
                .setLocationUri(location)
                .setInputFormat(PARQUET_INPUT_FORMAT)
                .setOutputFormat(PARQUET_OUTPUT_FORMAT)
                .setSerdeInfo(SerdeInfo.newBuilder().setSerializationLib(PARQUET_SERDE))
                .addAllColumns(columns)
                .build();
    }

    public static HiveTable table(
            String databaseName,
            String tableName,
            String description,
            List<FieldSchema> columns,
            List<FieldSchema> partitionKeys)
    {
        return HiveTable.newBuilder()
                .setName(TableName.format(PROJECT, CATALOG, databaseName, tableName))
                .setDescription(description)
                .setTableType("EXTERNAL_TABLE")
                .setStorageDescriptor(storage("gs://warehouse/" + tableName, columns))
                .addAllPartitionKeys(partitionKeys)
                .build();
    }

    public static Partition partition(String tableName, List<String> values)
    {
        return Partition.newBuilder()
                .addAllValues(values)
                .setStorageDescriptor(storage("gs://warehouse/" + tableName + "/" + String.join("/", values), List.of()))
                .build();
    }
}
