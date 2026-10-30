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
package io.trino.plugin.hive.metastore.lakehouse;

import com.google.cloud.biglake.hive.v1.FieldSchema;
import com.google.cloud.biglake.hive.v1.HiveDatabase;
import com.google.cloud.biglake.hive.v1.HiveTable;
import com.google.cloud.biglake.hive.v1.NamespaceName;
import com.google.cloud.biglake.hive.v1.Partition;
import com.google.cloud.biglake.hive.v1.SerdeInfo;
import com.google.cloud.biglake.hive.v1.StorageDescriptor;
import com.google.cloud.biglake.hive.v1.TableName;
import com.google.common.collect.ImmutableList;
import io.trino.metastore.lakehouse.LakehouseClient;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;

/**
 * A fixed Lakehouse catalog, standing in for the service while a query runs.
 *
 * <p>It answers the reads a query makes and holds nothing else, which is what a metastore that only
 * reads ever asks Lakehouse for.
 */
final class TestingLakehouseCatalog
        implements LakehouseClient
{
    static final String PROJECT = "testing-project";
    static final String CATALOG = "testing-catalog";
    static final String SCHEMA = "sales";
    static final String UNPARTITIONED_TABLE = "orders";
    static final String PARTITIONED_TABLE = "regional_orders";

    private static final String TEXT_SERDE = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
    private static final String TEXT_INPUT_FORMAT = "org.apache.hadoop.mapred.TextInputFormat";
    private static final String TEXT_OUTPUT_FORMAT = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";

    private static final List<String> REGIONS = ImmutableList.of("emea", "apac");

    @Override
    public List<HiveDatabase> listDatabases()
    {
        return ImmutableList.of(database());
    }

    @Override
    public Optional<HiveDatabase> getDatabase(String databaseName)
    {
        return SCHEMA.equals(databaseName) ? Optional.of(database()) : Optional.empty();
    }

    @Override
    public List<HiveTable> listTables(String databaseName)
    {
        if (!SCHEMA.equals(databaseName)) {
            return ImmutableList.of();
        }
        return ImmutableList.of(unpartitionedTable(), partitionedTable());
    }

    @Override
    public Optional<HiveTable> getTable(String databaseName, String tableName)
    {
        return listTables(databaseName).stream()
                .filter(table -> TableName.parse(table.getName()).getTable().equals(tableName))
                .findFirst();
    }

    @Override
    public Optional<List<Partition>> listPartitions(String databaseName, String tableName, String filter)
    {
        if (getTable(databaseName, tableName).isEmpty()) {
            return Optional.empty();
        }
        if (!PARTITIONED_TABLE.equals(tableName)) {
            return Optional.of(ImmutableList.of());
        }
        return Optional.of(REGIONS.stream()
                .filter(region -> filter.isEmpty() || filter.contains("'" + region + "'"))
                .map(TestingLakehouseCatalog::partition)
                .collect(toImmutableList()));
    }

    private static HiveDatabase database()
    {
        return HiveDatabase.newBuilder()
                .setName(NamespaceName.format(PROJECT, CATALOG, SCHEMA))
                .setLocationUri("local:///" + SCHEMA)
                .build();
    }

    private static HiveTable unpartitionedTable()
    {
        return table(UNPARTITIONED_TABLE, "local:///" + UNPARTITIONED_TABLE, ImmutableList.of());
    }

    private static HiveTable partitionedTable()
    {
        return table(
                PARTITIONED_TABLE,
                "local:///" + PARTITIONED_TABLE,
                ImmutableList.of(field("region", "string")));
    }

    private static HiveTable table(String tableName, String location, List<FieldSchema> partitionKeys)
    {
        return HiveTable.newBuilder()
                .setName(TableName.format(PROJECT, CATALOG, SCHEMA, tableName))
                .setTableType("EXTERNAL_TABLE")
                .setStorageDescriptor(storage(location))
                .addAllPartitionKeys(partitionKeys)
                .build();
    }

    private static Partition partition(String region)
    {
        return Partition.newBuilder()
                .addValues(region)
                .setStorageDescriptor(storage("local:///" + PARTITIONED_TABLE + "/region=" + region))
                .build();
    }

    private static StorageDescriptor storage(String location)
    {
        return StorageDescriptor.newBuilder()
                .setLocationUri(location)
                .setInputFormat(TEXT_INPUT_FORMAT)
                .setOutputFormat(TEXT_OUTPUT_FORMAT)
                .setSerdeInfo(SerdeInfo.newBuilder().setSerializationLib(TEXT_SERDE))
                .addColumns(field("order_id", "bigint"))
                .addColumns(field("item", "string"))
                .build();
    }

    private static FieldSchema field(String name, String type)
    {
        return FieldSchema.newBuilder().setName(name).setType(type).build();
    }
}
