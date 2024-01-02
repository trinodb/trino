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
package io.trino.plugin.kudu;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.kudu.properties.ColumnDesign;
import io.trino.plugin.kudu.properties.HashPartitionDefinition;
import io.trino.plugin.kudu.properties.KuduTableProperties;
import io.trino.plugin.kudu.properties.PartitionDesign;
import io.trino.plugin.kudu.properties.RangePartition;
import io.trino.plugin.kudu.properties.RangePartitionDefinition;
import io.trino.spi.connector.ColumnMetadata;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.SessionConfiguration;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.kudu.properties.KuduTableProperties.PARTITION_BY_HASH_BUCKETS;
import static io.trino.plugin.kudu.properties.KuduTableProperties.PARTITION_BY_HASH_COLUMNS;
import static io.trino.plugin.kudu.properties.KuduTableProperties.PARTITION_BY_RANGE_COLUMNS;
import static io.trino.plugin.kudu.properties.KuduTableProperties.PRIMARY_KEY;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;

public abstract class KuduTestTable
{
    private KuduTestTable() {}

    public static void create(KuduClient kuduClient, String tableName, List<KuduTestColumn> rows)
    {
        createTestTable(kuduClient, tableName, rows);
        addRows(kuduClient, tableName, rows);
    }

    private static void addRows(KuduClient kuduClient, String tableName, List<KuduTestColumn> rows)
    {
        try {
            KuduTable kuduTable = kuduClient.openTable(tableName);
            KuduSession session = kuduClient.newSession();
            session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
            Insert insert = kuduTable.newInsert();
            PartialRow kuduRow = insert.getRow();
            int colIndex = 0;
            for (KuduTestColumn testRow : rows) {
                if (BIGINT.equals(testRow.getTrinoType())) {
                    kuduRow.addLong(colIndex, Long.parseLong(String.valueOf(testRow.getColumnValue())));
                }
                else if (VARCHAR.equals(testRow.getTrinoType())) {
                    kuduRow.addString(colIndex, String.valueOf(testRow.getColumnValue()));
                }
                colIndex++;
            }
            session.apply(insert);
            session.close();
        }
        catch (KuduException ignore) {
        }
    }

    private static void createTestTable(KuduClient kuduClient, String tableName, List<KuduTestColumn> kuduTestColumns)
    {
        try {
            ImmutableList.Builder<ColumnMetadata> trinoColumns = ImmutableList.builder();
            for (KuduTestColumn kuduTestColumn : kuduTestColumns) {
                ImmutableMap.Builder<String, Object> primaryKeyBuilder = ImmutableMap.builder();
                primaryKeyBuilder.put(PRIMARY_KEY, true);
                ColumnMetadata columnMetadata = ColumnMetadata.builder()
                        .setName(kuduTestColumn.getColumnName())
                        .setType(kuduTestColumn.getTrinoType())
                        .setProperties(kuduTestColumn.isPrimaryKey() ? primaryKeyBuilder.buildOrThrow() : ImmutableMap.of())
                        .build();
                trinoColumns.add(columnMetadata);
            }
            Schema schema = buildSchema(trinoColumns.build());
            ImmutableMap.Builder<String, Object> kuduTablePropertiesBuilder = ImmutableMap.builder();
            // this requires us to always have a column named id
            Map<String, Object> kuduTableProperties = kuduTablePropertiesBuilder.put(PARTITION_BY_HASH_COLUMNS, ImmutableList.of("id"))
                    .put(PARTITION_BY_HASH_BUCKETS, 2)
                    .put(PARTITION_BY_RANGE_COLUMNS, ImmutableList.of())
                    .buildOrThrow();
            CreateTableOptions options = buildCreateTableOptions(schema, kuduTableProperties);
            kuduClient.createTable(tableName, schema, options);
        }
        catch (KuduException ignore) {
        }
    }

    private static Schema buildSchema(List<ColumnMetadata> columns)
    {
        List<ColumnSchema> kuduColumns = columns.stream()
                .map(KuduTestTable::toColumnSchema)
                .collect(toImmutableList());
        return new Schema(kuduColumns);
    }

    private static ColumnSchema toColumnSchema(ColumnMetadata columnMetadata)
    {
        String name = columnMetadata.getName();
        ColumnDesign design = KuduTableProperties.getColumnDesign(columnMetadata.getProperties());
        Type ktype = TypeHelper.toKuduClientType(columnMetadata.getType());
        ColumnSchema.ColumnSchemaBuilder builder = new ColumnSchema.ColumnSchemaBuilder(name, ktype);
        builder.key(design.isPrimaryKey()).nullable(design.isNullable());
        return builder.build();
    }

    private static CreateTableOptions buildCreateTableOptions(Schema schema, Map<String, Object> properties)
    {
        CreateTableOptions options = new CreateTableOptions();

        RangePartitionDefinition rangePartitionDefinition = null;
        PartitionDesign partitionDesign = KuduTableProperties.getPartitionDesign(properties);
        if (partitionDesign.getHash() != null) {
            for (HashPartitionDefinition partition : partitionDesign.getHash()) {
                options.addHashPartitions(partition.getColumns(), partition.getBuckets());
            }
        }
        //if (partitionDesign.getRange() != null) {
        //    rangePartitionDefinition = partitionDesign.getRange();
        //    options.setRangePartitionColumns(rangePartitionDefinition.getColumns());
        //}

        List<RangePartition> rangePartitions = KuduTableProperties.getRangePartitions(properties);
        if (rangePartitionDefinition != null && !rangePartitions.isEmpty()) {
            for (RangePartition rangePartition : rangePartitions) {
                PartialRow lower = KuduTableProperties.toRangeBoundToPartialRow(schema, rangePartitionDefinition, rangePartition.getLower());
                PartialRow upper = KuduTableProperties.toRangeBoundToPartialRow(schema, rangePartitionDefinition, rangePartition.getUpper());
                options.addRangePartition(lower, upper);
            }
        }

        Optional<Integer> numReplicas = KuduTableProperties.getNumReplicas(properties);
        numReplicas.ifPresent(options::setNumReplicas);

        return options;
    }
}
