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
package io.trino.plugin.hive.metastore.glue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.metastore.Column;
import io.trino.metastore.Partition;
import io.trino.metastore.StorageFormat;
import io.trino.metastore.Table;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.metastore.HiveType.HIVE_INT;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.HiveStorageFormat.AVRO;
import static io.trino.plugin.hive.HiveStorageFormat.PARQUET;
import static io.trino.plugin.hive.metastore.glue.GlueHiveMetastore.withAvroSchemaColumns;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Partition-level counterpart of {@code GlueAvroSchemaResolver}'s table-level resolution. Glue's stored
 * per-partition columns are a separate, independently-stale copy from the table's stored columns — resolving the
 * table alone (as {@code GlueAvroSchemaResolver} does) is not sufficient once a table's Avro schema has evolved,
 * because {@code HiveSplitManager} then rejects the partition when the newly-resolved table columns are not
 * coercible from the partition's still-stale stored columns.
 */
final class TestGlueHiveMetastoreAvroPartitionColumns
{
    // What the table resolves to once its columns come from the current Avro schema
    private static final List<Column> RESOLVED_TABLE_COLUMNS = ImmutableList.of(
            new Column("event_id", HIVE_STRING, Optional.empty(), ImmutableMap.of()),
            new Column("amount", HIVE_INT, Optional.empty(), ImmutableMap.of()));

    // What Glue has stored for an old partition: stale, from before the schema evolved
    private static final List<Column> STALE_PARTITION_COLUMNS = ImmutableList.of(
            new Column("event_id", HIVE_STRING, Optional.empty(), ImmutableMap.of()));

    @Test
    void testReplacesPartitionColumnsForAvroTableWithSchemaSet()
    {
        Table table = avroTable(RESOLVED_TABLE_COLUMNS, ImmutableMap.of("avro.schema.literal", "irrelevant-for-this-test"));
        Partition partition = partition(STALE_PARTITION_COLUMNS);

        Partition resolved = withAvroSchemaColumns(table, partition);

        assertThat(resolved.getColumns()).isEqualTo(RESOLVED_TABLE_COLUMNS);
    }

    @Test
    void testLeavesPartitionColumnsUnchangedWhenTableHasNoAvroSchemaSet()
    {
        Table table = avroTable(RESOLVED_TABLE_COLUMNS, ImmutableMap.of());
        Partition partition = partition(STALE_PARTITION_COLUMNS);

        Partition resolved = withAvroSchemaColumns(table, partition);

        assertThat(resolved.getColumns()).isEqualTo(STALE_PARTITION_COLUMNS);
    }

    @Test
    void testLeavesPartitionColumnsUnchangedForNonAvroTable()
    {
        Table table = Table.builder(avroTable(RESOLVED_TABLE_COLUMNS, ImmutableMap.of("avro.schema.literal", "irrelevant-for-this-test")))
                .withStorage(storage -> storage.setStorageFormat(PARQUET.toStorageFormat()))
                .build();
        Partition partition = partition(STALE_PARTITION_COLUMNS);

        Partition resolved = withAvroSchemaColumns(table, partition);

        assertThat(resolved.getColumns()).isEqualTo(STALE_PARTITION_COLUMNS);
    }

    private static Table avroTable(List<Column> dataColumns, Map<String, String> parameters)
    {
        return Table.builder()
                .setDatabaseName("test_db")
                .setTableName("test_table")
                .setOwner(Optional.empty())
                .setTableType("EXTERNAL_TABLE")
                .setDataColumns(dataColumns)
                .setParameters(parameters)
                .withStorage(storage -> storage
                        .setStorageFormat(AVRO.toStorageFormat())
                        .setLocation("/tmp/test_db/test_table"))
                .build();
    }

    private static Partition partition(List<Column> columns)
    {
        return Partition.builder()
                .setDatabaseName("test_db")
                .setTableName("test_table")
                .setValues(ImmutableList.of("2021-10-31", "20"))
                .setColumns(columns)
                .withStorage(storage -> storage
                        .setStorageFormat(StorageFormat.create(AVRO.getSerde(), AVRO.getInputFormat(), AVRO.getOutputFormat()))
                        .setLocation("/tmp/test_db/test_table/acquisition_date=2021-10-31/acquisition_hour=20"))
                .build();
    }
}
