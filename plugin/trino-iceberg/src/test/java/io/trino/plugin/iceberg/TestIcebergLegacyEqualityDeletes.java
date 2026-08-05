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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.plugin.iceberg.util.EqualityDeleteUtils.writeEqualityDeleteForTable;
import static io.trino.plugin.iceberg.util.EqualityDeleteUtils.writeEqualityDeleteForTableWithSchema;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.tpch.TpchTable.NATION;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Copy of equality delete tests from TestIcebergV2 but with iceberg.equality-deletes-blocks-hash-enabled=false
 * Remove when iceberg.equality-deletes-blocks-hash-enabled is removed
 */
public class TestIcebergLegacyEqualityDeletes
        extends AbstractTestQueryFramework
{
    private HiveMetastore metastore;
    private TrinoFileSystemFactory fileSystemFactory;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session icebergSession = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .setSchema("tpch")
                .build();

        QueryRunner queryRunner = DistributedQueryRunner.builder(icebergSession).build();

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        Path dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data");
        verify(dataDirectory.toFile().mkdirs());

        queryRunner.installPlugin(new TestingIcebergPlugin(dataDirectory));
        queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg", Map.of(
                "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                "hive.metastore.catalog.dir", dataDirectory.toString(),
                "fs.hadoop.enabled", "true",
                "iceberg.equality-deletes-blocks-hash-enabled", "false"));

        metastore = getHiveMetastore(queryRunner);
        fileSystemFactory = getFileSystemFactory(queryRunner);

        queryRunner.execute("CREATE SCHEMA tpch");
        copyTpchTables(queryRunner, "tpch", "tiny", List.of(NATION));

        return queryRunner;
    }

    @Test
    public void testMultipleEqualityDeletes()
            throws Exception
    {
        try (TestTable table = newTrinoTable("test_multiple_equality_deletes_", "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();
            Table icebergTable = loadTable(tableName);
            assertThat(icebergTable.currentSnapshot().summary()).containsEntry("total-equality-deletes", "0");

            for (int i = 1; i < 3; i++) {
                writeEqualityDeleteToNationTable(
                        icebergTable,
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableMap.of("regionkey", Integer.toUnsignedLong(i)));
            }

            assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation WHERE  (regionkey != 1L AND regionkey != 2L)");
        }
    }

    @Test
    public void testEqualityDeleteAppliesOnlyToCorrectDataVersion()
            throws Exception
    {
        try (TestTable table = newTrinoTable("test_multiple_equality_deletes_", "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();
            Table icebergTable = loadTable(tableName);
            assertThat(icebergTable.currentSnapshot().summary().get("total-equality-deletes")).isEqualTo("0");

            for (int i = 1; i < 3; i++) {
                writeEqualityDeleteToNationTable(
                        icebergTable,
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableMap.of("regionkey", Integer.toUnsignedLong(i)));
            }

            assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation WHERE  (regionkey != 1L AND regionkey != 2L)");

            // Reinsert the data for regionkey = 1. This should insert the data with a larger datasequence number and the delete file should not apply to it anymore.
            // Also delete something again so that the split has deletes and the delete logic is activated.
            assertUpdate("INSERT INTO " + tableName + " SELECT * FROM tpch.tiny.nation WHERE regionkey = 1", 5);
            writeEqualityDeleteToNationTable(
                    icebergTable,
                    Optional.empty(),
                    Optional.empty(),
                    ImmutableMap.of("regionkey", Integer.toUnsignedLong(3)));

            assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation WHERE (regionkey != 2L AND regionkey != 3L)");
        }
    }

    @Test
    public void testMultipleEqualityDeletesWithEquivalentSchemas()
            throws Exception
    {
        try (TestTable table = newTrinoTable("test_multiple_equality_deletes_equivalent_schemas_", "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();
            Table icebergTable = loadTable(tableName);
            assertThat(icebergTable.currentSnapshot().summary()).containsEntry("total-equality-deletes", "0");
            Schema deleteRowSchema = new Schema(ImmutableList.of("regionkey", "name").stream()
                    .map(name -> icebergTable.schema().findField(name))
                    .collect(toImmutableList()));
            List<Integer> equalityFieldIds = ImmutableList.of("regionkey", "name").stream()
                    .map(name -> deleteRowSchema.findField(name).fieldId())
                    .collect(toImmutableList());
            writeEqualityDeleteToNationTableWithDeleteColumns(
                    icebergTable,
                    Optional.empty(),
                    Optional.empty(),
                    ImmutableMap.of("regionkey", 1L, "name", "BRAZIL"),
                    deleteRowSchema,
                    equalityFieldIds);
            Schema equivalentDeleteRowSchema = new Schema(ImmutableList.of("name", "regionkey").stream()
                    .map(name -> icebergTable.schema().findField(name))
                    .collect(toImmutableList()));
            writeEqualityDeleteToNationTableWithDeleteColumns(
                    icebergTable,
                    Optional.empty(),
                    Optional.empty(),
                    ImmutableMap.of("name", "INDIA", "regionkey", 2L),
                    equivalentDeleteRowSchema,
                    equalityFieldIds);

            assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation WHERE NOT ((regionkey = 1 AND name = 'BRAZIL') OR (regionkey = 2 AND name = 'INDIA'))");
        }
    }

    @Test
    public void testMultipleEqualityDeletesWithDifferentSchemas()
            throws Exception
    {
        try (TestTable table = newTrinoTable("test_multiple_equality_deletes_different_schemas_", "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();
            Table icebergTable = loadTable(tableName);
            assertThat(icebergTable.currentSnapshot().summary()).containsEntry("total-equality-deletes", "0");
            writeEqualityDeleteToNationTableWithDeleteColumns(
                    icebergTable,
                    Optional.empty(),
                    Optional.empty(),
                    ImmutableMap.of("regionkey", 1L, "name", "BRAZIL"),
                    Optional.of(ImmutableList.of("regionkey", "name")));
            writeEqualityDeleteToNationTableWithDeleteColumns(
                    icebergTable,
                    Optional.empty(),
                    Optional.empty(),
                    ImmutableMap.of("name", "ALGERIA"),
                    Optional.of(ImmutableList.of("name")));
            writeEqualityDeleteToNationTableWithDeleteColumns(
                    icebergTable,
                    Optional.empty(),
                    Optional.empty(),
                    ImmutableMap.of("regionkey", 2L),
                    Optional.of(ImmutableList.of("regionkey")));

            assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation WHERE NOT ((regionkey = 1 AND name = 'BRAZIL') OR regionkey = 2 OR name = 'ALGERIA')");
        }
    }

    @Test
    public void testEqualityDeletesAcrossPartitions()
            throws Exception
    {
        try (TestTable table = newTrinoTable("test_equality_deletes_across_partitions_", "WITH (partitioning = ARRAY['partition']) AS SELECT 'part_1' as partition, * FROM tpch.tiny.nation")) {
            String tableName = table.getName();
            assertUpdate("INSERT INTO " + tableName + " SELECT 'part_2' as partition, * FROM tpch.tiny.nation", 25);
            Table icebergTable = loadTable(tableName);
            PartitionData partitionData1 = PartitionData.fromJson("{\"partitionValues\":[\"part_1\"]}", new Type[] {Types.StringType.get()});
            PartitionData partitionData2 = PartitionData.fromJson("{\"partitionValues\":[\"part_2\"]}", new Type[] {Types.StringType.get()});
            writeEqualityDeleteToNationTableWithDeleteColumns(
                    icebergTable,
                    Optional.of(icebergTable.spec()),
                    Optional.of(partitionData1),
                    ImmutableMap.of("regionkey", 1L),
                    Optional.of(ImmutableList.of("regionkey")));
            // Delete from both partitions so internal code doesn't skip all deletion logic for second partition invalidating this test
            writeEqualityDeleteToNationTableWithDeleteColumns(
                    icebergTable,
                    Optional.of(icebergTable.spec()),
                    Optional.of(partitionData2),
                    ImmutableMap.of("regionkey", 2L),
                    Optional.of(ImmutableList.of("regionkey")));

            assertQuery("SELECT * FROM " + tableName, "SELECT 'part_1', * FROM nation WHERE regionkey <> 1 UNION ALL select 'part_2', * FROM NATION where regionkey <> 2");
        }
    }

    @Test
    public void testMultipleEqualityDeletesWithNestedFields()
            throws Exception
    {
        try (TestTable table = newTrinoTable("test_multiple_equality_deletes_nested_fields_", "(id BIGINT, root ROW(nested BIGINT, nested_other BIGINT))")) {
            String tableName = table.getName();
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, row(10, 100))", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, row(20, 200))", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, row(20, 200))", 1);
            Table icebergTable = loadTable(tableName);
            assertThat(icebergTable.currentSnapshot().summary()).containsEntry("total-equality-deletes", "0");

            List<String> deleteFileColumns = ImmutableList.of("root.nested");
            Schema deleteRowSchema = icebergTable.schema().select(deleteFileColumns);
            List<Integer> equalityFieldIds = ImmutableList.of("root.nested").stream()
                    .map(name -> deleteRowSchema.findField(name).fieldId())
                    .collect(toImmutableList());
            Types.StructType nestedStructType = (Types.StructType) deleteRowSchema.findField("root").type();
            Record nestedStruct = GenericRecord.create(nestedStructType);
            nestedStruct.setField("nested", 20L);
            for (int i = 1; i < 3; i++) {
                writeEqualityDeleteToNationTableWithDeleteColumns(
                        icebergTable,
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableMap.of("root", nestedStruct),
                        deleteRowSchema,
                        equalityFieldIds);
            }

            assertThat(query("SELECT * FROM " + tableName))
                    .matches("VALUES (BIGINT '1', CAST(row(10, 100) AS ROW(nested BIGINT, nested_other BIGINT)))");

            // verify that the equality delete is effective also when not specifying the corresponding column in the projection list
            assertThat(query("SELECT id FROM " + tableName))
                    .matches("VALUES BIGINT '1'");
        }
    }

    @Test
    public void testOptimizingWholeTableRemovesEqualityDeletes()
            throws Exception
    {
        try (TestTable table = newTrinoTable("test_optimizing_whole_table_removes_eq_deletes_", "WITH (partitioning = ARRAY['nationkey']) AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();
            Table icebergTable = loadTable(tableName);
            writeEqualityDeleteToNationTable(icebergTable, Optional.of(icebergTable.spec()), Optional.of(new PartitionData(new Long[] {1L})));
            assertUpdate("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");
            assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation WHERE regionkey != 1 OR nationkey != 1");
            assertThat(loadTable(tableName).currentSnapshot().summary()).containsEntry("total-equality-deletes", "0");
        }
    }

    private void writeEqualityDeleteToNationTable(Table icebergTable, Optional<PartitionSpec> partitionSpec, Optional<PartitionData> partitionData)
            throws Exception
    {
        writeEqualityDeleteToNationTable(icebergTable, partitionSpec, partitionData, ImmutableMap.of("regionkey", 1L));
    }

    private void writeEqualityDeleteToNationTable(
            Table icebergTable,
            Optional<PartitionSpec> partitionSpec,
            Optional<PartitionData> partitionData,
            Map<String, Object> overwriteValues)
            throws Exception
    {
        writeEqualityDeleteToNationTableWithDeleteColumns(icebergTable, partitionSpec, partitionData, overwriteValues, Optional.empty());
    }

    private void writeEqualityDeleteToNationTableWithDeleteColumns(
            Table icebergTable,
            Optional<PartitionSpec> partitionSpec,
            Optional<PartitionData> partitionData,
            Map<String, Object> overwriteValues,
            Optional<List<String>> deleteFileColumns)
            throws Exception
    {
        writeEqualityDeleteForTable(icebergTable, fileSystemFactory, partitionSpec, partitionData, overwriteValues, deleteFileColumns);
    }

    private void writeEqualityDeleteToNationTableWithDeleteColumns(
            Table icebergTable,
            Optional<PartitionSpec> partitionSpec,
            Optional<PartitionData> partitionData,
            Map<String, Object> overwriteValues,
            Schema deleteRowSchema,
            List<Integer> equalityDeleteFieldIds)
            throws Exception
    {
        writeEqualityDeleteForTableWithSchema(icebergTable, fileSystemFactory, partitionSpec, partitionData, deleteRowSchema, equalityDeleteFieldIds, overwriteValues);
    }

    private BaseTable loadTable(String tableName)
    {
        return IcebergTestUtils.loadTable(tableName, metastore, fileSystemFactory, "hive", "tpch");
    }
}
