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
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.Column;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveType;
import io.trino.metastore.PrincipalPrivileges;
import io.trino.metastore.Storage;
import io.trino.plugin.base.util.Closables;
import io.trino.plugin.blackhole.BlackHolePlugin;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.iceberg.fileio.ForwardingFileIo;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.DoubleRange;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.TypeManager;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortField;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.util.EqualityDeleteUtils.writeEqualityDeleteForTable;
import static io.trino.plugin.iceberg.util.EqualityDeleteUtils.writeEqualityDeleteForTableWithSchema;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tpch.TpchTable.NATION;
import static java.lang.String.format;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.iceberg.FileFormat.ORC;
import static org.apache.iceberg.FileFormat.PARQUET;
import static org.apache.iceberg.TableProperties.DEFAULT_NAME_MAPPING;
import static org.apache.iceberg.TableProperties.SPLIT_SIZE;
import static org.apache.iceberg.mapping.NameMappingParser.toJson;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestIcebergV2
        extends AbstractTestQueryFramework
{
    private HiveMetastore metastore;
    private TrinoFileSystemFactory fileSystemFactory;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = IcebergQueryRunner.builder()
                .setInitialTables(NATION)
                .build();

        metastore = ((IcebergConnector) queryRunner.getCoordinator().getConnector(ICEBERG_CATALOG)).getInjector()
                .getInstance(HiveMetastoreFactory.class)
                .createMetastore(Optional.empty());

        queryRunner.installPlugin(new TestingHivePlugin(queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data")));
        queryRunner.createCatalog("hive", "hive", ImmutableMap.<String, String>builder()
                .put("hive.security", "allow-all")
                .buildOrThrow());

        try {
            queryRunner.installPlugin(new BlackHolePlugin());
            queryRunner.createCatalog("blackhole", "blackhole");
        }
        catch (RuntimeException e) {
            Closables.closeAllSuppress(e, queryRunner);
            throw e;
        }

        return queryRunner;
    }

    @BeforeAll
    public void initFileSystemFactory()
    {
        fileSystemFactory = getFileSystemFactory(getDistributedQueryRunner());
    }

    @Test
    public void testSettingFormatVersion()
    {
        String tableName = "test_seting_format_version_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (format_version = 2) AS SELECT * FROM tpch.tiny.nation", 25);
        assertThat(loadTable(tableName).operations().current().formatVersion()).isEqualTo(2);
        assertUpdate("DROP TABLE " + tableName);

        assertUpdate("CREATE TABLE " + tableName + " WITH (format_version = 1) AS SELECT * FROM tpch.tiny.nation", 25);
        assertThat(loadTable(tableName).operations().current().formatVersion()).isEqualTo(1);
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDefaultFormatVersion()
    {
        String tableName = "test_default_format_version_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.tiny.nation", 25);
        assertThat(loadTable(tableName).operations().current().formatVersion()).isEqualTo(2);
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testV2TableRead()
    {
        String tableName = "test_v2_table_read" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (format_version = 1) AS SELECT * FROM tpch.tiny.nation", 25);
        updateTableToV2(tableName);
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation");
    }

    @Test
    public void testV2TableWithPositionDelete()
            throws Exception
    {
        String tableName = "test_v2_row_delete" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.tiny.nation", 25);
        Table icebergTable = loadTable(tableName);

        String dataFilePath = (String) computeActual("SELECT file_path FROM \"" + tableName + "$files\" LIMIT 1").getOnlyValue();

        FileIO fileIo = new ForwardingFileIo(fileSystemFactory.create(SESSION));

        PositionDeleteWriter<Record> writer = Parquet.writeDeletes(fileIo.newOutputFile("local:///delete_file_" + UUID.randomUUID()))
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .forTable(icebergTable)
                .overwrite()
                .rowSchema(icebergTable.schema())
                .withSpec(PartitionSpec.unpartitioned())
                .buildPositionWriter();

        PositionDelete<Record> positionDelete = PositionDelete.create();
        PositionDelete<Record> record = positionDelete.set(dataFilePath, 0, GenericRecord.create(icebergTable.schema()));
        try (Closeable ignored = writer) {
            writer.write(record);
        }

        icebergTable.newRowDelta().addDeletes(writer.toDeleteFile()).commit();
        assertQuery("SELECT count(*) FROM " + tableName, "VALUES 24");
    }

    @Test
    public void testV2TableWithEqualityDelete()
            throws Exception
    {
        String tableName = "test_v2_equality_delete" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.tiny.nation", 25);
        Table icebergTable = loadTable(tableName);
        writeEqualityDeleteToNationTable(icebergTable, Optional.of(icebergTable.spec()), Optional.of(new PartitionData(new Long[] {1L})));
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation WHERE regionkey != 1");
        // nationkey is before the equality delete column in the table schema, comment is after
        assertQuery("SELECT nationkey, comment FROM " + tableName, "SELECT nationkey, comment FROM nation WHERE regionkey != 1");

        assertUpdate("INSERT INTO " + tableName + " SELECT * FROM tpch.tiny.nation", 25);
        writeEqualityDeleteToNationTable(icebergTable, Optional.of(icebergTable.spec()), Optional.of(new PartitionData(new Long[] {2L})), ImmutableMap.of("regionkey", 2L));
        // the equality delete file is applied to 2 data files
        assertQuery("SELECT count(*) FROM \"" + tableName + "$files\" WHERE content = " + FileContent.EQUALITY_DELETES.id(), "VALUES 2");
    }

    @Test
    public void testV2TableWithEqualityDeleteDifferentColumnOrder()
            throws Exception
    {
        // Specify equality delete filter with different column order from table definition
        String tableName = "test_v2_equality_delete_different_order" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.tiny.nation", 25);
        Table icebergTable = loadTable(tableName);
        writeEqualityDeleteToNationTable(icebergTable, Optional.of(icebergTable.spec()), Optional.empty(), ImmutableMap.of("regionkey", 1L, "name", "ARGENTINA"));
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation WHERE name != 'ARGENTINA'");
        // nationkey is before the equality delete column in the table schema, comment is after
        assertQuery("SELECT nationkey, comment FROM " + tableName, "SELECT nationkey, comment FROM nation WHERE name != 'ARGENTINA'");
    }

    @Test
    public void testV2TableWithEqualityDeleteWhenColumnIsNested()
            throws Exception
    {
        String tableName = "test_v2_equality_delete_column_nested" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS " +
                "SELECT regionkey, ARRAY[1,2] array_column, MAP(ARRAY[1], ARRAY[2]) map_column, " +
                "CAST(ROW(1, 2e0) AS ROW(x BIGINT, y DOUBLE)) row_column FROM tpch.tiny.nation", 25);
        Table icebergTable = loadTable(tableName);
        writeEqualityDeleteToNationTable(icebergTable, Optional.of(icebergTable.spec()), Optional.of(new PartitionData(new Long[] {1L})));
        assertQuery("SELECT array_column[1], map_column[1], row_column.x FROM " + tableName, "SELECT 1, 2, 1 FROM nation WHERE regionkey != 1");
    }

    @Test
    public void testParquetMissingFieldId()
            throws Exception
    {
        String hiveTableName = "test_hive_parquet" + randomNameSuffix();
        assertUpdate("CREATE TABLE hive.tpch." + hiveTableName + "(names ARRAY(varchar)) WITH (format = 'PARQUET')");
        assertUpdate("INSERT INTO hive.tpch." + hiveTableName + " VALUES ARRAY['Alice', 'Bob'], ARRAY['Carol', 'Dave'], ARRAY['Eve', 'Frank']", 3);

        String location = metastore.getTable("tpch", hiveTableName).orElseThrow().getStorage().getLocation();
        FileIterator files = fileSystemFactory.create(SESSION).listFiles(Location.of(location));
        ImmutableList.Builder<FileEntry> fileEntries = ImmutableList.builder();
        while (files.hasNext()) {
            FileEntry file = files.next();
            if (!file.location().path().contains(".trinoSchema") && !file.location().path().contains(".trinoPermissions")) {
                fileEntries.add(file);
            }
        }
        FileEntry parquetFile = getOnlyElement(fileEntries.build());

        String icebergTableName = "test_iceberg_parquet" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + icebergTableName + "(names ARRAY(varchar))");
        Table icebergTable = loadTable(icebergTableName);
        icebergTable.newAppend()
                .appendFile(DataFiles.builder(icebergTable.spec())
                        .withPath(parquetFile.location().toString())
                        .withFileSizeInBytes(parquetFile.length())
                        .withRecordCount(3)
                        .withFormat(PARQUET).build())
                .commit();
        icebergTable.updateProperties()
                .set("schema.name-mapping.default", "[{\"field-id\":1,\"names\":[\"names\"]}]")
                .commit();

        assertQuery("SELECT * FROM " + icebergTableName, "VALUES ARRAY['Alice', 'Bob'], ARRAY['Carol', 'Dave'], ARRAY['Eve', 'Frank']");

        assertUpdate("DROP TABLE hive.tpch." + hiveTableName);
        assertUpdate("DROP TABLE " + icebergTableName);
    }

    @Test
    public void testOptimizingV2TableRemovesEqualityDeletesWhenWholeTableIsScanned()
            throws Exception
    {
        String tableName = "test_optimize_table_cleans_equality_delete_file_when_whole_table_is_scanned" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (partitioning = ARRAY['regionkey']) AS SELECT * FROM tpch.tiny.nation", 25);
        Table icebergTable = loadTable(tableName);
        assertThat(icebergTable.currentSnapshot().summary()).containsEntry("total-equality-deletes", "0");
        writeEqualityDeleteToNationTable(icebergTable, Optional.of(icebergTable.spec()), Optional.of(new PartitionData(new Long[] {1L})));
        List<String> initialActiveFiles = getActiveFiles(tableName);
        assertUpdate("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation WHERE regionkey != 1");
        // nationkey is before the equality delete column in the table schema, comment is after
        assertQuery("SELECT nationkey, comment FROM " + tableName, "SELECT nationkey, comment FROM nation WHERE regionkey != 1");
        assertThat(loadTable(tableName).currentSnapshot().summary()).containsEntry("total-equality-deletes", "0");
        List<String> updatedFiles = getActiveFiles(tableName);
        assertThat(updatedFiles).doesNotContain(initialActiveFiles.toArray(new String[0]));
    }

    @Test
    public void testOptimizingV2TableDoesntRemoveEqualityDeletesWhenOnlyPartOfTheTableIsOptimized()
            throws Exception
    {
        String tableName = "test_optimize_table_with_equality_delete_file_for_different_partition_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (partitioning = ARRAY['regionkey']) AS SELECT * FROM tpch.tiny.nation", 25);
        Table icebergTable = loadTable(tableName);
        assertThat(icebergTable.currentSnapshot().summary()).containsEntry("total-equality-deletes", "0");
        List<String> initialActiveFiles = getActiveFiles(tableName);
        writeEqualityDeleteToNationTable(icebergTable, Optional.of(icebergTable.spec()), Optional.of(new PartitionData(new Long[] {1L})));
        assertUpdate("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE WHERE regionkey != 1");
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation WHERE regionkey != 1");
        // nationkey is before the equality delete column in the table schema, comment is after
        assertQuery("SELECT nationkey, comment FROM " + tableName, "SELECT nationkey, comment FROM nation WHERE regionkey != 1");
        assertThat(loadTable(tableName).currentSnapshot().summary()).containsEntry("total-equality-deletes", "1");
        List<String> updatedFiles = getActiveFiles(tableName);
        assertThat(updatedFiles).doesNotContain(initialActiveFiles.stream().filter(path -> !path.contains("regionkey=1")).toArray(String[]::new));
    }

    @Test
    public void testSelectivelyOptimizingLeavesEqualityDeletes()
            throws Exception
    {
        String tableName = "test_selectively_optimizing_leaves_eq_deletes_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (partitioning = ARRAY['nationkey']) AS SELECT * FROM tpch.tiny.nation", 25);
        Table icebergTable = loadTable(tableName);
        writeEqualityDeleteToNationTable(icebergTable, Optional.of(icebergTable.spec()), Optional.of(new PartitionData(new Long[] {1L})));
        assertUpdate("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE WHERE nationkey < 5");
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation WHERE regionkey != 1 OR nationkey != 1");
        assertThat(loadTable(tableName).currentSnapshot().summary()).containsEntry("total-equality-deletes", "1");
    }

    @Test
    public void testOptimizePopulateSplitOffsets()
    {
        // For optimize we need to set task_min_writer_count to 1, otherwise it will create more than one file.
        Session session = Session.builder(getSession())
                .setSystemProperty("task_min_writer_count", "1")
                .build();

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_optimize_split_offsets", "AS SELECT * FROM tpch.tiny.nation")) {
            assertUpdate(session, "ALTER TABLE " + table.getName() + " EXECUTE optimize");
            assertThat(computeActual("SELECT split_offsets FROM \"" + table.getName() + "$files\""))
                    .isEqualTo(resultBuilder(getSession(), ImmutableList.of(new ArrayType(BIGINT)))
                            .row(ImmutableList.of(4L))
                            .build());
        }
    }

    @Test
    public void testMultipleEqualityDeletes()
            throws Exception
    {
        String tableName = "test_multiple_equality_deletes_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.tiny.nation", 25);
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
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testEqualityDeleteAppliesOnlyToCorrectDataVersion()
            throws Exception
    {
        String tableName = "test_multiple_equality_deletes_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.tiny.nation", 25);
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
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testMultipleEqualityDeletesWithEquivalentSchemas()
            throws Exception
    {
        String tableName = "test_multiple_equality_deletes_equivalent_schemas_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.tiny.nation", 25);
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
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testMultipleEqualityDeletesWithDifferentSchemas()
            throws Exception
    {
        String tableName = "test_multiple_equality_deletes_different_schemas_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.tiny.nation", 25);
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
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testEqualityDeletesAcrossPartitions()
            throws Exception
    {
        String tableName = "test_equality_deletes_across_partitions_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (partitioning = ARRAY['partition']) AS SELECT 'part_1' as partition, * FROM tpch.tiny.nation", 25);
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
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testMultipleEqualityDeletesWithNestedFields()
            throws Exception
    {
        String tableName = "test_multiple_equality_deletes_nested_fields_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " ( id BIGINT, root ROW(nested BIGINT, nested_other BIGINT))");
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

        // TODO: support read equality deletes with nested fields(https://github.com/trinodb/trino/issues/18625)
        assertThat(query("SELECT * FROM " + tableName)).failure().hasMessageContaining("Multiple entries with same key");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testOptimizingWholeTableRemovesEqualityDeletes()
            throws Exception
    {
        String tableName = "test_optimizing_whole_table_removes_eq_deletes_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (partitioning = ARRAY['nationkey']) AS SELECT * FROM tpch.tiny.nation", 25);
        Table icebergTable = loadTable(tableName);
        writeEqualityDeleteToNationTable(icebergTable, Optional.of(icebergTable.spec()), Optional.of(new PartitionData(new Long[] {1L})));
        assertUpdate("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation WHERE regionkey != 1 OR nationkey != 1");
        assertThat(loadTable(tableName).currentSnapshot().summary()).containsEntry("total-equality-deletes", "0");
    }

    @Test
    public void testOptimizingV2TableWithEmptyPartitionSpec()
            throws Exception
    {
        String tableName = "test_optimize_table_with_global_equality_delete_file_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.tiny.nation", 25);
        Table icebergTable = loadTable(tableName);
        assertThat(icebergTable.currentSnapshot().summary()).containsEntry("total-equality-deletes", "0");
        writeEqualityDeleteToNationTable(icebergTable);
        List<String> initialActiveFiles = getActiveFiles(tableName);
        assertUpdate("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation WHERE regionkey != 1");
        // nationkey is before the equality delete column in the table schema, comment is after
        assertQuery("SELECT nationkey, comment FROM " + tableName, "SELECT nationkey, comment FROM nation WHERE regionkey != 1");
        assertThat(loadTable(tableName).currentSnapshot().summary()).containsEntry("total-equality-deletes", "0");
        List<String> updatedFiles = getActiveFiles(tableName);
        assertThat(updatedFiles).doesNotContain(initialActiveFiles.toArray(new String[0]));
    }

    @Test
    public void testOptimizingPartitionsOfV2TableWithGlobalEqualityDeleteFile()
            throws Exception
    {
        String tableName = "test_optimize_partitioned_table_with_global_equality_delete_file_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (partitioning = ARRAY['regionkey']) AS SELECT * FROM tpch.tiny.nation", 25);
        Table icebergTable = loadTable(tableName);
        assertThat(icebergTable.currentSnapshot().summary()).containsEntry("total-equality-deletes", "0");
        writeEqualityDeleteToNationTable(icebergTable, Optional.of(icebergTable.spec()), Optional.of(new PartitionData(new Long[] {1L})));
        List<String> initialActiveFiles = getActiveFiles(tableName);
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation WHERE regionkey != 1");
        assertUpdate("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE WHERE regionkey != 1");
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation WHERE regionkey != 1");
        // nationkey is before the equality delete column in the table schema, comment is after
        assertQuery("SELECT nationkey, comment FROM " + tableName, "SELECT nationkey, comment FROM nation WHERE regionkey != 1");
        assertThat(loadTable(tableName).currentSnapshot().summary()).containsEntry("total-equality-deletes", "1");
        List<String> updatedFiles = getActiveFiles(tableName);
        assertThat(updatedFiles)
                .doesNotContain(initialActiveFiles.stream()
                        .filter(path -> !path.contains("regionkey=1"))
                        .toArray(String[]::new));
    }

    @Test
    public void testOptimizeDuringWriteOperations()
            throws Exception
    {
        runOptimizeDuringWriteOperations(true);
        runOptimizeDuringWriteOperations(false);
    }

    private void runOptimizeDuringWriteOperations(boolean useSmallFiles)
            throws Exception
    {
        int threads = 5;
        int deletionThreads = threads - 1;
        int rows = 12;
        int rowsPerThread = rows / deletionThreads;

        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);

        // Slow down the delete operations so optimize is more likely to complete
        String blackholeTable = "blackhole_table_" + randomNameSuffix();
        assertUpdate("CREATE TABLE blackhole.default.%s (a INT, b INT) WITH (split_count = 1, pages_per_split = 1, rows_per_page = 1, page_processing_delay = '3s')".formatted(blackholeTable));

        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_optimize_during_write_operations",
                "(int_col INT)")) {
            String tableName = table.getName();

            // Testing both situations where a file is fully removed by the delete operation and when a row level delete is required.
            if (useSmallFiles) {
                for (int i = 0; i < rows; i++) {
                    assertUpdate(format("INSERT INTO %s VALUES %s", tableName, i), 1);
                }
            }
            else {
                String values = IntStream.range(0, rows).mapToObj(String::valueOf).collect(Collectors.joining(", "));
                assertUpdate(format("INSERT INTO %s VALUES %s", tableName, values), rows);
            }

            List<Future<List<Boolean>>> deletionFutures = IntStream.range(0, deletionThreads)
                    .mapToObj(threadNumber -> executor.submit(() -> {
                        barrier.await(10, SECONDS);
                        List<Boolean> successfulDeletes = new ArrayList<>();
                        for (int i = 0; i < rowsPerThread; i++) {
                            try {
                                int rowNumber = threadNumber * rowsPerThread + i;
                                getQueryRunner().execute(format("DELETE FROM %s WHERE int_col = %s OR ((SELECT count(*) FROM blackhole.default.%s) > 42)", tableName, rowNumber, blackholeTable));
                                successfulDeletes.add(true);
                            }
                            catch (RuntimeException e) {
                                successfulDeletes.add(false);
                            }
                        }
                        return successfulDeletes;
                    }))
                    .collect(toImmutableList());

            Future<?> optimizeFuture = executor.submit(() -> {
                try {
                    barrier.await(10, SECONDS);
                    // Allow for some deletes to start before running optimize
                    Thread.sleep(50);
                    assertUpdate("ALTER TABLE %s EXECUTE optimize".formatted(tableName));
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            List<String> expectedValues = new ArrayList<>();
            for (int threadNumber = 0; threadNumber < deletionThreads; threadNumber++) {
                List<Boolean> deleteOutcomes = deletionFutures.get(threadNumber).get();
                verify(deleteOutcomes.size() == rowsPerThread);
                for (int rowNumber = 0; rowNumber < rowsPerThread; rowNumber++) {
                    boolean successfulDelete = deleteOutcomes.get(rowNumber);
                    if (!successfulDelete) {
                        expectedValues.add(String.valueOf(threadNumber * rowsPerThread + rowNumber));
                    }
                }
            }

            optimizeFuture.get();
            assertThat(expectedValues.size()).isGreaterThan(0).isLessThan(rows);
            assertQuery("SELECT * FROM " + tableName, "VALUES " + String.join(", ", expectedValues));
        }
        finally {
            executor.shutdownNow();
            executor.awaitTermination(10, SECONDS);
        }
    }

    @Test
    public void testUpgradeTableToV2FromTrino()
    {
        String tableName = "test_upgrade_table_to_v2_from_trino_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (format_version = 1) AS SELECT * FROM tpch.tiny.nation", 25);
        assertThat(loadTable(tableName).operations().current().formatVersion()).isEqualTo(1);
        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES format_version = 2");
        assertThat(loadTable(tableName).operations().current().formatVersion()).isEqualTo(2);
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation");
    }

    @Test
    public void testDowngradingV2TableToV1Fails()
    {
        String tableName = "test_downgrading_v2_table_to_v1_fails_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (format_version = 2) AS SELECT * FROM tpch.tiny.nation", 25);
        assertThat(loadTable(tableName).operations().current().formatVersion()).isEqualTo(2);
        assertThat(query("ALTER TABLE " + tableName + " SET PROPERTIES format_version = 1"))
                .failure()
                .hasMessage("Failed to set new property values")
                .rootCause()
                .hasMessage("Cannot downgrade v2 table to v1");
    }

    @Test
    public void testUpgradingToInvalidVersionFails()
    {
        String tableName = "test_upgrading_to_invalid_version_fails_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (format_version = 2) AS SELECT * FROM tpch.tiny.nation", 25);
        assertThat(loadTable(tableName).operations().current().formatVersion()).isEqualTo(2);
        assertThat(query("ALTER TABLE " + tableName + " SET PROPERTIES format_version = 42"))
                .failure().hasMessage("line 1:79: Unable to set catalog 'iceberg' table property 'format_version' to [42]: format_version must be between 1 and 2");
    }

    @Test
    public void testUpdatingAllTableProperties()
    {
        String tableName = "test_updating_all_table_properties_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (format_version = 1, format = 'ORC') AS SELECT * FROM tpch.tiny.nation", 25);
        BaseTable table = loadTable(tableName);
        assertThat(table.operations().current().formatVersion()).isEqualTo(1);
        assertThat(table.properties().get(TableProperties.DEFAULT_FILE_FORMAT).equalsIgnoreCase("ORC")).isTrue();
        assertThat(table.spec().isUnpartitioned()).isTrue();

        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES format_version = 2, partitioning = ARRAY['regionkey'], format = 'PARQUET', sorted_by = ARRAY['comment']");
        table = loadTable(tableName);
        assertThat(table.operations().current().formatVersion()).isEqualTo(2);
        assertThat(table.properties().get(TableProperties.DEFAULT_FILE_FORMAT).equalsIgnoreCase("PARQUET")).isTrue();
        assertThat(table.spec().isPartitioned()).isTrue();
        List<PartitionField> partitionFields = table.spec().fields();
        assertThat(partitionFields).hasSize(1);
        assertThat(partitionFields.get(0).name()).isEqualTo("regionkey");
        assertThat(partitionFields.get(0).transform().isIdentity()).isTrue();
        assertThat(table.sortOrder().isSorted()).isTrue();
        List<SortField> sortFields = table.sortOrder().fields();
        assertThat(sortFields.size()).isEqualTo(1);
        assertThat(getOnlyElement(sortFields).sourceId()).isEqualTo(table.schema().findField("comment").fieldId());
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation");
    }

    @Test
    public void testUnsettingAllTableProperties()
    {
        String tableName = "test_unsetting_all_table_properties_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (format_version = 1, format = 'PARQUET', partitioning = ARRAY['regionkey'], sorted_by = ARRAY['comment']) " +
                "AS SELECT * FROM tpch.tiny.nation", 25);
        BaseTable table = loadTable(tableName);
        assertThat(table.operations().current().formatVersion()).isEqualTo(1);
        assertThat(table.properties().get(TableProperties.DEFAULT_FILE_FORMAT).equalsIgnoreCase("PARQUET")).isTrue();
        assertThat(table.spec().isPartitioned()).isTrue();
        List<PartitionField> partitionFields = table.spec().fields();
        assertThat(partitionFields).hasSize(1);
        assertThat(partitionFields.get(0).name()).isEqualTo("regionkey");
        assertThat(partitionFields.get(0).transform().isIdentity()).isTrue();

        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES format_version = DEFAULT, format = DEFAULT, partitioning = DEFAULT, sorted_by = DEFAULT");
        table = loadTable(tableName);
        assertThat(table.operations().current().formatVersion()).isEqualTo(2);
        assertThat(table.properties().get(TableProperties.DEFAULT_FILE_FORMAT).equalsIgnoreCase("PARQUET")).isTrue();
        assertThat(table.spec().isUnpartitioned()).isTrue();
        assertThat(table.sortOrder().isUnsorted()).isTrue();
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation");
    }

    @Test
    public void testDeletingEntireFile()
    {
        String tableName = "test_deleting_entire_file_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.tiny.nation WITH NO DATA", 0);
        assertUpdate("INSERT INTO " + tableName + " SELECT * FROM tpch.tiny.nation WHERE regionkey = 1", "SELECT count(*) FROM nation WHERE regionkey = 1");
        assertUpdate("INSERT INTO " + tableName + " SELECT * FROM tpch.tiny.nation WHERE regionkey != 1", "SELECT count(*) FROM nation WHERE regionkey != 1");

        assertThat(this.loadTable(tableName).newScan().planFiles()).hasSize(2);
        assertUpdate("DELETE FROM " + tableName + " WHERE regionkey <= 2", "SELECT count(*) FROM nation WHERE regionkey <= 2");
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation WHERE regionkey > 2");
        assertThat(this.loadTable(tableName).newScan().planFiles()).hasSize(2);
    }

    @Test
    public void testDeletingEntireFileFromPartitionedTable()
    {
        String tableName = "test_deleting_entire_file_from_partitioned_table_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (a INT, b INT) WITH (partitioning = ARRAY['a'])");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 1), (1, 3), (1, 5), (2, 1), (2, 3), (2, 5)", 6);
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 2), (1, 4), (1, 6), (2, 2), (2, 4), (2, 6)", 6);

        assertThat(this.loadTable(tableName).newScan().planFiles()).hasSize(4);
        assertUpdate("DELETE FROM " + tableName + " WHERE b % 2 = 0", 6);
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 1), (1, 3), (1, 5), (2, 1), (2, 3), (2, 5)");
        assertThat(this.loadTable(tableName).newScan().planFiles()).hasSize(4);
    }

    @Test
    public void testDeletingEntireFileWithNonTupleDomainConstraint()
    {
        String tableName = "test_deleting_entire_file_with_non_tuple_domain_constraint" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.tiny.nation WITH NO DATA", 0);
        assertUpdate("INSERT INTO " + tableName + " SELECT * FROM tpch.tiny.nation WHERE regionkey = 1", "SELECT count(*) FROM nation WHERE regionkey = 1");
        assertUpdate("INSERT INTO " + tableName + " SELECT * FROM tpch.tiny.nation WHERE regionkey != 1", "SELECT count(*) FROM nation WHERE regionkey != 1");

        assertThat(this.loadTable(tableName).newScan().planFiles()).hasSize(2);
        assertUpdate("DELETE FROM " + tableName + " WHERE regionkey % 2 = 1", "SELECT count(*) FROM nation WHERE regionkey % 2 = 1");
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation WHERE regionkey % 2 = 0");
        assertThat(this.loadTable(tableName).newScan().planFiles()).hasSize(2);
    }

    @Test
    public void testDeletingEntireFileWithMultipleSplits()
    {
        String tableName = "test_deleting_entire_file_with_multiple_splits" + randomNameSuffix();
        assertUpdate(
                Session.builder(getSession()).setCatalogSessionProperty("iceberg", "orc_writer_max_stripe_rows", "5").build(),
                "CREATE TABLE " + tableName + " WITH (format = 'ORC') AS SELECT * FROM tpch.tiny.nation", 25);
        // Set the split size to a small number of bytes so each ORC stripe gets its own split
        this.loadTable(tableName).updateProperties().set(SPLIT_SIZE, "100").commit();

        assertThat(this.loadTable(tableName).newScan().planFiles()).hasSize(1);
        // Ensure only one snapshot is committed to the table
        long initialSnapshotId = (long) computeScalar("SELECT snapshot_id FROM \"" + tableName + "$snapshots\" ORDER BY committed_at DESC FETCH FIRST 1 ROW WITH TIES");
        assertUpdate("DELETE FROM " + tableName + " WHERE regionkey < 10", 25);
        long parentSnapshotId = (long) computeScalar("SELECT parent_id FROM \"" + tableName + "$snapshots\" ORDER BY committed_at DESC FETCH FIRST 1 ROW WITH TIES");
        assertThat(initialSnapshotId).isEqualTo(parentSnapshotId);
        assertThat(query("SELECT * FROM " + tableName)).returnsEmptyResult();
        assertThat(this.loadTable(tableName).newScan().planFiles()).hasSize(1);
    }

    @Test
    public void testMultipleDeletes()
    {
        // Deletes only remove entire data files from the table if the whole file is removed in a single operation
        String tableName = "test_multiple_deletes_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.tiny.nation", 25);
        assertThat(this.loadTable(tableName).newScan().planFiles()).hasSize(1);
        // Ensure only one snapshot is committed to the table
        long initialSnapshotId = (long) computeScalar("SELECT snapshot_id FROM \"" + tableName + "$snapshots\" ORDER BY committed_at DESC FETCH FIRST 1 ROW WITH TIES");
        assertUpdate("DELETE FROM " + tableName + " WHERE regionkey % 2 = 1", "SELECT count(*) FROM nation WHERE regionkey % 2 = 1");
        long parentSnapshotId = (long) computeScalar("SELECT parent_id FROM \"" + tableName + "$snapshots\" ORDER BY committed_at DESC FETCH FIRST 1 ROW WITH TIES");
        assertThat(initialSnapshotId).isEqualTo(parentSnapshotId);

        assertUpdate("DELETE FROM " + tableName + " WHERE regionkey % 2 = 0", "SELECT count(*) FROM nation WHERE regionkey % 2 = 0");
        assertThat(query("SELECT * FROM " + tableName)).returnsEmptyResult();
        assertThat(this.loadTable(tableName).newScan().planFiles()).hasSize(1);
    }

    @Test
    public void testDeletingEntirePartitionedTable()
    {
        String tableName = "test_deleting_entire_partitioned_table_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (partitioning = ARRAY['regionkey']) AS SELECT * FROM tpch.tiny.nation", 25);

        assertThat(this.loadTable(tableName).newScan().planFiles()).hasSize(5);
        assertUpdate("DELETE FROM " + tableName + " WHERE regionkey < 10", "SELECT count(*) FROM nation WHERE regionkey < 10");
        assertThat(this.loadTable(tableName).newScan().planFiles()).hasSize(0);
        assertUpdate("DELETE FROM " + tableName + " WHERE regionkey < 10");
        assertThat(query("SELECT * FROM " + tableName)).returnsEmptyResult();
        assertThat(this.loadTable(tableName).newScan().planFiles()).hasSize(0);
    }

    @Test
    public void testFilesTable()
            throws Exception
    {
        String tableName = "test_files_table_" + randomNameSuffix();
        String tableLocation = "local:///" + tableName;
        assertUpdate("CREATE TABLE " + tableName + " WITH (location = '" + tableLocation + "', format_version = 2) AS SELECT * FROM tpch.tiny.nation", 25);
        BaseTable table = loadTable(tableName);
        Metrics metrics = new Metrics(
                10L,
                ImmutableMap.of(1, 2L, 2, 3L),
                ImmutableMap.of(1, 5L, 2, 3L, 3, 2L),
                ImmutableMap.of(1, 0L, 2, 2L),
                ImmutableMap.of(4, 1L),
                ImmutableMap.of(1, ByteBuffer.allocate(8).order(LITTLE_ENDIAN).putLong(0, 0L)),
                ImmutableMap.of(1, ByteBuffer.allocate(8).order(LITTLE_ENDIAN).putLong(0, 4L)));
        // Creating a simulated data file to verify the non-null values in the $files table
        DataFile dataFile = DataFiles.builder(PartitionSpec.unpartitioned())
                .withFormat(ORC)
                .withPath(tableLocation + "/data/test_files_table.orc")
                .withFileSizeInBytes(1234)
                .withMetrics(metrics)
                .withSplitOffsets(ImmutableList.of(4L))
                .withEncryptionKeyMetadata(ByteBuffer.wrap("Trino".getBytes(UTF_8)))
                .build();
        table.newAppend().appendFile(dataFile).commit();
        writeEqualityDeleteToNationTable(table);
        assertQuery(
                "SELECT " +
                        "content, " +
                        "file_format, " +
                        "record_count, " +
                        "CAST(column_sizes AS JSON), " +
                        "CAST(value_counts AS JSON), " +
                        "CAST(null_value_counts AS JSON), " +
                        "CAST(nan_value_counts AS JSON), " +
                        "CAST(lower_bounds AS JSON), " +
                        "CAST(upper_bounds AS JSON), " +
                        "key_metadata, " +
                        "split_offsets, " +
                        "equality_ids " +
                        "FROM \"" + tableName + "$files\"",
                """
                               VALUES
                                       (0,
                                        'PARQUET',
                                        25L,
                                        JSON '{"1":137,"2":216,"3":91,"4":801}',
                                        JSON '{"1":25,"2":25,"3":25,"4":25}',
                                        jSON '{"1":0,"2":0,"3":0,"4":0}',
                                        jSON '{}',
                                        JSON '{"1":"0","2":"ALGERIA","3":"0","4":" haggle. careful"}',
                                        JSON '{"1":"24","2":"VIETNAM","3":"4","4":"y final packaget"}',
                                        null,
                                        ARRAY[4L],
                                        null),
                                       (0,
                                        'ORC',
                                        10L,
                                        JSON '{"1":2,"2":3}',
                                        JSON '{"1":5,"2":3,"3":2}',
                                        JSON '{"1":0,"2":2}',
                                        JSON '{"4":1}',
                                        JSON '{"1":"0"}',
                                        JSON '{"1":"4"}',
                                        X'54 72 69 6e 6f',
                                        ARRAY[4L],
                                        null),
                                        (2,
                                        'PARQUET',
                                        1L,
                                        JSON '{"3":49}',
                                        JSON '{"3":1}',
                                        JSON '{"3":0}',
                                        JSON '{}',
                                        JSON '{"3":"1"}',
                                        JSON '{"3":"1"}',
                                        null,
                                        ARRAY[4],
                                        ARRAY[3])
                        """);
    }

    @Test
    public void testStatsFilePruning()
    {
        try (TestTable testTable = new TestTable(getQueryRunner()::execute, "test_stats_file_pruning_", "(a INT, b INT) WITH (partitioning = ARRAY['b'])")) {
            assertUpdate("INSERT INTO " + testTable.getName() + " VALUES (1, 10), (10, 10)", 2);
            assertUpdate("INSERT INTO " + testTable.getName() + " VALUES (200, 10), (300, 20)", 2);

            Optional<Long> snapshotId = Optional.of((long) computeScalar("SELECT snapshot_id FROM \"" + testTable.getName() + "$snapshots\" ORDER BY committed_at DESC FETCH FIRST 1 ROW WITH TIES"));
            TypeManager typeManager = new TestingTypeManager();
            Table table = loadTable(testTable.getName());
            TableStatistics withNoFilter = TableStatisticsReader.makeTableStatistics(
                    typeManager,
                    table,
                    snapshotId,
                    TupleDomain.all(),
                    TupleDomain.all(),
                    ImmutableSet.of(),
                    true,
                    fileSystemFactory.create(SESSION));
            assertThat(withNoFilter.getRowCount().getValue()).isEqualTo(4.0);

            TableStatistics withPartitionFilter = TableStatisticsReader.makeTableStatistics(
                    typeManager,
                    table,
                    snapshotId,
                    TupleDomain.withColumnDomains(ImmutableMap.of(
                            new IcebergColumnHandle(ColumnIdentity.primitiveColumnIdentity(2, "b"), INTEGER, ImmutableList.of(), INTEGER, true, Optional.empty()),
                            Domain.singleValue(INTEGER, 10L))),
                    TupleDomain.all(),
                    ImmutableSet.of(),
                    true,
                    fileSystemFactory.create(SESSION));
            assertThat(withPartitionFilter.getRowCount().getValue()).isEqualTo(3.0);

            IcebergColumnHandle column = new IcebergColumnHandle(ColumnIdentity.primitiveColumnIdentity(1, "a"), INTEGER, ImmutableList.of(), INTEGER, true, Optional.empty());
            TableStatistics withUnenforcedFilter = TableStatisticsReader.makeTableStatistics(
                    typeManager,
                    table,
                    snapshotId,
                    TupleDomain.all(),
                    TupleDomain.withColumnDomains(ImmutableMap.of(
                            column,
                            Domain.create(ValueSet.ofRanges(Range.greaterThan(INTEGER, 100L)), true))),
                    ImmutableSet.of(column),
                    true,
                    fileSystemFactory.create(SESSION));
            assertThat(withUnenforcedFilter.getRowCount().getValue()).isEqualTo(2.0);
        }
    }

    @Test
    public void testColumnStatsPruning()
    {
        try (TestTable testTable = new TestTable(getQueryRunner()::execute, "test_column_stats_pruning_", "(a INT, b INT) WITH (partitioning = ARRAY['b'])")) {
            assertUpdate("INSERT INTO " + testTable.getName() + " VALUES (1, 10), (10, 10)", 2);
            assertUpdate("INSERT INTO " + testTable.getName() + " VALUES (200, 10), (300, 20)", 2);

            Optional<Long> snapshotId = Optional.of((long) computeScalar("SELECT snapshot_id FROM \"" + testTable.getName() + "$snapshots\" ORDER BY committed_at DESC FETCH FIRST 1 ROW WITH TIES"));
            TypeManager typeManager = new TestingTypeManager();
            Table table = loadTable(testTable.getName());
            TableStatistics withNoProjectedColumns = TableStatisticsReader.makeTableStatistics(
                    typeManager,
                    table,
                    snapshotId,
                    TupleDomain.all(),
                    TupleDomain.all(),
                    ImmutableSet.of(),
                    true,
                    fileSystemFactory.create(SESSION));
            assertThat(withNoProjectedColumns.getRowCount().getValue()).isEqualTo(4.0);
            assertThat(withNoProjectedColumns.getColumnStatistics()).isEmpty();

            IcebergColumnHandle column = new IcebergColumnHandle(ColumnIdentity.primitiveColumnIdentity(1, "a"), INTEGER, ImmutableList.of(), INTEGER, true, Optional.empty());
            TableStatistics withProjectedColumns = TableStatisticsReader.makeTableStatistics(
                    typeManager,
                    table,
                    snapshotId,
                    TupleDomain.all(),
                    TupleDomain.all(),
                    ImmutableSet.of(column),
                    true,
                    fileSystemFactory.create(SESSION));
            assertThat(withProjectedColumns.getRowCount().getValue()).isEqualTo(4.0);
            assertThat(withProjectedColumns.getColumnStatistics()).containsOnlyKeys(column);
            assertThat(withProjectedColumns.getColumnStatistics().get(column))
                    .isEqualTo(ColumnStatistics.builder()
                            .setNullsFraction(Estimate.zero())
                            .setDistinctValuesCount(Estimate.of(4.0))
                            .setRange(new DoubleRange(1.0, 300.0))
                            .build());

            TableStatistics withPartitionFilterAndProjectedColumn = TableStatisticsReader.makeTableStatistics(
                    typeManager,
                    table,
                    snapshotId,
                    TupleDomain.all(),
                    TupleDomain.withColumnDomains(ImmutableMap.of(
                            new IcebergColumnHandle(ColumnIdentity.primitiveColumnIdentity(2, "b"), INTEGER, ImmutableList.of(), INTEGER, true, Optional.empty()),
                            Domain.singleValue(INTEGER, 10L))),
                    ImmutableSet.of(column),
                    true,
                    fileSystemFactory.create(SESSION));
            assertThat(withPartitionFilterAndProjectedColumn.getRowCount().getValue()).isEqualTo(3.0);
            assertThat(withPartitionFilterAndProjectedColumn.getColumnStatistics()).containsOnlyKeys(column);
            assertThat(withPartitionFilterAndProjectedColumn.getColumnStatistics().get(column))
                    .isEqualTo(ColumnStatistics.builder()
                            .setNullsFraction(Estimate.zero())
                            .setDistinctValuesCount(Estimate.of(4.0))
                            .setRange(new DoubleRange(1.0, 200.0))
                            .build());
        }
    }

    @Test
    public void testInt96TimestampWithTimeZone()
    {
        assertUpdate("CREATE TABLE hive.tpch.test_timestamptz_base (t timestamp) WITH (format = 'PARQUET')");
        assertUpdate("INSERT INTO hive.tpch.test_timestamptz_base (t) VALUES (timestamp '2022-07-26 12:13')", 1);

        // Writing TIMESTAMP WITH LOCAL TIME ZONE is not supported, so we first create Parquet object by writing unzoned
        // timestamp (which is converted to UTC using default timezone) and then creating another table that reads from the same file.
        String tableLocation = metastore.getTable("tpch", "test_timestamptz_base").orElseThrow().getStorage().getLocation();

        // TIMESTAMP WITH LOCAL TIME ZONE is not mapped to any Trino type, so we need to create the metastore entry manually
        metastore.createTable(
                new io.trino.metastore.Table(
                        "tpch",
                        "test_timestamptz",
                        Optional.of("hive"),
                        "EXTERNAL_TABLE",
                        new Storage(
                                HiveStorageFormat.PARQUET.toStorageFormat(),
                                Optional.of(tableLocation),
                                Optional.empty(),
                                false,
                                ImmutableMap.of()),
                        List.of(new Column("t", HiveType.HIVE_TIMESTAMPLOCALTZ, Optional.empty(), Map.of())),
                        List.of(),
                        ImmutableMap.of(),
                        Optional.empty(),
                        Optional.empty(),
                        OptionalLong.empty()),
                PrincipalPrivileges.fromHivePrivilegeInfos(ImmutableSet.of()));

        assertThat(query("SELECT * FROM hive.tpch.test_timestamptz"))
                .matches("VALUES TIMESTAMP '2022-07-26 17:13:00.000 UTC'");

        String path = (String) computeScalar("SELECT \"$path\" FROM hive.tpch.test_timestamptz_base");
        long size = (long) computeScalar("SELECT \"$file_size\" FROM hive.tpch.test_timestamptz_base");

        // Read a Parquet file from Iceberg connector
        assertUpdate("CREATE TABLE iceberg.tpch.test_timestamptz_migrated(t TIMESTAMP(6) WITH TIME ZONE)");

        BaseTable table = loadTable("test_timestamptz_migrated");

        table.updateProperties()
                .set(DEFAULT_NAME_MAPPING, toJson(MappingUtil.create(table.schema())))
                .commit();

        table.newAppend()
                .appendFile(DataFiles.builder(table.spec())
                        .withPath(path)
                        .withFormat(PARQUET)
                        .withFileSizeInBytes(size)
                        .withRecordCount(1)
                        .build())
                .commit();

        assertThat(query("SELECT * FROM iceberg.tpch.test_timestamptz_migrated"))
                .matches("VALUES TIMESTAMP '2022-07-26 17:13:00.000000 UTC'");

        assertUpdate("DROP TABLE hive.tpch.test_timestamptz_base");
        assertUpdate("DROP TABLE hive.tpch.test_timestamptz");
        assertUpdate("DROP TABLE iceberg.tpch.test_timestamptz_migrated");
    }

    @Test
    public void testSnapshotReferenceSystemTable()
    {
        String tableName = "test_snapshot_reference_system_table_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (partitioning = ARRAY['regionkey']) AS SELECT * FROM tpch.tiny.nation", 25);
        Table icebergTable = this.loadTable(tableName);
        long snapshotId1 = icebergTable.currentSnapshot().snapshotId();
        icebergTable.manageSnapshots()
                .createTag("test-tag", snapshotId1)
                .setMaxRefAgeMs("test-tag", 1)
                .commit();

        assertUpdate("INSERT INTO " + tableName + " SELECT * FROM tpch.tiny.nation LIMIT 5", 5);
        icebergTable.refresh();
        long snapshotId2 = icebergTable.currentSnapshot().snapshotId();
        icebergTable.manageSnapshots()
                .createBranch("test-branch", snapshotId2)
                .setMaxSnapshotAgeMs("test-branch", 1)
                .commit();

        assertUpdate("INSERT INTO " + tableName + " SELECT * FROM tpch.tiny.nation LIMIT 5", 5);
        icebergTable.refresh();
        long snapshotId3 = icebergTable.currentSnapshot().snapshotId();
        icebergTable.manageSnapshots()
                .createBranch("test-branch2", snapshotId3)
                .setMinSnapshotsToKeep("test-branch2", 1)
                .commit();

        assertQuery("SHOW COLUMNS FROM \"" + tableName + "$refs\"",
                "VALUES ('name', 'varchar', '', '')," +
                        "('type', 'varchar', '', '')," +
                        "('snapshot_id', 'bigint', '', '')," +
                        "('max_reference_age_in_ms', 'bigint', '', '')," +
                        "('min_snapshots_to_keep', 'integer', '', '')," +
                        "('max_snapshot_age_in_ms', 'bigint', '', '')");

        assertQuery("SELECT * FROM \"" + tableName + "$refs\"",
                "VALUES ('test-tag', 'TAG', " + snapshotId1 + ", 1, null, null)," +
                        "('test-branch', 'BRANCH', " + snapshotId2 + ", null, null, 1)," +
                        "('test-branch2', 'BRANCH', " + snapshotId3 + ", null, 1, null)," +
                        "('main', 'BRANCH', " + snapshotId3 + ", null, null, null)");
    }

    @Test
    public void testReadingSnapshotReference()
    {
        String tableName = "test_reading_snapshot_reference" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (partitioning = ARRAY['regionkey']) AS SELECT * FROM tpch.tiny.nation", 25);
        Table icebergTable = loadTable(tableName);
        long refSnapshotId = icebergTable.currentSnapshot().snapshotId();
        icebergTable.manageSnapshots()
                .createTag("test-tag", refSnapshotId)
                .createBranch("test-branch", refSnapshotId)
                .commit();
        assertQuery("SELECT * FROM \"" + tableName + "$refs\"",
                "VALUES ('test-tag', 'TAG', " + refSnapshotId + ", null, null, null)," +
                        "('test-branch', 'BRANCH', " + refSnapshotId + ", null, null, null)," +
                        "('main', 'BRANCH', " + refSnapshotId + ", null, null, null)");

        assertUpdate("INSERT INTO " + tableName + " SELECT * FROM tpch.tiny.nation LIMIT 5", 5);
        assertQuery("SELECT * FROM " + tableName + " FOR VERSION AS OF " + refSnapshotId,
                "SELECT * FROM nation");
        assertQuery("SELECT * FROM " + tableName + " FOR VERSION AS OF 'test-tag'",
                "SELECT * FROM nation");
        assertQuery("SELECT * FROM " + tableName + " FOR VERSION AS OF 'test-branch'",
                "SELECT * FROM nation");

        assertQueryFails("SELECT * FROM " + tableName + " FOR VERSION AS OF 'test-wrong-ref'",
                ".*?Cannot find snapshot with reference name: test-wrong-ref");
        assertQueryFails("SELECT * FROM " + tableName + " FOR VERSION AS OF 'TEST-TAG'",
                ".*?Cannot find snapshot with reference name: TEST-TAG");
    }

    @Test
    public void testNestedFieldPartitioning()
    {
        String tableName = "test_nested_field_partitioning_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INT, district ROW(name VARCHAR), state ROW(name VARCHAR)) WITH (partitioning = ARRAY['\"state.name\"'])");

        assertUpdate(
                "INSERT INTO " + tableName + " VALUES " +
                        "(1, ROW('Patna'), ROW('BH')), " +
                        "(2, ROW('Patna'), ROW('BH')), " +
                        "(3, ROW('Bengaluru'), ROW('KA')), " +
                        "(4, ROW('Bengaluru'), ROW('KA'))",
                4);
        assertUpdate(
                "INSERT INTO " + tableName + " VALUES " +
                        "(5, ROW('Patna'), ROW('BH')), " +
                        "(6, ROW('Patna'), ROW('BH')), " +
                        "(7, ROW('Bengaluru'), ROW('KA')), " +
                        "(8, ROW('Bengaluru'), ROW('KA'))",
                4);
        assertThat(loadTable(tableName).newScan().planFiles()).hasSize(4);

        assertUpdate("DELETE FROM " + tableName + " WHERE district.name = 'Bengaluru'", 4);
        assertThat(loadTable(tableName).newScan().planFiles()).hasSize(4);

        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES partitioning = ARRAY['\"state.name\"', '\"district.name\"']");
        Table icebergTable = updateTableToV2(tableName);
        assertThat(icebergTable.spec().fields().stream().map(PartitionField::name).toList())
                .containsExactlyInAnyOrder("state.name", "district.name");

        assertUpdate(
                "INSERT INTO " + tableName + " VALUES " +
                        "(9, ROW('Patna'), ROW('BH')), " +
                        "(10, ROW('Bengaluru'), ROW('BH')), " +
                        "(11, ROW('Bengaluru'), ROW('KA')), " +
                        "(12, ROW('Bengaluru'), ROW('KA'))",
                4);
        assertThat(loadTable(tableName).newScan().planFiles()).hasSize(7);

        assertQuery("SELECT id, district.name, state.name FROM " + tableName, "VALUES " +
                "(1, 'Patna', 'BH'), " +
                "(2, 'Patna', 'BH'), " +
                "(5, 'Patna', 'BH'), " +
                "(6, 'Patna', 'BH'), " +
                "(9, 'Patna', 'BH'), " +
                "(10, 'Bengaluru', 'BH'), " +
                "(11, 'Bengaluru', 'KA'), " +
                "(12, 'Bengaluru', 'KA')");

        assertUpdate("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");
        assertThat(loadTable(tableName).newScan().planFiles()).hasSize(3);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testHighlyNestedFieldPartitioning()
    {
        String tableName = "test_highly_nested_field_partitioning_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INT, country ROW(name VARCHAR, state ROW(name VARCHAR, district ROW(name VARCHAR))))" +
                " WITH (partitioning = ARRAY['\"country.state.district.name\"'])");

        assertUpdate(
                "INSERT INTO " + tableName + " VALUES " +
                        "(1, ROW('India', ROW('BH', ROW('Patna')))), " +
                        "(2, ROW('India', ROW('BH', ROW('Patna')))), " +
                        "(3, ROW('India', ROW('KA', ROW('Bengaluru')))), " +
                        "(4, ROW('India', ROW('KA', ROW('Bengaluru'))))",
                4);
        assertUpdate(
                "INSERT INTO " + tableName + " VALUES " +
                        "(5, ROW('India', ROW('BH', ROW('Patna')))), " +
                        "(6, ROW('India', ROW('BH', ROW('Patna')))), " +
                        "(7, ROW('India', ROW('KA', ROW('Bengaluru')))), " +
                        "(8, ROW('India', ROW('KA', ROW('Bengaluru'))))",
                4);
        assertThat(loadTable(tableName).newScan().planFiles()).hasSize(4);

        assertQuery("SELECT partition.\"country.state.district.name\" FROM \"" + tableName + "$partitions\"", "VALUES 'Patna', 'Bengaluru'");

        assertUpdate("DELETE FROM " + tableName + " WHERE country.state.district.name = 'Bengaluru'", 4);
        assertThat(loadTable(tableName).newScan().planFiles()).hasSize(2);

        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES partitioning = ARRAY['\"country.state.district.name\"', '\"country.state.name\"']");
        Table icebergTable = updateTableToV2(tableName);
        assertThat(icebergTable.spec().fields().stream().map(PartitionField::name).toList())
                .containsExactlyInAnyOrder("country.state.district.name", "country.state.name");

        assertUpdate(
                "INSERT INTO " + tableName + " VALUES " +
                        "(9, ROW('India', ROW('BH', ROW('Patna')))), " +
                        "(10, ROW('India', ROW('BH', ROW('Bengaluru')))), " +
                        "(11, ROW('India', ROW('KA', ROW('Bengaluru')))), " +
                        "(12, ROW('India', ROW('KA', ROW('Bengaluru'))))",
                4);

        assertThat(loadTable(tableName).newScan().planFiles()).hasSize(5);

        assertQuery("SELECT id, country.name, country.state.name, country.state.district.name FROM " + tableName, "VALUES " +
                "(1, 'India', 'BH', 'Patna'), " +
                "(2, 'India', 'BH', 'Patna'), " +
                "(5, 'India', 'BH', 'Patna'), " +
                "(6, 'India', 'BH', 'Patna'), " +
                "(9, 'India', 'BH', 'Patna'), " +
                "(10, 'India', 'BH', 'Bengaluru'), " +
                "(11, 'India', 'KA', 'Bengaluru'), " +
                "(12, 'India', 'KA', 'Bengaluru')");

        assertUpdate("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");
        assertThat(loadTable(tableName).newScan().planFiles()).hasSize(3);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testHighlyNestedFieldPartitioningWithTruncateTransform()
    {
        String tableName = "test_highly_nested_field_partitioning_with_transform_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INT, country ROW(name VARCHAR, state ROW(name VARCHAR, district ROW(name VARCHAR))))" +
                " WITH (partitioning = ARRAY['truncate(\"country.state.district.name\", 5)'])");

        assertUpdate(
                "INSERT INTO " + tableName + " VALUES " +
                        "(1, ROW('India', ROW('BH', ROW('Patna')))), " +
                        "(2, ROW('India', ROW('BH', ROW('Patna_Truncate')))), " +
                        "(3, ROW('India', ROW('DL', ROW('Delhi')))), " +
                        "(4, ROW('India', ROW('DL', ROW('Delhi_Truncate'))))",
                4);

        assertThat(loadTable(tableName).newScan().planFiles()).hasSize(2);
        List<MaterializedRow> files = computeActual("SELECT file_path, record_count FROM \"" + tableName + "$files\"").getMaterializedRows();
        List<MaterializedRow> partitionedFiles = files.stream()
                .filter(file -> ((String) file.getField(0)).contains("country.state.district.name_trunc="))
                .collect(toImmutableList());

        assertThat(partitionedFiles).hasSize(2);
        assertThat(partitionedFiles.stream().mapToLong(row -> (long) row.getField(1)).sum()).isEqualTo(4L);

        assertQuery("SELECT id, country.state.district.name, country.state.name, country.name FROM " + tableName, "VALUES " +
                "(1, 'Patna', 'BH', 'India'), " +
                "(2, 'Patna_Truncate', 'BH', 'India'), " +
                "(3, 'Delhi', 'DL', 'India'), " +
                "(4, 'Delhi_Truncate', 'DL', 'India')");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testHighlyNestedFieldPartitioningWithBucketTransform()
    {
        String tableName = "test_highly_nested_field_partitioning_with_transform_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INT, country ROW(name VARCHAR, state ROW(name VARCHAR, district ROW(name VARCHAR))))" +
                " WITH (partitioning = ARRAY['bucket(\"country.state.district.name\", 2)'])");

        assertUpdate(
                "INSERT INTO " + tableName + " VALUES " +
                        "(1, ROW('India', ROW('BH', ROW('Patna')))), " +
                        "(2, ROW('India', ROW('MH', ROW('Mumbai')))), " +
                        "(3, ROW('India', ROW('DL', ROW('Delhi')))), " +
                        "(4, ROW('India', ROW('KA', ROW('Bengaluru'))))",
                4);

        assertThat(loadTable(tableName).newScan().planFiles()).hasSize(2);
        List<MaterializedRow> files = computeActual("SELECT file_path, record_count FROM \"" + tableName + "$files\"").getMaterializedRows();
        List<MaterializedRow> partitionedFiles = files.stream()
                .filter(file -> ((String) file.getField(0)).contains("country.state.district.name_bucket="))
                .collect(toImmutableList());

        assertThat(partitionedFiles).hasSize(2);
        assertThat(partitionedFiles.stream().mapToLong(row -> (long) row.getField(1)).sum()).isEqualTo(4L);

        assertQuery("SELECT id, country.state.district.name, country.state.name, country.name FROM " + tableName, "VALUES " +
                "(1, 'Patna', 'BH', 'India'), " +
                "(2, 'Mumbai', 'MH', 'India'), " +
                "(3, 'Delhi', 'DL', 'India'), " +
                "(4, 'Bengaluru', 'KA', 'India')");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testHighlyNestedFieldPartitioningWithTimestampTransform()
    {
        testHighlyNestedFieldPartitioningWithTimestampTransform(
                "ARRAY['year(\"grandparent.parent.ts\")']",
                ".*?(grandparent\\.parent\\.ts_year=.*/).*",
                ImmutableSet.of("grandparent.parent.ts_year=2021/", "grandparent.parent.ts_year=2022/", "grandparent.parent.ts_year=2023/"));
        testHighlyNestedFieldPartitioningWithTimestampTransform(
                "ARRAY['month(\"grandparent.parent.ts\")']",
                ".*?(grandparent\\.parent\\.ts_month=.*/).*",
                ImmutableSet.of("grandparent.parent.ts_month=2021-01/", "grandparent.parent.ts_month=2022-02/", "grandparent.parent.ts_month=2023-03/"));
        testHighlyNestedFieldPartitioningWithTimestampTransform(
                "ARRAY['day(\"grandparent.parent.ts\")']",
                ".*?(grandparent\\.parent\\.ts_day=.*/).*",
                ImmutableSet.of("grandparent.parent.ts_day=2021-01-01/", "grandparent.parent.ts_day=2022-02-02/", "grandparent.parent.ts_day=2023-03-03/"));
        testHighlyNestedFieldPartitioningWithTimestampTransform(
                "ARRAY['hour(\"grandparent.parent.ts\")']",
                ".*?(grandparent\\.parent\\.ts_hour=.*/).*",
                ImmutableSet.of("grandparent.parent.ts_hour=2021-01-01-01/", "grandparent.parent.ts_hour=2022-02-02-02/", "grandparent.parent.ts_hour=2023-03-03-03/"));
    }

    private void testHighlyNestedFieldPartitioningWithTimestampTransform(String partitioning, String partitionDirectoryRegex, Set<String> expectedPartitionDirectories)
    {
        String tableName = "test_highly_nested_field_partitioning_with_timestamp_transform_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, grandparent ROW(parent ROW(ts TIMESTAMP(6), a INT), b INT)) WITH (partitioning = " + partitioning + ")");
        assertUpdate(
                "INSERT INTO " + tableName + " VALUES " +
                        "(1, ROW(ROW(TIMESTAMP '2021-01-01 01:01:01.111111', 1), 1)), " +
                        "(2, ROW(ROW(TIMESTAMP '2022-02-02 02:02:02.222222', 2), 2)), " +
                        "(3, ROW(ROW(TIMESTAMP '2023-03-03 03:03:03.333333', 3), 3)), " +
                        "(4, ROW(ROW(TIMESTAMP '2022-02-02 02:04:04.444444', 4), 4))",
                4);

        assertThat(loadTable(tableName).newScan().planFiles()).hasSize(3);
        Set<String> partitionedDirectories = computeActual("SELECT file_path FROM \"" + tableName + "$files\"")
                .getMaterializedRows().stream()
                .map(entry -> extractPartitionFolder((String) entry.getField(0), partitionDirectoryRegex))
                .flatMap(Optional::stream)
                .collect(toImmutableSet());

        assertThat(partitionedDirectories).isEqualTo(expectedPartitionDirectories);

        assertQuery("SELECT id, grandparent.parent.ts, grandparent.parent.a, grandparent.b FROM " + tableName, "VALUES " +
                "(1, '2021-01-01 01:01:01.111111', 1, 1), " +
                "(2, '2022-02-02 02:02:02.222222', 2, 2), " +
                "(3, '2023-03-03 03:03:03.333333', 3, 3), " +
                "(4, '2022-02-02 02:04:04.444444', 4, 4)");

        assertUpdate("DROP TABLE " + tableName);
    }

    private Optional<String> extractPartitionFolder(String file, String regex)
    {
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(file);
        if (matcher.matches()) {
            return Optional.of(matcher.group(1));
        }
        return Optional.empty();
    }

    private void writeEqualityDeleteToNationTable(Table icebergTable)
            throws Exception
    {
        writeEqualityDeleteToNationTable(icebergTable, Optional.empty(), Optional.empty());
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

    private Table updateTableToV2(String tableName)
    {
        BaseTable table = loadTable(tableName);
        TableOperations operations = table.operations();
        TableMetadata currentMetadata = operations.current();
        operations.commit(currentMetadata, currentMetadata.upgradeToFormatVersion(2));

        return table;
    }

    private BaseTable loadTable(String tableName)
    {
        return IcebergTestUtils.loadTable(tableName, metastore, fileSystemFactory, "hive", "tpch");
    }

    private List<String> getActiveFiles(String tableName)
    {
        return computeActual(format("SELECT file_path FROM \"%s$files\" WHERE content = %d", tableName, FileContent.DATA.id())).getOnlyColumn()
                .map(String.class::cast)
                .collect(toImmutableList());
    }
}
