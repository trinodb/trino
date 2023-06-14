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
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.TrinoViewHiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.cache.CachingHiveMetastore;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.file.FileMetastoreTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.hms.TrinoHiveCatalog;
import io.trino.plugin.iceberg.fileio.ForwardingFileIo;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.TypeManager;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
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
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.parquet.Parquet;
import org.assertj.core.api.Assertions;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.hive.metastore.cache.CachingHiveMetastore.memoizeMetastore;
import static io.trino.plugin.hive.metastore.file.TestingFileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergUtil.loadIcebergTable;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tpch.TpchTable.NATION;
import static java.lang.String.format;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.iceberg.FileFormat.ORC;
import static org.apache.iceberg.TableProperties.SPLIT_SIZE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestIcebergV2
        extends AbstractTestQueryFramework
{
    private HiveMetastore metastore;
    private java.nio.file.Path tempDir;
    private File metastoreDir;
    private TrinoFileSystemFactory fileSystemFactory;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        tempDir = Files.createTempDirectory("test_iceberg_v2");
        metastoreDir = tempDir.resolve("iceberg_data").toFile();
        metastore = createTestingFileHiveMetastore(metastoreDir);

        return IcebergQueryRunner.builder()
                .setInitialTables(NATION)
                .setMetastoreDirectory(metastoreDir)
                .build();
    }

    @BeforeClass
    public void initFileSystemFactory()
    {
        fileSystemFactory = getFileSystemFactory(getDistributedQueryRunner());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        deleteRecursively(tempDir, ALLOW_INSECURE);
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

        Path metadataDir = new Path(metastoreDir.toURI());
        String deleteFileName = "delete_file_" + UUID.randomUUID();
        FileIO fileIo = new ForwardingFileIo(fileSystemFactory.create(SESSION));

        Path path = new Path(metadataDir, deleteFileName);
        PositionDeleteWriter<Record> writer = Parquet.writeDeletes(fileIo.newOutputFile(path.toString()))
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
        writeEqualityDeleteToNationTable(icebergTable, Optional.of(icebergTable.spec()), Optional.of(new PartitionData(new Long[]{1L})));
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation WHERE regionkey != 1");
        // nationkey is before the equality delete column in the table schema, comment is after
        assertQuery("SELECT nationkey, comment FROM " + tableName, "SELECT nationkey, comment FROM nation WHERE regionkey != 1");
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
        writeEqualityDeleteToNationTable(icebergTable, Optional.of(icebergTable.spec()), Optional.of(new PartitionData(new Long[]{1L})));
        assertQuery("SELECT array_column[1], map_column[1], row_column.x FROM " + tableName, "SELECT 1, 2, 1 FROM nation WHERE regionkey != 1");
    }

    @Test
    public void testOptimizingV2TableRemovesEqualityDeletesWhenWholeTableIsScanned()
            throws Exception
    {
        String tableName = "test_optimize_table_cleans_equality_delete_file_when_whole_table_is_scanned" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (partitioning = ARRAY['regionkey']) AS SELECT * FROM tpch.tiny.nation", 25);
        Table icebergTable = loadTable(tableName);
        Assertions.assertThat(icebergTable.currentSnapshot().summary().get("total-equality-deletes")).isEqualTo("0");
        writeEqualityDeleteToNationTable(icebergTable, Optional.of(icebergTable.spec()), Optional.of(new PartitionData(new Long[]{1L})));
        List<String> initialActiveFiles = getActiveFiles(tableName);
        query("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation WHERE regionkey != 1");
        // nationkey is before the equality delete column in the table schema, comment is after
        assertQuery("SELECT nationkey, comment FROM " + tableName, "SELECT nationkey, comment FROM nation WHERE regionkey != 1");
        Assertions.assertThat(loadTable(tableName).currentSnapshot().summary().get("total-equality-deletes")).isEqualTo("0");
        List<String> updatedFiles = getActiveFiles(tableName);
        Assertions.assertThat(updatedFiles).doesNotContain(initialActiveFiles.toArray(new String[0]));
    }

    @Test
    public void testOptimizingV2TableDoesntRemoveEqualityDeletesWhenOnlyPartOfTheTableIsOptimized()
            throws Exception
    {
        String tableName = "test_optimize_table_with_equality_delete_file_for_different_partition_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (partitioning = ARRAY['regionkey']) AS SELECT * FROM tpch.tiny.nation", 25);
        Table icebergTable = loadTable(tableName);
        Assertions.assertThat(icebergTable.currentSnapshot().summary().get("total-equality-deletes")).isEqualTo("0");
        List<String> initialActiveFiles = getActiveFiles(tableName);
        writeEqualityDeleteToNationTable(icebergTable, Optional.of(icebergTable.spec()), Optional.of(new PartitionData(new Long[]{1L})));
        query("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE WHERE regionkey != 1");
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation WHERE regionkey != 1");
        // nationkey is before the equality delete column in the table schema, comment is after
        assertQuery("SELECT nationkey, comment FROM " + tableName, "SELECT nationkey, comment FROM nation WHERE regionkey != 1");
        Assertions.assertThat(loadTable(tableName).currentSnapshot().summary().get("total-equality-deletes")).isEqualTo("1");
        List<String> updatedFiles = getActiveFiles(tableName);
        Assertions.assertThat(updatedFiles).doesNotContain(initialActiveFiles.stream().filter(path -> !path.contains("regionkey=1")).toArray(String[]::new));
    }

    @Test
    public void testSelectivelyOptimizingLeavesEqualityDeletes()
            throws Exception
    {
        String tableName = "test_selectively_optimizing_leaves_eq_deletes_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (partitioning = ARRAY['nationkey']) AS SELECT * FROM tpch.tiny.nation", 25);
        Table icebergTable = loadTable(tableName);
        writeEqualityDeleteToNationTable(icebergTable, Optional.of(icebergTable.spec()), Optional.of(new PartitionData(new Long[]{1L})));
        query("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE WHERE nationkey < 5");
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation WHERE regionkey != 1 OR nationkey != 1");
        Assertions.assertThat(loadTable(tableName).currentSnapshot().summary().get("total-equality-deletes")).isEqualTo("1");
    }

    @Test
    public void testOptimizingWholeTableRemovesEqualityDeletes()
            throws Exception
    {
        String tableName = "test_optimizing_whole_table_removes_eq_deletes_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (partitioning = ARRAY['nationkey']) AS SELECT * FROM tpch.tiny.nation", 25);
        Table icebergTable = loadTable(tableName);
        writeEqualityDeleteToNationTable(icebergTable, Optional.of(icebergTable.spec()), Optional.of(new PartitionData(new Long[]{1L})));
        query("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation WHERE regionkey != 1 OR nationkey != 1");
        Assertions.assertThat(loadTable(tableName).currentSnapshot().summary().get("total-equality-deletes")).isEqualTo("0");
    }

    @Test
    public void testOptimizingV2TableWithEmptyPartitionSpec()
            throws Exception
    {
        String tableName = "test_optimize_table_with_global_equality_delete_file_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.tiny.nation", 25);
        Table icebergTable = loadTable(tableName);
        Assertions.assertThat(icebergTable.currentSnapshot().summary().get("total-equality-deletes")).isEqualTo("0");
        writeEqualityDeleteToNationTable(icebergTable);
        List<String> initialActiveFiles = getActiveFiles(tableName);
        query("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation WHERE regionkey != 1");
        // nationkey is before the equality delete column in the table schema, comment is after
        assertQuery("SELECT nationkey, comment FROM " + tableName, "SELECT nationkey, comment FROM nation WHERE regionkey != 1");
        Assertions.assertThat(loadTable(tableName).currentSnapshot().summary().get("total-equality-deletes")).isEqualTo("0");
        List<String> updatedFiles = getActiveFiles(tableName);
        Assertions.assertThat(updatedFiles).doesNotContain(initialActiveFiles.toArray(new String[0]));
    }

    @Test
    public void testOptimizingPartitionsOfV2TableWithGlobalEqualityDeleteFile()
            throws Exception
    {
        String tableName = "test_optimize_partitioned_table_with_global_equality_delete_file_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (partitioning = ARRAY['regionkey']) AS SELECT * FROM tpch.tiny.nation", 25);
        Table icebergTable = loadTable(tableName);
        Assertions.assertThat(icebergTable.currentSnapshot().summary().get("total-equality-deletes")).isEqualTo("0");
        writeEqualityDeleteToNationTable(icebergTable, Optional.of(icebergTable.spec()), Optional.of(new PartitionData(new Long[]{1L})));
        List<String> initialActiveFiles = getActiveFiles(tableName);
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation WHERE regionkey != 1");
        query("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE WHERE regionkey != 1");
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation WHERE regionkey != 1");
        // nationkey is before the equality delete column in the table schema, comment is after
        assertQuery("SELECT nationkey, comment FROM " + tableName, "SELECT nationkey, comment FROM nation WHERE regionkey != 1");
        Assertions.assertThat(loadTable(tableName).currentSnapshot().summary().get("total-equality-deletes")).isEqualTo("1");
        List<String> updatedFiles = getActiveFiles(tableName);
        Assertions.assertThat(updatedFiles)
                .doesNotContain(initialActiveFiles.stream()
                                .filter(path -> !path.contains("regionkey=1"))
                                .toArray(String[]::new));
    }

    @Test
    public void testUpgradeTableToV2FromTrino()
    {
        String tableName = "test_upgrade_table_to_v2_from_trino_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (format_version = 1) AS SELECT * FROM tpch.tiny.nation", 25);
        assertEquals(loadTable(tableName).operations().current().formatVersion(), 1);
        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES format_version = 2");
        assertEquals(loadTable(tableName).operations().current().formatVersion(), 2);
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation");
    }

    @Test
    public void testDowngradingV2TableToV1Fails()
    {
        String tableName = "test_downgrading_v2_table_to_v1_fails_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (format_version = 2) AS SELECT * FROM tpch.tiny.nation", 25);
        assertEquals(loadTable(tableName).operations().current().formatVersion(), 2);
        assertThatThrownBy(() -> query("ALTER TABLE " + tableName + " SET PROPERTIES format_version = 1"))
                .hasMessage("Failed to set new property values")
                .rootCause()
                .hasMessage("Cannot downgrade v2 table to v1");
    }

    @Test
    public void testUpgradingToInvalidVersionFails()
    {
        String tableName = "test_upgrading_to_invalid_version_fails_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (format_version = 2) AS SELECT * FROM tpch.tiny.nation", 25);
        assertEquals(loadTable(tableName).operations().current().formatVersion(), 2);
        assertThatThrownBy(() -> query("ALTER TABLE " + tableName + " SET PROPERTIES format_version = 42"))
                .hasMessage("Unable to set catalog 'iceberg' table property 'format_version' to [42]: format_version must be between 1 and 2");
    }

    @Test
    public void testUpdatingAllTableProperties()
    {
        String tableName = "test_updating_all_table_properties_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (format_version = 1, format = 'ORC') AS SELECT * FROM tpch.tiny.nation", 25);
        BaseTable table = loadTable(tableName);
        assertEquals(table.operations().current().formatVersion(), 1);
        assertTrue(table.properties().get(TableProperties.DEFAULT_FILE_FORMAT).equalsIgnoreCase("ORC"));
        assertTrue(table.spec().isUnpartitioned());

        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES format_version = 2, partitioning = ARRAY['regionkey'], format = 'PARQUET', sorted_by = ARRAY['comment']");
        table = loadTable(tableName);
        assertEquals(table.operations().current().formatVersion(), 2);
        assertTrue(table.properties().get(TableProperties.DEFAULT_FILE_FORMAT).equalsIgnoreCase("PARQUET"));
        assertTrue(table.spec().isPartitioned());
        List<PartitionField> partitionFields = table.spec().fields();
        assertThat(partitionFields).hasSize(1);
        assertEquals(partitionFields.get(0).name(), "regionkey");
        assertTrue(partitionFields.get(0).transform().isIdentity());
        assertTrue(table.sortOrder().isSorted());
        List<SortField> sortFields = table.sortOrder().fields();
        assertEquals(sortFields.size(), 1);
        assertEquals(getOnlyElement(sortFields).sourceId(), table.schema().findField("comment").fieldId());
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation");
    }

    @Test
    public void testUnsettingAllTableProperties()
    {
        String tableName = "test_unsetting_all_table_properties_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (format_version = 1, format = 'PARQUET', partitioning = ARRAY['regionkey'], sorted_by = ARRAY['comment']) " +
                "AS SELECT * FROM tpch.tiny.nation", 25);
        BaseTable table = loadTable(tableName);
        assertEquals(table.operations().current().formatVersion(), 1);
        assertTrue(table.properties().get(TableProperties.DEFAULT_FILE_FORMAT).equalsIgnoreCase("PARQUET"));
        assertTrue(table.spec().isPartitioned());
        List<PartitionField> partitionFields = table.spec().fields();
        assertThat(partitionFields).hasSize(1);
        assertEquals(partitionFields.get(0).name(), "regionkey");
        assertTrue(partitionFields.get(0).transform().isIdentity());

        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES format_version = DEFAULT, format = DEFAULT, partitioning = DEFAULT, sorted_by = DEFAULT");
        table = loadTable(tableName);
        assertEquals(table.operations().current().formatVersion(), 2);
        assertTrue(table.properties().get(TableProperties.DEFAULT_FILE_FORMAT).equalsIgnoreCase("ORC"));
        assertTrue(table.spec().isUnpartitioned());
        assertTrue(table.sortOrder().isUnsorted());
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
        assertThat(this.loadTable(tableName).newScan().planFiles()).hasSize(1);
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
        assertThat(this.loadTable(tableName).newScan().planFiles()).hasSize(2);
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
        assertThat(this.loadTable(tableName).newScan().planFiles()).hasSize(1);
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
        assertEquals(initialSnapshotId, parentSnapshotId);
        assertThat(query("SELECT * FROM " + tableName)).returnsEmptyResult();
        assertThat(this.loadTable(tableName).newScan().planFiles()).hasSize(0);
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
        assertEquals(initialSnapshotId, parentSnapshotId);

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
        String tableLocation = metastoreDir.getPath() + "/" + tableName;
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
        // TODO Currently, Trino does not include equality delete files stats in the $files table.
        //  Once it is fixed by https://github.com/trinodb/trino/pull/16232, include equality delete output in the test.
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
                                        'ORC',
                                        25L,
                                        null,
                                        JSON '{"1":25,"2":25,"3":25,"4":25}',
                                        JSON '{"1":0,"2":0,"3":0,"4":0}',
                                        null,
                                        JSON '{"1":"0","2":"ALGERIA","3":"0","4":" haggle. careful"}',
                                        JSON '{"1":"24","2":"VIETNAM","3":"4","4":"y final packaget"}',
                                        null,
                                        null,
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
                                        null)
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
            TableStatistics withNoFilter = TableStatisticsReader.makeTableStatistics(typeManager, table, snapshotId, TupleDomain.all(), TupleDomain.all(), true);
            assertEquals(withNoFilter.getRowCount().getValue(), 4.0);

            TableStatistics withPartitionFilter = TableStatisticsReader.makeTableStatistics(
                    typeManager,
                    table,
                    snapshotId,
                    TupleDomain.withColumnDomains(ImmutableMap.of(
                            new IcebergColumnHandle(ColumnIdentity.primitiveColumnIdentity(1, "b"), INTEGER, ImmutableList.of(), INTEGER, Optional.empty()),
                            Domain.singleValue(INTEGER, 10L))),
                    TupleDomain.all(),
                    true);
            assertEquals(withPartitionFilter.getRowCount().getValue(), 3.0);

            TableStatistics withUnenforcedFilter = TableStatisticsReader.makeTableStatistics(
                    typeManager,
                    table,
                    snapshotId,
                    TupleDomain.all(),
                    TupleDomain.withColumnDomains(ImmutableMap.of(
                            new IcebergColumnHandle(ColumnIdentity.primitiveColumnIdentity(0, "a"), INTEGER, ImmutableList.of(), INTEGER, Optional.empty()),
                            Domain.create(ValueSet.ofRanges(Range.greaterThan(INTEGER, 100L)), true))),
                    true);
            assertEquals(withUnenforcedFilter.getRowCount().getValue(), 2.0);
        }
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

    private void writeEqualityDeleteToNationTable(Table icebergTable, Optional<PartitionSpec> partitionSpec, Optional<PartitionData> partitionData, Map<String, Object> overwriteValues)
            throws Exception
    {
        Path metadataDir = new Path(metastoreDir.toURI());
        String deleteFileName = "delete_file_" + UUID.randomUUID();
        FileIO fileIo = new ForwardingFileIo(fileSystemFactory.create(SESSION));

        Schema deleteRowSchema = icebergTable.schema().select(overwriteValues.keySet());
        List<Integer> equalityFieldIds = overwriteValues.keySet().stream()
                .map(name -> deleteRowSchema.findField(name).fieldId())
                .collect(toImmutableList());
        Parquet.DeleteWriteBuilder writerBuilder = Parquet.writeDeletes(fileIo.newOutputFile(new Path(metadataDir, deleteFileName).toString()))
                .forTable(icebergTable)
                .rowSchema(deleteRowSchema)
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .equalityFieldIds(equalityFieldIds)
                .overwrite();
        if (partitionSpec.isPresent() && partitionData.isPresent()) {
            writerBuilder = writerBuilder
                    .withSpec(partitionSpec.get())
                    .withPartition(partitionData.get());
        }
        EqualityDeleteWriter<Record> writer = writerBuilder.buildEqualityWriter();

        Record dataDelete = GenericRecord.create(deleteRowSchema);
        try (Closeable ignored = writer) {
            writer.write(dataDelete.copy(overwriteValues));
        }

        icebergTable.newRowDelta().addDeletes(writer.toDeleteFile()).commit();
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
        IcebergTableOperationsProvider tableOperationsProvider = new FileMetastoreTableOperationsProvider(fileSystemFactory);
        CachingHiveMetastore cachingHiveMetastore = memoizeMetastore(metastore, 1000);
        TrinoCatalog catalog = new TrinoHiveCatalog(
                new CatalogName("hive"),
                cachingHiveMetastore,
                new TrinoViewHiveMetastore(cachingHiveMetastore, false, "trino-version", "test"),
                fileSystemFactory,
                new TestingTypeManager(),
                tableOperationsProvider,
                false,
                false,
                false);
        return (BaseTable) loadIcebergTable(catalog, tableOperationsProvider, SESSION, new SchemaTableName("tpch", tableName));
    }

    private List<String> getActiveFiles(String tableName)
    {
        return computeActual(format("SELECT file_path FROM \"%s$files\"", tableName)).getOnlyColumn()
                .map(String.class::cast)
                .collect(toImmutableList());
    }
}
