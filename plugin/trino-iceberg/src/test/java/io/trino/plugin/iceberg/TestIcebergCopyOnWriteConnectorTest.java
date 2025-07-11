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

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.filesystem.Location;
import io.trino.testing.MaterializedResult;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.iceberg.IcebergFileFormat.PARQUET;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkParquetFileSorting;
import static io.trino.plugin.iceberg.IcebergTestUtils.withSmallRowGroups;
import static io.trino.plugin.iceberg.WriteChangeMode.COW;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_DELETE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_MERGE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergCopyOnWriteConnectorTest
        extends BaseIcebergConnectorTest
{
    @Override
    protected IcebergQueryRunner.Builder createQueryRunnerBuilder()
    {
        return IcebergQueryRunner.builder()
                .setIcebergProperties(ImmutableMap.<String, String>builder()
                        .put("iceberg.file-format", format.name())
                        // Allows testing the sorting writer flushing to the file system with smaller tables
                        .put("iceberg.allowed-extra-properties", "extra.property.one,extra.property.two,extra.property.three,sorted_by")
                        .put("iceberg.writer-sort-buffer-size", "1MB")
                        .put("iceberg.write-change-mode", "cow")
                        .buildOrThrow())
                .setInitialTables(REQUIRED_TPCH_TABLES);
    }

    public TestIcebergCopyOnWriteConnectorTest()
    {
        super(PARQUET, COW);
    }

    @Override
    protected boolean supportsIcebergFileStatistics(String typeName)
    {
        return true;
    }

    @Override
    protected boolean supportsRowGroupStatistics(String typeName)
    {
        return
            !(typeName.equalsIgnoreCase("varbinary") ||
                    typeName.equalsIgnoreCase("time") ||
                    typeName.equalsIgnoreCase("time(6)") ||
                    typeName.equalsIgnoreCase("timestamp(3) with time zone") ||
                    typeName.equalsIgnoreCase("timestamp(6) with time zone"));
    }

    @Override
    protected boolean supportsPhysicalPushdown()
    {
        return true;
    }

    @Test
    public void testMergeSimpleUpdate()
    {
        skipTestUnless(hasBehavior(SUPPORTS_MERGE));

        for (IcebergFileFormat fileFormat : IcebergFileFormat.values()) {
            String targetTable = "merge_simple_target_" + randomNameSuffix();
            String sourceTable = "merge_simple_source_" + randomNameSuffix();
            createTableForWrites("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) " + "with (format = '" + fileFormat + "')", targetTable, Optional.of("customer"));

            assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch')", targetTable), 1);

            createTableForWrites("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) " + "with (format = '" + fileFormat + "')", sourceTable, Optional.empty());

            assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 6, 'Arches')", sourceTable), 1);

            assertUpdate(format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                    "    WHEN MATCHED AND s.address = 'Centreville' THEN DELETE" +
                    "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases + t.purchases, address = s.address" +
                    "    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)", 1);

            assertQuery("SELECT * FROM " + targetTable, "VALUES ('Aaron', 11, 'Arches')");

            assertUpdate("DROP TABLE " + sourceTable);
            assertUpdate("DROP TABLE " + targetTable);
        }
    }

    @Test
    public void testMergeSimpleDelete()
    {
        skipTestUnless(hasBehavior(SUPPORTS_MERGE));

        for (IcebergFileFormat fileFormat : IcebergFileFormat.values()) {
            String targetTable = "merge_simple_target_" + randomNameSuffix();
            String sourceTable = "merge_simple_source_" + randomNameSuffix();
            createTableForWrites("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)" + "with (format = '" + fileFormat + "')", targetTable, Optional.of("customer"));

            assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch')", targetTable), 1);

            createTableForWrites("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)" + "with (format = '" + fileFormat + "')", sourceTable, Optional.empty());

            assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 6, 'Arches')", sourceTable), 1);

            assertUpdate(format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                    "    WHEN MATCHED AND s.address = 'Arches' THEN DELETE" +
                    "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases + t.purchases, address = s.address" +
                    "    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)", 1);

            assertQuery("SELECT count(*) FROM " + targetTable, "SELECT 0");

            assertUpdate("DROP TABLE " + sourceTable);
            assertUpdate("DROP TABLE " + targetTable);
        }
    }

    @Test
    public void testMergeSimpleInsert()
    {
        skipTestUnless(hasBehavior(SUPPORTS_MERGE));

        for (IcebergFileFormat fileFormat : IcebergFileFormat.values()) {
            String targetTable = "merge_simple_target_" + randomNameSuffix();
            String sourceTable = "merge_simple_source_" + randomNameSuffix();
            createTableForWrites("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)" + "with (format = '" + fileFormat + "')", targetTable, Optional.of("customer"));

            assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable), 4);

            createTableForWrites("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)" + "with (format = '" + fileFormat + "')", sourceTable, Optional.empty());

            assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 6, 'Arches'), ('Ed', 7, 'Etherville'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire')", sourceTable), 4);

            assertUpdate(format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                    "    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)", 1);

            assertQuery("SELECT * FROM " + targetTable, "VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon'), ('Ed', 7, 'Etherville')");

            assertUpdate("DROP TABLE " + sourceTable);
            assertUpdate("DROP TABLE " + targetTable);
        }
    }

    @Test
    public void testSimpleDelete()
    {
        skipTestUnless(hasBehavior(SUPPORTS_DELETE));

        for (IcebergFileFormat fileFormat : IcebergFileFormat.values()) {
            String targetTable = "delete_simple_target_" + randomNameSuffix();
            createTableForWrites("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)" + "with (format = '" + fileFormat + "')", targetTable, Optional.of("customer"));

            assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'),('Dog', 6, 'Dogma'), ('Carol', 3, 'Cambridge')", targetTable), 3);

            assertUpdate("DELETE FROM " + targetTable + " WHERE purchases <= 4", 1);

            assertQuery("SELECT * FROM " + targetTable, "VALUES ('Aaron', 5, 'Antioch'),('Dog', 6, 'Dogma')");

            assertUpdate("DROP TABLE " + targetTable);
        }
    }

    @Test
    public void testMergeSimpleUpdateInsert()
    {
        skipTestUnless(hasBehavior(SUPPORTS_MERGE));

        for (IcebergFileFormat fileFormat : IcebergFileFormat.values()) {
            String targetTable = "merge_simple_target_" + randomNameSuffix();
            String sourceTable = "merge_simple_source_" + randomNameSuffix();
            createTableForWrites("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)" + "with (format = '" + fileFormat + "')", targetTable, Optional.of("customer"));

            assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable), 4);

            createTableForWrites("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)" + "with (format = '" + fileFormat + "')", sourceTable, Optional.empty());

            assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 6, 'Arches'), ('Ed', 7, 'Etherville'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire')", sourceTable), 4);

            assertUpdate(format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                    "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases + t.purchases, address = s.address" +
                    "    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)", 4);

            assertQuery("SELECT * FROM " + targetTable, "VALUES ('Aaron', 11, 'Arches'), ('Ed', 7, 'Etherville'), ('Bill', 7, 'Buena'), ('Carol', 12, 'Centreville'), ('Dave', 22, 'Darbyshire')");

            assertUpdate("DROP TABLE " + sourceTable);
            assertUpdate("DROP TABLE " + targetTable);
        }
    }

//    @Test
//    public void testMergeSimpleSelect()
//    {
//        skipTestUnless(hasBehavior(SUPPORTS_MERGE));
//
//        for (IcebergFileFormat fileFormat : IcebergFileFormat.values()) {
//            String targetTable = "merge_simple_target_" + randomNameSuffix();
//            String sourceTable = "merge_simple_source_" + randomNameSuffix();
//            createTableForWrites("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)" + "with (format = '" + fileFormat + "')", targetTable, Optional.of("customer"));
//
//            assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable), 4);
//
//            createTableForWrites("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)" + "with (format = '" + fileFormat + "')", sourceTable, Optional.empty());
//
//            assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 6, 'Arches'), ('Ed', 7, 'Etherville'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire')", sourceTable), 4);
//
//            assertUpdate(format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
//                    "    WHEN MATCHED AND s.address = 'Centreville' THEN DELETE" +
//                    "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases + t.purchases, address = s.address" +
//                    "    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)", 4);
//
//            assertQuery("SELECT * FROM " + targetTable, "VALUES ('Aaron', 11, 'Arches'), ('Ed', 7, 'Etherville'), ('Bill', 7, 'Buena'), ('Dave', 22, 'Darbyshire')");
//
//            assertUpdate("DROP TABLE " + sourceTable);
//            assertUpdate("DROP TABLE " + targetTable);
//        }
//    }

    @Test
    public void testCopyOnWriteUpdate()
    {
        for (IcebergFileFormat fileFormat : IcebergFileFormat.values()) {
            String tableName = "test_update" + randomNameSuffix();
            assertUpdate("CREATE TABLE " + tableName + " with (format = '" + fileFormat + "')" + " AS SELECT * FROM nation", 25);

            assertUpdate("UPDATE " + tableName + " SET nationkey = 100 + nationkey WHERE regionkey = 2", 5);
            assertThat(query("SELECT * FROM " + tableName))
                    .skippingTypesCheck()
                    .matches("SELECT IF(regionkey=2, nationkey + 100, nationkey) nationkey, name, regionkey, comment FROM tpch.tiny.nation");
            assertQuery(
                    "SELECT summary['total-delete-files'] FROM \"" + tableName + "$snapshots\" WHERE snapshot_id = " + getCurrentSnapshotId(tableName),
                    "VALUES '0'");

            // UPDATE after UPDATE
            assertUpdate("UPDATE " + tableName + " SET nationkey = nationkey * 2 WHERE regionkey IN (2,3)", 10);
            assertThat(query("SELECT * FROM " + tableName))
                    .skippingTypesCheck()
                    .matches("SELECT CASE regionkey WHEN 2 THEN 2*(nationkey+100) WHEN 3 THEN 2*nationkey ELSE nationkey END nationkey, name, regionkey, comment FROM tpch.tiny.nation");
            // Undeterministic number of files added, just check there is no delete files
            assertQuery(
                    "SELECT summary['total-delete-files'] FROM \"" + tableName + "$snapshots\" WHERE snapshot_id = " + getCurrentSnapshotId(tableName),
                    "VALUES '0'");
        }
    }

    @Test
    public void testCopyOnWriteUpdateMixed()
    {
        for (WriteChangeMode mode : WriteChangeMode.values()) {
            for (IcebergFileFormat fileFormat : IcebergFileFormat.values()) {
                String tableName = "test_update" + randomNameSuffix();
                assertUpdate("CREATE TABLE " + tableName + " with (format = '" + fileFormat + "')" + " AS SELECT * FROM nation", 25);

                mode = mode.alternate();
                assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES write_change_mode = '" + mode + "'");
                assertUpdate("UPDATE " + tableName + " SET nationkey = 100 + nationkey WHERE regionkey = 2", 5);
                assertThat(query("SELECT * FROM " + tableName))
                        .skippingTypesCheck()
                        .matches("SELECT IF(regionkey=2, nationkey + 100, nationkey) nationkey, name, regionkey, comment FROM tpch.tiny.nation");

                // UPDATE after UPDATE
                mode = mode.alternate();
                assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES write_change_mode = '" + mode + "'");
                assertUpdate("UPDATE " + tableName + " SET nationkey = nationkey * 2 WHERE regionkey IN (2,3)", 10);
                assertThat(query("SELECT * FROM " + tableName))
                        .skippingTypesCheck()
                        .matches("SELECT CASE regionkey WHEN 2 THEN 2*(nationkey+100) WHEN 3 THEN 2*nationkey ELSE nationkey END nationkey, name, regionkey, comment FROM tpch.tiny.nation");
                // Undeterministic number of files added, just check there is no delete files
            }
        }
    }

    @Test
    public void testCopyOnWriteMerge()
    {
        for (IcebergFileFormat fileFormat : IcebergFileFormat.values()) {
            String targetTable = "merge_various_target_" + randomNameSuffix();
            String sourceTable = "merge_various_source_" + randomNameSuffix();
            createTableForWrites("CREATE TABLE %s (customer VARCHAR, purchase VARCHAR)" + " with (format = '" + fileFormat + "')", targetTable, Optional.empty());

            assertUpdate(format("INSERT INTO %s (customer, purchase) VALUES ('Dave', 'dates'), ('Lou', 'limes'), ('Carol', 'candles')", targetTable), 3);

            createTableForWrites("CREATE TABLE %s (customer VARCHAR, purchase VARCHAR)" + " with (format = '" + fileFormat + "')", sourceTable, Optional.empty());

            assertUpdate(format("INSERT INTO %s (customer, purchase) VALUES ('Craig', 'candles'), ('Len', 'limes'), ('Joe', 'jellybeans')", sourceTable), 3);

            assertUpdate(format("MERGE INTO %s t USING %s s ON (t.purchase = s.purchase)", targetTable, sourceTable) +
                    "    WHEN MATCHED AND s.purchase = 'limes' THEN DELETE" +
                    "    WHEN MATCHED THEN UPDATE SET customer = CONCAT(t.customer, '_', s.customer)" +
                    "    WHEN NOT MATCHED THEN INSERT (customer, purchase) VALUES(s.customer, s.purchase)", 3);

            assertQuery("SELECT * FROM " + targetTable, "VALUES ('Dave', 'dates'), ('Carol_Craig', 'candles'), ('Joe', 'jellybeans')");

            assertQuery(
                    "SELECT summary['total-delete-files'] FROM \"" + targetTable + "$snapshots\" WHERE snapshot_id = " + getCurrentSnapshotId(targetTable),
                    "VALUES '0'");

            assertUpdate("DROP TABLE " + sourceTable);
            assertUpdate("DROP TABLE " + targetTable);
        }
    }

    @Override
    protected Optional<TypeCoercionTestSetup> filterTypeCoercionOnCreateTableAsSelectProvider(TypeCoercionTestSetup setup)
    {
        return Optional.of(setup);
    }

    @Test
    public void testCopyOnWriteDeleteMixed()
    {
        // delete successive parts of the table
        for (WriteChangeMode mode : WriteChangeMode.values()) {
            for (IcebergFileFormat fileFormat : IcebergFileFormat.values()) {
                String tableName = "test_delete_" + randomNameSuffix();
                assertUpdate("CREATE TABLE " + tableName + " with (format = '" + fileFormat + "')" + " AS SELECT * FROM orders", 15000);

                mode = mode.alternate();
                assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES write_change_mode = '" + mode + "'");
                assertUpdate("DELETE FROM " + tableName + " WHERE custkey <= 100", "SELECT count(*) FROM orders WHERE custkey <= 100");
                assertQuery("SELECT * FROM " + tableName, "SELECT * FROM orders WHERE custkey > 100");

                mode = mode.alternate();
                assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES write_change_mode = '" + mode + "'");
                assertUpdate("DELETE FROM " + tableName + " WHERE custkey <= 300", "SELECT count(*) FROM orders WHERE custkey > 100 AND custkey <= 300");
                assertQuery("SELECT * FROM " + tableName, "SELECT * FROM orders WHERE custkey > 300");

                mode = mode.alternate();
                assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES write_change_mode = '" + mode + "'");
                assertUpdate("DELETE FROM " + tableName + " WHERE custkey <= 500", "SELECT count(*) FROM orders WHERE custkey > 300 AND custkey <= 500");
                assertQuery("SELECT * FROM " + tableName, "SELECT * FROM orders WHERE custkey > 500");
            }
        }
    }

    @Override
    @Test
    public void testOptimizeTableAfterDeleteWithFormatVersion2()
    {
        String tableName = "test_optimize_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM nation", 25);

        List<String> initialFiles = getActiveFiles(tableName);

        assertUpdate("DELETE FROM " + tableName + " WHERE nationkey = 7", 1);

        // Verify that delete files do not exist
        assertQuery(
                "SELECT summary['total-delete-files'] FROM \"" + tableName + "$snapshots\" WHERE snapshot_id = " + getCurrentSnapshotId(tableName),
                "VALUES '0'");

        // Verify that data files are added and removed
        assertQuery(
                "SELECT summary['added-data-files'] FROM \"" + tableName + "$snapshots\" WHERE snapshot_id = " + getCurrentSnapshotId(tableName),
                "VALUES '1'");

        // For optimize we need to set task_writer_count to 1, otherwise it will create more than one file.
        computeActual(withSingleWriterPerTask(getSession()), "ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");

        List<String> updatedFiles = getActiveFiles(tableName);
        assertThat(updatedFiles)
                .hasSize(1)
                .isNotEqualTo(initialFiles);

        assertThat(query("SELECT * FROM " + tableName))
                .matches("SELECT * FROM nation WHERE nationkey != 7");

        assertUpdate("DROP TABLE " + tableName);
    }

    private List<String> getActiveFiles(String tableName)
    {
        return computeActual(format("SELECT file_path FROM \"%s$files\"", tableName)).getOnlyColumn()
                .map(String.class::cast)
                .collect(toImmutableList());
    }

    private Session withSingleWriterPerTask(Session session)
    {
        return Session.builder(session)
                .setSystemProperty("task_min_writer_count", "1")
                .build();
    }

    private Session with(Session session)
    {
        return Session.builder(session)
                .setSystemProperty("task_min_writer_count", "1")
                .build();
    }

    private long getCurrentSnapshotId(String tableName)
    {
        return (long) computeScalar("SELECT snapshot_id FROM \"" + tableName + "$snapshots\" ORDER BY committed_at DESC FETCH FIRST 1 ROW WITH TIES");
    }

    @Override
    @Test
    public void testOptimizeCleansUpDeleteFiles()
            throws IOException
    {
        String tableName = "test_optimize_" + randomNameSuffix();
        Session sessionWithShortRetentionUnlocked = prepareCleanUpSession();
        assertUpdate("CREATE TABLE " + tableName + " WITH (partitioning = ARRAY['regionkey']) AS SELECT * FROM nation", 25);

        List<String> allDataFilesInitially = getAllDataFilesFromTableDirectory(tableName);
        assertThat(allDataFilesInitially).hasSize(5);

        assertUpdate("DELETE FROM " + tableName + " WHERE nationkey = 7", 1);

        assertQuery(
                "SELECT summary['total-delete-files'] FROM \"" + tableName + "$snapshots\" WHERE snapshot_id = " + getCurrentSnapshotId(tableName),
                "VALUES '0'");

        List<String> allDataFilesAfterDelete = getAllDataFilesFromTableDirectory(tableName);
        assertThat(allDataFilesAfterDelete).hasSize(6);

        // For optimize we need to set task_min_writer_count to 1, otherwise it will create more than one file.
        computeActual(withSingleWriterPerTask(getSession()), "ALTER TABLE " + tableName + " EXECUTE OPTIMIZE WHERE regionkey = 3");
        computeActual(sessionWithShortRetentionUnlocked, "ALTER TABLE " + tableName + " EXECUTE EXPIRE_SNAPSHOTS (retention_threshold => '0s')");
        computeActual(sessionWithShortRetentionUnlocked, "ALTER TABLE " + tableName + " EXECUTE REMOVE_ORPHAN_FILES (retention_threshold => '0s')");

        assertQuery(
                "SELECT summary['total-delete-files'] FROM \"" + tableName + "$snapshots\" WHERE snapshot_id = " + getCurrentSnapshotId(tableName),
                "VALUES '0'");
        List<String> allDataFilesAfterOptimizeWithWhere = getAllDataFilesFromTableDirectory(tableName);
        assertThat(allDataFilesAfterOptimizeWithWhere)
                .hasSize(5)
                .doesNotContain(allDataFilesInitially.stream().filter(file -> file.contains("regionkey=3"))
                        .toArray(String[]::new))
                .contains(allDataFilesInitially.stream().filter(file -> !file.contains("regionkey=3"))
                        .toArray(String[]::new));

        assertThat(query("SELECT * FROM " + tableName))
                .matches("SELECT * FROM nation WHERE nationkey != 7");

        // For optimize we need to set task_min_writer_count to 1, otherwise it will create more than one file.
        computeActual(withSingleWriterPerTask(getSession()), "ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");
        computeActual(sessionWithShortRetentionUnlocked, "ALTER TABLE " + tableName + " EXECUTE EXPIRE_SNAPSHOTS (retention_threshold => '0s')");
        computeActual(sessionWithShortRetentionUnlocked, "ALTER TABLE " + tableName + " EXECUTE REMOVE_ORPHAN_FILES (retention_threshold => '0s')");

        assertQuery(
                "SELECT summary['total-delete-files'] FROM \"" + tableName + "$snapshots\" WHERE snapshot_id = " + getCurrentSnapshotId(tableName),
                "VALUES '0'");
        List<String> allDataFilesAfterFullOptimize = getAllDataFilesFromTableDirectory(tableName);
        assertThat(allDataFilesAfterFullOptimize)
                .hasSize(5)
                // All files skipped from OPTIMIZE as they have no deletes and there's only one file per partition
                .contains(allDataFilesAfterOptimizeWithWhere.toArray(new String[0]));

        assertThat(query("SELECT * FROM " + tableName))
                .matches("SELECT * FROM nation WHERE nationkey != 7");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Override
    @Test
    public void testOptimizeFilesDoNotInheritSequenceNumber()
            throws IOException
    {
        String tableName = "test_optimize_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM nation", 25);

        assertUpdate("DELETE FROM " + tableName + " WHERE nationkey = 7", 1);

        // For optimize we need to set task_min_writer_count to 1, otherwise it will create more than one file.
        computeActual(withSingleWriterPerTask(getSession()), "ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");

        List<IcebergEntry> activeEntries = getIcebergEntries(tableName);
        assertThat(activeEntries).hasSize(2);

        assertThat(activeEntries.stream().filter(entry -> entry.status() == 2))
                .hasSize(1)
                .allMatch(entry -> entry.sequenceNumber().equals(entry.fileSequenceNumber()));

        assertThat(query("SELECT * FROM " + tableName))
                .matches("SELECT * FROM nation WHERE nationkey != 7");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testRowGroupResetDictionary()
    {
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_row_group_reset_dictionary",
                "(plain_col varchar, dict_col int)")) {
            String tableName = table.getName();
            String values = IntStream.range(0, 100)
                    .mapToObj(i -> "('ABCDEFGHIJ" + i + "' , " + (i < 20 ? "1" : "null") + ")")
                    .collect(Collectors.joining(", "));
            assertUpdate(withSmallRowGroups(getSession()), "INSERT INTO " + tableName + " VALUES " + values, 100);

            MaterializedResult result = getDistributedQueryRunner().execute(String.format("SELECT * FROM %s", tableName));
            assertThat(result.getRowCount()).isEqualTo(100);
        }
    }

    @Override
    protected Optional<SetColumnTypeSetup> filterSetColumnTypesDataProvider(SetColumnTypeSetup setup)
    {
        switch ("%s -> %s".formatted(setup.sourceColumnType(), setup.newColumnType())) {
            case "row(x integer) -> row(y integer)":
                // TODO https://github.com/trinodb/trino/issues/15822 The connector returns incorrect NULL when a field in row type doesn't exist in Parquet files
                return Optional.of(setup.withNewValueLiteral("NULL"));
        }
        return super.filterSetColumnTypesDataProvider(setup);
    }

    @Override
    protected boolean isFileSorted(String path, String sortColumnName)
    {
        return checkParquetFileSorting(
                fileSystem.newInputFile(Location.of(path)),
                sortColumnName);
    }
}
