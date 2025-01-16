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
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import io.trino.Session;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.iceberg.fileio.ForwardingFileIo;
import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.FileIO;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getMetadataFileAndUpdatedMillis;
import static io.trino.plugin.iceberg.IcebergTestUtils.withSmallRowGroups;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.DROP_TABLE;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public abstract class BaseIcebergConnectorSmokeTest
        extends BaseConnectorSmokeTest
{
    protected final FileFormat format;
    protected TrinoFileSystem fileSystem;

    public BaseIcebergConnectorSmokeTest(FileFormat format)
    {
        this.format = requireNonNull(format, "format is null");
    }

    @BeforeAll
    public void initFileSystem()
    {
        fileSystem = getFileSystemFactory(getDistributedQueryRunner()).create(SESSION);
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_TOPN_PUSHDOWN -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .matches("" +
                        "CREATE TABLE iceberg." + schemaName + ".region \\(\n" +
                        "   regionkey bigint,\n" +
                        "   name varchar,\n" +
                        "   comment varchar\n" +
                        "\\)\n" +
                        "WITH \\(\n" +
                        "   format = '" + format.name() + "',\n" +
                        "   format_version = 2,\n" +
                        format("   location = '.*/" + schemaName + "/region.*'\n") +
                        "\\)");
    }

    @Test
    public void testHiddenPathColumn()
    {
        try (TestTable table = newTrinoTable("hidden_file_path", "(a int, b VARCHAR)", ImmutableList.of("(1, 'a')"))) {
            String filePath = (String) computeScalar(format("SELECT file_path FROM \"%s$files\"", table.getName()));

            assertQuery("SELECT DISTINCT \"$path\" FROM " + table.getName(), "VALUES " + "'" + filePath + "'");

            // Check whether the "$path" hidden column is correctly evaluated in the filter expression
            assertQuery(format("SELECT a FROM %s WHERE \"$path\" = '%s'", table.getName(), filePath), "VALUES 1");
        }
    }

    // Repeat test with invocationCount for better test coverage, since the tested aspect is inherently non-deterministic.
    @RepeatedTest(4)
    @Timeout(120)
    @Execution(ExecutionMode.SAME_THREAD)
    public void testDeleteRowsConcurrently()
            throws Exception
    {
        int threads = 4;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        List<String> rows = ImmutableList.of("(1, 0, 0, 0)", "(0, 1, 0, 0)", "(0, 0, 1, 0)", "(0, 0, 0, 1)");

        String[] expectedErrors = new String[] {"Failed to commit the transaction during write:",
                "Failed to replace table due to concurrent updates:",
                "Failed to commit during write:"};
        try (TestTable table = newTrinoTable(
                "test_concurrent_delete",
                "(col0 INTEGER, col1 INTEGER, col2 INTEGER, col3 INTEGER)")) {
            String tableName = table.getName();
            assertUpdate("INSERT INTO " + tableName + " VALUES " + String.join(", ", rows), 4);

            List<Future<Boolean>> futures = IntStream.range(0, threads)
                    .mapToObj(threadNumber -> executor.submit(() -> {
                        barrier.await(10, SECONDS);
                        String columnName = "col" + threadNumber;
                        try {
                            getQueryRunner().execute(format("DELETE FROM %s WHERE %s = 1", tableName, columnName));
                            return true;
                        }
                        catch (Exception e) {
                            assertThat(e.getMessage()).containsAnyOf(expectedErrors);
                            return false;
                        }
                    }))
                    .collect(toImmutableList());

            Stream<Optional<String>> expectedRows = Streams.mapWithIndex(futures.stream(), (future, index) -> {
                Optional<Boolean> value = tryGetFutureValue(future, 20, SECONDS);
                checkState(value.isPresent(), "Task %s did not complete in time", index);
                boolean deleteSuccessful = value.get();
                return deleteSuccessful ? Optional.empty() : Optional.of(rows.get((int) index));
            });
            List<String> expectedValues = expectedRows.filter(Optional::isPresent).map(Optional::get).collect(toImmutableList());
            assertThat(expectedValues).as("Expected at least one delete operation to pass").hasSizeLessThan(rows.size());
            assertThat(query("SELECT * FROM " + tableName)).matches("VALUES " + String.join(", ", expectedValues));
        }
        finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @Test
    public void testCreateOrReplaceTable()
    {
        try (TestTable table = newTrinoTable(
                "test_create_or_replace",
                " AS SELECT BIGINT '42' a, DOUBLE '-38.5' b")) {
            assertThat(query("SELECT a, b FROM " + table.getName()))
                    .matches("VALUES (BIGINT '42', -385e-1)");

            long v1SnapshotId = getMostRecentSnapshotId(table.getName());

            assertUpdate("CREATE OR REPLACE TABLE " + table.getName() + " AS SELECT BIGINT '-42' a, DOUBLE '38.5' b", 1);
            assertThat(query("SELECT a, b FROM " + table.getName()))
                    .matches("VALUES (BIGINT '-42', 385e-1)");

            assertThat(query("SELECT COUNT(snapshot_id) FROM \"" + table.getName() + "$history\""))
                    .matches("VALUES BIGINT '2'");

            assertThat(query("SELECT a, b  FROM " + table.getName() + " FOR VERSION AS OF " + v1SnapshotId))
                    .matches("VALUES (BIGINT '42', -385e-1)");
        }
    }

    @Test
    public void testCreateOrReplaceTableChangeColumnNamesAndTypes()
    {
        String tableName = "test_create_or_replace_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT BIGINT '42' a, DOUBLE '-38.5' b", 1);
        assertThat(query("SELECT CAST(a AS bigint), b FROM " + tableName))
                .matches("VALUES (BIGINT '42', -385e-1)");

        long v1SnapshotId = getMostRecentSnapshotId(tableName);

        assertUpdate("CREATE OR REPLACE TABLE " + tableName + " AS SELECT VARCHAR 'test' c, VARCHAR 'test2' d", 1);
        assertThat(query("SELECT c, d FROM " + tableName))
                .matches("VALUES (VARCHAR 'test', VARCHAR 'test2')");

        assertThat(query("SELECT a, b  FROM " + tableName + " FOR VERSION AS OF " + v1SnapshotId))
                .matches("VALUES (BIGINT '42', -385e-1)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testRegisterTableWithTableLocation()
    {
        String tableName = "test_register_table_with_table_location_" + randomNameSuffix();

        assertUpdate(format("CREATE TABLE %s (a int, b varchar, c boolean)", tableName));
        assertUpdate(format("INSERT INTO %s values(1, 'INDIA', true)", tableName), 1);
        assertUpdate(format("INSERT INTO %s values(2, 'USA', false)", tableName), 1);

        String tableLocation = getTableLocation(tableName);
        // Drop table from hive metastore and use the same table name to register again with the metadata
        dropTableFromMetastore(tableName);

        assertUpdate("CALL system.register_table (CURRENT_SCHEMA, '" + tableName + "', '" + tableLocation + "')");

        assertThat(query(format("SELECT * FROM %s", tableName)))
                .matches("VALUES " +
                        "ROW(INT '1', VARCHAR 'INDIA', BOOLEAN 'true'), " +
                        "ROW(INT '2', VARCHAR 'USA', BOOLEAN 'false')");
        assertUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    public void testRegisterTableWithComments()
    {
        String tableName = "test_register_table_with_comments_" + randomNameSuffix();

        assertUpdate(format("CREATE TABLE %s (a int, b varchar, c boolean)", tableName));
        assertUpdate(format("INSERT INTO %s values(1, 'INDIA', true)", tableName), 1);
        assertUpdate(format("COMMENT ON TABLE %s is 'my-table-comment'", tableName));
        assertUpdate(format("COMMENT ON COLUMN %s.a is 'a-comment'", tableName));
        assertUpdate(format("COMMENT ON COLUMN %s.b is 'b-comment'", tableName));
        assertUpdate(format("COMMENT ON COLUMN %s.c is 'c-comment'", tableName));

        String tableLocation = getTableLocation(tableName);
        // Drop table from hive metastore and use the same table name to register again with the metadata
        dropTableFromMetastore(tableName);

        assertUpdate("CALL system.register_table (CURRENT_SCHEMA, '" + tableName + "', '" + tableLocation + "')");

        assertThat(getTableComment(tableName)).isEqualTo("my-table-comment");
        assertThat(getColumnComment(tableName, "a")).isEqualTo("a-comment");
        assertThat(getColumnComment(tableName, "b")).isEqualTo("b-comment");
        assertThat(getColumnComment(tableName, "c")).isEqualTo("c-comment");
        assertUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    public void testRegisterTableWithShowCreateTable()
    {
        String tableName = "test_register_table_with_show_create_table_" + randomNameSuffix();

        assertUpdate(format("CREATE TABLE %s (a int, b varchar, c boolean)", tableName));
        assertUpdate(format("INSERT INTO %s values(1, 'INDIA', true)", tableName), 1);

        String tableLocation = getTableLocation(tableName);
        String showCreateTableOld = (String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue();
        // Drop table from hive metastore and use the same table name to register again with the metadata
        dropTableFromMetastore(tableName);

        assertUpdate("CALL system.register_table (CURRENT_SCHEMA, '" + tableName + "', '" + tableLocation + "')");
        String showCreateTableNew = (String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue();

        assertThat(showCreateTableOld).isEqualTo(showCreateTableNew);
        assertUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    public void testRegisterTableWithReInsert()
    {
        String tableName = "test_register_table_with_re_insert_" + randomNameSuffix();

        assertUpdate(format("CREATE TABLE %s (a int, b varchar, c boolean)", tableName));
        assertUpdate(format("INSERT INTO %s values(1, 'INDIA', true)", tableName), 1);
        assertUpdate(format("INSERT INTO %s values(2, 'USA', false)", tableName), 1);

        String tableLocation = getTableLocation(tableName);
        // Drop table from hive metastore and use the same table name to register again with the metadata
        dropTableFromMetastore(tableName);

        assertUpdate("CALL system.register_table (CURRENT_SCHEMA, '" + tableName + "', '" + tableLocation + "')");
        assertUpdate(format("INSERT INTO %s values(3, 'POLAND', true)", tableName), 1);

        assertThat(query(format("SELECT * FROM %s", tableName)))
                .matches("VALUES " +
                        "ROW(INT '1', VARCHAR 'INDIA', BOOLEAN 'true'), " +
                        "ROW(INT '2', VARCHAR 'USA', BOOLEAN 'false'), " +
                        "ROW(INT '3', VARCHAR 'POLAND', BOOLEAN 'true')");
        assertUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    public void testRegisterTableWithDroppedTable()
    {
        String tableName = "test_register_table_with_dropped_table_" + randomNameSuffix();

        assertUpdate(format("CREATE TABLE %s (a int, b varchar, c boolean)", tableName));
        assertUpdate(format("INSERT INTO %s values(1, 'INDIA', true)", tableName), 1);

        String tableLocation = getTableLocation(tableName);
        String tableNameNew = tableName + "_new";
        // Drop table to verify register_table call fails when no metadata can be found (table doesn't exist)
        assertUpdate(format("DROP TABLE %s", tableName));

        assertQueryFails(format("CALL system.register_table (CURRENT_SCHEMA, '%s', '%s')", tableNameNew, tableLocation),
                ".*No versioned metadata file exists at location.*");
    }

    @Test
    public void testRegisterTableWithDifferentTableName()
    {
        String tableName = "test_register_table_with_different_table_name_" + randomNameSuffix();

        assertUpdate(format("CREATE TABLE %s (a int, b varchar, c boolean)", tableName));
        assertUpdate(format("INSERT INTO %s values(1, 'INDIA', true)", tableName), 1);
        assertUpdate(format("INSERT INTO %s values(2, 'USA', false)", tableName), 1);

        String tableLocation = getTableLocation(tableName);
        String tableNameNew = tableName + "_new";
        // Drop table from glue metastore and use the same table name to register again with the metadata
        dropTableFromMetastore(tableName);

        assertUpdate(format("CALL system.register_table (CURRENT_SCHEMA, '%s', '%s')", tableNameNew, tableLocation));
        assertUpdate(format("INSERT INTO %s values(3, 'POLAND', true)", tableNameNew), 1);

        assertThat(query(format("SELECT * FROM %s", tableNameNew)))
                .matches("VALUES " +
                        "ROW(INT '1', VARCHAR 'INDIA', BOOLEAN 'true'), " +
                        "ROW(INT '2', VARCHAR 'USA', BOOLEAN 'false'), " +
                        "ROW(INT '3', VARCHAR 'POLAND', BOOLEAN 'true')");
        assertUpdate(format("DROP TABLE %s", tableNameNew));
    }

    @Test
    public void testRegisterTableWithMetadataFile()
    {
        String tableName = "test_register_table_with_metadata_file_" + randomNameSuffix();

        assertUpdate(format("CREATE TABLE %s (a int, b varchar, c boolean)", tableName));
        assertUpdate(format("INSERT INTO %s values(1, 'INDIA', true)", tableName), 1);
        assertUpdate(format("INSERT INTO %s values(2, 'USA', false)", tableName), 1);

        String tableLocation = getTableLocation(tableName);
        String metadataLocation = getMetadataLocation(tableName);
        String metadataFileName = metadataLocation.substring(metadataLocation.lastIndexOf("/") + 1);
        // Drop table from hive metastore and use the same table name to register again with the metadata
        dropTableFromMetastore(tableName);

        assertUpdate("CALL iceberg.system.register_table (CURRENT_SCHEMA, '" + tableName + "', '" + tableLocation + "', '" + metadataFileName + "')");
        assertUpdate(format("INSERT INTO %s values(3, 'POLAND', true)", tableName), 1);

        assertThat(query(format("SELECT * FROM %s", tableName)))
                .matches("VALUES " +
                        "ROW(INT '1', VARCHAR 'INDIA', BOOLEAN 'true'), " +
                        "ROW(INT '2', VARCHAR 'USA', BOOLEAN 'false'), " +
                        "ROW(INT '3', VARCHAR 'POLAND', BOOLEAN 'true')");
        assertUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    public void testCreateTableWithTrailingSpaceInLocation()
    {
        String tableName = "test_create_table_with_trailing_space_" + randomNameSuffix();
        String tableLocationWithTrailingSpace = schemaPath() + tableName + " ";

        assertQuerySucceeds(format("CREATE TABLE %s WITH (location = '%s') AS SELECT 1 AS a, 'INDIA' AS b, true AS c", tableName, tableLocationWithTrailingSpace));
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'INDIA', true)");

        assertThat(getTableLocation(tableName)).isEqualTo(tableLocationWithTrailingSpace);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testRegisterTableWithTrailingSpaceInLocation()
    {
        String tableName = "test_create_table_with_trailing_space_" + randomNameSuffix();
        String tableLocationWithTrailingSpace = schemaPath() + tableName + " ";

        assertQuerySucceeds(format("CREATE TABLE %s WITH (location = '%s') AS SELECT 1 AS a, 'INDIA' AS b, true AS c", tableName, tableLocationWithTrailingSpace));

        String registeredTableName = "test_register_table_with_trailing_space_" + randomNameSuffix();
        assertUpdate(format("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')", registeredTableName, tableLocationWithTrailingSpace));
        assertQuery("SELECT * FROM " + registeredTableName, "VALUES (1, 'INDIA', true)");

        assertThat(getTableLocation(registeredTableName)).isEqualTo(tableLocationWithTrailingSpace);

        assertUpdate("DROP TABLE " + registeredTableName);
        dropTableFromMetastore(tableName);
    }

    @Test
    public void testUnregisterTable()
    {
        String tableName = "test_unregister_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 a", 1);
        String tableLocation = getTableLocation(tableName);

        assertUpdate("CALL system.unregister_table(CURRENT_SCHEMA, '" + tableName + "')");
        assertQueryFails("SELECT * FROM " + tableName, ".* Table .* does not exist");

        assertUpdate("CALL iceberg.system.register_table(CURRENT_SCHEMA, '" + tableName + "', '" + tableLocation + "')");
        assertQuery("SELECT * FROM " + tableName, "VALUES 1");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testUnregisterBrokenTable()
    {
        String tableName = "test_unregister_broken_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 a", 1);
        String tableLocation = getTableLocation(tableName);

        // Break the table by deleting files from the storage
        deleteDirectory(tableLocation);

        // Verify unregister_table successfully deletes the table from metastore
        assertUpdate("CALL system.unregister_table(CURRENT_SCHEMA, '" + tableName + "')");
        assertQueryFails("SELECT * FROM " + tableName, ".* Table .* does not exist");
    }

    protected abstract void deleteDirectory(String location);

    @Test
    public void testUnregisterTableNotExistingSchema()
    {
        String schemaName = "test_unregister_table_not_existing_schema_" + randomNameSuffix();
        assertQueryFails(
                "CALL system.unregister_table('" + schemaName + "', 'non_existent_table')",
                "Schema " + schemaName + " not found");
    }

    @Test
    public void testUnregisterTableNotExistingTable()
    {
        String tableName = "test_unregister_table_not_existing_table_" + randomNameSuffix();
        assertQueryFails(
                "CALL system.unregister_table(CURRENT_SCHEMA, '" + tableName + "')",
                "Table .* not found");
    }

    @Test
    public void testRepeatUnregisterTable()
    {
        String tableName = "test_repeat_unregister_table_not_" + randomNameSuffix();
        assertQueryFails(
                "CALL system.unregister_table(CURRENT_SCHEMA, '" + tableName + "')",
                "Table .* not found");

        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 a", 1);
        String tableLocation = getTableLocation(tableName);

        assertUpdate("CALL system.unregister_table(CURRENT_SCHEMA, '" + tableName + "')");

        // Verify failure the procedure can't unregister the tables more than once
        assertQueryFails("CALL system.unregister_table(CURRENT_SCHEMA, '" + tableName + "')", "Table .* not found");

        assertUpdate("CALL iceberg.system.register_table(CURRENT_SCHEMA, '" + tableName + "', '" + tableLocation + "')");
        assertQuery("SELECT * FROM " + tableName, "VALUES 1");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testUnregisterTableAccessControl()
    {
        String tableName = "test_unregister_table_access_control_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 a", 1);

        assertAccessDenied(
                "CALL system.unregister_table(CURRENT_SCHEMA, '" + tableName + "')",
                "Cannot drop table .*",
                privilege(tableName, DROP_TABLE));

        assertQuery("SELECT * FROM " + tableName, "VALUES 1");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateTableWithNonExistingSchemaVerifyLocation()
    {
        String schemaName = "non_existing_schema_" + randomNameSuffix();
        String tableName = "test_create_table_in_non_existent_schema_" + randomNameSuffix();
        String tableLocation = schemaPath() + "/" + tableName;
        assertQueryFails(
                "CREATE TABLE " + schemaName + "." + tableName + " (a int, b int) WITH (location = '" + tableLocation + "')",
                "Schema (.*) not found");
        assertThat(locationExists(tableLocation))
                .as("location should not exist").isFalse();

        assertQueryFails(
                "CREATE TABLE " + schemaName + "." + tableName + " (a, b) WITH (location = '" + tableLocation + "') AS VALUES (1, 2), (3, 4)",
                "Schema (.*) not found");
        assertThat(locationExists(tableLocation))
                .as("location should not exist").isFalse();
    }

    @Test
    public void testSortedNationTable()
    {
        Session withSmallRowGroups = withSmallRowGroups(getSession());
        try (TestTable table = newTrinoTable(
                "test_sorted_nation_table",
                "WITH (sorted_by = ARRAY['comment'], format = '" + format.name() + "') AS SELECT * FROM nation WITH NO DATA")) {
            assertUpdate(withSmallRowGroups, "INSERT INTO " + table.getName() + " SELECT * FROM nation", 25);
            for (Object filePath : computeActual("SELECT file_path from \"" + table.getName() + "$files\"").getOnlyColumnAsSet()) {
                assertThat(isFileSorted(Location.of((String) filePath), "comment")).isTrue();
            }
            assertQuery("SELECT * FROM " + table.getName(), "SELECT * FROM nation");
        }
    }

    @Test
    public void testFileSortingWithLargerTable()
    {
        // Using a larger table forces buffered data to be written to disk
        Session withSmallRowGroups = Session.builder(getSession())
                .setCatalogSessionProperty("iceberg", "orc_writer_max_stripe_rows", "200")
                .setCatalogSessionProperty("iceberg", "parquet_writer_block_size", "20kB")
                .setCatalogSessionProperty("iceberg", "parquet_writer_batch_size", "200")
                .build();
        try (TestTable table = newTrinoTable(
                "test_sorted_lineitem_table",
                "WITH (sorted_by = ARRAY['comment'], format = '" + format.name() + "') AS TABLE tpch.tiny.lineitem WITH NO DATA")) {
            assertUpdate(
                    withSmallRowGroups,
                    "INSERT INTO " + table.getName() + " TABLE tpch.tiny.lineitem",
                    "VALUES 60175");
            for (Object filePath : computeActual("SELECT file_path from \"" + table.getName() + "$files\"").getOnlyColumnAsSet()) {
                assertThat(isFileSorted(Location.of((String) filePath), "comment")).isTrue();
            }
            assertQuery("SELECT * FROM " + table.getName(), "SELECT * FROM lineitem");
        }
    }

    @Test
    public void testDropTableWithMissingMetadataFile()
            throws Exception
    {
        String tableName = "test_drop_table_with_missing_metadata_file_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 x, 'INDIA' y", 1);

        Location metadataLocation = Location.of(getMetadataLocation(tableName));
        Location tableLocation = Location.of(getTableLocation(tableName));

        // Delete current metadata file
        fileSystem.deleteFile(metadataLocation);
        assertThat(fileSystem.newInputFile(metadataLocation).exists())
                .describedAs("Current metadata file should not exist")
                .isFalse();

        // try to drop table
        assertUpdate("DROP TABLE " + tableName);
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        assertThat(fileSystem.listFiles(tableLocation).hasNext())
                .describedAs("Table location should not exist")
                .isFalse();
    }

    @Test
    public void testDropTableWithMissingSnapshotFile()
            throws Exception
    {
        String tableName = "test_drop_table_with_missing_snapshot_file_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 x, 'INDIA' y", 1);

        String metadataLocation = getMetadataLocation(tableName);
        TableMetadata tableMetadata = TableMetadataParser.read(new ForwardingFileIo(fileSystem), metadataLocation);
        Location tableLocation = Location.of(tableMetadata.location());
        Location currentSnapshotFile = Location.of(tableMetadata.currentSnapshot().manifestListLocation());

        // Delete current snapshot file
        fileSystem.deleteFile(currentSnapshotFile);
        assertThat(fileSystem.newInputFile(currentSnapshotFile).exists())
                .describedAs("Current snapshot file should not exist")
                .isFalse();

        // try to drop table
        assertUpdate("DROP TABLE " + tableName);
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        assertThat(fileSystem.listFiles(tableLocation).hasNext())
                .describedAs("Table location should not exist")
                .isFalse();
    }

    @Test
    public void testDropTableWithMissingManifestListFile()
            throws Exception
    {
        String tableName = "test_drop_table_with_missing_manifest_list_file_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 x, 'INDIA' y", 1);

        String metadataLocation = getMetadataLocation(tableName);
        FileIO fileIo = new ForwardingFileIo(fileSystem);
        TableMetadata tableMetadata = TableMetadataParser.read(fileIo, metadataLocation);
        Location tableLocation = Location.of(tableMetadata.location());
        Location manifestListFile = Location.of(tableMetadata.currentSnapshot().allManifests(fileIo).get(0).path());

        // Delete Manifest List file
        fileSystem.deleteFile(manifestListFile);
        assertThat(fileSystem.newInputFile(manifestListFile).exists())
                .describedAs("Manifest list file should not exist")
                .isFalse();

        // try to drop table
        assertUpdate("DROP TABLE " + tableName);
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        assertThat(fileSystem.listFiles(tableLocation).hasNext())
                .describedAs("Table location should not exist")
                .isFalse();
    }

    @Test
    public void testDropTableWithMissingDataFile()
            throws Exception
    {
        String tableName = "test_drop_table_with_missing_data_file_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 x, 'INDIA' y", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'POLAND')", 1);

        Location tableLocation = Location.of(getTableLocation(tableName));
        Location tableDataPath = tableLocation.appendPath("data");
        FileIterator fileIterator = fileSystem.listFiles(tableDataPath);
        assertThat(fileIterator.hasNext()).isTrue();
        Location dataFile = fileIterator.next().location();

        // Delete data file
        fileSystem.deleteFile(dataFile);
        assertThat(fileSystem.newInputFile(dataFile).exists())
                .describedAs("Data file should not exist")
                .isFalse();

        // try to drop table
        assertUpdate("DROP TABLE " + tableName);
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        assertThat(fileSystem.listFiles(tableLocation).hasNext())
                .describedAs("Table location should not exist")
                .isFalse();
    }

    @Test
    public void testDropTableWithNonExistentTableLocation()
            throws Exception
    {
        String tableName = "test_drop_table_with_non_existent_table_location_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 x, 'INDIA' y", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'POLAND')", 1);

        Location tableLocation = Location.of(getTableLocation(tableName));

        // Delete table location
        fileSystem.deleteDirectory(tableLocation);
        assertThat(fileSystem.listFiles(tableLocation).hasNext())
                .describedAs("Table location should not exist")
                .isFalse();

        // try to drop table
        assertUpdate("DROP TABLE " + tableName);
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
    }

    // Verify the accuracy of Trino metadata tables while retrieving Iceberg table metadata from the underlying `TrinoCatalog` implementation
    @Test
    public void testMetadataTables()
    {
        try (TestTable table = newTrinoTable(
                "test_metadata_tables",
                "(id int, part varchar) WITH (partitioning = ARRAY['part'])")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 'p1')", 1);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (2, 'p1')", 1);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (3, 'p2')", 1);

            List<Long> snapshotIds = computeActual("SELECT snapshot_id FROM \"" + table.getName() + "$snapshots\" ORDER BY committed_at DESC")
                    .getOnlyColumn()
                    .map(Long.class::cast)
                    .collect(toImmutableList());
            List<Long> historySnapshotIds = computeActual("SELECT snapshot_id FROM \"" + table.getName() + "$history\" ORDER BY made_current_at DESC")
                    .getOnlyColumn()
                    .map(Long.class::cast)
                    .collect(toImmutableList());
            long filesCount = (long) computeScalar("SELECT count(*) FROM \"" + table.getName() + "$files\"");
            long partitionsCount = (long) computeScalar("SELECT count(*) FROM \"" + table.getName() + "$partitions\"");

            assertThat(snapshotIds).hasSize(4);
            assertThat(snapshotIds).hasSameElementsAs(historySnapshotIds);
            assertThat(filesCount).isEqualTo(3L);
            assertThat(partitionsCount).isEqualTo(2L);
        }
    }

    @Test
    public void testPartitionFilterRequired()
    {
        String tableName = "test_partition_" + randomNameSuffix();

        Session session = Session.builder(getSession())
                .setCatalogSessionProperty("iceberg", "query_partition_filter_required", "true")
                .build();

        assertUpdate(session, "CREATE TABLE " + tableName + " (id integer, a varchar, b varchar, ds varchar) WITH (partitioning = ARRAY['ds'])");
        assertUpdate(session, "INSERT INTO " + tableName + " (id, a, ds) VALUES (1, 'a', 'a')", 1);
        String query = "SELECT id FROM " + tableName + " WHERE a = 'a'";
        @Language("RegExp") String failureMessage = "Filter required for .*" + tableName + " on at least one of the partition columns: ds";
        assertQueryFails(session, query, failureMessage);
        assertQueryFails(session, "EXPLAIN " + query, failureMessage);
        assertUpdate(session, "DROP TABLE " + tableName);
    }

    protected abstract boolean isFileSorted(Location path, String sortColumnName);

    @Test
    public void testTableChangesFunction()
    {
        DateTimeFormatter instantMillisFormatter = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSVV").withZone(UTC);

        try (TestTable table = newTrinoTable(
                "test_table_changes_function_",
                "AS SELECT nationkey, name FROM tpch.tiny.nation WITH NO DATA")) {
            long initialSnapshot = getMostRecentSnapshotId(table.getName());
            assertUpdate("INSERT INTO " + table.getName() + " SELECT nationkey, name FROM nation", 25);
            long snapshotAfterInsert = getMostRecentSnapshotId(table.getName());
            String snapshotAfterInsertTime = getSnapshotTime(table.getName(), snapshotAfterInsert).format(instantMillisFormatter);

            assertQuery(
                    "SELECT nationkey, name, _change_type, _change_version_id, to_iso8601(_change_timestamp), _change_ordinal " +
                            "FROM TABLE(system.table_changes(CURRENT_SCHEMA, '%s', %s, %s))".formatted(table.getName(), initialSnapshot, snapshotAfterInsert),
                    "SELECT nationkey, name, 'insert', %s, '%s', 0 FROM nation".formatted(snapshotAfterInsert, snapshotAfterInsertTime));

            // Run with named arguments
            assertQuery(
                    "SELECT nationkey, name, _change_type, _change_version_id, to_iso8601(_change_timestamp), _change_ordinal " +
                            "FROM TABLE(system.table_changes(schema_name => CURRENT_SCHEMA, table_name => '%s', start_snapshot_id => %s, end_snapshot_id => %s))"
                                    .formatted(table.getName(), initialSnapshot, snapshotAfterInsert),
                    "SELECT nationkey, name, 'insert', %s, '%s', 0 FROM nation".formatted(snapshotAfterInsert, snapshotAfterInsertTime));

            assertUpdate("DELETE FROM " + table.getName(), 25);
            long snapshotAfterDelete = getMostRecentSnapshotId(table.getName());
            String snapshotAfterDeleteTime = getSnapshotTime(table.getName(), snapshotAfterDelete).format(instantMillisFormatter);

            assertQuery(
                    "SELECT nationkey, name, _change_type, _change_version_id, to_iso8601(_change_timestamp), _change_ordinal " +
                            "FROM TABLE(system.table_changes(CURRENT_SCHEMA, '%s', %s, %s))".formatted(table.getName(), snapshotAfterInsert, snapshotAfterDelete),
                    "SELECT nationkey, name, 'delete', %s, '%s', 0 FROM nation".formatted(snapshotAfterDelete, snapshotAfterDeleteTime));

            assertQuery(
                    "SELECT nationkey, name, _change_type, _change_version_id, to_iso8601(_change_timestamp), _change_ordinal " +
                            "FROM TABLE(system.table_changes(CURRENT_SCHEMA, '%s', %s, %s))".formatted(table.getName(), initialSnapshot, snapshotAfterDelete),
                    "SELECT nationkey, name, 'insert', %s, '%s', 0 FROM nation UNION SELECT nationkey, name, 'delete', %s, '%s', 1 FROM nation".formatted(
                            snapshotAfterInsert, snapshotAfterInsertTime, snapshotAfterDelete, snapshotAfterDeleteTime));
        }
    }

    @Test
    public void testRowLevelDeletesWithTableChangesFunction()
    {
        try (TestTable table = newTrinoTable(
                "test_row_level_deletes_with_table_changes_function_",
                "AS SELECT nationkey, regionkey, name FROM tpch.tiny.nation WITH NO DATA")) {
            assertUpdate("INSERT INTO " + table.getName() + " SELECT nationkey, regionkey, name FROM nation", 25);
            long snapshotAfterInsert = getMostRecentSnapshotId(table.getName());

            assertUpdate("DELETE FROM " + table.getName() + " WHERE regionkey = 2", 5);
            long snapshotAfterDelete = getMostRecentSnapshotId(table.getName());

            assertQueryFails(
                    "SELECT * FROM TABLE(system.table_changes(CURRENT_SCHEMA, '%s', %s, %s))".formatted(table.getName(), snapshotAfterInsert, snapshotAfterDelete),
                    "Table uses features which are not yet supported by the table_changes function");
        }
    }

    @Test
    public void testCreateOrReplaceWithTableChangesFunction()
    {
        DateTimeFormatter instantMillisFormatter = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSVV").withZone(UTC);

        try (TestTable table = newTrinoTable(
                "test_table_changes_function_",
                "AS SELECT nationkey, name FROM tpch.tiny.nation WITH NO DATA")) {
            long initialSnapshot = getMostRecentSnapshotId(table.getName());
            assertUpdate("INSERT INTO " + table.getName() + " SELECT nationkey, name FROM nation", 25);
            long snapshotAfterInsert = getMostRecentSnapshotId(table.getName());
            String snapshotAfterInsertTime = getSnapshotTime(table.getName(), snapshotAfterInsert).format(instantMillisFormatter);

            assertQuery(
                    "SELECT nationkey, name, _change_type, _change_version_id, to_iso8601(_change_timestamp), _change_ordinal " +
                            "FROM TABLE(system.table_changes(CURRENT_SCHEMA, '%s', %s, %s))".formatted(table.getName(), initialSnapshot, snapshotAfterInsert),
                    "SELECT nationkey, name, 'insert', %s, '%s', 0 FROM nation".formatted(snapshotAfterInsert, snapshotAfterInsertTime));

            assertUpdate("CREATE OR REPLACE TABLE " + table.getName() + " AS SELECT nationkey, name FROM nation LIMIT 0", 0);
            long snapshotAfterCreateOrReplace = getMostRecentSnapshotId(table.getName());

            assertQueryFails(
                    "SELECT * FROM TABLE(system.table_changes(CURRENT_SCHEMA, '%s', %s, %s))".formatted(table.getName(), initialSnapshot, snapshotAfterCreateOrReplace),
                    "Starting snapshot \\(exclusive\\) %s is not a parent ancestor of end snapshot %s".formatted(initialSnapshot, snapshotAfterCreateOrReplace));

            assertUpdate("INSERT INTO " + table.getName() + " SELECT nationkey, name FROM nation", 25);
            long snapshotAfterInsertIntoCreateOrReplace = getMostRecentSnapshotId(table.getName());
            String snapshotAfterInsertTimeIntoCreateOrReplace = getSnapshotTime(table.getName(), snapshotAfterInsertIntoCreateOrReplace).format(instantMillisFormatter);

            assertQuery(
                    "SELECT nationkey, name, _change_type, _change_version_id, to_iso8601(_change_timestamp), _change_ordinal " +
                            "FROM TABLE(system.table_changes(CURRENT_SCHEMA, '%s', %s, %s))".formatted(table.getName(), snapshotAfterCreateOrReplace, snapshotAfterInsertIntoCreateOrReplace),
                    "SELECT nationkey, name, 'insert', %s, '%s', 0 FROM nation".formatted(snapshotAfterInsertIntoCreateOrReplace, snapshotAfterInsertTimeIntoCreateOrReplace));
        }
    }

    @Test
    public void testMetadataDeleteAfterCommitEnabled()
            throws IOException
    {
        if (!hasBehavior(SUPPORTS_CREATE_TABLE)) {
            return;
        }

        int metadataPreviousVersionCount = 5;
        String tableName = "test_metadata_delete_after_commit_enabled" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + "(_bigint BIGINT, _varchar VARCHAR)");
        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES extra_properties = MAP(ARRAY['write.metadata.delete-after-commit.enabled'], ARRAY['true'])");
        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES extra_properties = MAP(ARRAY['write.metadata.previous-versions-max'], ARRAY['" + metadataPreviousVersionCount + "'])");
        String tableLocation = getTableLocation(tableName);

        Map<String, Long> historyMetadataFiles = getMetadataFileAndUpdatedMillis(fileSystem, tableLocation);
        for (int i = 0; i < 10; i++) {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
            Map<String, Long> metadataFiles = getMetadataFileAndUpdatedMillis(fileSystem, tableLocation);
            historyMetadataFiles.putAll(metadataFiles);
            assertThat(metadataFiles.size()).isLessThanOrEqualTo(1 + metadataPreviousVersionCount);
            Set<String> expectMetadataFiles = historyMetadataFiles
                    .entrySet()
                    .stream()
                    .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                    .limit(metadataPreviousVersionCount + 1)
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet());
            assertThat(metadataFiles.keySet()).containsAll(expectMetadataFiles);
        }
        assertUpdate("DROP TABLE " + tableName);
    }

    private long getMostRecentSnapshotId(String tableName)
    {
        return (long) Iterables.getOnlyElement(getQueryRunner().execute(format("SELECT snapshot_id FROM \"%s$snapshots\" ORDER BY committed_at DESC LIMIT 1", tableName))
                .getOnlyColumnAsSet());
    }

    private ZonedDateTime getSnapshotTime(String tableName, long snapshotId)
    {
        return (ZonedDateTime) Iterables.getOnlyElement(getQueryRunner().execute(format("SELECT committed_at FROM \"%s$snapshots\" WHERE snapshot_id = %s", tableName, snapshotId))
                .getOnlyColumnAsSet());
    }

    protected String getTableLocation(String tableName)
    {
        Pattern locationPattern = Pattern.compile(".*location = '(.*?)'.*", Pattern.DOTALL);
        Matcher m = locationPattern.matcher((String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue());
        if (m.find()) {
            String location = m.group(1);
            verify(!m.find(), "Unexpected second match");
            return location;
        }
        throw new IllegalStateException("Location not found in SHOW CREATE TABLE result");
    }

    protected abstract void dropTableFromMetastore(String tableName);

    protected abstract String getMetadataLocation(String tableName);

    protected abstract String schemaPath();

    protected abstract boolean locationExists(String location);
}
