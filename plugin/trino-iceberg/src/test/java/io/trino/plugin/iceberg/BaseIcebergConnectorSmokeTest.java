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
import io.trino.Session;
import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.apache.iceberg.FileFormat;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.plugin.iceberg.IcebergTestUtils.withSmallRowGroups;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.DROP_TABLE;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertTrue;

public abstract class BaseIcebergConnectorSmokeTest
        extends BaseConnectorSmokeTest
{
    protected final FileFormat format;

    public BaseIcebergConnectorSmokeTest(FileFormat format)
    {
        this.format = requireNonNull(format, "format is null");
    }

    @SuppressWarnings("DuplicateBranchesInSwitch")
    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_TOPN_PUSHDOWN:
                return false;

            case SUPPORTS_CREATE_VIEW:
                return true;

            case SUPPORTS_CREATE_MATERIALIZED_VIEW:
                return true;

            case SUPPORTS_DELETE:
            case SUPPORTS_UPDATE:
            case SUPPORTS_MERGE:
                return true;

            default:
                return super.hasBehavior(connectorBehavior);
        }
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
        try (TestTable table = new TestTable(getQueryRunner()::execute, "hidden_file_path", "(a int, b VARCHAR)", ImmutableList.of("(1, 'a')"))) {
            String filePath = (String) computeScalar(format("SELECT file_path FROM \"%s$files\"", table.getName()));

            assertQuery("SELECT DISTINCT \"$path\" FROM " + table.getName(), "VALUES " + "'" + filePath + "'");

            // Check whether the "$path" hidden column is correctly evaluated in the filter expression
            assertQuery(format("SELECT a FROM %s WHERE \"$path\" = '%s'", table.getName(), filePath), "VALUES 1");
        }
    }

    // Repeat test with invocationCount for better test coverage, since the tested aspect is inherently non-deterministic.
    @Test(timeOut = 120_000, invocationCount = 4)
    public void testDeleteRowsConcurrently()
            throws Exception
    {
        int threads = 4;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_concurrent_delete",
                "(col0 INTEGER, col1 INTEGER, col2 INTEGER, col3 INTEGER)")) {
            String tableName = table.getName();
            assertUpdate("INSERT INTO " + tableName + " VALUES (0, 0, 0, 0)", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 0, 0, 0)", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (0, 1, 0, 0)", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (0, 0, 1, 0)", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (0, 0, 0, 1)", 1);

            List<Future<Boolean>> futures = IntStream.range(0, threads)
                    .mapToObj(threadNumber -> executor.submit(() -> {
                        barrier.await(10, SECONDS);
                        try {
                            String columnName = "col" + threadNumber;
                            getQueryRunner().execute(format("DELETE FROM %s WHERE %s = 1", tableName, columnName));
                            return true;
                        }
                        catch (Exception e) {
                            return false;
                        }
                    }))
                    .collect(toImmutableList());

            futures.forEach(future -> assertTrue(getFutureValue(future)));
            assertThat(query("SELECT max(col0), max(col1), max(col2), max(col3) FROM " + tableName)).matches("VALUES (0, 0, 0, 0)");
        }
        finally {
            executor.shutdownNow();
            executor.awaitTermination(10, SECONDS);
        }
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
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_sorted_nation_table",
                "WITH (sorted_by = ARRAY['comment'], format = '" + format.name() + "') AS SELECT * FROM nation WITH NO DATA")) {
            assertUpdate(withSmallRowGroups, "INSERT INTO " + table.getName() + " SELECT * FROM nation", 25);
            for (Object filePath : computeActual("SELECT file_path from \"" + table.getName() + "$files\"").getOnlyColumnAsSet()) {
                assertTrue(isFileSorted((String) filePath, "comment"));
            }
            assertQuery("SELECT * FROM " + table.getName(), "SELECT * FROM nation");
        }
    }

    @Test
    public void testFileSortingWithLargerTable()
    {
        // Using a larger table forces buffered data to be written to disk
        Session withSmallRowGroups = withSmallRowGroups(getSession());
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_sorted_lineitem_table",
                "WITH (sorted_by = ARRAY['comment'], format = '" + format.name() + "') AS SELECT * FROM lineitem WITH NO DATA")) {
            assertUpdate(
                    withSmallRowGroups,
                    "INSERT INTO " + table.getName() + " SELECT * FROM lineitem",
                    "VALUES 60175");
            for (Object filePath : computeActual("SELECT file_path from \"" + table.getName() + "$files\"").getOnlyColumnAsSet()) {
                assertTrue(isFileSorted((String) filePath, "comment"));
            }
            assertQuery("SELECT * FROM " + table.getName(), "SELECT * FROM lineitem");
        }
    }

    protected abstract boolean isFileSorted(String path, String sortColumnName);

    private String getTableLocation(String tableName)
    {
        return (String) computeScalar("SELECT DISTINCT regexp_replace(\"$path\", '/[^/]*/[^/]*$', '') FROM " + tableName);
    }

    protected String getTableComment(String tableName)
    {
        return (String) computeScalar("SELECT comment FROM system.metadata.table_comments WHERE catalog_name = 'iceberg' AND schema_name = '" + getSession().getSchema().orElseThrow() + "' AND table_name = '" + tableName + "'");
    }

    protected String getColumnComment(String tableName, String columnName)
    {
        return (String) computeScalar("SELECT comment FROM information_schema.columns WHERE table_schema = '" + getSession().getSchema().orElseThrow() + "' AND table_name = '" + tableName + "' AND column_name = '" + columnName + "'");
    }

    protected abstract void dropTableFromMetastore(String tableName);

    protected abstract String getMetadataLocation(String tableName);

    protected abstract String schemaPath();

    protected abstract boolean locationExists(String location);
}
