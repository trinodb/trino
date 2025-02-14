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
package io.trino.plugin.sqlserver;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static io.trino.plugin.jdbc.JdbcWriteSessionProperties.NON_TRANSACTIONAL_INSERT;
import static io.trino.plugin.sqlserver.SqlServerQueryRunner.CATALOG;
import static io.trino.plugin.sqlserver.SqlServerSessionProperties.BULK_COPY_FOR_WRITE;
import static io.trino.plugin.sqlserver.SqlServerSessionProperties.BULK_COPY_FOR_WRITE_LOCK_DESTINATION_TABLE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSqlServerConnectorTest
        extends BaseSqlServerConnectorTest
{
    protected TestingSqlServer sqlServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        sqlServer = closeAfterClass(new TestingSqlServer());
        return SqlServerQueryRunner.builder(sqlServer)
                .addConnectorProperties(Map.of("sqlserver.stored-procedure-table-function-enabled", "true"))
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return sqlServer::execute;
    }

    @Test
    public void testCreateTableAsSelectWriteBulkiness()
    {
        testCreateTableAsSelectWriteBulkiness(true, true);
        testCreateTableAsSelectWriteBulkiness(true, false);
        testCreateTableAsSelectWriteBulkiness(false, true);
        testCreateTableAsSelectWriteBulkiness(false, false);
    }

    private void testCreateTableAsSelectWriteBulkiness(boolean bulkCopyForWrite, boolean bulkCopyLock)
    {
        String table = "bulk_copy_ctas_" + randomNameSuffix();
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(CATALOG, BULK_COPY_FOR_WRITE, Boolean.toString(bulkCopyForWrite))
                .setCatalogSessionProperty(CATALOG, BULK_COPY_FOR_WRITE_LOCK_DESTINATION_TABLE, Boolean.toString(bulkCopyLock))
                .build();

        // there should be enough rows in source table to minimal logging be enabled. `nation` table is too small.
        assertQuerySucceeds(session, format("CREATE TABLE %s as SELECT * FROM tpch.tiny.customer", table));
        assertQuery("SELECT * FROM " + table, "SELECT * FROM customer");

        // check that there are no locks remaining on the target table after bulk copy
        assertQuery("SELECT count(*) FROM " + table, "SELECT count(*) FROM customer");
        assertUpdate(format("INSERT INTO %s SELECT * FROM tpch.tiny.customer LIMIT 1", table), 1);
        assertQuery("SELECT count(*) FROM " + table, "SELECT count(*) + 1 FROM customer");

        assertUpdate("DROP TABLE " + table);
    }

    @Test
    public void testInsertWriteBulkiness()
            throws SQLException
    {
        testInsertWriteBulkiness(true, true, true);
        testInsertWriteBulkiness(true, true, false);
        testInsertWriteBulkiness(true, false, true);
        testInsertWriteBulkiness(true, false, false);
        testInsertWriteBulkiness(false, true, true);
        testInsertWriteBulkiness(false, true, false);
        testInsertWriteBulkiness(false, false, true);
        testInsertWriteBulkiness(false, false, false);
    }

    private void testInsertWriteBulkiness(boolean nonTransactionalInsert, boolean bulkCopyForWrite, boolean bulkCopyForWriteLockDestinationTable)
            throws SQLException
    {
        String table = "bulk_copy_insert_" + randomNameSuffix();
        assertQuerySucceeds(format("CREATE TABLE %s as SELECT * FROM tpch.tiny.customer WHERE 0 = 1", table));
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(CATALOG, NON_TRANSACTIONAL_INSERT, Boolean.toString(nonTransactionalInsert))
                .setCatalogSessionProperty(CATALOG, BULK_COPY_FOR_WRITE, Boolean.toString(bulkCopyForWrite))
                .setCatalogSessionProperty(CATALOG, BULK_COPY_FOR_WRITE_LOCK_DESTINATION_TABLE, Boolean.toString(bulkCopyForWriteLockDestinationTable))
                .build();

        // there should be enough rows in source table to minimal logging be enabled. `nation` table is too small.
        assertQuerySucceeds(session, format("INSERT INTO %s SELECT * FROM tpch.tiny.customer", table));
        assertQuery("SELECT * FROM " + table, "SELECT * FROM customer");

        // check whether minimal logging was applied.
        // Unlike fully logged operations, which use the transaction log to keep track of every row change,
        // minimally logged operations keep track of extent allocations and meta-data changes only.
        assertThat(getTableOperationsCount("LOP_INSERT_ROWS", table))
                .isEqualTo(bulkCopyForWrite && bulkCopyForWriteLockDestinationTable ? 0 : 1500);

        // check that there are no locks remaining on the target table after bulk copy
        assertQuery("SELECT count(*) FROM " + table, "SELECT count(*) FROM customer");
        assertUpdate(format("INSERT INTO %s SELECT * FROM tpch.tiny.customer LIMIT 1", table), 1);
        assertQuery("SELECT count(*) FROM " + table, "SELECT count(*) + 1 FROM customer");

        assertUpdate("DROP TABLE " + table);
    }

    @Test
    public void testInsertWriteBulkinessWithTimestamps()
    {
        testInsertWriteBulkinessWithTimestamps("timestamp");
        testInsertWriteBulkinessWithTimestamps("timestamp(3)");
        testInsertWriteBulkinessWithTimestamps("timestamp(6)");
        testInsertWriteBulkinessWithTimestamps("timestamp(9)");
        testInsertWriteBulkinessWithTimestamps("timestamp(12)");
    }

    private void testInsertWriteBulkinessWithTimestamps(String timestampType)
    {
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(CATALOG, BULK_COPY_FOR_WRITE, "true")
                .setCatalogSessionProperty(CATALOG, BULK_COPY_FOR_WRITE_LOCK_DESTINATION_TABLE, "true")
                .build();

        try (TestTable table = new TestTable((String sql) -> getQueryRunner().execute(session, sql), "bulk_copy_insert", format("(timestamp_col %s)", timestampType))) {
            // Insert values without using TestTable to ensure all the rows are written in a single batch
            List<String> timestampValues = ImmutableList.of(
                    "TIMESTAMP '1958-01-01 13:18:03'",
                    "TIMESTAMP '1958-01-01 13:18:03.1'",
                    "TIMESTAMP '1958-01-01 13:18:03.123'",
                    "TIMESTAMP '1958-01-01 13:18:03.123000'",
                    "TIMESTAMP '1958-01-01 13:18:03.123000000'",
                    "TIMESTAMP '1958-01-01 13:18:03.123000000000'",
                    "TIMESTAMP '2019-03-18 10:01:17.987000'",
                    "TIMESTAMP '2018-10-28 01:33:17.456000000'",
                    "TIMESTAMP '1970-01-01 00:00:00.000000000'",
                    "TIMESTAMP '1970-01-01 00:13:42.000000000'",
                    "TIMESTAMP '2018-04-01 02:13:55.123000000'",
                    "TIMESTAMP '1986-01-01 00:13:07.000000000000'");
            String valuesList = timestampValues.stream().map(s -> format("(%s)", s)).collect(joining(","));
            assertUpdate(format("INSERT INTO %s VALUES %s", table.getName(), valuesList), 12);

            // check that there are no locks remaining on the target table after bulk copy
            assertQuery("SELECT count(*) FROM " + table.getName(), "SELECT 12");
            assertUpdate(format("INSERT INTO %s VALUES (TIMESTAMP '2022-01-01 00:13:07.0000000')", table.getName()), 1);
            assertQuery("SELECT count(*) FROM " + table.getName(), "SELECT 13");
        }
    }

    // TODO move test to BaseConnectorTest https://github.com/trinodb/trino/issues/14517
    @Test
    public void testCreateAndDropTableWithSpecialCharacterName()
    {
        for (String tableName : testTableNameTestData()) {
            String tableNameInSql = "\"" + tableName.replace("\"", "\"\"") + "\"";
            // Until https://github.com/trinodb/trino/issues/17 the table name is effectively lowercase
            tableName = tableName.toLowerCase(ENGLISH);
            assertUpdate("CREATE TABLE " + tableNameInSql + " (a bigint, b double, c varchar(50))");
            assertThat(getQueryRunner().tableExists(getSession(), tableName)).isTrue();
            assertTableColumnNames(tableNameInSql, "a", "b", "c");

            assertUpdate("DROP TABLE " + tableNameInSql);
            assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        }
    }

    // TODO remove this test after https://github.com/trinodb/trino/issues/14517
    @Test
    public void testRenameColumnNameAdditionalTests()
    {
        for (String columnName : testTableNameTestData()) {
            String nameInSql = "\"" + columnName.replace("\"", "\"\"") + "\"";
            String tableName = "tcn_" + nameInSql.replaceAll("[^a-z0-9]", "") + randomNameSuffix();
            // Use complex identifier to test a source column name when renaming columns
            String sourceColumnName = "a;b$c";

            assertUpdate("CREATE TABLE " + tableName + "(\"" + sourceColumnName + "\" varchar(50))");
            assertTableColumnNames(tableName, sourceColumnName);

            assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN \"" + sourceColumnName + "\" TO " + nameInSql);
            assertTableColumnNames(tableName, columnName.toLowerCase(ENGLISH));

            assertUpdate("DROP TABLE " + tableName);
        }
    }

    // TODO move this test to BaseConnectorTest https://github.com/trinodb/trino/issues/14517
    @Test
    public void testRenameFromToTableWithSpecialCharacterName()
    {
        for (String tableName : testTableNameTestData()) {
            String tableNameInSql = "\"" + tableName.replace("\"", "\"\"") + "\"";
            String sourceTableName = "test_rename_source_" + randomNameSuffix();
            assertUpdate("CREATE TABLE " + sourceTableName + " AS SELECT 123 x", 1);

            assertUpdate("ALTER TABLE " + sourceTableName + " RENAME TO " + tableNameInSql);
            assertQuery("SELECT x FROM " + tableNameInSql, "VALUES 123");
            // test rename back is working properly
            assertUpdate("ALTER TABLE " + tableNameInSql + " RENAME TO " + sourceTableName);
            assertUpdate("DROP TABLE " + sourceTableName);
        }
    }

    private int getTableOperationsCount(String operation, String table)
            throws SQLException
    {
        try (Connection connection = sqlServer.createConnection();
                Handle handle = Jdbi.open(connection)) {
            // fn_dblog() function only returns information about the active portion of the transaction log such as open transactions or the last activity
            // therefore tests which use this function are flaky, but it's almost not possible to reproduce this flakiness.
            // There was no better option found to test if minimal logging was enabled than to query `LOP_INSERT_ROWS` from fn_dblog(NULL,NULL)
            return handle.createQuery("" +
                            "SELECT COUNT(*) as cnt " +
                            "FROM fn_dblog(NULL,NULL) " +
                            "WHERE Operation = :operation " +
                            "AND AllocUnitName = CONCAT('dbo.', :table_name)")
                    .bind("operation", operation)
                    .bind("table_name", table)
                    .mapTo(Integer.class)
                    .one();
        }
    }

    private List<String> testTableNameTestData()
    {
        return ImmutableList.<String>builder()
                .add("lowercase")
                .add("UPPERCASE")
                .add("MixedCase")
                .add("an_underscore")
                .add("a-hyphen-minus") // ASCII '-' is HYPHEN-MINUS in Unicode
                .add("a space")
                .add("atrailingspace ")
                .add(" aleadingspace")
                .add("a.dot")
                .add("a,comma")
                .add("a:colon")
                .add("a;semicolon")
                .add("an@at")
                .add("a\"quote")
                .add("an'apostrophe")
                .add("a`backtick`")
                .add("a/slash")
                .add("a\\backslash")
                .add("adigit0")
                .add("0startwithdigit")
                .add("[brackets]")
                .add("brackets[]inside")
                .add("open[bracket")
                .add("close]bracket")
                .build();
    }

    @Test
    @Override
    public void testSelectInformationSchemaTables()
    {
        // Isolate this test to avoid problem described in https://github.com/trinodb/trino/issues/10846
        executeExclusively(super::testSelectInformationSchemaTables);
    }

    @Test
    @Override
    public void testSelectInformationSchemaColumns()
    {
        // Isolate this test to avoid problem described in https://github.com/trinodb/trino/issues/10846
        executeExclusively(super::testSelectInformationSchemaColumns);
    }
}
