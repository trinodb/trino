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
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import io.trino.testng.services.Flaky;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import static io.trino.plugin.jdbc.JdbcWriteSessionProperties.NON_TRANSACTIONAL_INSERT;
import static io.trino.plugin.sqlserver.SqlServerQueryRunner.CATALOG;
import static io.trino.plugin.sqlserver.SqlServerQueryRunner.createSqlServerQueryRunner;
import static io.trino.plugin.sqlserver.SqlServerSessionProperties.BULK_COPY_FOR_WRITE;
import static io.trino.plugin.sqlserver.SqlServerSessionProperties.BULK_COPY_FOR_WRITE_LOCK_DESTINATION_TABLE;
import static io.trino.testing.DataProviders.cartesianProduct;
import static io.trino.testing.DataProviders.toDataProvider;
import static io.trino.testing.DataProviders.trueFalse;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestSqlServerConnectorTest
        extends BaseSqlServerConnectorTest
{
    protected TestingSqlServer sqlServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        sqlServer = closeAfterClass(new TestingSqlServer());
        return createSqlServerQueryRunner(
                sqlServer,
                ImmutableMap.of(),
                ImmutableMap.of("sqlserver.experimental.stored-procedure-table-function-enabled", "true"),
                REQUIRED_TPCH_TABLES);
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return sqlServer::execute;
    }

    @Flaky(issue = "fn_dblog() returns information only about the active portion of the transaction log, therefore it is flaky", match = ".*")
    @Test(dataProvider = "doubleTrueFalse")
    public void testCreateTableAsSelectWriteBulkiness(boolean bulkCopyForWrite, boolean bulkCopyLock)
            throws SQLException
    {
        String table = "bulk_copy_ctas_" + randomNameSuffix();
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(CATALOG, BULK_COPY_FOR_WRITE, Boolean.toString(bulkCopyForWrite))
                .setCatalogSessionProperty(CATALOG, BULK_COPY_FOR_WRITE_LOCK_DESTINATION_TABLE, Boolean.toString(bulkCopyLock))
                .build();

        // there should be enough rows in source table to minimal logging be enabled. `nation` table is too small.
        assertQuerySucceeds(session, format("CREATE TABLE %s as SELECT * FROM tpch.tiny.customer", table));
        assertQuery("SELECT * FROM " + table, "SELECT * FROM customer");

        // check whether minimal logging was applied.
        // Unlike fully logged operations, which use the transaction log to keep track of every row change,
        // minimally logged operations keep track of extent allocations and meta-data changes only.
        assertThat(getTableOperationsCount("LOP_INSERT_ROWS", table))
                .isEqualTo(bulkCopyForWrite && bulkCopyLock ? 0 : 1500);

        // check that there are no locks remaining on the target table after bulk copy
        assertQuery("SELECT count(*) FROM " + table, "SELECT count(*) FROM customer");
        assertUpdate(format("INSERT INTO %s SELECT * FROM tpch.tiny.customer LIMIT 1", table), 1);
        assertQuery("SELECT count(*) FROM " + table, "SELECT count(*) + 1 FROM customer");

        assertUpdate("DROP TABLE " + table);
    }

    @Flaky(issue = "fn_dblog() returns information only about the active portion of the transaction log, therefore it is flaky", match = ".*")
    @Test(dataProvider = "tripleTrueFalse")
    public void testInsertWriteBulkiness(boolean nonTransactionalInsert, boolean bulkCopyForWrite, boolean bulkCopyForWriteLockDestinationTable)
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

    @Test(dataProvider = "timestampTypes")
    public void testInsertWriteBulkinessWithTimestamps(String timestampType)
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
    @Test(dataProvider = "testTableNameDataProvider")
    public void testCreateAndDropTableWithSpecialCharacterName(String tableName)
    {
        String tableNameInSql = "\"" + tableName.replace("\"", "\"\"") + "\"";
        // Until https://github.com/trinodb/trino/issues/17 the table name is effectively lowercase
        tableName = tableName.toLowerCase(ENGLISH);
        assertUpdate("CREATE TABLE " + tableNameInSql + " (a bigint, b double, c varchar(50))");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertTableColumnNames(tableNameInSql, "a", "b", "c");

        assertUpdate("DROP TABLE " + tableNameInSql);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    // TODO remove this test after https://github.com/trinodb/trino/issues/14517
    @Test(dataProvider = "testTableNameDataProvider")
    public void testRenameColumnNameAdditionalTests(String columnName)
    {
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

    // TODO move this test to BaseConnectorTest https://github.com/trinodb/trino/issues/14517
    @Test(dataProvider = "testTableNameDataProvider")
    public void testRenameFromToTableWithSpecialCharacterName(String tableName)
    {
        String tableNameInSql = "\"" + tableName.replace("\"", "\"\"") + "\"";
        String sourceTableName = "test_rename_source_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + sourceTableName + " AS SELECT 123 x", 1);

        assertUpdate("ALTER TABLE " + sourceTableName + " RENAME TO " + tableNameInSql);
        assertQuery("SELECT x FROM " + tableNameInSql, "VALUES 123");
        // test rename back is working properly
        assertUpdate("ALTER TABLE " + tableNameInSql + " RENAME TO " + sourceTableName);
        assertUpdate("DROP TABLE " + sourceTableName);
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

    @DataProvider
    public static Object[][] doubleTrueFalse()
    {
        return cartesianProduct(trueFalse(), trueFalse());
    }

    @DataProvider
    public static Object[][] tripleTrueFalse()
    {
        return cartesianProduct(trueFalse(), trueFalse(), trueFalse());
    }

    @DataProvider
    public static Object[][] timestampTypes()
    {
        // Timestamp with timezone is not supported by the SqlServer connector
        return new Object[][] {
                {"timestamp"},
                {"timestamp(3)"},
                {"timestamp(6)"},
                {"timestamp(9)"},
                {"timestamp(12)"}
        };
    }

    // TODO replace TableNameDataProvider and ColumnNameDataProvider with ObjectNameDataProvider
    //  to one big single list of all special character cases, current list has additional special bracket cases,
    //  please don't forget to use this list as base
    @DataProvider
    public Object[][] testTableNameDataProvider()
    {
        return testTableNameTestData().stream()
                .collect(toDataProvider());
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
}
