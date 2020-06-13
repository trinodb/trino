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
package io.prestosql.plugin.sqlserver;

import io.prestosql.testing.AbstractTestIntegrationSmokeTest;
import io.prestosql.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static io.prestosql.plugin.sqlserver.SqlServerQueryRunner.createSqlServerQueryRunner;
import static io.prestosql.tpch.TpchTable.CUSTOMER;
import static io.prestosql.tpch.TpchTable.NATION;
import static io.prestosql.tpch.TpchTable.ORDERS;
import static io.prestosql.tpch.TpchTable.REGION;
import static java.lang.String.format;
import static org.testng.Assert.assertTrue;

@Test
public class TestSqlServerIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    protected TestingSqlServer sqlServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        sqlServer = new TestingSqlServer();
        sqlServer.start();
        return createSqlServerQueryRunner(sqlServer, CUSTOMER, NATION, ORDERS, REGION);
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        sqlServer.close();
    }

    @Test
    public void testInsert()
    {
        sqlServer.execute("CREATE TABLE test_insert (x bigint, y varchar(100))");
        assertUpdate("INSERT INTO test_insert VALUES (123, 'test')", 1);
        assertQuery("SELECT * FROM test_insert", "SELECT 123 x, 'test' y");
        assertUpdate("DROP TABLE test_insert");
    }

    @Test
    public void testInsertInPresenceOfNotSupportedColumn()
    {
        sqlServer.execute("CREATE TABLE test_insert_not_supported_column_present(x bigint, y sql_variant, z varchar(10))");
        // Check that column y is not supported.
        assertQuery("SELECT column_name FROM information_schema.columns WHERE table_name = 'test_insert_not_supported_column_present'", "VALUES 'x', 'z'");
        assertUpdate("INSERT INTO test_insert_not_supported_column_present (x, z) VALUES (123, 'test')", 1);
        assertQuery("SELECT x, z FROM test_insert_not_supported_column_present", "SELECT 123, 'test'");
        assertUpdate("DROP TABLE test_insert_not_supported_column_present");
    }

    @Test
    public void testView()
    {
        sqlServer.execute("CREATE VIEW test_view AS SELECT * FROM orders");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_view"));
        assertQuery("SELECT orderkey FROM test_view", "SELECT orderkey FROM orders");
        sqlServer.execute("DROP VIEW IF EXISTS test_view");
    }

    @Test
    public void testColumnComment()
            throws Exception
    {
        try (AutoCloseable ignoreTable = withTable("test_column_comment",
                "(col1 bigint, col2 bigint, col3 bigint)")) {
            sqlServer.execute("" +
                    "EXEC sp_addextendedproperty " +
                    " 'MS_Description', 'test comment', " +
                    " 'Schema', 'dbo', " +
                    " 'Table', 'test_column_comment', " +
                    " 'Column', 'col1'");

            // SQL Server JDBC driver doesn't support REMARKS for column comment https://github.com/Microsoft/mssql-jdbc/issues/646
            assertQuery(
                    "SELECT column_name, comment FROM information_schema.columns WHERE table_schema = 'dbo' AND table_name = 'test_column_comment'",
                    "VALUES ('col1', null), ('col2', null), ('col3', null)");
        }
    }

    @Test
    public void testDecimalPredicatePushdown()
            throws Exception
    {
        try (AutoCloseable ignoreTable = withTable("test_decimal_pushdown",
                "(short_decimal decimal(9, 3), long_decimal decimal(30, 10))")) {
            sqlServer.execute("INSERT INTO test_decimal_pushdown VALUES (123.321, 123456789.987654321)");

            assertQuery("SELECT * FROM test_decimal_pushdown WHERE short_decimal = 123.321",
                    "VALUES (123.321, 123456789.987654321)");
            assertQuery("SELECT * FROM test_decimal_pushdown WHERE long_decimal = 123456789.987654321",
                    "VALUES (123.321, 123456789.987654321)");
        }
    }

    private AutoCloseable withTable(String tableName, String tableDefinition)
    {
        sqlServer.execute(format("CREATE TABLE %s %s", tableName, tableDefinition));
        return () -> sqlServer.execute("DROP TABLE " + tableName);
    }
}
