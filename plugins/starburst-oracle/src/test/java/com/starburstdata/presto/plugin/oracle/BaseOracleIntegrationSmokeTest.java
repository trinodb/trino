/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.oracle;

import com.google.common.collect.ImmutableList;
import io.prestosql.testing.AbstractTestIntegrationSmokeTest;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.sql.SqlExecutor;
import io.prestosql.testing.sql.TestTable;
import org.testng.annotations.Test;

import static com.google.common.base.Strings.repeat;
import static com.starburstdata.presto.plugin.oracle.TestingOracleServer.USER;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static io.prestosql.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public abstract class BaseOracleIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    protected String getUser()
    {
        return USER;
    }

    @Override
    public void testDescribeTable()
    {
        MaterializedResult actualColumns = computeActual(
                getSession(), "DESC ORDERS").toTestTypes();

        MaterializedResult expectedColumns = MaterializedResult.resultBuilder(
                getSession(),
                VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "decimal(19,0)", "", "")
                .row("custkey", "decimal(19,0)", "", "")
                .row("orderstatus", "varchar(1)", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "timestamp", "", "")
                .row("orderpriority", "varchar(15)", "", "")
                .row("clerk", "varchar(15)", "", "")
                .row("shippriority", "decimal(10,0)", "", "")
                .row("comment", "varchar(79)", "", "")
                .build();

        assertEquals(actualColumns, expectedColumns);
    }

    @Test
    public void testDropTable()
    {
        String tableName = "test_drop" + randomTableSuffix();
        assertUpdate(format("CREATE TABLE %s AS SELECT 1 test_drop", tableName), 1);
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test
    public void testInsert()
    {
        try (TestTable table = new TestTable(inOracle(), getUser() + ".test_insert", "(x number(19), y varchar(100))")) {
            assertUpdate(format("INSERT INTO %s VALUES (123, 'test')", table.getName()), 1);
            assertQuery("SELECT * FROM " + table.getName(), "SELECT 123 x, 'test' y");
        }
    }

    @Test
    public void testCreateTableAsSelectIntoAnotherUsersSchema()
    {
        // running test in two schemas to ensure we test-cover table creation in a non-default schema
        testCreateTableAsSelectIntoAnotherUsersSchema("alice");
        testCreateTableAsSelectIntoAnotherUsersSchema("bob");
    }

    private void testCreateTableAsSelectIntoAnotherUsersSchema(String user)
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, format("oracle.%s.nationkeys_copy", user), "AS SELECT nationkey FROM nation", ImmutableList.of("123456789"))) {
            assertQuery(format("SELECT * FROM %s", table.getName()), "SELECT nationkey FROM nation UNION SELECT 123456789");
        }
    }

    @Test
    public void testViews()
    {
        try (TestView view = new TestView(inOracle(), getUser() + ".test_view", "AS SELECT 'O' as status FROM dual")) {
            assertQuery("SELECT status FROM " + view.getName(), "SELECT 'O'");
        }
    }

    @Test
    public void testSynonyms()
    {
        try (TestSynonym synonym = new TestSynonym(inOracle(), getUser() + ".test_synonym", "FOR ORDERS")) {
            assertQueryFails("SELECT orderkey FROM " + synonym.getName(), "line 1:22: Table 'oracle.*' does not exist");
        }
    }

    @Test
    public void testGetColumns()
    {
        // OracleClient.getColumns is using wildcard at the end of table name.
        // Here we test that columns do not leak between tables.
        // See OracleClient#getColumns for more details.
        try (TestTable ignored = new TestTable(inOracle(), "ordersx", "AS SELECT 'a' some_additional_column FROM dual")) {
            assertQuery(
                    format("SELECT column_name FROM information_schema.columns WHERE table_name = 'orders' AND table_schema = '%s'", getUser()),
                    "VALUES 'orderkey', 'custkey', 'orderstatus', 'totalprice', 'orderdate', 'orderpriority', 'clerk', 'shippriority', 'comment'");
        }
    }

    // TODO: Reuse methods from OracleDataTypes instead of hard-coding data types in the queries.

    @Test
    public void testPredicatePushdownForNumerics()
    {
        try (TestTable table = new TestTable(
                inOracle(),
                getUser() + ".test_predicate_pushdown_numeric",
                "(c_binary_float BINARY_FLOAT, c_binary_double BINARY_DOUBLE, c_number NUMBER(5,3))",
                ImmutableList.of("5.0f, 20.233, 5.0"))) {
            assertQuery(format("SELECT c_binary_double FROM %s WHERE c_binary_float = cast(5.0 as real)", table.getName()), "SELECT 20.233");
            assertQuery(format("SELECT c_binary_float FROM %s WHERE c_binary_double = cast(20.233 as double)", table.getName()), "SELECT 5.0");
            assertQuery(format("SELECT c_binary_float FROM %s WHERE c_number = cast(5.0 as decimal(5,3))", table.getName()), "SELECT 5.0");
        }
    }

    @Test
    public void testPredicatePushdownForChars()
    {
        try (TestTable table = new TestTable(
                inOracle(),
                getUser() + ".test_predicate_pushdown_char",
                "(c_char CHAR(7), c_nchar NCHAR(8), c_varchar VARCHAR2(20), c_nvarchar NVARCHAR2(20), c_clob CLOB, c_nclob NCLOB, c_long_char CHAR(2000), c_long_varchar VARCHAR2(4000))",
                ImmutableList.of("'my_char', 'my_nchar', 'my_varchar', 'my_nvarchar', 'my_clob', 'my_nclob', 'my_long_char', 'my_long_varchar'"))) {
            assertQuery(format("SELECT c_nchar FROM %s WHERE c_char = cast('my_char' as char(7))", table.getName()), "SELECT 'my_nchar'");
            assertQuery(format("SELECT c_char FROM %s WHERE c_nchar = cast('my_nchar' as char(8))", table.getName()), "SELECT 'my_char'");
            assertQuery(format("SELECT c_char FROM %s WHERE c_varchar = cast('my_varchar' as varchar(20))", table.getName()), "SELECT 'my_char'");
            assertQuery(format("SELECT c_char FROM %s WHERE c_nvarchar = cast('my_nvarchar' as varchar(20))", table.getName()), "SELECT 'my_char'");
            assertQuery(format("SELECT c_char FROM %s WHERE c_clob = cast('my_clob' as varchar)", table.getName()), "SELECT 'my_char'");
            assertQuery(format("SELECT c_char FROM %s WHERE c_nclob = cast('my_nclob' as varchar)", table.getName()), "SELECT 'my_char'");
            //Verify using a large value in WHERE, larger than the 2000 and 4000 bytes Oracle max
            assertQueryReturnsEmptyResult(format("SELECT c_char FROM %s WHERE c_long_char = '" + repeat("💩", 2000) + "'", table.getName()));
            assertQueryReturnsEmptyResult(format("SELECT c_char FROM %s WHERE c_long_varchar = '" + repeat("💩", 4000) + "'", table.getName()));
        }
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .matches("CREATE TABLE \\w+\\.\\w+\\.orders \\Q(\n" +
                        "   orderkey decimal(19, 0),\n" +
                        "   custkey decimal(19, 0),\n" +
                        "   orderstatus varchar(1),\n" +
                        "   totalprice double,\n" +
                        "   orderdate timestamp,\n" +
                        "   orderpriority varchar(15),\n" +
                        "   clerk varchar(15),\n" +
                        "   shippriority decimal(10, 0),\n" +
                        "   comment varchar(79)\n" +
                        ")");
    }

    // TODO: Add tests for BINARY and TEMPORAL

    private SqlExecutor inOracle()
    {
        return TestingOracleServer::executeInOracle;
    }
}
