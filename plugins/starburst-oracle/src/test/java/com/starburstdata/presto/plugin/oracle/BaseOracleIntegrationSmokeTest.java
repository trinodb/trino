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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.testing.AbstractTestIntegrationSmokeTest;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.sql.SqlExecutor;
import io.prestosql.testing.sql.TestTable;
import org.testng.annotations.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static com.google.common.base.Strings.repeat;
import static com.starburstdata.presto.plugin.oracle.OracleDataTypes.oracleTimestamp3TimeZoneDataType;
import static com.starburstdata.presto.plugin.oracle.OracleDataTypes.prestoTimestampWithTimeZoneDataType;
import static com.starburstdata.presto.plugin.oracle.TestingOracleServer.executeInOracle;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static io.prestosql.testing.datatype.DataType.timestampDataType;
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
        return OracleTestUsers.USER;
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
                .row("orderdate", "timestamp(3)", "", "")
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

    @Test
    public void testPredicatePushdownForNumerics()
    {
        try (TestTable table = new TestTable(
                inOracle(),
                getUser() + ".test_predicate_pushdown_numeric",
                "(c_binary_float BINARY_FLOAT, c_binary_double BINARY_DOUBLE, c_number NUMBER(5,3))",
                ImmutableList.of("5.0f, 20.233, 5.0"))) {
            assertThat(query(format("SELECT c_binary_double FROM %s WHERE c_binary_float = cast(5.0 as real)", table.getName()))).isCorrectlyPushedDown();
            assertThat(query(format("SELECT c_binary_float FROM %s WHERE c_binary_double = cast(20.233 as double)", table.getName()))).isCorrectlyPushedDown();
            assertThat(query(format("SELECT c_binary_float FROM %s WHERE c_number = cast(5.0 as decimal(5,3))", table.getName()))).isCorrectlyPushedDown();
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
            assertThat(query(format("SELECT c_nchar FROM %s WHERE c_char = cast('my_char' as char(7))", table.getName()))).isCorrectlyPushedDown();
            assertThat(query(format("SELECT c_char FROM %s WHERE c_nchar = cast('my_nchar' as char(8))", table.getName()))).isCorrectlyPushedDown();
            assertThat(query(format("SELECT c_char FROM %s WHERE c_varchar = cast('my_varchar' as varchar(20))", table.getName()))).isCorrectlyPushedDown();
            assertThat(query(format("SELECT c_char FROM %s WHERE c_nvarchar = cast('my_nvarchar' as varchar(20))", table.getName()))).isCorrectlyPushedDown();
            //Verify using a large value in WHERE, larger than the 2000 and 4000 bytes Oracle max
            assertThat(query(format("SELECT c_char FROM %s WHERE c_long_char = '" + repeat("💩", 2000) + "'", table.getName()))).isCorrectlyPushedDown();
            assertThat(query(format("SELECT c_char FROM %s WHERE c_long_varchar = '" + repeat("💩", 4000) + "'", table.getName()))).isCorrectlyPushedDown();

            assertThat(query(format("SELECT c_char FROM %s WHERE c_clob = cast('my_clob' as varchar)", table.getName()))).isNotFullyPushedDown(FilterNode.class);
            assertThat(query(format("SELECT c_char FROM %s WHERE c_nclob = cast('my_nclob' as varchar)", table.getName()))).isNotFullyPushedDown(FilterNode.class);
        }
    }

    /**
     * This test covers only predicate pushdown for Oracle (it doesn't test timestamp semantics).
     *
     * @see com.starburstdata.presto.plugin.oracle.TestOracleTypeMapping
     * @see io.prestosql.testing.AbstractTestDistributedQueries
     */
    @Test
    public void testPredicatePushdownForTimestamps()
    {
        LocalDateTime date1950 = LocalDateTime.of(1950, 5, 30, 23, 59, 59, 0);
        ZonedDateTime yakutat1978 = ZonedDateTime.of(1978, 4, 30, 23, 55, 10, 10, ZoneId.of("America/Yakutat"));
        ZonedDateTime pacific1976 = ZonedDateTime.of(1976, 3, 15, 0, 2, 22, 10, ZoneId.of("Pacific/Wake"));

        List<String> values = ImmutableList.<String>builder()
                .add(timestampDataType().toLiteral(date1950))
                .add(oracleTimestamp3TimeZoneDataType().toLiteral(yakutat1978))
                .add(prestoTimestampWithTimeZoneDataType().toLiteral(pacific1976))
                .add("'result_value'")
                .build();

        try (TestTable table = new TestTable(
                inOracle(),
                getUser() + ".test_predicate_pushdown_timestamp",
                "(t_timestamp TIMESTAMP, t_timestamp3_with_tz TIMESTAMP(3) WITH TIME ZONE, t_timestamp_with_tz TIMESTAMP WITH TIME ZONE, dummy_col VARCHAR(12))",
                ImmutableList.of(Joiner.on(", ").join(values)))) {
            assertThat(query(format(
                    "SELECT dummy_col FROM %s WHERE t_timestamp = %s",
                    table.getName(),
                    format("timestamp '%s'", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").format(date1950)))))
                    .isCorrectlyPushedDown();

            assertThat(query(format(
                    "SELECT dummy_col FROM %s WHERE t_timestamp3_with_tz = %s",
                    table.getName(),
                    prestoTimestampWithTimeZoneDataType().toLiteral(yakutat1978))))
                    .isCorrectlyPushedDown();

            assertThat(query(format(
                    "SELECT dummy_col FROM %s WHERE t_timestamp_with_tz = %s",
                    table.getName(),
                    prestoTimestampWithTimeZoneDataType().toLiteral(pacific1976))))
                    .isCorrectlyPushedDown();
        }
    }

    @Test
    public void testAggregationPushdown()
    {
        // TODO support aggregation pushdown with GROUPING SETS

        assertThat(query("SELECT count(*) FROM nation")).isCorrectlyPushedDown();
        assertThat(query("SELECT count(nationkey) FROM nation")).isCorrectlyPushedDown();
        assertThat(query("SELECT regionkey, min(nationkey) FROM nation GROUP BY regionkey")).isCorrectlyPushedDown();
        assertThat(query("SELECT regionkey, max(nationkey) FROM nation GROUP BY regionkey")).isCorrectlyPushedDown();
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey")).isCorrectlyPushedDown();
        assertThat(query("SELECT regionkey, avg(nationkey) FROM nation GROUP BY regionkey")).isCorrectlyPushedDown();

        try (TestTable testTable = new TestTable(inOracle(), getSession().getSchema().orElseThrow() + ".test_aggregation_pushdown",
                "(short_decimal decimal(9, 3), long_decimal decimal(30, 10))")) {
            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (100.000, 100000000.000000000)");
            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (123.321, 123456789.987654321)");

            assertThat(query("SELECT min(short_decimal), min(long_decimal) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT max(short_decimal), max(long_decimal) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT sum(short_decimal), sum(long_decimal) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT avg(short_decimal), avg(long_decimal) FROM " + testTable.getName())).isCorrectlyPushedDown();
        }
    }

    @Test
    public void testStddevPushdown()
    {
        try (TestTable testTable = new TestTable(inOracle(), getSession().getSchema().orElseThrow() + ".test_stddev_pushdown",
                "(t_double DOUBLE PRECISION)")) {
            assertThat(query("SELECT stddev_pop(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT stddev(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT stddev_samp(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();

            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (1)");

            assertThat(query("SELECT stddev_pop(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT stddev(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT stddev_samp(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();

            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (3)");
            assertThat(query("SELECT stddev_pop(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();

            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (5)");
            assertThat(query("SELECT stddev(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT stddev_samp(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
        }

        try (TestTable testTable = new TestTable(inOracle(), getSession().getSchema().orElseThrow() + ".test_stddev_pushdown",
                "(t_double DOUBLE PRECISION)")) {
            // Test non-whole number results
            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (1)");
            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (2)");
            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (4)");
            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (5)");

            assertThat(query("SELECT stddev_pop(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT stddev(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT stddev_samp(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
        }
    }

    @Test
    public void testVariancePushdown()
    {
        try (TestTable testTable = new TestTable(inOracle(), getSession().getSchema().orElseThrow() + ".test_variance_pushdown",
                "(t_double DOUBLE PRECISION)")) {
            assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();

            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (1)");

            assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();

            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (3)");
            assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();

            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (5)");
            assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
        }

        try (TestTable testTable = new TestTable(inOracle(), getSession().getSchema().orElseThrow() + ".test_variance_pushdown",
                "(t_double DOUBLE PRECISION)")) {
            // Test non-whole number results
            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (1)");
            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (2)");
            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (3)");
            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (4)");
            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (5)");

            assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
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
                        "   orderdate timestamp(3),\n" +
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
