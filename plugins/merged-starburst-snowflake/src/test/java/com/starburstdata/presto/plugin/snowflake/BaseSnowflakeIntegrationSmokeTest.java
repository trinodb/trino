/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake;

import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.testing.AbstractTestIntegrationSmokeTest;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.TestingSession;
import io.prestosql.testing.sql.SqlExecutor;
import io.prestosql.testing.sql.TestTable;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

import static com.google.common.base.Strings.repeat;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static io.prestosql.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public abstract class BaseSnowflakeIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    protected final SnowflakeServer server = new SnowflakeServer();
    protected final SqlExecutor snowflakeExecutor = server::safeExecute;

    @Override
    public void testDescribeTable()
    {
        MaterializedResult actualColumns = computeActual(
                getSession(), "DESC ORDERS").toTestTypes();

        MaterializedResult expectedColumns = MaterializedResult.resultBuilder(
                getSession(),
                VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "decimal(38,0)", "", "")
                .row("custkey", "decimal(38,0)", "", "")
                .row("orderstatus", "varchar(1)", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar(15)", "", "")
                .row("clerk", "varchar(15)", "", "")
                .row("shippriority", "decimal(38,0)", "", "")
                .row("comment", "varchar(79)", "", "")
                .build();

        assertEquals(actualColumns, expectedColumns);
    }

    @Test
    public void testDropTable()
    {
        String tableName = "test_drop_" + randomTableSuffix();
        assertUpdate(format("CREATE TABLE %s AS SELECT 1 test_drop", tableName), 1);
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));

        assertUpdate(format("DROP TABLE %s", tableName));
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test
    public void testInsert()
    {
        String tableName = TEST_SCHEMA + ".test_insert";
        try (TestTable testTable = new TestTable(snowflakeExecutor, tableName, "(x number(19), y varchar(100))")) {
            assertUpdate(format("INSERT INTO %s VALUES (123, 'test')", testTable.getName()), 1);
            assertQuery(format("SELECT * FROM %s", testTable.getName()), "SELECT 123 x, 'test' y");
        }
    }

    @Test
    public void testViews()
            throws SQLException
    {
        String viewName = "test_view_" + randomTableSuffix();
        server.execute(format("CREATE VIEW test_schema.%s AS SELECT * FROM orders", viewName));
        assertTrue(getQueryRunner().tableExists(getSession(), viewName));
        assertQuery(format("SELECT orderkey FROM %s", viewName), "SELECT orderkey FROM orders");
        server.execute(format("DROP VIEW test_schema.%s", viewName));
    }

    @Test
    public void testPredicatePushdownForNumerics()
    {
        String tableName = TEST_SCHEMA + ".test_predicate_pushdown_numeric";
        try (TestTable testTable = new TestTable(
                snowflakeExecutor,
                tableName,
                "(c_binary_float FLOAT, c_binary_double DOUBLE, c_number NUMBER(5,3))",
                ImmutableList.of("5.0, 20.233, 5.0"))) {
            // this expects the unqualified table name as an argument
            assertTrue(getQueryRunner().tableExists(getSession(), testTable.getName().substring(TEST_SCHEMA.length() + 1)));
            assertQuery(format("SELECT c_binary_double FROM %s WHERE c_binary_float = cast(5.0 as real)", testTable.getName()), "SELECT 20.233");
            assertQuery(format("SELECT c_binary_float FROM %s WHERE c_binary_double = cast(20.233 as double)", testTable.getName()), "SELECT 5.0");
            assertQuery(format("SELECT c_binary_float FROM %s WHERE c_number = cast(5.0 as decimal(5,3))", testTable.getName()), "SELECT 5.0");
        }
    }

    @Test
    public void testPredicatePushdownForChars()
    {
        String tableName = TEST_SCHEMA + ".test_predicate_pushdown_char";
        try (TestTable testTable = new TestTable(
                snowflakeExecutor,
                tableName,
                "(c_char CHAR(7), c_varchar VARCHAR(20), c_long_char CHAR(2000), c_long_varchar VARCHAR(4000))",
                ImmutableList.of("'my_char', 'my_varchar', 'my_long_char', 'my_long_varchar'"))) {
            // this expects the unqualified table name as an argument
            assertTrue(getQueryRunner().tableExists(getSession(), testTable.getName().substring(TEST_SCHEMA.length() + 1)));
            assertQuery(format("SELECT c_char FROM %s WHERE c_varchar = cast('my_varchar' as varchar(20))", testTable.getName()), "SELECT 'my_char'");
            assertQueryReturnsEmptyResult(format("SELECT c_char FROM %s WHERE c_long_char = '" + repeat("💩", 2000) + "'", testTable.getName()));
            assertQueryReturnsEmptyResult(format("SELECT c_char FROM %s WHERE c_long_varchar = '" + repeat("💩", 4000) + "'", testTable.getName()));
        }
    }

    @Test
    public void testLimitPushdown()
    {
        assertThat(query("SELECT name FROM nation LIMIT 30")).isCorrectlyPushedDown(); // Use high limit for result determinism

        // with filter over numeric column
        assertThat(query("SELECT name FROM nation WHERE regionkey = 3 LIMIT 5")).isCorrectlyPushedDown();

        // with filter over varchar column
        assertThat(query("SELECT name FROM nation WHERE name < 'EEE' LIMIT 5")).isCorrectlyPushedDown();
    }

    @Test
    @Override
    public void testSelectInformationSchemaTables()
    {
        String schema = getSession().getSchema().get();
        assertQuery("SELECT table_name FROM information_schema.tables WHERE table_schema = '" + schema + "' AND table_name = 'orders'", "VALUES 'orders'");
    }

    @Test
    @Override
    public void testSelectInformationSchemaColumns()
    {
        String schema = getSession().getSchema().get();
        String ordersTableWithColumns = "VALUES " +
                "('orders', 'orderkey'), " +
                "('orders', 'custkey'), " +
                "('orders', 'orderstatus'), " +
                "('orders', 'totalprice'), " +
                "('orders', 'orderdate'), " +
                "('orders', 'orderpriority'), " +
                "('orders', 'clerk'), " +
                "('orders', 'shippriority'), " +
                "('orders', 'comment')";

        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = '" + schema + "' AND table_name = 'orders'", ordersTableWithColumns);
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = '" + schema + "' AND table_name LIKE '%rders'", ordersTableWithColumns);
    }

    @Test
    public void testTimeRounding()
    {
        String tableName = TEST_SCHEMA + ".test_time";
        for (ZoneId sessionZone : ImmutableList.of(ZoneOffset.UTC, ZoneId.systemDefault(), ZoneId.of("Europe/Vilnius"), ZoneId.of("Asia/Kathmandu"), ZoneId.of(TestingSession.DEFAULT_TIME_ZONE_KEY.getId()))) {
            Session session = Session.builder(getQueryRunner().getDefaultSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                    .build();
            try (TestTable testTable = new TestTable(
                    snowflakeExecutor,
                    tableName,
                    "(x TIME)")) {
                assertUpdate(session, format("INSERT INTO %s VALUES (TIME '12:34:56.123')", testTable.getName()), 1);
                assertQuery(session, format("SELECT * FROM %s WHERE rand() > 42 OR x = TIME '12:34:56.123'", testTable.getName()), "SELECT '12:34:56.123' x");
            }
        }
    }

    @Test
    public void testCaseSensitiveColumnNames()
    {
        String tableName = TEST_SCHEMA + ".test_case_sensitive_column_names_";
        try (TestTable testTable = new TestTable(
                snowflakeExecutor,
                tableName,
                "(id varchar, \"lowercase\" varchar, \"UPPERCASE\" varchar, \"MixedCase\" varchar)",
                ImmutableList.of("'lowercase', 'lowercase value', NULL, NULL", "'uppercase', NULL, 'uppercase value', NULL", "'mixedcase', NULL, NULL, 'mixedcase value'"))) {
            assertQuery(
                    "SELECT id, mixedcase, uppercase, lowercase FROM " + testTable.getName(),
                    "VALUES " +
                            " ('lowercase', NULL, NULL, 'lowercase value'), " +
                            " ('uppercase', NULL, 'uppercase value', NULL), " +
                            " ('mixedcase', 'mixedcase value', NULL, NULL)");
        }
    }

    @Test
    public void testTimestampWithTimezoneValues()
    {
        testTimestampWithTimezoneValues(true);
    }

    protected void testTimestampWithTimezoneValues(boolean includeNegativeYear)
    {
        String tableName = TEST_SCHEMA + ".test_tstz_";
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(ZoneOffset.UTC.getId()))
                .build();

        // Snowflake literals cannot have a 5-digit year, nor a negative year, so need to use DATEADD for some values
        ImmutableList.Builder<String> data = ImmutableList.<String>builder()
                .add("TO_TIMESTAMP_TZ('1970-01-01T00:00:00.000 +14:00')")
                .add("TO_TIMESTAMP_TZ('1970-01-01T00:00:00.000 -13:00')")
                .add("TO_TIMESTAMP_TZ('0001-01-01T00:00:00.000Z')")
                .add("DATEADD(YEAR, 2, TO_TIMESTAMP_TZ('9999-12-31T23:59:59.999Z'))")
                .add("DATEADD(YEAR, 70000, TO_TIMESTAMP_TZ('3326-09-11T20:14:45.247Z'))")
                .add("DATEADD(YEAR, 70000, TO_TIMESTAMP_TZ('3326-09-11T07:14:45.247 -13:00'))");

        if (includeNegativeYear) {
            data
                    .add("DATEADD(YEAR, -2, TO_TIMESTAMP_TZ('0001-01-01T00:00:00.000Z'))")
                    .add("DATEADD(YEAR, -70000, TO_TIMESTAMP_TZ('613-04-22T03:45:14.753Z'))")
                    .add("DATEADD(YEAR, -70000, TO_TIMESTAMP_TZ('613-04-22T17:45:14.753 +14:00'))");
        }

        MaterializedResult.Builder expected = MaterializedResult.resultBuilder(session, createTimestampWithTimeZoneType(3))
                .row(LocalDateTime.of(1970, 1, 1, 0, 0).atZone(ZoneOffset.ofHoursMinutes(14, 0)))
                .row(LocalDateTime.of(1970, 1, 1, 0, 0).atZone(ZoneOffset.ofHoursMinutes(-13, 0)))
                .row(LocalDateTime.of(9999 + 2, 12, 31, 23, 59, 59, 999_000_000).atZone(ZoneId.of("UTC")))
                .row(LocalDateTime.of(1, 1, 1, 0, 0, 0, 0).atZone(ZoneId.of("UTC")))
                // 73326-09-11T20:14:45.247Z[UTC] is the timestamp with tz farthest in the future Presto can represent (for UTC)
                .row(LocalDateTime.of(3326 + 70000, 9, 11, 20, 14, 45, 247_000_000).atZone(ZoneId.of("UTC")))
                // same instant as above for the negative offset with highest absolute value Snowflake allows
                .row(LocalDateTime.of(3326 + 70000, 9, 11, 7, 14, 45, 247_000_000).atZone(ZoneOffset.ofHoursMinutes(-13, 0)));

        if (includeNegativeYear) {
            expected
                    .row(LocalDateTime.of(-1, 1, 1, 0, 0, 0, 0).atZone(ZoneId.of("UTC")))
                    // -69387-04-22T03:45:14.753Z[UTC] is the timestamp with tz farthest in the past Presto can represent
                    .row(LocalDateTime.of(613 - 70000, 4, 22, 3, 45, 14, 753_000_000).atZone(ZoneId.of("UTC")))
                    // same instant as above for the max offset Snowflake allows
                    .row(LocalDateTime.of(613 - 70000, 4, 22, 17, 45, 14, 753_000_000).atZone(ZoneOffset.ofHoursMinutes(14, 0)));
        }

        try (TestTable testTable = new TestTable(
                snowflakeExecutor,
                tableName,
                "(a TIMESTAMP_TZ)",
                data.build())) {
            MaterializedResult actual = computeActual(session, "SELECT a FROM " + testTable.getName()).toTestTypes();

            assertEqualsIgnoreOrder(actual, expected.build());
        }
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .matches("CREATE TABLE \\w+\\.\\w+\\.orders \\Q(\n" +
                        "   orderkey decimal(38, 0),\n" +
                        "   custkey decimal(38, 0),\n" +
                        "   orderstatus varchar(1),\n" +
                        "   totalprice double,\n" +
                        "   orderdate date,\n" +
                        "   orderpriority varchar(15),\n" +
                        "   clerk varchar(15),\n" +
                        "   shippriority decimal(38, 0),\n" +
                        "   comment varchar(79)\n" +
                        ")");
    }

    @Test
    public void testPredicatePushdown()
    {
        try (TestTable testTable = new TestTable(snowflakeExecutor::execute, getSession().getSchema().orElseThrow() + ".test_aggregation_pushdown",
                "(" +
                        "bigint_column bigint, " +
                        "short_decimal decimal(9, 3), " +
                        "long_decimal decimal(30, 10), " +
                        "varchar_column varchar(10))")) {
            snowflakeExecutor.execute("INSERT INTO " + testTable.getName() + " VALUES (100, 100.000, 100000000.000000000, 'ala')");
            snowflakeExecutor.execute("INSERT INTO " + testTable.getName() + " VALUES (123, 123.321, 123456789.987654321, 'kot')");
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE bigint_column = 100")).isCorrectlyPushedDown();
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE bigint_column > 100")).isCorrectlyPushedDown();
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE short_decimal = 100")).isCorrectlyPushedDown();
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE short_decimal > 100")).isCorrectlyPushedDown();
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE long_decimal > 100000000")).isCorrectlyPushedDown();
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE long_decimal = 100000000")).isCorrectlyPushedDown();
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE varchar_column > 'ala'")).isCorrectlyPushedDown();
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE varchar_column = 'ala'")).isCorrectlyPushedDown();

            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE bigint_column > 100 and varchar_column > 'ala'")).isCorrectlyPushedDown();
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

        try (TestTable testTable = new TestTable(snowflakeExecutor::execute, getSession().getSchema().orElseThrow() + ".test_aggregation_pushdown",
                "(short_decimal decimal(9, 3), long_decimal decimal(30, 10), varchar_column varchar(10))")) {
            snowflakeExecutor.execute("INSERT INTO " + testTable.getName() + " VALUES (100.000, 100000000.000000000, 'ala')");
            snowflakeExecutor.execute("INSERT INTO " + testTable.getName() + " VALUES (123.321, 123456789.987654321, 'kot')");

            assertThat(query("SELECT min(short_decimal), min(long_decimal) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT max(short_decimal), max(long_decimal) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT sum(short_decimal), sum(long_decimal) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT avg(short_decimal), avg(long_decimal) FROM " + testTable.getName())).isCorrectlyPushedDown();

            // smoke testing of more complex cases

            // WHERE on aggregation column
            assertThat(query("SELECT min(short_decimal), min(long_decimal) FROM " + testTable.getName() + " WHERE short_decimal < 110 AND long_decimal < 124")).isCorrectlyPushedDown();
            // WHERE on non-aggregation column
            assertThat(query("SELECT min(long_decimal) FROM " + testTable.getName() + " WHERE short_decimal < 110")).isCorrectlyPushedDown();
            // GROUP BY
            assertThat(query("SELECT short_decimal, min(long_decimal) FROM " + testTable.getName() + " GROUP BY short_decimal")).isCorrectlyPushedDown();
            // GROUP BY with WHERE on both grouping and aggregation column
            assertThat(query("SELECT short_decimal, min(long_decimal) FROM " + testTable.getName() + " WHERE short_decimal < 110 AND long_decimal < 124" + " GROUP BY short_decimal")).isCorrectlyPushedDown();
            // GROUP BY with WHERE on grouping column
            assertThat(query("SELECT short_decimal, min(long_decimal) FROM " + testTable.getName() + " WHERE short_decimal < 110 GROUP BY short_decimal")).isCorrectlyPushedDown();
            // GROUP BY with WHERE on aggregation column
            assertThat(query("SELECT short_decimal, min(long_decimal) FROM " + testTable.getName() + " WHERE long_decimal < 124 GROUP BY short_decimal")).isCorrectlyPushedDown();
            // GROUP BY with WHERE on neither grouping nor aggregation column
            assertThat(query("SELECT short_decimal, min(long_decimal) FROM " + testTable.getName() + " WHERE varchar_column = 'ala' GROUP BY short_decimal")).isCorrectlyPushedDown();
            // aggregation on varchar column
            assertThat(query("SELECT min(varchar_column) FROM " + testTable.getName())).isCorrectlyPushedDown();
            // aggregation on varchar column with GROUPING
            assertThat(query("SELECT short_decimal, min(varchar_column) FROM " + testTable.getName() + " GROUP BY short_decimal")).isCorrectlyPushedDown();
            // aggregation on varchar column with WHERE
            assertThat(query("SELECT min(varchar_column) FROM " + testTable.getName() + " WHERE varchar_column ='ala'")).isCorrectlyPushedDown();

            // agg(DISTINCT symbol) not supported yet
            assertThat(query("SELECT min(DISTINCT short_decimal) FROM " + testTable.getName())).isNotFullyPushedDown(AggregationNode.class);
            // TODO: Improve assertion framework. Here min(long_decimal) is pushed down. There remains ProjectNode above it which relates to DISTINCT in the query.
            assertThat(query("SELECT DISTINCT short_decimal, min(long_decimal) FROM " + testTable.getName() + " GROUP BY short_decimal")).isNotFullyPushedDown(ProjectNode.class);
        }

        try (TestTable testTable = new TestTable(snowflakeExecutor::execute, getSession().getSchema().orElseThrow() + ".test_aggregation_pushdown_timestamp_tz",
                "(timestamp_tz_column timestamp with time zone)")) {
            snowflakeExecutor.execute("INSERT INTO " + testTable.getName() + " VALUES (TIMESTAMP '1901-02-03 04:05:06.789')");
            snowflakeExecutor.execute("INSERT INTO " + testTable.getName() + " VALUES (TIMESTAMP '1911-02-03 04:05:06.789')");

            // Adding specific testcase for TIMESTAMP WITH TIME ZONE as it requires special rewrite handling when sending query to SF.
            assertThat(query("SELECT min(timestamp_tz_column) FROM " + testTable.getName())).isCorrectlyPushedDown();
        }
    }
}
