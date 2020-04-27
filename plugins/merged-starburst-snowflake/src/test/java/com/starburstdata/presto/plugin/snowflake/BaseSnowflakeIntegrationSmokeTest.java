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
import io.prestosql.spi.type.Type;
import io.prestosql.testing.AbstractTestIntegrationSmokeTest;
import io.prestosql.testing.H2QueryRunner;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.MaterializedRow;
import io.prestosql.testing.QueryAssertions;
import io.prestosql.testing.TestingSession;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;

import static com.google.common.base.Strings.repeat;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
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
    private final String tableSuffix = randomTableSuffix();

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
        String tableName = "test_drop_" + tableSuffix;
        assertUpdate(format("CREATE TABLE %s AS SELECT 1 test_drop", tableName), 1);
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));

        assertUpdate(format("DROP TABLE %s", tableName));
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test
    public void testInsert()
            throws SQLException
    {
        String tableName = "test_insert_" + tableSuffix;
        server.execute(format("CREATE TABLE test_schema.%s (x number(19), y varchar(100))", tableName));
        assertUpdate(format("INSERT INTO %s VALUES (123, 'test')", tableName), 1);
        assertQuery(format("SELECT * FROM %s", tableName), "SELECT 123 x, 'test' y");
        server.execute(format("DROP TABLE test_schema.%s", tableName));
    }

    @Test
    public void testViews()
            throws SQLException
    {
        String viewName = "test_view_" + tableSuffix;
        server.execute(format("CREATE VIEW test_schema.%s AS SELECT * FROM orders", viewName));
        assertTrue(getQueryRunner().tableExists(getSession(), viewName));
        assertQuery(format("SELECT orderkey FROM %s", viewName), "SELECT orderkey FROM orders");
        server.execute(format("DROP VIEW test_schema.%s", viewName));
    }

    @Test
    public void testPredicatePushdownForNumerics()
            throws SQLException
    {
        String tableName = "test_predicate_pushdown_numeric_" + tableSuffix;
        getQueryRunner().execute(getSession(), format("DROP TABLE IF EXISTS %s", tableName));
        server.execute(format("CREATE TABLE test_schema.%s(c_binary_float FLOAT, c_binary_double DOUBLE, c_number NUMBER(5,3))", tableName));
        server.execute(format("INSERT INTO test_schema.%s VALUES(5.0, 20.233, 5.0)", tableName));

        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertQuery(format("SELECT c_binary_double FROM %s WHERE c_binary_float = cast(5.0 as real)", tableName), "SELECT 20.233");
        assertQuery(format("SELECT c_binary_float FROM %s WHERE c_binary_double = cast(20.233 as double)", tableName), "SELECT 5.0");
        assertQuery(format("SELECT c_binary_float FROM %s WHERE c_number = cast(5.0 as decimal(5,3))", tableName), "SELECT 5.0");

        server.execute(format("DROP TABLE test_schema.%s", tableName));
    }

    @Test
    public void testPredicatePushdownForChars()
            throws SQLException
    {
        String tableName = "test_predicate_pushdown_char_" + tableSuffix;
        getQueryRunner().execute(getSession(), format("DROP TABLE IF EXISTS %s", tableName));
        server.execute(format("CREATE TABLE test_schema.%s(c_char CHAR(7), c_varchar VARCHAR(20), c_long_char CHAR(2000), c_long_varchar VARCHAR(4000))", tableName));
        server.execute(format("INSERT INTO test_schema.%s VALUES('my_char', 'my_varchar', 'my_long_char', 'my_long_varchar')", tableName));

        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertQuery(format("SELECT c_char FROM %s WHERE c_varchar = cast('my_varchar' as varchar(20))", tableName), "SELECT 'my_char'");
        assertQueryReturnsEmptyResult(format("SELECT c_char FROM %s WHERE c_long_char = '" + repeat("💩", 2000) + "'", tableName));
        assertQueryReturnsEmptyResult(format("SELECT c_char FROM %s WHERE c_long_varchar = '" + repeat("💩", 4000) + "'", tableName));

        server.execute(format("DROP TABLE test_schema.%s", tableName));
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

    @DataProvider
    public Object[][] testLegacyFlagProvider()
    {
        return new Object[][] {
                {true},
                {false}
        };
    }

    @Test(dataProvider = "testLegacyFlagProvider")
    public void testTimeRounding(boolean legacyTimestamp)
    {
        String tableName = "test_time_" + tableSuffix;
        for (ZoneId sessionZone : ImmutableList.of(ZoneOffset.UTC, ZoneId.systemDefault(), ZoneId.of("Europe/Vilnius"), ZoneId.of("Asia/Kathmandu"), ZoneId.of(TestingSession.DEFAULT_TIME_ZONE_KEY.getId()))) {
            Session session = Session.builder(getQueryRunner().getDefaultSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                    .setSystemProperty("legacy_timestamp", Boolean.toString(legacyTimestamp))
                    .build();
            getQueryRunner().execute(getSession(), format("CREATE TABLE test_schema.%s (x TIME)", tableName));
            assertUpdate(session, format("INSERT INTO %s VALUES (TIME '12:34:56.123')", tableName), 1);
            assertQuery(session, format("SELECT * FROM %s WHERE rand() > 42 OR x = TIME '12:34:56.123'", tableName), "SELECT '12:34:56.123' x");
            getQueryRunner().execute(getSession(), format("DROP TABLE test_schema.%s", tableName));
        }
    }

    @Test
    public void testTimestampWithTimezoneValues()
            throws SQLException
    {
        String tableName = "test_tstz_" + tableSuffix;
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(ZoneOffset.UTC.getId()))
                .setSystemProperty("legacy_timestamp", "false")
                .build();
        server.execute(
                format("USE SCHEMA %s", TEST_SCHEMA),
                format("CREATE TABLE test_schema.%s (a TIMESTAMP_TZ)", tableName),
                format(
                        "INSERT INTO %s VALUES " +
                                "(TO_TIMESTAMP_TZ('1970-01-01T00:00:00.000 +14:00'))," +
                                "(TO_TIMESTAMP_TZ('1970-01-01T00:00:00.000 -13:00'))," +
                                "(DATEADD(YEAR, 2, TO_TIMESTAMP_TZ('9999-12-31T23:59:59.999Z')))," +
                                "(DATEADD(YEAR, -2, TO_TIMESTAMP_TZ('0001-01-01T00:00:00.000Z')))," +
                                // Snowflake literals cannot have a 5-digit year
                                "(DATEADD(YEAR, 70000, TO_TIMESTAMP_TZ('3326-09-11T20:14:45.247Z')))," +
                                "(DATEADD(YEAR, 70000, TO_TIMESTAMP_TZ('3326-09-11T07:14:45.247 -13:00')))," +
                                // Snowflake literals cannot have a negative year
                                "(DATEADD(YEAR, -70000, TO_TIMESTAMP_TZ('613-04-22T03:45:14.753Z')))," +
                                "(DATEADD(YEAR, -70000, TO_TIMESTAMP_TZ('613-04-22T17:45:14.753 +14:00')))",
                        tableName));
        QueryAssertions.assertQuery(
                getQueryRunner(),
                session,
                format("SELECT a FROM %s", tableName),
                new H2QueryRunner()
                {
                    @Override
                    public MaterializedResult execute(Session session, String sql, List<? extends Type> resultTypes)
                    {
                        return new MaterializedResult(
                                ImmutableList.of(
                                        new MaterializedRow(1, ZonedDateTime.parse("1970-01-01T00:00:00.000Z[+14:00]")),
                                        new MaterializedRow(1, ZonedDateTime.parse("1970-01-01T00:00:00.000Z[-13:00]")),
                                        new MaterializedRow(1, ZonedDateTime.parse("9999-12-31T23:59:59.999Z[UTC]").plusYears(2)),
                                        new MaterializedRow(1, ZonedDateTime.parse("0001-01-01T00:00:00.000Z[UTC]").minusYears(2)),
                                        // 73326-09-11T20:14:45.247Z[UTC] is the timestamp with tz farthest in the future Presto can represent (for UTC)
                                        new MaterializedRow(1, ZonedDateTime.parse("3326-09-11T20:14:45.247Z[UTC]").plusYears(70000)),
                                        // same instant as above for the negative offset with highest absolute value Snowflake allows
                                        new MaterializedRow(1, ZonedDateTime.parse("3326-09-11T07:14:45.247Z[-13:00]").plusYears(70000)),
                                        // -69387-04-22T03:45:14.753Z[UTC] is the timestamp with tz farthest in the past Presto can represent
                                        new MaterializedRow(1, ZonedDateTime.parse("0613-04-22T03:45:14.753Z[UTC]").minusYears(70000)),
                                        // same instant as above for the max offset Snowflake allows
                                        new MaterializedRow(1, ZonedDateTime.parse("0613-04-22T17:45:14.753Z[+14:00]").minusYears(70000))),
                                resultTypes);
                    }
                },
                "",
                false,
                false);
        server.execute(format("DROP TABLE test_schema.%s", tableName));
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
}
