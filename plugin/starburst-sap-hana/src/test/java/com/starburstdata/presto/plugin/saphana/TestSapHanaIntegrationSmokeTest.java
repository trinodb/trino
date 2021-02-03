/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.saphana;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestIntegrationSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.testng.annotations.Test;

import static com.starburstdata.presto.plugin.saphana.SapHanaQueryRunner.createSapHanaQueryRunner;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.REGION;
import static org.assertj.core.api.Assertions.assertThat;

@Test
public class TestSapHanaIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    protected TestingSapHanaServer server;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        server = closeAfterClass(new TestingSapHanaServer());
        return createSapHanaQueryRunner(
                server,
                ImmutableMap.<String, String>builder()
                        .put("metadata.cache-ttl", "0m")
                        .put("metadata.cache-missing", "false")
                        .build(),
                ImmutableMap.of(),
                ImmutableList.of(CUSTOMER, NATION, ORDERS, REGION));
    }

    @Test
    public void testPredicatePushdown()
    {
        // varchar equality
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'ROMANIA'"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                .isFullyPushedDown();

        // varchar range
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name BETWEEN 'POLAND' AND 'RPA'"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                .isFullyPushedDown();

        // varchar different case
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'romania'"))
                .returnsEmptyResult()
                .isFullyPushedDown();

        // bigint equality
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE nationkey = 19"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                .isFullyPushedDown();

        // bigint range, with decimal to bigint simplification
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE nationkey BETWEEN 18.5 AND 19.5"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                .isFullyPushedDown();

        // date equality
        assertThat(query("SELECT orderkey FROM orders WHERE orderdate = DATE '1992-09-29'"))
                .matches("VALUES BIGINT '1250', 34406, 38436, 57570")
                .isFullyPushedDown();
    }

    @Test
    public void testDecimalPredicatePushdown()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        try (TestTable testTable = new TestTable(server::execute, schemaName + ".test_decimal_pushdown",
                "(short_decimal decimal(9, 3), long_decimal decimal(30, 10))", ImmutableList.of("123.321, 123456789.987654321"))) {
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE short_decimal <= 124"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE short_decimal <= 124"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE long_decimal <= 123456790"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE short_decimal <= 123.321"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE long_decimal <= 123456789.987654321"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE short_decimal = 123.321"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE long_decimal = 123456789.987654321"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
        }
    }

    @Test
    public void testCharPredicatePushdown()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        try (TestTable testTable = new TestTable(server::execute, schemaName + ".test_char_pushdown",
                "(char_1 char(1), char_5 char(5), char_10 char(10))", ImmutableList.of("'0', '0', '0'", "'1', '12345', '1234567890'"))) {
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE char_1 = '0' AND char_5 = '0'"))
                    .matches("VALUES (CHAR'0', CHAR'0    ', CHAR'0         ')")
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE char_5 = CHAR'12345' AND char_10 = '1234567890'"))
                    .matches("VALUES (CHAR'1', CHAR'12345', CHAR'1234567890')")
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE char_10 = CHAR'0'"))
                    .matches("VALUES (CHAR'0', CHAR'0    ', CHAR'0         ')")
                    .isFullyPushedDown();
        }
    }

    @Test
    public void testLimitPushdown()
    {
        assertThat(query("SELECT name FROM nation LIMIT 30")).isFullyPushedDown(); // Use high limit for result determinism

        // with filter over numeric column
        assertThat(query("SELECT name FROM nation WHERE regionkey = 3 LIMIT 5")).isFullyPushedDown();

        // with filter over varchar column
        assertThat(query("SELECT name FROM nation WHERE name < 'EEE' LIMIT 5")).isFullyPushedDown();

        // with aggregation
        assertThat(query("SELECT max(regionkey) FROM nation LIMIT 5")).isFullyPushedDown(); // global aggregation, LIMIT removed
        assertThat(query("SELECT regionkey, max(name) FROM nation GROUP BY regionkey LIMIT 5")).isFullyPushedDown();
        assertThat(query("SELECT DISTINCT regionkey FROM nation LIMIT 5")).isFullyPushedDown();

        // with filter and aggregation
        assertThat(query("SELECT regionkey, count(*) FROM nation WHERE nationkey < 5 GROUP BY regionkey LIMIT 3")).isFullyPushedDown();
        assertThat(query("SELECT regionkey, count(*) FROM nation WHERE name < 'EGYPT' GROUP BY regionkey LIMIT 3")).isFullyPushedDown();
    }

    @Test
    public void testAggregationPushdown()
    {
        // TODO support aggregation pushdown with GROUPING SETS
        // TODO support aggregation over expressions

        // SELECT DISTINCT
        assertThat(query("SELECT DISTINCT regionkey FROM nation")).isFullyPushedDown();

        // count()
        assertThat(query("SELECT count(*) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT count(nationkey) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT count(1) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT count() FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT count(DISTINCT regionkey) FROM nation")).isFullyPushedDown();

        // GROUP BY
        assertThat(query("SELECT regionkey, min(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        assertThat(query("SELECT regionkey, max(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        assertThat(query("SELECT regionkey, avg(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();

        // GROUP BY and WHERE on bigint column
        // GROUP BY and WHERE on aggregation key
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation WHERE regionkey < 4 GROUP BY regionkey")).isFullyPushedDown();

        // GROUP BY and WHERE on varchar column
        // GROUP BY and WHERE on "other" (not aggregation key, not aggregation input)
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation WHERE regionkey < 4 AND name > 'AAA' GROUP BY regionkey")).isFullyPushedDown();
    }

    @Test
    public void testStddevAggregationPushdown()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        try (TestTable testTable = new TestTable(server::execute, schemaName + ".test_stddev_pushdown",
                "(t_double DOUBLE PRECISION)")) {
            assertThat(query("SELECT stddev_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            server.execute("INSERT INTO " + testTable.getName() + " VALUES (1)");

            assertThat(query("SELECT stddev_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            server.execute("INSERT INTO " + testTable.getName() + " VALUES (3)");
            assertThat(query("SELECT stddev_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            server.execute("INSERT INTO " + testTable.getName() + " VALUES (5)");
            assertThat(query("SELECT stddev(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();
        }

        try (TestTable testTable = new TestTable(server::execute, schemaName + ".test_stddev_pushdown",
                "(t_double DOUBLE PRECISION)")) {
            // Test non-whole number results
            server.execute("INSERT INTO " + testTable.getName() + " VALUES (1)");
            server.execute("INSERT INTO " + testTable.getName() + " VALUES (2)");
            server.execute("INSERT INTO " + testTable.getName() + " VALUES (4)");
            server.execute("INSERT INTO " + testTable.getName() + " VALUES (5)");

            assertThat(query("SELECT stddev_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();
        }
    }

    @Test
    public void testVarianceAggregationPushdown()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        try (TestTable testTable = new TestTable(server::execute, schemaName + ".test_variance_pushdown",
                "(t_double DOUBLE PRECISION)")) {
            assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            server.execute("INSERT INTO " + testTable.getName() + " VALUES (1)");

            assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            server.execute("INSERT INTO " + testTable.getName() + " VALUES (3)");
            assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            server.execute("INSERT INTO " + testTable.getName() + " VALUES (5)");
            assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();
        }

        try (TestTable testTable = new TestTable(server::execute, schemaName + ".test_variance_pushdown",
                "(t_double DOUBLE PRECISION)")) {
            // Test non-whole number results
            server.execute("INSERT INTO " + testTable.getName() + " VALUES (1)");
            server.execute("INSERT INTO " + testTable.getName() + " VALUES (2)");
            server.execute("INSERT INTO " + testTable.getName() + " VALUES (3)");
            server.execute("INSERT INTO " + testTable.getName() + " VALUES (4)");
            server.execute("INSERT INTO " + testTable.getName() + " VALUES (5)");

            assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();
        }
    }

    @Test
    public void testSelectFromStandardView()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        String viewName = schemaName + ".nation_view_" + randomTableSuffix();
        server.execute("CREATE VIEW " + viewName + " AS SELECT nationkey FROM " + schemaName + ".nation WHERE name = 'ROMANIA'");
        assertThat(query("SELECT * FROM " + viewName)).matches("VALUES BIGINT '19'");
    }

    @Test
    public void testSelectFromStandardDimensionTables()
    {
        assertThat(query("SELECT * FROM _SYS_BI.M_TIME_DIMENSION_YEAR")).returnsEmptyResult();
        assertThat((String) computeActual("SHOW CREATE TABLE _SYS_BI.M_TIME_DIMENSION_YEAR").getOnlyValue())
                .isEqualTo("CREATE TABLE saphana._sys_bi.m_time_dimension_year (\n" +
                        "   year varchar(4) NOT NULL,\n" +
                        "   year_int integer,\n" +
                        "   is_leap_year tinyint\n" +
                        ")");

        assertThat(query("SELECT * FROM _SYS_BI.M_TIME_DIMENSION_MONTH")).returnsEmptyResult();
        assertThat((String) computeActual("SHOW CREATE TABLE _SYS_BI.M_TIME_DIMENSION_MONTH").getOnlyValue())
                .isEqualTo("CREATE TABLE saphana._sys_bi.m_time_dimension_month (\n" +
                        "   year varchar(4) NOT NULL,\n" +
                        "   halfyear varchar(2),\n" +
                        "   quarter varchar(2),\n" +
                        "   month varchar(2) NOT NULL,\n" +
                        "   calquarter varchar(5),\n" +
                        "   calmonth varchar(6),\n" +
                        "   year_int integer,\n" +
                        "   halfyear_int tinyint,\n" +
                        "   quarter_int tinyint,\n" +
                        "   month_int tinyint\n" +
                        ")");

        assertThat(query("SELECT * FROM _SYS_BI.M_TIME_DIMENSION_WEEK")).returnsEmptyResult();
        assertThat((String) computeActual("SHOW CREATE TABLE _SYS_BI.M_TIME_DIMENSION_WEEK").getOnlyValue())
                .isEqualTo("CREATE TABLE saphana._sys_bi.m_time_dimension_week (\n" +
                        "   year varchar(4) NOT NULL,\n" +
                        "   halfyear varchar(2),\n" +
                        "   quarter varchar(2),\n" +
                        "   month varchar(2),\n" +
                        "   week varchar(2) NOT NULL,\n" +
                        "   calquarter varchar(5),\n" +
                        "   calmonth varchar(6),\n" +
                        "   calweek varchar(6),\n" +
                        "   year_int integer,\n" +
                        "   halfyear_int tinyint,\n" +
                        "   quarter_int tinyint,\n" +
                        "   month_int tinyint,\n" +
                        "   week_int tinyint\n" +
                        ")");

        assertThat(query("SELECT * FROM _SYS_BI.M_TIME_DIMENSION")).returnsEmptyResult();
        assertThat((String) computeActual("SHOW CREATE TABLE _SYS_BI.M_TIME_DIMENSION").getOnlyValue())
                .isEqualTo("CREATE TABLE saphana._sys_bi.m_time_dimension (\n" +
                        "   datetimestamp timestamp(7) NOT NULL,\n" +
                        "   date_sql date,\n" +
                        "   datetime_sap varchar(14),\n" +
                        "   date_sap varchar(8),\n" +
                        "   year varchar(4),\n" +
                        "   quarter varchar(2),\n" +
                        "   month varchar(2),\n" +
                        "   week varchar(2),\n" +
                        "   week_year varchar(4),\n" +
                        "   day_of_week varchar(2),\n" +
                        "   day varchar(2),\n" +
                        "   hour varchar(2),\n" +
                        "   minute varchar(2),\n" +
                        "   second varchar(2),\n" +
                        "   calquarter varchar(5),\n" +
                        "   calmonth varchar(6),\n" +
                        "   calweek varchar(6),\n" +
                        "   year_int integer,\n" +
                        "   quarter_int tinyint,\n" +
                        "   month_int tinyint,\n" +
                        "   week_int tinyint,\n" +
                        "   week_year_int integer,\n" +
                        "   day_of_week_int tinyint,\n" +
                        "   day_int tinyint,\n" +
                        "   hour_int tinyint,\n" +
                        "   minute_int tinyint,\n" +
                        "   second_int tinyint,\n" +
                        "   month_last_day tinyint,\n" +
                        "   tzntstmps decimal(15, 0),\n" +
                        "   tzntstmpl decimal(21, 7)\n" +
                        ")");

        assertThat(query("SELECT * FROM _SYS_BI.M_FISCAL_CALENDAR")).returnsEmptyResult();
        assertThat((String) computeActual("SHOW CREATE TABLE _SYS_BI.M_FISCAL_CALENDAR").getOnlyValue())
                .isEqualTo("CREATE TABLE saphana._sys_bi.m_fiscal_calendar (\n" +
                        "   calendar_variant varchar(2) NOT NULL,\n" +
                        "   date varchar(8) NOT NULL,\n" +
                        "   date_sql date,\n" +
                        "   fiscal_year varchar(4),\n" +
                        "   fiscal_period varchar(3),\n" +
                        "   current_year_adjustment varchar(2)\n" +
                        ")");
    }
}
