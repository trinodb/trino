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
import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Verify.verify;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class BaseSapHanaConnectorTest
        extends BaseJdbcConnectorTest
{
    protected TestingSapHanaServer server;

    @Override
    @SuppressWarnings("DuplicateBranchesInSwitch") // options here are grouped per-feature
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_AGGREGATION_PUSHDOWN_STDDEV:
            case SUPPORTS_AGGREGATION_PUSHDOWN_VARIANCE:
            case SUPPORTS_AGGREGATION_PUSHDOWN_COUNT_DISTINCT:
            case SUPPORTS_JOIN_PUSHDOWN:
                return true;
            case SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM:
                return false;

            case SUPPORTS_COMMENT_ON_TABLE:
            case SUPPORTS_COMMENT_ON_COLUMN:
            case SUPPORTS_ADD_COLUMN_WITH_COMMENT:
            case SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT:
            case SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT:
                return false;

            case SUPPORTS_RENAME_SCHEMA:
                return false;

            case SUPPORTS_ARRAY:
            case SUPPORTS_ROW_TYPE:
            case SUPPORTS_NEGATIVE_DATE:
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    public void testDateYearOfEraPredicate()
    {
        // SAP HANA connector throws an exception for negative date values instead of an empty result
        assertQuery("SELECT orderdate FROM orders WHERE orderdate = DATE '1997-09-14'", "VALUES DATE '1997-09-14'");
        assertQueryFails("SELECT * FROM orders WHERE orderdate = DATE '-1996-09-14'", "SAP DBTech JDBC: Cannot convert data -1996-09-14 to type java.sql.Date.");
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                server::execute,
                "tpch.table",
                "(col_required BIGINT NOT NULL," +
                        "col_nullable BIGINT," +
                        "col_default BIGINT DEFAULT 43," +
                        "col_nonnull_default BIGINT NOT NULL DEFAULT 42," +
                        "col_required2 BIGINT NOT NULL)");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        switch (dataMappingTestSetup.getTrinoTypeName()) {
            case "time":
                verify(dataMappingTestSetup.getHighValueLiteral().equals("TIME '23:59:59.999'"), "super has changed high value for TIME");
                return Optional.of(
                        new DataMappingTestSetup(
                                dataMappingTestSetup.getTrinoTypeName(),
                                dataMappingTestSetup.getSampleValueLiteral(),
                                "TIME '23:59:59.000'")); // SAP HANA does not store second fraction, so 23:59:59.999 would became 00:00:00
            case "time(6)":
                // TODO https://starburstdata.atlassian.net/browse/SEP-9302
                return Optional.empty();
            case "timestamp(3) with time zone":
            case "timestamp(6) with time zone":
                return Optional.of(dataMappingTestSetup.asUnsupported());

            case "date":
                return Optional.of(dataMappingTestSetup)
                        .filter(testSetup -> !testSetup.getSampleValueLiteral().equals("DATE '1582-10-05'"));
        }

        return Optional.of(dataMappingTestSetup);
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
    public void testDecimalAvgPushdown()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        try (TestTable testTable = new TestTable(onRemoteDatabase(),
                schemaName + ".test_agg_pushdown_avg_max_decimal",
                "(t_decimal DECIMAL(38, 10))",
                ImmutableList.of("12345789.9876543210", format("%s.%s", "1".repeat(28), "9".repeat(10))))) {
            // For max decimal precision we cannot extend the scale and precision and hence the result doesn't match Trino avg semantics
            assertThatThrownBy(() -> assertThat(query("SELECT avg(t_decimal) FROM " + testTable.getName())).isFullyPushedDown())
                    .isInstanceOf(AssertionError.class)
                    .hasMessageContaining("elements not found:\n" +
                            "  <(555555555555555555561728450.9938271605)>\n" +
                            "and elements not expected:\n" +
                            "  <(555555555555555555561728450.9938270000)>");
        }

        try (TestTable testTable = new TestTable(onRemoteDatabase(),
                schemaName + ".test_agg_pushdown_avg_max_decimal",
                "(t_decimal DECIMAL(18, 18))",
                ImmutableList.of("0.987654321234567890", format("0.%s", "1".repeat(18))))) {
            // For decimal precisions lower than max supported precision we perform correct pushdown by extending scale and precision
            assertThat(query("SELECT avg(t_decimal) FROM " + testTable.getName())).isFullyPushedDown();
        }
    }

    @Test
    public void testSelectFromStandardView()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        String viewName = schemaName + ".nation_view_" + randomNameSuffix();
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

    @Override
    public void testNativeQuerySimple()
    {
        assertQuery("SELECT * FROM TABLE(system.query(query => 'SELECT 1 from dummy'))", "VALUES 1");
    }

    @Override
    public void testNativeQueryParameters()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query_simple", "SELECT * FROM TABLE(system.query(query => ?))")
                .addPreparedStatement("my_query", "SELECT * FROM TABLE(system.query(query => format('SELECT %s FROM %s', ?, ?)))")
                .build();
        assertQuery(session, "EXECUTE my_query_simple USING 'SELECT 1 a FROM dummy'", "VALUES 1");
        assertQuery(session, "EXECUTE my_query USING 'a', '(SELECT 2 a FROM dummy) t'", "VALUES 2");
    }

    @Override
    public void testDeleteWithLike()
    {
        assertThatThrownBy(super::testDeleteWithLike)
                .hasStackTraceContaining("TrinoException: This connector does not support modifying table rows");
    }

    @Test
    @Override
    public void testAddNotNullColumnToNonEmptyTable()
    {
        throw new SkipException("https://starburstdata.atlassian.net/browse/SEP-9683");
    }

    @Test
    @Override
    public void testInsertIntoNotNullColumn()
    {
        throw new SkipException("https://starburstdata.atlassian.net/browse/SEP-9684");
    }

    @Override
    protected Session joinPushdownEnabled(Session session)
    {
        return Session.builder(super.joinPushdownEnabled(session))
                // strategy is AUTOMATIC by default and would not work for certain test cases (even if statistics are collected)
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "join_pushdown_strategy", "EAGER")
                .build();
    }

    @Override
    protected OptionalInt maxSchemaNameLength()
    {
        return OptionalInt.of(127);
    }

    @Override
    protected void verifySchemaNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("Maximum length is 127");
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        return OptionalInt.of(127);
    }

    @Override
    protected void verifyTableNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("Maximum length is 127");
    }

    @Override
    protected OptionalInt maxColumnNameLength()
    {
        return OptionalInt.of(127);
    }

    @Override
    protected void verifyColumnNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("Maximum length is 127");
    }

    @Override
    protected String errorMessageForCreateTableAsSelectNegativeDate(String date)
    {
        return "SAP DBTech JDBC: Cannot convert data -0001-01-01 to type java.sql.Date.";
    }

    @Override
    protected String errorMessageForInsertNegativeDate(String date)
    {
        return "SAP DBTech JDBC: Cannot convert data -0001-01-01 to type java.sql.Date.";
    }

    @Override
    protected String errorMessageForInsertIntoNotNullColumn(String columnName)
    {
        return format(".*cannot insert NULL or update to NULL: %s.*", columnName.toUpperCase(ENGLISH));
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return server::execute;
    }
}
