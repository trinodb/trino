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
import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.spi.type.TimeZoneKey;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.testing.MaterializedResult;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.TestingSession;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Optional;

import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.base.Strings.repeat;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static io.trino.testing.assertions.Assert.assertEquals;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public abstract class BaseSnowflakeConnectorTest
        // Using BaseJdbcConnectorTest as a base class is not strictly accurate as we have to flavours of Snowflake connector: jdbc and distributed.
        // Still most of the extra testcases defined in BaseJdbcConnectorTest are applicable to both.
        extends BaseJdbcConnectorTest
{
    protected final SnowflakeServer server = new SnowflakeServer();
    protected final SqlExecutor snowflakeExecutor = server::safeExecute;

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_ARRAY:
            case SUPPORTS_COMMENT_ON_TABLE:
            case SUPPORTS_COMMENT_ON_COLUMN:
            case SUPPORTS_ROW_TYPE:
            case SUPPORTS_ADD_COLUMN_WITH_COMMENT:
                return false;
            case SUPPORTS_AGGREGATION_PUSHDOWN_STDDEV:
            case SUPPORTS_AGGREGATION_PUSHDOWN_VARIANCE:
            case SUPPORTS_AGGREGATION_PUSHDOWN_COVARIANCE:
            case SUPPORTS_AGGREGATION_PUSHDOWN_CORRELATION:
            case SUPPORTS_AGGREGATION_PUSHDOWN_REGRESSION:
            case SUPPORTS_AGGREGATION_PUSHDOWN_COUNT_DISTINCT:
            case SUPPORTS_JOIN_PUSHDOWN:
                return true;
            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    public void testCharVarcharComparison()
    {
        // Snowflake does not have a CHAR type. They map it to varchar, which does not have the same fixed width semantics
        assertThatThrownBy(super::testCharVarcharComparison)
                .isInstanceOf(AssertionError.class);

        // Also assert that CHAR columns end up as VARCHAR in Snowflake
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_char_is_varchar_",
                "AS SELECT CHAR 'is_actually_a_varchar' AS a_char")) {
            assertThat((String) computeActual("SHOW CREATE TABLE " + table.getName()).getOnlyValue())
                    .matches("CREATE TABLE \\w+\\.\\w+\\.\\w+ \\Q(\n" +
                            "   a_char varchar(21)\n" +
                            ")");
        }
    }

    @Override
    protected boolean isColumnNameRejected(Exception exception, String columnName, boolean delimited)
    {
        // Snowflake does not support column names containing double quotes
        return columnName.contains("\"") && nullToEmpty(exception.getMessage()).matches(".*(Snowflake columns cannot contain quotes).*");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        // Real: Snowflake does not have a REAL type, instead they are mapped to double. The round trip test fails because REAL '567.123' != DOUBLE '567.123'
        // Char: Snowflake does not have a CHAR type. They map it to varchar, which does not have the same fixed width semantics
        String name = dataMappingTestSetup.getTrinoTypeName();
        if (name.equals("real") || name.startsWith("char")) {
            return Optional.empty();
        }
        return Optional.of(dataMappingTestSetup);
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                server::safeExecute,
                format("%s.test_table_with_default_columns", TEST_SCHEMA),
                "(col_required BIGINT NOT NULL," +
                        "col_nullable BIGINT," +
                        "col_default BIGINT DEFAULT 43," +
                        "col_nonnull_default BIGINT NOT NULL DEFAULT 42," +
                        "col_required2 BIGINT NOT NULL)");
    }

    @Override
    public void testShowColumns()
    {
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");

        MaterializedResult expectedParametrizedVarchar = resultBuilder(getSession(),
                VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "decimal(19,0)", "", "")
                .row("custkey", "decimal(19,0)", "", "")
                .row("orderstatus", "varchar(1)", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar(15)", "", "")
                .row("clerk", "varchar(15)", "", "")
                .row("shippriority", "decimal(10,0)", "", "")
                .row("comment", "varchar(79)", "", "")
                .build();

        assertEquals(actual, expectedParametrizedVarchar);
    }

    @Test
    public void testDescribeInput()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "SELECT ? FROM nation WHERE nationkey = ? and name < ?")
                .build();
        MaterializedResult actual = computeActual(session, "DESCRIBE INPUT my_query");
        MaterializedResult expected = resultBuilder(session, BIGINT, VARCHAR)
                .row(0, "unknown")
                .row(1, "decimal(19,0)")
                .row(2, "varchar(25)")
                .build();
        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testDescribeOutput()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "SELECT * FROM nation")
                .build();

        MaterializedResult actual = computeActual(session, "DESCRIBE OUTPUT my_query");
        MaterializedResult expected = resultBuilder(session, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BOOLEAN)
                .row("nationkey", session.getCatalog().get(), session.getSchema().get(), "nation", "decimal(19,0)", 16, false)
                .row("name", session.getCatalog().get(), session.getSchema().get(), "nation", "varchar(25)", 0, false)
                .row("regionkey", session.getCatalog().get(), session.getSchema().get(), "nation", "decimal(19,0)", 16, false)
                .row("comment", session.getCatalog().get(), session.getSchema().get(), "nation", "varchar(152)", 0, false)
                .build();
        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testDescribeOutputNamedAndUnnamed()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "SELECT 1, name, regionkey AS my_alias FROM nation")
                .build();

        MaterializedResult actual = computeActual(session, "DESCRIBE OUTPUT my_query");
        MaterializedResult expected = resultBuilder(session, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BOOLEAN)
                .row("_col0", "", "", "", "integer", 4, false)
                .row("name", session.getCatalog().get(), session.getSchema().get(), "nation", "varchar(25)", 0, false)
                .row("my_alias", session.getCatalog().get(), session.getSchema().get(), "nation", "decimal(19,0)", 16, true)
                .build();
        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    @Override
    public void testInformationSchemaFiltering()
    {
        assertQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'orders' AND table_schema = 'test_schema_2' LIMIT 1",
                "SELECT 'orders'");
        assertQuery(
                "SELECT table_name FROM information_schema.columns WHERE data_type = 'decimal(19,0)' AND table_schema = 'test_schema_2' AND table_name = 'customer' and column_name = 'custkey' LIMIT 1",
                "SELECT 'customer'");
    }

    @Override
    public void testTableSampleBernoulli()
    {
        throw new SkipException("This test takes more than 10 minutes to finish.");
    }

    @Override
    public void testDescribeTable()
    {
        MaterializedResult actualColumns = computeActual(
                getSession(), "DESC ORDERS").toTestTypes();

        MaterializedResult expectedColumns = resultBuilder(
                getSession(),
                VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "decimal(19,0)", "", "")
                .row("custkey", "decimal(19,0)", "", "")
                .row("orderstatus", "varchar(1)", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
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
        String tableName = "test_drop_" + randomTableSuffix();
        assertUpdate(format("CREATE TABLE %s AS SELECT 1 test_drop", tableName), 1);
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));

        assertUpdate(format("DROP TABLE %s", tableName));
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test
    public void testViews()
            throws SQLException
    {
        String viewName = "test_view_" + randomTableSuffix();
        server.execute(format("CREATE VIEW test_schema_2.%s AS SELECT * FROM orders", viewName));
        assertTrue(getQueryRunner().tableExists(getSession(), viewName));
        assertQuery(format("SELECT orderkey FROM %s", viewName), "SELECT orderkey FROM orders");
        server.execute(format("DROP VIEW test_schema_2.%s", viewName));
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
    public void testTooLargeDomainCompactionThreshold()
    {
        assertQueryFails(
                Session.builder(getSession())
                        .setCatalogSessionProperty("snowflake", "domain_compaction_threshold", "10000")
                        .build(),
                "SELECT * from nation", "Domain compaction threshold \\(10000\\) cannot exceed 1000");
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

        MaterializedResult.Builder expected = resultBuilder(session, createTimestampWithTimeZoneType(3))
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
                        "   orderkey decimal(19, 0),\n" +
                        "   custkey decimal(19, 0),\n" +
                        "   orderstatus varchar(1),\n" +
                        "   totalprice double,\n" +
                        "   orderdate date,\n" +
                        "   orderpriority varchar(15),\n" +
                        "   clerk varchar(15),\n" +
                        "   shippriority decimal(10, 0),\n" +
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
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE bigint_column = 100")).isFullyPushedDown();
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE bigint_column > 100")).isFullyPushedDown();
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE short_decimal = 100")).isFullyPushedDown();
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE short_decimal > 100")).isFullyPushedDown();
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE long_decimal > 100000000")).isFullyPushedDown();
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE long_decimal = 100000000")).isFullyPushedDown();
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE varchar_column > 'ala'")).isFullyPushedDown();
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE varchar_column = 'ala'")).isFullyPushedDown();

            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE bigint_column > 100 and varchar_column > 'ala'")).isFullyPushedDown();
        }
    }

    @Test
    public void testTimestampWithTimeZoneAggregationPushdown()
    {
        try (TestTable testTable = new TestTable(onRemoteDatabase(), getSession().getSchema().orElseThrow() + ".test_aggregation_pushdown_timestamp_tz",
                "(timestamp_tz_column timestamp with time zone)")) {
            snowflakeExecutor.execute("INSERT INTO " + testTable.getName() + " VALUES (TIMESTAMP '1901-02-03 04:05:06.789')");
            snowflakeExecutor.execute("INSERT INTO " + testTable.getName() + " VALUES (TIMESTAMP '1911-02-03 04:05:06.789')");

            // Adding specific testcase for TIMESTAMP WITH TIME ZONE as it requires special rewrite handling when sending query to SF.
            assertThat(query("SELECT min(timestamp_tz_column) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT max(timestamp_tz_column) FROM " + testTable.getName())).isFullyPushedDown();
        }
    }

    @Override
    public void testAggregationWithUnsupportedResultType()
    {
        // Overridden because for approx_set(bigint) a ProjectNode is present above table scan because Snowflake requires a coercion from bigint to number
        // array_agg returns array, which is not supported
        assertThat(query("SELECT array_agg(nationkey) FROM nation"))
                .skipResultsCorrectnessCheckForPushdown() // array_agg doesn't have a deterministic order of elements in result array
                .isNotFullyPushedDown(AggregationNode.class);
        // histogram returns map, which is not supported
        assertThat(query("SELECT histogram(regionkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class);
        // multimap_agg returns multimap, which is not supported
        assertThat(query("SELECT multimap_agg(regionkey, nationkey) FROM nation"))
                .skipResultsCorrectnessCheckForPushdown() // multimap_agg doesn't have a deterministic order of values for a key
                .isNotFullyPushedDown(AggregationNode.class);
        // approx_set returns HyperLogLog, which is not supported
        assertThat(query("SELECT approx_set(nationkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class, ProjectNode.class);
    }

    @Test
    public void testSnowflakeTimestampWithPrecision()
    {
        try (TestTable testTable = new TestTable(snowflakeExecutor::execute, getSession().getSchema().orElseThrow() + ".test_timestamp_with_precision",
                "(" +
                        "timestamp0 timestamp(0)," +
                        "timestamp1 timestamp(1)," +
                        "timestamp2 timestamp(2)," +
                        "timestamp3 timestamp(3)," +
                        "timestamp4 timestamp(4)," +
                        "timestamp5 timestamp(5)," +
                        "timestamp6 timestamp(6)," +
                        "timestamp7 timestamp(7)," +
                        "timestamp8 timestamp(8)," +
                        "timestamp9 timestamp(9))",
                ImmutableList.of("" +
                        "TIMESTAMP '1901-02-03 04:05:06'," +
                        "TIMESTAMP '1901-02-03 04:05:06.1'," +
                        "TIMESTAMP '1901-02-03 04:05:06.12'," +
                        "TIMESTAMP '1901-02-03 04:05:06.123'," +
                        "TIMESTAMP '1901-02-03 04:05:06.1234'," +
                        "TIMESTAMP '1901-02-03 04:05:06.12345'," +
                        "TIMESTAMP '1901-02-03 04:05:06.123456'," +
                        "TIMESTAMP '1901-02-03 04:05:06.1234567'," +
                        "TIMESTAMP '1901-02-03 04:05:06.12345678'," +
                        "TIMESTAMP '1901-02-03 04:05:06.123456789'"))) {
            assertThat((String) computeActual("SHOW CREATE TABLE " + testTable.getName()).getOnlyValue())
                    .matches("CREATE TABLE \\w+\\.\\w+\\.\\w+ \\Q(\n" +
                            "   timestamp0 timestamp(0),\n" +
                            "   timestamp1 timestamp(1),\n" +
                            "   timestamp2 timestamp(2),\n" +
                            "   timestamp3 timestamp(3),\n" +
                            "   timestamp4 timestamp(4),\n" +
                            "   timestamp5 timestamp(5),\n" +
                            "   timestamp6 timestamp(6),\n" +
                            "   timestamp7 timestamp(7),\n" +
                            "   timestamp8 timestamp(8),\n" +
                            "   timestamp9 timestamp(9)\n" +
                            ")");

            assertThat(query("SELECT * FROM " + testTable.getName()))
                    .matches("VALUES (" +
                            "TIMESTAMP '1901-02-03 04:05:06'," +
                            "TIMESTAMP '1901-02-03 04:05:06.1'," +
                            "TIMESTAMP '1901-02-03 04:05:06.12'," +
                            "TIMESTAMP '1901-02-03 04:05:06.123'," +
                            "TIMESTAMP '1901-02-03 04:05:06.1234'," +
                            "TIMESTAMP '1901-02-03 04:05:06.12345'," +
                            "TIMESTAMP '1901-02-03 04:05:06.123456'," +
                            "TIMESTAMP '1901-02-03 04:05:06.1234567'," +
                            "TIMESTAMP '1901-02-03 04:05:06.12345678'," +
                            "TIMESTAMP '1901-02-03 04:05:06.123456789')");
        }
    }

    @Test
    public void testSnowflakeTimestampWithTimeZoneWithPrecision()
    {
        try (TestTable testTable = new TestTable(snowflakeExecutor::execute, getSession().getSchema().orElseThrow() + ".test_timestamptz_with_precision",
                "(" +
                        "timestamptz0 timestamp_tz(0)," +
                        "timestamptz1 timestamp_tz(1)," +
                        "timestamptz2 timestamp_tz(2)," +
                        "timestamptz3 timestamp_tz(3)," +
                        "timestamptz4 timestamp_tz(4)," +
                        "timestamptz5 timestamp_tz(5)," +
                        "timestamptz6 timestamp_tz(6)," +
                        "timestamptz7 timestamp_tz(7)," +
                        "timestamptz8 timestamp_tz(8)," +
                        "timestamptz9 timestamp_tz(9))",
                ImmutableList.of("" +
                        "'1901-02-03 04:05:06 +02:00'," +
                        "'1901-02-03 04:05:06.1 +02:00'," +
                        "'1901-02-03 04:05:06.12 +02:00'," +
                        "'1901-02-03 04:05:06.123 +02:00'," +
                        "'1901-02-03 04:05:06.1234 +02:00'," +
                        "'1901-02-03 04:05:06.12345 +02:00'," +
                        "'1901-02-03 04:05:06.123456 +02:00'," +
                        "'1901-02-03 04:05:06.1234567 +02:00'," +
                        "'1901-02-03 04:05:06.12345678 +02:00'," +
                        "'1901-02-03 04:05:06.123456789 +02:00'"))) {
            assertThat((String) computeActual("SHOW CREATE TABLE " + testTable.getName()).getOnlyValue())
                    .matches("CREATE TABLE \\w+\\.\\w+\\.\\w+ \\Q(\n" +
                            "   timestamptz0 timestamp(0) with time zone,\n" +
                            "   timestamptz1 timestamp(1) with time zone,\n" +
                            "   timestamptz2 timestamp(2) with time zone,\n" +
                            "   timestamptz3 timestamp(3) with time zone,\n" +
                            "   timestamptz4 timestamp(4) with time zone,\n" +
                            "   timestamptz5 timestamp(5) with time zone,\n" +
                            "   timestamptz6 timestamp(6) with time zone,\n" +
                            "   timestamptz7 timestamp(7) with time zone,\n" +
                            "   timestamptz8 timestamp(8) with time zone,\n" +
                            "   timestamptz9 timestamp(9) with time zone\n" +
                            ")");

            assertThat(query("SELECT * FROM " + testTable.getName()))
                    .matches("VALUES (" +
                            "TIMESTAMP '1901-02-03 04:05:06 +02:00'," +
                            "TIMESTAMP '1901-02-03 04:05:06.1 +02:00'," +
                            "TIMESTAMP '1901-02-03 04:05:06.12 +02:00'," +
                            "TIMESTAMP '1901-02-03 04:05:06.123 +02:00'," +
                            "TIMESTAMP '1901-02-03 04:05:06.1234 +02:00'," +
                            "TIMESTAMP '1901-02-03 04:05:06.12345 +02:00'," +
                            "TIMESTAMP '1901-02-03 04:05:06.123456 +02:00'," +
                            "TIMESTAMP '1901-02-03 04:05:06.1234567 +02:00'," +
                            "TIMESTAMP '1901-02-03 04:05:06.12345678 +02:00'," +
                            "TIMESTAMP '1901-02-03 04:05:06.123456789 +02:00')");
        }
    }

    @Test
    public void testSnowflakeTimeWithPrecision()
    {
        try (TestTable testTable = new TestTable(snowflakeExecutor::execute, getSession().getSchema().orElseThrow() + ".test_time_with_precision",
                "(" +
                        "time0 time(0)," +
                        "time1 time(1)," +
                        "time2 time(2)," +
                        "time3 time(3)," +
                        "time4 time(4)," +
                        "time5 time(5)," +
                        "time6 time(6)," +
                        "time7 time(7)," +
                        "time8 time(8)," +
                        "time9 time(9))",
                ImmutableList.of("" +
                        "TIME '04:05:06'," +
                        "TIME '04:05:06.1'," +
                        "TIME '04:05:06.12'," +
                        "TIME '04:05:06.123'," +
                        "TIME '04:05:06.1234'," +
                        "TIME '04:05:06.12345'," +
                        "TIME '04:05:06.123456'," +
                        "TIME '04:05:06.1234567'," +
                        "TIME '04:05:06.12345678'," +
                        "TIME '04:05:06.123456789'"))) {
            assertThat((String) computeActual("SHOW CREATE TABLE " + testTable.getName()).getOnlyValue())
                    .matches("CREATE TABLE \\w+\\.\\w+\\.\\w+ \\Q(\n" +
                            "   time0 time(3),\n" +
                            "   time1 time(3),\n" +
                            "   time2 time(3),\n" +
                            "   time3 time(3),\n" +
                            "   time4 time(3),\n" +
                            "   time5 time(3),\n" +
                            "   time6 time(3),\n" +
                            "   time7 time(3),\n" +
                            "   time8 time(3),\n" +
                            "   time9 time(3)\n" +
                            ")");

            assertThat(query("SELECT * FROM " + testTable.getName()))
                    .matches("VALUES (" +
                            "TIME '04:05:06.000'," +
                            "TIME '04:05:06.100'," +
                            "TIME '04:05:06.120'," +
                            "TIME '04:05:06.123'," +
                            "TIME '04:05:06.123'," +
                            "TIME '04:05:06.123'," +
                            "TIME '04:05:06.123'," +
                            "TIME '04:05:06.123'," +
                            "TIME '04:05:06.123'," +
                            "TIME '04:05:06.123')");
        }
    }

    @Test
    public void testSnowflakeTimestampRounding()
    {
        try (TestTable testTable = new TestTable(snowflakeExecutor::execute, getSession().getSchema().orElseThrow() + ".test_timestamp_rounding",
                "(t timestamp(9))",
                ImmutableList.of(
                        "TIMESTAMP '1901-02-03 04:05:06.123499999'",
                        "TIMESTAMP '1901-02-03 04:05:06.123900000'",
                        "TIMESTAMP '1969-12-31 23:59:59.999999999'",
                        "TIMESTAMP '2001-02-03 04:05:06.123499999'",
                        "TIMESTAMP '2001-02-03 04:05:06.123900000'"))) {
            assertThat(query("SELECT * FROM " + testTable.getName()))
                    .matches("VALUES " +
                            "TIMESTAMP '1901-02-03 04:05:06.123499999'," +
                            "TIMESTAMP '1901-02-03 04:05:06.123900000'," +
                            "TIMESTAMP '1969-12-31 23:59:59.999999999'," +
                            "TIMESTAMP '2001-02-03 04:05:06.123499999'," +
                            "TIMESTAMP '2001-02-03 04:05:06.123900000'");
        }
    }

    @Test
    public void testSnowflakeTimestampWithTimeZoneRounding()
    {
        try (TestTable testTable = new TestTable(snowflakeExecutor::execute, getSession().getSchema().orElseThrow() + ".test_timestamptz_rounding",
                "(t timestamp_tz(9))",
                ImmutableList.of(
                        "'1901-02-03 04:05:06.123499999 +02:00'",
                        "'1901-02-03 04:05:06.123900000 +02:00'",
                        "'2001-02-03 04:05:06.123499999 +02:00'",
                        "'2001-02-03 04:05:06.123900000 +02:00'"))) {
            assertThat(query("SELECT * FROM " + testTable.getName()))
                    .matches("VALUES " +
                            "TIMESTAMP '1901-02-03 04:05:06.123499999 +02:00'," +
                            "TIMESTAMP '1901-02-03 04:05:06.123900000 +02:00'," +
                            "TIMESTAMP '2001-02-03 04:05:06.123499999 +02:00'," +
                            "TIMESTAMP '2001-02-03 04:05:06.123900000 +02:00'");
        }
    }

    @Test
    public void testSnowflakeTimeRounding()
    {
        try (TestTable testTable = new TestTable(snowflakeExecutor::execute, getSession().getSchema().orElseThrow() + ".test_time_rounding",
                "(t time(9))",
                ImmutableList.of(
                        "TIME '04:05:06.123499999'",
                        "TIME '04:05:06.123900000'",
                        "TIME '23:59:59.999999999'"))) {
            assertThat(query("SELECT * FROM " + testTable.getName()))
                    .matches("VALUES " +
                            "TIME '04:05:06.123'," +
                            "TIME '04:05:06.123'," + // Snowflake truncates on cast
                            "TIME '23:59:59.999'");
        }
    }

    @Test
    @Override
    public void testReadMetadataWithRelationsConcurrentModifications()
    {
        // TODO Fix concurrent metadata modification test https://starburstdata.atlassian.net/browse/SEP-8789
        throw new SkipException("Test fails (SEP-8789)");
    }

    @Override
    public void testDeleteWithLike()
    {
        assertThatThrownBy(super::testDeleteWithLike)
                .hasStackTraceContaining("TrinoException: Unsupported delete");
    }

    @Override
    protected String errorMessageForInsertIntoNotNullColumn(String columnName)
    {
        return "NULL result in a non-nullable column";
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return snowflakeExecutor;
    }
}
