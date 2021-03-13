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
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.oracle.BaseOracleConnectorTest;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.JdbcSqlExecutor;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.sql.CallableStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Properties;

import static com.google.common.base.Strings.repeat;
import static com.starburstdata.presto.plugin.oracle.OracleDataTypes.oracleTimestamp3TimeZoneDataType;
import static com.starburstdata.presto.plugin.oracle.OracleDataTypes.prestoTimestampWithTimeZoneDataType;
import static com.starburstdata.presto.plugin.oracle.TestingStarburstOracleServer.executeInOracle;
import static io.trino.SystemSessionProperties.USE_MARK_DISTINCT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.testing.datatype.DataType.timestampDataType;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestStarburstOracleConnectorTest
        extends BaseOracleConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return OracleQueryRunner.builder()
                .withUnlockEnterpriseFeatures(true)
                .withConnectorProperties(ImmutableMap.<String, String>builder()
                        .putAll(TestingStarburstOracleServer.connectionProperties())
                        .put("oracle.connection-pool.enabled", "true")
                        .put("oracle.connection-pool.max-size", "10")
                        .build())
                .withTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_AGGREGATION_PUSHDOWN:
            case SUPPORTS_LIMIT_PUSHDOWN:
            case SUPPORTS_TOPN_PUSHDOWN:
                return true;
            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    public void testColumnName(String columnName)
    {
        if (columnName.equals("a\"quote")) {
            // Quote is not supported within column name
            assertThatThrownBy(() -> super.testColumnName(columnName))
                    .hasMessageMatching("Oracle does not support escaping '\"' in identifiers");
            throw new SkipException("works incorrectly, column name is trimmed");
        }

        super.testColumnName(columnName);
    }

    @Override
    public void testPredicatePushdown()
    {
        // varchar equality
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'ROMANIA'"))
                .matches("VALUES (CAST(3 AS DECIMAL(19,0)), CAST(19 AS DECIMAL(19,0)), CAST('ROMANIA' AS varchar(25)))")
                .isFullyPushedDown();

        // varchar range
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name BETWEEN 'POLAND' AND 'RPA'"))
                .matches("VALUES (CAST(3 AS DECIMAL(19,0)), CAST(19 AS DECIMAL(19,0)), CAST('ROMANIA' AS varchar(25)))")
                .isFullyPushedDown();

        // varchar different case
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'romania'"))
                .returnsEmptyResult()
                .isFullyPushedDown();

        // date equality
        assertThat(query("SELECT orderkey FROM orders WHERE orderdate = DATE '1992-09-29'"))
                .matches("VALUES CAST(1250 AS DECIMAL(19,0)), 34406, 38436, 57570")
                .isFullyPushedDown();

        // predicate over aggregation key (likely to be optimized before being pushed down into the connector)
        assertThat(query("SELECT * FROM (SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey) WHERE regionkey = 3"))
                .matches("VALUES (CAST(3 AS decimal(19,0)), CAST(77 AS decimal(38,0)))")
                .isFullyPushedDown();

        // predicate over aggregation result
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey HAVING sum(nationkey) = 77"))
                .matches("VALUES (CAST(3 AS decimal(19,0)), CAST(77 AS decimal(38,0)))")
                .isFullyPushedDown();
    }

    @Override
    protected String getUser()
    {
        return OracleTestUsers.USER;
    }

    @Override
    protected SqlExecutor onOracle()
    {
        Properties properties = new Properties();
        properties.setProperty("user", OracleTestUsers.USER);
        properties.setProperty("password", OracleTestUsers.PASSWORD);
        return new JdbcSqlExecutor(TestingStarburstOracleServer.getJdbcUrl(), properties);
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
    public void testGetColumns()
    {
        // OracleClient.getColumns is using wildcard at the end of table name.
        // Here we test that columns do not leak between tables.
        // See OracleClient#getColumns for more details.
        try (TestTable ignored = new TestTable(onOracle(), "ordersx", "AS SELECT 'a' some_additional_column FROM dual")) {
            assertQuery(
                    format("SELECT column_name FROM information_schema.columns WHERE table_name = 'orders' AND table_schema = '%s'", getUser()),
                    "VALUES 'orderkey', 'custkey', 'orderstatus', 'totalprice', 'orderdate', 'orderpriority', 'clerk', 'shippriority', 'comment'");
        }
    }

    @Test
    public void testAdditionalPredicatePushdownForChars()
    {
        try (TestTable table = new TestTable(
                onOracle(),
                getUser() + ".test_predicate_pushdown_char",
                "(c_long_char CHAR(2000), c_long_varchar VARCHAR2(4000))",
                ImmutableList.of("'my_long_char', 'my_long_varchar'"))) {
            // Verify using a large value in WHERE, larger than the 2000 and 4000 bytes Oracle max
            // this does not work in Oracle 11
            assertThat(query(format("SELECT c_long_char FROM %s WHERE c_long_char = '" + repeat("💩", 2000) + "'", table.getName()))).isFullyPushedDown();
            assertThat(query(format("SELECT c_long_varchar FROM %s WHERE c_long_varchar = '" + repeat("💩", 4000) + "'", table.getName()))).isFullyPushedDown();
        }
    }

    /**
     * This test covers only predicate pushdown for Oracle (it doesn't test timestamp semantics).
     *
     * @see com.starburstdata.presto.plugin.oracle.TestOracleTypeMapping
     * @see io.trino.testing.AbstractTestDistributedQueries
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
                onOracle(),
                getUser() + ".test_predicate_pushdown_timestamp",
                "(t_timestamp TIMESTAMP, t_timestamp3_with_tz TIMESTAMP(3) WITH TIME ZONE, t_timestamp_with_tz TIMESTAMP WITH TIME ZONE, dummy_col VARCHAR(12))",
                ImmutableList.of(Joiner.on(", ").join(values)))) {
            assertThat(query(format(
                    "SELECT dummy_col FROM %s WHERE t_timestamp = %s",
                    table.getName(),
                    format("timestamp '%s'", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").format(date1950)))))
                    .isFullyPushedDown();

            assertThat(query(format(
                    "SELECT dummy_col FROM %s WHERE t_timestamp3_with_tz = %s",
                    table.getName(),
                    prestoTimestampWithTimeZoneDataType().toLiteral(yakutat1978))))
                    .isFullyPushedDown();

            assertThat(query(format(
                    "SELECT dummy_col FROM %s WHERE t_timestamp_with_tz = %s",
                    table.getName(),
                    prestoTimestampWithTimeZoneDataType().toLiteral(pacific1976))))
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

        // with aggregation; requires aggregation pushdown
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

        // SELECT DISTINCT
        assertThat(query("SELECT DISTINCT regionkey FROM nation")).isFullyPushedDown();

        // count()
        assertThat(query("SELECT count(*) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT count(nationkey) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT count(1) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT count() FROM nation")).isFullyPushedDown();

        // GROUP BY
        assertThat(query("SELECT regionkey, min(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        assertThat(query("SELECT regionkey, max(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        assertThat(query("SELECT regionkey, avg(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation WHERE regionkey < 4 AND name > 'AAA' GROUP BY regionkey")).isFullyPushedDown();

        Session withMarkDistinct = Session.builder(getSession())
                .setSystemProperty(USE_MARK_DISTINCT, "true")
                .build();

        // distinct aggregation
        assertThat(query(withMarkDistinct, "SELECT count(DISTINCT regionkey) FROM nation")).isFullyPushedDown();
        // distinct aggregation with GROUP BY
        assertThat(query(withMarkDistinct, "SELECT count(DISTINCT nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        // distinct aggregation with varchar
        assertThat(query(withMarkDistinct, "SELECT count(DISTINCT comment) FROM nation")).isFullyPushedDown();
        // two distinct aggregations
        assertThat(query(withMarkDistinct, "SELECT count(DISTINCT regionkey), count(DISTINCT nationkey) FROM nation"))
                .isNotFullyPushedDown(MarkDistinctNode.class, ExchangeNode.class, ExchangeNode.class, ProjectNode.class);
        // distinct aggregation and a non-distinct aggregation
        assertThat(query(withMarkDistinct, "SELECT count(DISTINCT regionkey), sum(nationkey) FROM nation"))
                .isNotFullyPushedDown(MarkDistinctNode.class, ExchangeNode.class, ExchangeNode.class, ProjectNode.class);

        Session withoutMarkDistinct = Session.builder(getSession())
                .setSystemProperty(USE_MARK_DISTINCT, "false")
                .build();

        // distinct aggregation
        assertThat(query(withoutMarkDistinct, "SELECT count(DISTINCT regionkey) FROM nation")).isFullyPushedDown();
        // distinct aggregation with GROUP BY
        assertThat(query(withoutMarkDistinct, "SELECT count(DISTINCT nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        // distinct aggregation with varchar
        assertThat(query(withoutMarkDistinct, "SELECT count(DISTINCT comment) FROM nation")).isFullyPushedDown();
        // two distinct aggregations
        assertThat(query(withoutMarkDistinct, "SELECT count(DISTINCT regionkey), count(DISTINCT nationkey) FROM nation"))
                .isNotFullyPushedDown(AggregationNode.class, ExchangeNode.class, ExchangeNode.class);
        // distinct aggregation and a non-distinct aggregation
        assertThat(query(withoutMarkDistinct, "SELECT count(DISTINCT regionkey), sum(nationkey) FROM nation"))
                .isNotFullyPushedDown(AggregationNode.class, ExchangeNode.class, ExchangeNode.class);

        try (TestTable testTable = new TestTable(onOracle(), getSession().getSchema().orElseThrow() + ".test_aggregation_pushdown",
                "(short_decimal decimal(9, 3), long_decimal decimal(30, 10), varchar_column varchar(10))")) {
            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (100.000, 100000000.000000000, 'ala')");
            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (123.321, 123456789.987654321, 'kot')");

            assertThat(query("SELECT min(short_decimal), min(long_decimal) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT max(short_decimal), max(long_decimal) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT sum(short_decimal), sum(long_decimal) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT avg(short_decimal), avg(long_decimal) FROM " + testTable.getName())).isFullyPushedDown();

            // smoke testing of more complex cases
            // WHERE on aggregation column
            assertThat(query("SELECT min(short_decimal), min(long_decimal) FROM " + testTable.getName() + " WHERE short_decimal < 110 AND long_decimal < 124")).isFullyPushedDown();
            // WHERE on non-aggregation column
            assertThat(query("SELECT min(long_decimal) FROM " + testTable.getName() + " WHERE short_decimal < 110")).isFullyPushedDown();
            // GROUP BY
            assertThat(query("SELECT short_decimal, min(long_decimal) FROM " + testTable.getName() + " GROUP BY short_decimal")).isFullyPushedDown();
            // GROUP BY with WHERE on both grouping and aggregation column
            assertThat(query("SELECT short_decimal, min(long_decimal) FROM " + testTable.getName() + " WHERE short_decimal < 110 AND long_decimal < 124" + " GROUP BY short_decimal")).isFullyPushedDown();
            // GROUP BY with WHERE on grouping column
            assertThat(query("SELECT short_decimal, min(long_decimal) FROM " + testTable.getName() + " WHERE short_decimal < 110 GROUP BY short_decimal")).isFullyPushedDown();
            // GROUP BY with WHERE on aggregation column
            assertThat(query("SELECT short_decimal, min(long_decimal) FROM " + testTable.getName() + " WHERE long_decimal < 124 GROUP BY short_decimal")).isFullyPushedDown();
            // GROUP BY with WHERE on neither grouping nor aggregation column
            assertThat(query("SELECT short_decimal, min(long_decimal) FROM " + testTable.getName() + " WHERE varchar_column = 'ala' GROUP BY short_decimal")).isFullyPushedDown();
            // aggregation on varchar column
            assertThat(query("SELECT min(varchar_column) FROM " + testTable.getName())).isFullyPushedDown();
            // aggregation on varchar column with GROUPING
            assertThat(query("SELECT short_decimal, min(varchar_column) FROM " + testTable.getName() + " GROUP BY short_decimal")).isFullyPushedDown();
            // aggregation on varchar column with WHERE
            assertThat(query("SELECT min(varchar_column) FROM " + testTable.getName() + " WHERE varchar_column ='ala'")).isFullyPushedDown();

            assertThat(query("SELECT DISTINCT short_decimal, min(long_decimal) FROM " + testTable.getName() + " GROUP BY short_decimal")).isFullyPushedDown();
        }
    }

    @Test
    public void testStddevAggregationPushdown()
    {
        try (TestTable testTable = new TestTable(onOracle(), getSession().getSchema().orElseThrow() + ".test_stddev_pushdown",
                "(t_double DOUBLE PRECISION)")) {
            assertThat(query("SELECT stddev_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (1)");

            assertThat(query("SELECT stddev_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (3)");
            assertThat(query("SELECT stddev_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (5)");
            assertThat(query("SELECT stddev(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();
        }

        try (TestTable testTable = new TestTable(onOracle(), getSession().getSchema().orElseThrow() + ".test_stddev_pushdown",
                "(t_double DOUBLE PRECISION)")) {
            // Test non-whole number results
            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (1)");
            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (2)");
            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (4)");
            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (5)");

            assertThat(query("SELECT stddev_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();
        }
    }

    @Test
    public void testVarianceAggregationPushdown()
    {
        try (TestTable testTable = new TestTable(onOracle(), getSession().getSchema().orElseThrow() + ".test_variance_pushdown",
                "(t_double DOUBLE PRECISION)")) {
            assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (1)");

            assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (3)");
            assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (5)");
            assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();
        }

        try (TestTable testTable = new TestTable(onOracle(), getSession().getSchema().orElseThrow() + ".test_variance_pushdown",
                "(t_double DOUBLE PRECISION)")) {
            // Test non-whole number results
            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (1)");
            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (2)");
            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (3)");
            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (4)");
            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (5)");

            assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();
        }
    }

    @Test
    public void testCovarianceAggregationPushdown()
    {
        String schema = getSession().getSchema().orElseThrow();
        // empty table
        try (TestTable testTable = new TestTable(onOracle(), schema + ".test_covariance_pushdown", "(t_double1 DOUBLE PRECISION, t_double2 DOUBLE PRECISION, t_real1 REAL, t_real2 REAL)")) {
            assertThat(query("SELECT covar_pop(t_double1, t_double2), covar_pop(t_real1, t_real2) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT covar_samp(t_double1, t_double2), covar_samp(t_real1, t_real2) FROM " + testTable.getName())).isFullyPushedDown();
        }

        // test some values for which the aggregate functions return whole numbers
        try (TestTable testTable = new TestTable(
                onOracle(),
                schema + ".test_covariance_pushdown",
                "(t_double1 DOUBLE PRECISION, t_double2 DOUBLE PRECISION, t_real1 REAL, t_real2 REAL)",
                ImmutableList.of("2, 2, 2, 2", "4, 4, 4, 4"))) {
            assertThat(query("SELECT covar_pop(t_double1, t_double2), covar_pop(t_real1, t_real2) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT covar_samp(t_double1, t_double2), covar_samp(t_real1, t_real2) FROM " + testTable.getName())).isFullyPushedDown();
        }

        // non-whole number results
        try (TestTable testTable = new TestTable(
                onOracle(),
                schema + ".test_covariance_pushdown",
                "(t_double1 DOUBLE PRECISION, t_double2 DOUBLE PRECISION, t_real1 REAL, t_real2 REAL)",
                ImmutableList.of("1, 2, 1, 2", "100000000.123456, 4, 100000000.123456, 4", "123456789.987654, 8, 123456789.987654, 8"))) {
            assertThat(query("SELECT covar_pop(t_double1, t_double2), covar_pop(t_real1, t_real2) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT covar_samp(t_double1, t_double2), covar_samp(t_real1, t_real2) FROM " + testTable.getName())).isFullyPushedDown();
        }
    }

    @Test
    public void testJoinPushdownAutomatic()
    {
        PlanMatchPattern joinOverTableScans =
                node(JoinNode.class,
                        anyTree(node(TableScanNode.class)),
                        anyTree(node(TableScanNode.class)));

        Session session = joinPushdownAutomatic(getSession());

        try (TestTable left = joinTestTable("left", 1000, 1000, 100);
                TestTable right = joinTestTable("right", 10, 10, 100)) {
            // no stats on left and right
            assertThat(query(session, format("SELECT * FROM %s l JOIN %s r ON l.key = r.key", left.getName(), right.getName()))).isNotFullyPushedDown(joinOverTableScans);

            // stats only for left
            gatherStats(left.getName());
            assertThat(query(session, format("SELECT * FROM %s l JOIN %s r ON l.key = r.key", left.getName(), right.getName()))).isNotFullyPushedDown(joinOverTableScans);

            // both tables with stats
            gatherStats(right.getName());
            assertThat(query(session, format("SELECT * FROM %s l JOIN %s r ON l.key = r.key", left.getName(), right.getName()))).isFullyPushedDown();
        }

        try (TestTable left = joinTestTable("left", 1000, 1, 100);
                TestTable right = joinTestTable("right", 10, 1, 100)) {
            // single NDV in each table logically results in a cross join; should not be pushed down
            gatherStats(left.getName());
            gatherStats(right.getName());
            assertThat(query(session, format("SELECT * FROM %s l JOIN %s r ON l.key = r.key", left.getName(), right.getName()))).isNotFullyPushedDown(joinOverTableScans);
        }

        try (TestTable left = joinTestTable("left", 1000, 1000, 100);
                TestTable right = joinTestTable("right", 10, 10, 100)) {
            gatherStats(left.getName());
            gatherStats(right.getName());

            // sanity check
            assertThat(query(session, format("SELECT * FROM %s l JOIN %s r ON l.key = r.key", left.getName(), right.getName()))).isFullyPushedDown();

            // allow only very small tables in join pushdown
            Session onlySmallTablesAllowed = Session.builder(session)
                    .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "experimental_join_pushdown_automatic_max_table_size", "1kB")
                    .build();
            assertThat(query(onlySmallTablesAllowed, format("SELECT * FROM %s l JOIN %s r ON l.key = r.key", left.getName(), right.getName()))).isNotFullyPushedDown(joinOverTableScans);

            // require estimated join to be very small
            Session verySmallJoinRequired = Session.builder(session)
                    .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "experimental_join_pushdown_automatic_max_join_to_tables_ratio", "0.01")
                    .build();
            assertThat(query(verySmallJoinRequired, format("SELECT * FROM %s l JOIN %s r ON l.key = r.key", left.getName(), right.getName()))).isNotFullyPushedDown(joinOverTableScans);
        }
    }

    private TestTable joinTestTable(String name, long rowsCount, int keyDistinctValues, int paddingSize)
    {
        String padding = Strings.repeat("x", paddingSize);
        return new TestTable(
                getQueryRunner()::execute,
                name,
                format("(key, padding) AS SELECT mod(orderkey, %s), '%s' FROM tpch.sf100.orders LIMIT %s", keyDistinctValues, padding, rowsCount));
    }

    private static void gatherStats(String tableName)
    {
        executeInOracle(connection -> {
            try (CallableStatement statement = connection.prepareCall("{CALL DBMS_STATS.GATHER_TABLE_STATS(?, ?)}")) {
                statement.setString(1, OracleTestUsers.USER);
                statement.setString(2, tableName);
                statement.execute();
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    protected Session joinPushdownEnabled(Session session)
    {
        return Session.builder(super.joinPushdownEnabled(session))
                // strategy is AUTOMATIC by default and would not work without statistics
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "join_pushdown_strategy", "EAGER")
                .build();
    }

    protected Session joinPushdownAutomatic(Session session)
    {
        return Session.builder(super.joinPushdownEnabled(session))
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "join_pushdown_strategy", "AUTOMATIC")
                .build();
    }
}
