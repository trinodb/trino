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
import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.plugin.oracle.BaseOracleIntegrationSmokeTest;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.sql.SqlExecutor;
import io.prestosql.testing.sql.TestTable;
import org.testng.annotations.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Strings.repeat;
import static com.starburstdata.presto.plugin.oracle.OracleDataTypes.oracleTimestamp3TimeZoneDataType;
import static com.starburstdata.presto.plugin.oracle.OracleDataTypes.prestoTimestampWithTimeZoneDataType;
import static io.prestosql.plugin.jdbc.JdbcMetadataSessionProperties.AGGREGATION_PUSHDOWN_ENABLED;
import static io.prestosql.testing.datatype.DataType.timestampDataType;
import static io.prestosql.tpch.TpchTable.CUSTOMER;
import static io.prestosql.tpch.TpchTable.NATION;
import static io.prestosql.tpch.TpchTable.ORDERS;
import static io.prestosql.tpch.TpchTable.REGION;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class BaseStarburstOracleIntegrationSmokeTest
        extends BaseOracleIntegrationSmokeTest
{
    @Override
    protected String getUser()
    {
        return OracleTestUsers.USER;
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createQueryRunner(ImmutableMap.of());
    }

    protected QueryRunner createQueryRunner(Map<String, String> additionalProperties)
            throws Exception
    {
        return OracleQueryRunner.builder()
                .withConnectorProperties(ImmutableMap.<String, String>builder()
                        .putAll(TestingStarburstOracleServer.connectionProperties())
                        .put("allow-drop-table", "true")
                        .putAll(additionalProperties)
                        .build())
                .withTables(ImmutableList.of(CUSTOMER, NATION, ORDERS, REGION))
                .build();
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
            assertThat(query(format("SELECT c_long_char FROM %s WHERE c_long_char = '" + repeat("💩", 2000) + "'", table.getName()))).isCorrectlyPushedDown();
            assertThat(query(format("SELECT c_long_varchar FROM %s WHERE c_long_varchar = '" + repeat("💩", 4000) + "'", table.getName()))).isCorrectlyPushedDown();
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
                onOracle(),
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

    /**
     * Test that aggregation pushdown is disabled.
     * <p>
     * {@link BaseStarburstOracleAggregationPushdownTest} covers the case when it is enabled.
     */
    @Test
    public void testAggregationPushdownDisabled()
    {
        assertThat(query("SELECT DISTINCT nationkey FROM nation")).isNotFullyPushedDown(ProjectNode.class);

        assertThat(query("SELECT count(*) FROM nation")).isNotFullyPushedDown(AggregationNode.class);
        assertThat(query("SELECT count(nationkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class);

        assertThat(query("SELECT regionkey, min(nationkey) FROM nation GROUP BY regionkey")).isNotFullyPushedDown(ProjectNode.class);
        assertThat(query("SELECT regionkey, max(nationkey) FROM nation GROUP BY regionkey")).isNotFullyPushedDown(ProjectNode.class);
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey")).isNotFullyPushedDown(ProjectNode.class);
        assertThat(query("SELECT regionkey, avg(nationkey) FROM nation GROUP BY regionkey")).isNotFullyPushedDown(ProjectNode.class);
    }

    /**
     * Test that aggregation pushdown requires a license.
     * <p>
     * {@link BaseStarburstOracleAggregationPushdownTest} covers the case when it is enabled.
     */
    @Test
    public void testAggregationPushdownRequiresLicense()
    {
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty("oracle", AGGREGATION_PUSHDOWN_ENABLED, "true")
                .build();

        // Non-aggregation query still works
        assertThat(query(session, "SELECT name FROM nation WHERE nationkey = 3"))
                .matches("VALUES CAST('CANADA' AS varchar(25))")
                .isCorrectlyPushedDown();

        // Simple aggregation queries still work
        assertThat(query(session, "SELECT DISTINCT regionkey FROM nation")).isCorrectlyPushedDown();
        assertThat(query(session, "SELECT regionkey FROM nation GROUP BY regionkey")).isCorrectlyPushedDown();

        // "normal" aggregation query (one using an aggregation function) requires a license
        assertThatThrownBy(() -> assertThat(query(session, "SELECT count(*) FROM nation")))
                .hasMessage("Valid license required to use the feature: oracle-extensions");
        assertThatThrownBy(() -> assertThat(query(session, "SELECT count(nationkey) FROM nation")))
                .hasMessage("Valid license required to use the feature: oracle-extensions");
        assertThatThrownBy(() -> assertThat(query(session, "SELECT regionkey, min(nationkey) FROM nation GROUP BY regionkey")))
                .hasMessage("Valid license required to use the feature: oracle-extensions");
        assertThatThrownBy(() -> assertThat(query(session, "SELECT regionkey, avg(nationkey) FROM nation GROUP BY regionkey")))
                .hasMessage("Valid license required to use the feature: oracle-extensions");
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

    @Override
    protected SqlExecutor onOracle()
    {
        return TestingStarburstOracleServer::executeInOracle;
    }
}
