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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.oracle.BaseOracleConnectorTest;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.plan.JoinNode;
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
import java.util.Properties;

import static com.starburstdata.presto.plugin.oracle.TestingStarburstOracleServer.executeInOracle;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
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
                .withConnectorProperties(ImmutableMap.<String, String>builder()
                        .putAll(TestingStarburstOracleServer.connectionProperties())
                        .put("oracle.connection-pool.enabled", "true")
                        .put("oracle.connection-pool.max-size", "10")
                        .build())
                .build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_LIMIT_PUSHDOWN:
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
