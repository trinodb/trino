/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.stargate;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.testng.SkipException;
import org.testng.annotations.Test;

import static io.trino.SystemSessionProperties.USE_MARK_DISTINCT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class BaseStargateConnectorTest
        extends BaseJdbcConnectorTest
{
    protected DistributedQueryRunner remoteStarburst;

    protected abstract String getRemoteCatalogName();

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        Session remoteSession = Session.builder(remoteStarburst.getDefaultSession())
                .setCatalog(getRemoteCatalogName())
                .build();
        return sql -> remoteStarburst.execute(remoteSession, sql);
    }

    @Override
    @SuppressWarnings("DuplicateBranchesInSwitch")
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_AGGREGATION_PUSHDOWN_STDDEV:
            case SUPPORTS_AGGREGATION_PUSHDOWN_VARIANCE:
            case SUPPORTS_AGGREGATION_PUSHDOWN_COVARIANCE:
            case SUPPORTS_AGGREGATION_PUSHDOWN_CORRELATION:
            case SUPPORTS_AGGREGATION_PUSHDOWN_REGRESSION:
            case SUPPORTS_AGGREGATION_PUSHDOWN_COUNT_DISTINCT:
                return true;

            case SUPPORTS_JOIN_PUSHDOWN:
                return true;

            case SUPPORTS_COMMENT_ON_TABLE:
                // not yet supported in Stargate connector
                return false;

            case SUPPORTS_ROW_TYPE:
            case SUPPORTS_ARRAY:
                // TODO Add support in Stargate connector (https://starburstdata.atlassian.net/browse/SEP-4798)
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Test
    public void testAggregationPushdownWithTrinoSpecificFunctions()
    {
        assertThat(query("SELECT approx_distinct(regionkey) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT approx_distinct(name) FROM nation GROUP BY regionkey")).isFullyPushedDown();

        // TODO fix https://github.com/trinodb/trino/issues/9592
        //  assertThat(query("SELECT max_by(name, nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();

        // checksum returns varbinary
        assertThat(query("SELECT checksum(nationkey) FROM nation")).isFullyPushedDown();

        // geometric_mean returns double
        assertThat(query("SELECT geometric_mean(nationkey) FROM nation")).isFullyPushedDown();
    }

    @Override
    public void testDistinctAggregationPushdown()
    {
        // Overridden because Stargate connector supports pushdown of all aggregation functions including multiple DISTINCTs
        // SELECT DISTINCT
        assertThat(query("SELECT DISTINCT regionkey FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT min(DISTINCT regionkey) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT DISTINCT regionkey, min(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        try (TestTable emptyTable = createAggregationTestTable(getSession().getSchema().orElseThrow() + ".empty_table", ImmutableList.of())) {
            assertThat(query("SELECT DISTINCT a_bigint FROM " + emptyTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT min(DISTINCT a_bigint) FROM " + emptyTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT DISTINCT t_double, min(a_bigint) FROM " + emptyTable.getName() + " GROUP BY t_double")).isFullyPushedDown();
        }

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
        assertThat(query(withMarkDistinct, "SELECT count(DISTINCT regionkey), count(DISTINCT nationkey) FROM nation")).isFullyPushedDown();
        // distinct aggregation and a non-distinct aggregation
        assertThat(query(withMarkDistinct, "SELECT count(DISTINCT regionkey), sum(nationkey) FROM nation")).isFullyPushedDown();

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
        assertThat(query(withoutMarkDistinct, "SELECT count(DISTINCT regionkey), count(DISTINCT nationkey) FROM nation")).isFullyPushedDown();
        // distinct aggregation and a non-distinct aggregation
        assertThat(query(withoutMarkDistinct, "SELECT count(DISTINCT regionkey), sum(nationkey) FROM nation")).isFullyPushedDown();
    }

    @Test
    public void testCreateView()
    {
        // The test ensures that we do not inherit CREATE VIEW capabilities from trino-base-jdbc.
        // While generic VIEW support in base JDBC is feasible, Stargate connector would require special
        // considerations, see https://starburstdata.atlassian.net/browse/SEP-4795.
        assertThatThrownBy(() -> query("CREATE VIEW v AS SELECT 1 a"))
                .hasMessage("This connector does not support creating views");
    }

    @Override
    public void testDropNonEmptySchemaWithTable()
    {
        throw new SkipException("Stargate connector not support creating tables");
    }

    @Override
    protected Session joinPushdownEnabled(Session session)
    {
        return Session.builder(super.joinPushdownEnabled(session))
                // strategy is AUTOMATIC by default and would not work for certain test cases (even if statistics are collected)
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "join_pushdown_strategy", "EAGER")
                .build();
    }
}
