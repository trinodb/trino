/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.oracle;

import io.trino.Session;
import io.trino.plugin.oracle.BaseOracleConnectorSmokeTest;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.testing.TestingConnectorBehavior;
import org.testng.annotations.Test;

import static io.trino.plugin.jdbc.JdbcMetadataSessionProperties.AGGREGATION_PUSHDOWN_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class BaseUnlicensedStarburstOracleConnectorSmokeTest
        extends BaseOracleConnectorSmokeTest
{
    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_AGGREGATION_PUSHDOWN:
                return false;
            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    /**
     * Test that aggregation pushdown is disabled by default without a license.
     * <p>
     * {@link TestStarburstOracleConnectorTest} covers the case when a license is available.
     */
    @Test
    public void testAggregationPushdownDisabled()
    {
        assertThat(query("SELECT DISTINCT nationkey FROM nation")).isNotFullyPushedDown(AggregationNode.class, ProjectNode.class);

        assertThat(query("SELECT count(*) FROM nation")).isNotFullyPushedDown(AggregationNode.class);
        assertThat(query("SELECT count(nationkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class);

        assertThat(query("SELECT regionkey, min(nationkey) FROM nation GROUP BY regionkey")).isNotFullyPushedDown(AggregationNode.class, ProjectNode.class);
        assertThat(query("SELECT regionkey, max(nationkey) FROM nation GROUP BY regionkey")).isNotFullyPushedDown(AggregationNode.class, ProjectNode.class);
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey")).isNotFullyPushedDown(AggregationNode.class, ProjectNode.class);
        assertThat(query("SELECT regionkey, avg(nationkey) FROM nation GROUP BY regionkey")).isNotFullyPushedDown(AggregationNode.class, ProjectNode.class);
    }

    /**
     * Test that if aggregation pushdown is explicitly enabled without a license, an exception is raised during aggregate pushdown.
     * <p>
     * {@link TestStarburstOracleConnectorTest} covers the case when a license is available.
     */
    @Test
    public void testAggregationPushdownWithoutLicense()
    {
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty("oracle", AGGREGATION_PUSHDOWN_ENABLED, "true")
                .build();

        // Non-aggregation query still works
        assertThat(query(session, "SELECT name FROM nation WHERE nationkey = 3"))
                .matches("VALUES CAST('CANADA' AS varchar(25))")
                .isFullyPushedDown();

        // Simple aggregation queries still work
        assertThat(query(session, "SELECT DISTINCT regionkey FROM nation")).isFullyPushedDown();
        assertThat(query(session, "SELECT regionkey FROM nation GROUP BY regionkey")).isFullyPushedDown();

        // "normal" aggregation query (one using an aggregation function) requires a license
        assertThatThrownBy(() -> assertThat(query(session, "SELECT count(*) FROM nation")))
                .hasMessage("Starburst Enterprise requires valid license");
        assertThatThrownBy(() -> assertThat(query(session, "SELECT count(nationkey) FROM nation")))
                .hasMessage("Starburst Enterprise requires valid license");
        assertThatThrownBy(() -> assertThat(query(session, "SELECT regionkey, min(nationkey) FROM nation GROUP BY regionkey")))
                .hasMessage("Starburst Enterprise requires valid license");
        assertThatThrownBy(() -> assertThat(query(session, "SELECT regionkey, avg(nationkey) FROM nation GROUP BY regionkey")))
                .hasMessage("Starburst Enterprise requires valid license");
    }
}
