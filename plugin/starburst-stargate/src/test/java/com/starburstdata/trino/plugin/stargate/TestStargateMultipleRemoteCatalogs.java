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

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static com.starburstdata.trino.plugin.stargate.StargateQueryRunner.createRemoteStarburstQueryRunner;
import static com.starburstdata.trino.plugin.stargate.StargateQueryRunner.stargateConnectionUrl;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static org.assertj.core.api.Assertions.assertThat;

public class TestStargateMultipleRemoteCatalogs
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner remoteStarburst = closeAfterClass(createRemoteStarburstQueryRunner(Optional.empty()));
        remoteStarburst.installPlugin(new TestingMemoryPlugin());
        remoteStarburst.createCatalog("memory1", "testing_memory");
        remoteStarburst.createCatalog("memory2", "testing_memory");

        remoteStarburst.execute("CREATE SCHEMA memory1.tiny");
        remoteStarburst.execute("CREATE TABLE memory1.tiny.nation AS SELECT * FROM tpch.tiny.nation");

        remoteStarburst.execute("CREATE SCHEMA memory2.tiny");
        remoteStarburst.execute("CREATE TABLE memory2.tiny.region AS SELECT * FROM tpch.tiny.region");

        DistributedQueryRunner queryRunner = StargateQueryRunner.builder(remoteStarburst, "memory1")
                .withCatalog("p2p_memory1")
                .build();
        queryRunner.createCatalog("p2p_memory2", "stargate", ImmutableMap.of(
                "connection-url", stargateConnectionUrl(remoteStarburst, "memory2"),
                "connection-user", "p2p"));
        queryRunner.createCatalog("p2p_memory2_disabled", "stargate", ImmutableMap.of(
                "connection-url", stargateConnectionUrl(remoteStarburst, "memory2"),
                "connection-user", "different_user"));

        return queryRunner;
    }

    @Test
    void testJoinPushdown()
    {
        Session session = joinPushdownEnabled(getSession());

        assertThat(query(session, "SELECT r.name, n.name FROM p2p_memory1.tiny.nation n INNER JOIN p2p_memory2.tiny.region r ON n.regionkey = r.regionkey"))
                .isFullyPushedDown();
    }

    @Test
    void testNoJoinPushdownWithDifferentUser()
    {
        Session session = joinPushdownEnabled(getSession());

        assertThat(query(session, "SELECT r.name, n.name FROM p2p_memory1.tiny.nation n INNER JOIN p2p_memory2_disabled.tiny.region r ON n.regionkey = r.regionkey"))
                .isNotFullyPushedDown(node(JoinNode.class, anyTree(node(TableScanNode.class)), anyTree(node(TableScanNode.class))));
    }

    protected Session joinPushdownEnabled(Session session)
    {
        return Session.builder(session)
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "join_pushdown_enabled", "true")
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "join_pushdown_strategy", "EAGER")
                .setSystemProperty("join_pushdown_across_catalogs_enabled", "true")
                .build();
    }
}
