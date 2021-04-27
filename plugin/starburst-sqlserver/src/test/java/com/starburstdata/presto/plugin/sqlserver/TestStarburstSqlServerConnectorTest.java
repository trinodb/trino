/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.sqlserver;

import com.google.common.collect.ImmutableMap;
import com.starburstdata.presto.testing.SessionMutator;
import io.trino.Session;
import io.trino.plugin.sqlserver.TestSqlServerConnectorTest;
import io.trino.plugin.sqlserver.TestingSqlServer;
import io.trino.testing.QueryRunner;

import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerQueryRunner.createStarburstSqlServerQueryRunner;
import static io.trino.plugin.jdbc.JdbcMetadataSessionProperties.JOIN_PUSHDOWN_ENABLED;

public class TestStarburstSqlServerConnectorTest
        extends TestSqlServerConnectorTest
{
    private final SessionMutator sessionMutator = new SessionMutator(super::getSession);

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        sqlServer = new TestingSqlServer();
        sqlServer.start();
        return createStarburstSqlServerQueryRunner(sqlServer, false, ImmutableMap.of(), REQUIRED_TPCH_TABLES);
    }

    @Override
    protected Session getSession()
    {
        return sessionMutator.getSession();
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
    public void testJoinPushdownDisabled()
    {
        // disabling join pushdown as SQL Server collects stats by default and AUTOMATIC join pushdown triggers
        // failing assertions defined in OSS, where join pushdown is disabled.
        sessionMutator.withModifiedSession(
                session -> Session.builder(session)
                        .setCatalogSessionProperty(session.getCatalog().get(), JOIN_PUSHDOWN_ENABLED, "false")
                        .build())
                .call(super::testJoinPushdownDisabled);
    }
}
