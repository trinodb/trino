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

import com.starburstdata.presto.testing.SessionMutator;
import io.trino.Session;
import io.trino.plugin.sqlserver.TestSqlServerConnectorTest;
import io.trino.plugin.sqlserver.TestingSqlServer;
import io.trino.testing.QueryRunner;
import io.trino.testng.services.Flaky;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestStarburstSqlServerConnectorTest
        extends TestSqlServerConnectorTest
{
    private final SessionMutator sessionMutator = new SessionMutator(super::getSession);

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        sqlServer = closeAfterClass(new TestingSqlServer());
        return StarburstSqlServerQueryRunner.builder(sqlServer)
                .withTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    @Flaky(issue = "https://github.com/trinodb/trino/issues/12535", match = ".*but the following elements were unexpected.*")
    @Test(timeOut = 60_000)
    public void testAddColumnConcurrently()
            throws Exception
    {
        super.testAddColumnConcurrently();
    }

    @Override
    protected void verifyConcurrentAddColumnFailurePermissible(Exception e)
    {
        assertThat(e)
                .hasMessageContaining("was deadlocked on lock resources");
    }

    @Override
    protected Session getSession()
    {
        return sessionMutator.getSession();
    }
}
