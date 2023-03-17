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

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.plugin.sqlserver.TestingSqlServer;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import static io.trino.tpch.TpchTable.NATION;

public class TestSqlServerLicenseProtection
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingSqlServer sqlServer = closeAfterClass(new TestingSqlServer());
        return StarburstSqlServerQueryRunner.builder(sqlServer)
                .withTables(ImmutableList.of(NATION))
                .build();
    }

    @Test
    public void testLicenseProtectionOfParallelismViaSessionProperty()
    {
        Session noParallelismSession = Session.builder(getSession())
                .setCatalogSessionProperty("sqlserver", "parallel_connections_count", "1")
                .build();

        Session partitionsParallelismSession = Session.builder(getSession())
                .setCatalogSessionProperty("sqlserver", "parallel_connections_count", "4")
                .build();

        assertQuery(noParallelismSession, "SELECT * FROM nation");
        assertQueryFails(partitionsParallelismSession, "SELECT * FROM nation", "Starburst Enterprise requires valid license");
    }
}
