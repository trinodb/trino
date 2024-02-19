/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.sqlserver;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.sqlserver.TestSqlServerConnectorTest;
import io.trino.plugin.sqlserver.TestingSqlServer;
import io.trino.testing.QueryRunner;
import io.trino.testng.services.Flaky;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;

public class TestStarburstSqlServerConnectorTest
        extends TestSqlServerConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        sqlServer = closeAfterClass(new TestingSqlServer());
        return StarburstSqlServerQueryRunner.builder(sqlServer)
                .withConnectorProperties(ImmutableMap.of(
                        "sqlserver.experimental.stored-procedure-table-function-enabled", "true"))
                .withTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    @Flaky(issue = "https://github.com/trinodb/trino/issues/12535", match = ".*but the following elements were unexpected.*")
    @Test
    @Timeout(60)
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
}
