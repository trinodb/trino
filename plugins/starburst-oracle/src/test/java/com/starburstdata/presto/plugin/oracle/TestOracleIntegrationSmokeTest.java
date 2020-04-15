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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tpch.TpchTable;

public class TestOracleIntegrationSmokeTest
        extends BaseOracleIntegrationSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return OracleQueryRunner.builder()
                .withConnectorProperties(ImmutableMap.<String, String>builder()
                        .put("connection-url", TestingOracleServer.getJdbcUrl())
                        .put("connection-user", OracleTestUsers.USER)
                        .put("connection-password", OracleTestUsers.PASSWORD)
                        .put("allow-drop-table", "true")
                        .build())
                .withTables(ImmutableList.of(TpchTable.ORDERS, TpchTable.NATION))
                .build();
    }
}
