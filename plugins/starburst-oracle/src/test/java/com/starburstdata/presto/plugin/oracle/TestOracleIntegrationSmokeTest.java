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
import io.airlift.tpch.TpchTable;

import java.util.function.Function;

import static com.starburstdata.presto.plugin.oracle.OracleQueryRunner.createOracleQueryRunner;

public class TestOracleIntegrationSmokeTest
        extends BaseOracleIntegrationSmokeTest
{
    public TestOracleIntegrationSmokeTest()
    {
        super(() -> createOracleQueryRunner(
                ImmutableMap.<String, String>builder()
                        .put("connection-url", TestingOracleServer.getJdbcUrl())
                        .put("connection-user", TestingOracleServer.USER)
                        .put("connection-password", TestingOracleServer.PASSWORD)
                        .put("allow-drop-table", "true")
                        .build(),
                Function.identity(),
                ImmutableList.of(TpchTable.ORDERS, TpchTable.NATION)));
    }
}
