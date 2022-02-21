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
import io.trino.plugin.oracle.AbstractTestOracleTypeMapping;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import io.trino.tpch.TpchTable;

public class TestOracleTypeMapping
        extends AbstractTestOracleTypeMapping
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return OracleQueryRunner.builder()
                .withConnectorProperties(ImmutableMap.<String, String>builder()
                        .putAll(TestingStarburstOracleServer.connectionProperties())
                        .put("case-insensitive-name-matching", "true")
                        .buildOrThrow())
                .withTables(ImmutableList.of(TpchTable.ORDERS))
                .build();
    }

    @Override
    protected SqlExecutor getOracleSqlExecutor()
    {
        return TestingStarburstOracleServer::executeInOracle;
    }
}
