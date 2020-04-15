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
import org.testng.annotations.Test;

import java.util.Map;

@Test
public abstract class BaseOracleJdbcImpersonationTest
        extends BaseOracleImpersonationTest
{
    private final ImmutableMap<String, String> properties;

    protected BaseOracleJdbcImpersonationTest(Map<String, String> additionalProperties)
    {
        properties = ImmutableMap.<String, String>builder()
                .put("connection-url", TestingOracleServer.getJdbcUrl())
                .put("connection-user", TestingOracleServer.USER)
                .put("connection-password", TestingOracleServer.PASSWORD)
                .put("allow-drop-table", "true")
                .put("oracle.impersonation.enabled", "true")
                .put("oracle.synonyms.enabled", "true")
                .putAll(additionalProperties)
                .build();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return OracleQueryRunner.builder()
                .withConnectorProperties(properties)
                .withTables(ImmutableList.of())
                .build();
    }

    @Override
    protected String getProxyUser()
    {
        return "presto_test_user";
    }
}
