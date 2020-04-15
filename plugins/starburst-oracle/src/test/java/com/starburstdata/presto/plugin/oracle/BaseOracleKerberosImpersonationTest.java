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

import static com.google.common.io.Resources.getResource;

@Test
public abstract class BaseOracleKerberosImpersonationTest
        extends BaseOracleImpersonationTest
{
    private final Map<String, String> properties;

    protected BaseOracleKerberosImpersonationTest(Map<String, String> additionalProperties)
    {
        properties = ImmutableMap.<String, String>builder()
                .put("connection-url", TestingOracleServer.getJdbcUrl())
                .put("oracle.impersonation.enabled", "true")
                .put("oracle.synonyms.enabled", "true")
                .put("oracle.authentication.type", "KERBEROS")
                .put("kerberos.client.principal", "test@TESTING-KRB.STARBURSTDATA.COM")
                .put("kerberos.client.keytab", getResource("krb/client/test.keytab").getPath())
                .put("kerberos.config", getResource("krb/krb5.conf").getPath())
                .putAll(additionalProperties)
                .build();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return OracleQueryRunner
                .builder()
                .withConnectorProperties(properties)
                .withTables(ImmutableList.of())
                .build();
    }

    @Override
    protected String getProxyUser()
    {
        return "test";
    }
}
