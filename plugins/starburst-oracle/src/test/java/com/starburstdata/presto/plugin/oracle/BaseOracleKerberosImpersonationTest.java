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
import org.testng.annotations.Test;

import java.util.Map;
import java.util.function.Function;

import static com.starburstdata.presto.plugin.oracle.OracleQueryRunner.createOracleQueryRunner;
import static com.starburstdata.presto.plugin.oracle.TestingOracleServer.getResource;

@Test
public abstract class BaseOracleKerberosImpersonationTest
        extends BaseOracleImpersonationTest
{
    public BaseOracleKerberosImpersonationTest(Map<String, String> additionalProperties)
    {
        super(() -> createOracleQueryRunner(
                ImmutableMap.<String, String>builder()
                        .put("connection-url", TestingOracleServer.getJdbcUrl())
                        .put("oracle.impersonation.enabled", "true")
                        .put("oracle.synonyms.enabled", "true")
                        .put("oracle.authentication.type", "KERBEROS")
                        .put("kerberos.client.principal", "test@TESTING-KRB.STARBURSTDATA.COM")
                        .put("kerberos.client.keytab", getResource("krb/client/test.keytab").toString())
                        .put("kerberos.config", getResource("krb/krb5.conf").toString())
                        .build(),
                Function.identity(),
                ImmutableList.of()));
    }

    @Override
    protected String getProxyUser()
    {
        return "test";
    }
}
