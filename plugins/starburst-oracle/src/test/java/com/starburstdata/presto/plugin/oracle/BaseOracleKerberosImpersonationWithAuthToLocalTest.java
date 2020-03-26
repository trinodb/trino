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

import static com.starburstdata.presto.plugin.oracle.OracleQueryRunner.createOracleQueryRunner;
import static com.starburstdata.presto.plugin.oracle.OracleQueryRunner.createSession;
import static com.starburstdata.presto.plugin.oracle.TestingOracleServer.getResource;

@Test
public class BaseOracleKerberosImpersonationWithAuthToLocalTest
        extends BaseOracleImpersonationWithAuthToLocal
{
    public BaseOracleKerberosImpersonationWithAuthToLocalTest(Map<String, String> additionalProperties)
    {
        super(() -> createOracleQueryRunner(
                ImmutableMap.<String, String>builder()
                        .put("connection-url", TestingOracleServer.getJdbcUrl())
                        .put("oracle.impersonation.enabled", "true")
                        .put("auth-to-local.config-file", getResource("auth-to-local.json").toString())
                        .put("auth-to-local.refresh-period", "1s")
                        .put("oracle.synonyms.enabled", "true")
                        .put("oracle.authentication.type", "KERBEROS")
                        .put("kerberos.client.principal", "test@TESTING-KRB.STARBURSTDATA.COM")
                        .put("kerberos.client.keytab", getResource("krb/client/test.keytab").toString())
                        .put("kerberos.config", getResource("krb/krb5.conf").toString())
                        .build(),
                session -> createSession(session.getIdentity().getUser() + "/admin@company.com"),
                ImmutableList.of()));
    }

    @Override
    protected String getProxyUser()
    {
        return "test";
    }
}
