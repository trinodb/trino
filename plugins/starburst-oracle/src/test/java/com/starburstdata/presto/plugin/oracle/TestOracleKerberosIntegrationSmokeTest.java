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
import io.prestosql.Session;
import org.testng.SkipException;

import static com.starburstdata.presto.plugin.oracle.OracleQueryRunner.createOracleQueryRunner;
import static com.starburstdata.presto.plugin.oracle.TestingOracleServer.getResource;

public class TestOracleKerberosIntegrationSmokeTest
        extends BaseOracleIntegrationSmokeTest
{
    public TestOracleKerberosIntegrationSmokeTest()
    {
        super(() -> createOracleQueryRunner(
                ImmutableMap.<String, String>builder()
                        .put("connection-url", TestingOracleServer.getJdbcUrl())
                        .put("oracle.authentication.type", "KERBEROS")
                        .put("kerberos.client.principal", "test@TESTING-KRB.STARBURSTDATA.COM")
                        .put("kerberos.client.keytab", getResource("krb/client/test.keytab").toString())
                        .put("kerberos.config", getResource("krb/krb5.conf").toString())
                        .put("allow-drop-table", "true")
                        .build(),
                session -> Session.builder(session)
                        .setSchema("test")
                        .build(),
                ImmutableList.of(TpchTable.ORDERS, TpchTable.NATION)));
    }

    @Override
    protected String getUser()
    {
        return "test";
    }

    @Override
    public void testSelectInformationSchemaColumns()
    {
        throw new SkipException("This test is taking forever with kerberos authentication, due the connection retrying");
    }

    @Override
    public void testSelectInformationSchemaTables()
    {
        throw new SkipException("This test is taking forever with kerberos authentication, due the connection retrying");
    }
}
