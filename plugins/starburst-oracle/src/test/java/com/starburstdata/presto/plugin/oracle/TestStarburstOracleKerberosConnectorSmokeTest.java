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

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.oracle.BaseOracleConnectorSmokeTest;
import io.trino.testing.QueryRunner;

import static com.google.common.io.Resources.getResource;

public class TestStarburstOracleKerberosConnectorSmokeTest
        extends BaseOracleConnectorSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return OracleQueryRunner.builder()
                .withUnlockEnterpriseFeatures(true)
                .withConnectorProperties(
                    ImmutableMap.<String, String>builder()
                            .put("connection-url", TestingStarburstOracleServer.getJdbcUrl())
                            .put("oracle.authentication.type", "KERBEROS")
                            .put("kerberos.client.principal", "test@TESTING-KRB.STARBURSTDATA.COM")
                            .put("kerberos.client.keytab", getResource("krb/client/test.keytab").getPath())
                            .put("kerberos.config", getResource("krb/krb5.conf").getPath())
                            .buildOrThrow())
                .withTables(REQUIRED_TPCH_TABLES)
                .withSessionModifier(session -> Session.builder(session)
                        .setSchema("test")
                        .build())
                .build();
    }
}
