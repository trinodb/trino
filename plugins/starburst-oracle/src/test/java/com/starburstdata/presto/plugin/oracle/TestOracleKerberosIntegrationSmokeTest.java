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
import io.trino.testing.QueryRunner;
import org.testng.SkipException;
import org.testng.annotations.Test;

import static com.google.common.io.Resources.getResource;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOracleKerberosIntegrationSmokeTest
        extends BaseLicensedStarburstOracleIntegrationSmokeTest
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
                            .put("allow-drop-table", "true")
                            .build())
                .withSessionModifier(session -> Session.builder(session)
                        .setSchema("test")
                        .build())
                .build();
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

    /**
     * A copy of {@link BaseStarburstOracleAggregationPushdownTest#testLimitPushdown()} since this class doesn't inherit from it.
     */
    @Test
    @Override
    public void testLimitPushdown()
    {
        assertThat(query("SELECT name FROM nation LIMIT 30")).isFullyPushedDown(); // Use high limit for result determinism

        // with filter over numeric column
        assertThat(query("SELECT name FROM nation WHERE regionkey = 3 LIMIT 5")).isFullyPushedDown();

        // with filter over varchar column
        assertThat(query("SELECT name FROM nation WHERE name < 'EEE' LIMIT 5")).isFullyPushedDown();

        // with aggregation
        assertThat(query("SELECT max(regionkey) FROM nation LIMIT 5")).isFullyPushedDown(); // global aggregation, LIMIT removed
        assertThat(query("SELECT regionkey, max(name) FROM nation GROUP BY regionkey LIMIT 5")).isFullyPushedDown();
        assertThat(query("SELECT DISTINCT regionkey FROM nation LIMIT 5")).isFullyPushedDown();

        // with filter and aggregation
        assertThat(query("SELECT regionkey, count(*) FROM nation WHERE nationkey < 5 GROUP BY regionkey LIMIT 3")).isFullyPushedDown();
        assertThat(query("SELECT regionkey, count(*) FROM nation WHERE name < 'EGYPT' GROUP BY regionkey LIMIT 3")).isFullyPushedDown();
    }
}
