/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.saphana;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.testing.AbstractTestIntegrationSmokeTest;
import io.prestosql.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static com.starburstdata.presto.plugin.saphana.SapHanaQueryRunner.createSapHanaQueryRunner;
import static io.prestosql.tpch.TpchTable.CUSTOMER;
import static io.prestosql.tpch.TpchTable.NATION;
import static io.prestosql.tpch.TpchTable.ORDERS;
import static io.prestosql.tpch.TpchTable.REGION;
import static org.assertj.core.api.Assertions.assertThat;

@Test
public class TestSapHanaIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private TestingSapHanaServer server;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        server = new TestingSapHanaServer();
        return createSapHanaQueryRunner(
                server,
                ImmutableMap.<String, String>builder()
                        .put("metadata.cache-ttl", "0m")
                        .put("metadata.cache-missing", "false")
                        .build(),
                ImmutableMap.of(),
                ImmutableList.of(CUSTOMER, NATION, ORDERS, REGION));
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        server.close();
    }

    @Test
    public void testLimitPushdown()
    {
        assertThat(query("SELECT name FROM nation LIMIT 30")).isCorrectlyPushedDown(); // Use high limit for result determinism

        // with filter over numeric column
        assertThat(query("SELECT name FROM nation WHERE regionkey = 3 LIMIT 5")).isCorrectlyPushedDown();

        // with filter over varchar column
        assertThat(query("SELECT name FROM nation WHERE name < 'EEE' LIMIT 5")).isCorrectlyPushedDown();
    }
}
