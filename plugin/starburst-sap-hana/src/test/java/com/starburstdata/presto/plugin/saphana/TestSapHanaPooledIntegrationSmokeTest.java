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
import io.prestosql.testing.QueryRunner;
import org.testng.annotations.Test;

import static com.starburstdata.presto.plugin.saphana.SapHanaQueryRunner.createSapHanaQueryRunner;
import static io.prestosql.tpch.TpchTable.CUSTOMER;
import static io.prestosql.tpch.TpchTable.NATION;
import static io.prestosql.tpch.TpchTable.ORDERS;
import static io.prestosql.tpch.TpchTable.REGION;

@Test
public class TestSapHanaPooledIntegrationSmokeTest
        extends TestSapHanaIntegrationSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        server = closeAfterClass(new TestingSapHanaServer());
        return createSapHanaQueryRunner(
                server,
                ImmutableMap.<String, String>builder()
                        .put("metadata.cache-ttl", "0m")
                        .put("metadata.cache-missing", "false")
                        .put("connection-pool.enabled", "true")
                        .build(),
                ImmutableMap.of(),
                ImmutableList.of(CUSTOMER, NATION, ORDERS, REGION));
    }
}
