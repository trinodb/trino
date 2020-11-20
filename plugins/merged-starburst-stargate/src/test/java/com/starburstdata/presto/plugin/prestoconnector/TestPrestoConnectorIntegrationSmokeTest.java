/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.prestoconnector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.testing.AbstractTestIntegrationSmokeTest;
import io.prestosql.testing.QueryRunner;

import static com.starburstdata.presto.plugin.prestoconnector.PrestoConnectorQueryRunner.createPrestoConnectorLoopbackQueryRunner;
import static io.prestosql.tpch.TpchTable.CUSTOMER;
import static io.prestosql.tpch.TpchTable.NATION;
import static io.prestosql.tpch.TpchTable.ORDERS;
import static io.prestosql.tpch.TpchTable.REGION;

public class TestPrestoConnectorIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createPrestoConnectorLoopbackQueryRunner(
                // high node count is needed to avoid apparent deadlock e.g. in testConcurrentScans test. See also https://starburstdata.atlassian.net/browse/PRESTO-4793
                7,
                ImmutableMap.of(),
                true,
                ImmutableMap.of(),
                ImmutableList.of(CUSTOMER, NATION, ORDERS, REGION));
    }
}
