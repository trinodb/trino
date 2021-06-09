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
import com.starburstdata.presto.plugin.jdbc.dynamicfiltering.AbstractDynamicFilteringTest;
import io.trino.testing.QueryRunner;

import static com.starburstdata.presto.plugin.saphana.SapHanaQueryRunner.createSapHanaQueryRunner;
import static io.trino.tpch.TpchTable.ORDERS;

public class TestSapHanaDynamicFiltering
        extends AbstractDynamicFilteringTest
{
    private TestingSapHanaServer server;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        server = closeAfterClass(new TestingSapHanaServer());
        return createSapHanaQueryRunner(
                server,
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableList.of(ORDERS));
    }

    @Override
    protected boolean isJoinPushdownEnabledByDefault()
    {
        return true;
    }
}
