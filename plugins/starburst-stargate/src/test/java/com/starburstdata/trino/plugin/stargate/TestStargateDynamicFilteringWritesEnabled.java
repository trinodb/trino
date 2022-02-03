/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.stargate;

import com.starburstdata.presto.plugin.jdbc.dynamicfiltering.AbstractDynamicFilteringTest;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starburstdata.trino.plugin.stargate.StargateQueryRunner.createRemoteStarburstQueryRunnerWithMemory;
import static com.starburstdata.trino.plugin.stargate.StargateQueryRunner.createStargateQueryRunner;
import static com.starburstdata.trino.plugin.stargate.StargateQueryRunner.stargateConnectionUrl;
import static io.trino.tpch.TpchTable.ORDERS;

public class TestStargateDynamicFilteringWritesEnabled
        extends AbstractDynamicFilteringTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner remoteStarburst = closeAfterClass(createRemoteStarburstQueryRunnerWithMemory(
                List.of(ORDERS),
                Optional.empty()));
        return createStargateQueryRunner(
                true,
                Map.of("connection-url", stargateConnectionUrl(remoteStarburst, "memory")));
    }

    @Override
    protected boolean isJoinPushdownEnabledByDefault()
    {
        return true;
    }
}
