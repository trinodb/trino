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

import com.google.common.collect.ImmutableMap;
import com.starburstdata.presto.redirection.AbstractTableScanRedirectionTest;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.util.Map;
import java.util.Optional;

import static com.starburstdata.trino.plugin.stargate.StargateQueryRunner.createRemoteStarburstQueryRunnerWithMemory;
import static com.starburstdata.trino.plugin.stargate.StargateQueryRunner.createStargateQueryRunner;
import static com.starburstdata.trino.plugin.stargate.StargateQueryRunner.stargateConnectionUrl;

public class TestStargateTableScanRedirection
        extends AbstractTableScanRedirectionTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner remoteStarburst = closeAfterClass(createRemoteStarburstQueryRunnerWithMemory(
                REQUIRED_TPCH_TABLES,
                Optional.empty()));

        Map<String, String> connectorProperties = ImmutableMap.<String, String>builder()
                .put("connection-url", stargateConnectionUrl(remoteStarburst, "memory"))
                .putAll(getRedirectionProperties("p2p_remote", "tiny"))
                .buildOrThrow();

        return createStargateQueryRunner(
                false,
                Map.of(),
                connectorProperties);
    }
}
