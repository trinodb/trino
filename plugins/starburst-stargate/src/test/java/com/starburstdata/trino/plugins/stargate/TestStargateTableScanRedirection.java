/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.stargate;

import com.starburstdata.presto.redirection.AbstractTableScanRedirectionTest;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.util.Optional;

import static com.starburstdata.trino.plugins.stargate.StargateQueryRunner.createRemoteStarburstQueryRunnerWithMemory;
import static io.trino.testing.TestingNames.randomNameSuffix;

public class TestStargateTableScanRedirection
        extends AbstractTableScanRedirectionTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        String catalogName = "p2p_remote_" + randomNameSuffix();
        DistributedQueryRunner remoteStarburst = closeAfterClass(createRemoteStarburstQueryRunnerWithMemory(
                REQUIRED_TPCH_TABLES,
                Optional.empty()));

        return StargateQueryRunner.builder(remoteStarburst, "memory")
                .withCatalog(catalogName)
                .withConnectorProperties(getRedirectionProperties(catalogName, "tiny"))
                .build();
    }
}
