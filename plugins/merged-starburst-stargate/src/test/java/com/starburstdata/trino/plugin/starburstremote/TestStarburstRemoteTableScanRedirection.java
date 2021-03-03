/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.starburstremote;

import com.google.common.collect.ImmutableMap;
import com.starburstdata.presto.plugin.jdbc.redirection.AbstractTableScanRedirectionTest;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;

import java.util.Map;
import java.util.Optional;

import static com.starburstdata.trino.plugin.starburstremote.StarburstRemoteQueryRunner.createStarburstRemoteQueryRunner;
import static com.starburstdata.trino.plugin.starburstremote.StarburstRemoteQueryRunner.createStarburstRemoteQueryRunnerWithMemory;
import static com.starburstdata.trino.plugin.starburstremote.StarburstRemoteQueryRunner.starburstRemoteConnectionUrl;

public class TestStarburstRemoteTableScanRedirection
        extends AbstractTableScanRedirectionTest
{
    @Override
    protected QueryRunner createQueryRunner(Iterable<TpchTable<?>> tables)
            throws Exception
    {
        DistributedQueryRunner remoteStarburst = closeAfterClass(createStarburstRemoteQueryRunnerWithMemory(
                Map.of(),
                tables,
                Optional.empty()));

        Map<String, String> connectorProperties = ImmutableMap.<String, String>builder()
                .put("connection-url", starburstRemoteConnectionUrl(remoteStarburst, "memory"))
                .put("allow-drop-table", "true")
                .putAll(getRedirectionProperties("p2p_remote", "tiny"))
                .build();

        return createStarburstRemoteQueryRunner(
                false,
                Map.of(),
                connectorProperties);
    }
}
