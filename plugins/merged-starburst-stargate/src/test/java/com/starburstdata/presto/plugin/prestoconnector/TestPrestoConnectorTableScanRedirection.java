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

import com.google.common.collect.ImmutableMap;
import com.starburstdata.presto.plugin.jdbc.redirection.AbstractTableScanRedirectionTest;
import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tpch.TpchTable;

import java.util.Map;

import static com.starburstdata.presto.plugin.prestoconnector.PrestoConnectorQueryRunner.createPrestoConnectorQueryRunner;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoConnectorQueryRunner.createRemotePrestoQueryRunnerWithMemory;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoConnectorQueryRunner.prestoConnectorConnectionUrl;

public class TestPrestoConnectorTableScanRedirection
        extends AbstractTableScanRedirectionTest
{
    @Override
    protected QueryRunner createQueryRunner(Iterable<TpchTable<?>> tables)
            throws Exception
    {
        DistributedQueryRunner remotePresto = closeAfterClass(createRemotePrestoQueryRunnerWithMemory(
                Map.of(),
                tables));

        Map<String, String> connectorProperties = ImmutableMap.<String, String>builder()
                .put("connection-url", prestoConnectorConnectionUrl(remotePresto, "memory"))
                .put("allow-drop-table", "true")
                .putAll(getRedirectionProperties("p2p_remote", "tiny"))
                .build();

        return createPrestoConnectorQueryRunner(
                false,
                Map.of(),
                connectorProperties);
    }
}
