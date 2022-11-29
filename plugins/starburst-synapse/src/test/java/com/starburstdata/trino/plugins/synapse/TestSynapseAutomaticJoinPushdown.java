/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.synapse;

import io.trino.plugin.jdbc.BaseAutomaticJoinPushdownTest;
import io.trino.testing.QueryRunner;
import io.trino.testng.services.ManageTestResources;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.stream;
import static com.starburstdata.trino.plugins.synapse.SynapseQueryRunner.createSynapseQueryRunner;
import static java.lang.String.format;

public class TestSynapseAutomaticJoinPushdown
        extends BaseAutomaticJoinPushdownTest
{
    @ManageTestResources.Suppress(because = "Mock to remote server")
    private SynapseServer synapseServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        synapseServer = new SynapseServer();
        return createSynapseQueryRunner(
                Map.of(),
                synapseServer,
                "sqlserver",
                // Synapse tests are slow. Cache metadata to speed them up.
                Map.of("metadata.cache-ttl", "60m"),
                List.of());
    }

    @Override
    protected void gatherStats(String tableName)
    {
        List<String> columnNames = stream(computeActual("SHOW COLUMNS FROM " + tableName))
                .map(row -> (String) row.getField(0))
                .map(columnName -> "\"" + columnName + "\"")
                .collect(toImmutableList());
        for (Object columnName : columnNames) {
            synapseServer.execute(format("CREATE STATISTICS %1$s ON %2$s (%1$s)", columnName, tableName));
        }
        synapseServer.execute("UPDATE STATISTICS " + tableName);

        String schema = getSession().getSchema().orElseThrow();
        getQueryRunner().execute(format("CALL sqlserver.system.flush_statistics_cache('%s', '%s')", schema, tableName));
    }
}
