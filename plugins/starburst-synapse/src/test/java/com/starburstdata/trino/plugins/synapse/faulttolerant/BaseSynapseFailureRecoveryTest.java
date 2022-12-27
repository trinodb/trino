/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.synapse.faulttolerant;

import com.google.common.collect.ImmutableMap;
import com.starburstdata.trino.plugins.synapse.SynapseServer;
import io.trino.operator.RetryPolicy;
import io.trino.plugin.exchange.filesystem.FileSystemExchangePlugin;
import io.trino.plugin.jdbc.BaseJdbcFailureRecoveryTest;
import io.trino.testing.QueryRunner;
import io.trino.testng.services.ManageTestResources;
import io.trino.tpch.TpchTable;

import java.util.List;
import java.util.Map;

import static com.starburstdata.trino.plugins.synapse.SynapseQueryRunner.DEFAULT_CATALOG_NAME;
import static com.starburstdata.trino.plugins.synapse.SynapseQueryRunner.createSynapseQueryRunner;

public abstract class BaseSynapseFailureRecoveryTest
        extends BaseJdbcFailureRecoveryTest
{
    @ManageTestResources.Suppress(because = "Mock to remote server")
    private SynapseServer synapseServer;

    public BaseSynapseFailureRecoveryTest(RetryPolicy retryPolicy)
    {
        super(retryPolicy);
        synapseServer = new SynapseServer();
    }

    @Override
    protected QueryRunner createQueryRunner(
            List<TpchTable<?>> requiredTpchTables,
            Map<String, String> configProperties,
            Map<String, String> coordinatorProperties)
            throws Exception
    {
        return createSynapseQueryRunner(
                configProperties,
                synapseServer,
                DEFAULT_CATALOG_NAME,
                Map.of(),
                coordinatorProperties,
                requiredTpchTables,
                runner -> {
                    runner.installPlugin(new FileSystemExchangePlugin());
                    runner.loadExchangeManager("filesystem", ImmutableMap.of(
                            "exchange.base-directories", System.getProperty("java.io.tmpdir") + "/trino-local-file-system-exchange-manager"));
                });
    }
}
