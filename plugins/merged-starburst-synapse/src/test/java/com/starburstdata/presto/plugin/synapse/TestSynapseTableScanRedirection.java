/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.synapse;

import com.google.common.collect.ImmutableMap;
import com.starburstdata.presto.redirection.AbstractTableScanRedirectionTest;
import io.trino.testing.QueryRunner;

import static com.starburstdata.presto.plugin.synapse.SynapseQueryRunner.DEFAULT_CATALOG_NAME;
import static com.starburstdata.presto.plugin.synapse.SynapseQueryRunner.TEST_SCHEMA;
import static com.starburstdata.presto.plugin.synapse.SynapseQueryRunner.createSynapseQueryRunner;

public class TestSynapseTableScanRedirection
        extends AbstractTableScanRedirectionTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        SynapseServer synapseServer = new SynapseServer();
        return createSynapseQueryRunner(
                synapseServer,
                ImmutableMap.<String, String>builder()
                        .putAll(getRedirectionProperties(DEFAULT_CATALOG_NAME, TEST_SCHEMA))
                        // Synapse tests are slow. Cache metadata to speed them up.
                        .put("metadata.cache-ttl", "60m")
                        .buildOrThrow(),
                REQUIRED_TPCH_TABLES);
    }
}
