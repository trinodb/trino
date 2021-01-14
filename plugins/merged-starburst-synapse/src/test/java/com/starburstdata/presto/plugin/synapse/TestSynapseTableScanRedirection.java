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

import com.starburstdata.presto.plugin.jdbc.redirection.AbstractTableScanRedirectionTest;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;

import static com.starburstdata.presto.plugin.synapse.SynapseQueryRunner.CATALOG;
import static com.starburstdata.presto.plugin.synapse.SynapseQueryRunner.TEST_SCHEMA;
import static com.starburstdata.presto.plugin.synapse.SynapseQueryRunner.createSynapseQueryRunner;

public class TestSynapseTableScanRedirection
        extends AbstractTableScanRedirectionTest
{
    @Override
    protected QueryRunner createQueryRunner(Iterable<TpchTable<?>> tables)
            throws Exception
    {
        SynapseServer synapseServer = new SynapseServer();
        return createSynapseQueryRunner(
                synapseServer,
                true,
                getRedirectionProperties(CATALOG, TEST_SCHEMA),
                tables);
    }
}
