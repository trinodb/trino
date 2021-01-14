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

import io.trino.testing.AbstractTestDistributedQueries;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import io.trino.tpch.TpchTable;

import java.util.Map;
import java.util.Optional;

import static com.starburstdata.presto.plugin.synapse.SynapseQueryRunner.createSynapseQueryRunner;

// TODO(https://starburstdata.atlassian.net/browse/PRESTO-5088) Extend Synapse tests form SQL server tests
public class TestSynapseDistributedQueries
        extends AbstractTestDistributedQueries
{
    private SynapseServer synapseServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        synapseServer = new SynapseServer();
        return createSynapseQueryRunner(
                synapseServer,
                true,
                Map.of(),
                TpchTable.getTables());
    }

    @Override
    protected boolean supportsDelete()
    {
        return false;
    }

    @Override
    protected boolean supportsViews()
    {
        return false;
    }

    @Override
    protected boolean supportsArrays()
    {
        return false;
    }

    @Override
    protected boolean supportsCommentOnTable()
    {
        return false;
    }

    @Override
    protected boolean supportsCommentOnColumn()
    {
        return false;
    }

    @Override
    public void testInsertUnicode()
    {
        // TODO(https://starburstdata.atlassian.net/browse/PRESTO-5085) Test varchars with unicode with data setup done by synapse
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                synapseServer::execute,
                "table",
                "(col_required BIGINT NOT NULL," +
                        "col_nullable BIGINT," +
                        "col_default BIGINT DEFAULT 43," +
                        "col_nonnull_default BIGINT NOT NULL DEFAULT 42," +
                        "col_required2 BIGINT NOT NULL)");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("time")
                || typeName.equals("timestamp")
                || typeName.equals("timestamp(3) with time zone")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }

        if (typeName.equals("varbinary")) {
            // TODO this should either work or fail cleanly
            return Optional.empty();
        }

        return Optional.of(dataMappingTestSetup);
    }
}
