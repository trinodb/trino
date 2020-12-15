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

import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tpch.TpchTable;
import org.testng.SkipException;

import java.util.Map;

import static com.starburstdata.presto.plugin.prestoconnector.PrestoConnectorQueryRunner.createPrestoConnectorQueryRunner;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoConnectorQueryRunner.createRemotePrestoQueryRunnerWithMemory;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoConnectorQueryRunner.prestoConnectorConnectionUrl;

public class TestPrestoConnectorDistributedQueriesWritesEnabled
        extends BasePrestoConnectorDistributedQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner remotePresto = closeAfterClass(createRemotePrestoQueryRunnerWithMemory(
                Map.of(),
                TpchTable.getTables()));
        return createPrestoConnectorQueryRunner(
                true,
                Map.of(),
                Map.of(
                        "connection-url", prestoConnectorConnectionUrl(remotePresto, "memory"),
                        "allow-drop-table", "true"));
    }

    @Override
    public void testLargeIn(int valuesCount)
    {
        throw new SkipException("Skipping expensive test; already run as part of TestPrestoConnectorDistributedQueries");
    }

    @Override
    public void testCreateTableAsSelect()
    {
        // this test takes ages to complete
        throw new SkipException("test TODO");
    }

    @Override
    public void testAddColumn()
    {
        // memory connector does not support adding columns
        // TODO (https://starburstdata.atlassian.net/browse/PRESTO-4832) make sure this is tested
        throw new SkipException("test TODO");
    }

    @Override
    public void testDropColumn()
    {
        // memory connector does not support dropping columns
        // TODO (https://starburstdata.atlassian.net/browse/PRESTO-4832) make sure this is tested
        throw new SkipException("test TODO");
    }

    @Override
    public void testRenameColumn()
    {
        // memory connector does not support renaming columns
        // TODO (https://starburstdata.atlassian.net/browse/PRESTO-4832) make sure this is tested
        throw new SkipException("test TODO");
    }

    @Override
    public void testCommentColumn()
    {
        // memory connector does not support setting column comments
        // TODO (https://starburstdata.atlassian.net/browse/PRESTO-4832) make sure this is tested
        throw new SkipException("test TODO");
    }

    @Override
    public void testDelete()
    {
        // memory connector does not support deletes
        // TODO (https://starburstdata.atlassian.net/browse/PRESTO-4832) make sure this is tested
        throw new SkipException("test TODO");
    }
}
