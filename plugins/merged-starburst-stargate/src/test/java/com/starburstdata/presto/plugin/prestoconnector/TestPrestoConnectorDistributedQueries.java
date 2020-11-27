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

import io.prestosql.testing.AbstractTestDistributedQueries;
import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.sql.TestTable;
import io.prestosql.tpch.TpchTable;
import org.testng.SkipException;

import java.util.Map;
import java.util.Optional;

import static com.starburstdata.presto.plugin.prestoconnector.PrestoConnectorQueryRunner.createPrestoConnectorQueryRunner;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoConnectorQueryRunner.createRemotePrestoQueryRunner;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoConnectorQueryRunner.prestoConnectorConnectionUrl;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestPrestoConnectorDistributedQueries
        extends AbstractTestDistributedQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner remotePresto = closeAfterClass(createRemotePrestoQueryRunner(
                Map.of(),
                false,
                TpchTable.getTables()));
        return createPrestoConnectorQueryRunner(
                true, // TODO split out test class version with writes disabled (default option)
                Map.of(),
                Map.of(
                        "connection-url", prestoConnectorConnectionUrl(remotePresto, "memory"),
                        "allow-drop-table", "true"));
    }

    @Override
    protected boolean supportsViews()
    {
        // TODO https://starburstdata.atlassian.net/browse/PRESTO-4795
        return false;
    }

    @Override
    protected boolean supportsArrays()
    {
        // TODO https://starburstdata.atlassian.net/browse/PRESTO-4798
        return false;
    }

    @Override
    public void testInsertForDefaultColumn()
    {
        // TODO run the test against a backend catalog that supports default values for a column
        throw new SkipException("DEFAULT not supported in Presto");
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void testAddColumn()
    {
        // TODO (https://starburstdata.atlassian.net/browse/PRESTO-4759) memory connector does not support adding columns
        throw new SkipException("test TODO");
    }

    @Override
    public void testDropColumn()
    {
        // TODO (https://starburstdata.atlassian.net/browse/PRESTO-4759) memory connector does not support dropping columns
        throw new SkipException("test TODO");
    }

    @Override
    public void testRenameColumn()
    {
        // TODO (https://starburstdata.atlassian.net/browse/PRESTO-4759) memory connector does not support renaming columns
        throw new SkipException("test TODO");
    }

    @Override
    public void testCommentColumn()
    {
        // TODO (https://starburstdata.atlassian.net/browse/PRESTO-4759) memory connector does not support setting column comments
        throw new SkipException("test TODO");
    }

    @Override
    public void testCommentTable()
    {
        assertThatThrownBy(super::testCommentTable)
                .hasMessage("This connector does not support setting table comments")
                .hasStackTraceContaining("io.prestosql.spi.connector.ConnectorMetadata.setTableComment"); // not overridden, so we know this is not a remote exception
        throw new SkipException("not supported");
    }

    @Override
    public void testCreateTableAsSelect()
    {
        // TODO (https://starburstdata.atlassian.net/browse/PRESTO-4759) this test takes ages to complete
        throw new SkipException("test TODO");
    }

    @Override
    public void testDelete()
    {
        // TODO (https://starburstdata.atlassian.net/browse/PRESTO-4759) memory connector does not support deletes
        throw new SkipException("test TODO");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        switch (dataMappingTestSetup.getPrestoTypeName()) {
            case "time":
            case "timestamp":
            case "timestamp(3) with time zone":
                // TODO (https://starburstdata.atlassian.net/browse/PRESTO-4760) implement mapping for date-time types
                return Optional.of(dataMappingTestSetup.asUnsupported());
        }

        return Optional.of(dataMappingTestSetup);
    }
}
