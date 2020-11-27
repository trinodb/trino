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
import org.testng.annotations.Test;

import java.util.Map;

import static com.starburstdata.presto.plugin.prestoconnector.PrestoConnectorQueryRunner.createPrestoConnectorQueryRunner;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoConnectorQueryRunner.createRemotePrestoQueryRunner;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoConnectorQueryRunner.prestoConnectorConnectionUrl;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestPrestoConnectorDistributedQueries
        extends BasePrestoConnectorDistributedQueries
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
                false,
                Map.of(),
                Map.of(
                        "connection-url", prestoConnectorConnectionUrl(remotePresto, "memory"),
                        "allow-drop-table", "true"));
    }

    @Override
    public void testColumnName(String columnName)
    {
        assertThatThrownBy(() -> super.testColumnName(columnName))
                .hasMessage("This connector does not support creating tables");
        throw new SkipException("not supported");
    }

    @Override
    public void testCommentTable()
    {
        assertThatThrownBy(super::testCommentTable)
                .hasMessageContaining("but was:\n" +
                        "  <\"This connector does not support creating tables\">");
        throw new SkipException("not supported");
    }

    @Override
    public void testCreateTable()
    {
        assertThatThrownBy(super::testCreateTable)
                .hasMessage("This connector does not support creating tables");
        throw new SkipException("not supported");
    }

    @Override
    public void testDataMappingSmokeTest(DataMappingTestSetup dataMappingTestSetup)
    {
        assertThatThrownBy(() -> super.testDataMappingSmokeTest(dataMappingTestSetup))
                .hasMessageContaining("This connector does not support creating tables with data");
        throw new SkipException("not supported");
    }

    @Override
    public void testInsert()
    {
        assertThatThrownBy(super::testInsert)
                .hasMessage("This connector does not support creating tables with data");
        throw new SkipException("not supported");
    }

    @Override
    public void testInsertUnicode()
    {
        assertThatThrownBy(super::testInsertUnicode)
                .hasMessage("This connector does not support creating tables");
        throw new SkipException("not supported");
    }

    @Override
    public void testInsertWithCoercion()
    {
        assertThatThrownBy(super::testInsertWithCoercion)
                .hasMessage("This connector does not support creating tables");
        throw new SkipException("not supported");
    }

    @Override
    public void testQueryLoggingCount()
    {
        assertThatThrownBy(super::testQueryLoggingCount)
                .hasMessage("This connector does not support creating tables with data");
        throw new SkipException("not supported");
    }

    @Override
    public void testRenameTable()
    {
        assertThatThrownBy(super::testRenameTable)
                .hasMessage("This connector does not support creating tables with data");
        throw new SkipException("not supported");
    }

    @Override
    public void testSymbolAliasing()
    {
        assertThatThrownBy(super::testSymbolAliasing)
                .hasMessage("This connector does not support creating tables with data");
        throw new SkipException("not supported");
    }

    @Override
    @Test
    public void testWrittenStats()
    {
        assertThatThrownBy(super::testWrittenStats)
                .hasMessage("This connector does not support creating tables with data");
        throw new SkipException("not supported");
    }
}
