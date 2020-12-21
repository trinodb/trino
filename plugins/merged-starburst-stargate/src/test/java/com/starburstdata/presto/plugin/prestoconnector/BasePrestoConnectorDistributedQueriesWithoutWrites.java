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

import org.testng.SkipException;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class BasePrestoConnectorDistributedQueriesWithoutWrites
        extends BasePrestoConnectorDistributedQueries
{
    @Override
    public void testColumnName(String columnName)
    {
        assertThatThrownBy(() -> super.testColumnName(columnName))
                .hasMessageStartingWith("This connector does not support creating tables");
        throw new SkipException("not supported");
    }

    @Override
    public void testCommentColumn()
    {
        // TODO (https://starburstdata.atlassian.net/browse/PRESTO-4832) make sure this is tested
        assertThatThrownBy(super::testCommentColumn)
                .hasMessageStartingWith("This connector does not support creating tables");
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
        // TODO (https://starburstdata.atlassian.net/browse/PRESTO-4832) make sure this is tested
        assertThatThrownBy(super::testCreateTable)
                .hasMessageStartingWith("This connector does not support creating tables");
        throw new SkipException("not supported");
    }

    @Override
    public void testCreateTableAsSelect()
    {
        // TODO (https://starburstdata.atlassian.net/browse/PRESTO-4832) make sure this is tested
        assertThatThrownBy(super::testCreateTableAsSelect)
                .hasStackTraceContaining("This connector does not support creating tables");
        throw new SkipException("not supported");
    }

    @Override
    public void testInsert()
    {
        // TODO (https://starburstdata.atlassian.net/browse/PRESTO-4832) make sure this is tested
        assertThatThrownBy(super::testInsert)
                .hasMessageStartingWith("This connector does not support creating tables");
        throw new SkipException("not supported");
    }

    @Override
    public void testInsertUnicode()
    {
        // TODO (https://starburstdata.atlassian.net/browse/PRESTO-4832) make sure this is tested
        assertThatThrownBy(super::testInsertUnicode)
                .hasMessageStartingWith("This connector does not support creating tables");
        throw new SkipException("not supported");
    }

    @Override
    public void testDelete()
    {
        // TODO (https://starburstdata.atlassian.net/browse/PRESTO-4832) make sure this is tested
        assertThatThrownBy(super::testDelete)
                .hasStackTraceContaining("This connector does not support creating tables");
        throw new SkipException("not supported");
    }

    @Override
    public void testAddColumn()
    {
        // TODO (https://starburstdata.atlassian.net/browse/PRESTO-4832) make sure this is tested
        assertThatThrownBy(super::testAddColumn)
                .hasMessageStartingWith("This connector does not support creating tables");
        throw new SkipException("not supported");
    }

    @Override
    public void testDropColumn()
    {
        // TODO (https://starburstdata.atlassian.net/browse/PRESTO-4832) make sure this is tested
        assertThatThrownBy(super::testDropColumn)
                .hasMessageStartingWith("This connector does not support creating tables");
        throw new SkipException("not supported");
    }

    @Override
    public void testRenameColumn()
    {
        // TODO (https://starburstdata.atlassian.net/browse/PRESTO-4832) make sure this is tested
        assertThatThrownBy(super::testRenameColumn)
                .hasMessageStartingWith("This connector does not support creating tables");
        throw new SkipException("not supported");
    }

    @Override
    public void testDataMappingSmokeTest(DataMappingTestSetup dataMappingTestSetup)
    {
        // This is covered by TestPrestoConnectorDistributedQueriesWritesEnabled.testDataMappingSmokeTest
        throw new SkipException("not needed here");
    }

    @Override
    public void testQueryLoggingCount()
    {
        assertThatThrownBy(super::testQueryLoggingCount)
                .hasMessageStartingWith("This connector does not support creating tables");
        throw new SkipException("not supported");
    }

    @Override
    public void testRenameTable()
    {
        // TODO (https://starburstdata.atlassian.net/browse/PRESTO-4832) make sure this is tested
        assertThatThrownBy(super::testRenameTable)
                .hasMessageStartingWith("This connector does not support creating tables");
        throw new SkipException("not supported");
    }

    @Override
    public void testSymbolAliasing()
    {
        assertThatThrownBy(super::testSymbolAliasing)
                .hasMessageStartingWith("This connector does not support creating tables");
        throw new SkipException("not supported");
    }

    @Override
    @Test
    public void testWrittenStats()
    {
        assertThatThrownBy(super::testWrittenStats)
                .hasMessageStartingWith("This connector does not support creating tables");
        throw new SkipException("not supported");
    }
}
