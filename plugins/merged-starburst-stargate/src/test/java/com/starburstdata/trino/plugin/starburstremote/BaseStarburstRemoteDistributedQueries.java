/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.starburstremote;

import io.trino.testing.AbstractTestDistributedQueries;
import io.trino.testing.sql.TestTable;
import org.testng.SkipException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class BaseStarburstRemoteDistributedQueries
        extends AbstractTestDistributedQueries
{
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
        throw new SkipException("DEFAULT not supported in Trino");
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void testCommentTable()
    {
        assertThatThrownBy(super::testCommentTable)
                .hasMessage("This connector does not support setting table comments")
                .hasStackTraceContaining("io.trino.spi.connector.ConnectorMetadata.setTableComment"); // not overridden, so we know this is not a remote exception
        throw new SkipException("not supported");
    }
}
