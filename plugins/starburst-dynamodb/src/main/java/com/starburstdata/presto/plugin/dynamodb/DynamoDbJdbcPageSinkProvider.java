/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.dynamodb;

import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcPageSinkProvider;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

/*
 * The OEM key requires a com.starburstdata.* class to be on the stack trace when making calls to DynamoDB.
 * This class extends JdbcPageSinkProvider but forwards all calls to the base to make sure it is on the stack for testing the connector.
 */
public class DynamoDbJdbcPageSinkProvider
        extends JdbcPageSinkProvider
{
    @Inject
    public DynamoDbJdbcPageSinkProvider(JdbcClient jdbcClient)
    {
        super(jdbcClient);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle tableHandle)
    {
        return super.createPageSink(transactionHandle, session, tableHandle);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle tableHandle)
    {
        return super.createPageSink(transactionHandle, session, tableHandle);
    }
}
