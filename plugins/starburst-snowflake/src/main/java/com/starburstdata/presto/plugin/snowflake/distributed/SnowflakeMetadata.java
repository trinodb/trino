/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake.distributed;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.jdbc.DefaultJdbcMetadata;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.spi.QueryId;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;

import java.util.Optional;

import static com.starburstdata.presto.plugin.snowflake.jdbc.SnowflakeClient.checkColumnsForInvalidCharacters;
import static java.util.Objects.requireNonNull;

class SnowflakeMetadata
        extends DefaultJdbcMetadata
{
    private final SnowflakeConnectionManager connectionManager;

    SnowflakeMetadata(SnowflakeConnectionManager connectionManager, JdbcClient jdbcClient)
    {
        super(jdbcClient, true);
        this.connectionManager = requireNonNull(connectionManager, "connectionManager is null");
    }

    @Override
    public void cleanupQuery(ConnectorSession session)
    {
        connectionManager.closeConnections(QueryId.valueOf(session.getQueryId()));
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout)
    {
        checkColumnsForInvalidCharacters(tableMetadata.getColumns());
        return super.beginCreateTable(session, tableMetadata, layout);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        checkColumnsForInvalidCharacters(tableMetadata.getColumns());
        super.createTable(session, tableMetadata, ignoreExisting);
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle table, ColumnMetadata columnMetadata)
    {
        checkColumnsForInvalidCharacters(ImmutableList.of(columnMetadata));
        super.addColumn(session, table, columnMetadata);
    }
}
