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
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.JdbcMetadata;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorNewTableLayout;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;

import java.util.Optional;

import static com.starburstdata.presto.plugin.snowflake.jdbc.SnowflakeClient.checkColumnsForInvalidCharacters;
import static java.util.Objects.requireNonNull;

class SnowflakeMetadata
        extends JdbcMetadata
{
    private final SnowflakeConnectionManager connectionManager;

    SnowflakeMetadata(SnowflakeConnectionManager connectionManager, JdbcClient jdbcClient, boolean allowDropTable)
    {
        super(jdbcClient, allowDropTable);
        this.connectionManager = requireNonNull(connectionManager, "connectionManager is null");
    }

    @Override
    public void cleanupQuery(ConnectorSession session)
    {
        connectionManager.closeConnections(QueryId.valueOf(session.getQueryId()));
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
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
