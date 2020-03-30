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

import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.JdbcMetadata;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.connector.ConnectorSession;

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
}
