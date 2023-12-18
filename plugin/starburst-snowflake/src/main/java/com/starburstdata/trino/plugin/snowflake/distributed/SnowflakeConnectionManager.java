/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.snowflake.distributed;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.inject.Inject;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcQueryEventListener;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import net.snowflake.client.jdbc.SnowflakeConnectionV1;

import java.sql.Connection;
import java.sql.SQLException;

import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * Keeps Snowflake sessions open until query is finished so that temporary stages
 * are not removed before query finishes.
 */
@ThreadSafe
public class SnowflakeConnectionManager
        implements JdbcQueryEventListener
{
    private final ConnectionFactory connectionFactory;
    private final Multimap<QueryId, Connection> connections = ArrayListMultimap.create();

    @Inject
    public SnowflakeConnectionManager(ConnectionFactory connectionFactory)
    {
        this.connectionFactory = requireNonNull(connectionFactory, "connectionFactory is null");
    }

    synchronized SnowflakeConnectionV1 openConnection(ConnectorSession session)
    {
        try {
            QueryId queryId = QueryId.valueOf(session.getQueryId());
            Connection connection = connectionFactory.openConnection(session);
            connections.put(queryId, connection);
            return connection.unwrap(SnowflakeConnectionV1.class);
        }
        catch (SQLException exception) {
            throw new TrinoException(JDBC_ERROR, exception);
        }
    }

    synchronized void closeConnections(QueryId queryId)
    {
        try {
            for (Connection connection : connections.get(queryId)) {
                connection.close();
            }
        }
        catch (SQLException exception) {
            throw new TrinoException(JDBC_ERROR, exception);
        }
    }

    @Override
    public void beginQuery(ConnectorSession session) {}

    @Override
    public void cleanupQuery(ConnectorSession session)
    {
        closeConnections(QueryId.valueOf(session.getQueryId()));
    }
}
