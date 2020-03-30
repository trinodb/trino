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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.QueryId;
import net.snowflake.client.jdbc.SnowflakeConnectionV1;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.sql.Connection;
import java.sql.SQLException;

import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * Keeps Snowflake sessions open until query is finished so that temporary stages
 * are not removed before query finishes.
 */
@ThreadSafe
public class SnowflakeConnectionManager
{
    private final ConnectionFactory connectionFactory;
    private final Multimap<QueryId, Connection> connections = ArrayListMultimap.create();

    @Inject
    public SnowflakeConnectionManager(ConnectionFactory connectionFactory)
    {
        this.connectionFactory = requireNonNull(connectionFactory, "connectionFactory is null");
    }

    synchronized SnowflakeConnectionV1 openConnection(QueryId queryId, JdbcIdentity identity)
    {
        try {
            Connection connection = connectionFactory.openConnection(identity);
            connections.put(queryId, connection);
            return connection.unwrap(SnowflakeConnectionV1.class);
        }
        catch (SQLException exception) {
            throw new PrestoException(JDBC_ERROR, exception);
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
            throw new PrestoException(JDBC_ERROR, exception);
        }
    }
}
