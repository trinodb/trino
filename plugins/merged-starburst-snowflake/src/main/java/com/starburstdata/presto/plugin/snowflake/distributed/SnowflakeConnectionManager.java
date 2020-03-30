/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
