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
