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
package io.prestosql.plugin.snowflake;

import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcSplit;
import io.prestosql.plugin.jdbc.WriteMapping;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.Type;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;
import java.util.function.BiFunction;

import static io.prestosql.plugin.jdbc.StandardColumnMappings.timestampWriteFunctionUsingSqlTimestamp;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP_NANOS;

public class SnowflakeClient
        extends BaseJdbcClient
{
    @Inject
    public SnowflakeClient(BaseJdbcConfig config, ConnectionFactory connectionFactory)
    {
        super(config, "\"", connectionFactory);
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> sql + " LIMIT " + limit);
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (TIMESTAMP_NANOS.equals(type)) {
            return WriteMapping.longMapping("timestamp", timestampWriteFunctionUsingSqlTimestamp(TIMESTAMP_NANOS));
        }

        return super.toWriteMapping(session, type);
    }

    // Required since SnowflakeConnectionV1 does not support setReadOnly
    @Override
    public Connection getConnection(JdbcIdentity identity, JdbcSplit split) throws SQLException
    {
        return connectionFactory.openConnection(identity);
    }
}
