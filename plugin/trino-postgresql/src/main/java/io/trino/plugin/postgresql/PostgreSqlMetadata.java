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
package io.trino.plugin.postgresql;

import io.trino.plugin.jdbc.DefaultJdbcMetadata;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcQueryEventListener;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.TimestampTimeZoneDomain;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.type.Type;

import java.util.Set;

import static io.trino.plugin.jdbc.BaseJdbcClient.varcharLiteral;
import static java.lang.String.format;

public class PostgreSqlMetadata
        extends DefaultJdbcMetadata
{
    public PostgreSqlMetadata(JdbcClient jdbcClient, TimestampTimeZoneDomain timestampTimeZoneDomain, Set<JdbcQueryEventListener> jdbcQueryEventListeners)
    {
        super(jdbcClient, timestampTimeZoneDomain, true, jdbcQueryEventListeners);
    }

    @Override
    public void setDefaultValue(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle, String defaultValue)
    {
        JdbcTableHandle table = (JdbcTableHandle) tableHandle;
        JdbcColumnHandle column = (JdbcColumnHandle) columnHandle;
        jdbcClient.execute(session, format(
                "ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s",
                jdbcClient.quoted(table.asPlainTable().getRemoteTableName()),
                jdbcClient.quoted(column.getColumnName()),
                getDefaultValue(column.getColumnType(), defaultValue)));
    }

    @Override
    public void dropDefaultValue(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        JdbcTableHandle table = (JdbcTableHandle) tableHandle;
        JdbcColumnHandle column = (JdbcColumnHandle) columnHandle;
        jdbcClient.execute(session, format(
                "ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT",
                jdbcClient.quoted(table.asPlainTable().getRemoteTableName()),
                jdbcClient.quoted(column.getColumnName())));
    }

    private String getDefaultValue(Type columnType, String defaultValue)
    {
        return switch (columnType.getBaseName()) {
            case "varchar", "char" -> varcharLiteral(defaultValue);
            default -> defaultValue;
        };
    }
}
