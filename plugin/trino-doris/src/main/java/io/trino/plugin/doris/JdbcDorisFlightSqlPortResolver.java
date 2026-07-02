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
package io.trino.plugin.doris;

import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class JdbcDorisFlightSqlPortResolver
        implements DorisFlightSqlPortResolver
{
    private static final String SHOW_FRONTENDS_SQL = "SHOW FRONTENDS";
    private static final String ARROW_FLIGHT_SQL_PORT_COLUMN = "ArrowFlightSqlPort";

    private final DorisConfig config;
    private final DorisJdbcConnectionFactory connectionFactory;

    private volatile Integer cachedFlightSqlPort;

    @Inject
    public JdbcDorisFlightSqlPortResolver(DorisConfig config, DorisJdbcConnectionFactory connectionFactory)
    {
        this.config = requireNonNull(config, "config is null");
        this.connectionFactory = requireNonNull(connectionFactory, "connectionFactory is null");
    }

    @Override
    public int resolveFlightSqlPort()
    {
        return resolveFlightSqlPort(null);
    }

    @Override
    public int resolveFlightSqlPort(ConnectorSession session)
    {
        if (config.getFlightSqlPort() > 0) {
            return config.getFlightSqlPort();
        }

        Integer port = cachedFlightSqlPort;
        if (port != null) {
            return port;
        }

        synchronized (this) {
            if (cachedFlightSqlPort == null) {
                cachedFlightSqlPort = discoverFlightSqlPort(session);
            }
            return cachedFlightSqlPort;
        }
    }

    static int findArrowFlightSqlPortColumnIndex(List<String> columnNames)
    {
        for (int index = 0; index < columnNames.size(); index++) {
            if (ARROW_FLIGHT_SQL_PORT_COLUMN.equalsIgnoreCase(columnNames.get(index))) {
                return index;
            }
        }
        return -1;
    }

    private int discoverFlightSqlPort(ConnectorSession session)
    {
        try (Connection connection = openConnection(session);
                PreparedStatement statement = connection.prepareStatement(SHOW_FRONTENDS_SQL);
                ResultSet resultSet = statement.executeQuery()) {
            List<String> columnNames = getColumnNames(resultSet.getMetaData());
            int portColumnIndex = findArrowFlightSqlPortColumnIndex(columnNames);
            if (portColumnIndex < 0) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Doris SHOW FRONTENDS result does not expose ArrowFlightSqlPort");
            }

            while (resultSet.next()) {
                int port = resultSet.getInt(portColumnIndex + 1);
                if (!resultSet.wasNull() && port > 0) {
                    return port;
                }
            }
        }
        catch (SQLException e) {
            throw DorisJdbcConnectionFactory.jdbcOperationFailed("Failed to auto-discover Doris Flight SQL port", e);
        }

        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to auto-discover Doris Flight SQL port from SHOW FRONTENDS");
    }

    private Connection openConnection(ConnectorSession session)
            throws SQLException
    {
        if (session == null) {
            return connectionFactory.openConnection();
        }
        return connectionFactory.openConnection(session);
    }

    private static List<String> getColumnNames(ResultSetMetaData metadata)
            throws SQLException
    {
        List<String> columnNames = new ArrayList<>(metadata.getColumnCount());
        for (int index = 1; index <= metadata.getColumnCount(); index++) {
            columnNames.add(metadata.getColumnName(index));
        }
        return List.copyOf(columnNames);
    }
}
