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
package io.trino.plugin.jdbc;

import io.trino.spi.connector.ConnectorSession;

import java.sql.Connection;
import java.sql.SQLException;

import static java.util.Objects.requireNonNull;

public final class ConfiguringConnectionFactory
        implements ConnectionFactory
{
    private final ConnectionFactory delegate;
    private final Configurator configurator;

    public ConfiguringConnectionFactory(ConnectionFactory delegate, Configurator configurator)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.configurator = requireNonNull(configurator, "configurator is null");
    }

    @Override
    public Connection openConnection(ConnectorSession session)
            throws SQLException
    {
        Connection connection = delegate.openConnection(session);
        try {
            configurator.configure(connection);
        }
        catch (SQLException | RuntimeException e) {
            try (connection) {
                throw e;
            }
        }
        return connection;
    }

    @Override
    public void close()
            throws SQLException
    {
        delegate.close();
    }

    @FunctionalInterface
    public interface Configurator
    {
        void configure(Connection connection)
                throws SQLException;
    }
}
