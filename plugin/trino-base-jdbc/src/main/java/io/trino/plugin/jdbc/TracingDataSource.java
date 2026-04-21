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

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.instrumentation.jdbc.datasource.JdbcTelemetry;
import io.trino.spi.TrinoException;

import javax.sql.DataSource;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static java.util.Objects.requireNonNull;

public class TracingDataSource
{
    private final JdbcTelemetry jdbcTelemetry;
    private final Driver driver;
    private final String connectionUrl;

    public TracingDataSource(OpenTelemetry openTelemetry, Driver driver, String connectionUrl)
    {
        this.jdbcTelemetry = JdbcTelemetry.builder(requireNonNull(openTelemetry, "openTelemetry is null")).build();
        this.driver = requireNonNull(driver, "driver is null");
        this.connectionUrl = requireNonNull(connectionUrl, "connectionUrl is null");
    }

    public Connection getConnection(Properties properties)
            throws SQLException
    {
        DataSource dataSource = new JdbcDataSource(driver, connectionUrl, properties);
        return jdbcTelemetry.wrap(dataSource).getConnection();
    }

    private static class JdbcDataSource
            implements DataSource
    {
        private final Driver driver;
        private final String connectionUrl;
        private final Properties properties;

        public JdbcDataSource(Driver driver, String connectionUrl, Properties properties)
        {
            this.driver = requireNonNull(driver, "driver is null");
            this.connectionUrl = requireNonNull(connectionUrl, "connectionUrl is null");
            this.properties = requireNonNull(properties, "properties is null");
        }

        @Override
        public Connection getConnection()
                throws SQLException
        {
            Connection connection = driver.connect(connectionUrl, properties);
            if (connection == null) {
                throw new TrinoException(JDBC_ERROR, "Driver returned null connection, make sure the connection URL '%s' and credentials are valid".formatted(connectionUrl));
            }
            return connection;
        }

        @Override
        public Connection getConnection(String username, String password)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public PrintWriter getLogWriter()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setLogWriter(PrintWriter out)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setLoginTimeout(int seconds)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getLoginTimeout()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Logger getParentLogger()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T unwrap(Class<T> iface)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isWrapperFor(Class<?> iface)
        {
            throw new UnsupportedOperationException();
        }
    }
}
