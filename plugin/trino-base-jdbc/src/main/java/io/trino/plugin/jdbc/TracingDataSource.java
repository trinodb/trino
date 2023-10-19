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
import io.opentelemetry.instrumentation.jdbc.datasource.OpenTelemetryDataSource;

import javax.sql.DataSource;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

import static java.util.Objects.requireNonNull;

public class TracingDataSource
{
    private final OpenTelemetry openTelemetry;
    private final Driver driver;
    private final String connectionUrl;

    public TracingDataSource(OpenTelemetry openTelemetry, Driver driver, String connectionUrl)
    {
        this.openTelemetry = requireNonNull(openTelemetry, "openTelemetry is null");
        this.driver = requireNonNull(driver, "driver is null");
        this.connectionUrl = requireNonNull(connectionUrl, "connectionUrl is null");
    }

    public Connection getConnection(Properties properties)
            throws SQLException
    {
        DataSource dataSource = new JdbcDataSource(driver, connectionUrl, properties);
        try (OpenTelemetryDataSource openTelemetryDataSource = new OpenTelemetryDataSource(dataSource, openTelemetry)) {
            return openTelemetryDataSource.getConnection();
        }
        catch (Exception e) {
            throw new SQLException(e);
        }
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
            return driver.connect(connectionUrl, properties);
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
