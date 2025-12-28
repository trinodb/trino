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
import io.trino.plugin.jdbc.credential.CredentialPropertiesProvider;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.credential.DefaultCredentialPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class DriverConnectionFactory
        implements ConnectionFactory
{
    private final Driver driver;
    private final String connectionUrl;
    private final Properties sessionProperties;
    private final CredentialPropertiesProvider credentialPropertiesProvider;
    private final TracingDataSource dataSource;

    public DriverConnectionFactory(
            Driver driver,
            String connectionUrl,
            Properties sessionProperties,
            CredentialPropertiesProvider credentialPropertiesProvider,
            OpenTelemetry openTelemetry)
    {
        this.driver = requireNonNull(driver, "driver is null");
        this.connectionUrl = requireNonNull(connectionUrl, "connectionUrl is null");
        this.sessionProperties = new Properties();
        this.sessionProperties.putAll(requireNonNull(sessionProperties, "sessionProperties is null"));
        this.credentialPropertiesProvider = requireNonNull(credentialPropertiesProvider, "credentialPropertiesProvider is null");
        this.dataSource = new TracingDataSource(requireNonNull(openTelemetry, "openTelemetry is null"), driver, connectionUrl);
    }

    @Override
    public Connection openConnection(ConnectorSession session)
            throws SQLException
    {
        return openConnection(session, new Properties());
    }

    @Override
    public Connection openConnection(ConnectorSession session, Properties connectionProperties)
            throws SQLException
    {
        Properties properties = getConnectionProperties(session.getIdentity(), connectionProperties);
        Connection connection = dataSource.getConnection(properties);
        checkState(connection != null, "Driver returned null connection, make sure the connection URL '%s' is valid for the driver %s", connectionUrl, driver);
        return connection;
    }

    private Properties getConnectionProperties(ConnectorIdentity identity, Properties connectionProperties)
    {
        Properties properties = new Properties();
        properties.putAll(sessionProperties);
        properties.putAll(credentialPropertiesProvider.getCredentialProperties(identity));
        // Connection properties take precedence over session and credential properties.
        properties.putAll(connectionProperties);
        return properties;
    }

    public static Builder builder(Driver driver, String connectionUrl, CredentialProvider credentialProvider)
    {
        return new Builder(driver, connectionUrl, credentialProvider);
    }

    public static class Builder
    {
        private final Driver driver;
        private final String connectionUrl;
        private Properties connectionProperties = new Properties();
        private CredentialPropertiesProvider credentialPropertiesProvider;
        private OpenTelemetry openTelemetry = OpenTelemetry.noop();

        private Builder(Driver driver, String connectionUrl, CredentialProvider credentialProvider)
        {
            this.driver = requireNonNull(driver, "driver is null");
            this.connectionUrl = requireNonNull(connectionUrl, "connectionUrl is null");
            this.credentialPropertiesProvider = new DefaultCredentialPropertiesProvider(requireNonNull(credentialProvider, "credentialProvider is null"));
        }

        public Builder setConnectionProperties(Properties connectionProperties)
        {
            this.connectionProperties = connectionProperties;
            return this;
        }

        public Builder setCredentialPropertiesProvider(CredentialPropertiesProvider credentialPropertiesProvider)
        {
            this.credentialPropertiesProvider = credentialPropertiesProvider;
            return this;
        }

        public Builder setOpenTelemetry(OpenTelemetry openTelemetry)
        {
            this.openTelemetry = openTelemetry;
            return this;
        }

        public DriverConnectionFactory build()
        {
            return new DriverConnectionFactory(driver, connectionUrl, connectionProperties, credentialPropertiesProvider, openTelemetry);
        }
    }
}
