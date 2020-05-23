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
package io.prestosql.plugin.jdbc;

import io.prestosql.plugin.jdbc.credential.CredentialPropertiesProvider;
import io.prestosql.plugin.jdbc.credential.CredentialProvider;
import io.prestosql.plugin.jdbc.credential.DefaultCredentialPropertiesProvider;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class DriverConnectionFactory
        implements ConnectionFactory
{
    private final Driver driver;
    private final Function<JdbcIdentity, String> connectionUrl;
    private final Properties connectionProperties;
    private final CredentialPropertiesProvider credentialPropertiesProvider;

    public DriverConnectionFactory(Driver driver, BaseJdbcConfig config, CredentialProvider credentialProvider)
    {
        this(driver,
                config.getConnectionUrl(),
                new Properties(),
                credentialProvider);
    }

    public DriverConnectionFactory(Driver driver, String connectionUrl, Properties connectionProperties, CredentialProvider credentialProvider)
    {
        this(driver, connectionUrl, connectionProperties, new DefaultCredentialPropertiesProvider(credentialProvider));
    }

    public DriverConnectionFactory(Driver driver, String connectionUrl, Properties connectionProperties, CredentialPropertiesProvider credentialPropertiesProvider)
    {
        this(driver, jdbcIdentity -> connectionUrl, connectionProperties, credentialPropertiesProvider);
    }

    public DriverConnectionFactory(
            Driver driver,
            Function<JdbcIdentity, String> connectionUrl,
            Properties connectionProperties,
            CredentialPropertiesProvider credentialPropertiesProvider)
    {
        this.driver = requireNonNull(driver, "driver is null");
        this.connectionUrl = requireNonNull(connectionUrl, "connectionUrl is null");
        this.connectionProperties = new Properties();
        this.connectionProperties.putAll(requireNonNull(connectionProperties, "connectionProperties is null"));
        this.credentialPropertiesProvider = requireNonNull(credentialPropertiesProvider, "credentialPropertiesProvider is null");
    }

    @Override
    public Connection openConnection(JdbcIdentity identity)
            throws SQLException
    {
        Properties properties = getCredentialProperties(identity);
        Connection connection = driver.connect(connectionUrl.apply(identity), properties);
        checkState(connection != null, "Driver returned null connection");
        return connection;
    }

    private Properties getCredentialProperties(JdbcIdentity identity)
    {
        Properties properties = new Properties();
        properties.putAll(connectionProperties);
        properties.putAll(credentialPropertiesProvider.getCredentialProperties(identity));
        return properties;
    }
}
