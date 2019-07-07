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

import io.prestosql.plugin.jdbc.credential.CredentialProvider;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class DriverConnectionFactory
        implements ConnectionFactory
{
    private final Driver driver;
    private final String connectionUrl;
    private final Properties connectionProperties;
    private final Optional<String> userCredentialName;
    private final Optional<String> passwordCredentialName;

    public DriverConnectionFactory(Driver driver, BaseJdbcConfig config, CredentialProvider credentialProvider)
    {
        this(
                driver,
                config.getConnectionUrl(),
                Optional.ofNullable(config.getUserCredentialName()),
                Optional.ofNullable(config.getPasswordCredentialName()),
                basicConnectionProperties(credentialProvider));
    }

    public static Properties basicConnectionProperties(CredentialProvider credentialProvider)
    {
        Properties connectionProperties = new Properties();
        if (credentialProvider.getConnectionUser(Optional.empty()).isPresent()) {
            connectionProperties.setProperty("user", credentialProvider.getConnectionUser(Optional.empty()).get());
        }
        if (credentialProvider.getConnectionPassword(Optional.empty()).isPresent()) {
            connectionProperties.setProperty("password", credentialProvider.getConnectionPassword(Optional.empty()).get());
        }
        return connectionProperties;
    }

    public DriverConnectionFactory(Driver driver, String connectionUrl, Optional<String> userCredentialName, Optional<String> passwordCredentialName, Properties connectionProperties)
    {
        this.driver = requireNonNull(driver, "driver is null");
        this.connectionUrl = requireNonNull(connectionUrl, "connectionUrl is null");
        this.connectionProperties = new Properties();
        this.connectionProperties.putAll(requireNonNull(connectionProperties, "basicConnectionProperties is null"));
        this.userCredentialName = requireNonNull(userCredentialName, "userCredentialName is null");
        this.passwordCredentialName = requireNonNull(passwordCredentialName, "passwordCredentialName is null");
    }

    @Override
    public Connection openConnection(JdbcIdentity identity)
            throws SQLException
    {
        userCredentialName.ifPresent(credentialName -> setConnectionProperty(connectionProperties, identity.getExtraCredentials(), credentialName, "user"));
        passwordCredentialName.ifPresent(credentialName -> setConnectionProperty(connectionProperties, identity.getExtraCredentials(), credentialName, "password"));

        Connection connection = driver.connect(connectionUrl, connectionProperties);
        checkState(connection != null, "Driver returned null connection");
        return connection;
    }

    private static void setConnectionProperty(Properties connectionProperties, Map<String, String> extraCredentials, String credentialName, String propertyName)
    {
        String value = extraCredentials.get(credentialName);
        if (value != null) {
            connectionProperties.setProperty(propertyName, value);
        }
    }
}
