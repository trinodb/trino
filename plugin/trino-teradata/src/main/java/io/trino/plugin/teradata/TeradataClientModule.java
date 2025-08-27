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

package io.trino.plugin.teradata;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcJoinPushdownSupportModule;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.credential.CredentialProvider;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static io.airlift.configuration.ConfigBinder.configBinder;

/**
 * Guice module for configuring the Teradata JDBC client integration.
 * <p>
 * This module binds the {@link TeradataConfig} configuration class,
 * the Teradata-specific {@link JdbcClient} implementation, and
 * provides a {@link ConnectionFactory} tailored for Teradata connections.
 * </p>
 * <p>
 * The {@code ConnectionFactory} is built using Teradata-specific JDBC connection properties,
 * including session mode, character set, and authentication mechanism.
 * </p>
 * <p>
 * This class uses Google Guice for dependency injection and
 * OpenTelemetry for instrumentation of JDBC connections.
 * </p>
 *
 * @see TeradataConfig
 * @see TeradataClient
 */
public class TeradataClientModule
        extends AbstractConfigurationAwareModule
{
    /**
     * Provides a singleton {@link ConnectionFactory} for Teradata JDBC connections.
     * <p>
     * This factory is configured with Teradata-specific JDBC properties:
     * <ul>
     *   <li>{@code TMODE} - Teradata transaction mode</li>
     *   <li>{@code CHARSET} - Session character set</li>
     *   <li>{@code LOGMECH} - Logon mechanism</li>
     *   <li>{@code DATABASE} - Default database (optional)</li>
     * </ul>
     * </p>
     * <p>
     * Uses the {@link DriverConnectionFactory} builder to create the connection factory,
     * supplying the driver obtained from the JDBC connection URL and credentials via {@link CredentialProvider}.
     * OpenTelemetry instrumentation is enabled on the connection factory.
     * </p>
     *
     * @param config base JDBC configuration with connection URL
     * @param teradataConfig Teradata-specific configuration properties
     * @param credentialProvider provider for database credentials
     * @param openTelemetry OpenTelemetry instance for instrumentation
     * @return a singleton {@link ConnectionFactory} for Teradata connections
     * @throws SQLException if the JDBC driver cannot be loaded or initialized
     */
    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory getConnectionFactory(BaseJdbcConfig config, TeradataConfig teradataConfig, CredentialProvider credentialProvider, OpenTelemetry openTelemetry)
            throws SQLException
    {
        Properties connectionProperties = new Properties();
        Driver driver = DriverManager.getDriver(config.getConnectionUrl());
        String longMech = LogonMechanism.fromString(teradataConfig.getLogMech()).getMechanism();
        connectionProperties.put("LOGMECH", longMech);
        String clientId;
        switch (longMech) {
            case "TD2":
                break;
            case "BEARER":
                clientId = teradataConfig.getOidcClientId();
                if (clientId != null && !clientId.isEmpty()) {
                    connectionProperties.put("oidc_clientid", clientId);
                }
                String privateKey = teradataConfig.getOidcJWSPrivateKey();
                if (privateKey != null && !privateKey.isEmpty()) {
                    connectionProperties.put("jws_private_key", privateKey);
                }
                String certificate = teradataConfig.getOidcJWSCertificate();
                if (certificate != null && !certificate.isEmpty()) {
                    connectionProperties.put("jws_cert", certificate);
                }
                break;
            case "JWT":
                String token = teradataConfig.getOidcJwtToken();
                if (token != null && !token.trim().isEmpty()) {
                    connectionProperties.put("LOGDATA", token);
                }
                break;
            case "SECRET":
                clientId = teradataConfig.getOidcClientId();
                if (clientId != null && !clientId.isEmpty()) {
                    connectionProperties.put("oidc_clientid", clientId);
                }
                String clientSecret = teradataConfig.getOidcClientSecret();
                if (clientSecret != null && !clientSecret.isEmpty()) {
                    connectionProperties.put("LOGDATA", clientSecret);
                }
                break;
            default:
                throw new IllegalArgumentException("Unsupported logon mechanism: " + longMech);
        }
        return DriverConnectionFactory.builder(driver, config.getConnectionUrl(), credentialProvider).setConnectionProperties(connectionProperties).setOpenTelemetry(openTelemetry).build();
    }

    /**
     * Configures Guice bindings for the Teradata connector.
     * <p>
     * Binds the {@link TeradataConfig} configuration,
     * and binds {@link JdbcClient} annotated with {@link ForBaseJdbc} to {@link TeradataClient}.
     * The client is scoped as a singleton.
     * </p>
     *
     * @param binder Guice binder used to register bindings
     */
    @Override
    public void setup(Binder binder)
    {
        configBinder(binder).bindConfig(TeradataConfig.class);
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(TeradataClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);
        install(new JdbcJoinPushdownSupportModule());
    }
}
