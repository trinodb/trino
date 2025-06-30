/**
 * Unpublished work.
 * Copyright 2025 by Teradata Corporation. All rights reserved.
 * TERADATA CORPORATION CONFIDENTIAL AND TRADE SECRET
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
import io.trino.plugin.jdbc.credential.CredentialProvider;

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

        // Set Teradata JDBC-specific connection properties
        // TODO: Retire all properties that could be included in the connection URL instead
        // TODO: Incorporate query banding support if needed

        return DriverConnectionFactory.builder(DriverManager.getDriver(config.getConnectionUrl()), config.getConnectionUrl(), credentialProvider).setConnectionProperties(connectionProperties).setOpenTelemetry(openTelemetry).build();
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
    }
}
