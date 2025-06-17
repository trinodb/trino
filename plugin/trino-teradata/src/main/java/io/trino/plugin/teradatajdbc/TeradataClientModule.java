/**
 * Unpublished work.
 * Copyright 2025 by Teradata Corporation. All rights reserved
 * TERADATA CORPORATION CONFIDENTIAL AND TRADE SECRET
 */

package io.trino.plugin.teradatajdbc;

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

public class TeradataClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    public void setup(Binder binder)
    {
        configBinder(binder).bindConfig(TeradataConfig.class);
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(TeradataClient.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory getConnectionFactory(BaseJdbcConfig config, TeradataConfig teradataConfig, CredentialProvider credentialProvider, OpenTelemetry openTelemetry)
            throws SQLException
    {
        Properties connectionProperties = new Properties();

        // set connection TeradataJDBC specific properties
        // TODO retire all properties that could just be part of connection url
        connectionProperties.setProperty("TMODE", teradataConfig.getTeradataMode().name());
        connectionProperties.setProperty("CHARSET", teradataConfig.getSessionCharacterSet());
        connectionProperties.setProperty("LOGMECH", teradataConfig.getLogonMechanism());
        // optional TeradataJDBC properties
        if (teradataConfig.getDefaultDatabase() != null) {
            connectionProperties.setProperty("DATABASE", teradataConfig.getDefaultDatabase());
        }

        // TODO incorporate queryband
        return DriverConnectionFactory.builder(DriverManager.getDriver(config.getConnectionUrl()), config.getConnectionUrl(), credentialProvider)
                .setConnectionProperties(connectionProperties)
                .setOpenTelemetry(openTelemetry)
                .build();
    }
}
