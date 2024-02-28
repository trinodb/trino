/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.stargate;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.jdbc.TrinoDriver;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.credential.CredentialProviderModule;

import java.io.File;
import java.util.Optional;
import java.util.Properties;

import static com.starburstdata.trino.plugin.stargate.StargateConfig.PASSWORD;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class StargateAuthenticationModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(conditionalModule(
                StargateConfig.class,
                config -> PASSWORD.equalsIgnoreCase(config.getAuthenticationType()),
                new PasswordModule()));
    }

    private static class PasswordModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            install(new CredentialProviderModule());
            configBinder(binder).bindConfig(StargateCredentialConfig.class);
            binder.bind(StargateCatalogIdentityFactory.class)
                    .to(PasswordCatalogIdentityFactory.class)
                    .in(Scopes.SINGLETON);
        }

        @Provides
        @Singleton
        @TransportConnectionFactory
        public ConnectionFactory getConnectionFactory(
                BaseJdbcConfig config,
                StargateConfig connectorConfig,
                StargateSslConfig sslConfig,
                CredentialProvider credentialProvider)
        {
            Properties properties = new Properties();
            if (connectorConfig.isSslEnabled()) {
                setSslProperties(properties, sslConfig);
            }

            return new DriverConnectionFactory(new TrinoDriver(), config.getConnectionUrl(), properties, credentialProvider);
        }
    }

    private static void setSslProperties(Properties properties, StargateSslConfig sslConfig)
    {
        properties.setProperty("SSL", "true");
        setOptionalProperty(properties, "SSLTrustStorePath", sslConfig.getTruststoreFile().map(File::getAbsolutePath));
        setOptionalProperty(properties, "SSLTrustStorePassword", sslConfig.getTruststorePassword());
        setOptionalProperty(properties, "SSLTrustStoreType", sslConfig.getTruststoreType());
    }

    private static void setOptionalProperty(Properties properties, String propertyKey, Optional<String> maybeValue)
    {
        maybeValue.ifPresent(value -> properties.setProperty(propertyKey, value));
    }
}
