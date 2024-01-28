/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.saphana;

import com.google.inject.Binder;
import com.google.inject.BindingAnnotation;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.sap.db.jdbc.Driver;
import com.starburstdata.trino.plugin.jdbc.JdbcConnectionPoolConfig;
import com.starburstdata.trino.plugin.jdbc.PoolingConnectionFactory;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ExtraCredentialsBasedIdentityCacheMappingModule;
import io.trino.plugin.jdbc.IdentityCacheMapping;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.credential.CredentialProviderModule;
import io.trino.plugin.jdbc.credential.DefaultCredentialPropertiesProvider;
import io.trino.spi.catalog.CatalogName;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Properties;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class SapHanaAuthenticationModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(new PasswordModule());
    }

    private static class PasswordModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            install(new CredentialProviderModule());
            configBinder(binder).bindConfig(JdbcConnectionPoolConfig.class);
            install(new ExtraCredentialsBasedIdentityCacheMappingModule());
        }

        @Provides
        @Singleton
        @DefaultSapHanaBinding
        public ConnectionFactory getConnectionFactory(
                CatalogName catalogName,
                BaseJdbcConfig config,
                JdbcConnectionPoolConfig poolConfig,
                CredentialProvider credentialProvider,
                IdentityCacheMapping identityCacheMapping,
                OpenTelemetry openTelemetry)
        {
            if (poolConfig.isConnectionPoolEnabled()) {
                return new PoolingConnectionFactory(catalogName.toString(), Driver.class, config, poolConfig, credentialProvider, identityCacheMapping);
            }
            return new DriverConnectionFactory(new Driver(), config.getConnectionUrl(), new Properties(), new DefaultCredentialPropertiesProvider(credentialProvider), openTelemetry);
        }
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD})
    @BindingAnnotation
    public @interface DefaultSapHanaBinding {}
}
