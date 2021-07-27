/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.saphana;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.sap.db.jdbc.Driver;
import com.starburstdata.presto.plugin.jdbc.JdbcConnectionPoolConfig;
import com.starburstdata.presto.plugin.jdbc.PoolingConnectionFactory;
import com.starburstdata.presto.plugin.jdbc.auth.AuthenticationBasedIdentityCacheMappingModule;
import com.starburstdata.presto.plugin.jdbc.auth.PasswordPassThroughModule;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ExtraCredentialsBasedJdbcIdentityCacheMappingModule;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.credential.CredentialProviderModule;

import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class SapHanaAuthenticationModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(conditionalModule(
                SapHanaAuthenticationConfig.class,
                config -> config.getAuthenticationType() == SapHanaAuthenticationType.PASSWORD,
                new PasswordModule()));

        install(conditionalModule(
                SapHanaAuthenticationConfig.class,
                config -> config.getAuthenticationType() == SapHanaAuthenticationType.PASSWORD_PASS_THROUGH,
                new SapHanaPasswordPassThroughModule()));
    }

    private class PasswordModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            install(new CredentialProviderModule());
            configBinder(binder).bindConfig(JdbcConnectionPoolConfig.class);
            install(new ExtraCredentialsBasedJdbcIdentityCacheMappingModule());
        }

        @Provides
        @Singleton
        @ForBaseJdbc
        public ConnectionFactory getConnectionFactory(CatalogName catalogName, BaseJdbcConfig config, JdbcConnectionPoolConfig poolConfig, CredentialProvider credentialProvider)
        {
            return createBasicConnectionFactory(catalogName, config, poolConfig, credentialProvider);
        }
    }

    private class SapHanaPasswordPassThroughModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            configBinder(binder).bindConfig(JdbcConnectionPoolConfig.class);
            install(new PasswordPassThroughModule<>(SapHanaAuthenticationConfig.class, config -> false));
            install(new AuthenticationBasedIdentityCacheMappingModule());
        }

        @Provides
        @Singleton
        @ForBaseJdbc
        public ConnectionFactory getConnectionFactory(CatalogName catalogName, BaseJdbcConfig config, JdbcConnectionPoolConfig poolConfig, CredentialProvider credentialProvider)
        {
            return createBasicConnectionFactory(catalogName, config, poolConfig, credentialProvider);
        }
    }

    private ConnectionFactory createBasicConnectionFactory(CatalogName catalogName, BaseJdbcConfig config, JdbcConnectionPoolConfig poolConfig, CredentialProvider credentialProvider)
    {
        if (poolConfig.isConnectionPoolEnabled()) {
            return new PoolingConnectionFactory(catalogName.toString(), Driver.class, config, poolConfig, credentialProvider);
        }
        return new DriverConnectionFactory(new Driver(), config, credentialProvider);
    }
}
