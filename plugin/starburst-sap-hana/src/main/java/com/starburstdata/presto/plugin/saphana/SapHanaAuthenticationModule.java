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
import com.starburstdata.presto.plugin.jdbc.auth.PasswordPassThroughModule;
import com.starburstdata.trino.plugins.jdbc.JdbcConnectionPoolConfig;
import com.starburstdata.trino.plugins.jdbc.PoolingConnectionFactory;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ExtraCredentialsBasedIdentityCacheMappingModule;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.IdentityCacheMapping;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.credential.CredentialProviderModule;

import static com.starburstdata.presto.plugin.saphana.SapHanaAuthenticationType.PASSWORD;
import static com.starburstdata.presto.plugin.saphana.SapHanaAuthenticationType.PASSWORD_PASS_THROUGH;
import static com.starburstdata.presto.plugin.toolkit.guice.Modules.enumConditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class SapHanaAuthenticationModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(enumConditionalModule(
                SapHanaAuthenticationConfig.class,
                SapHanaAuthenticationConfig::getAuthenticationType,
                PASSWORD, new PasswordModule(),
                PASSWORD_PASS_THROUGH, new SapHanaPasswordPassThroughModule()));
    }

    private class PasswordModule
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
        @ForBaseJdbc
        public ConnectionFactory getConnectionFactory(CatalogName catalogName, BaseJdbcConfig config, JdbcConnectionPoolConfig poolConfig, CredentialProvider credentialProvider, IdentityCacheMapping identityCacheMapping)
        {
            return createBasicConnectionFactory(catalogName, config, poolConfig, credentialProvider, identityCacheMapping);
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
        }

        @Provides
        @Singleton
        @ForBaseJdbc
        public ConnectionFactory getConnectionFactory(CatalogName catalogName, BaseJdbcConfig config, JdbcConnectionPoolConfig poolConfig, CredentialProvider credentialProvider, IdentityCacheMapping identityCacheMapping)
        {
            return createBasicConnectionFactory(catalogName, config, poolConfig, credentialProvider, identityCacheMapping);
        }
    }

    private ConnectionFactory createBasicConnectionFactory(CatalogName catalogName, BaseJdbcConfig config, JdbcConnectionPoolConfig poolConfig, CredentialProvider credentialProvider, IdentityCacheMapping identityCacheMapping)
    {
        if (poolConfig.isConnectionPoolEnabled()) {
            return new PoolingConnectionFactory(catalogName.toString(), Driver.class, config, poolConfig, credentialProvider, identityCacheMapping);
        }
        return new DriverConnectionFactory(new Driver(), config, credentialProvider);
    }
}
