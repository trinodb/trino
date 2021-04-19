/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.synapse;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import com.starburstdata.presto.plugin.jdbc.auth.ForAuthentication;
import com.starburstdata.presto.plugin.jdbc.authtolocal.AuthToLocalModule;
import com.starburstdata.presto.plugin.sqlserver.SqlServerImpersonatingConnectionFactory;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.credential.CredentialProviderModule;

import java.util.Properties;

import static com.google.common.base.Preconditions.checkState;
import static com.starburstdata.presto.plugin.jdbc.auth.NoImpersonationModule.noImpersonationModuleWithCredentialProvider;
import static com.starburstdata.presto.plugin.synapse.SynapseConfig.SynapseAuthenticationType.ACTIVE_DIRECTORY_PASSWORD;
import static com.starburstdata.presto.plugin.synapse.SynapseConfig.SynapseAuthenticationType.PASSWORD;
import static io.airlift.configuration.ConditionalModule.installModuleIf;

public class StarburstSynapseAuthenticationModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(installModuleIf(
                SynapseConfig.class,
                SynapseConfig::isImpersonationEnabled,
                new ImpersonationModule(),
                noImpersonationModuleWithCredentialProvider()));

        install(installModuleIf(
                SynapseConfig.class,
                config -> config.getAuthenticationType() == ACTIVE_DIRECTORY_PASSWORD,
                new ActiveDirectoryPasswordModule()));

        install(installModuleIf(
                SynapseConfig.class,
                config -> config.getAuthenticationType() == PASSWORD,
                new PasswordModule()));
    }

    private static class ImpersonationModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.install(new AuthToLocalModule());
            binder.bind(ConnectionFactory.class).annotatedWith(ForBaseJdbc.class).to(SqlServerImpersonatingConnectionFactory.class).in(Scopes.SINGLETON);
        }
    }

    private class ActiveDirectoryPasswordModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            install(new CredentialProviderModule());
        }

        @Provides
        @Singleton
        @ForAuthentication
        public ConnectionFactory getConnectionFactory(
                BaseJdbcConfig baseJdbcConfig,
                CredentialProvider credentialProvider)
        {
            checkState(
                    !baseJdbcConfig.getConnectionUrl().contains("authentication="),
                    "Cannot specify 'authentication' parameter in JDBC URL when using Active Directory password authentication: %s",
                    baseJdbcConfig.getConnectionUrl());
            Properties properties = new Properties();
            properties.setProperty("authentication", "ActiveDirectoryPassword");

            return new DriverConnectionFactory(
                    new SQLServerDriver(),
                    baseJdbcConfig.getConnectionUrl(),
                    properties,
                    credentialProvider);
        }
    }

    private class PasswordModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            install(new CredentialProviderModule());
        }

        @Provides
        @Singleton
        @ForAuthentication
        public ConnectionFactory getConnectionFactory(BaseJdbcConfig baseJdbcConfig, CredentialProvider credentialProvider)
        {
            return new DriverConnectionFactory(new SQLServerDriver(), baseJdbcConfig, credentialProvider);
        }
    }
}
