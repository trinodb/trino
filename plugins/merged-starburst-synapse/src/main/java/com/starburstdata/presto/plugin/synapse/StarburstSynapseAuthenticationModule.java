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
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import com.starburstdata.presto.plugin.jdbc.auth.AuthenticationBasedIdentityCacheMappingModule;
import com.starburstdata.presto.plugin.jdbc.auth.ForImpersonation;
import com.starburstdata.presto.plugin.jdbc.auth.PasswordPassThroughModule;
import com.starburstdata.presto.plugin.toolkit.authtolocal.AuthToLocalModule;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.credential.CredentialProviderModule;
import io.trino.plugin.sqlserver.SqlServerConfig;
import io.trino.plugin.sqlserver.SqlServerConnectionFactory;

import java.util.Properties;

import static com.google.common.base.Preconditions.checkState;
import static com.google.inject.Scopes.SINGLETON;
import static com.starburstdata.presto.plugin.jdbc.auth.NoImpersonationModule.noImpersonationModuleWithCredentialProvider;
import static com.starburstdata.presto.plugin.synapse.SynapseConfig.SynapseAuthenticationType.ACTIVE_DIRECTORY_PASSWORD;
import static com.starburstdata.presto.plugin.synapse.SynapseConfig.SynapseAuthenticationType.ACTIVE_DIRECTORY_PASSWORD_PASS_THROUGH;
import static com.starburstdata.presto.plugin.synapse.SynapseConfig.SynapseAuthenticationType.PASSWORD;
import static com.starburstdata.presto.plugin.synapse.SynapseConfig.SynapseAuthenticationType.PASSWORD_PASS_THROUGH;
import static io.airlift.configuration.ConditionalModule.conditionalModule;

public class StarburstSynapseAuthenticationModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(conditionalModule(
                SynapseConfig.class,
                config -> config.getAuthenticationType() == ACTIVE_DIRECTORY_PASSWORD,
                moduleBinder -> {
                    install(new CredentialProviderModule());
                    install(new ActiveDirectoryPasswordModule());
                    install(conditionalModule(
                            SynapseConfig.class,
                            SynapseConfig::isImpersonationEnabled,
                            new ImpersonationModule(),
                            noImpersonationModuleWithCredentialProvider()));
                }));

        install(conditionalModule(
                SynapseConfig.class,
                config -> config.getAuthenticationType() == PASSWORD,
                moduleBinder -> {
                    install(new CredentialProviderModule());
                    install(new PasswordModule());
                    install(conditionalModule(
                            SynapseConfig.class,
                            SynapseConfig::isImpersonationEnabled,
                            new ImpersonationModule(),
                            noImpersonationModuleWithCredentialProvider()));
                }));

        install(conditionalModule(
                SynapseConfig.class,
                config -> config.getAuthenticationType() == ACTIVE_DIRECTORY_PASSWORD_PASS_THROUGH,
                moduleBinder -> {
                    install(new PasswordPassThroughModule<>(SynapseConfig.class, SynapseConfig::isImpersonationEnabled));
                    install(new ActiveDirectoryPasswordModule());
                    moduleBinder.bind(ConnectionFactory.class)
                            .annotatedWith(ForBaseJdbc.class)
                            .to(Key.get(ConnectionFactory.class, ForImpersonation.class))
                            .in(SINGLETON);
                }));

        install(conditionalModule(
                SynapseConfig.class,
                config -> config.getAuthenticationType() == PASSWORD_PASS_THROUGH,
                moduleBinder -> {
                    install(new PasswordPassThroughModule<>(SynapseConfig.class, SynapseConfig::isImpersonationEnabled));
                    install(new PasswordModule());
                    moduleBinder.bind(ConnectionFactory.class)
                            .annotatedWith(ForBaseJdbc.class)
                            .to(Key.get(ConnectionFactory.class, ForImpersonation.class))
                            .in(SINGLETON);
                }));
    }

    private static class ImpersonationModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.install(new AuthToLocalModule());
            binder.install(new AuthenticationBasedIdentityCacheMappingModule());
            binder.bind(ConnectionFactory.class).annotatedWith(ForBaseJdbc.class).to(SynapseImpersonatingConnectionFactory.class).in(Scopes.SINGLETON);
        }
    }

    private static class ActiveDirectoryPasswordModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
        }

        @Provides
        @Singleton
        @ForImpersonation
        public ConnectionFactory getConnectionFactory(
                BaseJdbcConfig baseJdbcConfig,
                SqlServerConfig sqlServerConfig,
                CredentialProvider credentialProvider)
        {
            checkState(
                    !baseJdbcConfig.getConnectionUrl().contains("authentication="),
                    "Cannot specify 'authentication' parameter in JDBC URL when using Active Directory password authentication: %s",
                    baseJdbcConfig.getConnectionUrl());
            Properties properties = new Properties();
            properties.setProperty("authentication", "ActiveDirectoryPassword");

            return new SqlServerConnectionFactory(
                    new DriverConnectionFactory(
                            new SQLServerDriver(),
                            baseJdbcConfig.getConnectionUrl(),
                            properties,
                            credentialProvider),
                    sqlServerConfig.isSnapshotIsolationDisabled());
        }
    }

    private static class PasswordModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
        }

        @Provides
        @Singleton
        @ForImpersonation
        public ConnectionFactory getConnectionFactory(BaseJdbcConfig baseJdbcConfig, SqlServerConfig sqlServerConfig, CredentialProvider credentialProvider)
        {
            return new SqlServerConnectionFactory(
                    new DriverConnectionFactory(new SQLServerDriver(), baseJdbcConfig, credentialProvider),
                    sqlServerConfig.isSnapshotIsolationDisabled());
        }
    }
}
