/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.starburstremote;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.starburstdata.presto.plugin.jdbc.auth.PasswordPassThroughModule;
import com.starburstdata.presto.plugin.jdbc.authtolocal.AuthToLocal;
import com.starburstdata.presto.plugin.jdbc.authtolocal.AuthToLocalModule;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.jdbc.TrinoDriver;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.credential.CredentialConfig;
import io.trino.plugin.jdbc.credential.CredentialPropertiesProvider;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.credential.CredentialProviderModule;
import io.trino.plugin.jdbc.credential.DefaultCredentialPropertiesProvider;
import io.trino.plugin.jdbc.credential.StaticCredentialProvider;

import java.io.File;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkState;
import static com.starburstdata.trino.plugin.starburstremote.StarburstRemoteAuthenticationType.KERBEROS;
import static com.starburstdata.trino.plugin.starburstremote.StarburstRemoteAuthenticationType.PASSWORD;
import static com.starburstdata.trino.plugin.starburstremote.StarburstRemoteAuthenticationType.PASSWORD_PASS_THROUGH;
import static io.airlift.configuration.ConditionalModule.installModuleIf;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class StarburstRemoteAuthenticationModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(installModuleIf(
                StarburstRemoteConfig.class,
                config -> config.getAuthenticationType() == PASSWORD_PASS_THROUGH,
                new StarburstRemotePasswordPassThroughModule()));

        install(installModuleIf(
                StarburstRemoteConfig.class,
                config -> config.getAuthenticationType() == PASSWORD && !config.isImpersonationEnabled(),
                new PasswordModule()));

        install(installModuleIf(
                StarburstRemoteConfig.class,
                config -> config.getAuthenticationType() == PASSWORD && config.isImpersonationEnabled(),
                new PasswordWithImpersonationModule()));

        install(installModuleIf(
                StarburstRemoteConfig.class,
                config -> config.getAuthenticationType() == KERBEROS && !config.isImpersonationEnabled(),
                new KerberosModule()));

        install(installModuleIf(
                StarburstRemoteConfig.class,
                config -> config.getAuthenticationType() == KERBEROS && config.isImpersonationEnabled(),
                new KerberosWithImpersonationModule()));
    }

    private static class StarburstRemotePasswordPassThroughModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            install(new PasswordPassThroughModule<>(StarburstRemoteConfig.class, StarburstRemoteConfig::isImpersonationEnabled));
        }

        @Provides
        @Singleton
        @ForBaseJdbc
        public ConnectionFactory getConnectionFactory(BaseJdbcConfig config, StarburstRemoteConfig starburstRemoteConfig, StarburstRemoteSslConfig sslConfig, CredentialProvider credentialProvider)
        {
            checkState(
                    !starburstRemoteConfig.isImpersonationEnabled(),
                    "User impersonation cannot be used along with PASSWORD_PASS_THROUGH authentication");
            checkState(starburstRemoteConfig.isSslEnabled(), "SSL must be enabled when using password pass-through authentication");

            Properties properties = new Properties();
            setSslProperties(properties, sslConfig);

            return new DriverConnectionFactory(new TrinoDriver(), config.getConnectionUrl(), properties, credentialProvider);
        }
    }

    private static class PasswordModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            install(new CredentialProviderModule());
            configBinder(binder).bindConfig(StarburstRemoteCredentialConfig.class);
        }

        @Provides
        @Singleton
        @ForBaseJdbc
        public ConnectionFactory getConnectionFactory(
                BaseJdbcConfig config,
                StarburstRemoteConfig connectorConfig,
                StarburstRemoteSslConfig sslConfig,
                CredentialProvider credentialProvider)
        {
            Properties properties = new Properties();
            if (connectorConfig.isSslEnabled()) {
                setSslProperties(properties, sslConfig);
            }

            return new DriverConnectionFactory(new TrinoDriver(), config.getConnectionUrl(), properties, credentialProvider);
        }
    }

    private static class PasswordWithImpersonationModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            install(new AuthToLocalModule());
            install(new CredentialProviderModule());
            configBinder(binder).bindConfig(StarburstRemoteCredentialConfig.class);
        }

        @Provides
        @Singleton
        @ForBaseJdbc
        public ConnectionFactory getConnectionFactory(
                BaseJdbcConfig config,
                StarburstRemoteConfig connectorConfig,
                StarburstRemoteSslConfig sslConfig,
                AuthToLocal authToLocal,
                CredentialProvider credentialProvider)
        {
            Properties properties = new Properties();
            if (connectorConfig.isSslEnabled()) {
                setSslProperties(properties, sslConfig);
            }

            return new DriverConnectionFactory(
                    new TrinoDriver(),
                    config.getConnectionUrl(),
                    properties,
                    new StarburstRemoteImpersonatingCredentialPropertiesProvider(credentialProvider, authToLocal));
        }
    }

    private abstract static class AbstractKerberosModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            configBinder(binder).bindConfig(StarburstRemoteKerberosConfig.class);
        }

        protected DriverConnectionFactory setupDriverConnectionFactory(
                String connectionUrl,
                StarburstRemoteConfig connectorConfig,
                StarburstRemoteSslConfig sslConfig,
                StarburstRemoteKerberosConfig kerberosConfig,
                CredentialPropertiesProvider credentialPropertiesProvider)
        {
            checkState(connectorConfig.isSslEnabled(), "SSL must be enabled when using Kerberos authentication");

            Properties properties = new Properties();
            setSslProperties(properties, sslConfig);
            setKerberosProperties(properties, kerberosConfig);

            return new DriverConnectionFactory(
                    new TrinoDriver(),
                    connectionUrl,
                    properties,
                    credentialPropertiesProvider);
        }

        private static void setKerberosProperties(Properties properties, StarburstRemoteKerberosConfig kerberosConfig)
        {
            properties.setProperty("KerberosPrincipal", kerberosConfig.getClientPrincipal());
            properties.setProperty("KerberosKeytabPath", kerberosConfig.getClientKeytabFile().getAbsolutePath());
            properties.setProperty("KerberosConfigPath", kerberosConfig.getConfigFile().getAbsolutePath());
            properties.setProperty("KerberosRemoteServiceName", kerberosConfig.getServiceName());
            setOptionalProperty(properties, "KerberosServicePrincipalPattern", kerberosConfig.getServicePrincipalPattern());
            properties.setProperty("KerberosUseCanonicalHostname", String.valueOf(kerberosConfig.isServiceUseCanonicalHostname()));
        }
    }

    private static class KerberosModule
            extends AbstractKerberosModule
    {
        @Override
        public void setup(Binder binder)
        {
            super.setup(binder);
            configBinder(binder).bindConfig(CredentialConfig.class);
        }

        @Provides
        @Singleton
        @ForBaseJdbc
        public ConnectionFactory getConnectionFactory(
                BaseJdbcConfig baseJdbcConfig,
                StarburstRemoteConfig connectorConfig,
                StarburstRemoteSslConfig sslConfig,
                StarburstRemoteKerberosConfig kerberosConfig,
                CredentialConfig credentialConfig)
        {
            checkState(credentialConfig.getConnectionPassword().isEmpty(), "connection-password should not be set when using Kerberos authentication");
            String user = credentialConfig.getConnectionUser().orElse(kerberosConfig.getClientPrincipal());

            return setupDriverConnectionFactory(
                    baseJdbcConfig.getConnectionUrl(),
                    connectorConfig,
                    sslConfig,
                    kerberosConfig,
                    new DefaultCredentialPropertiesProvider(new StaticCredentialProvider(Optional.of(user), Optional.empty())));
        }
    }

    private static class KerberosWithImpersonationModule
            extends AbstractKerberosModule
    {
        @Override
        protected void setup(Binder binder)
        {
            super.setup(binder);
            install(new AuthToLocalModule());
        }

        @Provides
        @Singleton
        @ForBaseJdbc
        public ConnectionFactory getConnectionFactory(
                BaseJdbcConfig baseJdbcConfig,
                StarburstRemoteConfig connectorConfig,
                StarburstRemoteSslConfig sslConfig,
                StarburstRemoteKerberosConfig kerberosConfig,
                AuthToLocal authToLocal)
        {
            return setupDriverConnectionFactory(
                    baseJdbcConfig.getConnectionUrl(),
                    connectorConfig,
                    sslConfig,
                    kerberosConfig,
                    new StarburstRemoteImpersonatingCredentialPropertiesProvider(
                            new StaticCredentialProvider(Optional.of(kerberosConfig.getClientPrincipal()), Optional.empty()),
                            authToLocal));
        }
    }

    private static void setSslProperties(Properties properties, StarburstRemoteSslConfig sslConfig)
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
