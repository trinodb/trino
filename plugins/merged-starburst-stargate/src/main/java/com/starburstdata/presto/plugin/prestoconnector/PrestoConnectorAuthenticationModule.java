/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.prestoconnector;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.starburstdata.presto.plugin.jdbc.auth.PassThroughCredentialProvider;
import com.starburstdata.presto.plugin.jdbc.authtolocal.AuthToLocal;
import com.starburstdata.presto.plugin.jdbc.authtolocal.AuthToLocalModule;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.jdbc.TrinoDriver;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.credential.CredentialConfig;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.credential.CredentialProviderModule;
import io.trino.plugin.jdbc.credential.StaticCredentialProvider;

import java.io.File;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkState;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoAuthenticationType.KERBEROS;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoAuthenticationType.PASSWORD;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoAuthenticationType.PASSWORD_PASS_THROUGH;
import static io.airlift.configuration.ConditionalModule.installModuleIf;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class PrestoConnectorAuthenticationModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(installModuleIf(
                PrestoConnectorConfig.class,
                config -> config.getPrestoAuthenticationType() == PASSWORD_PASS_THROUGH,
                new PasswordPassthroughModule()));

        install(installModuleIf(
                PrestoConnectorConfig.class,
                config -> config.getPrestoAuthenticationType() == PASSWORD && !config.isImpersonationEnabled(),
                new PasswordModule()));

        install(installModuleIf(
                PrestoConnectorConfig.class,
                config -> config.getPrestoAuthenticationType() == PASSWORD && config.isImpersonationEnabled(),
                new PasswordWithImpersonationModule()));

        install(installModuleIf(
                PrestoConnectorConfig.class,
                config -> config.getPrestoAuthenticationType() == KERBEROS,
                new KerberosModule()));
    }

    private static class PasswordPassthroughModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
        }

        @Provides
        @Singleton
        @ForBaseJdbc
        public ConnectionFactory getConnectionFactory(BaseJdbcConfig config, PrestoConnectorConfig prestoConnectorConfig, PrestoConnectorSslConfig sslConfig)
        {
            checkState(
                    !prestoConnectorConfig.isImpersonationEnabled(),
                    "User impersonation cannot be used along with PASSWORD_PASS_THROUGH authentication");
            checkState(prestoConnectorConfig.isSslEnabled(), "SSL must be enabled when using password pass-through authentication");

            Properties properties = new Properties();
            setSslProperties(properties, sslConfig);

            return new DriverConnectionFactory(new TrinoDriver(), config.getConnectionUrl(), properties, new PassThroughCredentialProvider());
        }
    }

    private static class PasswordModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            install(new CredentialProviderModule());
            configBinder(binder).bindConfig(PrestoConnectorCredentialConfig.class);
        }

        @Provides
        @Singleton
        @ForBaseJdbc
        public ConnectionFactory getConnectionFactory(
                BaseJdbcConfig config,
                PrestoConnectorConfig connectorConfig,
                PrestoConnectorSslConfig sslConfig,
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
            configBinder(binder).bindConfig(PrestoConnectorCredentialConfig.class);
        }

        @Provides
        @Singleton
        @ForBaseJdbc
        public ConnectionFactory getConnectionFactory(
                BaseJdbcConfig config,
                PrestoConnectorConfig connectorConfig,
                PrestoConnectorSslConfig sslConfig,
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
                    new PrestoConnectorImpersonatingCredentialPropertiesProvider(credentialProvider, authToLocal));
        }
    }

    private static class KerberosModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            configBinder(binder).bindConfig(CredentialConfig.class);
            configBinder(binder).bindConfig(PrestoConnectorKerberosConfig.class);
        }

        @Provides
        @Singleton
        @ForBaseJdbc
        public ConnectionFactory getConnectionFactory(
                BaseJdbcConfig baseJdbcConfig,
                PrestoConnectorConfig connectorConfig,
                PrestoConnectorSslConfig sslConfig,
                PrestoConnectorKerberosConfig kerberosConfig,
                CredentialConfig credentialConfig)
        {
            checkState(connectorConfig.isSslEnabled(), "SSL must be enabled when using Kerberos authentication");
            checkState(credentialConfig.getConnectionPassword().isEmpty(), "connection-password should not be set when using Kerberos authentication");

            Properties properties = new Properties();
            setSslProperties(properties, sslConfig);
            setKerberosProperties(properties, kerberosConfig);

            String user = credentialConfig.getConnectionUser().orElse(kerberosConfig.getClientPrincipal());

            return new DriverConnectionFactory(
                    new TrinoDriver(),
                    baseJdbcConfig.getConnectionUrl(),
                    properties,
                    new StaticCredentialProvider(Optional.of(user), Optional.empty()));
        }
    }

    private static void setKerberosProperties(Properties properties, PrestoConnectorKerberosConfig kerberosConfig)
    {
        properties.setProperty("KerberosPrincipal", kerberosConfig.getClientPrincipal());
        properties.setProperty("KerberosKeytabPath", kerberosConfig.getClientKeytabFile().getAbsolutePath());
        properties.setProperty("KerberosConfigPath", kerberosConfig.getConfigFile().getAbsolutePath());
        properties.setProperty("KerberosRemoteServiceName", kerberosConfig.getServiceName());
        setOptionalProperty(properties, "KerberosServicePrincipalPattern", kerberosConfig.getServicePrincipalPattern());
        properties.setProperty("KerberosUseCanonicalHostname", String.valueOf(kerberosConfig.isServiceUseCanonicalHostname()));
    }

    private static void setSslProperties(Properties properties, PrestoConnectorSslConfig sslConfig)
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
