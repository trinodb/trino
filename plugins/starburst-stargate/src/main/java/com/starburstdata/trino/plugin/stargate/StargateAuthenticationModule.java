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
import com.google.inject.Singleton;
import com.starburstdata.presto.plugin.jdbc.auth.AuthenticationBasedIdentityCacheMappingModule;
import com.starburstdata.presto.plugin.jdbc.auth.PasswordPassThroughModule;
import com.starburstdata.presto.plugin.jdbc.auth.SingletonIdentityCacheMappingModule;
import com.starburstdata.presto.plugin.toolkit.authtolocal.AuthToLocal;
import com.starburstdata.presto.plugin.toolkit.authtolocal.AuthToLocalModule;
import com.starburstdata.presto.plugin.toolkit.security.multiple.tokens.TokenPassThroughConfig;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.jdbc.TrinoDriver;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ExtraCredentialsBasedIdentityCacheMappingModule;
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
import static com.starburstdata.presto.plugin.toolkit.guice.Modules.enumConditionalModule;
import static com.starburstdata.presto.plugin.toolkit.guice.Modules.option;
import static com.starburstdata.trino.plugin.stargate.StargateAuthenticationType.KERBEROS;
import static com.starburstdata.trino.plugin.stargate.StargateAuthenticationType.OAUTH2_PASSTHROUGH;
import static com.starburstdata.trino.plugin.stargate.StargateAuthenticationType.PASSWORD;
import static com.starburstdata.trino.plugin.stargate.StargateAuthenticationType.PASSWORD_PASS_THROUGH;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class StargateAuthenticationModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(enumConditionalModule(
                StargateConfig.class,
                StargateConfig::getAuthenticationType,
                option(PASSWORD_PASS_THROUGH, new StargatePasswordPassThroughModule()),
                option(PASSWORD, conditionalModule(StargateConfig.class, StargateConfig::isImpersonationEnabled, new PasswordWithImpersonationModule(), new PasswordModule())),
                option(KERBEROS, conditionalModule(StargateConfig.class, StargateConfig::isImpersonationEnabled, new KerberosWithImpersonationModule(), new KerberosModule())),
                option(OAUTH2_PASSTHROUGH, new StargateOAuth2PassThroughModule())));
    }

    private static class StargatePasswordPassThroughModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            install(new PasswordPassThroughModule<>(StargateConfig.class, StargateConfig::isImpersonationEnabled));
        }

        @Provides
        @Singleton
        @TransportConnectionFactory
        public ConnectionFactory getConnectionFactory(BaseJdbcConfig config, StargateConfig stargateConfig, StargateSslConfig sslConfig, CredentialProvider credentialProvider)
        {
            checkState(
                    !stargateConfig.isImpersonationEnabled(),
                    "User impersonation cannot be used along with PASSWORD_PASS_THROUGH authentication");
            checkState(stargateConfig.isSslEnabled(), "SSL must be enabled when using password pass-through authentication");

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
            install(new ExtraCredentialsBasedIdentityCacheMappingModule());
            configBinder(binder).bindConfig(StargateCredentialConfig.class);
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

    private static class PasswordWithImpersonationModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            install(new AuthToLocalModule());
            install(new CredentialProviderModule());
            install(new AuthenticationBasedIdentityCacheMappingModule());
            configBinder(binder).bindConfig(StargateCredentialConfig.class);
        }

        @Provides
        @Singleton
        @TransportConnectionFactory
        public ConnectionFactory getConnectionFactory(
                BaseJdbcConfig config,
                StargateConfig connectorConfig,
                StargateSslConfig sslConfig,
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
                    new StargateImpersonatingCredentialPropertiesProvider(credentialProvider, authToLocal));
        }
    }

    private abstract static class AbstractKerberosModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            configBinder(binder).bindConfig(StargateKerberosConfig.class);
        }

        protected DriverConnectionFactory setupDriverConnectionFactory(
                String connectionUrl,
                StargateConfig connectorConfig,
                StargateSslConfig sslConfig,
                StargateKerberosConfig kerberosConfig,
                CredentialPropertiesProvider<String, String> credentialPropertiesProvider)
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

        private static void setKerberosProperties(Properties properties, StargateKerberosConfig kerberosConfig)
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
            install(new SingletonIdentityCacheMappingModule());
        }

        @Provides
        @Singleton
        @TransportConnectionFactory
        public ConnectionFactory getConnectionFactory(
                BaseJdbcConfig baseJdbcConfig,
                StargateConfig connectorConfig,
                StargateSslConfig sslConfig,
                StargateKerberosConfig kerberosConfig,
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
            install(new AuthenticationBasedIdentityCacheMappingModule());
        }

        @Provides
        @Singleton
        @TransportConnectionFactory
        public ConnectionFactory getConnectionFactory(
                BaseJdbcConfig baseJdbcConfig,
                StargateConfig connectorConfig,
                StargateSslConfig sslConfig,
                StargateKerberosConfig kerberosConfig,
                AuthToLocal authToLocal)
        {
            return setupDriverConnectionFactory(
                    baseJdbcConfig.getConnectionUrl(),
                    connectorConfig,
                    sslConfig,
                    kerberosConfig,
                    new StargateImpersonatingCredentialPropertiesProvider(
                            new StaticCredentialProvider(Optional.of(kerberosConfig.getClientPrincipal()), Optional.empty()),
                            authToLocal));
        }
    }

    private static class StargateOAuth2PassThroughModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            install(new AuthenticationBasedIdentityCacheMappingModule());
            configBinder(binder).bindConfig(TokenPassThroughConfig.class, "stargate");
        }

        @Provides
        @Singleton
        @TransportConnectionFactory
        public ConnectionFactory getConnectionFactory(
                BaseJdbcConfig baseJdbcConfig,
                StargateConfig connectorConfig,
                StargateSslConfig sslConfig,
                TokenPassThroughConfig tokenPassThroughConfig)
        {
            Properties properties = new Properties();
            if (connectorConfig.isSslEnabled()) {
                setSslProperties(properties, sslConfig);
            }

            return new DriverConnectionFactory(
                    new TrinoDriver(),
                    baseJdbcConfig.getConnectionUrl(),
                    properties,
                    new StargateOAuth2TokenPassthroughProvider(tokenPassThroughConfig));
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
