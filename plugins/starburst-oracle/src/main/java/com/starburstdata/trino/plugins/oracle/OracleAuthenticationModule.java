/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.oracle;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.starburstdata.presto.kerberos.ConnectorKerberosManagerModule;
import com.starburstdata.presto.kerberos.KerberosManager;
import com.starburstdata.presto.license.LicenseManager;
import com.starburstdata.presto.plugin.jdbc.auth.AuthenticationBasedIdentityCacheMappingModule;
import com.starburstdata.presto.plugin.jdbc.auth.ForImpersonation;
import com.starburstdata.presto.plugin.jdbc.auth.PasswordPassThroughModule;
import com.starburstdata.presto.plugin.jdbc.kerberos.KerberosConfig;
import com.starburstdata.presto.plugin.jdbc.kerberos.KerberosConnectionFactory;
import com.starburstdata.presto.plugin.jdbc.kerberos.PassThroughKerberosConnectionFactory;
import com.starburstdata.presto.plugin.toolkit.authtolocal.AuthToLocalModule;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.credential.CredentialProviderModule;
import io.trino.plugin.jdbc.credential.EmptyCredentialProvider;
import io.trino.plugin.oracle.OracleConfig;
import io.trino.spi.TrinoException;
import oracle.jdbc.driver.OracleDriver;
import oracle.net.ano.AnoServices;

import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkState;
import static com.starburstdata.presto.plugin.jdbc.auth.NoImpersonationModule.noImpersonationModuleWithCredentialProvider;
import static com.starburstdata.presto.plugin.jdbc.auth.NoImpersonationModule.noImpersonationModuleWithSingletonIdentity;
import static com.starburstdata.presto.plugin.toolkit.guice.Modules.enumConditionalModule;
import static com.starburstdata.presto.plugin.toolkit.guice.Modules.option;
import static com.starburstdata.trino.plugins.oracle.OracleAuthenticationType.KERBEROS;
import static com.starburstdata.trino.plugins.oracle.OracleAuthenticationType.KERBEROS_PASS_THROUGH;
import static com.starburstdata.trino.plugins.oracle.OracleAuthenticationType.PASSWORD;
import static com.starburstdata.trino.plugins.oracle.OracleAuthenticationType.PASSWORD_PASS_THROUGH;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static oracle.jdbc.OracleConnection.CONNECTION_PROPERTY_INCLUDE_SYNONYMS;
import static oracle.jdbc.OracleConnection.CONNECTION_PROPERTY_REPORT_REMARKS;
import static oracle.jdbc.OracleConnection.CONNECTION_PROPERTY_RESTRICT_GETTABLES;

public class OracleAuthenticationModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        OracleConfig oracleConfig = buildConfigObject(OracleConfig.class);

        install(enumConditionalModule(
                StarburstOracleConfig.class,
                StarburstOracleConfig::getAuthenticationType,
                option(PASSWORD, new UserPasswordModule()),
                option(PASSWORD_PASS_THROUGH,
                        nestedBinder -> {
                            if (oracleConfig.isConnectionPoolEnabled()) {
                                install(new PasswordPassThroughWithPoolingModule());
                            }
                            else {
                                install(new OraclePasswordPassThroughModule());
                            }
                        }),
                option(KERBEROS, new KerberosModule()),
                option(KERBEROS_PASS_THROUGH, new KerberosPassThroughModule())));
    }

    private static ConnectionFactory createBasicConnectionFactory(BaseJdbcConfig config,
            StarburstOracleConfig starburstOracleConfig,
            OracleConfig oracleConfig,
            CredentialProvider credentialProvider,
            CatalogName catalogName)
    {
        if (oracleConfig.isConnectionPoolEnabled()) {
            return new OraclePoolingConnectionFactory(
                    catalogName,
                    config,
                    getProperties(oracleConfig),
                    Optional.of(credentialProvider),
                    oracleConfig,
                    starburstOracleConfig.getAuthenticationType());
        }

        return new DriverConnectionFactory(
                new OracleDriver(),
                config.getConnectionUrl(),
                getProperties(oracleConfig),
                credentialProvider);
    }

    private static class ImpersonationModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        public void setup(Binder binder)
        {
            install(new AuthToLocalModule());
            binder.bind(ConnectionFactory.class).annotatedWith(ForBaseJdbc.class).to(OracleImpersonatingConnectionFactory.class).in(Scopes.SINGLETON);
            install(new AuthenticationBasedIdentityCacheMappingModule());
        }
    }

    private static class UserPasswordModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        public void setup(Binder binder)
        {
            install(new CredentialProviderModule());
            install(conditionalModule(
                    StarburstOracleConfig.class,
                    StarburstOracleConfig::isImpersonationEnabled,
                    new ImpersonationModule(),
                    noImpersonationModuleWithCredentialProvider()));
        }

        @Provides
        @Singleton
        @ForImpersonation
        public ConnectionFactory getConnectionFactory(BaseJdbcConfig config,
                StarburstOracleConfig starburstOracleConfig,
                OracleConfig oracleConfig,
                CredentialProvider credentialProvider,
                CatalogName catalogName)
        {
            return createBasicConnectionFactory(config, starburstOracleConfig, oracleConfig, credentialProvider, catalogName);
        }
    }

    private static class OraclePasswordPassThroughModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            install(new PasswordPassThroughModule<>(StarburstOracleConfig.class, StarburstOracleConfig::isImpersonationEnabled));
        }

        @Provides
        @Singleton
        @ForBaseJdbc
        public ConnectionFactory getConnectionFactory(BaseJdbcConfig config, OracleConfig oracleConfig, CredentialProvider credentialProvider)
        {
            return new DriverConnectionFactory(
                    new OracleDriver(),
                    config.getConnectionUrl(),
                    getProperties(oracleConfig),
                    credentialProvider);
        }
    }

    private static class PasswordPassThroughWithPoolingModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        public void setup(Binder binder)
        {
            install(new CredentialProviderModule());
            configBinder(binder).bindConfig(OracleConfig.class);
            checkState(
                    !buildConfigObject(StarburstOracleConfig.class).isImpersonationEnabled(),
                    "Impersonation is not allowed when using credentials pass-through");
            install(new AuthenticationBasedIdentityCacheMappingModule());
        }

        @Provides
        @Singleton
        @ForBaseJdbc
        public ConnectionFactory getConnectionFactory(
                BaseJdbcConfig config,
                OracleConfig oracleConfig,
                StarburstOracleConfig starburstOracleConfig,
                CredentialProvider credentialProvider,
                CatalogName catalogName)
        {
            // pass-through credentials are be handled by OraclePoolingConnectionFactory
            return new OraclePoolingConnectionFactory(
                    catalogName,
                    config,
                    getProperties(oracleConfig),
                    Optional.of(credentialProvider), // static credentials are needed to initialize the pool
                    oracleConfig,
                    starburstOracleConfig.getAuthenticationType());
        }
    }

    private static class KerberosModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        public void setup(Binder binder)
        {
            configBinder(binder).bindConfig(KerberosConfig.class);
            install(conditionalModule(
                    StarburstOracleConfig.class,
                    StarburstOracleConfig::isImpersonationEnabled,
                    new ImpersonationModule(),
                    noImpersonationModuleWithSingletonIdentity()));
        }

        @Inject
        @Provides
        @Singleton
        @ForImpersonation
        public ConnectionFactory getConnectionFactory(
                LicenseManager licenseManager,
                BaseJdbcConfig baseJdbcConfig,
                StarburstOracleConfig starburstOracleConfig,
                OracleConfig oracleConfig,
                KerberosConfig kerberosConfig,
                CatalogName catalogName)
        {
            if (oracleConfig.isConnectionPoolEnabled()) {
                ConnectionFactory connectionFactory = new OraclePoolingConnectionFactory(
                        catalogName,
                        baseJdbcConfig,
                        getKerberosProperties(oracleConfig),
                        Optional.empty(),
                        oracleConfig,
                        starburstOracleConfig.getAuthenticationType());
                return new KerberosConnectionFactory(licenseManager, connectionFactory, kerberosConfig, true);
            }
            return new KerberosConnectionFactory(licenseManager, new OracleDriver(), baseJdbcConfig, kerberosConfig, getKerberosProperties(oracleConfig), true);
        }
    }

    private static class KerberosPassThroughModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        public void setup(Binder binder)
        {
            install(new ConnectorKerberosManagerModule());
            checkState(
                    !buildConfigObject(StarburstOracleConfig.class).isImpersonationEnabled(),
                    "Impersonation is not allowed when using credentials pass-through");
            install(new AuthenticationBasedIdentityCacheMappingModule());
        }

        @Provides
        @Singleton
        @ForBaseJdbc
        public ConnectionFactory getConnectionFactory(BaseJdbcConfig baseJdbcConfig, OracleConfig oracleConfig, KerberosManager kerberosManager)
        {
            if (oracleConfig.isConnectionPoolEnabled()) {
                throw new TrinoException(CONFIGURATION_INVALID, "Connection pooling cannot be used with Kerberos pass-through authentication");
            }

            DriverConnectionFactory connectionFactory = new DriverConnectionFactory(
                    new OracleDriver(),
                    baseJdbcConfig.getConnectionUrl(),
                    getKerberosProperties(oracleConfig),
                    new EmptyCredentialProvider());

            return new PassThroughKerberosConnectionFactory(kerberosManager, connectionFactory);
        }
    }

    private static Properties getKerberosProperties(OracleConfig oracleConfig)
    {
        Properties properties = getProperties(oracleConfig);
        properties.setProperty(AnoServices.AUTHENTICATION_PROPERTY_SERVICES, "(" + AnoServices.AUTHENTICATION_KERBEROS5 + ")");
        return properties;
    }

    private static Properties getProperties(OracleConfig oracleConfig)
    {
        Properties properties = new Properties();
        if (oracleConfig.isSynonymsEnabled()) {
            properties.setProperty(CONNECTION_PROPERTY_INCLUDE_SYNONYMS, "true");
            properties.setProperty(CONNECTION_PROPERTY_RESTRICT_GETTABLES, "true");
        }
        if (oracleConfig.isRemarksReportingEnabled()) {
            properties.setProperty(CONNECTION_PROPERTY_REPORT_REMARKS, "true");
        }
        return properties;
    }
}
