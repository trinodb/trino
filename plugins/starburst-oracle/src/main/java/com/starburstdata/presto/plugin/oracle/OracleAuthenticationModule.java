/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.oracle;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.starburstdata.presto.kerberos.ConnectorKerberosManagerModule;
import com.starburstdata.presto.kerberos.KerberosManager;
import com.starburstdata.presto.license.LicenseManager;
import com.starburstdata.presto.plugin.jdbc.auth.ForAuthentication;
import com.starburstdata.presto.plugin.jdbc.auth.NoImpersonationModule;
import com.starburstdata.presto.plugin.jdbc.auth.PassThroughCredentialProvider;
import com.starburstdata.presto.plugin.jdbc.authtolocal.AuthToLocalModule;
import com.starburstdata.presto.plugin.jdbc.kerberos.KerberosConfig;
import com.starburstdata.presto.plugin.jdbc.kerberos.KerberosConnectionFactory;
import com.starburstdata.presto.plugin.jdbc.kerberos.PassThroughKerberosConnectionFactory;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.plugin.jdbc.ForBaseJdbc;
import io.prestosql.plugin.jdbc.credential.CredentialProvider;
import io.prestosql.plugin.jdbc.credential.CredentialProviderModule;
import io.prestosql.plugin.jdbc.credential.EmptyCredentialProvider;
import io.prestosql.plugin.oracle.OracleConfig;
import io.prestosql.spi.PrestoException;
import oracle.jdbc.driver.OracleDriver;
import oracle.net.ano.AnoServices;

import java.util.Optional;
import java.util.Properties;

import static com.starburstdata.presto.plugin.oracle.OracleAuthenticationType.KERBEROS;
import static com.starburstdata.presto.plugin.oracle.OracleAuthenticationType.KERBEROS_PASS_THROUGH;
import static com.starburstdata.presto.plugin.oracle.OracleAuthenticationType.PASSWORD;
import static com.starburstdata.presto.plugin.oracle.OracleAuthenticationType.PASSWORD_PASS_THROUGH;
import static io.airlift.configuration.ConditionalModule.installModuleIf;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.prestosql.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static java.util.Objects.requireNonNull;
import static oracle.jdbc.OracleConnection.CONNECTION_PROPERTY_INCLUDE_SYNONYMS;
import static oracle.jdbc.OracleConnection.CONNECTION_PROPERTY_REPORT_REMARKS;
import static oracle.jdbc.OracleConnection.CONNECTION_PROPERTY_RESTRICT_GETTABLES;

public class OracleAuthenticationModule
        extends AbstractConfigurationAwareModule
{
    private final String catalogName;

    public OracleAuthenticationModule(String catalogName)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        OracleConfig oracleConfig = buildConfigObject(OracleConfig.class);

        install(installModuleIf(
                StarburstOracleConfig.class,
                StarburstOracleConfig::isImpersonationEnabled,
                new ImpersonationModule(),
                new NoImpersonationModule()));

        install(installModuleIf(
                StarburstOracleConfig.class,
                config -> config.getAuthenticationType() == PASSWORD,
                new UserPasswordModule()));

        install(installModuleIf(
                StarburstOracleConfig.class,
                config -> config.getAuthenticationType() == PASSWORD_PASS_THROUGH && oracleConfig.isConnectionPoolEnabled(),
                new PasswordPassThroughWithPoolingModule()));

        install(installModuleIf(
                StarburstOracleConfig.class,
                config -> config.getAuthenticationType() == PASSWORD_PASS_THROUGH && !oracleConfig.isConnectionPoolEnabled(),
                new PasswordPassThroughModule()));

        install(installModuleIf(
                StarburstOracleConfig.class,
                config -> config.getAuthenticationType() == KERBEROS,
                new KerberosModule()));

        install(installModuleIf(
                StarburstOracleConfig.class,
                config -> config.getAuthenticationType() == KERBEROS_PASS_THROUGH,
                new KerberosPassThroughModule()));
    }

    private ConnectionFactory createBasicConnectionFactory(BaseJdbcConfig config,
            StarburstOracleConfig starburstOracleConfig,
            OracleConfig oracleConfig,
            CredentialProvider credentialProvider)
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

    private class ImpersonationModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.install(new AuthToLocalModule(catalogName));
            binder.bind(ConnectionFactory.class).annotatedWith(ForBaseJdbc.class).to(OracleImpersonatingConnectionFactory.class).in(Scopes.SINGLETON);
        }
    }

    private class UserPasswordModule
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
        public ConnectionFactory getConnectionFactory(BaseJdbcConfig config,
                StarburstOracleConfig starburstOracleConfig,
                OracleConfig oracleConfig,
                CredentialProvider credentialProvider)
        {
            return createBasicConnectionFactory(config, starburstOracleConfig, oracleConfig, credentialProvider);
        }
    }

    private static class PasswordPassThroughModule
            implements Module
    {
        @Override
        public void configure(Binder binder) {}

        @Provides
        @Singleton
        @ForAuthentication
        public ConnectionFactory getConnectionFactory(BaseJdbcConfig config, OracleConfig oracleConfig, StarburstOracleConfig starburstOracleConfig)
        {
            return new DriverConnectionFactory(
                    new OracleDriver(),
                    config.getConnectionUrl(),
                    getProperties(oracleConfig),
                    new PassThroughCredentialProvider());
        }
    }

    private class PasswordPassThroughWithPoolingModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            install(new CredentialProviderModule());
            configBinder(binder).bindConfig(OracleConfig.class);
        }

        @Provides
        @Singleton
        @ForAuthentication
        public ConnectionFactory getConnectionFactory(BaseJdbcConfig config, OracleConfig oracleConfig, StarburstOracleConfig starburstOracleConfig, CredentialProvider credentialProvider)
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

    private class KerberosModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            configBinder(binder).bindConfig(KerberosConfig.class);
        }

        @Inject
        @Provides
        @Singleton
        @ForAuthentication
        public ConnectionFactory getConnectionFactory(
                LicenseManager licenseManager,
                BaseJdbcConfig baseJdbcConfig,
                StarburstOracleConfig starburstOracleConfig,
                OracleConfig oracleConfig,
                KerberosConfig kerberosConfig)
        {
            if (oracleConfig.isConnectionPoolEnabled()) {
                ConnectionFactory connectionFactory = new OraclePoolingConnectionFactory(
                        catalogName,
                        baseJdbcConfig,
                        getKerberosProperties(oracleConfig),
                        Optional.empty(),
                        oracleConfig,
                        starburstOracleConfig.getAuthenticationType());
                return new KerberosConnectionFactory(licenseManager, connectionFactory, kerberosConfig);
            }
            return new KerberosConnectionFactory(licenseManager, baseJdbcConfig, kerberosConfig, getKerberosProperties(oracleConfig));
        }
    }

    private static class KerberosPassThroughModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.install(new ConnectorKerberosManagerModule());
        }

        @Provides
        @Singleton
        @ForAuthentication
        public ConnectionFactory getConnectionFactory(BaseJdbcConfig baseJdbcConfig, OracleConfig oracleConfig, StarburstOracleConfig starburstOracleConfig, KerberosManager kerberosManager)
        {
            if (oracleConfig.isConnectionPoolEnabled()) {
                throw new PrestoException(CONFIGURATION_INVALID, "Connection pooling cannot be used with Kerberos pass-through authentication");
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
