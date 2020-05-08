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
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.starburstdata.presto.kerberos.ConnectorKerberosManagerModule;
import com.starburstdata.presto.kerberos.KerberosManager;
import com.starburstdata.presto.plugin.jdbc.auth.ForAuthentication;
import com.starburstdata.presto.plugin.jdbc.auth.NoImpersonationModule;
import com.starburstdata.presto.plugin.jdbc.auth.PassThroughCredentialProvider;
import com.starburstdata.presto.plugin.jdbc.authtolocal.AuthToLocal;
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
import io.prestosql.plugin.jdbc.credential.EmptyCredentialProvider;
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
        install(installModuleIf(
                OracleConfig.class,
                OracleConfig::isImpersonationEnabled,
                new ImpersonationModule(),
                new NoImpersonationModule()));

        install(installModuleIf(
                OracleConfig.class,
                config -> config.getAuthenticationType() == PASSWORD,
                new UserPasswordModule()));

        install(installModuleIf(
                OracleConfig.class,
                config -> config.getAuthenticationType() == PASSWORD_PASS_THROUGH,
                new PasswordPassThroughModule()));

        install(installModuleIf(
                OracleConfig.class,
                config -> config.getAuthenticationType() == KERBEROS,
                new KerberosModule()));

        install(installModuleIf(
                OracleConfig.class,
                config -> config.getAuthenticationType() == KERBEROS_PASS_THROUGH,
                new KerberosPassThroughModule()));
    }

    private ConnectionFactory createBasicConnectionFactory(BaseJdbcConfig config,
            OracleConfig oracleConfig,
            OracleConnectionPoolingConfig poolingConfig,
            CredentialProvider credentialProvider)
    {
        if (oracleConfig.isConnectionPoolingEnabled()) {
            return new OraclePoolingConnectionFactory(
                    catalogName,
                    config,
                    getProperties(oracleConfig),
                    Optional.of(credentialProvider),
                    poolingConfig);
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
        }

        @Provides
        @Singleton
        @ForBaseJdbc
        public ConnectionFactory getConnectionFactory(@ForAuthentication ConnectionFactory connectionFactory, AuthToLocal authToLocal)
        {
            return new OracleImpersonatingConnectionFactory(connectionFactory, authToLocal);
        }
    }

    private class UserPasswordModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            configBinder(binder).bindConfig(OracleConnectionPoolingConfig.class);
        }

        @Provides
        @Singleton
        @ForAuthentication
        public ConnectionFactory getConnectionFactory(BaseJdbcConfig config,
                OracleConfig oracleConfig,
                OracleConnectionPoolingConfig poolingConfig,
                CredentialProvider credentialProvider)
        {
            return createBasicConnectionFactory(config, oracleConfig, poolingConfig, credentialProvider);
        }
    }

    private class PasswordPassThroughModule
            implements Module
    {
        @Override
        public void configure(Binder binder) {}

        @Provides
        @Singleton
        @ForAuthentication
        public ConnectionFactory getConnectionFactory(BaseJdbcConfig config, OracleConfig oracleConfig, OracleConnectionPoolingConfig poolingConfig)
        {
            return createBasicConnectionFactory(config, oracleConfig, poolingConfig, new PassThroughCredentialProvider());
        }
    }

    private class KerberosModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            configBinder(binder).bindConfig(KerberosConfig.class);
            configBinder(binder).bindConfig(OracleConnectionPoolingConfig.class);
        }

        @Provides
        @Singleton
        @ForAuthentication
        public ConnectionFactory getConnectionFactory(
                BaseJdbcConfig baseJdbcConfig,
                OracleConfig oracleConfig,
                OracleConnectionPoolingConfig poolingConfig,
                KerberosConfig kerberosConfig)
        {
            if (oracleConfig.isConnectionPoolingEnabled()) {
                ConnectionFactory connectionFactory = new OraclePoolingConnectionFactory(
                        catalogName,
                        baseJdbcConfig,
                        getKerberosProperties(oracleConfig),
                        Optional.empty(),
                        poolingConfig);
                return new KerberosConnectionFactory(connectionFactory, kerberosConfig);
            }
            return new KerberosConnectionFactory(baseJdbcConfig, kerberosConfig, getKerberosProperties(oracleConfig));
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
        public ConnectionFactory getConnectionFactory(BaseJdbcConfig baseJdbcConfig, OracleConfig oracleConfig, KerberosManager kerberosManager)
        {
            if (oracleConfig.isConnectionPoolingEnabled()) {
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
        return properties;
    }
}
