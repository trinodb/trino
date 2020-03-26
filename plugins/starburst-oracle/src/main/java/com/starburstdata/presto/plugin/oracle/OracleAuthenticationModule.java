/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.starburstdata.presto.plugin.oracle;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.prestosql.plugin.jdbc.AuthToLocalModule;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.plugin.jdbc.KerberosConfig;
import io.prestosql.plugin.jdbc.KerberosConnectionFactory;
import io.prestosql.plugin.jdbc.PassThroughConnectionFactory;
import io.prestosql.plugin.jdbc.credential.CredentialProvider;
import io.prestosql.plugin.jdbc.credential.CredentialProviderModule;
import io.prestosql.spi.PrestoException;
import oracle.jdbc.driver.OracleDriver;
import oracle.net.ano.AnoServices;

import javax.inject.Qualifier;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Optional;
import java.util.Properties;

import static com.starburstdata.presto.plugin.oracle.OracleAuthenticationType.KERBEROS;
import static com.starburstdata.presto.plugin.oracle.OracleAuthenticationType.PASS_THROUGH;
import static io.airlift.configuration.ConditionalModule.installModuleIf;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.prestosql.plugin.jdbc.GuiceUtils.installModuleIfOrElse;
import static io.prestosql.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
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
                config -> config.getAuthenticationType() == OracleAuthenticationType.USER_PASSWORD,
                userPasswordModule()));

        install(installModuleIf(
                OracleConfig.class,
                config -> config.getAuthenticationType() == KERBEROS,
                kerberosModule()));

        install(installModuleIf(
                OracleConfig.class,
                config -> config.getAuthenticationType() == PASS_THROUGH,
                passThroughModule()));

        install(installModuleIfOrElse(
                OracleConfig.class,
                OracleConfig::isImpersonationEnabled,
                impersonationModule(catalogName),
                noImpersonationModule()));
    }

    private Module userPasswordModule()
    {
        return new Module()
        {
            @Override
            public void configure(Binder binder)
            {
                install(new CredentialProviderModule());
                configBinder(binder).bindConfig(OracleConnectionPoolingConfig.class);
            }

            @Inject
            @Provides
            @Singleton
            @ForAuthentication
            private ConnectionFactory getConnectionFactory(BaseJdbcConfig config, OracleConfig oracleConfig, OracleConnectionPoolingConfig poolingConfig, CredentialProvider credentialProvider)
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
        };
    }

    private Module kerberosModule()
    {
        return new Module()
        {
            @Override
            public void configure(Binder binder)
            {
                configBinder(binder).bindConfig(KerberosConfig.class);
                configBinder(binder).bindConfig(OracleConnectionPoolingConfig.class);
            }

            @Inject
            @Provides
            @Singleton
            @ForAuthentication
            private ConnectionFactory getConnectionFactory(BaseJdbcConfig baseJdbcConfig, OracleConfig oracleConfig, OracleConnectionPoolingConfig poolingConfig, KerberosConfig kerberosConfig)
            {
                if (oracleConfig.isConnectionPoolingEnabled()) {
                    return new KerberosConnectionFactory(
                            new OraclePoolingConnectionFactory(
                                    catalogName,
                                    baseJdbcConfig,
                                    getKerberosProperties(oracleConfig),
                                    Optional.empty(),
                                    poolingConfig),
                            kerberosConfig);
                }
                return new KerberosConnectionFactory(baseJdbcConfig, kerberosConfig, getKerberosProperties(oracleConfig));
            }
        };
    }

    private Module passThroughModule()
    {
        return new Module()
        {
            @Override
            public void configure(Binder binder)
            {
            }

            @Inject
            @Provides
            @Singleton
            @ForAuthentication
            private ConnectionFactory getConnectionFactory(BaseJdbcConfig baseJdbcConfig, OracleConfig oracleConfig)
            {
                if (oracleConfig.isConnectionPoolingEnabled()) {
                    throw new PrestoException(CONFIGURATION_INVALID, "Connection pooling cannot be used with pass-through authentication");
                }
                // TODO we should be passing different properties depending on actual Credential that is passed through (not Kerberos always)
                return new PassThroughConnectionFactory(baseJdbcConfig, getKerberosProperties(oracleConfig));
            }
        };
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

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @Qualifier
    public @interface ForAuthentication
    {
    }

    private Module impersonationModule(String catalogName)
    {
        return binder -> {
            install(new AuthToLocalModule(catalogName));
            binder.bind(ConnectionFactory.class).to(OracleImpersonatingConnectionFactory.class);
        };
    }

    private Module noImpersonationModule()
    {
        return binder -> binder.bind(ConnectionFactory.class).to(Key.get(ConnectionFactory.class, ForAuthentication.class));
    }
}
