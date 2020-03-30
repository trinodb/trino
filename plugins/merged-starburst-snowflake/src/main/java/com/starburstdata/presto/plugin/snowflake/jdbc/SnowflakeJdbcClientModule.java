/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake.jdbc;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.starburstdata.presto.plugin.snowflake.SnowflakeConfig;
import com.starburstdata.presto.plugin.snowflake.SnowflakeImpersonationType;
import com.starburstdata.presto.plugin.snowflake.auth.CachingSnowflakeOauthService;
import com.starburstdata.presto.plugin.snowflake.auth.DefaultSnowflakeOauthService;
import com.starburstdata.presto.plugin.snowflake.auth.NativeOktaAuthClient;
import com.starburstdata.presto.plugin.snowflake.auth.NativeSnowflakeAuthClient;
import com.starburstdata.presto.plugin.snowflake.auth.OktaAuthClient;
import com.starburstdata.presto.plugin.snowflake.auth.OktaConfig;
import com.starburstdata.presto.plugin.snowflake.auth.SnowflakeAuthClient;
import com.starburstdata.presto.plugin.snowflake.auth.SnowflakeCredentialProviderConfig;
import com.starburstdata.presto.plugin.snowflake.auth.SnowflakeOauthConfig;
import com.starburstdata.presto.plugin.snowflake.auth.SnowflakeOauthService;
import com.starburstdata.presto.plugin.snowflake.auth.StatsCollectingOktaAuthClient;
import com.starburstdata.presto.plugin.snowflake.auth.StatsCollectingSnowflakeAuthClient;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.prestosql.plugin.jdbc.AuthToLocal;
import io.prestosql.plugin.jdbc.AuthToLocalModule;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.BaseJdbcConnectionPoolConfig;
import io.prestosql.plugin.jdbc.BaseJdbcStatisticsConfig;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.PollingConnectionFactory;
import io.prestosql.plugin.jdbc.caching.BaseJdbcCachingConfig;
import io.prestosql.plugin.jdbc.credential.CredentialProvider;
import io.prestosql.plugin.jdbc.credential.DefaultCredentialPropertiesProvider;
import io.prestosql.plugin.jdbc.credential.PassThroughCredentialProvider;
import io.prestosql.spi.PrestoException;
import org.weakref.jmx.ObjectNameBuilder;

import javax.inject.Qualifier;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Properties;

import static io.airlift.configuration.ConditionalModule.installModuleIf;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.prestosql.spi.StandardErrorCode.CONSTRAINT_VIOLATION;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class SnowflakeJdbcClientModule
        extends AbstractConfigurationAwareModule
{
    private String catalogName;
    private boolean distributedConnector;

    public SnowflakeJdbcClientModule(String catalogName, boolean distributedConnector)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.distributedConnector = distributedConnector;
    }

    @Override
    protected void setup(Binder binder)
    {
        binder.bind(JdbcClient.class).to(SnowflakeClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(BaseJdbcConfig.class);
        configBinder(binder).bindConfig(BaseJdbcCachingConfig.class);
        configBinder(binder).bindConfig(BaseJdbcStatisticsConfig.class);
        configBinder(binder).bindConfig(BaseJdbcConnectionPoolConfig.class);

        install(new SnowflakeConnectorObjectNameGeneratorModule(catalogName));

        install(installModuleIf(
                SnowflakeConfig.class,
                config -> config.getImpersonationType() == SnowflakeImpersonationType.NONE,
                noImpersonationModule()));

        install(installModuleIf(
                SnowflakeConfig.class,
                config -> config.getImpersonationType() == SnowflakeImpersonationType.ROLE,
                roleImpersonationModule()));

        install(installModuleIf(
                SnowflakeConfig.class,
                config -> config.getImpersonationType() == SnowflakeImpersonationType.OKTA_LDAP_PASSTHROUGH,
                oauthImpersonationModule(false)));

        install(installModuleIf(
                SnowflakeConfig.class,
                config -> config.getImpersonationType() == SnowflakeImpersonationType.ROLE_OKTA_LDAP_PASSTHROUGH,
                oauthImpersonationModule(true)));
    }

    @Provides
    @Singleton
    public SnowflakeClient getSnowflakeClient(BaseJdbcConfig config, BaseJdbcStatisticsConfig statisticsConfig, ConnectionFactory connectionFactory)
    {
        return new SnowflakeClient(config, statisticsConfig, connectionFactory, distributedConnector);
    }

    @Provides
    @Singleton
    @ForSnowflakeJdbcConnectionFactory
    public ConnectionFactory getBaseConnectionFactory(BaseJdbcConfig config, BaseJdbcConnectionPoolConfig connectionPoolingConfig, CredentialProvider credentialProvider, SnowflakeConfig snowflakeConfig)
    {
        if (connectionPoolingConfig.isConnectionPoolEnabled()) {
            return new PollingConnectionFactory(
                    catalogName,
                    SnowflakeProxyDriver.class,
                    getConnectionProperties(snowflakeConfig),
                    config,
                    connectionPoolingConfig,
                    new DefaultCredentialPropertiesProvider(credentialProvider));
        }

        return new DriverConnectionFactory(
                new SnowflakeProxyDriver(),
                config.getConnectionUrl(),
                getConnectionProperties(snowflakeConfig),
                credentialProvider);
    }

    private Module oauthImpersonationModule(boolean roleImpersonation)
    {
        return new AbstractConfigurationAwareModule()
        {
            @Override
            protected void setup(Binder binder)
            {
                if (buildConfigObject(SnowflakeConfig.class).getRole().isPresent()) {
                    throw new PrestoException(CONSTRAINT_VIOLATION, "Snowflake role should not be set when impersonation is enabled");
                }
                install(new AuthToLocalModule(catalogName));

                configBinder(binder).bindConfig(OktaConfig.class);
                configBinder(binder).bindConfig(SnowflakeOauthConfig.class);
                configBinder(binder).bindConfig(SnowflakeCredentialProviderConfig.class);
                binder.bind(PassThroughCredentialProvider.class).in(Scopes.SINGLETON);
                binder.bind(NativeOktaAuthClient.class).in(Scopes.SINGLETON);
                binder.bind(OktaAuthClient.class).to(StatsCollectingOktaAuthClient.class).in(Scopes.SINGLETON);
                binder.bind(SnowflakeAuthClient.class).to(StatsCollectingSnowflakeAuthClient.class).in(Scopes.SINGLETON);
                newExporter(binder).export(StatsCollectingOktaAuthClient.class)
                        .as(generateJmxName("StatsCollectingOktaAuthClient"));
                newExporter(binder).export(StatsCollectingSnowflakeAuthClient.class)
                        .as(generateJmxName("StatsCollectingSnowflakeAuthClient"));
            }

            private String generateJmxName(String type)
            {
                // Not using ObjectNames.generatedNameOf, because class names are gone after obfuscation
                return new ObjectNameBuilder("presto.plugin.snowflake.auth")
                        .withProperty("type", type)
                        .withProperty("name", catalogName)
                        .build();
            }

            @Provides
            @Singleton
            public NativeSnowflakeAuthClient getNativeSnowflakeAuthClient(SnowflakeOauthConfig snowflakeOauthConfig, AuthToLocal authToLocal)
            {
                if (roleImpersonation) {
                    return new NativeSnowflakeAuthClient(snowflakeOauthConfig, authToLocal);
                }
                return new NativeSnowflakeAuthClient(snowflakeOauthConfig);
            }

            @ForOauth
            @Provides
            @Singleton
            public CredentialProvider getCredentialProvider(
                    SnowflakeCredentialProviderConfig config,
                    CredentialProvider extraCredentialProvider,
                    PassThroughCredentialProvider passThroughCredentialProvider)
            {
                if (config.isUseExtraCredentials()) {
                    return extraCredentialProvider;
                }
                else {
                    return passThroughCredentialProvider;
                }
            }

            @Provides
            @Singleton
            public StatsCollectingOktaAuthClient getStatsCollectingOktaAuthClient(NativeOktaAuthClient delegate)
            {
                return new StatsCollectingOktaAuthClient(delegate);
            }

            @Provides
            @Singleton
            public StatsCollectingSnowflakeAuthClient getStatsCollectingSnowflakeAuthClient(NativeSnowflakeAuthClient delegate)
            {
                return new StatsCollectingSnowflakeAuthClient(delegate);
            }

            @Provides
            @Singleton
            public DefaultSnowflakeOauthService getDefaultSnowflakeOauthService(
                    SnowflakeAuthClient snowflakeAuthClient,
                    OktaAuthClient oktaAuthClient,
                    @ForOauth CredentialProvider credentialProvider)
            {
                return new DefaultSnowflakeOauthService(snowflakeAuthClient, oktaAuthClient, credentialProvider);
            }

            @Provides
            @Singleton
            public SnowflakeOauthService getSnowflakeOauthService(DefaultSnowflakeOauthService delegate, @ForOauth CredentialProvider credentialProvider, SnowflakeOauthConfig config)
            {
                return new CachingSnowflakeOauthService(delegate, credentialProvider, config.getCredentialTtl(), config.getCredentialCacheSize());
            }

            @Provides
            @Singleton
            public ConnectionFactory getConnectionFactory(
                    BaseJdbcConfig config,
                    BaseJdbcConnectionPoolConfig connectionPoolingConfig,
                    SnowflakeOauthService snowflakeOauthService,
                    SnowflakeConfig snowflakeConfig)
            {
                if (connectionPoolingConfig.isConnectionPoolEnabled()) {
                    return new PollingConnectionFactory(
                            catalogName,
                            SnowflakeProxyDriver.class,
                            getConnectionProperties(snowflakeConfig),
                            config,
                            connectionPoolingConfig,
                            new SnowflakeOauthPropertiesProvider(snowflakeOauthService));
                }

                return new DriverConnectionFactory(
                        new SnowflakeProxyDriver(),
                        config.getConnectionUrl(),
                        getConnectionProperties(snowflakeConfig),
                        new SnowflakeOauthPropertiesProvider(snowflakeOauthService));
            }

            @Provides
            @Singleton
            public OauthToLocal getAuthToLocal(SnowflakeOauthService snowflakeOauthService)
            {
                return new OauthToLocal(snowflakeOauthService);
            }
        };
    }

    private Module roleImpersonationModule()
    {
        return new AbstractConfigurationAwareModule()
        {
            @Override
            protected void setup(Binder binder)
            {
                if (buildConfigObject(SnowflakeConfig.class).getRole().isPresent()) {
                    throw new PrestoException(CONSTRAINT_VIOLATION, "Snowflake role should not be set when impersonation is enabled");
                }

                install(new AuthToLocalModule(catalogName));
            }

            @Provides
            @Singleton
            public ConnectionFactory getConnectionFactory(@ForSnowflakeJdbcConnectionFactory ConnectionFactory connectionFactory, AuthToLocal authToLocal)
            {
                return new SnowflakeImpersonationConnectionFactory(connectionFactory, authToLocal);
            }
        };
    }

    private Module noImpersonationModule()
    {
        return new AbstractConfigurationAwareModule()
        {
            @Override
            protected void setup(Binder binder)
            {
                if (!buildConfigObject(SnowflakeConfig.class).getRole().isPresent()) {
                    throw new PrestoException(CONSTRAINT_VIOLATION, "No snowflake role has been provided");
                }
            }

            @Provides
            @Singleton
            public ConnectionFactory getConnectionFactory(@ForSnowflakeJdbcConnectionFactory ConnectionFactory connectionFactory)
            {
                return connectionFactory;
            }
        };
    }

    private static Properties getConnectionProperties(SnowflakeConfig snowflakeConfig)
    {
        requireNonNull(snowflakeConfig, "snowflakeConfig is null");
        Properties properties = new Properties();
        snowflakeConfig.getRole().ifPresent(role -> properties.put("role", role));
        snowflakeConfig.getWarehouse().ifPresent(warehouse -> properties.put("warehouse", warehouse));
        snowflakeConfig.getDatabase().ifPresent(database -> properties.put("db", database));

        return properties;
    }

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @Qualifier
    public @interface ForOauth
    {
    }

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @Qualifier
    private @interface ForSnowflakeJdbcConnectionFactory
    {
    }
}
