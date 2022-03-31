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
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.starburstdata.presto.plugin.jdbc.JdbcConnectionPoolConfig;
import com.starburstdata.presto.plugin.jdbc.auth.AuthenticationBasedIdentityCacheMapping;
import com.starburstdata.presto.plugin.jdbc.auth.ForImpersonation;
import com.starburstdata.presto.plugin.jdbc.auth.PassThroughCredentialProvider;
import com.starburstdata.presto.plugin.jdbc.redirection.JdbcTableScanRedirectionModule;
import com.starburstdata.presto.plugin.jdbc.redirection.TableScanRedirection;
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
import com.starburstdata.presto.plugin.toolkit.authtolocal.AuthToLocal;
import com.starburstdata.presto.plugin.toolkit.authtolocal.AuthToLocalModule;
import com.starburstdata.presto.plugin.toolkit.security.multiple.tokens.TokenPassThroughConfig;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.jmx.ConnectorObjectNameGeneratorModule;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.ExtraCredentialsBasedIdentityCacheMapping;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.IdentityCacheMapping;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.MaxDomainCompactionThreshold;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.credential.CredentialProviderModule;
import io.trino.plugin.jdbc.credential.DefaultCredentialPropertiesProvider;
import io.trino.plugin.jdbc.mapping.IdentifierMapping;
import net.snowflake.client.jdbc.SnowflakeDriver;

import javax.inject.Qualifier;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkState;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static com.starburstdata.presto.plugin.snowflake.jdbc.SnowflakeClient.SNOWFLAKE_MAX_LIST_EXPRESSIONS;
import static com.starburstdata.presto.plugin.snowflake.jdbc.SnowflakeJdbcSessionProperties.WAREHOUSE;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.jdbc.JdbcModule.bindSessionPropertiesProvider;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class SnowflakeJdbcClientModule
        extends AbstractConfigurationAwareModule
{
    private static final String TIMESTAMP_FORMAT = "YYYY-MM-DD\"T\"HH24:MI:SS.FF9TZH:TZM";
    private static final String TIME_FORMAT = "HH24:MI:SS.FF9";

    private final String catalogName;
    private final boolean distributedConnector;

    public SnowflakeJdbcClientModule(String catalogName, boolean distributedConnector)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.distributedConnector = distributedConnector;
    }

    @Override
    protected void setup(Binder binder)
    {
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(SnowflakeClient.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, Key.get(int.class, MaxDomainCompactionThreshold.class)).setBinding().toInstance(SNOWFLAKE_MAX_LIST_EXPRESSIONS);

        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);
        configBinder(binder).bindConfig(JdbcConnectionPoolConfig.class);
        bindSessionPropertiesProvider(binder, SnowflakeJdbcSessionProperties.class);

        install(new CredentialProviderModule());

        install(new ConnectorObjectNameGeneratorModule(catalogName, "com.starburstdata.presto.plugin.snowflake", "starburst.plugin.snowflake"));

        install(conditionalModule(
                SnowflakeConfig.class,
                config -> config.getImpersonationType() == SnowflakeImpersonationType.NONE,
                noImpersonationModule()));

        install(conditionalModule(
                SnowflakeConfig.class,
                config -> config.getImpersonationType() == SnowflakeImpersonationType.ROLE,
                roleImpersonationModule()));

        install(conditionalModule(
                SnowflakeConfig.class,
                config -> config.getImpersonationType() == SnowflakeImpersonationType.OKTA_LDAP_PASSTHROUGH,
                oauthImpersonationModule(false)));

        install(conditionalModule(
                SnowflakeConfig.class,
                config -> config.getImpersonationType() == SnowflakeImpersonationType.ROLE_OKTA_LDAP_PASSTHROUGH,
                oauthImpersonationModule(true)));

        install(conditionalModule(
                SnowflakeConfig.class,
                config -> config.getImpersonationType() == SnowflakeImpersonationType.OAUTH2_PASSTHROUGH,
                oauth2PassthroughModule()));

        install(new JdbcTableScanRedirectionModule());
    }

    @Provides
    @Singleton
    public SnowflakeClient getSnowflakeClient(
            BaseJdbcConfig config,
            JdbcStatisticsConfig statisticsConfig,
            TableScanRedirection tableScanRedirection,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping)
    {
        return new SnowflakeClient(config, statisticsConfig, tableScanRedirection, connectionFactory, distributedConnector, queryBuilder, identifierMapping);
    }

    @Provides
    @Singleton
    public IdentityCacheMapping getIdentityCacheMapping(@ForWarehouseAware IdentityCacheMapping delegate)
    {
        return new WarehouseAwareIdentityCacheMapping(delegate);
    }

    @Provides
    @Singleton
    @ForImpersonation
    public ConnectionFactory getBaseConnectionFactory(
            BaseJdbcConfig config,
            JdbcConnectionPoolConfig connectionPoolingConfig,
            CredentialProvider credentialProvider,
            SnowflakeConfig snowflakeConfig,
            IdentityCacheMapping identityCacheMapping)
    {
        if (connectionPoolingConfig.isConnectionPoolEnabled()) {
            return new WarehouseAwarePoolingConnectionFactory(
                    catalogName,
                    SnowflakeDriver.class,
                    getConnectionProperties(snowflakeConfig),
                    config,
                    connectionPoolingConfig,
                    new DefaultCredentialPropertiesProvider(credentialProvider),
                    identityCacheMapping);
        }

        return new WarehouseAwareDriverConnectionFactory(
                new SnowflakeDriver(),
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
                    throw new IllegalStateException("Snowflake role should not be set when impersonation is enabled");
                }
                install(new AuthToLocalModule());
                binder.install(new WarehouseAwareAuthenticationBasedIdentityCacheMappingModule());

                configBinder(binder).bindConfig(OktaConfig.class);
                configBinder(binder).bindConfig(SnowflakeOauthConfig.class);
                configBinder(binder).bindConfig(SnowflakeCredentialProviderConfig.class);
                binder.bind(NativeOktaAuthClient.class).in(Scopes.SINGLETON);
                binder.bind(OktaAuthClient.class).to(StatsCollectingOktaAuthClient.class).in(Scopes.SINGLETON);
                binder.bind(SnowflakeAuthClient.class).to(StatsCollectingSnowflakeAuthClient.class).in(Scopes.SINGLETON);
                newExporter(binder).export(StatsCollectingOktaAuthClient.class).withGeneratedName();
                newExporter(binder).export(StatsCollectingSnowflakeAuthClient.class).withGeneratedName();
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
                    CredentialProvider extraCredentialProvider)
            {
                if (config.isUseExtraCredentials()) {
                    return extraCredentialProvider;
                }
                return new PassThroughCredentialProvider();
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
            @ForBaseJdbc
            public ConnectionFactory getConnectionFactory(
                    BaseJdbcConfig config,
                    JdbcConnectionPoolConfig connectionPoolingConfig,
                    SnowflakeOauthService snowflakeOauthService,
                    SnowflakeConfig snowflakeConfig,
                    IdentityCacheMapping identityCacheMapping)
            {
                if (connectionPoolingConfig.isConnectionPoolEnabled()) {
                    return new WarehouseAwarePoolingConnectionFactory(
                            catalogName,
                            SnowflakeDriver.class,
                            getConnectionProperties(snowflakeConfig),
                            config,
                            connectionPoolingConfig,
                            new SnowflakeOauthPropertiesProvider(snowflakeOauthService),
                            identityCacheMapping);
                }

                return new WarehouseAwareDriverConnectionFactory(
                        new SnowflakeDriver(),
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

    private Module oauth2PassthroughModule()
    {
        return new AbstractConfigurationAwareModule()
        {
            @Override
            protected void setup(Binder binder)
            {
                install(new WarehouseAwareAuthenticationBasedIdentityCacheMappingModule());
                configBinder(binder).bindConfig(TokenPassThroughConfig.class, "snowflake");
            }

            @Provides
            @Singleton
            @ForBaseJdbc
            public ConnectionFactory getConnectionFactory(
                    BaseJdbcConfig config,
                    JdbcConnectionPoolConfig connectionPoolingConfig,
                    SnowflakeConfig snowflakeConfig,
                    IdentityCacheMapping identityCacheMapping,
                    TokenPassThroughConfig tokenPassThroughConfig)
            {
                if (connectionPoolingConfig.isConnectionPoolEnabled()) {
                    return new WarehouseAwarePoolingConnectionFactory(
                            catalogName,
                            SnowflakeDriver.class,
                            getConnectionProperties(snowflakeConfig),
                            config,
                            connectionPoolingConfig,
                            new SnowflakeOAuth2TokenPassthroughProvider(tokenPassThroughConfig),
                            identityCacheMapping);
                }
                return new WarehouseAwareDriverConnectionFactory(
                        new SnowflakeDriver(),
                        config.getConnectionUrl(),
                        getConnectionProperties(snowflakeConfig),
                        new SnowflakeOAuth2TokenPassthroughProvider(tokenPassThroughConfig));
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
                checkState(buildConfigObject(SnowflakeConfig.class).getRole().isEmpty(), "Snowflake role should not be set when impersonation is enabled");
                install(new AuthToLocalModule());
                binder.install(new WarehouseAwareAuthenticationBasedIdentityCacheMappingModule());
                binder.bind(ConnectionFactory.class).annotatedWith(ForBaseJdbc.class).to(SnowflakeImpersonationConnectionFactory.class).in(Scopes.SINGLETON);
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
                install(new WarehouseAwareExtraCredentialsBasedIdentityCacheMappingModule());
            }

            @Provides
            @Singleton
            @ForBaseJdbc
            public ConnectionFactory getConnectionFactory(@ForImpersonation ConnectionFactory connectionFactory)
            {
                return connectionFactory;
            }
        };
    }

    private static Properties getConnectionProperties(SnowflakeConfig snowflakeConfig)
    {
        requireNonNull(snowflakeConfig, "snowflakeConfig is null");
        Properties properties = new Properties();

        snowflakeConfig.getRole().ifPresent(role -> properties.setProperty("role", role));
        snowflakeConfig.getWarehouse().ifPresent(warehouse -> properties.setProperty(WAREHOUSE, warehouse));
        snowflakeConfig.getDatabase().ifPresent(database -> properties.setProperty("db", database));

        properties.setProperty("JDBC_TREAT_DECIMAL_AS_INT", "false"); // avoid cast to Long which overflows
        properties.setProperty("TIMESTAMP_OUTPUT_FORMAT", TIMESTAMP_FORMAT);
        properties.setProperty("TIMESTAMP_NTZ_OUTPUT_FORMAT", TIMESTAMP_FORMAT);
        properties.setProperty("TIMESTAMP_TZ_OUTPUT_FORMAT", TIMESTAMP_FORMAT);
        properties.setProperty("TIMESTAMP_LTZ_OUTPUT_FORMAT", TIMESTAMP_FORMAT);
        properties.setProperty("TIME_OUTPUT_FORMAT", TIME_FORMAT);
        properties.setProperty("JSON_INDENT", "0");

        return properties;
    }

    private static class WarehouseAwareAuthenticationBasedIdentityCacheMappingModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.bind(IdentityCacheMapping.class)
                    .annotatedWith(ForWarehouseAware.class)
                    .to(AuthenticationBasedIdentityCacheMapping.class)
                    .in(Scopes.SINGLETON);
        }
    }

    private static class WarehouseAwareExtraCredentialsBasedIdentityCacheMappingModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.bind(IdentityCacheMapping.class)
                    .annotatedWith(ForWarehouseAware.class)
                    .to(ExtraCredentialsBasedIdentityCacheMapping.class)
                    .in(Scopes.SINGLETON);
        }
    }

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @Qualifier
    public @interface ForOauth {}

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @Qualifier
    public @interface ForWarehouseAware {}
}
