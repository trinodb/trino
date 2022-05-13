/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake.jdbc;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.starburstdata.trino.plugins.snowflake.SnowflakeConfig;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.configuration.ConfigBinder;
import io.trino.plugin.base.jmx.ConnectorObjectNameGeneratorModule;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.IdentityCacheMapping;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.MaxDomainCompactionThreshold;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.SingletonIdentityCacheMapping;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.credential.CredentialProviderModule;
import io.trino.plugin.jdbc.mapping.IdentifierMapping;
import net.snowflake.client.jdbc.SnowflakeDriver;

import javax.inject.Qualifier;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Properties;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static com.starburstdata.trino.plugins.snowflake.jdbc.SnowflakeClient.SNOWFLAKE_MAX_LIST_EXPRESSIONS;
import static com.starburstdata.trino.plugins.snowflake.jdbc.SnowflakeJdbcSessionProperties.WAREHOUSE;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.jdbc.JdbcModule.bindSessionPropertiesProvider;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

public class SnowflakeJdbcClientModule
        extends AbstractConfigurationAwareModule
{
    private static final String TIMESTAMP_FORMAT = "YYYY-MM-DD\"T\"HH24:MI:SS.FF9TZH:TZM";
    private static final String TIME_FORMAT = "HH24:MI:SS.FF9";

    private final String catalogName;
    // TODO If any module setup is needed by the JDBC client and needs to be disabled in the distributed connector,
    //  move all shared module configuration to a separate module and remove this field.
    private final boolean distributedConnector;

    public SnowflakeJdbcClientModule(String catalogName, boolean distributedConnector)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.distributedConnector = distributedConnector;
    }

    @Override
    protected void setup(Binder binder)
    {
        ConfigBinder.configBinder(binder).bindConfig(SnowflakeConfig.class);
        newOptionalBinder(binder, Key.get(JdbcClient.class, ForBaseJdbc.class))
                .setDefault()
                .to(SnowflakeClient.class)
                .in(Scopes.SINGLETON);
        newOptionalBinder(binder, Key.get(int.class, MaxDomainCompactionThreshold.class)).setBinding().toInstance(SNOWFLAKE_MAX_LIST_EXPRESSIONS);

        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);
        bindSessionPropertiesProvider(binder, SnowflakeJdbcSessionProperties.class);

        install(new CredentialProviderModule());

        install(new ConnectorObjectNameGeneratorModule(catalogName, "com.starburstdata.trino.plugins.snowflake", "starburst.plugin.snowflake"));

        newOptionalBinder(binder, Key.get(ConnectionFactory.class, ForBaseJdbc.class))
                .setDefault()
                .to(Key.get(ConnectionFactory.class, DefaultSnowflakeBinding.class))
                .in(Scopes.SINGLETON);

        binder.bind(IdentityCacheMapping.class).annotatedWith(ForWarehouseAware.class).to(SingletonIdentityCacheMapping.class).in(SINGLETON);
    }

    // TODO: Replace this method by annotating SnowflakeClient's constructor
    //       and binding a different parameter value for distributed/JDBC.
    @Provides
    @Singleton
    public SnowflakeClient getSnowflakeClient(
            BaseJdbcConfig config,
            JdbcStatisticsConfig statisticsConfig,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping)
    {
        return new SnowflakeClient(config, statisticsConfig, connectionFactory, distributedConnector, queryBuilder, identifierMapping);
    }

    @Provides
    @Singleton
    public IdentityCacheMapping getIdentityCacheMapping(@ForWarehouseAware IdentityCacheMapping delegate)
    {
        return new WarehouseAwareIdentityCacheMapping(delegate);
    }

    @Provides
    @Singleton
    @DefaultSnowflakeBinding
    public ConnectionFactory getConnectionFactory(
            BaseJdbcConfig config,
            CredentialProvider credentialProvider,
            SnowflakeConfig snowflakeConfig)
    {
        return new WarehouseAwareDriverConnectionFactory(
                new SnowflakeDriver(),
                config.getConnectionUrl(),
                getConnectionProperties(snowflakeConfig),
                credentialProvider);
    }

    public static Properties getConnectionProperties(SnowflakeConfig snowflakeConfig)
    {
        requireNonNull(snowflakeConfig, "snowflakeConfig is null");
        Properties properties = new Properties();

        snowflakeConfig.getWarehouse().ifPresent(warehouse -> properties.setProperty(WAREHOUSE, warehouse));
        snowflakeConfig.getDatabase().ifPresent(database -> properties.setProperty("db", database));
        snowflakeConfig.getRole().ifPresent(role -> properties.setProperty("role", role));

        properties.setProperty("JDBC_TREAT_DECIMAL_AS_INT", "false"); // avoid cast to Long which overflows
        properties.setProperty("TIMESTAMP_OUTPUT_FORMAT", TIMESTAMP_FORMAT);
        properties.setProperty("TIMESTAMP_NTZ_OUTPUT_FORMAT", TIMESTAMP_FORMAT);
        properties.setProperty("TIMESTAMP_TZ_OUTPUT_FORMAT", TIMESTAMP_FORMAT);
        properties.setProperty("TIMESTAMP_LTZ_OUTPUT_FORMAT", TIMESTAMP_FORMAT);
        properties.setProperty("TIME_OUTPUT_FORMAT", TIME_FORMAT);
        properties.setProperty("JSON_INDENT", "0");

        return properties;
    }

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @Qualifier
    public @interface ForWarehouseAware {}

    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD})
    @Qualifier
    public @interface DefaultSnowflakeBinding {}
}
