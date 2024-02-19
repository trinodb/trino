/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.snowflake.jdbc;

import com.google.inject.Binder;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.starburstdata.trino.plugin.jdbc.JdbcConnectionPoolConfig;
import com.starburstdata.trino.plugin.snowflake.SnowflakeConfig;
import com.starburstdata.trino.plugin.snowflake.SnowflakeConnectorFlavour;
import com.starburstdata.trino.plugin.snowflake.SnowflakeProxyConfig;
import com.starburstdata.trino.plugin.snowflake.SnowflakeSessionProperties;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.configuration.ConfigBinder;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.base.jmx.ConnectorObjectNameGeneratorModule;
import io.trino.plugin.base.mapping.IdentifierMapping;
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
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.plugin.jdbc.ptf.Query;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.type.TypeManager;
import net.snowflake.client.jdbc.SnowflakeDriver;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.lang.management.ManagementFactory;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeConnectorFlavour.DISTRIBUTED;
import static com.starburstdata.trino.plugin.snowflake.jdbc.SnowflakeClient.SNOWFLAKE_MAX_LIST_EXPRESSIONS;
import static com.starburstdata.trino.plugin.snowflake.jdbc.SnowflakeJdbcSessionProperties.WAREHOUSE;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.jdbc.JdbcModule.bindSessionPropertiesProvider;
import static java.lang.String.join;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

public class SnowflakeJdbcClientModule
        extends AbstractConfigurationAwareModule
{
    // TODO If any module setup is needed by the JDBC client and needs to be disabled in the distributed connector,
    //  move all shared module configuration to a separate module and remove this field.
    private final SnowflakeConnectorFlavour connectorFlavour;

    public SnowflakeJdbcClientModule(SnowflakeConnectorFlavour connectorFlavour)
    {
        this.connectorFlavour = connectorFlavour;
    }

    @Override
    protected void setup(Binder binder)
    {
        verifyPackageAccessAllowed(binder);

        ConfigBinder.configBinder(binder).bindConfig(SnowflakeConfig.class);
        newOptionalBinder(binder, Key.get(JdbcClient.class, ForBaseJdbc.class))
                .setDefault()
                .to(SnowflakeClient.class)
                .in(SINGLETON);
        newOptionalBinder(binder, Key.get(int.class, MaxDomainCompactionThreshold.class)).setBinding().toInstance(SNOWFLAKE_MAX_LIST_EXPRESSIONS);

        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);
        bindSessionPropertiesProvider(binder, SnowflakeSessionProperties.class);
        bindSessionPropertiesProvider(binder, SnowflakeJdbcSessionProperties.class);

        configBinder(binder).bindConfig(JdbcConnectionPoolConfig.class);

        install(new CredentialProviderModule());

        install(new ConnectorObjectNameGeneratorModule("com.starburstdata.trino.plugin.snowflake", "starburst.plugin.snowflake"));

        newOptionalBinder(binder, Key.get(IdentityCacheMapping.class, ForWarehouseAware.class))
                .setDefault()
                .to(SingletonIdentityCacheMapping.class)
                .in(SINGLETON);

        newOptionalBinder(binder, Key.get(Properties.class, ForSnowflakeConnectionFactory.class))
                .setDefault()
                .toProvider(SnowflakeDefaultConnectionPropertiesProvider.class)
                .in(SINGLETON);

        newOptionalBinder(binder, Key.get(ConnectionFactory.class, ForBaseJdbc.class))
                .setDefault().to(Key.get(ConnectionFactory.class, SnowflakeDefaultConnectionFactory.class));

        SnowflakeConfig snowflakeConfig = buildConfigObject(SnowflakeConfig.class);

        if (snowflakeConfig.isProxyEnabled()) {
            if (connectorFlavour == DISTRIBUTED) {
                binder.addError("Distributed connector does not support proxy settings");
            }

            configBinder(binder).bindConfig(SnowflakeProxyConfig.class);
            newOptionalBinder(binder, Key.get(Properties.class, ForSnowflakeConnectionFactory.class))
                    .setBinding()
                    .toProvider(SnowflakeProxyConnectionPropertiesProvider.class)
                    .in(SINGLETON);
        }

        if (connectorFlavour != DISTRIBUTED) {
            // The distributed connector doesn't use JDBC for query results fetching so query passthrough doesn't work as expected
            newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(Query.class).in(SINGLETON);
        }
    }

    @Provides
    @Singleton
    public SnowflakeClient getSnowflakeClient(
            BaseJdbcConfig config,
            SnowflakeConfig snowflakeConfig,
            JdbcStatisticsConfig statisticsConfig,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            TypeManager typeManager,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier)
    {
        return new SnowflakeClient(config, snowflakeConfig, statisticsConfig, connectionFactory, connectorFlavour, queryBuilder, typeManager, identifierMapping, queryModifier);
    }

    @Provides
    @Singleton
    public IdentityCacheMapping getIdentityCacheMapping(@ForWarehouseAware IdentityCacheMapping delegate)
    {
        return new WarehouseAwareIdentityCacheMapping(delegate);
    }

    @Provides
    @Singleton
    @SnowflakeDefaultConnectionFactory
    public ConnectionFactory getConnectionFactory(
            BaseJdbcConfig config,
            CredentialProvider credentialProvider,
            CatalogName catalogName,
            JdbcConnectionPoolConfig connectionPoolingConfig,
            IdentityCacheMapping identityCacheMapping,
            @ForSnowflakeConnectionFactory Properties connectionProperties)
    {
        return getDriverConnectionFactory(
                config,
                credentialProvider,
                catalogName,
                connectionPoolingConfig,
                identityCacheMapping,
                connectionProperties);
    }

    protected ConnectionFactory getDriverConnectionFactory(
            BaseJdbcConfig config,
            CredentialProvider credentialProvider,
            CatalogName catalogName,
            JdbcConnectionPoolConfig connectionPoolingConfig,
            IdentityCacheMapping identityCacheMapping,
            Properties connectionProperties)
    {
        if (connectionPoolingConfig.isConnectionPoolEnabled()) {
            return new WarehouseAwareDriverPoolingConnectionFactory(
                    catalogName.toString(),
                    connectionProperties,
                    config,
                    connectionPoolingConfig,
                    credentialProvider,
                    identityCacheMapping);
        }
        return new WarehouseAwareDriverConnectionFactory(
                new SnowflakeDriver(),
                config.getConnectionUrl(),
                connectionProperties,
                credentialProvider);
    }

    private static class SnowflakeProxyConnectionPropertiesProvider
            extends SnowflakeDefaultConnectionPropertiesProvider
    {
        private final SnowflakeProxyConfig snowflakeProxyConfig;

        @Inject
        public SnowflakeProxyConnectionPropertiesProvider(
                SnowflakeConfig snowflakeConfig,
                SnowflakeProxyConfig snowflakeProxyConfig)
        {
            super(snowflakeConfig);
            this.snowflakeProxyConfig = requireNonNull(snowflakeProxyConfig, "snowflakeProxyConfig is null");
        }

        @Override
        public Properties get()
        {
            Properties properties = super.get();
            properties.setProperty("useProxy", "true");
            properties.setProperty("proxyHost", snowflakeProxyConfig.getProxyHost());
            properties.setProperty("proxyPort", String.valueOf(snowflakeProxyConfig.getProxyPort()));
            properties.setProperty("proxyProtocol", snowflakeProxyConfig.getProxyProtocol().name().toLowerCase(ENGLISH));
            // see https://docs.snowflake.com/en/developer-guide/jdbc/jdbc-configure#bypassing-the-proxy-server
            properties.setProperty("nonProxyHosts", join("%7C", snowflakeProxyConfig.getNonProxyHosts()));
            snowflakeProxyConfig.getUsername().ifPresent(username -> properties.setProperty("proxyUser", username));
            snowflakeProxyConfig.getPassword().ifPresent(password -> properties.setProperty("proxyPassword", password));
            return properties;
        }
    }

    // public for use in galaxy-trino
    public static class SnowflakeDefaultConnectionPropertiesProvider
            implements Provider<Properties>
    {
        private static final String TIMESTAMP_FORMAT = "YYYY-MM-DD\"T\"HH24:MI:SS.FF9TZH:TZM";
        private static final String TIME_FORMAT = "HH24:MI:SS.FF9";

        private final SnowflakeConfig snowflakeConfig;

        @Inject
        public SnowflakeDefaultConnectionPropertiesProvider(SnowflakeConfig snowflakeConfig)
        {
            this.snowflakeConfig = requireNonNull(snowflakeConfig, "snowflakeConfig is null");
        }

        @Override
        public Properties get()
        {
            Properties properties = new Properties();

            snowflakeConfig.getWarehouse().ifPresent(warehouse -> properties.setProperty(WAREHOUSE, warehouse));
            snowflakeConfig.getDatabase().ifPresent(database -> properties.setProperty("db", database));
            snowflakeConfig.getRole().ifPresent(role -> properties.setProperty("role", role));

            properties.setProperty("JDBC_TREAT_DECIMAL_AS_INT", "false"); // avoid cast to Long which overflows
            properties.setProperty("JDBC_USE_SESSION_TIMEZONE", "false");
            properties.setProperty("TIMESTAMP_OUTPUT_FORMAT", TIMESTAMP_FORMAT);
            properties.setProperty("TIMESTAMP_NTZ_OUTPUT_FORMAT", TIMESTAMP_FORMAT);
            properties.setProperty("TIMESTAMP_TZ_OUTPUT_FORMAT", TIMESTAMP_FORMAT);
            properties.setProperty("TIMESTAMP_LTZ_OUTPUT_FORMAT", TIMESTAMP_FORMAT);
            properties.setProperty("TIME_OUTPUT_FORMAT", TIME_FORMAT);
            properties.setProperty("JSON_INDENT", "0");
            properties.setProperty("CLIENT_OUT_OF_BAND_TELEMETRY_ENABLED", "false");

            return properties;
        }
    }

    /**
     * The Snowflake JDBC requires reflective access to certain Java internals since Java 17.
     *
     * @throws IllegalArgumentException if the appropriate arguments were not provided to the JVM.
     */
    public static void verifyPackageAccessAllowed(Binder binder)
    {
        // Match an --add-opens argument that opens a package to unnamed modules.
        // The first group is the opened package.
        Pattern argPattern = Pattern.compile(
                "^--add-opens=(.*)=([A-Za-z0-9_.]+,)*ALL-UNNAMED(,[A-Za-z0-9_.]+)*$");
        // We don't need to check for a values in separate arguments because
        // they are joined with "=" before we get them.

        Set<String> openedModules = ManagementFactory.getRuntimeMXBean()
                .getInputArguments()
                .stream()
                .map(argPattern::matcher)
                .filter(Matcher::matches)
                .map(matcher -> matcher.group(1))
                .collect(toSet());

        if (!openedModules.contains("java.base/java.nio")) {
            binder.addError(
                    "The Snowflake connector requires a JVM argument to run on Java 17: "
                            + "--add-opens=java.base/java.nio=ALL-UNNAMED");
        }
    }

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @BindingAnnotation
    public @interface ForWarehouseAware {}

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @BindingAnnotation
    public @interface ForSnowflakeConnectionFactory {}
}
