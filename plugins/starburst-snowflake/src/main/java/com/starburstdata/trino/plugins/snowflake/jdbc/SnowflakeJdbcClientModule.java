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
import com.starburstdata.trino.plugins.snowflake.SnowflakeSessionProperties;
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
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.plugin.jdbc.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.ptf.Query;
import io.trino.spi.ptf.ConnectorTableFunction;
import io.trino.spi.type.TypeManager;
import net.snowflake.client.jdbc.SnowflakeDriver;

import javax.inject.Qualifier;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.management.ManagementFactory;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
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

    // TODO If any module setup is needed by the JDBC client and needs to be disabled in the distributed connector,
    //  move all shared module configuration to a separate module and remove this field.
    private final boolean distributedConnector;

    public SnowflakeJdbcClientModule(boolean distributedConnector)
    {
        this.distributedConnector = distributedConnector;
    }

    @Override
    protected void setup(Binder binder)
    {
        verifyPackageAccessAllowed(binder);

        ConfigBinder.configBinder(binder).bindConfig(SnowflakeConfig.class);
        newOptionalBinder(binder, Key.get(JdbcClient.class, ForBaseJdbc.class))
                .setDefault()
                .to(SnowflakeClient.class)
                .in(Scopes.SINGLETON);
        newOptionalBinder(binder, Key.get(int.class, MaxDomainCompactionThreshold.class)).setBinding().toInstance(SNOWFLAKE_MAX_LIST_EXPRESSIONS);

        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);
        bindSessionPropertiesProvider(binder, SnowflakeSessionProperties.class);
        bindSessionPropertiesProvider(binder, SnowflakeJdbcSessionProperties.class);

        install(new CredentialProviderModule());

        install(new ConnectorObjectNameGeneratorModule("com.starburstdata.trino.plugins.snowflake", "starburst.plugin.snowflake"));

        newOptionalBinder(binder, Key.get(ConnectionFactory.class, ForBaseJdbc.class))
                .setDefault()
                .to(Key.get(ConnectionFactory.class, DefaultSnowflakeBinding.class))
                .in(Scopes.SINGLETON);

        newOptionalBinder(binder, Key.get(IdentityCacheMapping.class, ForWarehouseAware.class))
                .setDefault()
                .to(SingletonIdentityCacheMapping.class)
                .in(Scopes.SINGLETON);

        if (!distributedConnector) {
            // The distributed connector doesn't use JDBC for query results fetching so query passthrough doesn't work as expected
            setupTableFunctions(binder);
        }
    }

    @SuppressWarnings("TrinoExperimentalSpi") // Allowed, as it was introduced before disallowing experimental SPIs usage
    private static void setupTableFunctions(Binder binder)
    {
        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(Query.class).in(Scopes.SINGLETON);
    }

    // TODO: Replace this method by annotating SnowflakeClient's constructor
    //       and binding a different parameter value for distributed/JDBC.
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
        return new SnowflakeClient(config, snowflakeConfig, statisticsConfig, connectionFactory, distributedConnector, queryBuilder, typeManager, identifierMapping, queryModifier);
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
        return getDriverConnectionFactory(config, credentialProvider, snowflakeConfig);
    }

    protected ConnectionFactory getDriverConnectionFactory(
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
                .collect(Collectors.toSet());

        if (!openedModules.contains("java.base/java.nio")) {
            binder.addError(
                    "The Snowflake connector requires a JVM argument to run on Java 17: "
                            + "--add-opens=java.base/java.nio=ALL-UNNAMED");
        }
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
