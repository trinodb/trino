/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.synapse;

import com.google.inject.Binder;
import com.google.inject.BindingAnnotation;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import com.starburstdata.trino.plugin.jdbc.JdbcConnectionPoolConfig;
import com.starburstdata.trino.plugin.jdbc.PoolingConnectionFactory;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.ForJdbcDynamicFiltering;
import io.trino.plugin.jdbc.IdentityCacheMapping;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcJoinPushdownSupportModule;
import io.trino.plugin.jdbc.JdbcRecordSetProvider;
import io.trino.plugin.jdbc.JdbcSplitManager;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.MaxDomainCompactionThreshold;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.ptf.Query;
import io.trino.plugin.sqlserver.SqlServerConfig;
import io.trino.plugin.sqlserver.SqlServerConnectionFactory;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.function.table.ConnectorTableFunction;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.jdbc.JdbcModule.bindSessionPropertiesProvider;
import static io.trino.plugin.sqlserver.SqlServerClient.SQL_SERVER_MAX_LIST_EXPRESSIONS;

public class StarburstSynapseModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected final void setup(Binder binder)
    {
        configBinder(binder).bindConfig(SqlServerConfig.class);
        configBinder(binder).bindConfig(JdbcConnectionPoolConfig.class);
        // The SNAPSHOT ISOLATION seems not supported by Synapse, but the docs (
        // https://docs.microsoft.com/en-us/sql/t-sql/statements/set-transaction-isolation-level-transact-sql?view=sql-server-ver15) don't explain
        // whether this is the expected behavior.
        configBinder(binder).bindConfigDefaults(SqlServerConfig.class, config -> config.setSnapshotIsolationDisabled(true));
        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);
        newOptionalBinder(binder, Key.get(int.class, MaxDomainCompactionThreshold.class)).setBinding().toInstance(SQL_SERVER_MAX_LIST_EXPRESSIONS);

        install(new JdbcJoinPushdownSupportModule());

        bindSessionPropertiesProvider(binder, StarburstSynapseSessionProperties.class);

        newOptionalBinder(binder, Key.get(JdbcClient.class, ForBaseJdbc.class))
                .setDefault()
                .to(StarburstSynapseClient.class)
                .in(Scopes.SINGLETON);

        newOptionalBinder(binder, Key.get(ConnectorSplitManager.class, ForJdbcDynamicFiltering.class))
                .setDefault()
                .to(JdbcSplitManager.class)
                .in(Scopes.SINGLETON);

        newOptionalBinder(binder, Key.get(ConnectorRecordSetProvider.class, ForBaseJdbc.class))
                .setDefault()
                .to(JdbcRecordSetProvider.class)
                .in(Scopes.SINGLETON);

        newOptionalBinder(binder, Key.get(ConnectionFactory.class, ForBaseJdbc.class))
                .setDefault()
                .to(Key.get(ConnectionFactory.class, DefaultSynapseBinding.class))
                .in(Scopes.SINGLETON);

        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(Query.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @DefaultSynapseBinding
    public static ConnectionFactory getConnectionFactory(
            CatalogName catalogName,
            BaseJdbcConfig config,
            JdbcConnectionPoolConfig connectionPoolingConfig,
            SqlServerConfig sqlServerConfig,
            CredentialProvider credentialProvider,
            IdentityCacheMapping identityCacheMapping)
    {
        if (connectionPoolingConfig.isConnectionPoolEnabled()) {
            return new SqlServerConnectionFactory(
                    new PoolingConnectionFactory(
                            catalogName.toString(),
                            SQLServerDriver.class,
                            config,
                            connectionPoolingConfig,
                            credentialProvider,
                            identityCacheMapping),
                    sqlServerConfig.isSnapshotIsolationDisabled());
        }
        return new SqlServerConnectionFactory(
                DriverConnectionFactory.builder(new SQLServerDriver(), config.getConnectionUrl(), credentialProvider).build(),
                sqlServerConfig.isSnapshotIsolationDisabled());
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD})
    @BindingAnnotation
    public @interface DefaultSynapseBinding {}
}
