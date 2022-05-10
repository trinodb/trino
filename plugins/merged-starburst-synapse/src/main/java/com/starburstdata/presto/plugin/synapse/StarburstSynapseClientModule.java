/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.synapse;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcJoinPushdownSupportModule;
import io.trino.plugin.jdbc.JdbcRecordSetProvider;
import io.trino.plugin.jdbc.JdbcSplitManager;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.MaxDomainCompactionThreshold;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.sqlserver.SqlServerConfig;
import io.trino.plugin.sqlserver.SqlServerConnectionFactory;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSplitManager;

import javax.inject.Named;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.jdbc.JdbcModule.bindSessionPropertiesProvider;
import static io.trino.plugin.sqlserver.SqlServerClient.SQL_SERVER_MAX_LIST_EXPRESSIONS;

public class StarburstSynapseClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected final void setup(Binder binder)
    {
        configBinder(binder).bindConfig(SqlServerConfig.class);
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

        newOptionalBinder(binder, Key.get(ConnectorSplitManager.class, ForBaseJdbc.class))
                .setDefault()
                .to(JdbcSplitManager.class)
                .in(Scopes.SINGLETON);

        newOptionalBinder(binder, Key.get(ConnectorRecordSetProvider.class, ForBaseJdbc.class))
                .setDefault()
                .to(JdbcRecordSetProvider.class)
                .in(Scopes.SINGLETON);

        newOptionalBinder(binder, Key.get(ConnectionFactory.class, ForBaseJdbc.class))
                .setDefault()
                .to(Key.get(ConnectionFactory.class, Names.named("default synapse connection factory")))
                .in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @Named("default synapse connection factory")
    public static ConnectionFactory getConnectionFactory(
            BaseJdbcConfig config,
            SqlServerConfig sqlServerConfig,
            CredentialProvider credentialProvider)
    {
        return new SqlServerConnectionFactory(
                new DriverConnectionFactory(new SQLServerDriver(), config, credentialProvider),
                sqlServerConfig.isSnapshotIsolationDisabled());
    }
}
