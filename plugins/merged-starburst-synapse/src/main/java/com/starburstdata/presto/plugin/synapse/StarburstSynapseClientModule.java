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
import com.google.inject.Scopes;
import com.starburstdata.presto.plugin.jdbc.StarburstJdbcMetadataFactory;
import com.starburstdata.presto.plugin.jdbc.dynamicfiltering.ForDynamicFiltering;
import com.starburstdata.presto.plugin.jdbc.redirection.JdbcTableScanRedirectionModule;
import com.starburstdata.presto.plugin.jdbc.stats.JdbcStatisticsConfig;
import com.starburstdata.presto.plugin.sqlserver.StarburstCommonSqlServerConfig;
import com.starburstdata.presto.plugin.sqlserver.StarburstCommonSqlServerSessionProperties;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcJoinPushdownSupportModule;
import io.trino.plugin.jdbc.JdbcMetadataFactory;
import io.trino.plugin.jdbc.JdbcRecordSetProvider;
import io.trino.plugin.jdbc.JdbcSplitManager;
import io.trino.plugin.jdbc.MaxDomainCompactionThreshold;
import io.trino.plugin.sqlserver.SqlServerConfig;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSplitManager;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.jdbc.JdbcModule.bindSessionPropertiesProvider;
import static io.trino.plugin.sqlserver.SqlServerClient.SQL_SERVER_MAX_LIST_EXPRESSIONS;

public class StarburstSynapseClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(SqlServerConfig.class);
        // The SNAPSHOT ISOLATION seems not supported by Synapse, but the docs (
        // https://docs.microsoft.com/en-us/sql/t-sql/statements/set-transaction-isolation-level-transact-sql?view=sql-server-ver15) don't explain
        // whether this is the expected behavior.
        configBinder(binder).bindConfigDefaults(SqlServerConfig.class, config -> config.setSnapshotIsolationDisabled(true));

        configBinder(binder).bindConfig(StarburstCommonSqlServerConfig.class);

        newOptionalBinder(binder, JdbcMetadataFactory.class).setBinding().to(StarburstJdbcMetadataFactory.class).in(Scopes.SINGLETON);
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(StarburstSynapseClient.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, Key.get(int.class, MaxDomainCompactionThreshold.class)).setBinding().toInstance(SQL_SERVER_MAX_LIST_EXPRESSIONS);

        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);

        bindSessionPropertiesProvider(binder, StarburstCommonSqlServerSessionProperties.class);

        binder.bind(ConnectorSplitManager.class).annotatedWith(ForDynamicFiltering.class).to(JdbcSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorRecordSetProvider.class).annotatedWith(ForDynamicFiltering.class).to(JdbcRecordSetProvider.class).in(Scopes.SINGLETON);

        install(new StarburstSynapseAuthenticationModule());
        install(new JdbcJoinPushdownSupportModule());
        install(new JdbcTableScanRedirectionModule());
    }
}
