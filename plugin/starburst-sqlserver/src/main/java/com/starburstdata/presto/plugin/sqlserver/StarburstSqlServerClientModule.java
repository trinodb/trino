/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.sqlserver;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Scopes;
import com.starburstdata.managed.statistics.connector.ConnectorStatisticsProvider;
import com.starburstdata.presto.plugin.jdbc.redirection.JdbcTableScanRedirectionModule;
import com.starburstdata.presto.plugin.jdbc.statistics.JdbcManagedStatisticsModule;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.ForJdbcDynamicFiltering;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcJoinPushdownSupportModule;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.MaxDomainCompactionThreshold;
import io.trino.plugin.jdbc.ptf.Procedure;
import io.trino.plugin.jdbc.ptf.Query;
import io.trino.plugin.sqlserver.SqlServerConfig;
import io.trino.plugin.sqlserver.SqlServerSessionProperties;
import io.trino.plugin.sqlserver.SqlServerTableProperties;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.ptf.ConnectorTableFunction;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.jdbc.JdbcModule.bindSessionPropertiesProvider;
import static io.trino.plugin.jdbc.JdbcModule.bindTablePropertiesProvider;
import static io.trino.plugin.sqlserver.SqlServerClient.SQL_SERVER_MAX_LIST_EXPRESSIONS;

public class StarburstSqlServerClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        newOptionalBinder(binder, Key.get(ConnectorSplitManager.class, ForJdbcDynamicFiltering.class)).setBinding().to(SqlServerSplitManager.class).in(SINGLETON);

        configBinder(binder).bindConfig(SqlServerConfig.class);

        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(StarburstSqlServerClient.class).in(SINGLETON);
        newOptionalBinder(binder, Key.get(int.class, MaxDomainCompactionThreshold.class)).setBinding().toInstance(SQL_SERVER_MAX_LIST_EXPRESSIONS);

        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);

        bindSessionPropertiesProvider(binder, SqlServerSessionProperties.class);
        bindSessionPropertiesProvider(binder, StarburstSqlServerSessionProperties.class);

        bindTablePropertiesProvider(binder, SqlServerTableProperties.class);

        install(new SqlServerAuthenticationModule());
        install(new CatalogOverridingModule());
        install(new JdbcJoinPushdownSupportModule());
        install(new JdbcTableScanRedirectionModule());
        install(new JdbcManagedStatisticsModule());
        newOptionalBinder(binder, ConnectorStatisticsProvider.class).setBinding().to(SqlServerCollectingStatisticsProvider.class).in(Scopes.SINGLETON);

        @SuppressWarnings("TrinoExperimentalSpi")
        Class<ConnectorTableFunction> clazz = ConnectorTableFunction.class;
        newSetBinder(binder, clazz).addBinding().toProvider(Query.class).in(Scopes.SINGLETON);
        install(conditionalModule(
                SqlServerConfig.class,
                SqlServerConfig::isStoredProcedureTableFunctionEnabled,
                internalBinder -> newSetBinder(internalBinder, clazz).addBinding().toProvider(Procedure.class).in(Scopes.SINGLETON)));
    }
}
