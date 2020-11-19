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
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import com.starburstdata.presto.plugin.jdbc.auth.ForAuthentication;
import com.starburstdata.presto.plugin.jdbc.auth.NoImpersonationModule;
import com.starburstdata.presto.plugin.jdbc.authtolocal.AuthToLocalModule;
import com.starburstdata.presto.plugin.jdbc.dynamicfiltering.ForDynamicFiltering;
import com.starburstdata.presto.plugin.jdbc.redirection.TableScanRedirectionModule;
import com.starburstdata.presto.plugin.jdbc.stats.JdbcStatisticsConfig;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.plugin.jdbc.ForBaseJdbc;
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.JdbcRecordSetProvider;
import io.prestosql.plugin.jdbc.JdbcSplitManager;
import io.prestosql.plugin.jdbc.credential.CredentialProvider;
import io.prestosql.plugin.jdbc.credential.CredentialProviderModule;
import io.prestosql.spi.connector.ConnectorRecordSetProvider;
import io.prestosql.spi.connector.ConnectorSplitManager;

import static io.airlift.configuration.ConditionalModule.installModuleIf;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class StarburstSqlServerClientModule
        extends AbstractConfigurationAwareModule
{
    private final String catalogName;

    public StarburstSqlServerClientModule(String catalogName)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(StarburstSqlServerClient.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);

        binder.bind(ConnectorSplitManager.class).annotatedWith(ForDynamicFiltering.class).to(JdbcSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorRecordSetProvider.class).annotatedWith(ForDynamicFiltering.class).to(JdbcRecordSetProvider.class).in(Scopes.SINGLETON);

        install(new CredentialProviderModule());

        install(installModuleIf(
                SqlServerConfig.class,
                SqlServerConfig::isImpersonationEnabled,
                new ImpersonationModule(),
                new NoImpersonationModule()));
        install(new TableScanRedirectionModule());
    }

    @Provides
    @Singleton
    @ForAuthentication
    public ConnectionFactory getConnectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider)
    {
        return new DriverConnectionFactory(new SQLServerDriver(), config, credentialProvider);
    }

    private class ImpersonationModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.install(new AuthToLocalModule(catalogName));
            binder.bind(ConnectionFactory.class).annotatedWith(ForBaseJdbc.class).to(SqlServerImpersonatingConnectionFactory.class).in(Scopes.SINGLETON);
        }
    }
}
