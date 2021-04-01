/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.starburstremote;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Scopes;
import com.starburstdata.presto.plugin.jdbc.JdbcJoinPushdownConfig;
import com.starburstdata.presto.plugin.jdbc.JdbcJoinPushdownSessionProperties;
import com.starburstdata.presto.plugin.jdbc.dynamicfiltering.ForDynamicFiltering;
import com.starburstdata.presto.plugin.jdbc.redirection.JdbcTableScanRedirectionModule;
import com.starburstdata.presto.plugin.jdbc.stats.JdbcStatisticsConfig;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcMetadataConfig;
import io.trino.plugin.jdbc.JdbcRecordSetProvider;
import io.trino.plugin.jdbc.JdbcSplitManager;
import io.trino.plugin.jdbc.MaxDomainCompactionThreshold;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSplitManager;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConditionalModule.installModuleIf;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.jdbc.JdbcModule.bindSessionPropertiesProvider;

public class StarburstRemoteModule
        extends AbstractConfigurationAwareModule
{
    // Values below are set based on tests. Automated tests cover domains with up to 5_000 values.
    // We also did manual test with domain with 50_000 values.
    public static final int STARBURST_REMOTE_DEFAULT_DOMAIN_COMPACTION_THRESHOLD = 5_000;
    public static final int STARBURST_REMOTE_MAX_DOMAIN_COMPACTION_THRESHOLD = 50_000;

    @Override
    public void setup(Binder binder)
    {
        configBinder(binder).bindConfig(StarburstRemoteConfig.class);
        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);
        configBinder(binder).bindConfig(StarburstRemoteJdbcConfig.class);
        configBinder(binder).bindConfig(JdbcJoinPushdownConfig.class);

        bindSessionPropertiesProvider(binder, JdbcJoinPushdownSessionProperties.class);

        install(installModuleIf(
                StarburstRemoteConfig.class,
                StarburstRemoteConfig::isSslEnabled,
                new SslModule(),
                new NoSslModule()));

        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(StarburstRemoteClient.class).in(Scopes.SINGLETON);

        binder.bind(ConnectorSplitManager.class).annotatedWith(ForDynamicFiltering.class).to(JdbcSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorRecordSetProvider.class).annotatedWith(ForDynamicFiltering.class).to(JdbcRecordSetProvider.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfigDefaults(JdbcMetadataConfig.class, config -> {
            config.setDomainCompactionThreshold(STARBURST_REMOTE_DEFAULT_DOMAIN_COMPACTION_THRESHOLD);
        });
        newOptionalBinder(binder, Key.get(int.class, MaxDomainCompactionThreshold.class)).setBinding().toInstance(STARBURST_REMOTE_MAX_DOMAIN_COMPACTION_THRESHOLD);

        install(new StarburstRemoteAuthenticationModule());
        install(new JdbcTableScanRedirectionModule());
    }

    private static class SslModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            configBinder(binder).bindConfig(StarburstRemoteSslConfig.class);
        }
    }

    private static class NoSslModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            // Bind the config to an instance rather than using 'bindConfig' so the properties cannot be set.
            binder.bind(StarburstRemoteSslConfig.class).toInstance(new StarburstRemoteSslConfig());
        }
    }
}
