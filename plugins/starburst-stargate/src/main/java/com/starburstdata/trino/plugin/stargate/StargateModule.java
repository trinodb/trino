/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.stargate;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Scopes;
import com.starburstdata.presto.plugin.jdbc.dynamicfiltering.ForDynamicFiltering;
import com.starburstdata.presto.plugin.jdbc.redirection.JdbcTableScanRedirectionModule;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcJoinPushdownSupportModule;
import io.trino.plugin.jdbc.JdbcMetadataConfig;
import io.trino.plugin.jdbc.JdbcMetadataFactory;
import io.trino.plugin.jdbc.JdbcRecordSetProvider;
import io.trino.plugin.jdbc.JdbcSplitManager;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.MaxDomainCompactionThreshold;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSplitManager;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class StargateModule
        extends AbstractConfigurationAwareModule
{
    // Values below are set based on tests. Automated tests cover domains with up to 5_000 values.
    // We also did manual test with domain with 50_000 values.
    public static final int STARGATE_DEFAULT_DOMAIN_COMPACTION_THRESHOLD = 5_000;
    public static final int STARGATE_MAX_DOMAIN_COMPACTION_THRESHOLD = 50_000;

    @Override
    public void setup(Binder binder)
    {
        configBinder(binder).bindConfig(StargateConfig.class);
        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);
        configBinder(binder).bindConfig(StargateJdbcConfig.class);

        install(conditionalModule(
                StargateConfig.class,
                StargateConfig::isSslEnabled,
                new SslModule(),
                new NoSslModule()));

        newOptionalBinder(binder, JdbcMetadataFactory.class).setBinding().to(StargateMetadataFactory.class).in(Scopes.SINGLETON);
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(StargateClient.class).in(Scopes.SINGLETON);

        binder.bind(ConnectorSplitManager.class).annotatedWith(ForDynamicFiltering.class).to(JdbcSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorRecordSetProvider.class).annotatedWith(ForDynamicFiltering.class).to(JdbcRecordSetProvider.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfigDefaults(JdbcMetadataConfig.class, config -> {
            config.setDomainCompactionThreshold(STARGATE_DEFAULT_DOMAIN_COMPACTION_THRESHOLD);
        });
        newOptionalBinder(binder, Key.get(int.class, MaxDomainCompactionThreshold.class)).setBinding().toInstance(STARGATE_MAX_DOMAIN_COMPACTION_THRESHOLD);

        install(new StargateAuthenticationModule());
        install(new JdbcJoinPushdownSupportModule());
        install(new JdbcTableScanRedirectionModule());
    }

    private static class SslModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            configBinder(binder).bindConfig(StargateSslConfig.class);
        }
    }

    private static class NoSslModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            // Bind the config to an instance rather than using 'bindConfig' so the properties cannot be set.
            binder.bind(StargateSslConfig.class).toInstance(new StargateSslConfig());
        }
    }
}
