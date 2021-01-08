/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.prestoconnector;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Scopes;
import com.starburstdata.presto.plugin.jdbc.dynamicfiltering.ForDynamicFiltering;
import com.starburstdata.presto.plugin.jdbc.redirection.TableScanRedirectionModule;
import com.starburstdata.presto.plugin.jdbc.stats.JdbcStatisticsConfig;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.prestosql.plugin.jdbc.ForBaseJdbc;
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.JdbcMetadataConfig;
import io.prestosql.plugin.jdbc.JdbcRecordSetProvider;
import io.prestosql.plugin.jdbc.JdbcSplitManager;
import io.prestosql.plugin.jdbc.MaxDomainCompactionThreshold;
import io.prestosql.spi.connector.ConnectorRecordSetProvider;
import io.prestosql.spi.connector.ConnectorSplitManager;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class PrestoConnectorModule
        extends AbstractConfigurationAwareModule
{
    // Values below are set based on tests. Automated tests cover domains with up to 5_000 values.
    // We also did manual test with domain with 50_000 values.
    public static final int PRESTO_CONNECTOR_DEFAULT_DOMAIN_COMPACTION_THRESHOLD = 5_000;
    public static final int PRESTO_CONNECTOR_MAX_DOMAIN_COMPACTION_THRESHOLD = 50_000;

    @Override
    public void setup(Binder binder)
    {
        configBinder(binder).bindConfig(PrestoConnectorConfig.class);
        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);
        configBinder(binder).bindConfig(PrestoConnectorJdbcConfig.class);

        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(PrestoConnectorClient.class).in(Scopes.SINGLETON);

        binder.bind(ConnectorSplitManager.class).annotatedWith(ForDynamicFiltering.class).to(JdbcSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorRecordSetProvider.class).annotatedWith(ForDynamicFiltering.class).to(JdbcRecordSetProvider.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfigDefaults(JdbcMetadataConfig.class, config -> {
            config.setDomainCompactionThreshold(PRESTO_CONNECTOR_DEFAULT_DOMAIN_COMPACTION_THRESHOLD);
        });
        newOptionalBinder(binder, Key.get(int.class, MaxDomainCompactionThreshold.class)).setBinding().toInstance(PRESTO_CONNECTOR_MAX_DOMAIN_COMPACTION_THRESHOLD);

        install(new PrestoConnectorAuthenticationModule());
        install(new TableScanRedirectionModule());
    }
}
