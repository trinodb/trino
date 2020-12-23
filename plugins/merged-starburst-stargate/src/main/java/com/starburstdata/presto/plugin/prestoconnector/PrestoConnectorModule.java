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
    // Limit was determined experimentally by running modified TestPrestoConnectorDistributedQueries#testLargeIn.
    // We started observing parser problems at around 1000 values with cold Presto code
    // and at around 2000 values after JIT triggered.
    public static final int PRESTO_CONNECTOR_DEFAULT_DOMAIN_COMPACTION_THRESHOLD = 500;

    @Override
    public void setup(Binder binder)
    {
        configBinder(binder).bindConfig(PrestoConnectorConfig.class);
        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);

        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(PrestoConnectorClient.class).in(Scopes.SINGLETON);

        binder.bind(ConnectorSplitManager.class).annotatedWith(ForDynamicFiltering.class).to(JdbcSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorRecordSetProvider.class).annotatedWith(ForDynamicFiltering.class).to(JdbcRecordSetProvider.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfigDefaults(JdbcMetadataConfig.class, config -> {
            config.setDomainCompactionThreshold(PRESTO_CONNECTOR_DEFAULT_DOMAIN_COMPACTION_THRESHOLD);
        });
        // TODO(https://github.com/prestosql/presto/issues/6075, https://starburstdata.atlassian.net/browse/PRESTO-4833)
        //  Setting threshold to more than 500 may result in stack overflow errors when pushed down query is parsed on remote Presto end.
        //  we should be able to drop this constraint after fixing linked issue, so `NOT IN` predicates are retained in this for in pushed-down predicate.
        newOptionalBinder(binder, Key.get(int.class, MaxDomainCompactionThreshold.class)).setBinding().toInstance(PRESTO_CONNECTOR_DEFAULT_DOMAIN_COMPACTION_THRESHOLD);

        install(new PrestoConnectorAuthenticationModule());
    }
}
