/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.oracle;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Scopes;
import com.starburstdata.presto.license.LicenseManager;
import com.starburstdata.presto.plugin.jdbc.StarburstJdbcMetadataFactory;
import com.starburstdata.presto.plugin.jdbc.dynamicfiltering.DynamicFilteringModule;
import com.starburstdata.presto.plugin.jdbc.dynamicfiltering.ForDynamicFiltering;
import com.starburstdata.presto.plugin.jdbc.dynamicfiltering.jdbc.DynamicFilteringJdbcRecordSetProvider;
import com.starburstdata.presto.plugin.jdbc.redirection.JdbcTableScanRedirectionModule;
import com.starburstdata.presto.plugin.jdbc.stats.JdbcStatisticsConfig;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcJoinPushdownSupportModule;
import io.trino.plugin.jdbc.JdbcMetadataConfig;
import io.trino.plugin.jdbc.JdbcMetadataFactory;
import io.trino.plugin.jdbc.JdbcRecordSetProvider;
import io.trino.plugin.jdbc.MaxDomainCompactionThreshold;
import io.trino.plugin.oracle.OracleConfig;
import io.trino.plugin.oracle.OracleSessionProperties;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSplitManager;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static com.starburstdata.presto.license.StarburstFeature.ORACLE_EXTENSIONS;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.jdbc.JdbcModule.bindProcedure;
import static io.trino.plugin.jdbc.JdbcModule.bindSessionPropertiesProvider;
import static io.trino.plugin.oracle.OracleClient.ORACLE_MAX_LIST_EXPRESSIONS;
import static java.util.Objects.requireNonNull;

public class OracleClientModule
        extends AbstractConfigurationAwareModule
{
    private final String catalogName;
    private final LicenseManager licenseManager;

    public OracleClientModule(String catalogName, LicenseManager licenseManager)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.licenseManager = requireNonNull(licenseManager, "licenseManager is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        binder.bind(OracleSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorSplitManager.class).annotatedWith(ForDynamicFiltering.class)
                .to(OracleSplitManager.class).in(Scopes.SINGLETON);

        newOptionalBinder(binder, JdbcMetadataFactory.class).setBinding().to(StarburstJdbcMetadataFactory.class).in(Scopes.SINGLETON);
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(StarburstOracleClient.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, Key.get(int.class, MaxDomainCompactionThreshold.class)).setBinding().toInstance(ORACLE_MAX_LIST_EXPRESSIONS);

        bindProcedure(binder, AnalyzeProcedure.class);

        bindSessionPropertiesProvider(binder, StarburstOracleSessionProperties.class);
        bindSessionPropertiesProvider(binder, OracleSessionProperties.class);

        configBinder(binder).bindConfig(OracleConfig.class);
        configBinder(binder).bindConfig(StarburstOracleConfig.class);
        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);

        install(new OracleAuthenticationModule());

        install(new DynamicFilteringModule(catalogName, licenseManager));

        newOptionalBinder(binder, ConnectorRecordSetProvider.class).setBinding()
                .to(DynamicFilteringJdbcRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorRecordSetProvider.class).annotatedWith(ForDynamicFiltering.class)
                .to(JdbcRecordSetProvider.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfigDefaults(JdbcMetadataConfig.class, config -> config.setAggregationPushdownEnabled(licenseManager.hasFeature(ORACLE_EXTENSIONS)));

        install(new JdbcJoinPushdownSupportModule());
        install(new JdbcTableScanRedirectionModule());
    }
}
