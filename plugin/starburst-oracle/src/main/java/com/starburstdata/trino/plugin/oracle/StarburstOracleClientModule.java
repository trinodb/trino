/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.oracle;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Scopes;
import com.starburstdata.trino.plugin.license.LicenseManager;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.ForJdbcDynamicFiltering;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcJoinPushdownSupportModule;
import io.trino.plugin.jdbc.JdbcMetadataConfig;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.MaxDomainCompactionThreshold;
import io.trino.plugin.jdbc.ptf.Query;
import io.trino.plugin.oracle.OracleConfig;
import io.trino.plugin.oracle.OracleSessionProperties;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.function.table.ConnectorTableFunction;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static com.starburstdata.trino.plugin.oracle.StarburstOracleConfig.PASSWORD;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.jdbc.JdbcModule.bindProcedure;
import static io.trino.plugin.jdbc.JdbcModule.bindSessionPropertiesProvider;
import static io.trino.plugin.oracle.OracleClient.ORACLE_MAX_LIST_EXPRESSIONS;
import static java.util.Objects.requireNonNull;

public class StarburstOracleClientModule
        extends AbstractConfigurationAwareModule
{
    private final LicenseManager licenseManager;

    public StarburstOracleClientModule(LicenseManager licenseManager)
    {
        this.licenseManager = requireNonNull(licenseManager, "licenseManager is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        newOptionalBinder(binder, Key.get(ConnectorSplitManager.class, ForJdbcDynamicFiltering.class)).setBinding().to(OracleSplitManager.class).in(SINGLETON);

        newOptionalBinder(binder, Key.get(JdbcClient.class, ForBaseJdbc.class))
                .setDefault()
                .to(StarburstOracleClient.class)
                .in(Scopes.SINGLETON);
        newOptionalBinder(binder, Key.get(int.class, MaxDomainCompactionThreshold.class)).setBinding().toInstance(ORACLE_MAX_LIST_EXPRESSIONS);

        bindProcedure(binder, AnalyzeProcedure.class);

        binder.bind(LicenseManager.class).toInstance(licenseManager);
        bindSessionPropertiesProvider(binder, StarburstOracleSessionProperties.class);
        bindSessionPropertiesProvider(binder, OracleSessionProperties.class);

        configBinder(binder).bindConfig(OracleConfig.class);
        configBinder(binder).bindConfig(StarburstOracleConfig.class);
        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);

        configBinder(binder).bindConfigDefaults(JdbcMetadataConfig.class, config -> config.setAggregationPushdownEnabled(licenseManager.hasLicense()));

        install(new JdbcJoinPushdownSupportModule());

        install(conditionalModule(
                StarburstOracleConfig.class,
                config -> PASSWORD.equalsIgnoreCase(config.getAuthenticationType()),
                new UserPasswordModule()));

        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(Query.class).in(Scopes.SINGLETON);
    }
}
