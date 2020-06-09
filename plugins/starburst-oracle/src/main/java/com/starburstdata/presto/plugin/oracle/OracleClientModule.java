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
import com.google.inject.Scopes;
import com.starburstdata.presto.plugin.jdbc.stats.JdbcStatisticsConfig;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.prestosql.plugin.jdbc.ForBaseJdbc;
import io.prestosql.plugin.jdbc.JdbcClient;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.prestosql.plugin.jdbc.JdbcModule.bindProcedure;
import static io.prestosql.plugin.jdbc.JdbcModule.bindSessionPropertiesProvider;
import static java.util.Objects.requireNonNull;

public class OracleClientModule
        extends AbstractConfigurationAwareModule
{
    private final String catalogName;

    public OracleClientModule(String catalogName)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        binder.bind(OracleSplitManager.class).in(Scopes.SINGLETON);

        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(OracleClient.class).in(Scopes.SINGLETON);

        bindProcedure(binder, AnalyzeProcedure.class);

        bindSessionPropertiesProvider(binder, OracleSessionProperties.class);

        configBinder(binder).bindConfig(OracleConfig.class);
        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);

        install(new OracleAuthenticationModule(catalogName));
    }
}
