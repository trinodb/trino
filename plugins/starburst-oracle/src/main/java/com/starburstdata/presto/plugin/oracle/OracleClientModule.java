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
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.BaseJdbcStatisticsConfig;
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.SessionPropertiesProvider;
import io.prestosql.plugin.jdbc.caching.BaseJdbcCachingConfig;
import io.prestosql.spi.procedure.Procedure;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class OracleClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(JdbcClient.class).to(OracleClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(BaseJdbcConfig.class);
        configBinder(binder).bindConfig(BaseJdbcCachingConfig.class);
        configBinder(binder).bindConfig(BaseJdbcStatisticsConfig.class);
        configBinder(binder).bindConfig(OracleConfig.class);
        Multibinder<Procedure> procedures = newSetBinder(binder, Procedure.class);
        procedures.addBinding().toProvider(AnalyzeProcedure.class).in(Scopes.SINGLETON);

        Multibinder<SessionPropertiesProvider> sessionProperties = newSetBinder(binder, SessionPropertiesProvider.class);
        sessionProperties.addBinding().to(OracleSessionProperties.class);
    }
}
