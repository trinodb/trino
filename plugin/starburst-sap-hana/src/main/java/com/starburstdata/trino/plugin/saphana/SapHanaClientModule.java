/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.saphana;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcJoinPushdownSupportModule;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.ptf.Query;
import io.trino.spi.function.table.ConnectorTableFunction;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class SapHanaClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    public void setup(Binder binder)
    {
        newOptionalBinder(binder, Key.get(JdbcClient.class, ForBaseJdbc.class))
                .setDefault()
                .to(SapHanaClient.class)
                .in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);

        install(new SapHanaAuthenticationModule());
        install(new JdbcJoinPushdownSupportModule());

        newOptionalBinder(binder, Key.get(ConnectionFactory.class, ForBaseJdbc.class))
                .setDefault()
                .to(Key.get(ConnectionFactory.class, SapHanaAuthenticationModule.DefaultSapHanaBinding.class))
                .in(Scopes.SINGLETON);

        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(Query.class).in(Scopes.SINGLETON);
    }
}
