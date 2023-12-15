/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.dynamicfiltering;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.hive.HivePageSourceProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;

public final class TestingHiveDynamicRowFilteringExtension
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(ConnectorPageSourceProvider.class)
                .annotatedWith(ForDynamicRowFiltering.class)
                .to(HivePageSourceProvider.class)
                .in(Scopes.SINGLETON);
        binder.bind(Runnable.class)
                .annotatedWith(ForDynamicRowFiltering.class)
                .toInstance(() -> {});
        install(new DynamicRowFilteringModule(() -> true));
    }
}
