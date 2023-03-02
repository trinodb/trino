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
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;

import java.util.function.BooleanSupplier;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class DynamicRowFilteringModule
        implements Module
{
    private final BooleanSupplier enableFeature; // used for license check in SEP

    public DynamicRowFilteringModule(BooleanSupplier enableFeature)
    {
        this.enableFeature = enableFeature;
    }

    @Override
    public void configure(Binder binder)
    {
        Provider<CatalogName> catalogName = binder.getProvider(CatalogName.class);
        configBinder(binder).bindConfig(DynamicRowFilteringConfig.class);
        configBinder(binder).bindConfigDefaults(DynamicRowFilteringConfig.class, config ->
                config.setDynamicRowFilteringEnabled(enableFeature.getAsBoolean()));
        binder.bind(DynamicPageFilterCache.class).in(Scopes.SINGLETON);
        newExporter(binder).export(DynamicPageFilterCache.class)
                .as(generator -> generator.generatedNameOf(DynamicPageFilterCache.class, catalogName.get().toString()));
        Multibinder<SessionPropertiesProvider> sessionProperties = newSetBinder(binder, SessionPropertiesProvider.class);
        sessionProperties.addBinding().to(DynamicRowFilteringSessionProperties.class).in(SINGLETON);

        newOptionalBinder(binder, ConnectorPageSourceProvider.class)
                .setBinding().to(DynamicRowFilteringPageSourceProvider.class).in(SINGLETON);
    }
}
