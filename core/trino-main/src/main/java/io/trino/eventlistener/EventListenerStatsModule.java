package io.trino.eventlistener;

import com.google.inject.AbstractModule;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import io.trino.plugin.base.CatalogName;

import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class EventListenerStatsModule
        extends AbstractModule
{
    @Override
    public void configure()
    {
        bind(EventListenerStats.class).in(Scopes.SINGLETON);
        Provider<CatalogName> catalogName = () -> new CatalogName("postgresql");
        newExporter(binder()).export(EventListenerStats.class)
                .as(generator -> generator.generatedNameOf(EventListenerStats.class, catalogName.get().toString()));
    }
}
