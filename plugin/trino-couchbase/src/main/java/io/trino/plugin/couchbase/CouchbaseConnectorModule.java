package io.trino.plugin.couchbase;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class CouchbaseConnectorModule
            extends AbstractConfigurationAwareModule {
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(CouchbaseConnector.class).in(Scopes.SINGLETON);
        binder.bind(CouchbaseMetadata.class).in(Scopes.SINGLETON);
        binder.bind(CouchbaseSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(CouchbasePageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(CouchbaseClient.class).in(Scopes.SINGLETON);

        newExporter(binder).export(CouchbaseClient.class).withGeneratedName();

        configBinder(binder).bindConfig(CouchbaseConfig.class);
    }
}
