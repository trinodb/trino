package io.prestosql.plugin.influx;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class InfluxModule implements Module {

    @Override
    public void configure(Binder binder) {
        configBinder(binder).bindConfig(InfluxConfig.class);
        binder.bind(InfluxClient.class).in(Scopes.SINGLETON);
        binder.bind(InfluxConnector.class).in(Scopes.SINGLETON);
        binder.bind(InfluxMetadata.class).in(Scopes.SINGLETON);
        binder.bind(InfluxSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(InfluxRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(InfluxHandleResolver.class).in(Scopes.SINGLETON);
    }
}
