package io.trino.loki;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

// import static io.airlift.configuration.ConfigBinder.configBinder;

public class LokiModule implements Module {
    @Override
    public void configure(Binder binder) {
        binder.bind(LokiConnector.class).in(Scopes.SINGLETON);
        //TODO binder.bind(LokiMetadata.class).in(Scopes.SINGLETON);
        //TODO binder.bind(LokiClient.class).in(Scopes.SINGLETON);
        //TODO binder.bind(LokiSplitManager.class).in(Scopes.SINGLETON);
        //TODO configBinder(binder).bindConfig(LokiConnectorConfig.class);
    }
}
