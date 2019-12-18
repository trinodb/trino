package io.prestosql.plugin.influx;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.ConnectorHandleResolver;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class InfluxConnectorFactory implements ConnectorFactory {

    @Override
    public String getName() {
        return "influx";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver() {
        return new InfluxHandleResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
        requireNonNull(config, "config is null");

        Bootstrap app = new Bootstrap(
            new JsonModule(),
            new InfluxModule());

        Injector injector = app
            .strictConfig()
            .doNotInitializeLogging()
            .setRequiredConfigurationProperties(config)
            .initialize();

        return injector.getInstance(InfluxConnector.class);
    }
}
