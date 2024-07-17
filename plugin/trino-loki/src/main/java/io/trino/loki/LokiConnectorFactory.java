package io.trino.loki;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;

public class LokiConnectorFactory implements ConnectorFactory {
    @Override
    public String getName() {
        return "loki";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
        return null;
    }
}
