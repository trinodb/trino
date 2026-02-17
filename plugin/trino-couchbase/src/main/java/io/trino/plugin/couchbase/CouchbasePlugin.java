package io.trino.plugin.couchbase;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

import static java.util.Objects.requireNonNull;

public class CouchbasePlugin
        implements Plugin
{
    private ConnectorFactory connectorFactory;

    public CouchbasePlugin()
    {
        connectorFactory = new CouchbaseConnectorFactory();
    }

    @VisibleForTesting
    CouchbasePlugin(ConnectorFactory connectorFactory)
    {
        connectorFactory = requireNonNull(connectorFactory, "factory is null");
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories() {
        return ImmutableList.of(connectorFactory);
    }
}
