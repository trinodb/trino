package io.trino.plugin.couchbase;


import io.trino.spi.connector.ConnectorTableHandle;

import java.util.Set;

public record CouchbaseTableHandle(
        String schema,
        String name,
        Set<CouchbaseColumnHandle> columns)
        implements ConnectorTableHandle
{

}
