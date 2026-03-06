package io.trino.plugin.couchbase;

import io.trino.spi.connector.*;

import java.util.List;

public class CouchbasePageSourceProvider
        implements ConnectorPageSourceProvider {
    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<ColumnHandle> columns, DynamicFilter dynamicFilter) {
        return null;
    }
}
