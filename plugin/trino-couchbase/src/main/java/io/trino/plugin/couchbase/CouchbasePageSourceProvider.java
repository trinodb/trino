package io.trino.plugin.couchbase;

import io.trino.spi.connector.*;
import jakarta.inject.Inject;

import java.util.List;
import java.util.stream.Collectors;

public class CouchbasePageSourceProvider
        implements ConnectorPageSourceProvider {
    private final CouchbaseClient client;

    @Inject
    public CouchbasePageSourceProvider(CouchbaseClient client) {
        this.client = client;
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<ColumnHandle> columns, DynamicFilter dynamicFilter) {
        return new CouchbasePageSource(
                client,
                (CouchbaseTransactionHandle) transaction,
                session,
                (CouchbaseSplit) split,
                (CouchbaseTableHandle) table,
                columns.stream().map(CouchbaseColumnHandle.class::cast).toList(),
                dynamicFilter
        );
    }
}
