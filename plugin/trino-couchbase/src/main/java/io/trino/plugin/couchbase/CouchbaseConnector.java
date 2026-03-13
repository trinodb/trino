package io.trino.plugin.couchbase;

import com.google.inject.Inject;
import io.trino.spi.connector.*;
import io.trino.spi.transaction.IsolationLevel;

public class CouchbaseConnector implements Connector {
    private final CouchbaseMetadata metadata;
    private final CouchbaseSplitManager splitManager;
    private final CouchbasePageSourceProvider pageSourceProvider;

    @Inject
    public CouchbaseConnector(
            CouchbaseMetadata metadata,
            CouchbaseSplitManager splitManager,
            CouchbasePageSourceProvider pageSourceProvider
    ) {
        this.metadata = metadata;
        this.splitManager = splitManager;
        this.pageSourceProvider = pageSourceProvider;
    }

    @Override
    public void shutdown() {

    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {
        return CouchbaseTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle) {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider() {
        return pageSourceProvider;
    }
}
