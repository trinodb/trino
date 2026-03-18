package io.trino.plugin.couchbase;

import io.trino.spi.connector.*;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;

public class CouchbaseSplitManager
            implements ConnectorSplitManager {
    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableHandle table, DynamicFilter dynamicFilter, Constraint constraint) {
        return new FixedSplitSource(new CouchbaseSplit());
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableFunctionHandle function) {
        return new FixedSplitSource(new CouchbaseSplit());
    }
}
