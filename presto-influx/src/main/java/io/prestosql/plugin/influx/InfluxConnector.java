package io.prestosql.plugin.influx;

import io.airlift.bootstrap.LifeCycleManager;
import io.prestosql.spi.connector.*;
import io.prestosql.spi.transaction.IsolationLevel;

import javax.inject.Inject;

import static io.prestosql.plugin.influx.InfluxTransactionHandle.INSTANCE;
import static io.prestosql.spi.transaction.IsolationLevel.READ_COMMITTED;
import static io.prestosql.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

public class InfluxConnector implements Connector {

    private final LifeCycleManager lifeCycleManager;
    private final InfluxMetadata metadata;
    private final InfluxSplitManager splitManager;
    private final InfluxRecordSetProvider recordSetProvider;

    @Inject
    public InfluxConnector(LifeCycleManager lifeCycleManager,
                           InfluxMetadata metadata,
                           InfluxSplitManager splitManager,
                           InfluxRecordSetProvider recordSetProvider) {
        this.lifeCycleManager = lifeCycleManager;
        this.metadata = metadata;
        this.splitManager = splitManager;
        this.recordSetProvider = recordSetProvider;
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly) {
        checkConnectorSupports(READ_COMMITTED, isolationLevel);
        return INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        return recordSetProvider;
    }

    @Override
    public final void shutdown()
    {
        lifeCycleManager.stop();
    }
}
