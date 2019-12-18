package io.prestosql.plugin.influx;

import io.prestosql.spi.connector.*;

import javax.inject.Inject;
import java.util.Collections;

public class InfluxSplitManager implements ConnectorSplitManager {

    private final InfluxClient client;

    @Inject
    public InfluxSplitManager(InfluxClient client)
    {
        this.client = client;
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableHandle table, SplitSchedulingStrategy splitSchedulingStrategy)
    {
        return new FixedSplitSource(Collections.singletonList(new InfluxSplit(client.getHostAddress())));
    }
}
