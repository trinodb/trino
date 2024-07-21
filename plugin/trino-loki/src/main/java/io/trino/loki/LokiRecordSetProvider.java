package io.trino.loki;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.spi.connector.*;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class LokiRecordSetProvider implements ConnectorRecordSetProvider {

    private final LokiClient prometheusClient;

    @Inject
    public LokiRecordSetProvider(LokiClient lokiClient)
    {
        this.prometheusClient = requireNonNull(lokiClient, "prometheusClient is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<? extends ColumnHandle> columns)
    {
        LokiSplit lokiSplit = (LokiSplit) split;

        ImmutableList.Builder<LokiColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : columns) {
            handles.add((LokiColumnHandle) handle);
        }

        return new LokiRecordSet(prometheusClient, lokiSplit, handles.build());
    }
}
