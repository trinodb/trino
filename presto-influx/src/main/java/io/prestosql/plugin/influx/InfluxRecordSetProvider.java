package io.prestosql.plugin.influx;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.connector.*;
import org.influxdb.dto.QueryResult;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class InfluxRecordSetProvider implements ConnectorRecordSetProvider {

    private final InfluxClient client;

    @Inject
    public InfluxRecordSetProvider(InfluxClient client) {
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<? extends ColumnHandle> columns) {
        ImmutableList.Builder<InfluxColumnHandle> handles = ImmutableList.builder();
        List<String> columnNames = new ArrayList<>();
        for (ColumnHandle handle : columns) {
            InfluxColumnHandle influxColumnHandle = (InfluxColumnHandle) handle;
            handles.add(influxColumnHandle);
            columnNames.add(influxColumnHandle.getInfluxName());
        }
        QueryResult.Series series = new QueryResult.Series();
        series.setColumns(columnNames);
        return new InfluxRecordSet(handles.build(), Collections.singletonList(series));
    }
}
