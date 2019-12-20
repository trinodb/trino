package io.prestosql.plugin.influx;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.connector.*;
import org.influxdb.dto.QueryResult;

import javax.inject.Inject;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class InfluxRecordSetProvider implements ConnectorRecordSetProvider {

    private final InfluxClient client;

    @Inject
    public InfluxRecordSetProvider(InfluxClient client) {
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle tableHandle, List<? extends ColumnHandle> columns) {
        InfluxTableHandle table = (InfluxTableHandle) tableHandle;
        client.logger.info("getRecordSet(" + split + ", " + table + ", " + columns + ")");
        InfluxQL query = new InfluxQL();
        ImmutableList.Builder<InfluxColumn> handles = ImmutableList.builder();
        boolean hasTime = false;
        for (ColumnHandle handle: columns) {
            InfluxColumnHandle column = (InfluxColumnHandle) handle;
            hasTime |= column.getInfluxName().equals("time");
            InfluxError.GENERAL.check(column.getMeasurement().equals(table.getMeasurement()), "bad measurement for " + column + " in " + table);
            InfluxError.GENERAL.check(column.getRetentionPolicy().equals(table.getRetentionPolicy()), "bad retention-policy for " + column + " in " + table);
            query.append(query.isEmpty()? "SELECT ": ", ").add(column);
            handles.add(column);
        }
        if (!hasTime) {
            query.append(query.isEmpty()? "SELECT time": ", time");
            handles.add(InfluxColumn.TIME);
        }
        query.append(' ').append(table.getFromWhere());
        List<QueryResult.Series> results = client.execute(query.toString());  // actually run the query against our Influx server
        return new InfluxRecordSet(handles.build(), results);
    }
}
