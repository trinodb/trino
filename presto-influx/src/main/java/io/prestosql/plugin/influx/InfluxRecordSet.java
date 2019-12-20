package io.prestosql.plugin.influx;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.RecordSet;
import io.prestosql.spi.type.Type;
import org.influxdb.dto.QueryResult;

import java.util.*;

public class InfluxRecordSet implements RecordSet {

    private final List<InfluxColumn> columns;
    private final List<Type> columnTypes;
    private final List<Object[]> rows;

    public InfluxRecordSet(List<InfluxColumn> columns, List<QueryResult.Series> results) {
        this.columns = columns;
        ImmutableList.Builder<Type> columnTypes = new ImmutableList.Builder<>();
        Map<String, Integer> mapping = new HashMap<>();
        for (InfluxColumn column: columns) {
            columnTypes.add(column.getType());
            mapping.put(column.getInfluxName(), mapping.size());
        }
        this.columnTypes = columnTypes.build();
        this.rows = new ArrayList<>();
        final int IGNORE = -1;
        for (QueryResult.Series series: results) {
            if (series.getValues().isEmpty()) {
                continue;
            }
            // we can't push down group-bys so we have no tags to consider
            int[] fields = new int[series.getColumns().size()];
            for (int i = 0; i < fields.length; i++) {
                fields[i] = mapping.getOrDefault(series.getColumns().get(i), IGNORE);
            }
            for (List<Object> values: series.getValues()) {
                Object[] row = new Object[columns.size()];
                for (int i = 0; i < fields.length; i++) {
                    int slot = fields[i];
                    if (slot != IGNORE) {
                        row[slot] = values.get(i);
                    }
                }
                rows.add(row);
            }
        }
    }

    @Override
    public List<Type> getColumnTypes() {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor() {
        return new InfluxRecordCursor(columns, rows);
    }
}
