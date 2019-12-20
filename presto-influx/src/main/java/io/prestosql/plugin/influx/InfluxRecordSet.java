package io.prestosql.plugin.influx;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.RecordSet;
import io.prestosql.spi.type.Type;
import org.influxdb.dto.QueryResult;

import java.time.ZoneId;
import java.util.*;

public class InfluxRecordSet implements RecordSet {

    private final List<InfluxColumn> columns;
    private final List<Type> columnTypes;
    private final List<List<Object>> rows;

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
        Object[] row = new Object[columns.size()];
        for (QueryResult.Series series: results) {
            if (series.getValues().isEmpty()) {
                continue;
            }
            Arrays.fill(row, null);
            if (series.getTags() != null) {
                for (Map.Entry<String, String> tag : series.getTags().entrySet()) {
                    row[mapping.get(tag.getKey())] = tag.getValue();
                }
            }
            int[] fields = new int[series.getColumns().size()];
            for (int i = 0; i < fields.length; i++) {
                fields[i] = mapping.get(series.getColumns().get(i));
            }
            for (List<Object> values: series.getValues()) {
                for (int i = 0; i < fields.length; i++) {
                    row[fields[i]] = values.get(i);
                }
                rows.add(ImmutableList.copyOf(row));
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
