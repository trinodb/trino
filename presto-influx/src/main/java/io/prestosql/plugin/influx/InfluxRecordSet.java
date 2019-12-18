package io.prestosql.plugin.influx;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.RecordSet;
import io.prestosql.spi.type.Type;
import org.influxdb.dto.QueryResult;

import java.util.List;

public class InfluxRecordSet implements RecordSet {

    private final List<InfluxColumnHandle> columns;
    private final List<Type> columnTypes;
    private final List<QueryResult.Series> results;

    public InfluxRecordSet(List<InfluxColumnHandle> columns, List<QueryResult.Series> results) {
        this.columns = columns;
        ImmutableList.Builder<Type> columnTypes = new ImmutableList.Builder<>();
        for (InfluxColumnHandle column: columns) {
            columnTypes.add(column.getType());
        }
        this.columnTypes = columnTypes.build();
        this.results = results;
    }

    @Override
    public List<Type> getColumnTypes() {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor() {
        return new InfluxRecordCursor(results, columns);
    }
}
