package io.prestosql.plugin.influx;

import io.airlift.slice.Slice;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.type.Type;
import org.influxdb.dto.QueryResult;

import java.util.List;

public class InfluxRecordCursor implements RecordCursor {

    private final List<QueryResult.Series> results;
    private final List<InfluxColumnHandle> columns;
    private int rowId;

    public InfluxRecordCursor(List<QueryResult.Series> results, List<InfluxColumnHandle> columns) {
        this.results = results;
        this.columns = columns;
        this.rowId = -1;
    }

    @Override
    public long getCompletedBytes() {
        return 0;
    }

    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    @Override
    public Type getType(int field) {
        return null;
    }

    @Override
    public boolean advanceNextPosition() {
        rowId++;
        return rowId < 1;
    }

    @Override
    public boolean getBoolean(int field) {
        return false;
    }

    @Override
    public long getLong(int field) {
        return 0;
    }

    @Override
    public double getDouble(int field) {
        return 0;
    }

    @Override
    public Slice getSlice(int field) {
        return null;
    }

    @Override
    public Object getObject(int field) {
        return null;
    }

    @Override
    public boolean isNull(int field) {
        return true;
    }

    @Override
    public void close() {
    }
}
