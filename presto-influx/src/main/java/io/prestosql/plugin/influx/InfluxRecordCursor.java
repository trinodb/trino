package io.prestosql.plugin.influx;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.type.DateTimeEncoding;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.spi.type.Type;

import java.time.Instant;
import java.util.List;
import java.util.Objects;


public class InfluxRecordCursor implements RecordCursor {

    private final List<InfluxColumn> columns;
    private final List<Object[]> rows;
    private Object[] row;
    private int rowId;

    public InfluxRecordCursor(List<InfluxColumn> columns, List<Object[]> rows) {
        this.columns = columns;
        this.rows = rows;
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
        return columns.get(field).getType();
    }

    @Override
    public boolean advanceNextPosition() {
        row = ++rowId < rows.size()? rows.get(rowId): null;
        return row != null;
    }

    @Override
    public boolean getBoolean(int field) {
        Object value = getObject(field);
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue() != 0;
        }
        if (value != null) {
            InfluxError.BAD_VALUE.fail("cannot cast " + columns.get(field) + ": " + value.getClass() + ": " + value + " to boolean");
        }
        return false;
    }

    @Override
    public long getLong(int field) {
        Object value = getObject(field);
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value != null) {
            InfluxError.BAD_VALUE.fail("cannot cast " + columns.get(field) + ": " + value.getClass() + ": " + value + " to long");
        }
        return 0;
    }

    @Override
    public double getDouble(int field) {
        Object value = getObject(field);
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        if (value != null) {
            InfluxError.BAD_VALUE.fail("cannot cast " + columns.get(field) + ": " + value.getClass() + ": " + value + " to double");
        }
        return 0;
    }

    @Override
    public Slice getSlice(int field) {
        String value = Objects.toString(getObject(field), null);
        return value != null? Slices.utf8Slice(value): null;
    }

    @Override
    public Object getObject(int field) {
        return row[field];
    }

    @Override
    public boolean isNull(int field) {
        return row[field] == null;
    }

    @Override
    public void close() {
        rowId = rows.size();
        row = null;
    }
}
