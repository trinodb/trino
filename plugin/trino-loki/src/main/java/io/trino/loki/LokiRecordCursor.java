package io.trino.loki;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.Type;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;

public class LokiRecordCursor implements RecordCursor {

    private final List<LokiColumnHandle> columnHandles;
    private final int[] fieldToColumnIndex;

    private final Iterator<QueryResult.LogEntry> entryItr;
    private final QueryResult result;

    // TODO: include labels
    private QueryResult.LogEntry entry;

    public LokiRecordCursor(List<LokiColumnHandle> columnHandles, QueryResult result) {
        this.columnHandles = columnHandles;

        fieldToColumnIndex = new int[columnHandles.size()];
        for (int i = 0; i < columnHandles.size(); i++) {
            LokiColumnHandle columnHandle = columnHandles.get(i);
            fieldToColumnIndex[i] = columnHandle.ordinalPosition();
        }

        this.entryItr = result.getData().getStreams()
                .stream()
                .flatMap(stream -> stream.getValues().stream())
                .iterator();
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
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).columnType();
    }

    @Override
    public boolean advanceNextPosition() {
        if (!entryItr.hasNext()) {
            return false;
        }
        entry = entryItr.next();
        return true;
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
        checkFieldType(field, DOUBLE);
        return (double) requireNonNull(getFieldValue(field));
    }

    @Override
    public Slice getSlice(int field) {
        checkFieldType(field, createUnboundedVarcharType());
        return Slices.utf8Slice((String) requireNonNull(getFieldValue(field)));
    }

    @Override
    public Object getObject(int field) {
        return getFieldValue(field);
    }

    @Override
    public boolean isNull(int field) {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return getFieldValue(field) == null;
    }

    @Override
    public void close() {

    }

    private void checkFieldType(int field, Type expected)
    {
        Type actual = getType(field);
        checkArgument(actual.equals(expected), "Expected field %s to be type %s but is %s", field, expected, actual);
    }
}
