/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.loki;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.Type;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.DateTimeEncoding.packTimeWithTimeZone;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;

public class LokiRecordCursor implements RecordCursor {

    private final List<LokiColumnHandle> columnHandles;
    private final int[] fieldToColumnIndex;

    private final Iterator<QueryResult.LogEntry> entryItr;

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

    private Object getEntryValue(int field)
    {
        checkState(entry != null, "Cursor has not been advanced yet");

        int columnIndex = fieldToColumnIndex[field];
        return switch (columnIndex) {
            // TODO: case 0 -> getSqlMapFromMap(columnHandles.get(columnIndex).columnType(), fields.labels());
            case 0 -> entry.getTs();
            case 1 -> entry.getLine();
            default -> null;
        };
    }

    @Override
    public boolean getBoolean(int field) {
        return false;
    }

    @Override
    public long getLong(int field) {
        Type type = getType(field);
        if (type.equals(LokiMetadata.TIMESTAMP_COLUMN_TYPE)) {
            Long nanos = (Long) requireNonNull(getEntryValue(field));
            // render with the fixed offset of the Trino server
            int offsetMinutes = Instant.ofEpochMilli(nanos / 1000).atZone(ZoneId.systemDefault()).getOffset().getTotalSeconds() / 60;
            return packTimeWithTimeZone(nanos, offsetMinutes);
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported type " + getType(field));
    }

    @Override
    public double getDouble(int field) {
        checkFieldType(field, DOUBLE);
        return (double) requireNonNull(getEntryValue(field));
    }

    @Override
    public Slice getSlice(int field) {
        checkFieldType(field, createUnboundedVarcharType());
        return Slices.utf8Slice((String) requireNonNull(getEntryValue(field)));
    }

    @Override
    public Object getObject(int field) {
        return getEntryValue(field);
    }

    @Override
    public boolean isNull(int field) {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return getEntryValue(field) == null;
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
