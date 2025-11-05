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
package io.trino.plugin.loki;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.SqlMap;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeUtils;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.block.MapValueBuilder.buildMapValue;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;

public class LokiRecordCursor
        implements RecordCursor
{
    private final List<LokiColumnHandle> columnHandles;
    private final LokiQueryResultIterator resultIterator;

    private boolean advanced;

    public LokiRecordCursor(List<LokiColumnHandle> columnHandles, LokiQueryResultIterator queryResult)
    {
        requireNonNull(columnHandles, "columnHandles is null");
        requireNonNull(queryResult, "queryResult is null");

        this.resultIterator = queryResult;
        this.columnHandles = columnHandles.stream()
                .sorted(Comparator.comparing(LokiColumnHandle::ordinalPosition))
                .collect(toImmutableList());
    }

    @Override
    public boolean advanceNextPosition()
    {
        this.advanced = true;
        return this.resultIterator.advanceNextPosition();
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).type();
    }

    @Override
    public boolean getBoolean(int field)
    {
        throw new UnsupportedOperationException("Loki connector does not support boolean fields");
    }

    @Override
    public long getLong(int field)
    {
        checkFieldType(field, TIMESTAMP_TZ_MILLIS);
        return (long) requireNonNull(getFieldValue(field));
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, DOUBLE);
        return (double) requireNonNull(getFieldValue(field));
    }

    @Override
    public Slice getSlice(int field)
    {
        checkFieldType(field, createUnboundedVarcharType());
        return Slices.utf8Slice((String) requireNonNull(getFieldValue(field)));
    }

    @Override
    public Object getObject(int field)
    {
        return getFieldValue(field);
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return getFieldValue(field) == null;
    }

    private Object getFieldValue(int field)
    {
        checkState(this.advanced, "Cursor has not been advanced yet");

        if (field < 0 || field >= columnHandles.size()) {
            throw new IllegalArgumentException("Invalid field index: " + field);
        }

        LokiColumnHandle column = this.columnHandles.get(field);
        return switch (column.name()) {
            case "labels" -> getSqlMapFromLabels((MapType) column.type(), this.resultIterator.getLabels());
            case "timestamp" -> this.resultIterator.getTimestamp();
            case "value" -> this.resultIterator.getValue();
            default -> throw new IllegalArgumentException("Unknown column: " + column.name());
        };
    }

    @Override
    public void close()
    {
    }

    private void checkFieldType(int field, Type expected)
    {
        Type actual = getType(field);
        checkArgument(actual.equals(expected), "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    // Copy from Loki to handle map<string,string>
    static SqlMap getSqlMapFromLabels(MapType mapType, Map<String, String> labels)
    {
        Type keyType = mapType.getKeyType();
        Type valueType = mapType.getValueType();

        return buildMapValue(mapType, labels.size(), (keyBuilder, valueBuilder) -> {
            labels.forEach((key, value) -> {
                TypeUtils.writeNativeValue(keyType, keyBuilder, key);
                TypeUtils.writeNativeValue(valueType, valueBuilder, value);
            });
        });
    }
}
