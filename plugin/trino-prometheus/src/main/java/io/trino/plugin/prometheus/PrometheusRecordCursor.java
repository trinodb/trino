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
package io.trino.plugin.prometheus;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSource;
import com.google.common.io.CountingInputStream;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeUtils;
import io.trino.spi.type.VarcharType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.prometheus.PrometheusClient.TIMESTAMP_COLUMN_TYPE;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.block.MapValueBuilder.buildMapValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;

public class PrometheusRecordCursor
        implements RecordCursor
{
    private final List<PrometheusColumnHandle> columnHandles;
    private final int[] fieldToColumnIndex;

    private final Iterator<PrometheusStandardizedRow> metricsItr;
    private final long totalBytes;

    private PrometheusStandardizedRow fields;

    public PrometheusRecordCursor(List<PrometheusColumnHandle> columnHandles, ByteSource byteSource)
    {
        this.columnHandles = columnHandles;

        fieldToColumnIndex = new int[columnHandles.size()];
        for (int i = 0; i < columnHandles.size(); i++) {
            PrometheusColumnHandle columnHandle = columnHandles.get(i);
            fieldToColumnIndex[i] = columnHandle.getOrdinalPosition();
        }

        try (CountingInputStream input = new CountingInputStream(byteSource.openStream())) {
            metricsItr = prometheusResultsInStandardizedForm(new PrometheusQueryResponseParse(input).getResults()).iterator();
            totalBytes = input.getCount();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return totalBytes;
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
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (!metricsItr.hasNext()) {
            return false;
        }
        fields = metricsItr.next();
        return true;
    }

    private Object getFieldValue(int field)
    {
        checkState(fields != null, "Cursor has not been advanced yet");

        int columnIndex = fieldToColumnIndex[field];
        switch (columnIndex) {
            case 0:
                return getBlockFromMap(columnHandles.get(columnIndex).getColumnType(), fields.getLabels());
            case 1:
                return fields.getTimestamp();
            case 2:
                return fields.getValue();
        }
        return null;
    }

    @Override
    public boolean getBoolean(int field)
    {
        return true;
    }

    @Override
    public long getLong(int field)
    {
        Type type = getType(field);
        if (type.equals(TIMESTAMP_COLUMN_TYPE)) {
            Instant dateTime = (Instant) requireNonNull(getFieldValue(field));
            // render with the fixed offset of the Trino server
            int offsetMinutes = dateTime.atZone(ZoneId.systemDefault()).getOffset().getTotalSeconds() / 60;
            return packDateTimeWithZone(dateTime.toEpochMilli(), offsetMinutes);
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported type " + getType(field));
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

    private void checkFieldType(int field, Type expected)
    {
        Type actual = getType(field);
        checkArgument(actual.equals(expected), "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    private List<PrometheusStandardizedRow> prometheusResultsInStandardizedForm(List<PrometheusMetricResult> results)
    {
        return results.stream().map(result ->
                result.getTimeSeriesValues().getValues().stream().map(prometheusTimeSeriesValue -> new PrometheusStandardizedRow(
                        result.getMetricHeader(),
                        prometheusTimeSeriesValue.getTimestamp(),
                        Double.parseDouble(prometheusTimeSeriesValue.getValue())))
                        .collect(Collectors.toList()))
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    static Block getBlockFromMap(Type type, Map<?, ?> map)
    {
        // on functions like COUNT() the Type won't be a MapType
        if (!(type instanceof MapType mapType)) {
            return null;
        }
        Type keyType = mapType.getKeyType();
        Type valueType = mapType.getValueType();

        return buildMapValue(mapType, map.size(), (keyBuilder, valueBuilder) -> {
            map.forEach((key, value) -> {
                writeObject(keyBuilder, keyType, key);
                writeObject(valueBuilder, valueType, value);
            });
        });
    }

    static Map<Object, Object> getMapFromBlock(Type type, Block block)
    {
        MapType mapType = (MapType) type;
        Type keyType = mapType.getKeyType();
        Type valueType = mapType.getValueType();
        Map<Object, Object> map = new HashMap<>(block.getPositionCount() / 2);
        for (int i = 0; i < block.getPositionCount(); i += 2) {
            map.put(readObject(keyType, block, i), readObject(valueType, block, i + 1));
        }
        return map;
    }

    private static void writeObject(BlockBuilder builder, Type type, Object obj)
    {
        if (type instanceof ArrayType arrayType) {
            Type elementType = arrayType.getElementType();
            ((ArrayBlockBuilder) builder).buildEntry(elementBuilder -> {
                for (Object item : (List<?>) obj) {
                    writeObject(elementBuilder, elementType, item);
                }
            });
        }
        else if (type instanceof MapType mapType) {
            ((MapBlockBuilder) builder).buildEntry((keyBuilder, valueBuilder) -> {
                for (Map.Entry<?, ?> entry : ((Map<?, ?>) obj).entrySet()) {
                    writeObject(keyBuilder, mapType.getKeyType(), entry.getKey());
                    writeObject(valueBuilder, mapType.getValueType(), entry.getValue());
                }
            });
        }
        else {
            if (BOOLEAN.equals(type)
                    || TINYINT.equals(type)
                    || SMALLINT.equals(type)
                    || INTEGER.equals(type)
                    || BIGINT.equals(type)
                    || DOUBLE.equals(type)
                    || type instanceof VarcharType) {
                TypeUtils.writeNativeValue(type, builder, obj);
            }
        }
    }

    private static Object readObject(Type type, Block block, int position)
    {
        if (type instanceof ArrayType) {
            Type elementType = ((ArrayType) type).getElementType();
            return getArrayFromBlock(elementType, block.getObject(position, Block.class));
        }
        if (type instanceof MapType) {
            return getMapFromBlock(type, block.getObject(position, Block.class));
        }
        if (type.getJavaType() == Slice.class) {
            Slice slice = (Slice) requireNonNull(TypeUtils.readNativeValue(type, block, position));
            return (type instanceof VarcharType) ? slice.toStringUtf8() : slice.getBytes();
        }

        return TypeUtils.readNativeValue(type, block, position);
    }

    private static List<Object> getArrayFromBlock(Type elementType, Block block)
    {
        ImmutableList.Builder<Object> arrayBuilder = ImmutableList.builder();
        for (int i = 0; i < block.getPositionCount(); ++i) {
            arrayBuilder.add(readObject(elementType, block, i));
        }
        return arrayBuilder.build();
    }

    @Override
    public void close() {}
}
