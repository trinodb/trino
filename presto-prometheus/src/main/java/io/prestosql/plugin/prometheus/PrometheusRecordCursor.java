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
package io.prestosql.plugin.prometheus;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteSource;
import com.google.common.io.CountingInputStream;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeUtils;
import io.prestosql.spi.type.VarcharType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;

//import static io.prestosql.spi.type.StandardTypes.VARCHAR;

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
                return fields.labels;
            case 1:
                return fields.timestamp;
            case 2:
                return fields.value;
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
        if (type.equals(TIMESTAMP)) {
            return ((Timestamp) getFieldValue(field)).toInstant().toEpochMilli();
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, "Unsupported type " + getType(field));
        }
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, DOUBLE);
        return (double) getFieldValue(field);
    }

    @Override
    public Slice getSlice(int field)
    {
        checkFieldType(field, createUnboundedVarcharType());
        return Slices.utf8Slice((String) getFieldValue(field));
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
        if (getFieldValue(field) == null) {
            return true;
        }
        return false;
    }

    private void checkFieldType(int field, Type expected)
    {
        Type actual = getType(field);
        checkArgument(actual.equals(expected), "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    private List<PrometheusStandardizedRow> prometheusResultsInStandardizedForm(List<PrometheusMetricResult> results)
    {
        return results.stream().map(result ->
                result.getTimeSeriesValues().getValues().stream().map(prometheusTimeSeriesValue ->
                        new PrometheusStandardizedRow(
                                getBlockFromMap(columnHandles.get(0).getColumnType(), metricHeaderToMap(result.getMetricHeader())),
                                prometheusTimeSeriesValue.getTimestamp(),
                                Double.parseDouble(prometheusTimeSeriesValue.getValue())))
                        .collect(Collectors.toList()))
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    private Map<String, String> metricHeaderToMap(Map<String, String> mapToConvert)
    {
        return ImmutableMap.<String, String>builder().putAll(mapToConvert).build();
    }

    static Block getBlockFromMap(Type mapType, Map<?, ?> map)
    {
        // on functions like COUNT() the Type won't be a MapType
        if (!(mapType instanceof MapType)) {
            return null;
        }
        Type keyType = mapType.getTypeParameters().get(0);
        Type valueType = mapType.getTypeParameters().get(1);

        BlockBuilder mapBlockBuilder = mapType.createBlockBuilder(null, 1);
        BlockBuilder builder = mapBlockBuilder.beginBlockEntry();

        for (Map.Entry<?, ?> entry : map.entrySet()) {
            writeObject(builder, keyType, entry.getKey());
            writeObject(builder, valueType, entry.getValue());
        }

        mapBlockBuilder.closeEntry();
        return (Block) mapType.getObject(mapBlockBuilder, 0);
    }

    static Map<Object, Object> getMapFromBlock(Type type, Block block)
    {
        Map<Object, Object> map = new HashMap<>(block.getPositionCount() / 2);
        Type keyType = Types.getKeyType(type);
        Type valueType = Types.getValueType(type);
        for (int i = 0; i < block.getPositionCount(); i += 2) {
            map.put(readObject(keyType, block, i), readObject(valueType, block, i + 1));
        }
        return map;
    }

    static void writeObject(BlockBuilder builder, Type type, Object obj)
    {
        if (Types.isArrayType(type)) {
            BlockBuilder arrayBldr = builder.beginBlockEntry();
            Type elementType = Types.getElementType(type);
            for (Object item : (List<?>) obj) {
                writeObject(arrayBldr, elementType, item);
            }
            builder.closeEntry();
        }
        else if (Types.isMapType(type)) {
            BlockBuilder mapBlockBuilder = builder.beginBlockEntry();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) obj).entrySet()) {
                writeObject(mapBlockBuilder, Types.getKeyType(type), entry.getKey());
                writeObject(mapBlockBuilder, Types.getValueType(type), entry.getValue());
            }
            builder.closeEntry();
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

    static Object readObject(Type type, Block block, int position)
    {
        if (Types.isArrayType(type)) {
            Type elementType = Types.getElementType(type);
            return getArrayFromBlock(elementType, block.getObject(position, Block.class));
        }
        else if (Types.isMapType(type)) {
            return getMapFromBlock(type, block.getObject(position, Block.class));
        }
        else {
            if (type.getJavaType() == Slice.class) {
                Slice slice = (Slice) TypeUtils.readNativeValue(type, block, position);
                return (type instanceof VarcharType) ? slice.toStringUtf8() : slice.getBytes();
            }

            return TypeUtils.readNativeValue(type, block, position);
        }
    }

    static List<Object> getArrayFromBlock(Type elementType, Block block)
    {
        ImmutableList.Builder<Object> arrayBuilder = ImmutableList.builder();
        for (int i = 0; i < block.getPositionCount(); ++i) {
            arrayBuilder.add(readObject(elementType, block, i));
        }
        return arrayBuilder.build();
    }

    @Override
    public void close()
    {
    }
}
