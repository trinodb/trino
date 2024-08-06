package io.trino.loki;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.SqlMap;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeUtils;
import io.trino.spi.type.VarcharType;

import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.block.MapValueBuilder.buildMapValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.packTimeWithTimeZone;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;

public abstract class LokiRecordCursor
        implements RecordCursor
{
    final List<LokiColumnHandle> columnHandles;
    final int[] fieldToColumnIndex;

    public LokiRecordCursor(List<LokiColumnHandle> columnHandles)
    {

        this.columnHandles = columnHandles;

        fieldToColumnIndex = new int[columnHandles.size()];
        for (int i = 0; i < columnHandles.size(); i++) {
            LokiColumnHandle columnHandle = columnHandles.get(i);
            fieldToColumnIndex[i] = columnHandle.ordinalPosition();
        }
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

    abstract Object getEntryValue(int field);

    // Copy from Loki to handle map<string,string>
    static SqlMap getSqlMapFromMap(Type type, Map<?, ?> map)
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

    static Map<Object, Object> getMapFromSqlMap(Type type, SqlMap sqlMap)
    {
        MapType mapType = (MapType) type;
        Type keyType = mapType.getKeyType();
        Type valueType = mapType.getValueType();

        int rawOffset = sqlMap.getRawOffset();
        Block rawKeyBlock = sqlMap.getRawKeyBlock();
        Block rawValueBlock = sqlMap.getRawValueBlock();

        Map<Object, Object> map = new HashMap<>(sqlMap.getSize());
        for (int i = 0; i < sqlMap.getSize(); i++) {
            map.put(readObject(keyType, rawKeyBlock, rawOffset + i), readObject(valueType, rawValueBlock, rawOffset + i));
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
        if (type instanceof ArrayType arrayType) {
            Type elementType = arrayType.getElementType();
            return getArrayFromBlock(elementType, arrayType.getObject(block, position));
        }
        if (type instanceof MapType mapType) {
            return getMapFromSqlMap(type, mapType.getObject(block, position));
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
    // End of copy from prometheus

    @Override
    public boolean getBoolean(int field)
    {
        return false;
    }

    @Override
    public long getLong(int field)
    {
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
    public double getDouble(int field)
    {
        checkFieldType(field, DOUBLE);
        return (double) requireNonNull(getEntryValue(field));
    }

    @Override
    public Slice getSlice(int field)
    {
        checkFieldType(field, createUnboundedVarcharType());
        return Slices.utf8Slice((String) requireNonNull(getEntryValue(field)));
    }

    @Override
    public Object getObject(int field)
    {
        return getEntryValue(field);
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return getEntryValue(field) == null;
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
}
