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
package io.trino.plugin.paimon;

import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.ArrayValueBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.MapValueBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.RowValueBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.VectorType;
import org.apache.paimon.utils.InternalRowUtils;

import java.math.BigDecimal;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.base.util.JsonTypeUtil.jsonParse;
import static io.trino.plugin.paimon.PaimonTrinoTypeConversions.paimonTimeMillisToTrinoPicos;
import static io.trino.plugin.paimon.PaimonTrinoTypeConversions.paimonTimestampToTrino;
import static io.trino.plugin.paimon.PaimonTrinoTypeConversions.paimonTimestampToTrinoTimestampWithTimeZone;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.Chars.truncateToLengthAndTrimSpaces;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.encodeShortScaledValue;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class PaimonPageBuilder
{
    private final PageBuilder pageBuilder;
    private final List<Type> columnTypes;
    private final List<DataType> logicalTypes;

    public PaimonPageBuilder(List<Type> columnTypes, List<DataType> logicalTypes)
    {
        this.columnTypes = List.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
        this.logicalTypes = List.copyOf(requireNonNull(logicalTypes, "logicalTypes is null"));
        checkArgument(this.columnTypes.size() == this.logicalTypes.size(),
                "columnTypes and logicalTypes size mismatch: %s != %s",
                this.columnTypes.size(),
                this.logicalTypes.size());
        for (int i = 0; i < this.columnTypes.size(); i++) {
            validateLogicalType(this.columnTypes.get(i), this.logicalTypes.get(i));
        }
        this.pageBuilder = new PageBuilder(this.columnTypes);
    }

    public boolean isFull()
    {
        return pageBuilder.isFull();
    }

    public boolean isEmpty()
    {
        return pageBuilder.isEmpty();
    }

    public int getPositionCount()
    {
        return pageBuilder.getPositionCount();
    }

    public long getSizeInBytes()
    {
        return pageBuilder.getSizeInBytes();
    }

    public void appendRow(InternalRow row)
    {
        requireNonNull(row, "row is null");
        pageBuilder.declarePosition();
        for (int i = 0; i < columnTypes.size(); i++) {
            BlockBuilder output = pageBuilder.getBlockBuilder(i);
            appendTo(columnTypes.get(i), logicalTypes.get(i), InternalRowUtils.get(row, i, logicalTypes.get(i)), output);
        }
    }

    public Page build()
    {
        Page page = pageBuilder.build();
        pageBuilder.reset();
        return page;
    }

    private static void writeSlice(BlockBuilder output, Type type, Object value)
    {
        if (type.getBaseName().equals(JSON)) {
            type.writeSlice(output, jsonParse(utf8Slice(((Variant) value).toJson())));
        }
        else if (type instanceof CharType charType) {
            type.writeSlice(output, truncateToLengthAndTrimSpaces(wrappedBuffer(((BinaryString) value).toBytes()), charType));
        }
        else if (type instanceof VarcharType) {
            type.writeSlice(output, wrappedBuffer(((BinaryString) value).toBytes()));
        }
        else if (type instanceof VarbinaryType) {
            if (value instanceof Blob blob) {
                type.writeSlice(output, wrappedBuffer(blob.toData()));
            }
            else {
                type.writeSlice(output, wrappedBuffer((byte[]) value));
            }
        }
        else {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Slice: " + type.getTypeDescriptor());
        }
    }

    private static void writeObject(BlockBuilder output, Type type, Object value)
    {
        if (type instanceof DecimalType decimalType) {
            BigDecimal decimal = ((Decimal) value).toBigDecimal();
            type.writeObject(output, Decimals.encodeScaledValue(decimal, decimalType.getScale()));
        }
        else {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Object: " + type.getTypeDescriptor());
        }
    }

    private static void appendTo(Type type, DataType logicalType, Object value, BlockBuilder output)
    {
        if (value == null) {
            output.appendNull();
            return;
        }

        Class<?> javaType = type.getJavaType();
        if (javaType == boolean.class) {
            type.writeBoolean(output, (Boolean) value);
        }
        else if (javaType == long.class) {
            if (type.equals(BIGINT) || type.equals(INTEGER) || type.equals(TINYINT) || type.equals(SMALLINT)
                    || type.equals(DATE)) {
                type.writeLong(output, ((Number) value).longValue());
            }
            else if (type.equals(REAL)) {
                type.writeLong(output, Float.floatToIntBits((Float) value));
            }
            else if (type instanceof DecimalType decimalType) {
                BigDecimal decimal = ((Decimal) value).toBigDecimal();
                type.writeLong(output, encodeShortScaledValue(decimal, decimalType.getScale()));
            }
            else if (type instanceof TimestampType) {
                type.writeLong(output, (long) paimonTimestampToTrino(type, (Timestamp) value));
            }
            else if (type instanceof TimestampWithTimeZoneType) {
                type.writeLong(output, (long) paimonTimestampToTrinoTimestampWithTimeZone(type, (Timestamp) value));
            }
            else if (type instanceof TimeType) {
                type.writeLong(output, paimonTimeMillisToTrinoPicos((int) value));
            }
            else {
                throw new TrinoException(
                        GENERIC_INTERNAL_ERROR,
                        format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
            }
        }
        else if (javaType == double.class) {
            type.writeDouble(output, ((Number) value).doubleValue());
        }
        else if (type instanceof DecimalType) {
            writeObject(output, type, value);
        }
        else if (javaType == Slice.class) {
            writeSlice(output, type, value);
        }
        else if (javaType == LongTimestamp.class) {
            type.writeObject(output, paimonTimestampToTrino(type, (Timestamp) value));
        }
        else if (javaType == LongTimestampWithTimeZone.class) {
            type.writeObject(output, paimonTimestampToTrinoTimestampWithTimeZone(type, (Timestamp) value));
        }
        else if (type instanceof ArrayType || type instanceof MapType || type instanceof RowType) {
            writeBlock(output, type, logicalType, value);
        }
        else {
            throw new TrinoException(
                    GENERIC_INTERNAL_ERROR,
                    format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
        }
    }

    private static void writeBlock(BlockBuilder output, Type type, DataType logicalType, Object value)
    {
        if (type instanceof ArrayType) {
            ArrayBlockBuilder arrayBlockBuilder = (ArrayBlockBuilder) output;
            InternalArray arrayData = (InternalArray) value;
            int arraySize = arrayData.size();
            validateArraySize(arraySize);
            DataType elementType = arrayElementLogicalType(logicalType);
            try {
                arrayBlockBuilder.buildEntry((ArrayValueBuilder<Throwable>) elementBuilder -> {
                    for (int i = 0; i < arraySize; i++) {
                        appendTo(type.getTypeParameters().get(0),
                                elementType,
                                InternalRowUtils.get(arrayData, i, elementType),
                                elementBuilder);
                    }
                });
            }
            catch (Throwable e) {
                throw propagateBlockBuilderFailure(e);
            }
            return;
        }
        if (type instanceof RowType) {
            RowBlockBuilder rowBlockBuilder = (RowBlockBuilder) output;
            org.apache.paimon.types.RowType rowLogicalType = rowLogicalType(logicalType);
            validateRowFieldCount(type.getTypeParameters().size(), rowLogicalType.getFieldCount());
            try {
                rowBlockBuilder.buildEntry((RowValueBuilder<Throwable>) fieldBuilders -> {
                    InternalRow rowData = (InternalRow) value;
                    for (int index = 0; index < type.getTypeParameters().size(); index++) {
                        Type fieldType = type.getTypeParameters().get(index);
                        DataType fieldLogicalType = rowLogicalType.getTypeAt(index);
                        appendTo(fieldType, fieldLogicalType, InternalRowUtils.get(rowData, index, fieldLogicalType), fieldBuilders.get(index));
                    }
                });
            }
            catch (Throwable e) {
                throw propagateBlockBuilderFailure(e);
            }
            return;
        }
        if (type instanceof MapType) {
            InternalMap mapData = (InternalMap) value;
            InternalArray keyArray = mapData.keyArray();
            InternalArray valueArray = mapData.valueArray();
            int mapSize = mapData.size();
            validateMapArraySizes(mapSize, keyArray.size(), valueArray.size());
            DataType keyType;
            DataType valueType;
            boolean multiset = false;
            if (logicalType instanceof org.apache.paimon.types.MapType mapType) {
                keyType = mapType.getKeyType();
                valueType = mapType.getValueType();
            }
            else if (logicalType instanceof MultisetType multisetType) {
                if (!type.getTypeParameters().get(1).equals(INTEGER)) {
                    throw new UnsupportedOperationException("Paimon MULTISET requires Trino integer count type metadata");
                }
                keyType = multisetType.getElementType();
                valueType = new IntType(false);
                multiset = true;
            }
            else {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unhandled Paimon logical type for Map: " + logicalType);
            }
            boolean validateMultisetCounts = multiset;
            MapBlockBuilder mapBlockBuilder = (MapBlockBuilder) output;
            try {
                mapBlockBuilder.buildEntry((MapValueBuilder<Throwable>) (keyBuilder, valueBuilder) -> {
                    for (int i = 0; i < mapSize; i++) {
                        appendTo(type.getTypeParameters().get(0), keyType, InternalRowUtils.get(keyArray, i, keyType), keyBuilder);
                        Object mapValue = InternalRowUtils.get(valueArray, i, valueType);
                        if (validateMultisetCounts) {
                            validateMultisetCount(mapValue);
                        }
                        appendTo(type.getTypeParameters().get(1), valueType, mapValue, valueBuilder);
                    }
                });
            }
            catch (Throwable e) {
                throw propagateBlockBuilderFailure(e);
            }
            return;
        }
        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Block: " + type.getTypeDescriptor());
    }

    private static void validateArraySize(int arraySize)
    {
        if (arraySize < 0) {
            throw new IllegalArgumentException("Paimon ARRAY/VECTOR size must be non-negative: " + arraySize);
        }
    }

    private static void validateMapArraySizes(int mapSize, int keyArraySize, int valueArraySize)
    {
        if (mapSize < 0) {
            throw new IllegalArgumentException("Paimon MAP size must be non-negative: " + mapSize);
        }
        if (keyArraySize != mapSize || valueArraySize != mapSize) {
            throw new IllegalArgumentException("Paimon MAP key/value array size mismatch: map size %s, key array size %s, value array size %s"
                    .formatted(mapSize, keyArraySize, valueArraySize));
        }
    }

    private static void validateMultisetCount(Object count)
    {
        if (count == null) {
            throw new IllegalArgumentException("Paimon MULTISET does not allow null counts");
        }
        int value = ((Number) count).intValue();
        if (value <= 0) {
            throw new IllegalArgumentException("Paimon MULTISET count must be positive: " + value);
        }
    }

    private static DataType arrayElementLogicalType(DataType logicalType)
    {
        if (logicalType instanceof VectorType vectorType) {
            return vectorType.getElementType();
        }
        if (logicalType instanceof org.apache.paimon.types.ArrayType arrayType) {
            return arrayType.getElementType();
        }
        throw new UnsupportedOperationException("Paimon ARRAY or VECTOR logical type metadata is required");
    }

    private static org.apache.paimon.types.RowType rowLogicalType(DataType logicalType)
    {
        if (logicalType instanceof org.apache.paimon.types.RowType rowType) {
            return rowType;
        }
        throw new UnsupportedOperationException("Paimon ROW logical type metadata is required");
    }

    private static void validateLogicalType(Type type, DataType logicalType)
    {
        if (type instanceof ArrayType) {
            validateLogicalType(type.getTypeParameters().get(0), arrayElementLogicalType(logicalType));
            return;
        }
        if (type instanceof RowType) {
            org.apache.paimon.types.RowType rowLogicalType = rowLogicalType(logicalType);
            validateRowFieldCount(type.getTypeParameters().size(), rowLogicalType.getFieldCount());
            for (int index = 0; index < type.getTypeParameters().size(); index++) {
                validateLogicalType(type.getTypeParameters().get(index), rowLogicalType.getTypeAt(index));
            }
            return;
        }
        if (type instanceof MapType) {
            if (logicalType instanceof org.apache.paimon.types.MapType mapType) {
                validateLogicalType(type.getTypeParameters().get(0), mapType.getKeyType());
                validateLogicalType(type.getTypeParameters().get(1), mapType.getValueType());
                return;
            }
            if (logicalType instanceof MultisetType multisetType) {
                if (!type.getTypeParameters().get(1).equals(INTEGER)) {
                    throw new UnsupportedOperationException("Paimon MULTISET requires Trino integer count type metadata");
                }
                validateLogicalType(type.getTypeParameters().get(0), multisetType.getElementType());
                return;
            }
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unhandled Paimon logical type for Map: " + logicalType);
        }
    }

    private static void validateRowFieldCount(int trinoFieldCount, int paimonFieldCount)
    {
        if (trinoFieldCount != paimonFieldCount) {
            throw new IllegalArgumentException("Paimon ROW field count mismatch: expected "
                    + paimonFieldCount + ", got " + trinoFieldCount);
        }
    }

    private static RuntimeException propagateBlockBuilderFailure(Throwable failure)
    {
        if (failure instanceof RuntimeException runtimeException) {
            return runtimeException;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        return new RuntimeException(failure);
    }
}
