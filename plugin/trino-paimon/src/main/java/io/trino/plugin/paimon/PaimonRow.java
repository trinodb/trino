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
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeUtils;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.GenericVariantBuilder;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.VectorType;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.paimon.PaimonTrinoTypeConversions.trinoTimePicosToPaimonMillis;
import static io.trino.plugin.paimon.PaimonTrinoTypeConversions.trinoTimestampToPaimon;
import static io.trino.plugin.paimon.PaimonTrinoTypeConversions.trinoTimestampWithTimeZoneToPaimon;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.Decimals.MAX_SHORT_PRECISION;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class PaimonRow
        implements InternalRow,
                   Serializable
{
    private RowKind rowKind;
    private final Page page;
    private final int position;
    private final List<Type> types;
    private final List<DataType> logicalTypes;

    public PaimonRow(Page singlePage, RowKind rowKind, List<Type> types, List<DataType> logicalTypes)
    {
        this(requireNonNull(singlePage, "singlePage is null"), 0, rowKind, types, logicalTypes);
        verify(singlePage.getPositionCount() == 1, "singlePage must have only one row");
    }

    public PaimonRow(Page page, int position, RowKind rowKind, List<Type> types, List<DataType> logicalTypes)
    {
        this(page, position, rowKind, types, logicalTypes, true);
    }

    /**
     * Creates a row without defensively copying type metadata.
     * Callers must pass immutable, null-free lists whose sizes match the page channel count.
     */
    public static PaimonRow fromTrustedTypeLists(
            Page page,
            int position,
            RowKind rowKind,
            List<Type> types,
            List<DataType> logicalTypes)
    {
        return new PaimonRow(page, position, rowKind, types, logicalTypes, false);
    }

    private PaimonRow(
            Page page,
            int position,
            RowKind rowKind,
            List<Type> types,
            List<DataType> logicalTypes,
            boolean copyTypeMetadata)
    {
        requireNonNull(page, "page is null");
        requireNonNull(rowKind, "rowKind is null");
        verify(position >= 0 && position < page.getPositionCount(),
                "position %s is not valid for page with %s positions",
                position,
                page.getPositionCount());
        requireNonNull(types, "types is null");
        requireNonNull(logicalTypes, "logicalTypes is null");
        verify(types.size() == page.getChannelCount(), "types size must match page channel count");
        verify(logicalTypes.size() == page.getChannelCount(), "logicalTypes size must match page channel count");
        this.page = page;
        this.position = position;
        this.rowKind = rowKind;
        this.types = copyTypeMetadata ? copyTypes(types) : types;
        this.logicalTypes = copyTypeMetadata ? copyLogicalTypes(logicalTypes) : logicalTypes;
    }

    private static List<Type> copyTypes(List<Type> types)
    {
        requireNonNull(types, "types is null");
        return Collections.unmodifiableList(new ArrayList<>(types.stream()
                .map(type -> requireNonNull(type, "type is null"))
                .toList()));
    }

    private static List<DataType> copyLogicalTypes(List<DataType> logicalTypes)
    {
        requireNonNull(logicalTypes, "logicalTypes is null");
        return Collections.unmodifiableList(new ArrayList<>(logicalTypes.stream()
                .map(type -> requireNonNull(type, "logicalType is null"))
                .toList()));
    }

    private static Variant parseVariantFromBlock(Block block, int position, Type type)
    {
        if (!type.getBaseName().equals(JSON)) {
            throw new UnsupportedOperationException("Paimon VARIANT requires Trino JSON type metadata");
        }
        try {
            Slice slice = (Slice) TypeUtils.readNativeValue(type, block, position);
            String json = slice.toStringUtf8();
            return GenericVariantBuilder.parseJson(json, true);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to parse Variant from JSON", e);
        }
    }

    private static byte readByte(Block block, int position)
    {
        long value = (long) TypeUtils.readNativeValue(TINYINT, block, position);
        return (byte) value;
    }

    private static int readInt(Block block, int position, Type type)
    {
        if (type instanceof TimeType) {
            return trinoTimePicosToPaimonMillis((long) TypeUtils.readNativeValue(type, block, position));
        }
        return toIntExact((long) TypeUtils.readNativeValue(INTEGER, block, position));
    }

    private static Timestamp readTimestamp(Block block, int position, Type type)
    {
        if (type instanceof TimestampType) {
            return trinoTimestampToPaimon(TypeUtils.readNativeValue(type, block, position));
        }
        if (type instanceof TimestampWithTimeZoneType) {
            return trinoTimestampWithTimeZoneToPaimon(TypeUtils.readNativeValue(type, block, position));
        }
        long value = (long) TypeUtils.readNativeValue(TIMESTAMP_MICROS, block, position);
        return Timestamp.fromMicros(value);
    }

    static byte[] normalizeBinaryValue(byte[] value, DataType logicalType)
    {
        requireNonNull(value, "value is null");
        requireNonNull(logicalType, "logicalType is null");
        return switch (logicalType.getTypeRoot()) {
            case BINARY -> {
                int length = DataTypeChecks.getLength(logicalType);
                if (value.length > length) {
                    throw new IllegalArgumentException(
                            "Cannot write %s bytes to Paimon BINARY(%s); value would be truncated"
                                    .formatted(value.length, length));
                }
                yield value.length == length ? value : Arrays.copyOf(value, length);
            }
            case VARBINARY -> {
                int length = DataTypeChecks.getLength(logicalType);
                if (value.length > length) {
                    throw new IllegalArgumentException(
                            "Cannot write %s bytes to Paimon VARBINARY(%s); value would be truncated"
                                    .formatted(value.length, length));
                }
                yield value;
            }
            default -> value;
        };
    }

    @Override
    public int getFieldCount()
    {
        return page.getChannelCount();
    }

    @Override
    public RowKind getRowKind()
    {
        return rowKind;
    }

    @Override
    public void setRowKind(RowKind rowKind)
    {
        this.rowKind = requireNonNull(rowKind, "rowKind is null");
    }

    @Override
    public boolean isNullAt(int i)
    {
        return page.getBlock(i).isNull(position);
    }

    @Override
    public boolean getBoolean(int i)
    {
        return (boolean) TypeUtils.readNativeValue(BOOLEAN, page.getBlock(i), position);
    }

    @Override
    public byte getByte(int i)
    {
        return readByte(page.getBlock(i), position);
    }

    @Override
    public short getShort(int i)
    {
        long value = (long) TypeUtils.readNativeValue(SMALLINT, page.getBlock(i), position);
        if (value < Short.MIN_VALUE || value > Short.MAX_VALUE) {
            throw new IllegalArgumentException("Value out of range for short: " + value);
        }
        return (short) value;
    }

    @Override
    public int getInt(int i)
    {
        long value = readInt(page.getBlock(i), position, types.get(i));
        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Value out of range for int: " + value);
        }
        return toIntExact(value);
    }

    @Override
    public long getLong(int i)
    {
        return (long) TypeUtils.readNativeValue(BIGINT, page.getBlock(i), position);
    }

    @Override
    public float getFloat(int i)
    {
        return Float.intBitsToFloat(toIntExact((long) TypeUtils.readNativeValue(REAL, page.getBlock(i), position)));
    }

    @Override
    public double getDouble(int i)
    {
        return (double) TypeUtils.readNativeValue(DOUBLE, page.getBlock(i), position);
    }

    @Override
    public BinaryString getString(int i)
    {
        return BinaryString.fromBytes(getBinary(i));
    }

    @Override
    public Decimal getDecimal(int i, int decimalPrecision, int decimalScale)
    {
        Object value = TypeUtils.readNativeValue(
                DecimalType.createDecimalType(decimalPrecision, decimalScale),
                page.getBlock(i),
                position);
        if (decimalPrecision <= MAX_SHORT_PRECISION) {
            return Decimal.fromUnscaledLong((Long) value, decimalPrecision, decimalScale);
        }
        else {
            BigDecimal bigDecimalValue = new BigDecimal(DecimalUtils.toBigInteger(value), decimalScale);
            return Decimal.fromBigDecimal(bigDecimalValue, decimalPrecision, decimalScale);
        }
    }

    @Override
    public Timestamp getTimestamp(int i, int timestampPrecision)
    {
        return readTimestamp(page.getBlock(i), position, types.get(i));
    }

    @Override
    public byte[] getBinary(int i)
    {
        Slice slice = (Slice) TypeUtils.readNativeValue(VARBINARY, page.getBlock(i), position);
        return normalizeBinaryValue(slice.getBytes(), logicalType(i));
    }

    @Override
    public Variant getVariant(int i)
    {
        if (isNullAt(i)) {
            return null;
        }
        return parseVariantFromBlock(page.getBlock(i), position, types.get(i));
    }

    @Override
    public Blob getBlob(int i)
    {
        if (isNullAt(i)) {
            return null;
        }
        return Blob.fromData(getBinary(i));
    }

    @Override
    public InternalArray getArray(int i)
    {
        if (isNullAt(i)) {
            return null;
        }
        Type type = types.get(i);
        if (type instanceof ArrayType arrayType) {
            return new TrinoArray(
                    arrayType.getObject(page.getBlock(i), position),
                    arrayType.getElementType(),
                    nestedLogicalType(i, 0));
        }
        throw new UnsupportedOperationException("Array type metadata is required");
    }

    @Override
    public InternalVector getVector(int i)
    {
        InternalArray array = getArray(i);
        return array == null ? null : asVector(array, logicalType(i));
    }

    @Override
    public InternalMap getMap(int i)
    {
        if (isNullAt(i)) {
            return null;
        }
        Type type = types.get(i);
        if (type instanceof MapType mapType) {
            SqlMap sqlMap = mapType.getObject(page.getBlock(i), position);
            return new TrinoMap(
                    sqlMap,
                    mapType.getKeyType(),
                    mapType.getValueType(),
                    mapKeyLogicalType(logicalType(i)),
                    mapValueLogicalType(logicalType(i)),
                    isMultiset(logicalType(i)));
        }
        throw new UnsupportedOperationException("Map type metadata is required");
    }

    @Override
    public InternalRow getRow(int i, int numFields)
    {
        if (isNullAt(i)) {
            return null;
        }
        Type type = types.get(i);
        if (type instanceof RowType rowType) {
            validateRowFieldCount(numFields, rowType.getFields().size());
            return new TrinoNestedRow(
                    rowType.getObject(page.getBlock(i), position),
                    rowKind,
                    rowType.getTypeParameters(),
                    nestedLogicalTypes(i));
        }
        throw new UnsupportedOperationException("Row type metadata is required");
    }

    private static void validateRowFieldCount(int requestedFieldCount, int actualFieldCount)
    {
        if (requestedFieldCount != actualFieldCount) {
            throw new IllegalArgumentException("Paimon ROW field count mismatch: expected "
                    + requestedFieldCount + ", got " + actualFieldCount);
        }
    }

    /**
     * Base class for InternalArray implementations wrapping Trino Block.
     */
    private abstract static class AbstractTrinoArray
            implements InternalArray
    {
        protected final Block block;
        protected final Type type;
        protected final DataType logicalType;

        AbstractTrinoArray(Block block, Type type, DataType logicalType)
        {
            this.block = block;
            this.type = requireNonNull(type, "type is null");
            this.logicalType = requireNonNull(logicalType, "logicalType is null");
        }

        /**
         * Get the actual position in the block for a logical position.
         */
        protected abstract int getPosition(int pos);

        @Override
        public boolean isNullAt(int pos)
        {
            return block.isNull(getPosition(pos));
        }

        @Override
        public boolean getBoolean(int pos)
        {
            return (boolean) TypeUtils.readNativeValue(BOOLEAN, block, getPosition(pos));
        }

        @Override
        public byte getByte(int pos)
        {
            return readByte(block, getPosition(pos));
        }

        @Override
        public short getShort(int pos)
        {
            long value = (long) TypeUtils.readNativeValue(SMALLINT, block, getPosition(pos));
            return (short) value;
        }

        @Override
        public int getInt(int pos)
        {
            return readInt(block, getPosition(pos), type);
        }

        @Override
        public long getLong(int pos)
        {
            return (long) TypeUtils.readNativeValue(BIGINT, block, getPosition(pos));
        }

        @Override
        public float getFloat(int pos)
        {
            return Float.intBitsToFloat(toIntExact((long) TypeUtils.readNativeValue(REAL, block, getPosition(pos))));
        }

        @Override
        public double getDouble(int pos)
        {
            return (double) TypeUtils.readNativeValue(DOUBLE, block, getPosition(pos));
        }

        @Override
        public BinaryString getString(int pos)
        {
            return BinaryString.fromBytes(getBinary(pos));
        }

        @Override
        public Decimal getDecimal(int pos, int precision, int scale)
        {
            Object value = TypeUtils.readNativeValue(
                    DecimalType.createDecimalType(precision, scale),
                    block,
                    getPosition(pos));
            if (precision <= MAX_SHORT_PRECISION) {
                return Decimal.fromUnscaledLong((Long) value, precision, scale);
            }
            else {
                BigDecimal bigDecimalValue = new BigDecimal(DecimalUtils.toBigInteger(value), scale);
                return Decimal.fromBigDecimal(bigDecimalValue, precision, scale);
            }
        }

        @Override
        public Timestamp getTimestamp(int pos, int precision)
        {
            return readTimestamp(block, getPosition(pos), type);
        }

        @Override
        public byte[] getBinary(int pos)
        {
            Slice slice = (Slice) TypeUtils.readNativeValue(VARBINARY, block, getPosition(pos));
            return normalizeBinaryValue(slice.getBytes(), logicalType);
        }

        @Override
        public Variant getVariant(int pos)
        {
            if (isNullAt(pos)) {
                return null;
            }
            return parseVariantFromBlock(block, getPosition(pos), type);
        }

        @Override
        public Blob getBlob(int pos)
        {
            if (isNullAt(pos)) {
                return null;
            }
            return Blob.fromData(getBinary(pos));
        }

        @Override
        public InternalArray getArray(int pos)
        {
            if (isNullAt(pos)) {
                return null;
            }
            if (type instanceof ArrayType arrayType) {
                return new TrinoArray(
                        arrayType.getObject(block, getPosition(pos)),
                        arrayType.getElementType(),
                        nestedLogicalType(0));
            }
            throw new UnsupportedOperationException("Array type metadata is required");
        }

        @Override
        public InternalVector getVector(int pos)
        {
            InternalArray array = getArray(pos);
            return array == null ? null : asVector(array, logicalType);
        }

        @Override
        public InternalMap getMap(int pos)
        {
            if (isNullAt(pos)) {
                return null;
            }
            if (type instanceof MapType mapType) {
                SqlMap sqlMap = mapType.getObject(block, getPosition(pos));
                return new TrinoMap(
                        sqlMap,
                        mapType.getKeyType(),
                        mapType.getValueType(),
                        mapKeyLogicalType(logicalType),
                        mapValueLogicalType(logicalType),
                        isMultiset(logicalType));
            }
            throw new UnsupportedOperationException("Map type metadata is required");
        }

        @Override
        public InternalRow getRow(int pos, int numFields)
        {
            if (isNullAt(pos)) {
                return null;
            }
            if (type instanceof RowType rowType) {
                validateRowFieldCount(numFields, rowType.getFields().size());
                return new TrinoNestedRow(
                        rowType.getObject(block, getPosition(pos)),
                        RowKind.INSERT,
                        rowType.getTypeParameters(),
                        nestedLogicalTypes());
            }
            throw new UnsupportedOperationException("Row type metadata is required");
        }

        @Override
        public boolean[] toBooleanArray()
        {
            boolean[] result = new boolean[size()];
            for (int i = 0; i < size(); i++) {
                result[i] = getBoolean(i);
            }
            return result;
        }

        @Override
        public byte[] toByteArray()
        {
            byte[] result = new byte[size()];
            for (int i = 0; i < size(); i++) {
                result[i] = getByte(i);
            }
            return result;
        }

        @Override
        public short[] toShortArray()
        {
            short[] result = new short[size()];
            for (int i = 0; i < size(); i++) {
                result[i] = getShort(i);
            }
            return result;
        }

        @Override
        public int[] toIntArray()
        {
            int[] result = new int[size()];
            for (int i = 0; i < size(); i++) {
                result[i] = getInt(i);
            }
            return result;
        }

        @Override
        public long[] toLongArray()
        {
            long[] result = new long[size()];
            for (int i = 0; i < size(); i++) {
                result[i] = getLong(i);
            }
            return result;
        }

        @Override
        public float[] toFloatArray()
        {
            float[] result = new float[size()];
            for (int i = 0; i < size(); i++) {
                result[i] = getFloat(i);
            }
            return result;
        }

        @Override
        public double[] toDoubleArray()
        {
            double[] result = new double[size()];
            for (int i = 0; i < size(); i++) {
                result[i] = getDouble(i);
            }
            return result;
        }

        private DataType nestedLogicalType(int index)
        {
            return DataTypeChecks.getNestedTypes(logicalType).get(index);
        }

        private List<DataType> nestedLogicalTypes()
        {
            return DataTypeChecks.getNestedTypes(logicalType);
        }
    }

    /**
     * TrinoArray implementation for {@link InternalArray}.
     */
    private static class TrinoArray
            extends AbstractTrinoArray
    {
        TrinoArray(Block block, Type type, DataType logicalType)
        {
            super(block, type, logicalType);
        }

        @Override
        protected int getPosition(int pos)
        {
            return pos;
        }

        @Override
        public int size()
        {
            return block.getPositionCount();
        }
    }

    private DataType logicalType(int index)
    {
        return logicalTypes.get(index);
    }

    private DataType nestedLogicalType(int columnIndex, int nestedIndex)
    {
        return DataTypeChecks.getNestedTypes(logicalType(columnIndex)).get(nestedIndex);
    }

    private List<DataType> nestedLogicalTypes(int columnIndex)
    {
        return DataTypeChecks.getNestedTypes(logicalType(columnIndex));
    }

    private static InternalVector asVector(InternalArray array, DataType logicalType)
    {
        if (!(logicalType instanceof VectorType vectorType)) {
            throw new UnsupportedOperationException("Paimon VECTOR logical type metadata is required");
        }
        validateVector(array, vectorType);
        if (array instanceof InternalVector vector) {
            return vector;
        }
        return new TrinoVector(array);
    }

    private static DataType mapKeyLogicalType(DataType logicalType)
    {
        return switch (logicalType.getTypeRoot()) {
            case MAP, MULTISET -> DataTypeChecks.getNestedTypes(logicalType).get(0);
            default -> throw new UnsupportedOperationException("Paimon MAP or MULTISET logical type metadata is required");
        };
    }

    private static DataType mapValueLogicalType(DataType logicalType)
    {
        return switch (logicalType.getTypeRoot()) {
            case MAP -> DataTypeChecks.getNestedTypes(logicalType).get(1);
            case MULTISET -> new IntType(false);
            default -> throw new UnsupportedOperationException("Paimon MAP or MULTISET logical type metadata is required");
        };
    }

    private static boolean isMultiset(DataType logicalType)
    {
        return logicalType.getTypeRoot() == DataTypeRoot.MULTISET;
    }

    private static void validateVector(InternalArray array, VectorType vectorType)
    {
        if (array.size() != vectorType.getLength()) {
            throw new IllegalArgumentException("Paimon VECTOR length mismatch: expected "
                    + vectorType.getLength() + ", got " + array.size());
        }
        for (int position = 0; position < array.size(); position++) {
            if (array.isNullAt(position)) {
                throw new IllegalArgumentException("Paimon VECTOR does not allow null elements");
            }
        }
    }

    private record TrinoVector(InternalArray array)
            implements InternalVector
    {
        @Override
        public int size()
        {
            return array.size();
        }

        @Override
        public boolean isNullAt(int pos)
        {
            return array.isNullAt(pos);
        }

        @Override
        public boolean getBoolean(int pos)
        {
            return array.getBoolean(pos);
        }

        @Override
        public byte getByte(int pos)
        {
            return array.getByte(pos);
        }

        @Override
        public short getShort(int pos)
        {
            return array.getShort(pos);
        }

        @Override
        public int getInt(int pos)
        {
            return array.getInt(pos);
        }

        @Override
        public long getLong(int pos)
        {
            return array.getLong(pos);
        }

        @Override
        public float getFloat(int pos)
        {
            return array.getFloat(pos);
        }

        @Override
        public double getDouble(int pos)
        {
            return array.getDouble(pos);
        }

        @Override
        public BinaryString getString(int pos)
        {
            return array.getString(pos);
        }

        @Override
        public Decimal getDecimal(int pos, int precision, int scale)
        {
            return array.getDecimal(pos, precision, scale);
        }

        @Override
        public Timestamp getTimestamp(int pos, int precision)
        {
            return array.getTimestamp(pos, precision);
        }

        @Override
        public byte[] getBinary(int pos)
        {
            return array.getBinary(pos);
        }

        @Override
        public Variant getVariant(int pos)
        {
            return array.getVariant(pos);
        }

        @Override
        public Blob getBlob(int pos)
        {
            return array.getBlob(pos);
        }

        @Override
        public InternalArray getArray(int pos)
        {
            return array.getArray(pos);
        }

        @Override
        public InternalVector getVector(int pos)
        {
            return array.getVector(pos);
        }

        @Override
        public InternalMap getMap(int pos)
        {
            return array.getMap(pos);
        }

        @Override
        public InternalRow getRow(int pos, int numFields)
        {
            return array.getRow(pos, numFields);
        }

        @Override
        public boolean[] toBooleanArray()
        {
            return array.toBooleanArray();
        }

        @Override
        public byte[] toByteArray()
        {
            return array.toByteArray();
        }

        @Override
        public short[] toShortArray()
        {
            return array.toShortArray();
        }

        @Override
        public int[] toIntArray()
        {
            return array.toIntArray();
        }

        @Override
        public long[] toLongArray()
        {
            return array.toLongArray();
        }

        @Override
        public float[] toFloatArray()
        {
            return array.toFloatArray();
        }

        @Override
        public double[] toDoubleArray()
        {
            return array.toDoubleArray();
        }
    }

    /**
     * TrinoMap implementation for {@link InternalMap}.
     */
    private record TrinoMap(
            SqlMap sqlMap,
            Type keyType,
            Type valueType,
            DataType keyLogicalType,
            DataType valueLogicalType,
            boolean multiset)
            implements InternalMap
    {
        private TrinoMap
        {
            if (multiset) {
                if (!valueType.equals(INTEGER)) {
                    throw new UnsupportedOperationException("Paimon MULTISET requires Trino integer count type metadata");
                }
                validateMultisetCounts(sqlMap);
            }
        }

        @Override
        public int size()
        {
            return sqlMap.getSize();
        }

        @Override
        public InternalArray keyArray()
        {
            Block keyBlock = sqlMap.getRawKeyBlock();
            int offset = sqlMap.getRawOffset();
            int count = sqlMap.getSize();
            return new TrinoArrayView(keyBlock, offset, count, keyType, keyLogicalType);
        }

        @Override
        public InternalArray valueArray()
        {
            Block valueBlock = sqlMap.getRawValueBlock();
            int offset = sqlMap.getRawOffset();
            int count = sqlMap.getSize();
            return new TrinoArrayView(valueBlock, offset, count, valueType, valueLogicalType);
        }

        private static void validateMultisetCounts(SqlMap sqlMap)
        {
            Block valueBlock = sqlMap.getRawValueBlock();
            int offset = sqlMap.getRawOffset();
            int count = sqlMap.getSize();
            for (int index = 0; index < count; index++) {
                int position = offset + index;
                if (valueBlock.isNull(position)) {
                    throw new IllegalArgumentException("Paimon MULTISET does not allow null counts");
                }
                int value = toIntExact((long) TypeUtils.readNativeValue(INTEGER, valueBlock, position));
                if (value <= 0) {
                    throw new IllegalArgumentException("Paimon MULTISET count must be positive: " + value);
                }
            }
        }
    }

    /**
     * TrinoNestedRow implementation for nested {@link InternalRow}.
     */
    private static class TrinoNestedRow
            implements InternalRow
    {
        private final SqlRow sqlRow;
        private RowKind rowKind;
        private final List<Type> types;
        private final List<DataType> logicalTypes;

        TrinoNestedRow(SqlRow sqlRow, RowKind rowKind, List<Type> types, List<DataType> logicalTypes)
        {
            this.sqlRow = sqlRow;
            this.rowKind = rowKind;
            requireNonNull(types, "types is null");
            requireNonNull(logicalTypes, "logicalTypes is null");
            verify(types.size() == sqlRow.getFieldCount(), "types size must match row field count");
            verify(logicalTypes.size() == sqlRow.getFieldCount(), "logicalTypes size must match row field count");
            this.types = copyTypes(types);
            this.logicalTypes = copyLogicalTypes(logicalTypes);
        }

        @Override
        public int getFieldCount()
        {
            return sqlRow.getFieldCount();
        }

        @Override
        public RowKind getRowKind()
        {
            return rowKind;
        }

        @Override
        public void setRowKind(RowKind rowKind)
        {
            this.rowKind = requireNonNull(rowKind, "rowKind is null");
        }

        @Override
        public boolean isNullAt(int pos)
        {
            Block fieldBlock = sqlRow.getRawFieldBlock(pos);
            return fieldBlock.isNull(sqlRow.getRawIndex());
        }

        @Override
        public boolean getBoolean(int pos)
        {
            Block fieldBlock = sqlRow.getRawFieldBlock(pos);
            return (boolean) TypeUtils.readNativeValue(BOOLEAN, fieldBlock, sqlRow.getRawIndex());
        }

        @Override
        public byte getByte(int pos)
        {
            Block fieldBlock = sqlRow.getRawFieldBlock(pos);
            return readByte(fieldBlock, sqlRow.getRawIndex());
        }

        @Override
        public short getShort(int pos)
        {
            Block fieldBlock = sqlRow.getRawFieldBlock(pos);
            long value = (long) TypeUtils.readNativeValue(SMALLINT, fieldBlock, sqlRow.getRawIndex());
            return (short) value;
        }

        @Override
        public int getInt(int pos)
        {
            Block fieldBlock = sqlRow.getRawFieldBlock(pos);
            return readInt(fieldBlock, sqlRow.getRawIndex(), types.get(pos));
        }

        @Override
        public long getLong(int pos)
        {
            Block fieldBlock = sqlRow.getRawFieldBlock(pos);
            return (long) TypeUtils.readNativeValue(BIGINT, fieldBlock, sqlRow.getRawIndex());
        }

        @Override
        public float getFloat(int pos)
        {
            Block fieldBlock = sqlRow.getRawFieldBlock(pos);
            return Float.intBitsToFloat(toIntExact((long) TypeUtils.readNativeValue(REAL, fieldBlock, sqlRow.getRawIndex())));
        }

        @Override
        public double getDouble(int pos)
        {
            Block fieldBlock = sqlRow.getRawFieldBlock(pos);
            return (double) TypeUtils.readNativeValue(DOUBLE, fieldBlock, sqlRow.getRawIndex());
        }

        @Override
        public BinaryString getString(int pos)
        {
            return BinaryString.fromBytes(getBinary(pos));
        }

        @Override
        public Decimal getDecimal(int pos, int precision, int scale)
        {
            Block fieldBlock = sqlRow.getRawFieldBlock(pos);
            Object value = TypeUtils.readNativeValue(
                    DecimalType.createDecimalType(precision, scale),
                    fieldBlock,
                    sqlRow.getRawIndex());
            if (precision <= MAX_SHORT_PRECISION) {
                return Decimal.fromUnscaledLong((Long) value, precision, scale);
            }
            else {
                BigDecimal bigDecimalValue = new BigDecimal(DecimalUtils.toBigInteger(value), scale);
                return Decimal.fromBigDecimal(bigDecimalValue, precision, scale);
            }
        }

        @Override
        public Timestamp getTimestamp(int pos, int precision)
        {
            Block fieldBlock = sqlRow.getRawFieldBlock(pos);
            return readTimestamp(fieldBlock, sqlRow.getRawIndex(), types.get(pos));
        }

        @Override
        public byte[] getBinary(int pos)
        {
            Block fieldBlock = sqlRow.getRawFieldBlock(pos);
            Slice slice = (Slice) TypeUtils.readNativeValue(VARBINARY, fieldBlock, sqlRow.getRawIndex());
            return normalizeBinaryValue(slice.getBytes(), logicalType(pos));
        }

        @Override
        public Variant getVariant(int pos)
        {
            if (isNullAt(pos)) {
                return null;
            }
            Block fieldBlock = sqlRow.getRawFieldBlock(pos);
            return parseVariantFromBlock(fieldBlock, sqlRow.getRawIndex(), types.get(pos));
        }

        @Override
        public Blob getBlob(int pos)
        {
            if (isNullAt(pos)) {
                return null;
            }
            return Blob.fromData(getBinary(pos));
        }

        @Override
        public InternalArray getArray(int pos)
        {
            if (isNullAt(pos)) {
                return null;
            }
            Block fieldBlock = sqlRow.getRawFieldBlock(pos);
            Type type = types.get(pos);
            if (type instanceof ArrayType arrayType) {
                return new TrinoArray(
                        arrayType.getObject(fieldBlock, sqlRow.getRawIndex()),
                        arrayType.getElementType(),
                        nestedLogicalType(pos, 0));
            }
            throw new UnsupportedOperationException("Array type metadata is required");
        }

        @Override
        public InternalVector getVector(int pos)
        {
            InternalArray array = getArray(pos);
            return array == null ? null : asVector(array, logicalType(pos));
        }

        @Override
        public InternalMap getMap(int pos)
        {
            if (isNullAt(pos)) {
                return null;
            }
            Block fieldBlock = sqlRow.getRawFieldBlock(pos);
            Type type = types.get(pos);
            if (type instanceof MapType mapType) {
                SqlMap sqlMap = mapType.getObject(fieldBlock, sqlRow.getRawIndex());
                return new TrinoMap(
                        sqlMap,
                        mapType.getKeyType(),
                        mapType.getValueType(),
                        mapKeyLogicalType(logicalType(pos)),
                        mapValueLogicalType(logicalType(pos)),
                        isMultiset(logicalType(pos)));
            }
            throw new UnsupportedOperationException("Map type metadata is required");
        }

        @Override
        public InternalRow getRow(int pos, int numFields)
        {
            if (isNullAt(pos)) {
                return null;
            }
            Block fieldBlock = sqlRow.getRawFieldBlock(pos);
            Type type = types.get(pos);
            if (type instanceof RowType rowType) {
                validateRowFieldCount(numFields, rowType.getFields().size());
                return new TrinoNestedRow(
                        rowType.getObject(fieldBlock, sqlRow.getRawIndex()),
                        rowKind,
                        rowType.getTypeParameters(),
                        nestedLogicalTypes(pos));
            }
            throw new UnsupportedOperationException("Row type metadata is required");
        }

        private DataType logicalType(int index)
        {
            return logicalTypes.get(index);
        }

        private DataType nestedLogicalType(int columnIndex, int nestedIndex)
        {
            return DataTypeChecks.getNestedTypes(logicalType(columnIndex)).get(nestedIndex);
        }

        private List<DataType> nestedLogicalTypes(int columnIndex)
        {
            return DataTypeChecks.getNestedTypes(logicalType(columnIndex));
        }
    }

    /**
     * TrinoArrayView implementation with offset and length for viewing part of a
     * Block. Used for Map key/value arrays.
     */
    private static class TrinoArrayView
            extends AbstractTrinoArray
    {
        private final int offset;
        private final int length;

        TrinoArrayView(Block block, int offset, int length, Type type, DataType logicalType)
        {
            super(block, type, logicalType);
            this.offset = offset;
            this.length = length;
        }

        @Override
        protected int getPosition(int pos)
        {
            return offset + pos;
        }

        @Override
        public int size()
        {
            return length;
        }
    }
}
