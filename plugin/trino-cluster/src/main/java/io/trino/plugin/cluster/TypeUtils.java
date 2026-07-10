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
package io.trino.plugin.cluster;

import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.jdbc.Row;
import io.trino.jdbc.RowField;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Collection;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.jdbc.StandardColumnMappings.fromTrinoTimestamp;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.block.ArrayValueBuilder.buildArrayValue;
import static io.trino.spi.block.MapValueBuilder.buildMapValue;
import static io.trino.spi.block.RowValueBuilder.buildRowValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;

final class TypeUtils
{
    private TypeUtils() {}

    static Object[] getJdbcObjectArray(ConnectorSession session, Type elementType, Block block)
            throws SQLException
    {
        int positionCount = block.getPositionCount();
        Object[] valuesArray = new Object[positionCount];
        int subArrayLength = 1;
        for (int i = 0; i < positionCount; i++) {
            Object objectValue = trinoNativeToJdbcObject(session, elementType, readNativeValue(elementType, block, i));
            valuesArray[i] = objectValue;
            if (objectValue != null && objectValue.getClass().isArray()) {
                subArrayLength = Math.max(subArrayLength, Array.getLength(objectValue));
            }
        }
        return valuesArray;
    }

    public static int arrayDepth(Object jdbcArray)
    {
        checkArgument(jdbcArray.getClass().isArray(), "jdbcArray is not an array");
        int depth = 0;
        while (jdbcArray != null && jdbcArray.getClass().isArray()) {
            depth++;
            if (Array.getLength(jdbcArray) == 0) {
                return depth;
            }
            jdbcArray = Array.get(jdbcArray, 0);
        }
        return depth;
    }

    private static Object trinoNativeToJdbcObject(ConnectorSession session, Type trinoType, Object object)
            throws SQLException
    {
        if (object == null) {
            return null;
        }

        if (DOUBLE.equals(trinoType) || BOOLEAN.equals(trinoType) || BIGINT.equals(trinoType)) {
            return object;
        }

        if (trinoType instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) trinoType;
            if (decimalType.isShort()) {
                BigInteger unscaledValue = BigInteger.valueOf((long) object);
                return new BigDecimal(unscaledValue, decimalType.getScale(), new MathContext(decimalType.getPrecision()));
            }
            BigInteger unscaledValue = ((Int128) object).toBigInteger();
            return new BigDecimal(unscaledValue, decimalType.getScale(), new MathContext(decimalType.getPrecision()));
        }

        if (REAL.equals(trinoType)) {
            return intBitsToFloat(toIntExact((long) object));
        }

        if (TINYINT.equals(trinoType)) {
            return SignedBytes.checkedCast((long) object);
        }

        if (SMALLINT.equals(trinoType)) {
            return Shorts.checkedCast((long) object);
        }

        if (INTEGER.equals(trinoType)) {
            return toIntExact((long) object);
        }

        if (DATE.equals(trinoType)) {
            return Date.valueOf(LocalDate.ofEpochDay((long) object));
        }

        if (TIMESTAMP_MILLIS.equals(trinoType)) {
            return fromTrinoTimestamp((long) object);
        }

        if (TIMESTAMP_TZ_MILLIS.equals(trinoType)) {
            //TODO: Fix this - we need to use the timezone in the value while generating the JDBC equivalent.
            long millisUtc = unpackMillisUtc((long) object);
            return new Timestamp(millisUtc);
        }

        if (trinoType instanceof VarcharType || trinoType instanceof CharType) {
            return ((Slice) object).toStringUtf8();
        }

        if (trinoType instanceof ArrayType) {
            // process subarray of multi-dimensional array
            return getJdbcObjectArray(session, ((ArrayType) trinoType).getElementType(), (Block) object);
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported type: " + trinoType);
    }

    public static Block jdbcObjectArrayToBlock(Type type, Object[] elements)
    {
        BlockBuilder builder = type.createBlockBuilder(null, elements.length);
        for (Object element : elements) {
            appendToBlockBuilder(type, element, builder);
        }
        return builder.build();
    }

    public static Object[] toBoxedArray(Object jdbcArray)
    {
        requireNonNull(jdbcArray, "jdbcArray is null");
        checkArgument(jdbcArray.getClass().isArray(), "object is not an array: %s", jdbcArray.getClass().getName());

        if (!jdbcArray.getClass().getComponentType().isPrimitive()) {
            return (Object[]) jdbcArray;
        }

        int elementCount = Array.getLength(jdbcArray);
        Object[] elements = new Object[elementCount];
        for (int i = 0; i < elementCount; i++) {
            elements[i] = Array.get(jdbcArray, i);
        }
        return elements;
    }

    static Object toNativeContainerValue(Type type, Object elementsData)
    {
        if (elementsData == null) {
            return null;
        }

        if (type instanceof ArrayType arrayType) {
            Collection<?> elementsArray = (Collection<?>) elementsData;
            return buildArrayValue(arrayType, elementsArray.size(), valueBuilder -> {
                for (Object subElement : elementsArray) {
                    appendToBlockBuilder(type.getTypeParameters().get(0), subElement, valueBuilder);
                }
            });
        }
        if (type instanceof RowType rowType) {
            return buildRowValue(rowType, fields -> {
                int fieldIndex = 0;
                for (Object subElement : (Iterable<?>) ((Row) elementsData).getFields()) {
                    appendToBlockBuilder(type.getTypeParameters().get(fieldIndex), ((RowField) subElement).getValue(), fields.get(fieldIndex));
                    fieldIndex++;
                }
            });
        }
        if (type instanceof MapType mapType) {
            Map<?, ?> elementMap = (Map<?, ?>) elementsData;
            return buildMapValue(
                    mapType,
                    elementMap.size(),
                    (keyBuilder, valueBuilder) -> {
                        elementMap.forEach((key, value) -> {
                            appendToBlockBuilder(mapType.getKeyType(), key, keyBuilder);
                            appendToBlockBuilder(mapType.getValueType(), value, valueBuilder);
                        });
                    });
        }
        if (type instanceof BooleanType) {
            return elementsData;
        }
        if (type instanceof TinyintType) {
            return (long) (byte) elementsData;
        }
        if (type instanceof SmallintType) {
            return (long) (short) elementsData;
        }
        if (type instanceof IntegerType) {
            return (long) (int) elementsData;
        }
        if (type instanceof BigintType) {
            return elementsData;
        }
        if (type instanceof RealType) {
            return (long) Float.floatToRawIntBits((float) elementsData);
        }
        if (type instanceof DoubleType) {
            return elementsData;
        }
        if (type instanceof VarcharType) {
            return Slices.utf8Slice(elementsData.toString());
        }
        if (type instanceof DateType) {
            long daysSinceEpochInLocalZone = ((Date) elementsData).toLocalDate().toEpochDay();
            return daysSinceEpochInLocalZone;
        }
        if (type instanceof TimestampType) {
            Instant instant = ((Timestamp) elementsData).toInstant();
            long epochSecond = instant.getEpochSecond();
            int nano = instant.getNano();
            return epochSecond * 1000 + nano / 1_000_000;
        }

        throw new UnsupportedOperationException("unknown type");
    }

    static void appendToBlockBuilder(Type type, Object elementsData, BlockBuilder blockBuilder)
    {
        writeNativeValue(type, blockBuilder, toNativeContainerValue(type, elementsData));
    }
}
