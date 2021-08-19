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
package io.trino.plugin.phoenix5;

import com.google.common.base.CharMatcher;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.sql.Date;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.decodeUnscaledValue;
import static io.trino.spi.type.Decimals.encodeScaledValue;
import static io.trino.spi.type.Decimals.encodeShortScaledValue;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.joda.time.DateTimeZone.UTC;

public final class TypeUtils
{
    private TypeUtils() {}

    public static String getArrayElementPhoenixTypeName(ConnectorSession session, PhoenixClient client, Type elementType)
    {
        if (elementType instanceof VarcharType) {
            return "VARCHAR";
        }

        if (elementType instanceof CharType) {
            return "CHAR";
        }

        if (elementType instanceof DecimalType) {
            return "DECIMAL";
        }

        return client.toWriteMapping(session, elementType).getDataType().toUpperCase(ENGLISH);
    }

    public static Block jdbcObjectArrayToBlock(ConnectorSession session, Type type, Object[] elements)
    {
        BlockBuilder builder = type.createBlockBuilder(null, elements.length);
        for (Object element : elements) {
            writeNativeValue(type, builder, jdbcObjectToTrinoNative(session, element, type));
        }
        return builder.build();
    }

    public static Object[] getJdbcObjectArray(ConnectorSession session, Type elementType, Block block)
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
        if (elementType instanceof ArrayType) {
            handleArrayNulls(valuesArray, subArrayLength);
        }
        return valuesArray;
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

    private static void handleArrayNulls(Object[] valuesArray, int length)
    {
        for (int i = 0; i < valuesArray.length; i++) {
            if (valuesArray[i] == null) {
                valuesArray[i] = new Object[length];
            }
        }
    }

    private static Object jdbcObjectToTrinoNative(ConnectorSession session, Object jdbcObject, Type type)
    {
        if (jdbcObject == null) {
            return null;
        }

        if (BOOLEAN.equals(type)
                || TINYINT.equals(type)
                || SMALLINT.equals(type)
                || INTEGER.equals(type)
                || BIGINT.equals(type)
                || DOUBLE.equals(type)) {
            return jdbcObject;
        }

        if (type instanceof ArrayType) {
            return jdbcObjectArrayToBlock(session, ((ArrayType) type).getElementType(), (Object[]) jdbcObject);
        }

        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            BigDecimal value = (BigDecimal) jdbcObject;
            if (decimalType.isShort()) {
                return encodeShortScaledValue(value, decimalType.getScale());
            }
            return encodeScaledValue(value, decimalType.getScale());
        }

        if (REAL.equals(type)) {
            return floatToRawIntBits((float) jdbcObject);
        }

        if (DATE.equals(type)) {
            long localMillis = ((Date) jdbcObject).getTime();
            // Convert it to a ~midnight in UTC.
            long utcMillis = ISOChronology.getInstance().getZone().getMillisKeepLocal(UTC, localMillis);
            // convert to days
            return MILLISECONDS.toDays(utcMillis);
        }

        if (type instanceof VarcharType) {
            return utf8Slice((String) jdbcObject);
        }

        if (type instanceof CharType) {
            return utf8Slice(CharMatcher.is(' ').trimTrailingFrom((String) jdbcObject));
        }

        throw new TrinoException(NOT_SUPPORTED, format("Unsupported type %s and object type %s", type, jdbcObject.getClass()));
    }

    private static Object trinoNativeToJdbcObject(ConnectorSession session, Type type, Object object)
    {
        if (object == null) {
            return null;
        }

        if (DOUBLE.equals(type) || BOOLEAN.equals(type) || BIGINT.equals(type)) {
            return object;
        }

        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            if (decimalType.isShort()) {
                BigInteger unscaledValue = BigInteger.valueOf((long) object);
                return new BigDecimal(unscaledValue, decimalType.getScale(), new MathContext(decimalType.getPrecision()));
            }
            BigInteger unscaledValue = decodeUnscaledValue((Slice) object);
            return new BigDecimal(unscaledValue, decimalType.getScale(), new MathContext(decimalType.getPrecision()));
        }

        if (REAL.equals(type)) {
            return intBitsToFloat(toIntExact((long) object));
        }

        if (TINYINT.equals(type)) {
            return SignedBytes.checkedCast((long) object);
        }

        if (SMALLINT.equals(type)) {
            return Shorts.checkedCast((long) object);
        }

        if (INTEGER.equals(type)) {
            return toIntExact((long) object);
        }

        if (DATE.equals(type)) {
            // convert to midnight in default time zone
            long millis = DAYS.toMillis((long) object);
            return new Date(UTC.getMillisKeepLocal(DateTimeZone.getDefault(), millis));
        }

        if (type instanceof VarcharType || type instanceof CharType) {
            return ((Slice) object).toStringUtf8();
        }

        if (type instanceof ArrayType) {
            // process subarray of multi-dimensional array
            return getJdbcObjectArray(session, ((ArrayType) type).getElementType(), (Block) object);
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported type: " + type);
    }
}
