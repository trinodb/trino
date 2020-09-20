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
package io.prestosql.plugin.phoenix;

import com.google.common.base.CharMatcher;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.Slice;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.sql.Date;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.Decimals.decodeUnscaledValue;
import static io.prestosql.spi.type.Decimals.encodeScaledValue;
import static io.prestosql.spi.type.Decimals.encodeShortScaledValue;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.TypeUtils.readNativeValue;
import static io.prestosql.spi.type.TypeUtils.writeNativeValue;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
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

        return client.toWriteMapping(session, elementType).getDataType().toUpperCase();
    }

    public static Block jdbcObjectArrayToBlock(ConnectorSession session, Type type, Object[] elements)
    {
        BlockBuilder builder = type.createBlockBuilder(null, elements.length);
        for (Object element : elements) {
            writeNativeValue(type, builder, jdbcObjectToPrestoNative(session, element, type));
        }
        return builder.build();
    }

    public static Object[] getJdbcObjectArray(ConnectorSession session, Type elementType, Block block)
    {
        int positionCount = block.getPositionCount();
        Object[] valuesArray = new Object[positionCount];
        int subArrayLength = 1;
        for (int i = 0; i < positionCount; i++) {
            Object objectValue = prestoNativeToJdbcObject(session, elementType, readNativeValue(elementType, block, i));
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

    private static Object jdbcObjectToPrestoNative(ConnectorSession session, Object jdbcObject, Type prestoType)
    {
        if (jdbcObject == null) {
            return null;
        }

        if (BOOLEAN.equals(prestoType)
                || TINYINT.equals(prestoType)
                || SMALLINT.equals(prestoType)
                || INTEGER.equals(prestoType)
                || BIGINT.equals(prestoType)
                || DOUBLE.equals(prestoType)) {
            return jdbcObject;
        }

        if (prestoType instanceof ArrayType) {
            return jdbcObjectArrayToBlock(session, ((ArrayType) prestoType).getElementType(), (Object[]) jdbcObject);
        }

        if (prestoType instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) prestoType;
            BigDecimal value = (BigDecimal) jdbcObject;
            if (decimalType.isShort()) {
                return encodeShortScaledValue(value, decimalType.getScale());
            }
            return encodeScaledValue(value, decimalType.getScale());
        }

        if (REAL.equals(prestoType)) {
            return floatToRawIntBits((float) jdbcObject);
        }

        if (DATE.equals(prestoType)) {
            long localMillis = ((Date) jdbcObject).getTime();
            // Convert it to a ~midnight in UTC.
            long utcMillis = ISOChronology.getInstance().getZone().getMillisKeepLocal(UTC, localMillis);
            // convert to days
            return MILLISECONDS.toDays(utcMillis);
        }

        if (prestoType instanceof VarcharType) {
            return utf8Slice((String) jdbcObject);
        }

        if (prestoType instanceof CharType) {
            return utf8Slice(CharMatcher.is(' ').trimTrailingFrom((String) jdbcObject));
        }

        throw new PrestoException(NOT_SUPPORTED, format("Unsupported type %s and object type %s", prestoType, jdbcObject.getClass()));
    }

    private static Object prestoNativeToJdbcObject(ConnectorSession session, Type prestoType, Object prestoNative)
    {
        if (prestoNative == null) {
            return null;
        }

        if (DOUBLE.equals(prestoType) || BOOLEAN.equals(prestoType) || BIGINT.equals(prestoType)) {
            return prestoNative;
        }

        if (prestoType instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) prestoType;
            if (decimalType.isShort()) {
                BigInteger unscaledValue = BigInteger.valueOf((long) prestoNative);
                return new BigDecimal(unscaledValue, decimalType.getScale(), new MathContext(decimalType.getPrecision()));
            }
            BigInteger unscaledValue = decodeUnscaledValue((Slice) prestoNative);
            return new BigDecimal(unscaledValue, decimalType.getScale(), new MathContext(decimalType.getPrecision()));
        }

        if (REAL.equals(prestoType)) {
            return intBitsToFloat(toIntExact((long) prestoNative));
        }

        if (TINYINT.equals(prestoType)) {
            return SignedBytes.checkedCast((long) prestoNative);
        }

        if (SMALLINT.equals(prestoType)) {
            return Shorts.checkedCast((long) prestoNative);
        }

        if (INTEGER.equals(prestoType)) {
            return toIntExact((long) prestoNative);
        }

        if (DATE.equals(prestoType)) {
            // convert to midnight in default time zone
            long millis = DAYS.toMillis((long) prestoNative);
            return new Date(UTC.getMillisKeepLocal(DateTimeZone.getDefault(), millis));
        }

        if (prestoType instanceof VarcharType || prestoType instanceof CharType) {
            return ((Slice) prestoNative).toStringUtf8();
        }

        if (prestoType instanceof ArrayType) {
            // process subarray of multi-dimensional array
            return getJdbcObjectArray(session, ((ArrayType) prestoType).getElementType(), (Block) prestoNative);
        }

        throw new PrestoException(NOT_SUPPORTED, "Unsupported type: " + prestoType);
    }
}
