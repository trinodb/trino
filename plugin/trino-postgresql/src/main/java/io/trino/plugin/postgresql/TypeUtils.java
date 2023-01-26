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
package io.trino.plugin.postgresql;

import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.joda.time.DateTimeZone;
import org.postgresql.util.PGobject;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.jdbc.StandardColumnMappings.fromTrinoTimestamp;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.toIntExact;
import static java.util.concurrent.TimeUnit.DAYS;
import static org.joda.time.DateTimeZone.UTC;

final class TypeUtils
{
    private TypeUtils() {}

    // PostgreSQL jdbc array element names understood by org.postgresql.jdbc2.TypeInfoCache#getPGArrayType
    // for multidimensional arrays, this should return the base type (e.g. array(array(integer)) returns 'integer')
    static String getArrayElementPgTypeName(ConnectorSession session, PostgreSqlClient client, Type elementType)
    {
        if (DOUBLE.equals(elementType)) {
            return "float8";
        }

        if (REAL.equals(elementType)) {
            return "float4";
        }

        // TypeInfoCache#getPGArrayType apparently doesn't allow for specifying variable-length limits.
        // Since PostgreSQL char[] can only hold single byte elements, map all CharType to varchar
        if (elementType instanceof VarcharType || elementType instanceof CharType) {
            return "varchar";
        }

        // TypeInfoCache#getPGArrayType apparently doesn't allow for specifying variable-length limits.
        // Map all timestamptz(x) types to unparametrized one which defaults to highest precision
        if (elementType instanceof TimestampWithTimeZoneType) {
            return "timestamptz";
        }

        // TypeInfoCache#getPGArrayType doesn't allow for specifying variable-length limits.
        // Map all timestamp(x) types to unparametrized one which defaults to highest precision
        if (elementType instanceof TimestampType) {
            return "timestamp";
        }

        if (elementType instanceof DecimalType) {
            return "decimal";
        }

        if (elementType instanceof ArrayType) {
            return getArrayElementPgTypeName(session, client, ((ArrayType) elementType).getElementType());
        }

        return client.toWriteMapping(session, elementType).getDataType();
    }

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
        // PostgreSql requires subarrays with matching dimensions, including arrays of null
        if (elementType instanceof ArrayType) {
            handleArrayNulls(valuesArray, subArrayLength);
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

    private static void handleArrayNulls(Object[] valuesArray, int length)
    {
        for (int i = 0; i < valuesArray.length; i++) {
            if (valuesArray[i] == null) {
                valuesArray[i] = new Object[length];
            }
        }
    }

    private static Object trinoNativeToJdbcObject(ConnectorSession session, Type trinoType, Object trinoNative)
            throws SQLException
    {
        if (trinoNative == null) {
            return null;
        }

        if (DOUBLE.equals(trinoType) || BOOLEAN.equals(trinoType) || BIGINT.equals(trinoType)) {
            return trinoNative;
        }

        if (trinoType instanceof DecimalType decimalType) {
            if (decimalType.isShort()) {
                BigInteger unscaledValue = BigInteger.valueOf((long) trinoNative);
                return new BigDecimal(unscaledValue, decimalType.getScale(), new MathContext(decimalType.getPrecision()));
            }
            BigInteger unscaledValue = ((Int128) trinoNative).toBigInteger();
            return new BigDecimal(unscaledValue, decimalType.getScale(), new MathContext(decimalType.getPrecision()));
        }

        if (REAL.equals(trinoType)) {
            return intBitsToFloat(toIntExact((long) trinoNative));
        }

        if (TINYINT.equals(trinoType)) {
            return SignedBytes.checkedCast((long) trinoNative);
        }

        if (SMALLINT.equals(trinoType)) {
            return Shorts.checkedCast((long) trinoNative);
        }

        if (INTEGER.equals(trinoType)) {
            return toIntExact((long) trinoNative);
        }

        if (DATE.equals(trinoType)) {
            // convert to midnight in default time zone
            long millis = DAYS.toMillis((long) trinoNative);
            return new Date(UTC.getMillisKeepLocal(DateTimeZone.getDefault(), millis));
        }

        if (trinoType instanceof TimestampType timestampType && timestampType.isShort()) {
            return toPgTimestamp(fromTrinoTimestamp((long) trinoNative));
        }

        if (trinoType instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            // PostgreSQL does not store zone, only the point in time
            int precision = timestampWithTimeZoneType.getPrecision();
            if (precision <= TimestampWithTimeZoneType.MAX_SHORT_PRECISION) {
                long millisUtc = unpackMillisUtc((long) trinoNative);
                return new Timestamp(millisUtc);
            }
            LongTimestampWithTimeZone value = (LongTimestampWithTimeZone) trinoNative;
            long epochSeconds = floorDiv(value.getEpochMillis(), MILLISECONDS_PER_SECOND);
            long nanosOfSecond = floorMod(value.getEpochMillis(), MILLISECONDS_PER_SECOND) * NANOSECONDS_PER_MILLISECOND
                    + value.getPicosOfMilli() / PICOSECONDS_PER_NANOSECOND;
            return OffsetDateTime.ofInstant(Instant.ofEpochSecond(epochSeconds, nanosOfSecond), UTC_KEY.getZoneId());
        }

        if (trinoType instanceof VarcharType || trinoType instanceof CharType) {
            return ((Slice) trinoNative).toStringUtf8();
        }

        if (trinoType instanceof ArrayType arrayType) {
            // process subarray of multi-dimensional array
            return getJdbcObjectArray(session, arrayType.getElementType(), (Block) trinoNative);
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported type: " + trinoType);
    }

    static PGobject toPgTimestamp(LocalDateTime localDateTime)
            throws SQLException
    {
        PGobject pgObject = new PGobject();
        pgObject.setType("timestamp");
        pgObject.setValue(localDateTime.toString());
        return pgObject;
    }
}
