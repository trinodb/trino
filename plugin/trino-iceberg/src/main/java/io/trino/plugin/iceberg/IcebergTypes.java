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
package io.trino.plugin.iceberg;

import com.google.common.math.LongMath;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.UUID;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.base.io.ByteBuffers.getWrappedBytes;
import static io.trino.plugin.iceberg.util.Timestamps.timestampTzFromMicros;
import static io.trino.plugin.iceberg.util.Timestamps.timestampTzToMicros;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.UuidType.javaUuidToTrinoUuid;
import static io.trino.spi.type.UuidType.trinoUuidToJavaUuid;
import static java.lang.Float.floatToIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.multiplyExact;
import static java.lang.Math.toIntExact;
import static java.math.RoundingMode.UNNECESSARY;
import static java.util.Objects.requireNonNull;

public final class IcebergTypes
{
    private IcebergTypes() {}

    /**
     * Convert value from Trino representation to Iceberg representation.
     *
     * @apiNote This accepts a Trino type because, currently, no two Iceberg types translate to one Trino type.
     */
    public static Object convertTrinoValueToIceberg(io.trino.spi.type.Type type, Object trinoNativeValue)
    {
        requireNonNull(trinoNativeValue, "trinoNativeValue is null");

        if (type == BOOLEAN) {
            //noinspection RedundantCast
            return (boolean) trinoNativeValue;
        }

        if (type == INTEGER) {
            return toIntExact((long) trinoNativeValue);
        }

        if (type == BIGINT) {
            //noinspection RedundantCast
            return (long) trinoNativeValue;
        }

        if (type == REAL) {
            return intBitsToFloat(toIntExact((long) trinoNativeValue));
        }

        if (type == DOUBLE) {
            //noinspection RedundantCast
            return (double) trinoNativeValue;
        }

        if (type instanceof DecimalType decimalType) {
            if (decimalType.isShort()) {
                return BigDecimal.valueOf((long) trinoNativeValue).movePointLeft(decimalType.getScale());
            }
            return new BigDecimal(((Int128) trinoNativeValue).toBigInteger(), decimalType.getScale());
        }

        if (type == DATE) {
            return toIntExact((long) trinoNativeValue);
        }

        if (type.equals(TIME_MICROS)) {
            return LongMath.divide((long) trinoNativeValue, PICOSECONDS_PER_MICROSECOND, UNNECESSARY);
        }

        if (type.equals(TIMESTAMP_MICROS)) {
            //noinspection RedundantCast
            return (long) trinoNativeValue;
        }

        if (type.equals(TIMESTAMP_TZ_MICROS)) {
            return timestampTzToMicros((LongTimestampWithTimeZone) trinoNativeValue);
        }

        if (type instanceof VarcharType) {
            return ((Slice) trinoNativeValue).toStringUtf8();
        }

        if (type instanceof VarbinaryType) {
            return ByteBuffer.wrap(((Slice) trinoNativeValue).getBytes());
        }

        if (type == UuidType.UUID) {
            return trinoUuidToJavaUuid(((Slice) trinoNativeValue));
        }

        throw new UnsupportedOperationException("Unsupported type: " + type);
    }

    /**
     * Convert value from Iceberg representation to Trino representation.
     */
    public static Object convertIcebergValueToTrino(Type icebergType, Object value)
    {
        if (value == null) {
            return null;
        }
        if (icebergType instanceof Types.BooleanType) {
            //noinspection RedundantCast
            return (boolean) value;
        }
        if (icebergType instanceof Types.IntegerType) {
            return (long) (int) value;
        }
        if (icebergType instanceof Types.LongType) {
            //noinspection RedundantCast
            return (long) value;
        }
        if (icebergType instanceof Types.FloatType) {
            return (long) floatToIntBits((float) value);
        }
        if (icebergType instanceof Types.DoubleType) {
            //noinspection RedundantCast
            return (double) value;
        }
        if (icebergType instanceof Types.DecimalType icebergDecimalType) {
            DecimalType trinoDecimalType = DecimalType.createDecimalType(icebergDecimalType.precision(), icebergDecimalType.scale());
            if (trinoDecimalType.isShort()) {
                return Decimals.encodeShortScaledValue((BigDecimal) value, trinoDecimalType.getScale());
            }
            return Decimals.encodeScaledValue((BigDecimal) value, trinoDecimalType.getScale());
        }
        if (icebergType instanceof Types.StringType) {
            // Partition values are passed as String, but min/max values are passed as a CharBuffer
            if (value instanceof CharBuffer) {
                value = new String(((CharBuffer) value).array());
            }
            return utf8Slice(((String) value));
        }
        if (icebergType instanceof Types.BinaryType) {
            return Slices.wrappedBuffer(getWrappedBytes((ByteBuffer) value).clone());
        }
        if (icebergType instanceof Types.DateType) {
            return (long) (int) value;
        }
        if (icebergType instanceof Types.TimeType) {
            return multiplyExact((long) value, PICOSECONDS_PER_MICROSECOND);
        }
        if (icebergType instanceof Types.TimestampType icebergTimestampType) {
            long epochMicros = (long) value;
            if (icebergTimestampType.shouldAdjustToUTC()) {
                return timestampTzFromMicros(epochMicros);
            }
            return epochMicros;
        }
        if (icebergType instanceof Types.UUIDType) {
            return javaUuidToTrinoUuid((UUID) value);
        }

        throw new UnsupportedOperationException("Unsupported iceberg type: " + icebergType);
    }
}
