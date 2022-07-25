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

import io.airlift.slice.Slices;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.UuidType;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.UUID;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.iceberg.util.Timestamps.timestampTzFromMicros;
import static io.trino.spi.type.Decimals.isShortDecimal;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;

public final class IcebergTypes
{
    private IcebergTypes() {}

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
            return (Boolean) value;
        }
        if (icebergType instanceof Types.IntegerType) {
            return ((Integer) value).longValue();
        }
        if (icebergType instanceof Types.LongType) {
            //noinspection RedundantCast
            return (Long) value;
        }
        if (icebergType instanceof Types.FloatType) {
            return (long) Float.floatToIntBits((Float) value);
        }
        if (icebergType instanceof Types.DoubleType) {
            //noinspection RedundantCast
            return (Double) value;
        }
        if (icebergType instanceof Types.DecimalType) {
            Types.DecimalType icebergDecimalType = (Types.DecimalType) icebergType;
            DecimalType trinoDecimalType = DecimalType.createDecimalType(icebergDecimalType.precision(), icebergDecimalType.scale());
            if (isShortDecimal(trinoDecimalType)) {
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
            return Slices.wrappedBuffer(((ByteBuffer) value).array().clone());
        }
        if (icebergType instanceof Types.DateType) {
            return ((Integer) value).longValue();
        }
        if (icebergType instanceof Types.TimeType) {
            return Math.multiplyExact((Long) value, PICOSECONDS_PER_MICROSECOND);
        }
        if (icebergType instanceof Types.TimestampType) {
            long epochMicros = (long) value;
            if (((Types.TimestampType) icebergType).shouldAdjustToUTC()) {
                return timestampTzFromMicros(epochMicros);
            }
            return epochMicros;
        }
        if (icebergType instanceof Types.UUIDType) {
            return UuidType.javaUuidToTrinoUuid((UUID) value);
        }

        throw new UnsupportedOperationException("Unsupported iceberg type: " + icebergType);
    }
}
