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
package io.trino.type;

import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.variant.Variant;
import io.trino.util.variant.VariantUtil;
import io.trino.util.variant.VariantWriter;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.function.OperatorType.SUBSCRIPT;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.multiplyExact;
import static java.lang.Math.toIntExact;

public final class VariantOperators
{
    private VariantOperators() {}

    @ScalarOperator(SUBSCRIPT)
    @SqlType(StandardTypes.VARIANT)
    public static Variant dereference(@SqlType(StandardTypes.VARIANT) Variant value, @SqlType(StandardTypes.BIGINT) long index)
    {
        checkArrayIndex(index, value.getArrayLength());
        return value.getArrayElement(toIntExact(index) - 1);
    }

    private static void checkArrayIndex(long index, int arrayLength)
    {
        if (index == 0) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "VARIANT array indices start at 1");
        }
        if (index < 0) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "VARIANT array subscript is negative: " + index);
        }
        if (index > arrayLength) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "VARIANT array subscript must be less than or equal to array length: %d > %d".formatted(index, arrayLength));
        }
    }

    @SqlNullable
    @ScalarOperator(SUBSCRIPT)
    @SqlType(StandardTypes.VARIANT)
    public static Variant dereference(@SqlType(StandardTypes.VARIANT) Variant value, @SqlType(StandardTypes.VARCHAR) Slice fieldName)
    {
        return value.getObjectField(fieldName).orElse(null);
    }

    @SqlNullable
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean castToBoolean(@SqlType(StandardTypes.VARIANT) Variant value)
    {
        return VariantUtil.asBoolean(value);
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.VARIANT)
    public static Variant castFromBoolean(@SqlType(StandardTypes.BOOLEAN) boolean value)
    {
        return Variant.ofBoolean(value);
    }

    @SqlNullable
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TINYINT)
    public static Long castToTinyint(@SqlType(StandardTypes.VARIANT) Variant value)
    {
        return VariantUtil.asTinyint(value);
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.VARIANT)
    public static Variant castFromTinyint(@SqlType(StandardTypes.TINYINT) long value)
    {
        return Variant.ofByte((byte) value);
    }

    @SqlNullable
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.SMALLINT)
    public static Long castToSmallint(@SqlType(StandardTypes.VARIANT) Variant value)
    {
        return VariantUtil.asSmallint(value);
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.VARIANT)
    public static Variant castFromSmallint(@SqlType(StandardTypes.SMALLINT) long value)
    {
        return Variant.ofShort((short) value);
    }

    @SqlNullable
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.INTEGER)
    public static Long castToInteger(@SqlType(StandardTypes.VARIANT) Variant value)
    {
        return VariantUtil.asInteger(value);
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.VARIANT)
    public static Variant castFromInteger(@SqlType(StandardTypes.INTEGER) long value)
    {
        return Variant.ofInt((int) value);
    }

    @SqlNullable
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.BIGINT)
    public static Long castToBigint(@SqlType(StandardTypes.VARIANT) Variant value)
    {
        return VariantUtil.asBigint(value);
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.VARIANT)
    public static Variant castFromBigint(@SqlType(StandardTypes.BIGINT) long value)
    {
        return Variant.ofLong(value);
    }

    @SqlNullable
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.REAL)
    public static Long castToReal(@SqlType(StandardTypes.VARIANT) Variant value)
    {
        return VariantUtil.asReal(value);
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.VARIANT)
    public static Variant castFromReal(@SqlType(StandardTypes.REAL) long value)
    {
        float floatValue = intBitsToFloat((int) value);
        return Variant.ofFloat(floatValue);
    }

    @SqlNullable
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.DOUBLE)
    public static Double castToDouble(@SqlType(StandardTypes.VARIANT) Variant value)
    {
        return VariantUtil.asDouble(value);
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.VARIANT)
    public static Variant castFromDouble(@SqlType(StandardTypes.DOUBLE) double value)
    {
        return Variant.ofDouble(value);
    }

    @SqlNullable
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.VARCHAR)
    public static Slice castToVarchar(@SqlType(StandardTypes.VARIANT) Variant value)
    {
        return VariantUtil.asVarchar(value);
    }

    @ScalarOperator(CAST)
    @LiteralParameters("x")
    @SqlType(StandardTypes.VARIANT)
    public static Variant castFromVarchar(@SqlType("varchar(x)") Slice value)
    {
        return Variant.ofString(value);
    }

    @SqlNullable
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.VARBINARY)
    public static Slice castToVarbinary(@SqlType(StandardTypes.VARIANT) Variant value)
    {
        return VariantUtil.asVarbinary(value);
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.VARIANT)
    public static Variant castFromVarbinary(@SqlType(StandardTypes.VARBINARY) Slice value)
    {
        return Variant.ofBinary(value);
    }

    @SqlNullable
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.DATE)
    public static Long castToDate(@SqlType(StandardTypes.VARIANT) Variant value)
    {
        return VariantUtil.asDate(value);
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.VARIANT)
    public static Variant castFromDate(@SqlType(StandardTypes.DATE) long value)
    {
        return Variant.ofDate(toIntExact(value));
    }

    @SqlNullable
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.UUID)
    public static Slice castToUuid(@SqlType(StandardTypes.VARIANT) Variant value)
    {
        return VariantUtil.asUuid(value);
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.VARIANT)
    public static Variant castFromUuid(@SqlType(StandardTypes.UUID) Slice value)
    {
        return Variant.ofUuid(value);
    }

    @SqlNullable
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.JSON)
    public static Slice castToJson(@SqlType(StandardTypes.VARIANT) Variant value)
    {
        return VariantUtil.asJson(value);
    }

    private static final VariantWriter JSON_VARIANT_WRITER = VariantWriter.create(JsonType.JSON);

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.VARIANT)
    public static Variant castFromJson(@SqlType(StandardTypes.JSON) Slice value)
    {
        return JSON_VARIANT_WRITER.write(value);
    }

    @ScalarOperator(CAST)
    public static final class VariantToTimeCast
    {
        private VariantToTimeCast() {}

        @LiteralParameters("p")
        @SqlNullable
        @SqlType("time(p)")
        public static Long castToTime(@LiteralParameter("p") long precision, @SqlType(StandardTypes.VARIANT) Variant value)
        {
            return VariantUtil.asTime(value, toIntExact(precision));
        }
    }

    @ScalarOperator(CAST)
    public static final class VariantFromTimeCast
    {
        private VariantFromTimeCast() {}

        @LiteralParameters("p")
        @SqlType(StandardTypes.VARIANT)
        public static Variant castFromTime(@LiteralParameter("p") long precision, @SqlType("time(p)") long epochPicos)
        {
            return Variant.ofTimeMicrosNtz(epochPicos / 1_000_000L);
        }
    }

    @ScalarOperator(CAST)
    public static final class VariantToTimestampCast
    {
        private VariantToTimestampCast() {}

        @LiteralParameters("p")
        @SqlNullable
        @SqlType("timestamp(p)")
        public static Long castToShortTimestamp(@LiteralParameter("p") long precision, @SqlType(StandardTypes.VARIANT) Variant value)
        {
            return VariantUtil.asShortTimestamp(value, toIntExact(precision));
        }

        @LiteralParameters("p")
        @SqlNullable
        @SqlType("timestamp(p)")
        public static LongTimestamp castToLongTimestamp(@LiteralParameter("p") long precision, @SqlType(StandardTypes.VARIANT) Variant value)
        {
            return VariantUtil.asLongTimestamp(value, toIntExact(precision));
        }
    }

    @ScalarOperator(CAST)
    public static final class VariantFromTimestampCast
    {
        private VariantFromTimestampCast() {}

        @LiteralParameters("p")
        @SqlType(StandardTypes.VARIANT)
        public static Variant castFromTimestamp(@LiteralParameter("p") long precision, @SqlType("timestamp(p)") long epochMicros)
        {
            return Variant.ofTimestampMicrosNtz(epochMicros);
        }

        @LiteralParameters("p")
        @SqlType(StandardTypes.VARIANT)
        public static Variant castFromTimestamp(@LiteralParameter("p") long precision, @SqlType("timestamp(p)") LongTimestamp timestamp)
        {
            long nanosFromMicros = multiplyExact(timestamp.getEpochMicros(), 1_000L);
            long extraNanos = timestamp.getPicosOfMicro() / 1_000; // 1000 ps = 1 ns
            long nanos = Math.addExact(nanosFromMicros, extraNanos);

            return Variant.ofTimestampNanosNtz(nanos);
        }
    }

    @ScalarOperator(CAST)
    public static final class VariantToTimestampWithTimeZoneCasts
    {
        private VariantToTimestampWithTimeZoneCasts() {}

        @LiteralParameters("p")
        @SqlNullable
        @SqlType("timestamp(p) with time zone")
        public static Long castToShortTimestampWithTimeZone(@LiteralParameter("p") long precision, @SqlType(StandardTypes.VARIANT) Variant variant)
        {
            return VariantUtil.asShortTimestampWithTimeZone(variant, toIntExact(precision));
        }

        @LiteralParameters("p")
        @SqlNullable
        @SqlType("timestamp(p) with time zone")
        public static LongTimestampWithTimeZone castToLongTimestampWithTimeZone(@LiteralParameter("p") long precision, @SqlType(StandardTypes.VARIANT) Variant variant)
        {
            return VariantUtil.asLongTimestampWithTimeZone(variant, toIntExact(precision));
        }
    }

    @ScalarOperator(CAST)
    public static final class VariantFromTimestampWithTimeZoneCasts
    {
        private VariantFromTimestampWithTimeZoneCasts() {}

        @LiteralParameters("p")
        @SqlType(StandardTypes.VARIANT)
        public static Variant castFromTimestampWithTimeZone(@LiteralParameter("p") long precision, @SqlType("timestamp(p) with time zone") long packedEpochMillis)
        {
            long epochMillis = unpackMillisUtc(packedEpochMillis);
            return Variant.ofTimestampMicrosUtc(multiplyExact(epochMillis, 1_000L));
        }

        @LiteralParameters("p")
        @SqlType(StandardTypes.VARIANT)
        public static Variant castFromTimestampWithTimeZone(@LiteralParameter("p") long precision, @SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestamp)
        {
            if (precision <= 6) {
                long millisFromMillis = multiplyExact(timestamp.getEpochMillis(), 1000L);
                int extraMillis = timestamp.getPicosOfMilli() / 1_000_000;
                long epochMicros = Math.addExact(millisFromMillis, extraMillis);
                return Variant.ofTimestampMicrosUtc(epochMicros);
            }

            long nanosFromMillis = multiplyExact(timestamp.getEpochMillis(), 1_000_000L);
            int extraNanos = timestamp.getPicosOfMilli() / 1_000;
            long nanos = Math.addExact(nanosFromMillis, extraNanos);
            return Variant.ofTimestampNanosUtc(nanos);
        }
    }
}
