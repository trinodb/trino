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
package io.trino.operator.scalar;

import io.airlift.slice.Slice;
import io.trino.json.Json;
import io.trino.json.JsonItemBuilder;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimeWithTimeZone;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.StandardTypes;

import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackOffsetMinutes;
import static io.trino.spi.type.DateTimeEncoding.unpackTimeNanos;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static java.lang.Math.toIntExact;

@Description("Constructs a JSON scalar from an SQL value")
public final class JsonScalarFunction
{
    private JsonScalarFunction() {}

    @ScalarFunction("json_scalar")
    @SqlNullable
    @SqlType(StandardTypes.JSON)
    public static Json fromUnknown(@SqlNullable @SqlType("unknown") Boolean value)
    {
        // SQL:2023 §6.41 GR 2a: a SQL null input yields the SQL null value, not the JSON null item
        return null;
    }

    @ScalarFunction("json_scalar")
    @SqlType(StandardTypes.JSON)
    public static Json fromBoolean(@SqlType(StandardTypes.BOOLEAN) boolean value)
    {
        return JsonItemBuilder.encodeBoolean(value);
    }

    @ScalarFunction("json_scalar")
    @SqlType(StandardTypes.JSON)
    public static Json fromTinyint(@SqlType(StandardTypes.TINYINT) long value)
    {
        return JsonItemBuilder.encode(w -> w.tinyintValue(value));
    }

    @ScalarFunction("json_scalar")
    @SqlType(StandardTypes.JSON)
    public static Json fromSmallint(@SqlType(StandardTypes.SMALLINT) long value)
    {
        return JsonItemBuilder.encode(w -> w.smallintValue(value));
    }

    @ScalarFunction("json_scalar")
    @SqlType(StandardTypes.JSON)
    public static Json fromInteger(@SqlType(StandardTypes.INTEGER) long value)
    {
        return JsonItemBuilder.encode(w -> w.integerValue(value));
    }

    @ScalarFunction("json_scalar")
    @SqlType(StandardTypes.JSON)
    public static Json fromBigint(@SqlType(StandardTypes.BIGINT) long value)
    {
        return JsonItemBuilder.encodeBigint(value);
    }

    @ScalarFunction("json_scalar")
    @SqlType(StandardTypes.JSON)
    public static Json fromReal(@SqlType(StandardTypes.REAL) long value)
    {
        return JsonItemBuilder.encode(w -> w.realBits((int) value));
    }

    @ScalarFunction("json_scalar")
    @SqlType(StandardTypes.JSON)
    public static Json fromDouble(@SqlType(StandardTypes.DOUBLE) double value)
    {
        return JsonItemBuilder.encodeDouble(value);
    }

    @ScalarFunction("json_scalar")
    @LiteralParameters("x")
    @SqlType(StandardTypes.JSON)
    public static Json fromVarchar(@SqlType("varchar(x)") Slice value)
    {
        return JsonItemBuilder.encodeVarchar(value);
    }

    @ScalarFunction("json_scalar")
    @SqlType(StandardTypes.JSON)
    public static Json fromDate(@SqlType(StandardTypes.DATE) long value)
    {
        return JsonItemBuilder.encodeDate(toIntExact(value));
    }

    @ScalarFunction("json_scalar")
    @LiteralParameters("p")
    @SqlType(StandardTypes.JSON)
    public static Json fromTime(
            @LiteralParameter("p") long precision,
            @SqlType("time(p)") long picosOfDay)
    {
        return JsonItemBuilder.encodeTime(toIntExact(precision), picosOfDay);
    }

    @ScalarFunction("json_scalar")
    @Description("Constructs a JSON scalar from a decimal value")
    public static final class FromDecimal
    {
        private FromDecimal() {}

        @LiteralParameters({"p", "s"})
        @SqlType(StandardTypes.JSON)
        public static Json fromShort(
                @LiteralParameter("p") long precision,
                @LiteralParameter("s") long scale,
                @SqlType("decimal(p, s)") long value)
        {
            return JsonItemBuilder.encodeShortDecimal((int) precision, (int) scale, value);
        }

        @LiteralParameters({"p", "s"})
        @SqlType(StandardTypes.JSON)
        public static Json fromLong(
                @LiteralParameter("p") long precision,
                @LiteralParameter("s") long scale,
                @SqlType("decimal(p, s)") Int128 value)
        {
            return JsonItemBuilder.encodeLongDecimal((int) precision, (int) scale, value);
        }
    }

    @ScalarFunction("json_scalar")
    @Description("Constructs a JSON scalar from a time-with-time-zone value")
    public static final class FromTimeWithTimeZone
    {
        private FromTimeWithTimeZone() {}

        @LiteralParameters("p")
        @SqlType(StandardTypes.JSON)
        public static Json fromShort(
                @LiteralParameter("p") long precision,
                @SqlType("time(p) with time zone") long packedTime)
        {
            return JsonItemBuilder.encodeTimeWithTimeZone(
                    toIntExact(precision),
                    unpackTimeNanos(packedTime) * PICOSECONDS_PER_NANOSECOND,
                    unpackOffsetMinutes(packedTime));
        }

        @LiteralParameters("p")
        @SqlType(StandardTypes.JSON)
        public static Json fromLong(
                @LiteralParameter("p") long precision,
                @SqlType("time(p) with time zone") LongTimeWithTimeZone time)
        {
            return JsonItemBuilder.encodeTimeWithTimeZone(toIntExact(precision), time.getPicoseconds(), time.getOffsetMinutes());
        }
    }

    @ScalarFunction("json_scalar")
    @Description("Constructs a JSON scalar from a timestamp value")
    public static final class FromTimestamp
    {
        private FromTimestamp() {}

        @LiteralParameters("p")
        @SqlType(StandardTypes.JSON)
        public static Json fromShort(
                @LiteralParameter("p") long precision,
                @SqlType("timestamp(p)") long epochMicros)
        {
            return JsonItemBuilder.encodeTimestamp(toIntExact(precision), epochMicros, 0);
        }

        @LiteralParameters("p")
        @SqlType(StandardTypes.JSON)
        public static Json fromLong(
                @LiteralParameter("p") long precision,
                @SqlType("timestamp(p)") LongTimestamp value)
        {
            return JsonItemBuilder.encodeTimestamp(toIntExact(precision), value.getEpochMicros(), value.getPicosOfMicro());
        }
    }

    @ScalarFunction("json_scalar")
    @Description("Constructs a JSON scalar from a timestamp-with-time-zone value")
    public static final class FromTimestampWithTimeZone
    {
        private FromTimestampWithTimeZone() {}

        @LiteralParameters("p")
        @SqlType(StandardTypes.JSON)
        public static Json fromShort(
                @LiteralParameter("p") long precision,
                @SqlType("timestamp(p) with time zone") long packedEpochMillis)
        {
            return JsonItemBuilder.encodeTimestampWithTimeZone(
                    toIntExact(precision),
                    unpackMillisUtc(packedEpochMillis),
                    0,
                    unpackZoneKey(packedEpochMillis).getKey());
        }

        @LiteralParameters("p")
        @SqlType(StandardTypes.JSON)
        public static Json fromLong(
                @LiteralParameter("p") long precision,
                @SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestamp)
        {
            return JsonItemBuilder.encodeTimestampWithTimeZone(
                    toIntExact(precision),
                    timestamp.getEpochMillis(),
                    timestamp.getPicosOfMilli(),
                    timestamp.getTimeZoneKey());
        }
    }
}
