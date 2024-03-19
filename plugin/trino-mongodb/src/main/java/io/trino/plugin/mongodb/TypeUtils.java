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
package io.trino.plugin.mongodb;

import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Primitives;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.Slice;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.Chars.padSpaces;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.roundDiv;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.toIntExact;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;

public final class TypeUtils
{
    private static final Set<Type> PUSHDOWN_SUPPORTED_PRIMITIVE_TYPES = ImmutableSet.of(
            BOOLEAN,
            TINYINT,
            SMALLINT,
            INTEGER,
            BIGINT,
            REAL,
            DOUBLE,
            DATE,
            TIME_MILLIS,
            TIMESTAMP_MILLIS,
            TIMESTAMP_TZ_MILLIS);

    private TypeUtils() {}

    public static boolean isJsonType(Type type)
    {
        return type.getBaseName().equals(JSON);
    }

    public static boolean isPushdownSupportedType(Type type)
    {
        return type instanceof CharType
                || type instanceof VarcharType
                || type instanceof DecimalType
                || type instanceof ObjectIdType
                || PUSHDOWN_SUPPORTED_PRIMITIVE_TYPES.contains(type);
    }

    public static Optional<Object> translateValue(Object trinoNativeValue, Type type)
    {
        requireNonNull(trinoNativeValue, "trinoNativeValue is null");
        requireNonNull(type, "type is null");
        checkArgument(Primitives.wrap(type.getJavaType()).isInstance(trinoNativeValue), "%s (%s) is not a valid representation for %s", trinoNativeValue, trinoNativeValue.getClass(), type);

        if (type == BOOLEAN) {
            return Optional.of(trinoNativeValue);
        }

        if (type == TINYINT) {
            return Optional.of((long) SignedBytes.checkedCast(((Long) trinoNativeValue)));
        }

        if (type == SMALLINT) {
            return Optional.of((long) Shorts.checkedCast(((Long) trinoNativeValue)));
        }

        if (type == INTEGER) {
            return Optional.of((long) toIntExact(((Long) trinoNativeValue)));
        }

        if (type == BIGINT) {
            return Optional.of(trinoNativeValue);
        }

        if (type == REAL) {
            return Optional.of(intBitsToFloat(toIntExact((long) trinoNativeValue)));
        }

        if (type == DOUBLE) {
            return Optional.of(trinoNativeValue);
        }

        if (type instanceof DecimalType decimalType) {
            if (decimalType.isShort()) {
                return Optional.of(Decimal128.parse(Decimals.toString((long) trinoNativeValue, decimalType.getScale())));
            }
            return Optional.of(Decimal128.parse(Decimals.toString((Int128) trinoNativeValue, decimalType.getScale())));
        }

        if (type instanceof ObjectIdType) {
            return Optional.of(new ObjectId(((Slice) trinoNativeValue).getBytes()));
        }

        if (type instanceof CharType charType) {
            Slice slice = padSpaces(((Slice) trinoNativeValue), charType);
            return Optional.of(slice.toStringUtf8());
        }

        if (type instanceof VarcharType) {
            return Optional.of(((Slice) trinoNativeValue).toStringUtf8());
        }

        if (type == DATE) {
            long days = (long) trinoNativeValue;
            return Optional.of(LocalDate.ofEpochDay(days));
        }

        if (type == TIME_MILLIS) {
            long picos = (long) trinoNativeValue;
            return Optional.of(LocalTime.ofNanoOfDay(roundDiv(picos, PICOSECONDS_PER_NANOSECOND)));
        }

        if (type == TIMESTAMP_MILLIS) {
            long epochMicros = (long) trinoNativeValue;
            long epochSecond = floorDiv(epochMicros, MICROSECONDS_PER_SECOND);
            int nanoFraction = floorMod(epochMicros, MICROSECONDS_PER_SECOND) * NANOSECONDS_PER_MICROSECOND;
            Instant instant = Instant.ofEpochSecond(epochSecond, nanoFraction);
            return Optional.of(LocalDateTime.ofInstant(instant, UTC));
        }

        if (type == TIMESTAMP_TZ_MILLIS) {
            long millisUtc = unpackMillisUtc((long) trinoNativeValue);
            Instant instant = Instant.ofEpochMilli(millisUtc);
            return Optional.of(LocalDateTime.ofInstant(instant, UTC));
        }

        return Optional.empty();
    }
}
