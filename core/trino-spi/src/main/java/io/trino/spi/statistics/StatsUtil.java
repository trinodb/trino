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
package io.trino.spi.statistics;

import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;

import java.util.OptionalDouble;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalConversions.longDecimalToDouble;
import static io.trino.spi.type.DecimalConversions.shortDecimalToDouble;
import static io.trino.spi.type.Decimals.longTenToNth;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public final class StatsUtil
{
    private StatsUtil() {}

    public static OptionalDouble toStatsRepresentation(Type type, Object value)
    {
        requireNonNull(type, "type is null");
        requireNonNull(value, "value is null");

        if (type == BOOLEAN) {
            return OptionalDouble.of((boolean) value ? 1 : 0);
        }
        if (type == TINYINT || type == SMALLINT || type == INTEGER || type == BIGINT) {
            return OptionalDouble.of((long) value);
        }
        if (type == REAL) {
            return OptionalDouble.of(intBitsToFloat(toIntExact((Long) value)));
        }
        if (type == DOUBLE) {
            return OptionalDouble.of((double) value);
        }
        if (type instanceof DecimalType decimalType) {
            if (decimalType.isShort()) {
                return OptionalDouble.of(shortDecimalToDouble((long) value, longTenToNth(decimalType.getScale())));
            }
            return OptionalDouble.of(longDecimalToDouble((Int128) value, decimalType.getScale()));
        }
        if (type == DATE) {
            return OptionalDouble.of((long) value);
        }
        if (type instanceof TimestampType timestampType) {
            if (timestampType.isShort()) {
                return OptionalDouble.of((long) value);
            }
            return OptionalDouble.of(((LongTimestamp) value).getEpochMicros());
        }
        if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            if (timestampWithTimeZoneType.isShort()) {
                return OptionalDouble.of(unpackMillisUtc((long) value));
            }
            return OptionalDouble.of(((LongTimestampWithTimeZone) value).getEpochMillis());
        }

        return OptionalDouble.empty();
    }
}
