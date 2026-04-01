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
package io.trino.plugin.ducklake;

import io.airlift.slice.Slices;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.RealType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.math.BigDecimal;
import java.time.LocalDate;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;

/**
 * Converts JDBC values from SQLite inlined data tables to Trino-native values
 * compatible with InMemoryRecordSet.
 */
public final class DucklakeInlinedValueConverter
{
    private DucklakeInlinedValueConverter() {}

    /**
     * Convert a JDBC value to the representation expected by InMemoryRecordSet for the given Trino type.
     * Returns null for null input.
     */
    public static Object convertJdbcValue(Object jdbcValue, Type trinoType)
    {
        if (jdbcValue == null) {
            return null;
        }

        if (trinoType.equals(BOOLEAN)) {
            return toBoolean(jdbcValue);
        }
        if (trinoType.equals(TINYINT) || trinoType.equals(SMALLINT) || trinoType.equals(INTEGER) || trinoType.equals(BIGINT)) {
            return toLong(jdbcValue);
        }
        if (trinoType instanceof RealType) {
            return (long) Float.floatToIntBits(toFloat(jdbcValue));
        }
        if (trinoType.equals(DOUBLE)) {
            return toDouble(jdbcValue);
        }
        if (trinoType instanceof DateType) {
            return toEpochDays(jdbcValue);
        }
        if (trinoType instanceof TimestampType || trinoType instanceof TimestampWithTimeZoneType) {
            return toMicros(jdbcValue);
        }
        if (trinoType instanceof VarcharType) {
            return Slices.utf8Slice(jdbcValue.toString());
        }
        if (trinoType instanceof VarbinaryType) {
            if (jdbcValue instanceof byte[] bytes) {
                return Slices.wrappedBuffer(bytes);
            }
            return Slices.wrappedBuffer(jdbcValue.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
        if (trinoType instanceof DecimalType decimalType) {
            return toDecimal(jdbcValue, decimalType);
        }

        // Fallback: try as string for any remaining types
        return Slices.utf8Slice(jdbcValue.toString());
    }

    private static Boolean toBoolean(Object value)
    {
        if (value instanceof Boolean b) {
            return b;
        }
        if (value instanceof Number n) {
            return n.intValue() != 0;
        }
        return Boolean.parseBoolean(value.toString());
    }

    private static long toLong(Object value)
    {
        if (value instanceof Number n) {
            return n.longValue();
        }
        return Long.parseLong(value.toString());
    }

    private static float toFloat(Object value)
    {
        if (value instanceof Number n) {
            return n.floatValue();
        }
        return Float.parseFloat(value.toString());
    }

    private static double toDouble(Object value)
    {
        if (value instanceof Number n) {
            return n.doubleValue();
        }
        return Double.parseDouble(value.toString());
    }

    private static long toEpochDays(Object value)
    {
        if (value instanceof Number n) {
            return n.longValue();
        }
        // SQLite may store dates as ISO strings
        return LocalDate.parse(value.toString()).toEpochDay();
    }

    private static long toMicros(Object value)
    {
        if (value instanceof Number n) {
            return n.longValue();
        }
        // Parse ISO timestamp string to micros since epoch
        String str = value.toString();
        java.time.LocalDateTime ldt = java.time.LocalDateTime.parse(str.replace(" ", "T"));
        return ldt.toEpochSecond(java.time.ZoneOffset.UTC) * 1_000_000 + ldt.getNano() / 1_000;
    }

    private static Object toDecimal(Object value, DecimalType decimalType)
    {
        BigDecimal decimal;
        if (value instanceof BigDecimal bd) {
            decimal = bd;
        }
        else {
            decimal = new BigDecimal(value.toString());
        }
        decimal = decimal.setScale(decimalType.getScale(), java.math.RoundingMode.HALF_UP);

        if (decimalType.isShort()) {
            return decimal.unscaledValue().longValueExact();
        }
        return Int128.valueOf(decimal.unscaledValue());
    }
}
