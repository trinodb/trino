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
package io.trino.plugin.faker;

import io.trino.spi.TrinoException;
import io.trino.spi.predicate.Range;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeParameter;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

import static io.trino.spi.StandardErrorCode.INVALID_COLUMN_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.StandardTypes.INTERVAL_DAY_TO_SECOND;
import static io.trino.spi.type.StandardTypes.INTERVAL_YEAR_TO_MONTH;
import static java.lang.Math.toIntExact;

final class IntervalValues
{
    private IntervalValues() {}

    static boolean isInterval(Type type)
    {
        return type.getBaseName().equals(INTERVAL_DAY_TO_SECOND) || type.getBaseName().equals(INTERVAL_YEAR_TO_MONTH);
    }

    static void checkSupported(Type type)
    {
        if (isInterval(type) && type.getJavaType() != long.class) {
            throw new TrinoException(NOT_SUPPORTED, "Faker does not support day-time intervals with fractional precision greater than 6: " + type.getDisplayName());
        }
    }

    static long parse(String value, Type type)
    {
        checkSupported(type);
        long quantum = quantum(type);
        BigDecimal amount = new BigDecimal(value);
        if (type.getBaseName().equals(INTERVAL_DAY_TO_SECOND)) {
            amount = amount.movePointRight(6);
        }
        RoundingMode rounding = field(type, 1) == 5
                ? (amount.signum() < 0 ? RoundingMode.HALF_DOWN : RoundingMode.HALF_UP)
                : RoundingMode.DOWN;
        BigInteger units = amount.divide(BigDecimal.valueOf(quantum), 0, rounding).toBigIntegerExact();
        BigInteger result = units.multiply(BigInteger.valueOf(quantum));
        if (result.abs().compareTo(BigInteger.valueOf(maxValue(type))) > 0) {
            throw new TrinoException(INVALID_COLUMN_PROPERTY, "Interval value exceeds " + type.getDisplayName() + ": " + value);
        }
        return result.longValueExact();
    }

    static long quantum(Type type)
    {
        checkSupported(type);
        int end = field(type, 1);
        if (type.getBaseName().equals(INTERVAL_YEAR_TO_MONTH)) {
            return end == 0 ? 12 : 1;
        }
        return end == 5 ? BigInteger.TEN.pow(6 - field(type, 3)).longValueExact() : fieldUnit(end);
    }

    static Bounds bounds(Type type, Range range)
    {
        long max = maxValue(type);
        Range bounded = range.intersect(Range.range(type, -max, true, max, true))
                .orElseThrow(() -> new IllegalArgumentException("Range contains no values of " + type.getDisplayName()));
        long quantum = quantum(type);
        long low = (long) bounded.getLowBoundedValue();
        long high = (long) bounded.getHighBoundedValue();
        long lowUnits = Math.ceilDiv(low, quantum) + (!bounded.isLowInclusive() && low % quantum == 0 ? 1 : 0);
        long highUnits = Math.floorDiv(high, quantum) - (!bounded.isHighInclusive() && high % quantum == 0 ? 1 : 0);
        return new Bounds(lowUnits, highUnits, quantum);
    }

    record Bounds(long low, long high, long quantum)
    {
        long at(long index, long step)
        {
            return BigInteger.valueOf(low).add(BigInteger.valueOf(step / quantum).multiply(BigInteger.valueOf(index)))
                    .min(BigInteger.valueOf(high)).multiply(BigInteger.valueOf(quantum)).longValueExact();
        }
    }

    private static long maxValue(Type type)
    {
        long fieldUnit = type.getBaseName().equals(INTERVAL_YEAR_TO_MONTH) ? (field(type, 0) == 0 ? 12 : 1) : fieldUnit(field(type, 0));
        long storageMax = type.getBaseName().equals(INTERVAL_YEAR_TO_MONTH) ? Integer.MAX_VALUE : Long.MAX_VALUE;
        long quantum = quantum(type);
        BigInteger limit = BigInteger.TEN.pow(field(type, 2)).multiply(BigInteger.valueOf(fieldUnit)).subtract(BigInteger.ONE)
                .min(BigInteger.valueOf(storageMax));
        return limit.divide(BigInteger.valueOf(quantum)).multiply(BigInteger.valueOf(quantum)).longValueExact();
    }

    private static int field(Type type, int index)
    {
        return toIntExact(((TypeParameter.Numeric) type.getTypeDescriptor().getParameters().get(index)).value());
    }

    private static long fieldUnit(int field)
    {
        return switch (field) {
            case 2 -> 86_400_000_000L;
            case 3 -> 3_600_000_000L;
            case 4 -> 60_000_000L;
            case 5 -> 1_000_000L;
            default -> throw new IllegalArgumentException("Not a day-time field: " + field);
        };
    }
}
