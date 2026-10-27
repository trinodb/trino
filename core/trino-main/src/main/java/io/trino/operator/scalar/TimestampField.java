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
import io.trino.spi.TrinoException;

import java.time.DateTimeException;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.TemporalAdjusters;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.util.DateTimeZoneIndex.convertLocalToUtc;
import static io.trino.util.DateTimeZoneIndex.convertUtcToLocal;
import static io.trino.util.DateTimeZoneIndex.getOffsetFromLocalMillis;
import static io.trino.util.DateTimeZoneIndex.getOffsetMillis;
import static java.lang.Math.addExact;
import static java.lang.Math.floorDiv;
import static java.lang.Math.multiplyExact;
import static java.lang.Math.subtractExact;
import static java.time.ZoneOffset.UTC;

public enum TimestampField
{
    MILLISECOND("millisecond", 1),
    SECOND("second", 1_000),
    MINUTE("minute", 60_000),
    HOUR("hour", 3_600_000),
    DAY("day", 86_400_000),
    WEEK("week", 604_800_000),
    MONTH("month", 0),
    QUARTER("quarter", 0),
    YEAR("year", 0);

    private static final TimestampField[] FIELDS = values();

    // Bounds of the legacy millisecond calendar. Keep its error categories for
    // oversized calendar additions before scaling results to timestamp precision.
    private static final int MIN_CALENDAR_YEAR = -292_275_054;
    private static final int MAX_CALENDAR_YEAR = 292_278_993;

    private final String name;
    private final long unitMillis;

    TimestampField(String name, long unitMillis)
    {
        this.name = name;
        this.unitMillis = unitMillis;
    }

    public static TimestampField forDate(Slice unit)
    {
        for (TimestampField field : FIELDS) {
            if (field.ordinal() >= DAY.ordinal() && field.matches(unit)) {
                return field;
            }
        }
        throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "'" + unit.toStringUtf8() + "' is not a valid DATE field");
    }

    public static TimestampField forTimestamp(Slice unit)
    {
        for (TimestampField field : FIELDS) {
            if (field.matches(unit)) {
                return field;
            }
        }
        throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "'" + unit.toStringUtf8() + "' is not a valid TIMESTAMP field");
    }

    private boolean matches(Slice unit)
    {
        if (unit.length() != name.length()) {
            return false;
        }
        for (int index = 0; index < name.length(); index++) {
            if ((unit.getByte(index) | 0x20) != name.charAt(index)) {
                return false;
            }
        }
        return true;
    }

    public long add(long epochMillis, long value, ZoneId zoneId)
    {
        try {
            long localMillis = convertUtcToLocal(epochMillis, zoneId);
            if (ordinal() <= HOUR.ordinal()) {
                return subtractExact(addExact(localMillis, multiplyExact(value, unitMillis)), getOffsetMillis(epochMillis, zoneId));
            }
            if (this == DAY || this == WEEK) {
                localMillis = addExact(localMillis, multiplyExact(value, unitMillis));
            }
            else {
                LocalDateTime local = localDateTime(localMillis);
                local = switch (this) {
                    case MONTH -> addMonths(local, value);
                    case QUARTER -> addMonths(local, multiplyExact(value, 3));
                    case YEAR -> addYears(local, value);
                    default -> throw new IllegalStateException("Unexpected calendar field: " + this);
                };
                localMillis = local.toInstant(UTC).toEpochMilli();
            }
            if (this == QUARTER) {
                // Quarter duration arithmetic historically resolves overlaps independently
                // of the original instant, unlike month and year field arithmetic.
                return subtractExact(localMillis, getOffsetFromLocalMillis(localMillis, zoneId));
            }
            return convertLocalToUtc(localMillis, zoneId, epochMillis);
        }
        catch (DateTimeException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    private static LocalDateTime addMonths(LocalDateTime local, long months)
    {
        if (months != (int) months) {
            long year = local.getYear() + floorDiv(months, 12) + (local.getMonthValue() - 1 + Math.floorMod(months, 12)) / 12;
            if (year < MIN_CALENDAR_YEAR || year > MAX_CALENDAR_YEAR) {
                throw new IllegalArgumentException("Magnitude of add amount is too large: " + months);
            }
        }
        return local.plusMonths(months);
    }

    private static LocalDateTime addYears(LocalDateTime local, long years)
    {
        if (years == 0) {
            return local;
        }
        if (years != (int) years) {
            throw new ArithmeticException("Value cannot fit in an int: " + years);
        }
        int targetYear = addExact(local.getYear(), (int) years);
        if (targetYear < MIN_CALENDAR_YEAR || targetYear > MAX_CALENDAR_YEAR) {
            throw new IllegalArgumentException("Value " + targetYear + " for year must be in the range [" + MIN_CALENDAR_YEAR + ',' + MAX_CALENDAR_YEAR + ']');
        }
        return local.plusYears(years);
    }

    public long difference(long endMillis, long startMillis, ZoneId zoneId)
    {
        if (ordinal() <= HOUR.ordinal()) {
            return subtractExact(endMillis, startMillis) / unitMillis;
        }
        long localEnd = convertUtcToLocal(endMillis, zoneId);
        long localStart = convertUtcToLocal(startMillis, zoneId);
        if (this == DAY || this == WEEK) {
            return subtractExact(localEnd, localStart) / unitMillis;
        }
        if (localEnd < localStart) {
            return -calendarDifference(localDateTime(localStart), localDateTime(localEnd));
        }
        return calendarDifference(localDateTime(localEnd), localDateTime(localStart));
    }

    private long calendarDifference(LocalDateTime end, LocalDateTime start)
    {
        if (this == YEAR) {
            long years = (long) end.getYear() - start.getYear();
            if (start.plusYears(years).isAfter(end)) {
                years--;
            }
            return years;
        }
        long months = ((long) end.getYear() - start.getYear()) * 12 + end.getMonthValue() - start.getMonthValue();
        // Count a clipped month-end anniversary as a complete month (January 31 to February 28).
        if (start.plusMonths(months).isAfter(end)) {
            months--;
        }
        if (this == QUARTER) {
            return months / 3;
        }
        return months;
    }

    public long truncate(long epochMillis, ZoneId zoneId)
    {
        long localMillis = convertUtcToLocal(epochMillis, zoneId);
        if (ordinal() <= HOUR.ordinal()) {
            long rounded = multiplyExact(floorDiv(localMillis, unitMillis), unitMillis);
            return subtractExact(rounded, getOffsetMillis(epochMillis, zoneId));
        }
        LocalDateTime local = localDateTime(localMillis);
        local = switch (this) {
            case DAY -> local.toLocalDate().atStartOfDay();
            case WEEK -> local.toLocalDate().with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY)).atStartOfDay();
            case MONTH -> local.toLocalDate().withDayOfMonth(1).atStartOfDay();
            case QUARTER -> local.toLocalDate().withDayOfMonth(1).withMonth((local.getMonthValue() - 1) / 3 * 3 + 1).atStartOfDay();
            case YEAR -> local.toLocalDate().withDayOfYear(1).atStartOfDay();
            default -> throw new IllegalStateException("Unexpected calendar field: " + this);
        };
        return convertLocalToUtc(local.toInstant(UTC).toEpochMilli(), zoneId, epochMillis);
    }

    private static LocalDateTime localDateTime(long epochMillis)
    {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), UTC);
    }
}
