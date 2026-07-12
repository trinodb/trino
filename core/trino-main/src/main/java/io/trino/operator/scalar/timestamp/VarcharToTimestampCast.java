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
package io.trino.operator.scalar.timestamp;

import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestamp;
import io.trino.type.DateTimes;

import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.regex.Matcher;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.operator.scalar.StringFunctions.trim;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.type.TimestampType.MAX_PRECISION;
import static io.trino.spi.type.TimestampType.MAX_SHORT_PRECISION;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.round;
import static io.trino.type.DateTimes.longTimestamp;
import static io.trino.type.DateTimes.rescale;
import static java.lang.Math.min;
import static java.time.ZoneOffset.UTC;

// fallible
@ScalarOperator(CAST)
public final class VarcharToTimestampCast
{
    private VarcharToTimestampCast() {}

    @LiteralParameters({"x", "p"})
    @SqlType("timestamp(p)")
    public static long castToShort(@LiteralParameter("p") long precision, @SqlType("varchar(x)") Slice value)
    {
        try {
            return castToShortTimestamp((int) precision, trim(value));
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp: " + value.toStringUtf8(), e);
        }
    }

    @LiteralParameters({"x", "p"})
    @SqlType("timestamp(p)")
    public static LongTimestamp castToLong(@LiteralParameter("p") long precision, @SqlType("varchar(x)") Slice value)
    {
        try {
            return castToLongTimestamp((int) precision, trim(value));
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp: " + value.toStringUtf8(), e);
        }
    }

    public static long castToShortTimestamp(int precision, Slice value)
    {
        checkArgument(precision <= MAX_SHORT_PRECISION, "precision must be less than max short timestamp precision");

        PlainTimestamp parsed = parsePlainTimestamp(value);
        if (parsed == null) {
            return castToShortTimestamp(precision, value.toStringUtf8());
        }

        long fractionValue = parsed.fractionValue();
        int actualPrecision = parsed.fractionPrecision();
        if (actualPrecision > precision) {
            fractionValue = round(fractionValue, actualPrecision - precision);
        }

        // scale to micros
        return parsed.epochSecond() * MICROSECONDS_PER_SECOND + rescale(fractionValue, actualPrecision, 6);
    }

    public static LongTimestamp castToLongTimestamp(int precision, Slice value)
    {
        checkArgument(precision > MAX_SHORT_PRECISION && precision <= MAX_PRECISION, "precision out of range");

        PlainTimestamp parsed = parsePlainTimestamp(value);
        if (parsed == null) {
            return castToLongTimestamp(precision, value.toStringUtf8());
        }

        long fractionValue = parsed.fractionValue();
        int actualPrecision = parsed.fractionPrecision();
        if (actualPrecision > precision) {
            fractionValue = round(fractionValue, actualPrecision - precision);
        }

        return longTimestamp(parsed.epochSecond(), rescale(fractionValue, actualPrecision, 12));
    }

    /**
     * The fields of a timestamp with no time zone, read straight from the bytes. It does not escape
     * the cast, so it costs nothing next to the String, the Matcher, and the group substrings that
     * the general path allocates for every row.
     */
    private record PlainTimestamp(long epochSecond, long fractionValue, int fractionPrecision) {}

    /**
     * Parses {@code yyyy-MM-dd[ HH:mm[:ss[.fraction]]]}, the shape a timestamp almost always has,
     * straight from the bytes.
     * <p>
     * Returns null for everything else, including a signed year, a time zone, and any spacing the
     * shape above does not cover, so that the caller falls back to {@link DateTimes#DATETIME_PATTERN},
     * which accepts all of it.
     */
    private static PlainTimestamp parsePlainTimestamp(Slice value)
    {
        int length = value.length();
        int index = 0;

        int yearEnd = digitsEnd(value, index, length);
        int yearDigits = yearEnd - index;
        // the pattern accepts any number of year digits, but more than nine cannot be a year
        if (yearDigits < 4 || yearDigits > 9) {
            return null;
        }
        int year = parseDigits(value, index, yearEnd);
        index = yearEnd;

        if (index == length || value.getByte(index) != '-') {
            return null;
        }
        index++;

        int monthEnd = digitsEnd(value, index, min(index + 2, length));
        if (monthEnd == index) {
            return null;
        }
        int month = parseDigits(value, index, monthEnd);
        index = monthEnd;

        if (index == length || value.getByte(index) != '-') {
            return null;
        }
        index++;

        int dayEnd = digitsEnd(value, index, min(index + 2, length));
        if (dayEnd == index) {
            return null;
        }
        int day = parseDigits(value, index, dayEnd);
        index = dayEnd;

        int hour = 0;
        int minute = 0;
        int second = 0;
        long fractionValue = 0;
        int fractionPrecision = 0;

        if (index != length) {
            if (value.getByte(index) != ' ') {
                return null;
            }
            index++;

            int hourEnd = digitsEnd(value, index, min(index + 2, length));
            if (hourEnd == index) {
                return null;
            }
            hour = parseDigits(value, index, hourEnd);
            index = hourEnd;

            if (index == length || value.getByte(index) != ':') {
                return null;
            }
            index++;

            int minuteEnd = digitsEnd(value, index, min(index + 2, length));
            if (minuteEnd == index) {
                return null;
            }
            minute = parseDigits(value, index, minuteEnd);
            index = minuteEnd;

            if (index != length) {
                if (value.getByte(index) != ':') {
                    return null;
                }
                index++;

                int secondEnd = digitsEnd(value, index, min(index + 2, length));
                if (secondEnd == index) {
                    return null;
                }
                second = parseDigits(value, index, secondEnd);
                index = secondEnd;

                if (index != length) {
                    if (value.getByte(index) != '.') {
                        return null;
                    }
                    index++;

                    int fractionEnd = digitsEnd(value, index, length);
                    // anything left over is a time zone, which the general path handles
                    if (fractionEnd == index || fractionEnd != length) {
                        return null;
                    }
                    fractionPrecision = fractionEnd - index;
                    // the general path parses the fraction with Long.parseLong, which overflows past this
                    if (fractionPrecision > 18) {
                        return null;
                    }
                    fractionValue = parseLongDigits(value, index, fractionEnd);
                }
            }
        }

        try {
            // with no time zone the general path resolves against UTC, which never shifts the value
            return new PlainTimestamp(
                    LocalDateTime.of(year, month, day, hour, minute, second).toEpochSecond(UTC),
                    fractionValue,
                    fractionPrecision);
        }
        catch (DateTimeException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp: " + value.toStringUtf8(), e);
        }
    }

    private static int digitsEnd(Slice value, int start, int end)
    {
        int index = start;
        while (index < end && isDigit(value.getByte(index))) {
            index++;
        }
        return index;
    }

    private static boolean isDigit(byte value)
    {
        return value >= '0' && value <= '9';
    }

    private static int parseDigits(Slice value, int start, int end)
    {
        int result = 0;
        for (int index = start; index < end; index++) {
            result = (result * 10) + (value.getByte(index) - '0');
        }
        return result;
    }

    private static long parseLongDigits(Slice value, int start, int end)
    {
        long result = 0;
        for (int index = start; index < end; index++) {
            result = (result * 10) + (value.getByte(index) - '0');
        }
        return result;
    }

    public static long castToShortTimestamp(int precision, String value)
    {
        checkArgument(precision <= MAX_SHORT_PRECISION, "precision must be less than max short timestamp precision");

        Matcher matcher = DateTimes.DATETIME_PATTERN.matcher(value);
        if (!matcher.matches()) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp: " + value);
        }

        String year = matcher.group("year");
        String month = matcher.group("month");
        String day = matcher.group("day");
        String hour = matcher.group("hour");
        String minute = matcher.group("minute");
        String second = matcher.group("second");
        String fraction = matcher.group("fraction");
        String timezone = matcher.group("timezone");

        ZoneId zone = UTC;
        long epochSecond;
        try {
            if (timezone != null) {
                zone = ZoneId.of(timezone);
            }
            epochSecond = ZonedDateTime.of(
                            Integer.parseInt(year),
                            Integer.parseInt(month),
                            Integer.parseInt(day),
                            hour == null ? 0 : Integer.parseInt(hour),
                            minute == null ? 0 : Integer.parseInt(minute),
                            second == null ? 0 : Integer.parseInt(second),
                            0,
                            zone)
                    .toLocalDateTime()
                    .toEpochSecond(UTC);
        }
        catch (DateTimeException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp: " + value, e);
        }

        int actualPrecision = 0;
        long fractionValue = 0;
        if (fraction != null) {
            actualPrecision = fraction.length();
            fractionValue = Long.parseLong(fraction);
        }

        if (actualPrecision > precision) {
            fractionValue = round(fractionValue, actualPrecision - precision);
        }

        // scale to micros
        return epochSecond * MICROSECONDS_PER_SECOND + rescale(fractionValue, actualPrecision, 6);
    }

    public static LongTimestamp castToLongTimestamp(int precision, String value)
    {
        checkArgument(precision > MAX_SHORT_PRECISION && precision <= MAX_PRECISION, "precision out of range");

        Matcher matcher = DateTimes.DATETIME_PATTERN.matcher(value);
        if (!matcher.matches()) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp: " + value);
        }

        String year = matcher.group("year");
        String month = matcher.group("month");
        String day = matcher.group("day");
        String hour = matcher.group("hour");
        String minute = matcher.group("minute");
        String second = matcher.group("second");
        String fraction = matcher.group("fraction");
        String timezone = matcher.group("timezone");

        ZoneId zone = UTC;
        long epochSecond;
        try {
            if (timezone != null) {
                zone = ZoneId.of(timezone);
            }
            epochSecond = ZonedDateTime.of(
                            Integer.parseInt(year),
                            Integer.parseInt(month),
                            Integer.parseInt(day),
                            hour == null ? 0 : Integer.parseInt(hour),
                            minute == null ? 0 : Integer.parseInt(minute),
                            second == null ? 0 : Integer.parseInt(second),
                            0,
                            zone)
                    .toLocalDateTime()
                    .toEpochSecond(UTC);
        }
        catch (DateTimeException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp: " + value, e);
        }

        int actualPrecision = 0;
        long fractionValue = 0;
        if (fraction != null) {
            actualPrecision = fraction.length();
            fractionValue = Long.parseLong(fraction);
        }

        if (actualPrecision > precision) {
            fractionValue = round(fractionValue, actualPrecision - precision);
        }

        return longTimestamp(epochSecond, rescale(fractionValue, actualPrecision, 12));
    }
}
