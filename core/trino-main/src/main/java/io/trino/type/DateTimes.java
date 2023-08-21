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

import io.trino.spi.block.Block;
import io.trino.spi.type.DateTimeEncoding;
import io.trino.spi.type.LongTimeWithTimeZone;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.DateTimeEncoding.packTimeWithTimeZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimestampType.MAX_SHORT_PRECISION;
import static java.lang.Math.floorMod;
import static java.lang.Math.multiplyExact;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoField.MICRO_OF_SECOND;

public final class DateTimes
{
    public static final Pattern DATETIME_PATTERN = Pattern.compile("" +
            "(?<year>[-+]?\\d{4,})-(?<month>\\d{1,2})-(?<day>\\d{1,2})" +
            "( (?:(?<hour>\\d{1,2}):(?<minute>\\d{1,2})(?::(?<second>\\d{1,2})(?:\\.(?<fraction>\\d+))?)?)?" +
            "\\s*(?<timezone>.+)?)?");
    private static final String TIMESTAMP_FORMATTER_PATTERN = "uuuu-MM-dd HH:mm:ss";
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern(TIMESTAMP_FORMATTER_PATTERN);

    public static final Pattern TIME_PATTERN = Pattern.compile("" +
            "(?<hour>\\d{1,2}):(?<minute>\\d{1,2})(?::(?<second>\\d{1,2})(?:\\.(?<fraction>\\d+))?)?" +
            "\\s*((?<sign>[+-])(?<offsetHour>\\d\\d):(?<offsetMinute>\\d\\d))?");

    private static final long[] POWERS_OF_TEN = {
            1L,
            10L,
            100L,
            1000L,
            10_000L,
            100_000L,
            1_000_000L,
            10_000_000L,
            100_000_000L,
            1_000_000_000L,
            10_000_000_000L,
            100_000_000_000L,
            1000_000_000_000L
    };

    public static final int MILLISECONDS_PER_SECOND = 1000;
    public static final long MILLISECONDS_PER_MINUTE = 60 * MILLISECONDS_PER_SECOND;
    public static final long MILLISECONDS_PER_DAY = 24 * 60 * 60 * MILLISECONDS_PER_SECOND;
    public static final int MICROSECONDS_PER_SECOND = 1_000_000;
    public static final int MICROSECONDS_PER_MILLISECOND = 1000;
    public static final long MICROSECONDS_PER_DAY = MILLISECONDS_PER_DAY * MICROSECONDS_PER_MILLISECOND;
    public static final long PICOSECONDS_PER_SECOND = 1_000_000_000_000L;
    public static final long NANOSECONDS_PER_SECOND = 1_000_000_000;
    public static final long NANOSECONDS_PER_MINUTE = NANOSECONDS_PER_SECOND * 60;
    public static final long NANOSECONDS_PER_HOUR = NANOSECONDS_PER_MINUTE * 60;
    public static final long NANOSECONDS_PER_DAY = NANOSECONDS_PER_HOUR * 24;
    public static final int NANOSECONDS_PER_MILLISECOND = 1_000_000;
    public static final int NANOSECONDS_PER_MICROSECOND = 1_000;
    public static final int PICOSECONDS_PER_MILLISECOND = 1_000_000_000;
    public static final int PICOSECONDS_PER_MICROSECOND = 1_000_000;
    public static final int PICOSECONDS_PER_NANOSECOND = 1000;
    public static final long SECONDS_PER_MINUTE = 60;
    public static final long MINUTES_PER_HOUR = 60;
    public static final long HOURS_PER_DAY = 24;
    public static final long PICOSECONDS_PER_MINUTE = PICOSECONDS_PER_SECOND * SECONDS_PER_MINUTE;
    public static final long PICOSECONDS_PER_HOUR = PICOSECONDS_PER_MINUTE * MINUTES_PER_HOUR;
    public static final long PICOSECONDS_PER_DAY = PICOSECONDS_PER_HOUR * HOURS_PER_DAY;
    public static final long SECONDS_PER_DAY = SECONDS_PER_MINUTE * MINUTES_PER_HOUR * HOURS_PER_DAY;

    private DateTimes() {}

    private static long roundDiv(long value, long factor)
    {
        checkArgument(factor > 0, "factor must be positive");

        if (factor == 1) {
            return value;
        }

        if (value >= 0) {
            return (value + (factor / 2)) / factor;
        }

        return (value + 1 - (factor / 2)) / factor;
    }

    public static long scaleEpochMicrosToMillis(long value)
    {
        return Math.floorDiv(value, MICROSECONDS_PER_MILLISECOND);
    }

    public static long epochMicrosToMillisWithRounding(long epochMicros)
    {
        return roundDiv(epochMicros, MICROSECONDS_PER_MILLISECOND);
    }

    public static long scaleEpochMillisToSeconds(long epochMillis)
    {
        return Math.floorDiv(epochMillis, MILLISECONDS_PER_SECOND);
    }

    public static long scaleEpochMicrosToSeconds(long epochMicros)
    {
        return Math.floorDiv(epochMicros, MICROSECONDS_PER_SECOND);
    }

    public static long scaleEpochMillisToMicros(long epochMillis)
    {
        return multiplyExact(epochMillis, MICROSECONDS_PER_MILLISECOND);
    }

    public static long epochSecondToMicrosWithRounding(long epochSecond, long picoOfSecond)
    {
        return epochSecond * MICROSECONDS_PER_SECOND + roundDiv(picoOfSecond, PICOSECONDS_PER_MICROSECOND);
    }

    public static int getMicrosOfSecond(long epochMicros)
    {
        return floorMod(epochMicros, MICROSECONDS_PER_SECOND);
    }

    public static int getMillisOfSecond(long epochMillis)
    {
        return floorMod(epochMillis, MILLISECONDS_PER_SECOND);
    }

    public static int getMicrosOfMilli(long epochMicros)
    {
        return floorMod(epochMicros, MICROSECONDS_PER_MILLISECOND);
    }

    public static long toEpochMicros(long epochMillis, int picosOfMilli)
    {
        return scaleEpochMillisToMicros(epochMillis) + picosOfMilli / 1_000_000;
    }

    public static long round(long value, int magnitude)
    {
        return roundToNearest(value, POWERS_OF_TEN[magnitude]);
    }

    public static long roundToNearest(long value, long bound)
    {
        return roundDiv(value, bound) * bound;
    }

    public static long scaleFactor(int fromPrecision, int toPrecision)
    {
        if (fromPrecision > toPrecision) {
            throw new IllegalArgumentException("fromPrecision must be <= toPrecision");
        }

        return POWERS_OF_TEN[toPrecision - fromPrecision];
    }

    /**
     * Rescales a value of the given precision to another precision by adding 0s or truncating.
     */
    public static long rescale(long value, int fromPrecision, int toPrecision)
    {
        if (fromPrecision <= toPrecision) {
            value *= scaleFactor(fromPrecision, toPrecision);
        }
        else {
            value /= scaleFactor(toPrecision, fromPrecision);
        }

        return value;
    }

    public static long rescaleWithRounding(long value, int fromPrecision, int toPrecision)
    {
        value = round(value, fromPrecision - toPrecision);
        value = rescale(value, fromPrecision, toPrecision);
        return value;
    }

    public static boolean timestampHasTimeZone(String value)
    {
        Matcher matcher = DATETIME_PATTERN.matcher(value);
        if (!matcher.matches()) {
            throw new IllegalArgumentException(format("Invalid TIMESTAMP '%s'", value));
        }

        return matcher.group("timezone") != null;
    }

    public static int extractTimestampPrecision(String value)
    {
        Matcher matcher = DATETIME_PATTERN.matcher(value);
        if (!matcher.matches()) {
            throw new IllegalArgumentException(format("Invalid TIMESTAMP '%s'", value));
        }

        String fraction = matcher.group("fraction");
        if (fraction == null) {
            return 0;
        }

        return fraction.length();
    }

    public static LocalDateTime toLocalDateTime(TimestampType type, Block block, int position)
    {
        int precision = type.getPrecision();

        long epochMicros;
        int picosOfMicro = 0;
        if (precision <= MAX_SHORT_PRECISION) {
            epochMicros = type.getLong(block, position);
        }
        else {
            LongTimestamp timestamp = (LongTimestamp) type.getObject(block, position);
            epochMicros = timestamp.getEpochMicros();
            picosOfMicro = timestamp.getPicosOfMicro();
        }

        long epochSecond = scaleEpochMicrosToSeconds(epochMicros);
        int nanoFraction = getMicrosOfSecond(epochMicros) * NANOSECONDS_PER_MICROSECOND + (int) (roundToNearest(picosOfMicro, PICOSECONDS_PER_NANOSECOND) / PICOSECONDS_PER_NANOSECOND);

        Instant instant = Instant.ofEpochSecond(epochSecond, nanoFraction);
        return LocalDateTime.ofInstant(instant, UTC);
    }

    public static ZonedDateTime toZonedDateTime(TimestampWithTimeZoneType type, Block block, int position)
    {
        int precision = type.getPrecision();

        long epochMillis;
        int picosOfMilli = 0;
        ZoneId zoneId;
        if (precision <= TimestampWithTimeZoneType.MAX_SHORT_PRECISION) {
            long packedEpochMillis = type.getLong(block, position);
            epochMillis = unpackMillisUtc(packedEpochMillis);
            zoneId = unpackZoneKey(packedEpochMillis).getZoneId();
        }
        else {
            LongTimestampWithTimeZone timestamp = (LongTimestampWithTimeZone) type.getObject(block, position);
            epochMillis = timestamp.getEpochMillis();
            picosOfMilli = timestamp.getPicosOfMilli();
            zoneId = getTimeZoneKey(timestamp.getTimeZoneKey()).getZoneId();
        }

        long epochSecond = scaleEpochMillisToSeconds(epochMillis);
        int nanoFraction = getMillisOfSecond(epochMillis) * NANOSECONDS_PER_MILLISECOND + (int) (roundToNearest(picosOfMilli, PICOSECONDS_PER_NANOSECOND) / PICOSECONDS_PER_NANOSECOND);

        return Instant.ofEpochSecond(epochSecond, nanoFraction).atZone(zoneId);
    }

    /**
     * Formats a timestamp of the given precision. This method doesn't do any rounding, so it's expected that the
     * combination of [epochMicros, picosSecond] is already rounded to the provided precision if necessary
     */
    public static String formatTimestamp(int precision, long epochMicros, int picosOfMicro, ZoneId zoneId)
    {
        return formatTimestamp(precision, epochMicros, picosOfMicro, zoneId, TIMESTAMP_FORMATTER);
    }

    /**
     * Formats a timestamp of the given precision. This method doesn't do any rounding, so it's expected that the
     * combination of [epochMicros, picosSecond] is already rounded to the provided precision if necessary
     */
    public static String formatTimestamp(int precision, long epochMicros, int picosOfMicro, ZoneId zoneId, DateTimeFormatter yearToSecondFormatter)
    {
        checkArgument(picosOfMicro >= 0 && picosOfMicro < PICOSECONDS_PER_MICROSECOND, "picosOfMicro is out of range [0, 1_000_000]");

        Instant instant = Instant.ofEpochSecond(scaleEpochMicrosToSeconds(epochMicros));
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, zoneId);
        long picoFraction = ((long) getMicrosOfSecond(epochMicros)) * PICOSECONDS_PER_MICROSECOND + picosOfMicro;

        StringBuilder builder = new StringBuilder(estimateTimestampFormatLength(precision));
        formatTimestampIntoBuilder(builder, precision, dateTime, picoFraction, yearToSecondFormatter);
        return builder.toString();
    }

    public static String formatTimestampWithTimeZone(int precision, long epochMillis, int picosOfMilli, ZoneId zoneId)
    {
        Instant instant = Instant.ofEpochMilli(epochMillis);
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, zoneId);
        long picoFraction = ((long) getMillisOfSecond(epochMillis)) * PICOSECONDS_PER_MILLISECOND + picosOfMilli;

        String zoneIdString = zoneId.getId();
        StringBuilder builder = new StringBuilder(estimateTimestampFormatLength(precision) + zoneIdString.length() + 1);
        formatTimestampIntoBuilder(builder, precision, dateTime, picoFraction, TIMESTAMP_FORMATTER);
        return builder.append(' ').append(zoneIdString).toString();
    }

    private static int estimateTimestampFormatLength(int precision)
    {
        return TIMESTAMP_FORMATTER_PATTERN.length() + (precision == 0 ? 0 : precision + 1);
    }

    public static String formatTimestamp(int precision, LocalDateTime dateTime, long picoFraction, DateTimeFormatter yearToSecondFormatter, Consumer<StringBuilder> zoneIdFormatter)
    {
        StringBuilder builder = new StringBuilder();
        formatTimestampIntoBuilder(builder, precision, dateTime, picoFraction, yearToSecondFormatter);
        zoneIdFormatter.accept(builder);
        return builder.toString();
    }

    private static void formatTimestampIntoBuilder(StringBuilder builder, int precision, LocalDateTime dateTime, long picoFraction, DateTimeFormatter yearToSecondFormatter)
    {
        yearToSecondFormatter.formatTo(dateTime, builder);
        if (precision > 0) {
            long scalePrecision = rescale(picoFraction, 12, precision);
            builder.append('.');
            int decimalOffset = builder.length();
            builder.setLength(decimalOffset + precision);

            for (int index = builder.length() - 1; index >= decimalOffset; index--) {
                long temp = scalePrecision / 10;
                int digit = (int) (scalePrecision - (temp * 10));
                scalePrecision = temp;
                builder.setCharAt(index, (char) ('0' + digit));
            }
        }
    }

    public static Object parseTimestamp(int precision, String value)
    {
        if (precision <= MAX_SHORT_PRECISION) {
            return parseShortTimestamp(value);
        }

        return parseLongTimestamp(value);
    }

    public static Object parseTimestampWithTimeZone(int precision, String value)
    {
        if (precision <= TimestampWithTimeZoneType.MAX_SHORT_PRECISION) {
            return parseShortTimestampWithTimeZone(value);
        }

        return parseLongTimestampWithTimeZone(value);
    }

    private static long parseShortTimestamp(String value)
    {
        Matcher matcher = DATETIME_PATTERN.matcher(value);
        if (!matcher.matches() || matcher.group("timezone") != null) {
            throw new IllegalArgumentException("Invalid TIMESTAMP: " + value);
        }

        String year = matcher.group("year");
        String month = matcher.group("month");
        String day = matcher.group("day");
        String hour = matcher.group("hour");
        String minute = matcher.group("minute");
        String second = matcher.group("second");
        String fraction = matcher.group("fraction");

        long epochSecond = toEpochSecond(year, month, day, hour, minute, second, UTC);

        int precision = 0;
        long fractionValue = 0;
        if (fraction != null) {
            precision = fraction.length();
            fractionValue = Long.parseLong(fraction);
        }

        if (precision > MAX_SHORT_PRECISION) {
            throw new IllegalArgumentException(format("Cannot parse '%s' as short timestamp. Max allowed precision = %s", value, MAX_SHORT_PRECISION));
        }

        // scale to micros
        return multiplyExact(epochSecond, MICROSECONDS_PER_SECOND) + rescale(fractionValue, precision, 6);
    }

    private static LongTimestamp parseLongTimestamp(String value)
    {
        Matcher matcher = DATETIME_PATTERN.matcher(value);
        if (!matcher.matches() || matcher.group("timezone") != null) {
            throw new IllegalArgumentException("Invalid TIMESTAMP: " + value);
        }

        String year = matcher.group("year");
        String month = matcher.group("month");
        String day = matcher.group("day");
        String hour = matcher.group("hour");
        String minute = matcher.group("minute");
        String second = matcher.group("second");
        String fraction = matcher.group("fraction");

        if (fraction == null || fraction.length() <= MAX_SHORT_PRECISION) {
            throw new IllegalArgumentException(format("Cannot parse '%s' as long timestamp. Precision must be in the range [%s, %s]", value, MAX_SHORT_PRECISION + 1, TimestampType.MAX_PRECISION));
        }

        int precision = fraction.length();
        long epochSecond = toEpochSecond(year, month, day, hour, minute, second, UTC);
        long picoFraction = rescale(Long.parseLong(fraction), precision, 12);

        return longTimestamp(epochSecond, picoFraction);
    }

    private static long parseShortTimestampWithTimeZone(String value)
    {
        Matcher matcher = DATETIME_PATTERN.matcher(value);
        if (!matcher.matches() || matcher.group("timezone") == null) {
            throw new IllegalArgumentException("Invalid TIMESTAMP WITH TIME ZONE: " + value);
        }

        String year = matcher.group("year");
        String month = matcher.group("month");
        String day = matcher.group("day");
        String hour = matcher.group("hour");
        String minute = matcher.group("minute");
        String second = matcher.group("second");
        String fraction = matcher.group("fraction");
        String timezone = matcher.group("timezone");

        ZoneId zoneId = ZoneId.of(timezone);
        long epochSecond = toEpochSecond(year, month, day, hour, minute, second, zoneId);

        int precision = 0;
        long fractionValue = 0;
        if (fraction != null) {
            precision = fraction.length();
            fractionValue = Long.parseLong(fraction);
        }

        if (precision > MAX_SHORT_PRECISION) {
            throw new IllegalArgumentException(format("Cannot parse '%s' as short timestamp. Max allowed precision = %s", value, MAX_SHORT_PRECISION));
        }

        long epochMillis = epochSecond * 1000 + rescale(fractionValue, precision, 3);

        return DateTimeEncoding.packDateTimeWithZone(epochMillis, timezone);
    }

    private static LongTimestampWithTimeZone parseLongTimestampWithTimeZone(String value)
    {
        Matcher matcher = DATETIME_PATTERN.matcher(value);
        if (!matcher.matches() || matcher.group("timezone") == null) {
            throw new IllegalArgumentException("Invalid TIMESTAMP: " + value);
        }

        String year = matcher.group("year");
        String month = matcher.group("month");
        String day = matcher.group("day");
        String hour = matcher.group("hour");
        String minute = matcher.group("minute");
        String second = matcher.group("second");
        String fraction = matcher.group("fraction");
        String timezone = matcher.group("timezone");

        if (fraction == null || fraction.length() <= TimestampWithTimeZoneType.MAX_SHORT_PRECISION) {
            throw new IllegalArgumentException(format("Cannot parse '%s' as long timestamp. Precision must be in the range [%s, %s]", value, TimestampWithTimeZoneType.MAX_SHORT_PRECISION + 1, TimestampWithTimeZoneType.MAX_PRECISION));
        }

        ZoneId zoneId = ZoneId.of(timezone);
        long epochSecond = toEpochSecond(year, month, day, hour, minute, second, zoneId);

        return LongTimestampWithTimeZone.fromEpochSecondsAndFraction(epochSecond, rescale(Long.parseLong(fraction), fraction.length(), 12), getTimeZoneKey(timezone));
    }

    private static long toEpochSecond(String year, String month, String day, String hour, String minute, String second, ZoneId zoneId)
    {
        LocalDateTime timestamp = LocalDateTime.of(
                Integer.parseInt(year),
                Integer.parseInt(month),
                Integer.parseInt(day),
                hour == null ? 0 : Integer.parseInt(hour),
                minute == null ? 0 : Integer.parseInt(minute),
                second == null ? 0 : Integer.parseInt(second),
                0);

        List<ZoneOffset> offsets = zoneId.getRules().getValidOffsets(timestamp);
        if (offsets.isEmpty()) {
            throw new IllegalArgumentException("Invalid TIMESTAMP due to daylight savings transition");
        }

        return timestamp.toEpochSecond(offsets.get(0));
    }

    public static boolean timeHasTimeZone(String value)
    {
        Matcher matcher = TIME_PATTERN.matcher(value);
        if (!matcher.matches()) {
            throw new IllegalArgumentException(format("Invalid TIME '%s'", value));
        }

        return matcher.group("offsetHour") != null && matcher.group("offsetMinute") != null;
    }

    public static int extractTimePrecision(String value)
    {
        Matcher matcher = TIME_PATTERN.matcher(value);
        if (!matcher.matches()) {
            throw new IllegalArgumentException(format("Invalid TIME '%s'", value));
        }

        String fraction = matcher.group("fraction");
        if (fraction == null) {
            return 0;
        }

        return fraction.length();
    }

    public static long parseTime(String value)
    {
        Matcher matcher = TIME_PATTERN.matcher(value);
        if (!matcher.matches() || matcher.group("offsetHour") != null || matcher.group("offsetMinute") != null) {
            throw new IllegalArgumentException("Invalid TIME: " + value);
        }

        int hour = Integer.parseInt(matcher.group("hour"));
        int minute = Integer.parseInt(matcher.group("minute"));
        int second = matcher.group("second") == null ? 0 : Integer.parseInt(matcher.group("second"));

        if (hour > 23 || minute > 59 || second > 59) {
            throw new IllegalArgumentException("Invalid TIME: " + value);
        }

        int precision = 0;
        String fraction = matcher.group("fraction");
        long fractionValue = 0;
        if (fraction != null) {
            precision = fraction.length();
            fractionValue = Long.parseLong(fraction);
        }

        if (precision > TimeType.MAX_PRECISION) {
            throw new IllegalArgumentException("Invalid TIME: " + value);
        }

        return (((hour * 60L) + minute) * 60 + second) * PICOSECONDS_PER_SECOND + rescale(fractionValue, precision, 12);
    }

    public static Object parseTimeWithTimeZone(int precision, String value)
    {
        if (precision <= TimeWithTimeZoneType.MAX_SHORT_PRECISION) {
            return parseShortTimeWithTimeZone(value);
        }

        return parseLongTimeWithTimeZone(value);
    }

    public static long parseShortTimeWithTimeZone(String value)
    {
        Matcher matcher = TIME_PATTERN.matcher(value);
        if (!matcher.matches() || matcher.group("offsetHour") == null || matcher.group("offsetMinute") == null) {
            throw new IllegalArgumentException("Invalid TIME WITH TIME ZONE: " + value);
        }

        int hour = Integer.parseInt(matcher.group("hour"));
        int minute = Integer.parseInt(matcher.group("minute"));
        int second = matcher.group("second") == null ? 0 : Integer.parseInt(matcher.group("second"));
        int offsetSign = matcher.group("sign").equals("+") ? 1 : -1;
        int offsetHour = Integer.parseInt((matcher.group("offsetHour")));
        int offsetMinute = Integer.parseInt((matcher.group("offsetMinute")));

        if (hour > 23 || minute > 59 || second > 59 || !isValidOffset(offsetHour, offsetMinute)) {
            throw new IllegalArgumentException("Invalid TIME WITH TIME ZONE: " + value);
        }

        int precision = 0;
        String fraction = matcher.group("fraction");
        long fractionValue = 0;
        if (fraction != null) {
            precision = fraction.length();
            fractionValue = Long.parseLong(fraction);
        }

        long nanos = (((hour * 60L) + minute) * 60 + second) * NANOSECONDS_PER_SECOND + rescale(fractionValue, precision, 9);
        return packTimeWithTimeZone(nanos, calculateOffsetMinutes(offsetSign, offsetHour, offsetMinute));
    }

    public static LongTimeWithTimeZone parseLongTimeWithTimeZone(String value)
    {
        Matcher matcher = TIME_PATTERN.matcher(value);
        if (!matcher.matches() || matcher.group("offsetHour") == null || matcher.group("offsetMinute") == null) {
            throw new IllegalArgumentException("Invalid TIME WITH TIME ZONE: " + value);
        }

        int hour = Integer.parseInt(matcher.group("hour"));
        int minute = Integer.parseInt(matcher.group("minute"));
        int second = matcher.group("second") == null ? 0 : Integer.parseInt(matcher.group("second"));
        int offsetSign = matcher.group("sign").equals("+") ? 1 : -1;
        int offsetHour = Integer.parseInt((matcher.group("offsetHour")));
        int offsetMinute = Integer.parseInt((matcher.group("offsetMinute")));

        if (hour > 23 || minute > 59 || second > 59 || !isValidOffset(offsetHour, offsetMinute)) {
            throw new IllegalArgumentException("Invalid TIME WITH TIME ZONE: " + value);
        }

        int precision = 0;
        String fraction = matcher.group("fraction");
        long fractionValue = 0;
        if (fraction != null) {
            precision = fraction.length();
            fractionValue = Long.parseLong(fraction);
        }

        long picos = (((hour * 60) + minute) * 60 + second) * PICOSECONDS_PER_SECOND + rescale(fractionValue, precision, 12);
        return new LongTimeWithTimeZone(picos, calculateOffsetMinutes(offsetSign, offsetHour, offsetMinute));
    }

    public static LongTimestamp longTimestamp(long precision, Instant start)
    {
        checkArgument(precision > MAX_SHORT_PRECISION && precision <= TimestampType.MAX_PRECISION, "Precision is out of range");
        return new LongTimestamp(
                start.getEpochSecond() * MICROSECONDS_PER_SECOND + start.getLong(MICRO_OF_SECOND),
                (int) round((start.getNano() % PICOSECONDS_PER_NANOSECOND) * (long) PICOSECONDS_PER_NANOSECOND, (int) (TimestampType.MAX_PRECISION - precision)));
    }

    public static LongTimestamp longTimestamp(long epochSecond, long fractionInPicos)
    {
        return new LongTimestamp(
                multiplyExact(epochSecond, MICROSECONDS_PER_SECOND) + fractionInPicos / PICOSECONDS_PER_MICROSECOND,
                (int) (fractionInPicos % PICOSECONDS_PER_MICROSECOND));
    }

    public static LongTimestampWithTimeZone longTimestampWithTimeZone(long precision, Instant start, TimeZoneKey timeZoneKey)
    {
        checkArgument(precision <= TimestampWithTimeZoneType.MAX_PRECISION, "Precision is out of range");

        long epochMilli = start.toEpochMilli();
        int picosOfMilli = (int) round((start.getNano() % NANOSECONDS_PER_MILLISECOND) * (long) PICOSECONDS_PER_NANOSECOND, (int) (TimestampWithTimeZoneType.MAX_PRECISION - precision));
        if (picosOfMilli == PICOSECONDS_PER_MILLISECOND) {
            epochMilli++;
            picosOfMilli = 0;
        }
        return LongTimestampWithTimeZone.fromEpochMillisAndFraction(epochMilli, picosOfMilli, timeZoneKey);
    }

    public static LongTimestampWithTimeZone longTimestampWithTimeZone(long epochSecond, long fractionInPicos, ZoneId zoneId)
    {
        return LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                multiplyExact(epochSecond, MILLISECONDS_PER_SECOND) + fractionInPicos / PICOSECONDS_PER_MILLISECOND,
                (int) (fractionInPicos % PICOSECONDS_PER_MILLISECOND),
                getTimeZoneKey(zoneId.getId()));
    }

    public static long roundToEpochMillis(LongTimestampWithTimeZone timestamp)
    {
        long epochMillis = timestamp.getEpochMillis();
        if (roundToNearest(timestamp.getPicosOfMilli(), PICOSECONDS_PER_MILLISECOND) == PICOSECONDS_PER_MILLISECOND) {
            epochMillis++;
        }
        return epochMillis;
    }

    public static int calculateOffsetMinutes(int sign, int offsetHour, int offsetMinute)
    {
        return sign * (offsetHour * 60 + offsetMinute);
    }

    public static int getOffsetMinutes(Instant instant, TimeZoneKey zoneKey)
    {
        return zoneKey
                .getZoneId()
                .getRules()
                .getOffset(instant)
                .getTotalSeconds() / 60;
    }

    public static boolean isValidOffset(int hour, int minute)
    {
        return (hour == 14 && minute == 0) ||
                (hour >= 0 && hour < 14 && minute >= 0 && minute <= 59);
    }
}
