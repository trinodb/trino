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
package io.trino.util;

import com.google.common.annotations.VisibleForTesting;
import io.trino.client.IntervalDayTime;
import io.trino.client.IntervalYearMonth;
import io.trino.spi.TrinoException;
import io.trino.spi.type.TimeZoneKey;
import io.trino.sql.tree.CompositeIntervalQualifier;
import io.trino.sql.tree.IntervalField;
import io.trino.sql.tree.IntervalLiteral;
import org.joda.time.DateTime;
import org.joda.time.DurationFieldType;
import org.joda.time.MutablePeriod;
import org.joda.time.Period;
import org.joda.time.PeriodType;
import org.joda.time.ReadWritablePeriod;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;
import org.joda.time.format.DateTimePrinter;
import org.joda.time.format.ISODateTimeFormat;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;
import org.joda.time.format.PeriodParser;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.Month;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.StandardErrorCode.INVALID_LITERAL;
import static io.trino.sql.tree.IntervalLiteral.Sign.NEGATIVE;
import static io.trino.sql.tree.IntervalLiteral.Sign.POSITIVE;
import static io.trino.util.DateTimeZoneIndex.getChronology;
import static io.trino.util.DateTimeZoneIndex.packDateTimeWithZone;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;

public final class DateTimeUtils
{
    private DateTimeUtils() {}

    private static final DateTimeFormatter DATE_FORMATTER = ISODateTimeFormat.date().withZoneUTC();

    public static int parseDate(String value)
    {
        // Note: update DomainTranslator.Visitor.createVarcharCastToDateComparisonExtractionResult whenever varchar->date conversion (CAST) behavior changes.

        // in order to follow the standard, we should validate the value:
        // - the required format is 'YYYY-MM-DD'
        // - all components should be unsigned numbers
        // https://github.com/trinodb/trino/issues/10677

        OptionalInt days = parseIfIso8601DateFormat(value);
        if (days.isPresent()) {
            return days.getAsInt();
        }
        return toIntExact(TimeUnit.MILLISECONDS.toDays(DATE_FORMATTER.parseMillis(value)));
    }

    /**
     * Parse date if it is in the format {@code yyyy-MM-dd}.
     *
     * @return the number of days since 1970-01-01 or empty when value does not match the expected format
     * @throws DateTimeException when value matches the expected format but is invalid (month or day number out of range)
     */
    @VisibleForTesting
    static OptionalInt parseIfIso8601DateFormat(String value)
    {
        if (value.length() != 10 || value.charAt(4) != '-' || value.charAt(7) != '-') {
            return OptionalInt.empty();
        }

        OptionalInt year = parseIntSimple(value, 0, 4);
        if (year.isEmpty()) {
            return OptionalInt.empty();
        }

        OptionalInt month = parseIntSimple(value, 5, 2);
        if (month.isEmpty()) {
            return OptionalInt.empty();
        }

        OptionalInt day = parseIntSimple(value, 8, 2);
        if (day.isEmpty()) {
            return OptionalInt.empty();
        }

        int y = year.getAsInt();
        int m = month.getAsInt();
        int d = day.getAsInt();
        // Mirror LocalDate.of's validation order and exception messages so that callers
        // observing DateTimeException.getMessage() see the same text.
        if (m < 1 || m > 12) {
            throw new DateTimeException("Invalid value for MonthOfYear (valid values 1 - 12): " + m);
        }
        if (d < 1 || d > 31) {
            throw new DateTimeException("Invalid value for DayOfMonth (valid values 1 - 28/31): " + d);
        }
        int monthLength = FastDate.daysInMonth(y, m);
        if (d > monthLength) {
            if (d == 29 && m == 2) {
                throw new DateTimeException("Invalid date 'February 29' as '" + y + "' is not a leap year");
            }
            throw new DateTimeException("Invalid date '" + Month.of(m).name() + " " + d + "'");
        }
        return OptionalInt.of(FastDate.daysFromYmd(y, m, d));
    }

    /**
     * Parse positive integer with radix 10.
     *
     * @return parsed value or empty if any non digit found
     */
    private static OptionalInt parseIntSimple(String input, int offset, int length)
    {
        checkArgument(length > 0, "Invalid length %s", length);

        int result = 0;
        for (int i = 0; i < length; i++) {
            int digit = input.charAt(offset + i) - '0';
            if (digit < 0 || digit > 9) {
                return OptionalInt.empty();
            }
            result = result * 10 + digit;
        }
        return OptionalInt.of(result);
    }

    public static String printDate(int days)
    {
        long ymd = FastDate.ymdFromEpochDay(days);
        int year = (int) (ymd >> 32);
        if (year < 0 || year > 9999) {
            return DATE_FORMATTER.print(TimeUnit.DAYS.toMillis(days));
        }
        int month = (int) ((ymd >> 8) & 0xFF);
        int day = (int) (ymd & 0xFF);
        char[] buf = new char[10];
        buf[0] = (char) ('0' + year / 1000);
        buf[1] = (char) ('0' + (year / 100) % 10);
        buf[2] = (char) ('0' + (year / 10) % 10);
        buf[3] = (char) ('0' + year % 10);
        buf[4] = '-';
        buf[5] = (char) ('0' + month / 10);
        buf[6] = (char) ('0' + month % 10);
        buf[7] = '-';
        buf[8] = (char) ('0' + day / 10);
        buf[9] = (char) ('0' + day % 10);
        return new String(buf);
    }

    /**
     * Fast civil-from-days (year/month/day from days-since-1970-01-01) using Ben Joffe's
     * 64-bit algorithm: <a href="https://www.benjoffe.com/fast-date-64">www.benjoffe.com/fast-date-64</a>.
     * Reference C++ implementation (Boost Software License 1.0):
     * <a href="https://github.com/benjoffe/fast-date-benchmarks/blob/main/algorithms/benjoffe_fast64.hpp">benjoffe_fast64.hpp</a>.
     * <p>
     * Math.unsignedMultiplyHigh is intrinsified by HotSpot to {@code mulq}/{@code umulh},
     * so each mul-shift-by-64 step is a single machine instruction. Operates on the x86 SCALE=32
     * variant of the algorithm — the ARM variant differs only in immediate-encoding efficiency.
     */
    public static final class FastDate
    {
        private FastDate() {}

        private static final long ERAS = 14704L;
        private static final long D_SHIFT = 146097L * ERAS - 719469L;
        private static final long Y_SHIFT = 400L * ERAS - 1L;
        private static final long C1 = 505_054_698_555_331L;
        private static final long C2 = 50_504_432_782_230_121L;
        private static final long C3 = 8_619_973_866_219_416L;
        private static final long YPT_MULT = 782_432L;
        private static final long BUMP_THRESHOLD = 126_464L;
        private static final long SHIFT_NO_BUMP = 977_792L;
        private static final long SHIFT_BUMP = 191_360L;

        // The choice of ERAS=14704 maximizes the algorithm's safe range while keeping D_SHIFT
        // within uint32. Below this bound, the (D_SHIFT - days) computation overflows uint32
        // and corrupts every downstream value. Affects only ~7172 days at the very bottom of
        // the int DATE range — dates around year -5,877,660, far outside any realistic use.
        private static final int FAST_MIN_DAYS = (int) (D_SHIFT - 0xFFFFFFFFL);   // = -2_147_476_476

        // Cumulative days before the start of each civil month, indexed 1..12 (0 unused).
        private static final int[] DOY_PREFIX_NON_LEAP = {0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334};
        private static final int[] DOY_PREFIX_LEAP = {0, 0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335};

        // Days-in-month, indexed by civil month 1..12 (0 unused).
        private static final int[] DAYS_IN_MONTH_NON_LEAP = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
        private static final int[] DAYS_IN_MONTH_LEAP = {0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

        /**
         * Returns {@code (year << 32) | (month << 8) | day} where year is signed,
         * month is 1..12, and day is 1..31. Valid for the entire {@code int} range of
         * days-since-epoch (≈ ±5.88M years).
         */
        public static long ymdFromEpochDay(int days)
        {
            if (days < FAST_MIN_DAYS) {
                LocalDate date = LocalDate.ofEpochDay(days);
                return ((long) date.getYear() << 32) | ((long) date.getMonthValue() << 8) | date.getDayOfMonth();
            }
            // Mirror the C++ uint32 wraparound by zero-extending into a long.
            long rev = (D_SHIFT - days) & 0xFFFFFFFFL;
            long cen = Math.unsignedMultiplyHigh(C1, rev);
            long jul = rev + cen - (cen >>> 2);
            long numLow = C2 * jul;
            long numHigh = Math.unsignedMultiplyHigh(C2, jul);
            long yrs = (Y_SHIFT - numHigh) & 0xFFFFFFFFL;
            long ypt = Math.unsignedMultiplyHigh(YPT_MULT, numLow);
            long bump = (ypt < BUMP_THRESHOLD) ? 1L : 0L;
            long shift = (bump != 0) ? SHIFT_BUMP : SHIFT_NO_BUMP;
            long n = (yrs & 3L) * 512L + shift - ypt;
            long month = n >>> 16;
            long day = Math.unsignedMultiplyHigh(C3, n & 0xFFFFL) + 1L;
            int year = (int) (yrs + bump);
            return ((long) year << 32) | (month << 8) | day;
        }

        public static int yearOf(int days)
        {
            return (int) (ymdFromEpochDay(days) >> 32);
        }

        public static int monthOf(int days)
        {
            return (int) ((ymdFromEpochDay(days) >> 8) & 0xFF);
        }

        public static int dayOf(int days)
        {
            return (int) (ymdFromEpochDay(days) & 0xFF);
        }

        public static int quarterOf(int days)
        {
            return (monthOf(days) - 1) / 3 + 1;
        }

        public static int dayOfYearOf(int days)
        {
            long ymd = ymdFromEpochDay(days);
            int year = (int) (ymd >> 32);
            int month = (int) ((ymd >> 8) & 0xFF);
            int day = (int) (ymd & 0xFF);
            return (isLeap(year) ? DOY_PREFIX_LEAP : DOY_PREFIX_NON_LEAP)[month] + day;
        }

        public static int daysInMonthOf(int days)
        {
            long ymd = ymdFromEpochDay(days);
            int year = (int) (ymd >> 32);
            int month = (int) ((ymd >> 8) & 0xFF);
            return (isLeap(year) ? DAYS_IN_MONTH_LEAP : DAYS_IN_MONTH_NON_LEAP)[month];
        }

        /**
         * Civil days in {@code month} of {@code year}. {@code month} is 1..12.
         */
        public static int daysInMonth(int year, int month)
        {
            return (isLeap(year) ? DAYS_IN_MONTH_LEAP : DAYS_IN_MONTH_NON_LEAP)[month];
        }

        public static boolean isLeap(int year)
        {
            // Java % returns same sign as dividend; (year & 3) == 0 is equivalent to
            // year % 4 == 0 for both signs in two's complement.
            return ((year & 3) == 0) && (year % 100 != 0 || year % 400 == 0);
        }

        /**
         * Days-since-1970-01-01 from civil (year, month, day) using Ben Joffe's {@code to_rata_die}.
         * <p>
         * Precondition: the input is a valid civil date. No range or leap-day validation is
         * performed. Safe across the full {@code int} days range (year shift of 5,880,000 covers
         * roughly ±5.88M years centred on the UNIX epoch).
         * <p>
         * The {@code yrs / 100} division lowers to a single mul-shift on HotSpot since 100 is a
         * compile-time constant; all other divisions are by powers of two.
         */
        public static int daysFromYmd(int year, int month, int day)
        {
            long bump = month <= 2 ? 1L : 0L;
            long yrs = (year + 5_880_000L) - bump;
            long cen = yrs / 100L;
            long shift = bump != 0 ? 8829L : -2919L;
            long yearDays = yrs * 365L + (yrs >>> 2) - cen + (cen >>> 2);
            long monthDays = (979L * month + shift) >> 5;
            return (int) (yearDays + monthDays + day - 2_148_345_369L);
        }
    }

    private static final DateTimeFormatter TIMESTAMP_WITH_OR_WITHOUT_TIME_ZONE_FORMATTER;

    static {
        DateTimeParser[] timestampWithoutTimeZoneParser = {
                DateTimeFormat.forPattern("yyyyyy-M-d").getParser(),
                DateTimeFormat.forPattern("yyyyyy-M-d H:m").getParser(),
                DateTimeFormat.forPattern("yyyyyy-M-d H:m:s").getParser(),
                DateTimeFormat.forPattern("yyyyyy-M-d H:m:s.SSS").getParser(),
        };

        DateTimeParser[] timestampWithTimeZoneParser = {
                DateTimeFormat.forPattern("yyyyyy-M-dZ").getParser(),
                DateTimeFormat.forPattern("yyyyyy-M-d Z").getParser(),
                DateTimeFormat.forPattern("yyyyyy-M-d H:mZ").getParser(),
                DateTimeFormat.forPattern("yyyyyy-M-d H:m Z").getParser(),
                DateTimeFormat.forPattern("yyyyyy-M-d H:m:sZ").getParser(),
                DateTimeFormat.forPattern("yyyyyy-M-d H:m:s Z").getParser(),
                DateTimeFormat.forPattern("yyyyyy-M-d H:m:s.SSSZ").getParser(),
                DateTimeFormat.forPattern("yyyyyy-M-d H:m:s.SSS Z").getParser(),
                DateTimeFormat.forPattern("yyyyyy-M-dZZZ").getParser(),
                DateTimeFormat.forPattern("yyyyyy-M-d ZZZ").getParser(),
                DateTimeFormat.forPattern("yyyyyy-M-d H:mZZZ").getParser(),
                DateTimeFormat.forPattern("yyyyyy-M-d H:m ZZZ").getParser(),
                DateTimeFormat.forPattern("yyyyyy-M-d H:m:sZZZ").getParser(),
                DateTimeFormat.forPattern("yyyyyy-M-d H:m:s ZZZ").getParser(),
                DateTimeFormat.forPattern("yyyyyy-M-d H:m:s.SSSZZZ").getParser(),
                DateTimeFormat.forPattern("yyyyyy-M-d H:m:s.SSS ZZZ").getParser(),
        };

        DateTimePrinter timestampWithTimeZonePrinter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS ZZZ").getPrinter();

        DateTimeParser[] timestampWithOrWithoutTimeZoneParser = Stream.concat(Stream.of(timestampWithoutTimeZoneParser), Stream.of(timestampWithTimeZoneParser))
                .toArray(DateTimeParser[]::new);
        TIMESTAMP_WITH_OR_WITHOUT_TIME_ZONE_FORMATTER = new DateTimeFormatterBuilder()
                .append(timestampWithTimeZonePrinter, timestampWithOrWithoutTimeZoneParser)
                .toFormatter()
                .withOffsetParsed();
    }

    /**
     * Parse a string (optionally containing a zone) as a value of TIMESTAMP WITH TIME ZONE type.
     * If the string doesn't specify a zone, it is interpreted in {@code timeZoneKey} zone.
     * <p>
     * For example: {@code "2000-01-01 01:23:00"} is parsed to TIMESTAMP WITH TIME ZONE
     * {@code 2000-01-01T01:23:00 <provided zone>} and {@code "2000-01-01 01:23:00 +01:23"}
     * is parsed to TIMESTAMP WITH TIME ZONE {@code 2000-01-01T01:23:00.000+01:23}.
     *
     * @return stack representation of TIMESTAMP WITH TIME ZONE type
     */
    public static long convertToTimestampWithTimeZone(TimeZoneKey timeZoneKey, String timestampWithTimeZone)
    {
        DateTime dateTime = TIMESTAMP_WITH_OR_WITHOUT_TIME_ZONE_FORMATTER.withChronology(getChronology(timeZoneKey)).withOffsetParsed().parseDateTime(timestampWithTimeZone);
        return packDateTimeWithZone(dateTime);
    }

    private static final int YEAR_FIELD = 0;
    private static final int MONTH_FIELD = 1;
    private static final int DAY_FIELD = 3;
    private static final int HOUR_FIELD = 4;
    private static final int MINUTE_FIELD = 5;
    private static final int SECOND_FIELD = 6;
    private static final int MILLIS_FIELD = 7;

    private static final PeriodFormatter INTERVAL_DAY_SECOND_FORMATTER = createPeriodFormatter(new IntervalField.Day(), new IntervalField.Second(OptionalInt.empty()));
    private static final PeriodFormatter INTERVAL_DAY_MINUTE_FORMATTER = createPeriodFormatter(new IntervalField.Day(), new IntervalField.Minute());
    private static final PeriodFormatter INTERVAL_DAY_HOUR_FORMATTER = createPeriodFormatter(new IntervalField.Day(), new IntervalField.Hour());
    private static final PeriodFormatter INTERVAL_DAY_FORMATTER = createPeriodFormatter(new IntervalField.Day(), new IntervalField.Day());

    private static final PeriodFormatter INTERVAL_HOUR_SECOND_FORMATTER = createPeriodFormatter(new IntervalField.Hour(), new IntervalField.Second(OptionalInt.empty()));
    private static final PeriodFormatter INTERVAL_HOUR_MINUTE_FORMATTER = createPeriodFormatter(new IntervalField.Hour(), new IntervalField.Minute());
    private static final PeriodFormatter INTERVAL_HOUR_FORMATTER = createPeriodFormatter(new IntervalField.Hour(), new IntervalField.Hour());

    private static final PeriodFormatter INTERVAL_MINUTE_SECOND_FORMATTER = createPeriodFormatter(new IntervalField.Minute(), new IntervalField.Second(OptionalInt.empty()));
    private static final PeriodFormatter INTERVAL_MINUTE_FORMATTER = createPeriodFormatter(new IntervalField.Minute(), new IntervalField.Minute());

    private static final PeriodFormatter INTERVAL_SECOND_FORMATTER = createPeriodFormatter(new IntervalField.Second(OptionalInt.empty()), new IntervalField.Second(OptionalInt.empty()));

    private static final PeriodFormatter INTERVAL_YEAR_MONTH_FORMATTER = createPeriodFormatter(new IntervalField.Year(), new IntervalField.Month());
    private static final PeriodFormatter INTERVAL_YEAR_FORMATTER = createPeriodFormatter(new IntervalField.Year(), new IntervalField.Year());

    private static final PeriodFormatter INTERVAL_MONTH_FORMATTER = createPeriodFormatter(new IntervalField.Month(), new IntervalField.Month());

    public static long parseDayTimeInterval(String value, IntervalField startField, Optional<IntervalField> endField)
    {
        try {
            if (startField instanceof IntervalField.Day && endField.isEmpty()) {
                return parsePeriodMillis(INTERVAL_DAY_FORMATTER, value);
            }
            if (startField instanceof IntervalField.Day && endField.get() instanceof IntervalField.Second) {
                return parsePeriodMillis(INTERVAL_DAY_SECOND_FORMATTER, value);
            }
            if (startField instanceof IntervalField.Day && endField.get() instanceof IntervalField.Minute) {
                return parsePeriodMillis(INTERVAL_DAY_MINUTE_FORMATTER, value);
            }
            if (startField instanceof IntervalField.Day && endField.get() instanceof IntervalField.Hour) {
                return parsePeriodMillis(INTERVAL_DAY_HOUR_FORMATTER, value);
            }

            if (startField instanceof IntervalField.Hour && endField.isEmpty()) {
                return parsePeriodMillis(INTERVAL_HOUR_FORMATTER, value);
            }
            if (startField instanceof IntervalField.Hour && endField.get() instanceof IntervalField.Second) {
                return parsePeriodMillis(INTERVAL_HOUR_SECOND_FORMATTER, value);
            }
            if (startField instanceof IntervalField.Hour && endField.get() instanceof IntervalField.Minute) {
                return parsePeriodMillis(INTERVAL_HOUR_MINUTE_FORMATTER, value);
            }

            if (startField instanceof IntervalField.Minute && endField.isEmpty()) {
                return parsePeriodMillis(INTERVAL_MINUTE_FORMATTER, value);
            }
            if (startField instanceof IntervalField.Minute && endField.get() instanceof IntervalField.Second) {
                return parsePeriodMillis(INTERVAL_MINUTE_SECOND_FORMATTER, value);
            }

            if (startField instanceof IntervalField.Second && endField.isEmpty()) {
                return parsePeriodMillis(INTERVAL_SECOND_FORMATTER, value);
            }
        }
        catch (IllegalArgumentException e) {
            throw invalidInterval(e, value, startField, endField.orElse(startField));
        }

        throw invalidQualifier(startField, endField.orElse(startField));
    }

    public static IntervalLiteral formatDayTimeInterval(Duration duration)
    {
        long millis = duration.toMillis();
        IntervalLiteral.Sign sign = millis < 0 ? NEGATIVE : POSITIVE;
        Period period = new Period(Math.abs(millis)).normalizedStandard(PeriodType.dayTime());
        // Always use INTERVAL DAY TO SECOND. The output is more verbose
        // (e.g., "1 0:00:00" instead of "1"), but this avoids the need to
        // determine the minimal field range and choose a specialized formatter.
        String value = INTERVAL_DAY_SECOND_FORMATTER.print(period);

        return new IntervalLiteral(value, sign, new CompositeIntervalQualifier(OptionalInt.empty(), new IntervalField.Day(), new IntervalField.Second(OptionalInt.empty())));
    }

    private static long parsePeriodMillis(PeriodFormatter periodFormatter, String value)
    {
        Period period = parsePeriod(periodFormatter, value);
        return IntervalDayTime.toMillis(
                period.getValue(DAY_FIELD),
                period.getValue(HOUR_FIELD),
                period.getValue(MINUTE_FIELD),
                period.getValue(SECOND_FIELD),
                period.getValue(MILLIS_FIELD));
    }

    public static long parseYearMonthInterval(String value, IntervalField startField, Optional<IntervalField> endField)
    {
        try {
            if (startField instanceof IntervalField.Year && endField.isEmpty()) {
                return parsePeriodMonths(value, INTERVAL_YEAR_FORMATTER);
            }
            if (startField instanceof IntervalField.Year && endField.get() instanceof IntervalField.Month) {
                return parsePeriodMonths(value, INTERVAL_YEAR_MONTH_FORMATTER);
            }

            if (startField instanceof IntervalField.Month && endField.isEmpty()) {
                return parsePeriodMonths(value, INTERVAL_MONTH_FORMATTER);
            }
        }
        catch (IllegalArgumentException e) {
            throw invalidInterval(e, value, startField, endField.orElse(startField));
        }

        throw invalidQualifier(startField, endField.orElse(startField));
    }

    private static long parsePeriodMonths(String value, PeriodFormatter periodFormatter)
    {
        Period period = parsePeriod(periodFormatter, value);
        return IntervalYearMonth.toMonths(
                period.getValue(YEAR_FIELD),
                period.getValue(MONTH_FIELD));
    }

    private static Period parsePeriod(PeriodFormatter periodFormatter, String value)
    {
        boolean negative = value.startsWith("-");
        if (negative) {
            value = value.substring(1);
        }

        Period period = periodFormatter.parsePeriod(value);
        for (DurationFieldType type : period.getFieldTypes()) {
            checkArgument(period.get(type) >= 0, "Period field %s is negative", type);
        }

        if (negative) {
            period = period.negated();
        }
        return period;
    }

    private static TrinoException invalidInterval(Throwable throwable, String value, IntervalField startField, IntervalField endField)
    {
        String message;
        if (startField == endField) {
            message = format("Invalid INTERVAL %s value: %s", startField.name(), value);
        }
        else {
            message = format("Invalid INTERVAL %s TO %s value: %s", startField.name(), endField.name(), value);
        }
        return new TrinoException(INVALID_LITERAL, message, throwable);
    }

    private static TrinoException invalidQualifier(IntervalField startField, IntervalField endField)
    {
        throw new TrinoException(INVALID_LITERAL, "Invalid interval qualifier: " + startField + " TO " + endField);
    }

    private static PeriodFormatter createPeriodFormatter(IntervalField startField, IntervalField endField)
    {
        if (endField == null) {
            endField = startField;
        }

        List<PeriodParser> parsers = new ArrayList<>();

        PeriodFormatterBuilder builder = new PeriodFormatterBuilder()
                // Ensures zero-valued fields are printed instead of omitted. This affects printing only, not parsing.
                // Example for INTERVAL HOUR TO SECOND:
                //   With printZeroIfSupported():    "2:00:45"
                //   Without printZeroIfSupported(): "2::45"
                .printZeroIfSupported();
        switch (startField) {
            case IntervalField.Year _:
                builder.appendYears();
                parsers.add(builder.toParser());
                if (endField instanceof IntervalField.Year) {
                    break;
                }
                builder.appendLiteral("-");
                // fall through

            case IntervalField.Month _:
                builder.appendMonths();
                parsers.add(builder.toParser());
                if (!(endField instanceof IntervalField.Month)) {
                    throw invalidQualifier(startField, endField);
                }
                break;

            case IntervalField.Day _:
                builder.appendDays();
                parsers.add(builder.toParser());
                if (endField instanceof IntervalField.Day) {
                    break;
                }
                builder.appendLiteral(" ");
                // fall through

            case IntervalField.Hour _:
                builder.appendHours();
                parsers.add(builder.toParser());
                if (endField instanceof IntervalField.Hour) {
                    break;
                }
                builder.appendLiteral(":");
                // Ensures fixed-width, zero-padded minutes. This affects printing only, not parsing.
                // Applies to the next appended field (minutes).
                // Example for INTERVAL HOUR TO MINUTE:
                //   With minimumPrintedDigits(2): "2:05"
                //   Without minimumPrintedDigits(2): "2:5"
                builder.minimumPrintedDigits(2);
                // fall through

            case IntervalField.Minute _:
                builder.appendMinutes();
                parsers.add(builder.toParser());
                if (endField instanceof IntervalField.Minute) {
                    break;
                }
                builder.appendLiteral(":");
                // Ensures fixed-width, zero-padded seconds. This affects printing only, not parsing.
                // Applies to the next appended field (seconds).
                // Example for INTERVAL HOUR TO SECOND:
                //   With minimumPrintedDigits(2): "2:05:07"
                //   Without minimumPrintedDigits(2): "2:05:7"
                builder.minimumPrintedDigits(2);
                // fall through

            case IntervalField.Second _:
                builder.appendSecondsWithOptionalMillis();
                parsers.add(builder.toParser());
                break;
        }

        return new PeriodFormatter(builder.toPrinter(), new OrderedPeriodParser(parsers));
    }

    private static class OrderedPeriodParser
            implements PeriodParser
    {
        private final List<PeriodParser> parsers;

        private OrderedPeriodParser(List<PeriodParser> parsers)
        {
            this.parsers = parsers;
        }

        @Override
        public int parseInto(ReadWritablePeriod period, String text, int position, Locale locale)
        {
            int bestValidPos = position;
            ReadWritablePeriod bestValidPeriod = null;

            for (PeriodParser parser : parsers) {
                ReadWritablePeriod parsedPeriod = new MutablePeriod();
                int parsePos = parser.parseInto(parsedPeriod, text, position, locale);
                if (parsePos >= position) {
                    if (parsePos > bestValidPos) {
                        bestValidPos = parsePos;
                        bestValidPeriod = parsedPeriod;
                        if (parsePos >= text.length()) {
                            break;
                        }
                    }
                }
            }

            // Restore the state to the best valid parse.
            if (bestValidPeriod != null) {
                period.setPeriod(bestValidPeriod);
            }
            return bestValidPos;
        }
    }
}
