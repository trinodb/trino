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
import io.airlift.slice.Slice;
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

    public static int parseDate(Slice value)
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
        // only the fallback formatter needs the value as a String
        return toIntExact(TimeUnit.MILLISECONDS.toDays(DATE_FORMATTER.parseMillis(value.toStringUtf8())));
    }

    /**
     * Parse date if it is in the format {@code yyyy-MM-dd}.
     *
     * @return the number of days since 1970-01-01 or empty when value does not match the expected format
     * @throws DateTimeException when value matches the expected format but is invalid (month or day number out of range)
     */
    @VisibleForTesting
    static OptionalInt parseIfIso8601DateFormat(Slice value)
    {
        // the format is all ASCII, so a byte length of ten is also a length of ten characters
        if (value.length() != 10 || value.getByte(4) != '-' || value.getByte(7) != '-') {
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

        LocalDate date = LocalDate.of(year.getAsInt(), month.getAsInt(), day.getAsInt());
        return OptionalInt.of(toIntExact(date.toEpochDay()));
    }

    /**
     * Parse positive integer with radix 10.
     *
     * @return parsed value or empty if any non digit found
     */
    private static OptionalInt parseIntSimple(Slice input, int offset, int length)
    {
        checkArgument(length > 0, "Invalid length %s", length);

        int result = 0;
        for (int i = 0; i < length; i++) {
            int digit = input.getByte(offset + i) - '0';
            if (digit < 0 || digit > 9) {
                return OptionalInt.empty();
            }
            result = result * 10 + digit;
        }
        return OptionalInt.of(result);
    }

    public static String printDate(int days)
    {
        return DATE_FORMATTER.print(TimeUnit.DAYS.toMillis(days));
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

    private static final int PICOS_PER_MICRO = 1_000_000;
    private static final int YEAR_FIELD = 0;
    private static final int MONTH_FIELD = 1;
    private static final int DAY_FIELD = 3;
    private static final int HOUR_FIELD = 4;
    private static final int MINUTE_FIELD = 5;
    private static final int SECOND_FIELD = 6;

    private static final PeriodFormatter INTERVAL_DAY_SECOND_FORMATTER = createPeriodFormatter(new IntervalField.Day(), new IntervalField.Second(Optional.empty()));
    private static final PeriodFormatter INTERVAL_DAY_MINUTE_FORMATTER = createPeriodFormatter(new IntervalField.Day(), new IntervalField.Minute());
    private static final PeriodFormatter INTERVAL_DAY_HOUR_FORMATTER = createPeriodFormatter(new IntervalField.Day(), new IntervalField.Hour());
    private static final PeriodFormatter INTERVAL_DAY_FORMATTER = createPeriodFormatter(new IntervalField.Day(), new IntervalField.Day());

    private static final PeriodFormatter INTERVAL_HOUR_SECOND_FORMATTER = createPeriodFormatter(new IntervalField.Hour(), new IntervalField.Second(Optional.empty()));
    private static final PeriodFormatter INTERVAL_HOUR_MINUTE_FORMATTER = createPeriodFormatter(new IntervalField.Hour(), new IntervalField.Minute());
    private static final PeriodFormatter INTERVAL_HOUR_FORMATTER = createPeriodFormatter(new IntervalField.Hour(), new IntervalField.Hour());

    private static final PeriodFormatter INTERVAL_MINUTE_SECOND_FORMATTER = createPeriodFormatter(new IntervalField.Minute(), new IntervalField.Second(Optional.empty()));
    private static final PeriodFormatter INTERVAL_MINUTE_FORMATTER = createPeriodFormatter(new IntervalField.Minute(), new IntervalField.Minute());

    private static final PeriodFormatter INTERVAL_SECOND_FORMATTER = createPeriodFormatter(new IntervalField.Second(Optional.empty()), new IntervalField.Second(Optional.empty()));

    private static final PeriodFormatter INTERVAL_YEAR_MONTH_FORMATTER = createPeriodFormatter(new IntervalField.Year(), new IntervalField.Month());
    private static final PeriodFormatter INTERVAL_YEAR_FORMATTER = createPeriodFormatter(new IntervalField.Year(), new IntervalField.Year());

    private static final PeriodFormatter INTERVAL_MONTH_FORMATTER = createPeriodFormatter(new IntervalField.Month(), new IntervalField.Month());

    public static long parseDayTimeInterval(String value, IntervalField startField, Optional<IntervalField> endField)
    {
        return parseDayTimeIntervalToPicos(value, startField, endField)[0];
    }

    /// Parses a day-time interval keeping picosecond resolution, returning `{micros, picosOfMicro}`.
    public static long[] parseDayTimeIntervalToPicos(String value, IntervalField startField, Optional<IntervalField> endField)
    {
        FormatterSpec spec = dayTimeFormatter(startField, endField);
        try {
            return parsePeriodToPicos(spec.formatter(), value, spec.fractionSeparators());
        }
        catch (IllegalArgumentException e) {
            throw invalidInterval(e, value, startField, endField.orElse(startField));
        }
    }

    private record FormatterSpec(PeriodFormatter formatter, int fractionSeparators) {}

    private static FormatterSpec dayTimeFormatter(IntervalField startField, Optional<IntervalField> endField)
    {
        if (startField instanceof IntervalField.Day && endField.isEmpty()) {
            return new FormatterSpec(INTERVAL_DAY_FORMATTER, -1);
        }
        if (startField instanceof IntervalField.Day && endField.get() instanceof IntervalField.Second) {
            return new FormatterSpec(INTERVAL_DAY_SECOND_FORMATTER, 3);
        }
        if (startField instanceof IntervalField.Day && endField.get() instanceof IntervalField.Minute) {
            return new FormatterSpec(INTERVAL_DAY_MINUTE_FORMATTER, -1);
        }
        if (startField instanceof IntervalField.Day && endField.get() instanceof IntervalField.Hour) {
            return new FormatterSpec(INTERVAL_DAY_HOUR_FORMATTER, -1);
        }

        if (startField instanceof IntervalField.Hour && endField.isEmpty()) {
            return new FormatterSpec(INTERVAL_HOUR_FORMATTER, -1);
        }
        if (startField instanceof IntervalField.Hour && endField.get() instanceof IntervalField.Second) {
            return new FormatterSpec(INTERVAL_HOUR_SECOND_FORMATTER, 2);
        }
        if (startField instanceof IntervalField.Hour && endField.get() instanceof IntervalField.Minute) {
            return new FormatterSpec(INTERVAL_HOUR_MINUTE_FORMATTER, -1);
        }

        if (startField instanceof IntervalField.Minute && endField.isEmpty()) {
            return new FormatterSpec(INTERVAL_MINUTE_FORMATTER, -1);
        }
        if (startField instanceof IntervalField.Minute && endField.get() instanceof IntervalField.Second) {
            return new FormatterSpec(INTERVAL_MINUTE_SECOND_FORMATTER, 1);
        }

        if (startField instanceof IntervalField.Second && endField.isEmpty()) {
            return new FormatterSpec(INTERVAL_SECOND_FORMATTER, 0);
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

        return new IntervalLiteral(value, sign, new CompositeIntervalQualifier(Optional.empty(), new IntervalField.Day(), new IntervalField.Second(Optional.empty())));
    }

    /// Parses a day-time interval to microseconds. `fractionSeparators` is the number of field
    /// separators (`:` or space) that must precede a fractional part for it to fall on the seconds
    /// field — e.g. 3 for `DAY TO SECOND` (`D H:M:S.f`), 0 for `SECOND` — or `-1` when the qualifier
    /// has no seconds field and so admits no fraction at all. A misplaced dot (`12.1` DAY TO SECOND)
    /// is rejected rather than silently treated as a seconds fraction.
    private static long[] parsePeriodToPicos(PeriodFormatter periodFormatter, String value, int fractionSeparators)
    {
        boolean negative = value.startsWith("-");
        if (negative) {
            value = value.substring(1);
        }

        // Joda Period only has millisecond resolution, so parse the fractional seconds (picosecond
        // precision) directly from the string and let Joda handle the whole-second fields.
        long fractionMicros = 0;
        int picosOfMicro = 0;
        int dot = value.indexOf('.');
        if (dot >= 0) {
            String integerPart = value.substring(0, dot);
            long separators = integerPart.chars().filter(character -> character == ':' || character == ' ').count();
            if (fractionSeparators < 0 || separators != fractionSeparators) {
                throw new IllegalArgumentException("Invalid fractional value in interval: " + value);
            }
            long[] fraction = parseFractionToPicos(value.substring(dot + 1));
            fractionMicros = fraction[0];
            picosOfMicro = (int) fraction[1];
            value = integerPart;
        }

        Period period = periodFormatter.parsePeriod(value);
        for (DurationFieldType type : period.getFieldTypes()) {
            checkArgument(period.get(type) >= 0, "Period field %s is negative", type);
        }

        long micros = IntervalDayTime.toMicros(
                period.getValue(DAY_FIELD),
                period.getValue(HOUR_FIELD),
                period.getValue(MINUTE_FIELD),
                period.getValue(SECOND_FIELD),
                fractionMicros);
        if (!negative) {
            return new long[] {micros, picosOfMicro};
        }
        // negate the (micros, picosOfMicro) pair, where picosOfMicro is a positive increment
        if (picosOfMicro > 0) {
            return new long[] {-(micros + 1), PICOS_PER_MICRO - picosOfMicro};
        }
        return new long[] {-micros, 0};
    }

    /// Parses up to twelve fractional-second digits into `{microsecondFraction, picosOfMicro}` (each a
    /// six-digit count), truncating any digits beyond picosecond precision.
    private static long[] parseFractionToPicos(String fraction)
    {
        StringBuilder padded = new StringBuilder(fraction);
        while (padded.length() < 12) {
            padded.append('0');
        }
        long microsFraction = Long.parseLong(padded.substring(0, 6));
        long picosOfMicro = Long.parseLong(padded.substring(6, 12));
        return new long[] {microsFraction, picosOfMicro};
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

    public static long parseDayTimeInterval(String value, io.trino.spi.type.IntervalField startField, io.trino.spi.type.IntervalField endField)
    {
        return parseDayTimeInterval(value, toAstField(startField), startField == endField ? Optional.empty() : Optional.of(toAstField(endField)));
    }

    public static long[] parseDayTimeIntervalToPicos(String value, io.trino.spi.type.IntervalField startField, io.trino.spi.type.IntervalField endField)
    {
        return parseDayTimeIntervalToPicos(value, toAstField(startField), startField == endField ? Optional.empty() : Optional.of(toAstField(endField)));
    }

    public static long parseYearMonthInterval(String value, io.trino.spi.type.IntervalField startField, io.trino.spi.type.IntervalField endField)
    {
        return parseYearMonthInterval(value, toAstField(startField), startField == endField ? Optional.empty() : Optional.of(toAstField(endField)));
    }

    private static IntervalField toAstField(io.trino.spi.type.IntervalField field)
    {
        return switch (field) {
            case YEAR -> new IntervalField.Year();
            case MONTH -> new IntervalField.Month();
            case DAY -> new IntervalField.Day();
            case HOUR -> new IntervalField.Hour();
            case MINUTE -> new IntervalField.Minute();
            case SECOND -> new IntervalField.Second(Optional.empty());
        };
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
