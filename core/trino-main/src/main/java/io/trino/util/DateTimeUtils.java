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
import io.trino.sql.tree.CompositeIntervalQualifier;
import io.trino.sql.tree.IntervalField;
import io.trino.sql.tree.IntervalLiteral;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.util.Locale;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.StandardErrorCode.INVALID_LITERAL;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.sql.tree.IntervalLiteral.Sign.NEGATIVE;
import static io.trino.sql.tree.IntervalLiteral.Sign.POSITIVE;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.YEAR;

public final class DateTimeUtils
{
    private DateTimeUtils() {}

    private static final Pattern DATE_PATTERN = Pattern.compile("([+-]?[0-9]{1,9})-([0-9]{1,2})-([0-9]{1,2})");
    private static final DateTimeFormatter DATE_PRINTER = new DateTimeFormatterBuilder()
            .appendValue(YEAR, 4, 9, SignStyle.NORMAL)
            .appendLiteral('-')
            .appendValue(MONTH_OF_YEAR, 2)
            .appendLiteral('-')
            .appendValue(DAY_OF_MONTH, 2)
            .toFormatter(Locale.ENGLISH);

    public static int parseDate(Slice value)
    {
        // Note: update DomainTranslator.Visitor.createVarcharCastToDateComparisonExtractionResult whenever varchar->date conversion (CAST) behavior changes.

        // in order to follow the standard, we should validate the value:
        // - the required format is 'YYYY-MM-DD'
        // - all components should be unsigned numbers
        // https://github.com/trinodb/trino/issues/10677

        OptionalInt days = parseIfIso8601DateFormat(value);
        if (days.isPresent()) {
            return days.orElseThrow();
        }
        // Preserve the variable-width and signed years accepted by the legacy date parser.
        String date = value.toStringUtf8();
        Matcher matcher = DATE_PATTERN.matcher(date);
        checkArgument(matcher.matches(), "Invalid date '%s'", date);
        return toIntExact(LocalDate.of(
                Integer.parseInt(matcher.group(1)),
                Integer.parseInt(matcher.group(2)),
                Integer.parseInt(matcher.group(3))).toEpochDay());
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

        int parsedYear = year.orElseThrow();
        int parsedMonth = month.orElseThrow();
        int parsedDay = day.orElseThrow();
        if (parsedMonth < 1 || parsedMonth > 12 || parsedDay < 1 || parsedDay > FastDate.daysInMonth(parsedYear, parsedMonth)) {
            throw new DateTimeException("Invalid date '" + value.toStringUtf8() + "'");
        }
        return OptionalInt.of(FastDate.daysFromYmd(parsedYear, parsedMonth, parsedDay));
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

    public static Slice printDate(int days)
    {
        long ymd = FastDate.ymdFromEpochDay(days);
        int year = (int) (ymd >> 32);
        if (year < 0 || year > 9999) {
            return utf8Slice(DATE_PRINTER.format(LocalDate.ofEpochDay(days)));
        }
        int month = (int) ((ymd >> 8) & 0xFF);
        int day = (int) (ymd & 0xFF);
        byte[] buffer = new byte[10];
        buffer[0] = (byte) ('0' + year / 1000);
        buffer[1] = (byte) ('0' + (year / 100) % 10);
        buffer[2] = (byte) ('0' + (year / 10) % 10);
        buffer[3] = (byte) ('0' + year % 10);
        buffer[4] = '-';
        buffer[5] = (byte) ('0' + month / 10);
        buffer[6] = (byte) ('0' + month % 10);
        buffer[7] = '-';
        buffer[8] = (byte) ('0' + day / 10);
        buffer[9] = (byte) ('0' + day % 10);
        return wrappedBuffer(buffer);
    }

    private static final int PICOS_PER_MICRO = 1_000_000;

    private static long[] parseDayTimeIntervalToPicos(String value, IntervalField startField, Optional<IntervalField> endField, int fractionalPrecision)
    {
        int firstField = dayTimeFieldIndex(startField);
        int lastField = dayTimeFieldIndex(endField.orElse(startField));
        if (firstField < 0 || lastField < firstField) {
            throw invalidQualifier(startField, endField.orElse(startField));
        }
        try {
            return parsePeriodToPicos(value, firstField, lastField, fractionalPrecision);
        }
        catch (IllegalArgumentException e) {
            if (e.getCause() instanceof ArithmeticException) {
                throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Value out of range for an interval", e);
            }
            throw invalidInterval(e, value, startField, endField.orElse(startField));
        }
    }

    private static int dayTimeFieldIndex(IntervalField field)
    {
        return switch (field) {
            case IntervalField.Day _ -> 0;
            case IntervalField.Hour _ -> 1;
            case IntervalField.Minute _ -> 2;
            case IntervalField.Second _ -> 3;
            default -> -1;
        };
    }

    public static IntervalLiteral formatDayTimeInterval(Duration duration)
    {
        long millis = duration.toMillis();
        IntervalLiteral.Sign sign = millis < 0 ? NEGATIVE : POSITIVE;
        // Take each component's magnitude separately, since abs(Long.MIN_VALUE) overflows.
        int days = toIntExact(Math.abs(millis / TimeUnit.DAYS.toMillis(1)));
        long hours = Math.abs(millis / TimeUnit.HOURS.toMillis(1) % 24);
        long minutes = Math.abs(millis / TimeUnit.MINUTES.toMillis(1) % 60);
        long seconds = Math.abs(millis / TimeUnit.SECONDS.toMillis(1) % 60);
        int fraction = (int) Math.abs(millis % 1_000);
        String value = format(Locale.ENGLISH, "%d %d:%02d:%02d", days, hours, minutes, seconds);
        if (fraction != 0) {
            value += format(Locale.ENGLISH, ".%03d", fraction);
        }
        // Always use DAY TO SECOND, retaining zero-valued trailing fields.
        return new IntervalLiteral(value, sign, new CompositeIntervalQualifier(Optional.empty(), new IntervalField.Day(), new IntervalField.Second(Optional.empty())));
    }

    private static long[] parsePeriodToPicos(String value, int firstField, int lastField, int fractionalPrecision)
    {
        boolean negative = value.startsWith("-");
        if (negative) {
            value = value.substring(1);
        }

        long fractionMicros = 0;
        int picosOfMicro = 0;
        int dot = value.indexOf('.');
        if (dot >= 0) {
            String integerPart = value.substring(0, dot);
            long separators = integerPart.chars().filter(character -> character == ':' || character == ' ').count();
            if (lastField != 3 || separators != lastField - firstField) {
                throw new IllegalArgumentException("Invalid fractional value in interval: " + value);
            }
            long[] fraction = parseFractionToPicos(value.substring(dot + 1), fractionalPrecision, negative);
            fractionMicros = fraction[0];
            picosOfMicro = (int) fraction[1];
            value = integerPart;
        }

        char firstSeparator = firstField == 0 ? ' ' : ':';
        long[] parsed = parseFields(value, lastField - firstField + 1, firstSeparator, true, lastField == 3);
        long[] fields = new long[4];
        System.arraycopy(parsed, 0, fields, firstField, parsed.length);
        if (negative) {
            // Accumulate negative fields directly, since the magnitude of Long.MIN_VALUE is not a long.
            fractionMicros = -fractionMicros;
            if (picosOfMicro > 0) {
                fractionMicros--;
                picosOfMicro = PICOS_PER_MICRO - picosOfMicro;
            }
        }
        long sign = negative ? -1 : 1;
        long micros = IntervalDayTime.toMicros(sign * fields[0], sign * fields[1], sign * fields[2], sign * fields[3], fractionMicros);
        return new long[] {micros, picosOfMicro};
    }

    /**
     * Parses a prefix of the qualified fields, leaving omitted trailing fields zero.
     * The leading day-time field can be a long; other fields retain the legacy int range.
     */
    private static long[] parseFields(String value, int fieldCount, char firstSeparator, boolean longLeadingField, boolean secondsField)
    {
        long[] fields = new long[fieldCount];
        int position = 0;
        int leadingEnd = 0;
        for (int field = 0; field < fieldCount; field++) {
            if (position == value.length()) {
                // A consumed separator may be followed by an omitted final field.
                checkArgument(field != 0 || !longLeadingField, "Missing interval leading field");
                break;
            }
            int start = position;
            boolean negativeField = value.charAt(position) == '-';
            boolean positiveField = value.charAt(position) == '+';
            // The legacy parser skips a leading '+' before expanding its numeric limit.
            int maxDigits = positiveField ? 11 : 10;
            if (value.charAt(position) == '+' || negativeField) {
                position++;
                checkArgument(position < value.length() && value.charAt(position) >= '0' && value.charAt(position) <= '9', "Missing interval field");
            }
            int digits = position;
            while (position < value.length() && value.charAt(position) >= '0' && value.charAt(position) <= '9') {
                position++;
            }
            int integerEnd = position;
            int fractionDigits = 0;
            if (secondsField && field + 1 == fieldCount && position < value.length() && value.charAt(position) == ',') {
                // The legacy seconds parser accepted a decimal comma, but its millisecond
                // component was not included in the interval value. Preserve that syntax.
                checkArgument((field == 0 && longLeadingField) || integerEnd - digits < maxDigits, "Interval field exceeds numeric limit");
                int fractionStart = ++position;
                while (position < value.length() && value.charAt(position) >= '0' && value.charAt(position) <= '9') {
                    position++;
                }
                fractionDigits = position - fractionStart;
                if (negativeField) {
                    for (int index = fractionStart; index < Math.min(position, fractionStart + 3); index++) {
                        checkArgument(value.charAt(index) == '0', "Interval fraction is negative");
                    }
                }
            }
            checkArgument(integerEnd > digits || fractionDigits > 0, "Missing interval field");
            if (field == 0 && longLeadingField) {
                checkArgument(integerEnd > digits, "Missing interval leading field");
                // Its integer part was replaced by a single zero before legacy validation.
                checkArgument(fractionDigits <= 9, "Interval fraction exceeds nine digits");
                leadingEnd = integerEnd;
            }
            else {
                checkArgument(integerEnd - digits + fractionDigits <= maxDigits, "Interval field exceeds numeric limit");
                if (integerEnd > digits) {
                    int parsed = Integer.parseInt(value.substring(start, integerEnd));
                    checkArgument(parsed >= 0, "Interval field is negative");
                    fields[field] = parsed;
                }
            }
            if (position == value.length()) {
                break;
            }
            checkArgument(field + 1 < fieldCount, "Unexpected trailing interval field");
            char separator = field == 0 ? firstSeparator : ':';
            checkArgument(value.charAt(position) == separator, "Invalid interval separator");
            position++;
        }
        if (longLeadingField) {
            // Validate the trailing syntax first, as for the legacy parser, so malformed
            // input cannot be classified as numeric overflow merely because its prefix is large.
            try {
                fields[0] = Long.parseLong(value.substring(0, leadingEnd));
            }
            catch (NumberFormatException e) {
                throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Value out of range for an interval", e);
            }
            checkArgument(fields[0] >= 0, "Interval field is negative");
        }
        return fields;
    }

    /// Rounds the complete input once to the requested precision, with ties toward positive infinity.
    /// The microsecond component can include a carry into the next second.
    private static long[] parseFractionToPicos(String fraction, int precision, boolean negative)
    {
        checkArgument(precision >= 0 && precision <= 12, "Invalid fractional precision: %s", precision);
        checkArgument(fraction.chars().allMatch(character -> character >= '0' && character <= '9'), "Invalid interval fraction: %s", fraction);
        long units = 0;
        for (int i = 0; i < precision; i++) {
            units = units * 10 + (i < fraction.length() ? fraction.charAt(i) - '0' : 0);
        }
        if (fraction.length() > precision) {
            int next = fraction.charAt(precision) - '0';
            if (next > 5 || (next == 5 && (!negative || fraction.substring(precision + 1).chars().anyMatch(character -> character != '0')))) {
                units++;
            }
        }
        long picos = units;
        for (int i = precision; i < 12; i++) {
            picos *= 10;
        }
        return new long[] {picos / PICOS_PER_MICRO, picos % PICOS_PER_MICRO};
    }

    public static long parseYearMonthInterval(String value, IntervalField startField, Optional<IntervalField> endField)
    {
        try {
            if (startField instanceof IntervalField.Year && (endField.isEmpty() || endField.orElseThrow() instanceof IntervalField.Month)) {
                boolean negative = value.startsWith("-");
                String magnitude = value;
                if (negative) {
                    magnitude = value.substring(1);
                }
                long[] fields = parseFields(magnitude, endField.isPresent() ? 2 : 1, '-', false, false);
                int sign = negative ? -1 : 1;
                int months = 0;
                if (fields.length == 2) {
                    months = (int) fields[1];
                }
                return IntervalYearMonth.toMonths(sign * (int) fields[0], sign * months);
            }
            if (startField instanceof IntervalField.Month && endField.isEmpty()) {
                return Integer.parseInt(value);
            }
        }
        catch (IllegalArgumentException e) {
            throw invalidInterval(e, value, startField, endField.orElse(startField));
        }
        throw invalidQualifier(startField, endField.orElse(startField));
    }

    public static long[] parseDayTimeIntervalToPicos(String value, io.trino.spi.type.IntervalField startField, io.trino.spi.type.IntervalField endField, int fractionalPrecision)
    {
        return parseDayTimeIntervalToPicos(value, startField, endField, fractionalPrecision, 1);
    }

    /// Applies the literal's outer sign before rounding, while validating the original string sign.
    public static long[] parseDayTimeIntervalToPicos(String value, io.trino.spi.type.IntervalField startField, io.trino.spi.type.IntervalField endField, int fractionalPrecision, int sign)
    {
        if (value.length() > 1 && (value.charAt(0) == '+' || value.charAt(0) == '-') && (value.charAt(1) == '+' || value.charAt(1) == '-')) {
            IntervalField start = toAstField(startField);
            throw invalidInterval(new IllegalArgumentException("Invalid interval sign"), value, start, startField == endField ? start : toAstField(endField));
        }
        if (sign < 0) {
            value = value.startsWith("-") ? value.substring(1) : "-" + (value.startsWith("+") ? value.substring(1) : value);
        }
        return parseDayTimeIntervalToPicos(value, toAstField(startField), startField == endField ? Optional.empty() : Optional.of(toAstField(endField)), fractionalPrecision);
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
}
