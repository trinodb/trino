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
package io.trino.json;

import io.trino.json.JsonDateTimeTemplate.FieldSegment;
import io.trino.json.JsonDateTimeTemplate.LiteralSegment;
import io.trino.json.JsonDateTimeTemplate.Segment;
import io.trino.json.ir.TypedValue;
import io.trino.spi.type.DateType;
import io.trino.spi.type.LongTimeWithTimeZone;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.OptionalInt;

import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.packTimeWithTimeZone;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_SECOND;
import static io.trino.type.DateTimes.longTimestamp;
import static io.trino.type.DateTimes.rescale;
import static java.lang.Character.isDigit;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.multiplyExact;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/// Applies a parsed [JsonDateTimeTemplate] to a value string and produces a
/// [TypedValue] of the template's inferred type.
final class JsonDateTimeValueParser
{
    private JsonDateTimeValueParser() {}

    public static TypedValue parse(List<Segment> segments, Type type, String value)
    {
        Cursor cursor = new Cursor(value);
        ParsedValues parsedValues = new ParsedValues();
        for (Segment segment : segments) {
            switch (segment) {
                case LiteralSegment literal -> cursor.expect(literal.literal());
                case FieldSegment field -> parseField(field, cursor, parsedValues);
            }
        }

        if (!cursor.atEnd()) {
            throw new IllegalArgumentException("unexpected trailing input");
        }

        return buildTypedValue(type, parsedValues);
    }

    private static TypedValue buildTypedValue(Type type, ParsedValues parsedValues)
    {
        // Per SQL:2023 §9.46, default values for unspecified fields are fixed constants, not
        // session-dependent values. Using session.getStart() would make the same query yield
        // different results depending on when it's evaluated.
        int year = parsedValues.resolveYear();
        if (parsedValues.dayOfYear.isPresent()) {
            LocalDate date = LocalDate.ofYearDay(year, parsedValues.dayOfYear.getAsInt());
            year = date.getYear();
            parsedValues.month = OptionalInt.of(date.getMonthValue());
            parsedValues.day = OptionalInt.of(date.getDayOfMonth());
        }

        int month = parsedValues.month.orElse(1);
        int day = parsedValues.day.orElse(1);
        int hour = parsedValues.resolveHour();
        int minute = parsedValues.resolveMinute();
        int second = parsedValues.resolveSecond();
        long fractionPicos = parsedValues.fractionPicos();

        return switch (type) {
            case DateType _ -> new TypedValue(type, (long) toIntExact(LocalDate.of(year, month, day).toEpochDay()));
            case TimeType timeType -> new TypedValue(timeType, timeOfDayPicos(hour, minute, second, fractionPicos));
            case TimeWithTimeZoneType timeWithTimeZoneType -> {
                int offsetMinutes = parsedValues.resolveOffsetMinutes();
                long picos = timeOfDayPicos(hour, minute, second, fractionPicos);
                if (timeWithTimeZoneType.getPrecision() <= TimeWithTimeZoneType.MAX_SHORT_PRECISION) {
                    long nanos = picos / PICOSECONDS_PER_NANOSECOND;
                    yield new TypedValue(timeWithTimeZoneType, packTimeWithTimeZone(nanos, offsetMinutes));
                }
                yield TypedValue.fromValueAsObject(timeWithTimeZoneType, new LongTimeWithTimeZone(picos, offsetMinutes));
            }
            case TimestampType timestampType -> {
                long epochSecond = LocalDateTime.of(year, month, day, hour, minute, second).toEpochSecond(ZoneOffset.UTC);
                if (timestampType.getPrecision() <= TimestampType.MAX_SHORT_PRECISION) {
                    long epochMicros = multiplyExact(epochSecond, MICROSECONDS_PER_SECOND) + fractionPicos / PICOSECONDS_PER_MICROSECOND;
                    yield new TypedValue(timestampType, epochMicros);
                }
                yield TypedValue.fromValueAsObject(timestampType, longTimestamp(epochSecond, fractionPicos));
            }
            case TimestampWithTimeZoneType timestampWithTimeZoneType -> {
                int offsetMinutes = parsedValues.resolveOffsetMinutes();
                ZoneOffset zoneOffset = ZoneOffset.ofTotalSeconds(offsetMinutes * 60);
                long epochSecond = LocalDateTime.of(year, month, day, hour, minute, second).toEpochSecond(zoneOffset);
                if (timestampWithTimeZoneType.getPrecision() <= TimestampWithTimeZoneType.MAX_SHORT_PRECISION) {
                    long epochMillis = epochSecond * MILLISECONDS_PER_SECOND + fractionPicos / PICOSECONDS_PER_MILLISECOND;
                    yield new TypedValue(timestampWithTimeZoneType, packDateTimeWithZone(epochMillis, zoneOffset.getId()));
                }
                yield TypedValue.fromValueAsObject(
                        timestampWithTimeZoneType,
                        LongTimestampWithTimeZone.fromEpochSecondsAndFraction(epochSecond, fractionPicos, getTimeZoneKey(zoneOffset.getId())));
            }
            default -> throw new IllegalStateException("unexpected datetime() template type: " + type);
        };
    }

    private static long timeOfDayPicos(int hour, int minute, int second, long fractionPicos)
    {
        return (((hour * 60L) + minute) * 60 + second) * PICOSECONDS_PER_SECOND + fractionPicos;
    }

    private static void parseField(FieldSegment segment, Cursor cursor, ParsedValues values)
    {
        switch (segment.field()) {
            case YEAR -> {
                values.year = OptionalInt.of(readUnsignedNumber(cursor, segment));
                values.yearDigits = cursor.previousDigits().length();
            }
            case ROUNDED_YEAR -> {
                values.roundedYear = OptionalInt.of(readUnsignedNumber(cursor, segment));
                values.roundedYearDigits = cursor.previousDigits().length();
            }
            case MONTH -> values.month = OptionalInt.of(readUnsignedNumber(cursor, segment));
            case DAY -> values.day = OptionalInt.of(readUnsignedNumber(cursor, segment));
            case DAY_OF_YEAR -> values.dayOfYear = OptionalInt.of(readUnsignedNumber(cursor, segment));
            case HOUR12 -> {
                // Bounds-check at parse time. `resolveHour` does `hour12 % 12`, so an
                // out-of-range value (e.g. 13 A.M., 25 P.M.) would fold silently into a
                // valid-looking hour and never reach `parseTime`'s validator.
                int hour12 = readUnsignedNumber(cursor, segment);
                if (hour12 < 1 || hour12 > 12) {
                    throw new IllegalArgumentException("hour-of-half-day value out of range [1, 12]: " + hour12);
                }
                values.hour12 = OptionalInt.of(hour12);
            }
            case HOUR24 -> values.hour24 = OptionalInt.of(readUnsignedNumber(cursor, segment));
            case MINUTE -> values.minute = OptionalInt.of(readUnsignedNumber(cursor, segment));
            case SECOND -> values.second = OptionalInt.of(readUnsignedNumber(cursor, segment));
            case SECOND_OF_DAY -> {
                // Bounds-check here, not downstream. SECOND_OF_DAY is decomposed by resolveHour /
                // resolveMinute / resolveSecond (floorDiv / floorMod against 3600 / 60), so an
                // out-of-range input like 99999 would synthesize a superficially-valid time
                // (e.g. 27:46:39) and fail downstream with a message that doesn't mention the
                // template field or its range.
                int secondOfDay = readUnsignedNumber(cursor, segment);
                if (secondOfDay < 0 || secondOfDay > 86399) {
                    throw new IllegalArgumentException("second-of-day value out of range [0, 86399]: " + secondOfDay);
                }
                values.secondOfDay = OptionalInt.of(secondOfDay);
            }
            case FRACTION -> {
                values.fractionValue = readUnsignedLongNumber(cursor, segment);
                values.fractionDigits = cursor.previousDigits().length();
            }
            case AM_PM -> values.pm = readAmPm(cursor);
            case TIME_ZONE_HOUR -> readTimeZoneHour(cursor, segment, values);
            case TIME_ZONE_MINUTE -> {
                // Bounds-check at parse time. `resolveOffsetMinutes` does `offsetMinute + offsetHour * 60`,
                // so a value like 99 would carry into the hours (e.g. `+05:99` → `+06:39`) and
                // would never raise an error for clearly-invalid input.
                int timeZoneMinute = readUnsignedNumber(cursor, segment);
                if (timeZoneMinute < 0 || timeZoneMinute > 59) {
                    throw new IllegalArgumentException("time-zone-minute value out of range [0, 59]: " + timeZoneMinute);
                }
                values.timeZoneMinute = OptionalInt.of(timeZoneMinute);
            }
        }
    }

    private static int readUnsignedNumber(Cursor cursor, FieldSegment segment)
    {
        return Integer.parseInt(cursor.readDigits(minimumDigits(segment), segment.width()));
    }

    // FRACTION values at FF10..FF12 precision don't fit in int; use a long variant.
    private static long readUnsignedLongNumber(Cursor cursor, FieldSegment segment)
    {
        return Long.parseLong(cursor.readDigits(minimumDigits(segment), segment.width()));
    }

    private static int minimumDigits(FieldSegment segment)
    {
        // Delimited fields tolerate variable widths; un-delimited fields must consume
        // exactly their declared width to avoid ambiguity with the next field.
        return segment.delimited() ? 1 : segment.width();
    }

    private static boolean readAmPm(Cursor cursor)
    {
        if (cursor.regionMatchesIgnoreCase("A.M.")) {
            cursor.advance("A.M.".length());
            return false;
        }
        if (cursor.regionMatchesIgnoreCase("P.M.")) {
            cursor.advance("P.M.".length());
            return true;
        }
        throw new IllegalArgumentException(format("invalid AM/PM marker at position %d", cursor.position()));
    }

    private static void readTimeZoneHour(Cursor cursor, FieldSegment segment, ParsedValues values)
    {
        if (cursor.atEnd()) {
            throw new IllegalArgumentException(format("expected time zone hour at position %d", cursor.position()));
        }

        char signCharacter = cursor.current();
        if (signCharacter != '+' && signCharacter != '-' && signCharacter != ' ') {
            throw new IllegalArgumentException(format("time zone hour must start with '+', '-' or space at position %d", cursor.position()));
        }
        cursor.advance(1);

        String hourDigits = segment.delimited() ? cursor.readDigits(1, 2) : cursor.readFixedDigits(2);
        // Track sign as a separate flag so offsets like `-00:30` (where the signed hour
        // would round to 0) preserve the negative bit through resolveOffsetMinutes.
        values.timeZoneHour = OptionalInt.of(Integer.parseInt(hourDigits));
        values.timeZoneOffsetNegative = signCharacter == '-';
    }

    private static final class ParsedValues
    {
        private OptionalInt year = OptionalInt.empty();
        private int yearDigits;
        private OptionalInt roundedYear = OptionalInt.empty();
        private int roundedYearDigits;
        private OptionalInt month = OptionalInt.empty();
        private OptionalInt day = OptionalInt.empty();
        private OptionalInt dayOfYear = OptionalInt.empty();
        private OptionalInt hour12 = OptionalInt.empty();
        private OptionalInt hour24 = OptionalInt.empty();
        private OptionalInt minute = OptionalInt.empty();
        private OptionalInt second = OptionalInt.empty();
        private OptionalInt secondOfDay = OptionalInt.empty();
        private boolean pm;
        private long fractionValue;
        private int fractionDigits;
        private OptionalInt timeZoneHour = OptionalInt.empty();
        private OptionalInt timeZoneMinute = OptionalInt.empty();
        private boolean timeZoneOffsetNegative;

        // Fixed reference year (1970, per SQL:2023's use of epoch-based defaults) used to fill in the
        // century/millennium prefix when the template specifies only low-order year digits.
        private static final int REFERENCE_YEAR = 1970;

        public int resolveYear()
        {
            if (year.isPresent()) {
                return prefixYear(REFERENCE_YEAR, year.getAsInt(), yearDigits);
            }
            if (roundedYear.isPresent()) {
                return roundYear(REFERENCE_YEAR, roundedYear.getAsInt(), roundedYearDigits);
            }
            return REFERENCE_YEAR;
        }

        public int resolveHour()
        {
            if (secondOfDay.isPresent()) {
                return floorDiv(secondOfDay.getAsInt(), 3600);
            }
            if (hour24.isPresent()) {
                return hour24.getAsInt();
            }
            if (hour12.isPresent()) {
                int hour = hour12.getAsInt() % 12;
                if (pm) {
                    hour += 12;
                }
                return hour;
            }
            return 0;
        }

        public int resolveMinute()
        {
            if (secondOfDay.isPresent()) {
                return floorDiv(floorMod(secondOfDay.getAsInt(), 3600), 60);
            }
            return minute.orElse(0);
        }

        public int resolveSecond()
        {
            if (secondOfDay.isPresent()) {
                return floorMod(secondOfDay.getAsInt(), 60);
            }
            return second.orElse(0);
        }

        public long fractionPicos()
        {
            return fractionDigits > 0 ? rescale(fractionValue, fractionDigits, 12) : 0L;
        }

        public int resolveOffsetMinutes()
        {
            if (!timeZoneHour.isPresent()) {
                return 0;
            }
            int magnitude = timeZoneHour.getAsInt() * 60 + timeZoneMinute.orElse(0);
            return timeZoneOffsetNegative ? -magnitude : magnitude;
        }

        private static int prefixYear(int currentYear, int value, int digits)
        {
            if (digits >= 4) {
                return value;
            }
            int prefix = floorDiv(currentYear, pow10(digits));
            return prefix * pow10(digits) + value;
        }

        private static int roundYear(int currentYear, int value, int digits)
        {
            if (digits > 2) {
                return value;
            }

            int currentCentury = floorDiv(currentYear, 100) * 100;
            int currentTwoDigits = floorMod(currentYear, 100);
            if (currentTwoDigits <= 49) {
                return value <= 49 ? currentCentury + value : currentCentury - 100 + value;
            }
            return value <= 49 ? currentCentury + 100 + value : currentCentury + value;
        }

        private static int pow10(int exponent)
        {
            int value = 1;
            for (int index = 0; index < exponent; index++) {
                value *= 10;
            }
            return value;
        }
    }

    private static final class Cursor
    {
        private final String value;
        private int position;
        private String previousDigits = "";

        private Cursor(String value)
        {
            this.value = requireNonNull(value, "value is null");
        }

        public boolean atEnd()
        {
            return position == value.length();
        }

        public char current()
        {
            return value.charAt(position);
        }

        public int position()
        {
            return position;
        }

        public void expect(String literal)
        {
            if (!value.startsWith(literal, position)) {
                throw new IllegalArgumentException(format("expected literal '%s' at position %d", literal, position));
            }
            position += literal.length();
        }

        public boolean regionMatchesIgnoreCase(String text)
        {
            return value.regionMatches(true, position, text, 0, text.length());
        }

        public void advance(int length)
        {
            position += length;
        }

        public String readDigits(int minimumDigits, int maximumDigits)
        {
            int start = position;
            int count = 0;
            while (position < value.length() && isDigit(value.charAt(position)) && count < maximumDigits) {
                position++;
                count++;
            }
            if (count < minimumDigits) {
                throw new IllegalArgumentException(format("expected at least %d digits at position %d", minimumDigits, start));
            }
            previousDigits = value.substring(start, position);
            return previousDigits;
        }

        public String readFixedDigits(int digits)
        {
            int start = position;
            String result = readDigits(digits, digits);
            if (result.length() != digits) {
                throw new IllegalArgumentException(format("expected %d digits at position %d", digits, start));
            }
            return result;
        }

        public String previousDigits()
        {
            return previousDigits;
        }
    }
}
