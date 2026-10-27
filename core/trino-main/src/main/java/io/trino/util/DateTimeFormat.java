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

import io.airlift.concurrent.ThreadLocalCache;
import io.trino.spi.type.TimeZoneNotSupportedException;

import java.math.BigDecimal;
import java.time.DateTimeException;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.chrono.IsoEra;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.TextStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalField;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;

/**
 * The SQL date/time format syntaxes, evaluated with the JDK's calendar and time zone rules.
 * Numeric widths describe printing; parsing also accepts shorter values, as the SQL functions
 * historically have. Parsed fields are applied from largest to smallest, with later occurrences
 * of the same field taking precedence.
 */
public final class DateTimeFormat
{
    private static final ThreadLocalCache<String, DateTimeFormat> PATTERN_FORMATTERS = new ThreadLocalCache<>(100, DateTimeFormat::createPattern);
    private static final LocalDateTime EPOCH = LocalDateTime.of(1970, 1, 1, 0, 0);
    private static final Map<String, String> SHORT_ZONE_NAMES = Map.ofEntries(
            Map.entry("UT", "UTC"),
            Map.entry("UTC", "UTC"),
            Map.entry("GMT", "UTC"),
            Map.entry("EST", "America/New_York"),
            Map.entry("EDT", "America/New_York"),
            Map.entry("CST", "America/Chicago"),
            Map.entry("CDT", "America/Chicago"),
            Map.entry("MST", "America/Denver"),
            Map.entry("MDT", "America/Denver"),
            Map.entry("PST", "America/Los_Angeles"),
            Map.entry("PDT", "America/Los_Angeles"));
    private static final List<String> ZONE_IDS = ZoneId.getAvailableZoneIds().stream()
            .sorted(Comparator.comparingInt(String::length).reversed())
            .toList();
    private static final Pattern OFFSET = Pattern.compile("([+-])(\\d{2})(?::?(\\d{2}))?(?::?(\\d{2})(?:[.,](\\d{1,3}))?)?");
    private static final Pattern ISO_DATE = Pattern.compile("([+-]?\\d{1,9})(?:-(?:(\\d{1,2})(?:-(\\d{1,2}))?|(\\d{3})|[Ww](\\d{1,2})(?:-(\\d))?))?");
    private static final Pattern ISO_TIME = Pattern.compile("(\\d{1,2})(?::(\\d{1,2})(?::(\\d{1,2}))?)?(?:[.,](\\d{1,9}))?");

    private final List<Token> tokens;
    private final boolean parserSupported;

    private DateTimeFormat(List<Token> tokens)
    {
        this.tokens = List.copyOf(tokens);
        parserSupported = tokens.stream().noneMatch(token -> token.field() == Field.LONG_ZONE_NAME);
    }

    public static DateTimeFormat forPattern(String pattern)
    {
        if (pattern == null || pattern.isEmpty()) {
            throw new IllegalArgumentException("Invalid pattern specification");
        }
        return PATTERN_FORMATTERS.get(pattern);
    }

    private static DateTimeFormat createPattern(String pattern)
    {
        List<Token> tokens = new ArrayList<>();
        StringBuilder literal = new StringBuilder();
        boolean quoted = false;
        for (int index = 0; index < pattern.length(); ) {
            char character = pattern.charAt(index);
            if (character == '\'') {
                if (index + 1 < pattern.length() && pattern.charAt(index + 1) == '\'') {
                    literal.append('\'');
                    index += 2;
                }
                else {
                    quoted = !quoted;
                    index++;
                }
                continue;
            }
            if (quoted || !isPatternLetter(character)) {
                literal.append(character);
                index++;
                continue;
            }
            appendLiteral(tokens, literal);
            int end = index + 1;
            while (end < pattern.length() && pattern.charAt(end) == character) {
                end++;
            }
            int width = end - index;
            boolean nextNumeric = nextTokenIsNumeric(pattern, end);
            Field field = switch (character) {
                case 'G' -> Field.ERA;
                case 'C' -> Field.CENTURY;
                case 'y' -> Field.YEAR;
                case 'Y' -> Field.YEAR_OF_ERA;
                case 'x' -> Field.WEEK_YEAR;
                case 'M' -> Field.MONTH;
                case 'w' -> Field.WEEK;
                case 'D' -> Field.DAY_OF_YEAR;
                case 'd' -> Field.DAY_OF_MONTH;
                case 'e', 'E' -> Field.DAY_OF_WEEK;
                case 'a' -> Field.HALFDAY;
                case 'H' -> Field.HOUR;
                case 'k' -> Field.CLOCK_HOUR;
                case 'K' -> Field.HALFDAY_HOUR;
                case 'h' -> Field.CLOCK_HALFDAY_HOUR;
                case 'm' -> Field.MINUTE;
                case 's' -> Field.SECOND;
                case 'S' -> Field.FRACTION;
                case 'z' -> width < 4 ? Field.SHORT_ZONE_NAME : Field.LONG_ZONE_NAME;
                case 'Z' -> width >= 3 ? Field.ZONE_ID : Field.OFFSET;
                default -> throw new IllegalArgumentException("Illegal pattern component: " + pattern.substring(index, end));
            };
            boolean text = character == 'E' || character == 'a' || character == 'G' || (character == 'M' && width >= 3);
            int maxDigits = 2;
            if (field == Field.YEAR || field == Field.YEAR_OF_ERA || field == Field.WEEK_YEAR) {
                maxDigits = nextNumeric ? width : 9;
            }
            else if (field == Field.CENTURY) {
                maxDigits = width;
            }
            else if (field == Field.DAY_OF_YEAR) {
                maxDigits = 3;
            }
            else if (field == Field.FRACTION) {
                maxDigits = width;
            }
            int pivot = LocalDate.now().getYear() - 30;
            if (field == Field.WEEK_YEAR) {
                pivot = LocalDate.now().get(IsoFields.WEEK_BASED_YEAR) - 30;
            }
            if (width == 2 && (field == Field.YEAR || field == Field.YEAR_OF_ERA)) {
                field = Field.YEAR;
            }
            tokens.add(new Token(field, width, maxDigits, text, width == 2 && isYear(field), !nextNumeric, pivot, ""));
            index = end;
        }
        appendLiteral(tokens, literal);
        return new DateTimeFormat(tokens);
    }

    public static DateTimeFormat forMysqlPattern(String pattern)
    {
        List<Token> tokens = new ArrayList<>();
        StringBuilder literal = new StringBuilder();
        boolean escaped = false;
        for (int index = 0; index < pattern.length(); index++) {
            char character = pattern.charAt(index);
            if (!escaped) {
                if (character == '%') {
                    escaped = true;
                }
                else {
                    literal.append(character);
                }
                continue;
            }
            escaped = false;
            appendLiteral(tokens, literal);
            switch (character) {
                case 'a' -> tokens.add(text(Field.DAY_OF_WEEK, 3));
                case 'b' -> tokens.add(text(Field.MONTH, 3));
                case 'c' -> tokens.add(number(Field.MONTH, 1, 2));
                case 'd' -> tokens.add(number(Field.DAY_OF_MONTH, 2, 2));
                case 'e' -> tokens.add(number(Field.DAY_OF_MONTH, 1, 2));
                case 'f' -> tokens.add(number(Field.FRACTION, 6, 9));
                case 'H' -> tokens.add(number(Field.HOUR, 2, 2));
                case 'h', 'I' -> tokens.add(number(Field.CLOCK_HALFDAY_HOUR, 2, 2));
                case 'i' -> tokens.add(number(Field.MINUTE, 2, 2));
                case 'j' -> tokens.add(number(Field.DAY_OF_YEAR, 3, 3));
                case 'k' -> tokens.add(number(Field.HOUR, 1, 2));
                case 'l' -> tokens.add(number(Field.CLOCK_HALFDAY_HOUR, 1, 2));
                case 'M' -> tokens.add(text(Field.MONTH, 4));
                case 'm' -> tokens.add(number(Field.MONTH, 2, 2));
                case 'p' -> tokens.add(text(Field.HALFDAY, 1));
                case 'r' -> tokens.addAll(forMysqlPattern("%h:%i:%s %p").tokens);
                case 'S', 's' -> tokens.add(number(Field.SECOND, 2, 2));
                case 'T' -> tokens.addAll(forMysqlPattern("%H:%i:%s").tokens);
                case 'v' -> tokens.add(number(Field.WEEK, 2, 2));
                case 'x' -> tokens.add(number(Field.WEEK_YEAR, 4, 4));
                case 'W' -> tokens.add(text(Field.DAY_OF_WEEK, 4));
                case 'Y' -> tokens.add(number(Field.YEAR, 4, 4));
                case 'y' -> tokens.add(new Token(Field.YEAR, 2, 2, false, true, false, 2020, ""));
                case 'w', 'U', 'u', 'V', 'X', 'D' -> throw new IllegalArgumentException("%%%s not supported in date format string".formatted(character));
                default -> literal.append(character);
            }
        }
        appendLiteral(tokens, literal);
        if (tokens.isEmpty()) {
            throw new IllegalArgumentException("Both printing and parsing not supported");
        }
        return new DateTimeFormat(tokens);
    }

    private static Token number(Field field, int width, int maxDigits)
    {
        return new Token(field, width, maxDigits, false, false, false, 0, "");
    }

    private static Token text(Field field, int width)
    {
        return new Token(field, width, 0, true, false, false, 0, "");
    }

    private static void appendLiteral(List<Token> tokens, StringBuilder literal)
    {
        if (!literal.isEmpty()) {
            tokens.add(new Token(Field.LITERAL, 0, 0, false, false, false, 0, literal.toString()));
            literal.setLength(0);
        }
    }

    public String format(long epochMillis, ZoneId zone, Locale locale)
    {
        ZonedDateTime dateTime = Instant.ofEpochMilli(epochMillis).atZone(zone);
        StringBuilder result = new StringBuilder();
        for (Token token : tokens) {
            if (token.field() == Field.LITERAL) {
                result.append(token.literal());
            }
            else if (token.field() == Field.ZONE_ID) {
                result.append(zone.getId());
            }
            else if (token.field() == Field.SHORT_ZONE_NAME || token.field() == Field.LONG_ZONE_NAME) {
                result.append(DateTimeFormatter.ofPattern(token.field() == Field.SHORT_ZONE_NAME ? "z" : "zzzz", locale).format(dateTime));
            }
            else if (token.field() == Field.OFFSET) {
                int seconds = dateTime.getOffset().getTotalSeconds();
                int minutes = seconds / 60;
                result.append(seconds < 0 ? '-' : '+');
                appendNumber(result, Math.abs(minutes / 60), 2);
                if (token.width() >= 2) {
                    result.append(':');
                }
                appendNumber(result, Math.abs(minutes % 60), 2);
            }
            else if (token.field() == Field.FRACTION) {
                StringBuilder fraction = new StringBuilder();
                appendNumber(fraction, dateTime.getNano(), 9);
                result.append(fraction, 0, Math.min(token.width(), 9));
                result.append("0".repeat(Math.max(0, token.width() - 9)));
            }
            else if (token.text()) {
                result.append(textValue(token.field(), (int) value(token.field(), dateTime), token.width() >= 4 ? TextStyle.FULL : TextStyle.SHORT, locale));
            }
            else {
                long value = value(token.field(), dateTime);
                if (token.twoDigitYear()) {
                    value = Math.abs(value) % 100;
                }
                appendNumber(result, value, token.width());
            }
        }
        return result.toString();
    }

    private static void appendNumber(StringBuilder result, long value, int width)
    {
        if (value < 0) {
            result.append('-');
            value = -value;
        }
        String digits = Long.toString(value);
        result.append("0".repeat(Math.max(0, width - digits.length()))).append(digits);
    }

    public ZonedDateTime parse(String text, ZoneId defaultZone, Locale locale)
    {
        if (!parserSupported) {
            throw new UnsupportedOperationException("Parsing not supported");
        }
        List<ParsedField> fields = new ArrayList<>();
        ZoneId zone = defaultZone;
        ZoneOffset parsedOffset = null;
        int position = 0;
        for (Token token : tokens) {
            if (token.field() == Field.LITERAL) {
                if (!text.startsWith(token.literal(), position)) {
                    throw invalidFormat(text, position);
                }
                position += token.literal().length();
            }
            else if (token.field() == Field.ZONE_ID || token.field() == Field.SHORT_ZONE_NAME) {
                String matched = null;
                for (String candidate : token.field() == Field.ZONE_ID ? ZONE_IDS : SHORT_ZONE_NAMES.keySet()) {
                    if (text.startsWith(candidate, position) && (matched == null || candidate.length() > matched.length())) {
                        matched = candidate;
                    }
                }
                if (matched == null) {
                    throw invalidFormat(text, position);
                }
                zone = ZoneId.of(token.field() == Field.ZONE_ID ? matched : SHORT_ZONE_NAMES.get(matched));
                position += matched.length();
            }
            else if (token.field() == Field.OFFSET) {
                if (text.regionMatches(true, position, "Z", 0, 1)) {
                    parsedOffset = UTC;
                    position++;
                }
                else {
                    Matcher matcher = OFFSET.matcher(text).region(position, text.length());
                    if (!matcher.lookingAt()) {
                        throw invalidFormat(text, position);
                    }
                    parsedOffset = parseOffset(matcher.group());
                    position = matcher.end();
                }
            }
            else if (token.text()) {
                int matchedValue = -1;
                int matchedLength = 0;
                int count = switch (token.field()) {
                    case MONTH -> 12;
                    case DAY_OF_WEEK -> 7;
                    case HALFDAY, ERA -> 2;
                    default -> throw new IllegalArgumentException("Not a text field: " + token.field());
                };
                int first = token.field() == Field.ERA || token.field() == Field.HALFDAY ? 0 : 1;
                for (int value = first; value < first + count; value++) {
                    for (TextStyle style : List.of(TextStyle.SHORT, TextStyle.FULL)) {
                        String candidate = textValue(token.field(), value, style, locale);
                        if (candidate.length() > matchedLength && text.regionMatches(true, position, candidate, 0, candidate.length())) {
                            matchedValue = value;
                            matchedLength = candidate.length();
                        }
                    }
                }
                if (token.field() == Field.ERA && locale.getLanguage().equals("en")) {
                    for (String alias : new String[] {"BCE", "CE"}) {
                        if (alias.length() > matchedLength && text.regionMatches(true, position, alias, 0, alias.length())) {
                            matchedValue = alias.equals("BCE") ? 0 : 1;
                            matchedLength = alias.length();
                        }
                    }
                }
                if (matchedLength == 0) {
                    throw invalidFormat(text, position);
                }
                fields.add(new ParsedField(token.field(), matchedValue));
                position += matchedLength;
            }
            else {
                int start = position;
                boolean signed = isYear(token.field());
                if (signed && position < text.length() && (text.charAt(position) == '-' || text.charAt(position) == '+')) {
                    position++;
                }
                int digitsStart = position;
                int maxDigits = token.maxDigits();
                if (token.twoDigitYear() && token.lenientYear()) {
                    maxDigits = 9;
                }
                while (position < text.length() && position - digitsStart < maxDigits && text.charAt(position) >= '0' && text.charAt(position) <= '9') {
                    position++;
                }
                int digits = position - digitsStart;
                if (digits == 0 || (token.twoDigitYear() && !token.lenientYear() && digits != 2)) {
                    throw invalidFormat(text, start);
                }
                int value;
                if (token.field() == Field.FRACTION) {
                    String fraction = text.substring(digitsStart, position);
                    value = Integer.parseInt((fraction + "000").substring(0, 3));
                }
                else {
                    try {
                        value = Integer.parseInt(text.substring(start, position));
                    }
                    catch (NumberFormatException e) {
                        throw invalidFormat(text, start);
                    }
                    if (token.twoDigitYear() && digits == 2 && start == digitsStart) {
                        int base = token.pivotYear() - 50;
                        value = base + Math.floorMod(value - base, 100);
                    }
                }
                fields.add(new ParsedField(token.field(), value));
            }
        }
        if (position != text.length()) {
            throw invalidFormat(text, position);
        }
        return resolve(fields, parsedOffset == null ? zone : parsedOffset, text);
    }

    private static ZonedDateTime resolve(List<ParsedField> fields, ZoneId zone, String text)
    {
        fields.sort(Comparator.comparingInt(field -> field.field().sortOrder()));
        if (!fields.isEmpty() && fields.getFirst().field().ordinal() >= Field.MONTH.ordinal() && fields.getFirst().field().ordinal() <= Field.DAY_OF_WEEK.ordinal()) {
            fields.addFirst(new ParsedField(Field.YEAR, 2000));
        }
        LocalDateTime dateTime = EPOCH;
        try {
            for (ParsedField field : fields) {
                dateTime = field.set(dateTime, true);
            }
            for (int index = 0; index < fields.size(); index++) {
                dateTime = fields.get(index).set(dateTime, index == fields.size() - 1);
            }
            return resolveLocalDateTime(dateTime, zone, text);
        }
        catch (DateTimeException | ArithmeticException e) {
            throw new IllegalArgumentException("Cannot parse \"" + text + "\": " + e.getMessage(), e);
        }
    }

    public static LocalDate parseIsoDate(String value)
    {
        Matcher matcher = ISO_DATE.matcher(value);
        if (!matcher.matches()) {
            throw invalidFormat(value, 0);
        }
        int year = Integer.parseInt(matcher.group(1));
        try {
            if (matcher.group(4) != null) {
                return LocalDate.ofYearDay(year, Integer.parseInt(matcher.group(4)));
            }
            if (matcher.group(5) != null) {
                int week = Integer.parseInt(matcher.group(5));
                int day = matcher.group(6) == null ? 1 : Integer.parseInt(matcher.group(6));
                LocalDate date = LocalDate.of(year, 1, 4).with(IsoFields.WEEK_OF_WEEK_BASED_YEAR, week).with(ChronoField.DAY_OF_WEEK, day);
                if (date.get(IsoFields.WEEK_BASED_YEAR) != year || date.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR) != week) {
                    throw new DateTimeException("Invalid week of week-based year: " + week);
                }
                return date;
            }
            return LocalDate.of(year, matcher.group(2) == null ? 1 : Integer.parseInt(matcher.group(2)), matcher.group(3) == null ? 1 : Integer.parseInt(matcher.group(3)));
        }
        catch (DateTimeException e) {
            throw new IllegalArgumentException("Invalid format: \"" + value + "\": " + e.getMessage(), e);
        }
    }

    public static ZonedDateTime parseIsoTimestamp(String value, ZoneId defaultZone)
    {
        int separator = Math.max(value.indexOf('T'), value.indexOf('t'));
        String datePart = separator < 0 ? value : value.substring(0, separator);
        String timePart = separator < 0 ? "" : value.substring(separator + 1);
        LocalDate date = datePart.isEmpty() ? EPOCH.toLocalDate() : parseIsoDate(datePart);
        ZoneId zone = defaultZone;
        int zoneStart = -1;
        for (int index = 0; index < timePart.length(); index++) {
            if (timePart.charAt(index) == '+' || timePart.charAt(index) == '-' || timePart.charAt(index) == 'Z' || timePart.charAt(index) == 'z') {
                zoneStart = index;
                break;
            }
        }
        if (zoneStart >= 0) {
            String zonePart = timePart.substring(zoneStart);
            zone = zonePart.equalsIgnoreCase("Z") ? UTC : parseOffset(zonePart);
            timePart = timePart.substring(0, zoneStart);
        }
        LocalDateTime dateTime = date.atStartOfDay();
        if (!timePart.isEmpty()) {
            Matcher matcher = ISO_TIME.matcher(timePart);
            if (!matcher.matches()) {
                throw invalidFormat(value, separator + 1);
            }
            int hour = Integer.parseInt(matcher.group(1));
            int minute = matcher.group(2) == null ? 0 : Integer.parseInt(matcher.group(2));
            int second = matcher.group(3) == null ? 0 : Integer.parseInt(matcher.group(3));
            try {
                dateTime = date.atTime(hour, minute, second);
                if (matcher.group(4) != null) {
                    long unitMillis = matcher.group(3) != null ? 1000 : matcher.group(2) != null ? 60_000 : 3_600_000;
                    long fractionMillis = new BigDecimal("0." + matcher.group(4)).multiply(BigDecimal.valueOf(unitMillis)).longValue();
                    dateTime = dateTime.plusNanos(fractionMillis * 1_000_000);
                }
            }
            catch (DateTimeException e) {
                throw new IllegalArgumentException("Invalid format: \"" + value + "\": " + e.getMessage(), e);
            }
        }
        if (datePart.isEmpty() && timePart.isEmpty()) {
            throw invalidFormat(value, value.length());
        }
        return resolveLocalDateTime(dateTime, zone, value);
    }

    private static ZonedDateTime resolveLocalDateTime(LocalDateTime dateTime, ZoneId zone, String text)
    {
        try {
            long localMillis = dateTime.toInstant(UTC).toEpochMilli();
            int offsetMillis = DateTimeZoneIndex.getOffsetFromLocalMillis(localMillis, zone);
            Instant instant = Instant.ofEpochMilli(Math.subtractExact(localMillis, offsetMillis));
            if (zone.getRules().getOffset(instant).getTotalSeconds() * 1000 != offsetMillis) {
                throw new IllegalArgumentException("Cannot parse \"" + text + "\": Illegal instant due to time zone offset transition (" + zone + ")");
            }
            return instant.atZone(zone);
        }
        catch (DateTimeException | ArithmeticException e) {
            throw new IllegalArgumentException("Cannot parse \"" + text + "\": " + e.getMessage(), e);
        }
    }

    private static ZoneOffset parseOffset(String value)
    {
        Matcher matcher = OFFSET.matcher(value);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid time zone offset: " + value);
        }
        int hours = Integer.parseInt(matcher.group(2));
        int minutes = matcher.group(3) == null ? 0 : Integer.parseInt(matcher.group(3));
        int seconds = matcher.group(4) == null ? 0 : Integer.parseInt(matcher.group(4));
        int millis = matcher.group(5) == null ? 0 : Integer.parseInt((matcher.group(5) + "000").substring(0, 3));
        if (hours > 23 || minutes > 59 || seconds > 59) {
            throw new IllegalArgumentException("Invalid time zone offset: " + value);
        }
        int sign = matcher.group(1).equals("-") ? -1 : 1;
        int totalSeconds = hours * 3600 + minutes * 60 + seconds;
        if (millis != 0 || totalSeconds > 18 * 3600) {
            StringBuilder canonical = new StringBuilder(matcher.group(1));
            appendNumber(canonical, hours, 2);
            canonical.append(':');
            appendNumber(canonical, minutes, 2);
            if (seconds != 0 || millis != 0) {
                canonical.append(':');
                appendNumber(canonical, seconds, 2);
            }
            if (millis != 0) {
                canonical.append('.');
                appendNumber(canonical, millis, 3);
            }
            // These were parseable by the legacy formatter, but have no SQL time-zone key.
            throw new TimeZoneNotSupportedException(canonical.toString());
        }
        return ZoneOffset.ofTotalSeconds(sign * totalSeconds);
    }

    private static String textValue(Field field, int value, TextStyle style, Locale locale)
    {
        return switch (field) {
            case ERA -> IsoEra.of(value).getDisplayName(TextStyle.SHORT, locale);
            case MONTH -> Month.of(value).getDisplayName(style, locale);
            case DAY_OF_WEEK -> DayOfWeek.of(value).getDisplayName(style, locale);
            case HALFDAY -> new DateTimeFormatterBuilder().appendText(ChronoField.AMPM_OF_DAY).toFormatter(locale).format(EPOCH.withHour(value * 12));
            default -> throw new IllegalArgumentException("Not a text field: " + field);
        };
    }

    private static long value(Field field, ZonedDateTime dateTime)
    {
        return switch (field) {
            case ERA -> dateTime.getYear() <= 0 ? 0 : 1;
            case CENTURY -> dateTime.getLong(ChronoField.YEAR_OF_ERA) / 100;
            case YEAR -> dateTime.getYear();
            case YEAR_OF_ERA -> dateTime.getLong(ChronoField.YEAR_OF_ERA);
            case WEEK_YEAR -> dateTime.get(IsoFields.WEEK_BASED_YEAR);
            case MONTH -> dateTime.getMonthValue();
            case WEEK -> dateTime.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);
            case DAY_OF_YEAR -> dateTime.getDayOfYear();
            case DAY_OF_MONTH -> dateTime.getDayOfMonth();
            case DAY_OF_WEEK -> dateTime.getDayOfWeek().getValue();
            case HALFDAY -> dateTime.get(ChronoField.AMPM_OF_DAY);
            case HOUR -> dateTime.getHour();
            case CLOCK_HOUR -> dateTime.get(ChronoField.CLOCK_HOUR_OF_DAY);
            case HALFDAY_HOUR -> dateTime.get(ChronoField.HOUR_OF_AMPM);
            case CLOCK_HALFDAY_HOUR -> dateTime.get(ChronoField.CLOCK_HOUR_OF_AMPM);
            case MINUTE -> dateTime.getMinute();
            case SECOND -> dateTime.getSecond();
            default -> throw new IllegalArgumentException("Not a numeric field: " + field);
        };
    }

    private static boolean isYear(Field field)
    {
        return field == Field.YEAR || field == Field.YEAR_OF_ERA || field == Field.WEEK_YEAR;
    }

    private static boolean isPatternLetter(char character)
    {
        return (character >= 'A' && character <= 'Z') || (character >= 'a' && character <= 'z');
    }

    private static boolean nextTokenIsNumeric(String pattern, int index)
    {
        if (index >= pattern.length()) {
            return false;
        }
        char character = pattern.charAt(index);
        if (character == 'M') {
            int end = index + 1;
            while (end < pattern.length() && pattern.charAt(end) == character) {
                end++;
            }
            return end - index <= 2;
        }
        return "cCxyYdhHmsSeDFwWkK".indexOf(character) >= 0;
    }

    private static IllegalArgumentException invalidFormat(String text, int position)
    {
        String message = "Invalid format: \"" + text + "\"";
        if (position >= text.length()) {
            message += " is too short";
        }
        else if (position > 0) {
            message += " is malformed at \"" + text.substring(position) + "\"";
        }
        return new IllegalArgumentException(message);
    }

    // Order follows field range (year, month, week, day, hour...) and then field duration.
    private enum Field
    {
        ERA,
        CENTURY,
        YEAR,
        YEAR_OF_ERA,
        WEEK_YEAR,
        MONTH,
        WEEK,
        DAY_OF_YEAR,
        DAY_OF_MONTH,
        DAY_OF_WEEK,
        HALFDAY,
        HOUR,
        CLOCK_HOUR,
        HALFDAY_HOUR,
        CLOCK_HALFDAY_HOUR,
        MINUTE,
        SECOND,
        FRACTION,
        LITERAL,
        OFFSET,
        ZONE_ID,
        SHORT_ZONE_NAME,
        LONG_ZONE_NAME;

        public int sortOrder()
        {
            return switch (this) {
                case ERA -> 0;
                case CENTURY -> 1;
                case YEAR, YEAR_OF_ERA, WEEK_YEAR -> 2;
                case MONTH -> 3;
                case WEEK -> 4;
                case DAY_OF_YEAR -> 5;
                case DAY_OF_MONTH -> 6;
                case DAY_OF_WEEK -> 7;
                case HALFDAY -> 8;
                case HOUR, CLOCK_HOUR -> 9;
                case HALFDAY_HOUR, CLOCK_HALFDAY_HOUR -> 10;
                case MINUTE -> 11;
                case SECOND -> 12;
                case FRACTION -> 13;
                default -> throw new IllegalArgumentException("Not a parsed field: " + this);
            };
        }
    }

    private record Token(Field field, int width, int maxDigits, boolean text, boolean twoDigitYear, boolean lenientYear, int pivotYear, String literal)
    {
        private Token
        {
            requireNonNull(field, "field is null");
            requireNonNull(literal, "literal is null");
        }
    }

    private record ParsedField(Field field, int value)
    {
        public LocalDateTime set(LocalDateTime dateTime, boolean reset)
        {
            TemporalField temporalField = switch (field) {
                case ERA -> ChronoField.ERA;
                case CENTURY, YEAR_OF_ERA -> ChronoField.YEAR_OF_ERA;
                case YEAR -> ChronoField.YEAR;
                case WEEK_YEAR -> IsoFields.WEEK_BASED_YEAR;
                case MONTH -> ChronoField.MONTH_OF_YEAR;
                case WEEK -> IsoFields.WEEK_OF_WEEK_BASED_YEAR;
                case DAY_OF_YEAR -> ChronoField.DAY_OF_YEAR;
                case DAY_OF_MONTH -> ChronoField.DAY_OF_MONTH;
                case DAY_OF_WEEK -> ChronoField.DAY_OF_WEEK;
                case HALFDAY -> ChronoField.AMPM_OF_DAY;
                case HOUR -> ChronoField.HOUR_OF_DAY;
                case CLOCK_HOUR -> ChronoField.CLOCK_HOUR_OF_DAY;
                case HALFDAY_HOUR -> ChronoField.HOUR_OF_AMPM;
                case CLOCK_HALFDAY_HOUR -> ChronoField.CLOCK_HOUR_OF_AMPM;
                case MINUTE -> ChronoField.MINUTE_OF_HOUR;
                case SECOND -> ChronoField.SECOND_OF_MINUTE;
                case FRACTION -> ChronoField.MILLI_OF_SECOND;
                default -> throw new IllegalArgumentException("Not a parsed field: " + field);
            };
            long adjustedValue = value;
            if (field == Field.CENTURY) {
                adjustedValue = value * 100L + dateTime.get(ChronoField.YEAR_OF_ERA) % 100;
            }
            if (field == Field.WEEK) {
                int weekYear = dateTime.get(IsoFields.WEEK_BASED_YEAR);
                int maximum = LocalDate.of(weekYear, 12, 28).get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);
                if (value < 1 || value > maximum) {
                    throw new DateTimeException("Invalid week of week-based year: " + value);
                }
            }
            dateTime = dateTime.with(temporalField, adjustedValue);
            if (!reset) {
                return dateTime;
            }
            return switch (field) {
                case ERA -> value == 0 ? LocalDateTime.ofInstant(Instant.ofEpochMilli(Long.MIN_VALUE), UTC) : LocalDate.of(1, 1, 1).atStartOfDay();
                case CENTURY -> dateTime.withYear(dateTime.getYear() / 100 * 100).withDayOfYear(1).toLocalDate().atStartOfDay();
                case YEAR, YEAR_OF_ERA -> dateTime.withDayOfYear(1).toLocalDate().atStartOfDay();
                case WEEK_YEAR -> dateTime.with(IsoFields.WEEK_OF_WEEK_BASED_YEAR, 1).with(ChronoField.DAY_OF_WEEK, 1).toLocalDate().atStartOfDay();
                case MONTH -> dateTime.withDayOfMonth(1).toLocalDate().atStartOfDay();
                case WEEK -> dateTime.with(ChronoField.DAY_OF_WEEK, 1).toLocalDate().atStartOfDay();
                case DAY_OF_MONTH, DAY_OF_YEAR, DAY_OF_WEEK -> dateTime.toLocalDate().atStartOfDay();
                case HALFDAY -> dateTime.withHour(dateTime.getHour() / 12 * 12).withMinute(0).withSecond(0).withNano(0);
                case HOUR, CLOCK_HOUR, HALFDAY_HOUR, CLOCK_HALFDAY_HOUR -> dateTime.withMinute(0).withSecond(0).withNano(0);
                case MINUTE -> dateTime.withSecond(0).withNano(0);
                case SECOND -> dateTime.withNano(0);
                case FRACTION -> dateTime;
                default -> throw new IllegalArgumentException("Not a parsed field: " + field);
            };
        }
    }
}
