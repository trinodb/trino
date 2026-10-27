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
package io.trino.plugin.teradata.functions.dateformat;

import io.trino.spi.TrinoException;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.Token;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_AMPM;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoField.YEAR;

public final class DateFormatParser
{
    private DateFormatParser() {}

    public static Formatter createDateTimeFormatter(String format)
    {
        List<? extends Token> tokens = tokenize(format);
        if (tokens.isEmpty()) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Date format must not be empty");
        }
        DateTimeFormatterBuilder printer = new DateTimeFormatterBuilder();
        StringBuilder pattern = new StringBuilder();
        List<Integer> fields = new ArrayList<>();
        for (Token token : tokens) {
            if (token.getType() == DateFormat.TEXT) {
                printer.appendLiteral(token.getText());
                pattern.append(Pattern.quote(token.getText()));
                continue;
            }
            ChronoField field = field(token);
            fields.add(token.getType());
            if (token.getType() == DateFormat.YY) {
                printer.appendValueReduced(YEAR, 2, 2, 2000);
                pattern.append("([0-9]{2})");
            }
            else if (token.getType() == DateFormat.YYYY) {
                printer.appendValue(YEAR, 4, 9, SignStyle.NORMAL);
                pattern.append("([+-]?[0-9]{1,4}+)");
            }
            else {
                printer.appendValue(field, 2);
                pattern.append("([0-9]{1,2}+)");
            }
        }
        return new Formatter(Pattern.compile(pattern.toString()), List.copyOf(fields), printer.toFormatter());
    }

    private static ChronoField field(Token token)
    {
        try {
            return Formatter.fieldType(token.getType());
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(
                    INVALID_FUNCTION_ARGUMENT,
                    format("Failed to tokenize string [%s] at offset [%d]", token.getText(), token.getCharPositionInLine()));
        }
    }

    public record Formatter(Pattern parser, List<Integer> fields, DateTimeFormatter printer)
    {
        public long parseMillis(String value)
        {
            Matcher matcher = parser.matcher(value);
            if (!matcher.matches()) {
                throw new DateTimeParseException("Invalid date/time", value, 0);
            }
            Map<ChronoField, Integer> values = new EnumMap<>(ChronoField.class);
            for (int index = 0; index < fields.size(); index++) {
                int tokenType = fields.get(index);
                int parsed = Integer.parseInt(matcher.group(index + 1));
                if (tokenType == DateFormat.YY) {
                    parsed += 2000;
                }
                ChronoField field = fieldType(tokenType);
                field.checkValidValue(parsed);
                values.put(field, parsed);
            }
            int defaultYear = values.containsKey(MONTH_OF_YEAR) || values.containsKey(DAY_OF_MONTH) ? 2000 : 1970;
            LocalDate date = LocalDate.of(values.getOrDefault(YEAR, defaultYear), values.getOrDefault(MONTH_OF_YEAR, 1), values.getOrDefault(DAY_OF_MONTH, 1));
            LocalTime time = LocalTime.of(values.getOrDefault(HOUR_OF_DAY, 0), values.getOrDefault(MINUTE_OF_HOUR, 0), values.getOrDefault(SECOND_OF_MINUTE, 0));
            if (values.containsKey(HOUR_OF_AMPM)) {
                time = time.with(HOUR_OF_AMPM, values.get(HOUR_OF_AMPM));
            }
            return date.atTime(time).toInstant(UTC).toEpochMilli();
        }

        private static ChronoField fieldType(int tokenType)
        {
            return switch (tokenType) {
                case DateFormat.DD -> DAY_OF_MONTH;
                case DateFormat.HH24 -> HOUR_OF_DAY;
                case DateFormat.HH -> HOUR_OF_AMPM;
                case DateFormat.MI -> MINUTE_OF_HOUR;
                case DateFormat.MM -> MONTH_OF_YEAR;
                case DateFormat.SS -> SECOND_OF_MINUTE;
                case DateFormat.YY, DateFormat.YYYY -> YEAR;
                default -> throw new IllegalArgumentException("Unsupported date field: " + tokenType);
            };
        }

        public String format(long epochMillis, ZoneId zone, Locale locale)
        {
            return printer.withLocale(locale).withZone(zone).format(Instant.ofEpochMilli(epochMillis));
        }
    }

    public static List<? extends Token> tokenize(String format)
    {
        DateFormat lexer = new DateFormat(CharStreams.fromString(format));
        return lexer.getAllTokens();
    }
}
