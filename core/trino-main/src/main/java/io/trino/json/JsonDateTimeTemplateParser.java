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

import io.trino.json.JsonDateTimeTemplate.Field;
import io.trino.json.JsonDateTimeTemplate.FieldSegment;
import io.trino.json.JsonDateTimeTemplate.LiteralSegment;
import io.trino.json.JsonDateTimeTemplate.Segment;
import io.trino.spi.type.Type;
import org.antlr.v4.runtime.ANTLRErrorListener;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;

import java.util.ArrayList;
import java.util.List;

import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimeWithTimeZoneType.createTimeWithTimeZoneType;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.antlr.v4.runtime.Token.EOF;

/// Lexes and validates a datetime() format-template string per ISO/IEC 9075-2:2023 §9.46.
final class JsonDateTimeTemplateParser
{
    private static final ANTLRErrorListener ERROR_LISTENER = new BaseErrorListener()
    {
        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String message, RecognitionException e)
        {
            throw new IllegalArgumentException(format("invalid datetime() format template at position %d: %s", charPositionInLine, message));
        }
    };

    private JsonDateTimeTemplateParser() {}

    /// The `template` is the canonical form of the parsed template: field tokens upper-cased,
    /// literal text preserved verbatim.
    record Parsed(String template, Type type, int precision, List<Segment> segments) {}

    public static Parsed parse(String template)
    {
        if (template.isEmpty()) {
            throw new IllegalArgumentException("datetime() format template is empty");
        }

        JsonDateTimeTemplateLexer lexer = new JsonDateTimeTemplateLexer(CharStreams.fromString(template));
        lexer.removeErrorListeners();
        lexer.addErrorListener(ERROR_LISTENER);

        List<Segment> segments = new ArrayList<>();
        TemplateState state = new TemplateState();
        // The lexer matches field tokens case-insensitively, so `yyyy-mm-dd` and `YYYY-MM-DD`
        // describe the same template. Rebuild the template in a canonical spelling as it is
        // lexed, so that equivalent templates are indistinguishable to equals() and toString().
        // Only the field tokens are folded: literal text is matched case-sensitively.
        StringBuilder canonical = new StringBuilder(template.length());
        // Whether the previously-emitted segment was a DELIMITER literal (not a quoted literal).
        // Two adjacent delimiters are rejected to avoid ambiguous templates like `YYYY--MM`,
        // but a delimiter next to a quoted literal (e.g. `YYYY-"T"MM`) is fine.
        boolean previousWasDelimiter = false;
        // Track the most recent un-finalized FieldSegment so we can resolve its `delimited`
        // flag in this same pass: it depends on what follows the field (a literal/EOF closes it
        // delimited, a field closes it un-delimited).
        int pendingFieldIndex = -1;
        boolean pendingFieldDelimitedLeft = false;
        boolean leftIsBoundary = true;
        for (Token lexerToken = lexer.nextToken(); lexerToken.getType() != EOF; lexerToken = lexer.nextToken()) {
            int type = lexerToken.getType();
            if (type == JsonDateTimeTemplateLexer.DELIMITER || type == JsonDateTimeTemplateLexer.QUOTED_LITERAL) {
                String literal = toLiteral(lexerToken);
                if (literal.isEmpty()) {
                    throw new IllegalArgumentException("datetime() format template cannot contain empty quoted literal");
                }
                if (previousWasDelimiter && type == JsonDateTimeTemplateLexer.DELIMITER) {
                    throw new IllegalArgumentException("datetime() format template cannot contain consecutive delimiters");
                }
                if (pendingFieldIndex != -1) {
                    finalizePendingField(segments, pendingFieldIndex, pendingFieldDelimitedLeft, true);
                    pendingFieldIndex = -1;
                }
                segments.add(new LiteralSegment(literal));
                canonical.append(lexerToken.getText());
                previousWasDelimiter = type == JsonDateTimeTemplateLexer.DELIMITER;
                leftIsBoundary = true;
            }
            else {
                FieldSegment fieldSegment = toFieldSegment(lexerToken);
                state.add(fieldSegment.field(), fieldSegment.width());
                canonical.append(lexerToken.getText().toUpperCase(ENGLISH));
                if (pendingFieldIndex != -1) {
                    // A field follows another field directly — the previous field is not right-delimited.
                    finalizePendingField(segments, pendingFieldIndex, pendingFieldDelimitedLeft, false);
                }
                segments.add(fieldSegment);
                pendingFieldIndex = segments.size() - 1;
                pendingFieldDelimitedLeft = leftIsBoundary;
                leftIsBoundary = false;
                previousWasDelimiter = false;
            }
        }

        // End of input — close any pending field with right boundary = literal/EOF.
        if (pendingFieldIndex != -1) {
            finalizePendingField(segments, pendingFieldIndex, pendingFieldDelimitedLeft, true);
        }

        if (state.isEmpty()) {
            throw new IllegalArgumentException("datetime() format template must contain at least one datetime field");
        }

        return new Parsed(canonical.toString(), state.type(), state.precision, List.copyOf(segments));
    }

    private static void finalizePendingField(List<Segment> segments, int index, boolean delimitedLeft, boolean delimitedRight)
    {
        FieldSegment pending = (FieldSegment) segments.get(index);
        segments.set(index, new FieldSegment(pending.field(), pending.width(), delimitedLeft && delimitedRight));
    }

    private static FieldSegment toFieldSegment(Token lexerToken)
    {
        return switch (lexerToken.getType()) {
            case JsonDateTimeTemplateLexer.YYYY -> new FieldSegment(Field.YEAR, 4, false);
            case JsonDateTimeTemplateLexer.YYY -> new FieldSegment(Field.YEAR, 3, false);
            case JsonDateTimeTemplateLexer.YY -> new FieldSegment(Field.YEAR, 2, false);
            case JsonDateTimeTemplateLexer.Y -> new FieldSegment(Field.YEAR, 1, false);
            case JsonDateTimeTemplateLexer.RRRR -> new FieldSegment(Field.ROUNDED_YEAR, 4, false);
            case JsonDateTimeTemplateLexer.RR -> new FieldSegment(Field.ROUNDED_YEAR, 2, false);
            case JsonDateTimeTemplateLexer.MM -> new FieldSegment(Field.MONTH, 2, false);
            case JsonDateTimeTemplateLexer.DD -> new FieldSegment(Field.DAY, 2, false);
            case JsonDateTimeTemplateLexer.DDD -> new FieldSegment(Field.DAY_OF_YEAR, 3, false);
            case JsonDateTimeTemplateLexer.HH, JsonDateTimeTemplateLexer.HH12 -> new FieldSegment(Field.HOUR12, 2, false);
            case JsonDateTimeTemplateLexer.HH24 -> new FieldSegment(Field.HOUR24, 2, false);
            case JsonDateTimeTemplateLexer.MI -> new FieldSegment(Field.MINUTE, 2, false);
            case JsonDateTimeTemplateLexer.SS -> new FieldSegment(Field.SECOND, 2, false);
            case JsonDateTimeTemplateLexer.SSSSS -> new FieldSegment(Field.SECOND_OF_DAY, 5, false);
            case JsonDateTimeTemplateLexer.FRACTION -> new FieldSegment(Field.FRACTION, fractionWidth(lexerToken.getText()), false);
            case JsonDateTimeTemplateLexer.AM_PM -> new FieldSegment(Field.AM_PM, 0, false);
            case JsonDateTimeTemplateLexer.TZH -> new FieldSegment(Field.TIME_ZONE_HOUR, 3, false);
            case JsonDateTimeTemplateLexer.TZM -> new FieldSegment(Field.TIME_ZONE_MINUTE, 2, false);
            default -> throw new IllegalStateException("unexpected datetime() template field token: " + lexerToken.getText());
        };
    }

    private static int fractionWidth(String text)
    {
        // FF1..FF9 are the standard widths; FF10..FF12 extend coverage to Trino's
        // maximum TIME / TIMESTAMP precision of 12.
        int width = Integer.parseInt(text.substring(2));
        if (width < 1 || width > 12) {
            throw new IllegalArgumentException("invalid datetime() format template fraction width: " + text);
        }
        return width;
    }

    private static String toLiteral(Token lexerToken)
    {
        if (lexerToken.getType() == JsonDateTimeTemplateLexer.DELIMITER) {
            return lexerToken.getText();
        }
        // Strip surrounding quotes and unescape doubled embedded quotes.
        String quoted = lexerToken.getText();
        return quoted.substring(1, quoted.length() - 1).replace("\"\"", "\"");
    }

    private static final class TemplateState
    {
        private boolean year;
        private boolean roundedYear;
        private boolean month;
        private boolean day;
        private boolean dayOfYear;
        private boolean hour12;
        private boolean hour24;
        private boolean minute;
        private boolean second;
        private boolean secondOfDay;
        private boolean fraction;
        private boolean amPm;
        private boolean timeZoneHour;
        private boolean timeZoneMinute;
        private int precision;

        public void add(Field field, int width)
        {
            switch (field) {
                case YEAR -> checkUnique("year", year);
                case ROUNDED_YEAR -> checkUnique("rounded year", roundedYear);
                case MONTH -> checkUnique("month", month);
                case DAY -> checkUnique("day", day);
                case DAY_OF_YEAR -> checkUnique("day of year", dayOfYear);
                case HOUR12 -> checkUnique("12-hour field", hour12);
                case HOUR24 -> checkUnique("24-hour field", hour24);
                case MINUTE -> checkUnique("minute", minute);
                case SECOND -> checkUnique("second", second);
                case SECOND_OF_DAY -> checkUnique("second-of-day", secondOfDay);
                case FRACTION -> checkUnique("fraction", fraction);
                case AM_PM -> checkUnique("AM/PM", amPm);
                case TIME_ZONE_HOUR -> checkUnique("time zone hour", timeZoneHour);
                case TIME_ZONE_MINUTE -> checkUnique("time zone minute", timeZoneMinute);
            }

            switch (field) {
                case YEAR -> year = true;
                case ROUNDED_YEAR -> roundedYear = true;
                case MONTH -> month = true;
                case DAY -> day = true;
                case DAY_OF_YEAR -> dayOfYear = true;
                case HOUR12 -> hour12 = true;
                case HOUR24 -> hour24 = true;
                case MINUTE -> minute = true;
                case SECOND -> second = true;
                case SECOND_OF_DAY -> secondOfDay = true;
                case AM_PM -> amPm = true;
                case TIME_ZONE_HOUR -> timeZoneHour = true;
                case TIME_ZONE_MINUTE -> timeZoneMinute = true;
                case FRACTION -> {
                    fraction = true;
                    precision = Math.max(precision, width);
                }
            }
        }

        public boolean isEmpty()
        {
            return !(year || roundedYear || month || day || dayOfYear || hour12 || hour24 || minute || second || secondOfDay || fraction || amPm || timeZoneHour || timeZoneMinute);
        }

        public Type type()
        {
            validate();
            boolean hasDateFields = year || roundedYear || month || day || dayOfYear;
            boolean hasTimeFields = hour12 || hour24 || minute || second || secondOfDay || fraction || amPm;
            boolean hasTimeZoneFields = timeZoneHour || timeZoneMinute;

            if (hasDateFields) {
                if (hasTimeZoneFields) {
                    return createTimestampWithTimeZoneType(precision);
                }
                if (hasTimeFields) {
                    return createTimestampType(precision);
                }
                return DATE;
            }

            if (hasTimeZoneFields) {
                return createTimeWithTimeZoneType(precision);
            }
            return createTimeType(precision);
        }

        private void validate()
        {
            if (year && roundedYear) {
                throw new IllegalArgumentException("datetime() format template cannot contain both year and rounded year fields");
            }
            if (dayOfYear && (month || day)) {
                throw new IllegalArgumentException("datetime() format template with DDD cannot contain MM or DD");
            }
            if (hour24 && (hour12 || amPm)) {
                throw new IllegalArgumentException("datetime() format template with HH24 cannot contain HH, HH12, A.M. or P.M.");
            }
            if (hour12 && !amPm) {
                throw new IllegalArgumentException("datetime() format template with HH or HH12 requires A.M. or P.M.");
            }
            if (amPm && !hour12) {
                throw new IllegalArgumentException("datetime() format template with A.M. or P.M. requires HH or HH12");
            }
            if (secondOfDay && (hour12 || hour24 || minute || second || amPm)) {
                throw new IllegalArgumentException("datetime() format template with SSSSS cannot contain HH, HH12, HH24, MI, SS, A.M. or P.M.");
            }
            if (timeZoneMinute && !timeZoneHour) {
                throw new IllegalArgumentException("datetime() format template with TZM requires TZH");
            }
        }

        private static void checkUnique(String fieldName, boolean seen)
        {
            if (seen) {
                throw new IllegalArgumentException(format("datetime() format template contains duplicate %s field", fieldName));
            }
        }
    }
}
