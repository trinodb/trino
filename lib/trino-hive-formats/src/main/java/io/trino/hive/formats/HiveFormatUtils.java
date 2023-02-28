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
package io.trino.hive.formats;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.base.type.DecodedTimestamp;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DecimalConversions;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.Type;
import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;
import org.joda.time.MutableDateTime;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeParser;
import org.joda.time.format.DateTimeParserBucket;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.SignStyle;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.overflows;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static java.lang.StrictMath.floorDiv;
import static java.lang.StrictMath.floorMod;
import static java.lang.StrictMath.toIntExact;
import static java.lang.String.format;
import static java.math.RoundingMode.HALF_UP;
import static java.time.format.ResolverStyle.LENIENT;
import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoField.YEAR;
import static java.util.Objects.requireNonNull;

public final class HiveFormatUtils
{
    public static final String TIMESTAMP_FORMATS_KEY = "timestamp.formats";
    private static final char TIMESTAMP_FORMATS_SEPARATOR = ',';
    private static final char TIMESTAMP_FORMATS_ESCAPE = '\\';

    private static final DateTimeFormatter DATE_PARSER = new DateTimeFormatterBuilder()
            .parseLenient()
            .appendValue(YEAR, 1, 10, SignStyle.NORMAL)
            .appendLiteral('-')
            .appendValue(MONTH_OF_YEAR, 1, 2, SignStyle.NORMAL)
            .appendLiteral('-')
            .appendValue(DAY_OF_MONTH, 1, 2, SignStyle.NORMAL)
            .toFormatter()
            .withResolverStyle(LENIENT);

    private static final DateTimeFormatter DATE_FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(YEAR, 1, 10, SignStyle.NORMAL)
            .appendLiteral('-')
            .appendValue(MONTH_OF_YEAR, 2, 2, SignStyle.NORMAL)
            .appendLiteral('-')
            .appendValue(DAY_OF_MONTH, 2, 2, SignStyle.NORMAL)
            .toFormatter();

    private static final DateTimeFormatter DEFAULT_TIMESTAMP_PARSER = new DateTimeFormatterBuilder()
            .parseLenient()
            // Date part
            .appendValue(YEAR, 1, 10, SignStyle.NORMAL)
            .appendLiteral('-')
            .appendValue(MONTH_OF_YEAR, 1, 2, SignStyle.NORMAL)
            .appendLiteral('-')
            .appendValue(DAY_OF_MONTH, 1, 2, SignStyle.NORMAL)
            // Time part
            .optionalStart().appendLiteral(" ")
            .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NORMAL)
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NORMAL)
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NORMAL)
            .optionalStart().appendFraction(NANO_OF_SECOND, 1, 9, true).optionalEnd()
            .optionalEnd()
            .toFormatter()
            .withResolverStyle(LENIENT);

    private static final DateTimeFormatter TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
            // Date part
            .appendValue(YEAR, 1, 10, SignStyle.NORMAL)
            .appendLiteral('-')
            .appendValue(MONTH_OF_YEAR, 2, 2, SignStyle.NORMAL)
            .appendLiteral('-')
            .appendValue(DAY_OF_MONTH, 2, 2, SignStyle.NORMAL)
            // Time part
            .optionalStart().appendLiteral(" ")
            .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NORMAL)
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NORMAL)
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NORMAL)
            .optionalStart().appendFraction(NANO_OF_SECOND, 0, 9, true).optionalEnd()
            .optionalEnd()
            .toFormatter();

    private static final String MILLIS_TIMESTAMP_FORMAT = "millis";

    private static final DateTime STARTING_TIMESTAMP_VALUE = new DateTime(1970, 1, 1, 0, 0, 0, 0, ISOChronology.getInstanceUTC());

    private HiveFormatUtils() {}

    public static LocalDate parseHiveDate(String value)
    {
        value = value.trim();
        int index = value.indexOf(" ");
        if (index != -1) {
            value = value.substring(0, index);
        }
        return LocalDate.parse(value, DATE_PARSER);
    }

    public static void writeDecimal(String value, DecimalType decimalType, BlockBuilder builder)
    {
        BigDecimal bigDecimal = parseDecimal(value, decimalType);
        if (overflows(bigDecimal, decimalType.getPrecision())) {
            throw new NumberFormatException(format("Cannot convert '%s' to %s. Value too large.", value, decimalType));
        }
        if (decimalType.isShort()) {
            decimalType.writeLong(builder, bigDecimal.unscaledValue().longValueExact());
        }
        else {
            decimalType.writeObject(builder, Int128.valueOf(bigDecimal.unscaledValue()));
        }
    }

    public static BigDecimal parseDecimal(String value, DecimalType decimalType)
    {
        try {
            return new BigDecimal(value).setScale(DecimalConversions.intScale(decimalType.getScale()), HALF_UP);
        }
        catch (NumberFormatException e) {
            throw new NumberFormatException(format("Cannot convert '%s' to %s. Value is not a number.", value, decimalType));
        }
    }

    public static Function<String, DecodedTimestamp> createTimestampParser(List<String> timestampFormats)
    {
        requireNonNull(timestampFormats, "timestampFormats is null");
        if (timestampFormats.isEmpty()) {
            return value -> parseHiveTimestamp(null, value);
        }

        DateTimeParser[] parsers = new DateTimeParser[timestampFormats.size()];
        for (int i = 0; i < timestampFormats.size(); ++i) {
            String formatString = timestampFormats.get(i);
            if (formatString.equalsIgnoreCase(MILLIS_TIMESTAMP_FORMAT)) {
                // Use milliseconds parser if pattern matches our special-case millis pattern string
                parsers[i] = new MillisDateFormatParser();
            }
            else {
                parsers[i] = DateTimeFormat.forPattern(formatString).getParser();
            }
        }
        org.joda.time.format.DateTimeFormatter dateTimeFormatter = new org.joda.time.format.DateTimeFormatterBuilder()
                .append(null, parsers)
                .toFormatter()
                .withDefaultYear(1970);
        return value -> parseHiveTimestamp(dateTimeFormatter, value);
    }

    private static DecodedTimestamp parseHiveTimestamp(org.joda.time.format.DateTimeFormatter dateTimeFormatter, String value)
    {
        value = value.trim();
        if (dateTimeFormatter != null) {
            try {
                // This is how Hive performs a timestamp conversion
                // reset value in case any date fields are missing from the date pattern
                MutableDateTime dateTime = new MutableDateTime(STARTING_TIMESTAMP_VALUE, ISOChronology.getInstanceUTC());

                // Using parseInto() avoids throwing exception when parsing,
                // allowing fallback to default timestamp parsing if custom patterns fail.
                int parsedLength = dateTimeFormatter.parseInto(dateTime, value, 0);
                // Only accept parse results if we parsed the entire string
                if (parsedLength == value.length()) {
                    long millis = dateTime.getMillis();
                    long epochSeconds = floorDiv(millis, (long) MILLISECONDS_PER_SECOND);
                    long fractionalSecond = floorMod(millis, (long) MILLISECONDS_PER_SECOND);
                    int nanosOfSecond = toIntExact(fractionalSecond * (long) NANOSECONDS_PER_MILLISECOND);
                    return new DecodedTimestamp(epochSeconds, nanosOfSecond);
                }
            }
            catch (Exception ignored) {
            }
        }

        return parseHiveTimestamp(value);
    }

    public static DecodedTimestamp parseHiveTimestamp(String value)
    {
        // Otherwise try default timestamp parsing
        // default parser uses Java util time
        LocalDateTime localDateTime;
        try {
            localDateTime = LocalDateTime.parse(value, DEFAULT_TIMESTAMP_PARSER);
        }
        catch (DateTimeParseException e) {
            // Try ISO-8601 format
            localDateTime = LocalDateTime.parse(value);
        }
        return new DecodedTimestamp(localDateTime.toEpochSecond(ZoneOffset.UTC), localDateTime.getNano());
    }

    public static List<String> getTimestampFormatsSchemaProperty(Map<String, String> serdeProperties)
    {
        String property = serdeProperties.get(TIMESTAMP_FORMATS_KEY);
        if (property == null) {
            return ImmutableList.of();
        }

        ImmutableList.Builder<String> result = ImmutableList.builder();
        StringBuilder buffer = new StringBuilder();

        int position = 0;
        while (position < property.length()) {
            char c = property.charAt(position);
            if (c == TIMESTAMP_FORMATS_ESCAPE) {
                // the next character must be an escape or separator
                checkArgument(
                        position + 1 < property.length(),
                        "Invalid '%s' property value '%s': unterminated escape at end of value",
                        TIMESTAMP_FORMATS_KEY,
                        property);
                char nextCharacter = property.charAt(position + 1);
                checkArgument(
                        nextCharacter == TIMESTAMP_FORMATS_SEPARATOR || nextCharacter == TIMESTAMP_FORMATS_ESCAPE,
                        "Invalid '%s' property value '%s': Illegal escaped character at %s",
                        TIMESTAMP_FORMATS_KEY,
                        property,
                        position);

                buffer.append(nextCharacter);
                position++;
            }
            else if (c == TIMESTAMP_FORMATS_SEPARATOR) {
                // ignore empty values
                if (!buffer.isEmpty()) {
                    result.add(buffer.toString());
                    buffer.setLength(0);
                }
            }
            else {
                buffer.append(c);
            }
            position++;
        }

        if (!buffer.isEmpty()) {
            result.add(buffer.toString());
        }
        return result.build();
    }

    public static String formatHiveDate(Block block, int position)
    {
        LocalDate localDate = LocalDate.ofEpochDay(DATE.getLong(block, position));
        return localDate.format(DATE_FORMATTER);
    }

    public static void formatHiveDate(Block block, int position, StringBuilder builder)
    {
        LocalDate localDate = LocalDate.ofEpochDay(DATE.getLong(block, position));
        DATE_FORMATTER.formatTo(localDate, builder);
    }

    public static String formatHiveTimestamp(Type type, Block block, int position)
    {
        SqlTimestamp objectValue = (SqlTimestamp) type.getObjectValue(null, block, position);
        LocalDateTime localDateTime = objectValue.toLocalDateTime();
        return TIMESTAMP_FORMATTER.format(localDateTime);
    }

    public static void formatHiveTimestamp(Type type, Block block, int position, StringBuilder builder)
    {
        SqlTimestamp objectValue = (SqlTimestamp) type.getObjectValue(null, block, position);
        LocalDateTime localDateTime = objectValue.toLocalDateTime();
        TIMESTAMP_FORMATTER.formatTo(localDateTime, builder);
    }

    private static class MillisDateFormatParser
            implements DateTimeParser
    {
        private static final Pattern PATTERN = Pattern.compile("(-?\\d+)(\\.\\d+)?$");

        private static final DateTimeFieldType[] DATE_TIME_FIELDS = {
                DateTimeFieldType.year(),
                DateTimeFieldType.monthOfYear(),
                DateTimeFieldType.dayOfMonth(),
                DateTimeFieldType.hourOfDay(),
                DateTimeFieldType.minuteOfHour(),
                DateTimeFieldType.secondOfMinute(),
                DateTimeFieldType.millisOfSecond()
        };

        @Override
        public int estimateParsedLength()
        {
            return 13; // Shouldn't hit 14 digits until year 2286
        }

        @Override
        public int parseInto(DateTimeParserBucket bucket, String text, int position)
        {
            text = text.substring(position);

            Matcher matcher = PATTERN.matcher(text);
            if (!matcher.matches()) {
                return -1;
            }

            // Joda DateTime only has precision to millis, cut off any fractional portion
            long millis = Long.parseLong(matcher.group(1));
            DateTime dateTime = new DateTime(millis, ISOChronology.getInstanceUTC());
            for (DateTimeFieldType field : DATE_TIME_FIELDS) {
                bucket.saveField(field, dateTime.get(field));
            }
            return text.length();
        }
    }
}
