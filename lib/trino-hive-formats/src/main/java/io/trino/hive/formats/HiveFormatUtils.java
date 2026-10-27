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
import io.trino.plugin.base.time.DateTimeFormat;
import io.trino.plugin.base.type.DecodedTimestamp;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DecimalConversions;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.Type;

import java.math.BigDecimal;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.SignStyle;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.hive.formats.HiveFormatsErrorCode.HIVE_INVALID_METADATA;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.overflows;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static java.lang.StrictMath.floorDiv;
import static java.lang.StrictMath.floorMod;
import static java.lang.StrictMath.toIntExact;
import static java.lang.String.format;
import static java.math.RoundingMode.HALF_UP;
import static java.time.ZoneOffset.UTC;
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
            .parseCaseInsensitive()
            .parseLenient()
            // Date part
            .appendValue(YEAR, 1, 10, SignStyle.NORMAL)
            .appendLiteral('-')
            .appendValue(MONTH_OF_YEAR, 1, 2, SignStyle.NORMAL)
            .appendLiteral('-')
            .appendValue(DAY_OF_MONTH, 1, 2, SignStyle.NORMAL)
            // Time part
            .optionalStart()
            .appendLiteral(" ")
            .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NORMAL)
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NORMAL)
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NORMAL)
            .optionalStart().appendFraction(NANO_OF_SECOND, 1, 9, true).optionalEnd()
            .optionalEnd()
            .toFormatter()
            .withResolverStyle(LENIENT);

    private static final DateTimeFormatter ISO_TIMESTAMP_PARSER = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .parseLenient()
            .append(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
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
            .optionalStart()
            .appendLiteral(" ")
            .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NORMAL)
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NORMAL)
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NORMAL)
            .optionalStart().appendFraction(NANO_OF_SECOND, 0, 9, true).optionalEnd()
            .optionalEnd()
            .toFormatter();

    private static final String MILLIS_TIMESTAMP_FORMAT = "millis";

    private static final Pattern MILLIS_TIMESTAMP_PATTERN = Pattern.compile("(-?\\d+)(\\.\\d+)?$");

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
            return scaleDecimal(new BigDecimal(value), decimalType);
        }
        catch (NumberFormatException e) {
            throw new NumberFormatException(format("Cannot convert '%s' to %s. Value is not a number.", value, decimalType));
        }
    }

    public static BigDecimal scaleDecimal(BigDecimal bigDecimal, DecimalType decimalType)
    {
        return bigDecimal.setScale(DecimalConversions.intScale(decimalType.getScale()), HALF_UP);
    }

    public static Function<String, DecodedTimestamp> createTimestampParser(List<String> timestampFormats)
    {
        requireNonNull(timestampFormats, "timestampFormats is null");
        if (timestampFormats.isEmpty()) {
            return value -> parseHiveTimestamp(null, value);
        }

        List<Function<String, Long>> parsers = new ArrayList<>();
        for (String format : timestampFormats) {
            if (format.equalsIgnoreCase(MILLIS_TIMESTAMP_FORMAT)) {
                // Custom formats have millisecond precision, including the special millis format.
                parsers.add(value -> {
                    Matcher matcher = MILLIS_TIMESTAMP_PATTERN.matcher(value);
                    if (!matcher.matches()) {
                        throw new IllegalArgumentException("Invalid millisecond timestamp: " + value);
                    }
                    return Long.parseLong(matcher.group(1));
                });
            }
            else {
                DateTimeFormat formatter = DateTimeFormat.forPattern(format);
                parsers.add(value -> formatter.parse(value, UTC, Locale.getDefault(), 1970).toInstant().toEpochMilli());
            }
        }
        return value -> parseHiveTimestamp(parsers, value);
    }

    private static DecodedTimestamp parseHiveTimestamp(List<Function<String, Long>> parsers, String value)
    {
        value = value.trim();
        if (parsers != null) {
            for (Function<String, Long> parser : parsers) {
                try {
                    long millis = parser.apply(value);
                    long epochSeconds = floorDiv(millis, (long) MILLISECONDS_PER_SECOND);
                    long fractionalSecond = floorMod(millis, (long) MILLISECONDS_PER_SECOND);
                    int nanosOfSecond = toIntExact(fractionalSecond * (long) NANOSECONDS_PER_MILLISECOND);
                    return new DecodedTimestamp(epochSeconds, nanosOfSecond);
                }
                catch (RuntimeException e) {
                    // The first complete pattern determines the fields. Invalid field values
                    // fall back to the default Hive syntax rather than a different custom pattern.
                    if (e.getCause() instanceof DateTimeException || e.getCause() instanceof ArithmeticException || e instanceof NumberFormatException) {
                        break;
                    }
                }
            }
        }
        return parseTrimmedHiveTimestamp(value);
    }

    public static DecodedTimestamp parseHiveTimestamp(String value)
    {
        return parseTrimmedHiveTimestamp(value.trim());
    }

    private static DecodedTimestamp parseTrimmedHiveTimestamp(String value)
    {
        // Otherwise try default timestamp parsing
        // default parser uses Java util time
        LocalDateTime localDateTime;
        try {
            localDateTime = LocalDateTime.parse(value, DEFAULT_TIMESTAMP_PARSER);
        }
        catch (DateTimeParseException e) {
            // Try ISO-8601 format
            localDateTime = LocalDateTime.parse(value, ISO_TIMESTAMP_PARSER);
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
                if (position + 1 >= property.length()) {
                    throw new TrinoException(
                            HIVE_INVALID_METADATA,
                            "Invalid '%s' property value '%s': unterminated escape at end of value".formatted(TIMESTAMP_FORMATS_KEY, property));
                }
                char nextCharacter = property.charAt(position + 1);
                if (nextCharacter != TIMESTAMP_FORMATS_SEPARATOR && nextCharacter != TIMESTAMP_FORMATS_ESCAPE) {
                    throw new TrinoException(
                            HIVE_INVALID_METADATA,
                            "Invalid '%s' property value '%s': Illegal escaped character at %s".formatted(TIMESTAMP_FORMATS_KEY, property, position));
                }

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
        LocalDate localDate = LocalDate.ofEpochDay(DATE.getInt(block, position));
        return localDate.format(DATE_FORMATTER);
    }

    public static void formatHiveDate(Block block, int position, StringBuilder builder)
    {
        LocalDate localDate = LocalDate.ofEpochDay(DATE.getInt(block, position));
        DATE_FORMATTER.formatTo(localDate, builder);
    }

    public static String formatHiveTimestamp(Type type, Block block, int position)
    {
        SqlTimestamp objectValue = (SqlTimestamp) type.getObjectValue(block, position);
        LocalDateTime localDateTime = objectValue.toLocalDateTime();
        return TIMESTAMP_FORMATTER.format(localDateTime);
    }

    public static void formatHiveTimestamp(Type type, Block block, int position, StringBuilder builder)
    {
        SqlTimestamp objectValue = (SqlTimestamp) type.getObjectValue(block, position);
        LocalDateTime localDateTime = objectValue.toLocalDateTime();
        TIMESTAMP_FORMATTER.formatTo(localDateTime, builder);
    }
}
