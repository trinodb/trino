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
package io.trino.plugin.iceberg;

import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Types;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.HexFormat;
import java.util.Optional;
import java.util.UUID;

import static io.trino.plugin.base.io.ByteBuffers.getWrappedBytes;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static java.lang.Double.parseDouble;
import static java.lang.Float.parseFloat;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.lang.Math.toIntExact;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class IcebergDefaultValues
{
    private static final DateTimeFormatter TIMESTAMP_TZ_FORMATTER = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss"))
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .appendLiteral(' ')
            .appendZoneId()
            .toFormatter();

    private IcebergDefaultValues() {}

    /**
     * Formats an Iceberg default value as a SQL expression string for display and parsing.
     * <p>
     * This is used to populate {@link ColumnMetadata#getDefaultValue()} which is displayed
     * in SHOW CREATE TABLE and used by the planner for INSERT operations.
     *
     * @return SQL expression string, or empty if no default is set
     */
    public static Optional<String> formatIcebergDefaultAsSql(Object icebergDefault, org.apache.iceberg.types.Type icebergType)
    {
        if (icebergDefault == null) {
            return Optional.empty();
        }

        return Optional.of(switch (icebergType.typeId()) {
            case BOOLEAN, INTEGER, LONG -> icebergDefault.toString();
            case FLOAT -> {
                // Format as REAL literal to ensure Trino parses it as REAL, not DECIMAL
                String value;
                if (icebergDefault instanceof Number number) {
                    value = String.valueOf(number.floatValue());
                }
                else if (icebergDefault instanceof String str) {
                    value = extractNumericValue(str);
                }
                else {
                    value = icebergDefault.toString();
                }
                yield "REAL '" + value + "'";
            }
            case DOUBLE -> {
                // Format as DOUBLE literal to ensure Trino parses it as DOUBLE, not DECIMAL
                String value;
                if (icebergDefault instanceof Number number) {
                    value = String.valueOf(number.doubleValue());
                }
                else if (icebergDefault instanceof String str) {
                    value = extractNumericValue(str);
                }
                else {
                    value = icebergDefault.toString();
                }
                yield "DOUBLE '" + value + "'";
            }
            case DATE -> {
                // Iceberg stores as Integer (days since epoch)
                int days = (Integer) icebergDefault;
                yield "DATE '" + LocalDate.ofEpochDay(days) + "'";
            }
            case TIME -> {
                // Iceberg stores as Long (microseconds since midnight)
                long micros = (Long) icebergDefault;
                long nanos = micros * 1000;
                yield "TIME '" + LocalTime.ofNanoOfDay(nanos) + "'";
            }
            case TIMESTAMP -> {
                // Iceberg stores as Long (microseconds since epoch)
                long micros = (Long) icebergDefault;
                Instant instant = Instant.ofEpochSecond(micros / 1_000_000, (micros % 1_000_000) * 1000);
                LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
                // Format with space separator (Trino SQL format) instead of 'T' (ISO 8601)
                // Use 'uuuu' (proleptic year) instead of 'yyyy' to correctly handle year 0
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSSSS");
                // Check if this is timestamp with time zone
                if (icebergType instanceof Types.TimestampType timestampType && timestampType.shouldAdjustToUTC()) {
                    yield "TIMESTAMP '" + localDateTime.format(formatter) + " UTC'";
                }
                yield "TIMESTAMP '" + localDateTime.format(formatter) + "'";
            }
            case STRING -> {
                // Escape single quotes by doubling them
                String value = icebergDefault.toString().replace("'", "''");
                yield "'" + value + "'";
            }
            case UUID -> "UUID '" + icebergDefault + "'";
            case BINARY, FIXED -> {
                ByteBuffer buffer = (ByteBuffer) icebergDefault;
                byte[] bytes = getWrappedBytes(buffer);
                yield "X'" + HexFormat.of().formatHex(bytes).toUpperCase(ENGLISH) + "'";
            }
            case DECIMAL -> "DECIMAL '%s'".formatted(icebergDefault);
            default -> throw new TrinoException(NOT_SUPPORTED, "Unsupported default value type: " + icebergType);
        });
    }

    /**
     * Parses a SQL literal expression and converts it to an Iceberg native value.
     * <p>
     * This is the inverse of {@link #formatIcebergDefaultAsSql}. It handles simple literals
     * that Trino's analyzer accepts for DEFAULT column values.
     *
     * @param sqlExpression The SQL literal expression (e.g., "42", "'hello'", "DATE '2020-01-01'")
     * @return The Iceberg native value for the default
     */
    public static Object parseDefaultValue(String sqlExpression, Type trinoType, org.apache.iceberg.types.Type icebergType)
    {
        requireNonNull(sqlExpression, "sqlExpression is null");

        // Handle NULL literal - applies to all types
        if (sqlExpression.equalsIgnoreCase("null")) {
            return null;
        }

        // Parse boolean literals
        if (trinoType == BOOLEAN) {
            if (sqlExpression.equalsIgnoreCase("true")) {
                return true;
            }
            if (sqlExpression.equalsIgnoreCase("false")) {
                return false;
            }
            throw new TrinoException(INVALID_ARGUMENTS, "Invalid boolean default value: " + sqlExpression);
        }

        // Parse integer literals
        if (trinoType == INTEGER) {
            try {
                return parseInt(sqlExpression);
            }
            catch (NumberFormatException e) {
                throw new TrinoException(INVALID_ARGUMENTS, "Invalid integer default value: " + sqlExpression);
            }
        }

        // Parse bigint literals
        if (trinoType == BIGINT) {
            try {
                return parseLong(sqlExpression);
            }
            catch (NumberFormatException e) {
                throw new TrinoException(INVALID_ARGUMENTS, "Invalid bigint default value: " + sqlExpression);
            }
        }

        // Parse real literals
        if (trinoType == REAL) {
            try {
                return parseFloat(extractNumericValue(sqlExpression));
            }
            catch (NumberFormatException e) {
                throw new TrinoException(INVALID_ARGUMENTS, "Invalid real default value: " + sqlExpression);
            }
        }

        // Parse double literals
        if (trinoType == DOUBLE) {
            try {
                return parseDouble(extractNumericValue(sqlExpression));
            }
            catch (NumberFormatException e) {
                throw new TrinoException(INVALID_ARGUMENTS, "Invalid double default value: " + sqlExpression);
            }
        }

        // Parse string literals: 'value' -> value
        if (trinoType instanceof VarcharType) {
            if (sqlExpression.startsWith("'") && sqlExpression.endsWith("'") && sqlExpression.length() >= 2) {
                // Unescape doubled single quotes
                return sqlExpression.substring(1, sqlExpression.length() - 1).replace("''", "'");
            }
            throw new TrinoException(INVALID_ARGUMENTS, "Invalid varchar default value: " + sqlExpression);
        }

        // Parse decimal literals
        if (trinoType instanceof DecimalType) {
            try {
                return new BigDecimal(extractNumericValue(sqlExpression));
            }
            catch (NumberFormatException e) {
                throw new TrinoException(INVALID_ARGUMENTS, "Invalid decimal default value: " + sqlExpression);
            }
        }

        // Parse date literals: DATE 'YYYY-MM-DD' -> days since epoch
        if (trinoType == DATE) {
            String datePattern = "DATE '";
            if (sqlExpression.toUpperCase(ENGLISH).startsWith(datePattern) && sqlExpression.endsWith("'")) {
                String dateString = sqlExpression.substring(datePattern.length(), sqlExpression.length() - 1);
                try {
                    return toIntExact(LocalDate.parse(dateString).toEpochDay());
                }
                catch (DateTimeParseException e) {
                    throw new TrinoException(INVALID_ARGUMENTS, "Invalid date default value: " + sqlExpression);
                }
            }
            throw new TrinoException(INVALID_ARGUMENTS, "Invalid date default value format: " + sqlExpression);
        }

        // Parse time literals: TIME 'HH:MM:SS.ssssss' -> microseconds since midnight
        if (trinoType.equals(TIME_MICROS)) {
            String timePattern = "TIME '";
            if (sqlExpression.toUpperCase(ENGLISH).startsWith(timePattern) && sqlExpression.endsWith("'")) {
                String timeString = sqlExpression.substring(timePattern.length(), sqlExpression.length() - 1);
                try {
                    return LocalTime.parse(timeString).toNanoOfDay() / 1000;  // Convert nanos to micros
                }
                catch (DateTimeParseException e) {
                    throw new TrinoException(INVALID_ARGUMENTS, "Invalid time default value: " + sqlExpression);
                }
            }
            throw new TrinoException(INVALID_ARGUMENTS, "Invalid time default value format: " + sqlExpression);
        }

        // Parse timestamp literals: TIMESTAMP 'YYYY-MM-DD HH:MM:SS.ssssss' -> microseconds since epoch
        if (trinoType.equals(TIMESTAMP_MICROS)) {
            String timestampPattern = "TIMESTAMP '";
            if (sqlExpression.toUpperCase(ENGLISH).startsWith(timestampPattern) && sqlExpression.endsWith("'")) {
                String timestampString = sqlExpression.substring(timestampPattern.length(), sqlExpression.length() - 1);
                try {
                    // Trino uses space separator, not 'T' like ISO 8601
                    DateTimeFormatter formatter = new DateTimeFormatterBuilder()
                            .parseCaseInsensitive()
                            .append(DateTimeFormatter.ISO_LOCAL_DATE)
                            .appendLiteral(' ')
                            .append(DateTimeFormatter.ISO_LOCAL_TIME)
                            .toFormatter();
                    LocalDateTime ldt = LocalDateTime.parse(timestampString, formatter);
                    Instant instant = ldt.toInstant(ZoneOffset.UTC);
                    return instant.getEpochSecond() * 1_000_000 + instant.getNano() / 1000;
                }
                catch (DateTimeParseException e) {
                    throw new TrinoException(INVALID_ARGUMENTS, "Invalid timestamp default value: " + sqlExpression);
                }
            }
            throw new TrinoException(INVALID_ARGUMENTS, "Invalid timestamp default value format: " + sqlExpression);
        }

        // Parse timestamp with time zone literals: TIMESTAMP 'YYYY-MM-DD HH:MM:SS.ssssss TIMEZONE' -> microseconds since epoch (UTC)
        if (trinoType.equals(TIMESTAMP_TZ_MICROS)) {
            String timestampPattern = "TIMESTAMP '";
            if (sqlExpression.toUpperCase(ENGLISH).startsWith(timestampPattern) && sqlExpression.endsWith("'")) {
                String timestampString = sqlExpression.substring(timestampPattern.length(), sqlExpression.length() - 1);
                try {
                    ZonedDateTime zdt = ZonedDateTime.parse(timestampString, TIMESTAMP_TZ_FORMATTER);
                    Instant instant = zdt.toInstant();
                    return instant.getEpochSecond() * 1_000_000 + instant.getNano() / 1000;
                }
                catch (DateTimeParseException e) {
                    throw new TrinoException(INVALID_ARGUMENTS, "Invalid timestamp with time zone default value: " + sqlExpression, e);
                }
            }
            throw new TrinoException(INVALID_ARGUMENTS, "Invalid timestamp with time zone default value format: " + sqlExpression);
        }

        // Parse UUID literals: UUID 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx'
        if (trinoType.equals(UuidType.UUID)) {
            String uuidPattern = "UUID '";
            if (sqlExpression.toUpperCase(ENGLISH).startsWith(uuidPattern) && sqlExpression.endsWith("'")) {
                String uuidString = sqlExpression.substring(uuidPattern.length(), sqlExpression.length() - 1);
                try {
                    return UUID.fromString(uuidString);
                }
                catch (IllegalArgumentException e) {
                    throw new TrinoException(INVALID_ARGUMENTS, "Invalid UUID default value: " + sqlExpression);
                }
            }
            throw new TrinoException(INVALID_ARGUMENTS, "Invalid UUID default value format: " + sqlExpression);
        }

        // Parse binary literals: X'hex' -> ByteBuffer
        if (trinoType.equals(VarbinaryType.VARBINARY)) {
            String binaryPattern = "X'";
            if (sqlExpression.toUpperCase(ENGLISH).startsWith(binaryPattern) && sqlExpression.endsWith("'")) {
                String hexString = sqlExpression.substring(binaryPattern.length(), sqlExpression.length() - 1);
                try {
                    byte[] bytes = HexFormat.of().parseHex(hexString);
                    return ByteBuffer.wrap(bytes);
                }
                catch (IllegalArgumentException e) {
                    throw new TrinoException(INVALID_ARGUMENTS, "Invalid binary default value: " + sqlExpression);
                }
            }
            throw new TrinoException(INVALID_ARGUMENTS, "Invalid binary default value format: " + sqlExpression);
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported default value type: " + trinoType.getDisplayName());
    }

    /**
     * Extracts the numeric value from SQL expressions.
     * Handles formats like "123", "DECIMAL '123.45'", "REAL '1.5'", etc.
     */
    private static String extractNumericValue(String sqlExpression)
    {
        // Handle typed literal format: TYPE 'value' (e.g., DECIMAL '123.45')
        if (sqlExpression.contains("'")) {
            int firstQuote = sqlExpression.indexOf('\'');
            int lastQuote = sqlExpression.lastIndexOf('\'');
            if (firstQuote >= 0 && lastQuote > firstQuote) {
                return sqlExpression.substring(firstQuote + 1, lastQuote);
            }
        }
        // Plain numeric value
        return sqlExpression;
    }

    /**
     * Converts a parsed default value Object to an Iceberg Literal.
     * This is needed for the UpdateSchema API which requires Literal types.
     *
     * @param value The parsed value from parseDefaultValue
     * @param icebergType The Iceberg type
     * @return The Iceberg Literal
     */
    public static Literal<?> toIcebergLiteral(Object value, org.apache.iceberg.types.Type icebergType)
    {
        if (value == null) {
            return null;
        }

        return switch (icebergType.typeId()) {
            case BOOLEAN -> Literal.of((Boolean) value);
            case INTEGER, DATE -> Literal.of((Integer) value);
            case LONG, TIME, TIMESTAMP -> Literal.of((Long) value);
            case FLOAT -> Literal.of((Float) value);
            case DOUBLE -> Literal.of((Double) value);
            case STRING -> Literal.of((CharSequence) value);
            case UUID -> Literal.of((UUID) value);
            case BINARY, FIXED -> Literal.of((ByteBuffer) value);
            case DECIMAL -> Literal.of((BigDecimal) value);
            default -> throw new TrinoException(NOT_SUPPORTED, "Unsupported type for literal conversion: " + icebergType);
        };
    }
}
