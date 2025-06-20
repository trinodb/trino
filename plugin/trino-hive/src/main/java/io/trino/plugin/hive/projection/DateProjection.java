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
package io.trino.plugin.hive.projection;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.TrinoException;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.DateType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.hive.projection.PartitionProjectionProperties.COLUMN_PROJECTION_FORMAT;
import static io.trino.plugin.hive.projection.PartitionProjectionProperties.COLUMN_PROJECTION_INTERVAL;
import static io.trino.plugin.hive.projection.PartitionProjectionProperties.COLUMN_PROJECTION_INTERVAL_UNIT;
import static io.trino.plugin.hive.projection.PartitionProjectionProperties.COLUMN_PROJECTION_RANGE;
import static io.trino.plugin.hive.projection.PartitionProjectionProperties.getProjectionPropertyRequiredValue;
import static io.trino.plugin.hive.projection.PartitionProjectionProperties.getProjectionPropertyValue;
import static io.trino.plugin.hive.util.HiveUtil.HIVE_DATE_PARSER;
import static io.trino.plugin.hive.util.HiveUtil.HIVE_TIMESTAMP_PARSER;
import static io.trino.plugin.hive.util.HiveUtil.parseHiveDate;
import static io.trino.plugin.hive.util.HiveUtil.parseHiveTimestamp;
import static io.trino.plugin.hive.util.HiveWriteUtils.HIVE_DATE_FORMATTER;
import static io.trino.plugin.hive.util.HiveWriteUtils.HIVE_TIMESTAMP_FORMATTER;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.time.temporal.ChronoUnit.MONTHS;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

final class DateProjection
        implements Projection
{
    private static final ZoneId UTC_TIME_ZONE_ID = ZoneId.of("UTC");
    // Limited to only DAYS, HOURS, MINUTES, SECONDS as we are not fully sure how everything above day
    // is implemented in Athena. So we limit it to a subset of interval units which are explicitly clear how to calculate.
    // The rest will be implemented if this is required as it would require making compatibility tests
    // for results received from Athena and verifying if we receive identical with Trino.
    private static final Set<ChronoUnit> DATE_PROJECTION_INTERVAL_UNITS = ImmutableSet.of(DAYS, HOURS, MINUTES, SECONDS);
    private static final Pattern DATE_RANGE_BOUND_EXPRESSION_PATTERN = Pattern.compile("^\\s*NOW\\s*(([+-])\\s*([0-9]+)\\s*(DAY|HOUR|MINUTE|SECOND)S?\\s*)?$");

    private final String columnName;
    private final String dateFormatPattern;
    private final DateTimeFormatter dateFormat;
    private final Instant leftBound;
    private final Instant rightBound;
    private final Type columnType;
    private final int interval;
    private final ChronoUnit intervalUnit;

    public DateProjection(String columnName, Type columnType, Map<String, Object> columnProperties)
    {
        if (!(columnType instanceof VarcharType) &&
                !(columnType instanceof DateType) &&
                !(columnType instanceof TimestampType timestampType && timestampType.isShort())) {
            throw new InvalidProjectionException(columnName, columnType);
        }

        this.columnName = requireNonNull(columnName, "columnName is null");
        this.columnType = requireNonNull(columnType, "columnType is null");

        String dateFormatPattern = getProjectionPropertyRequiredValue(
                columnName,
                columnProperties,
                COLUMN_PROJECTION_FORMAT,
                String::valueOf);
        this.dateFormatPattern = requireNonNull(dateFormatPattern, "dateFormatPattern is null");

        List<String> range = getProjectionPropertyRequiredValue(
                columnName,
                columnProperties,
                COLUMN_PROJECTION_RANGE,
                value -> ((List<?>) value).stream()
                        .map(String.class::cast)
                        .collect(toImmutableList()));
        if (range.size() != 2) {
            throw invalidRangeProperty(columnName, dateFormatPattern, Optional.empty());
        }

        this.dateFormat = DateTimeFormatter.ofPattern(dateFormatPattern, ENGLISH);

        leftBound = parseDateRangerBound(columnName, range.get(0), dateFormatPattern);
        rightBound = parseDateRangerBound(columnName, range.get(1), dateFormatPattern);
        if (!leftBound.isBefore(rightBound)) {
            throw invalidRangeProperty(columnName, dateFormatPattern, Optional.empty());
        }

        interval = getProjectionPropertyValue(columnProperties, COLUMN_PROJECTION_INTERVAL, Integer.class::cast).orElse(1);
        intervalUnit = getProjectionPropertyValue(columnProperties, COLUMN_PROJECTION_INTERVAL_UNIT, ChronoUnit.class::cast)
                .orElseGet(() -> resolveDefaultChronoUnit(columnName, dateFormatPattern));

        if (!DATE_PROJECTION_INTERVAL_UNITS.contains(intervalUnit)) {
            throw new InvalidProjectionException(
                    columnName,
                    format(
                            "Property: '%s' value '%s' is invalid. Available options: %s",
                            COLUMN_PROJECTION_INTERVAL_UNIT,
                            intervalUnit,
                            DATE_PROJECTION_INTERVAL_UNITS));
        }
    }

    @Override
    public List<String> getProjectedValues(Optional<Domain> partitionValueFilter)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();

        Instant leftBound = adjustBoundToDateFormat(this.leftBound);
        Instant rightBound = adjustBoundToDateFormat(this.rightBound);

        Instant currentValue = leftBound;
        while (!currentValue.isAfter(rightBound)) {
            String currentValueFormatted = formatValue(currentValue);
            if (isValueInDomain(partitionValueFilter, currentValue, currentValueFormatted)) {
                builder.add(currentValueFormatted);
            }
            currentValue = currentValue.atZone(UTC_TIME_ZONE_ID)
                    .plus(interval, intervalUnit)
                    .toInstant();
        }

        return builder.build();
    }

    @Override
    public String toPartitionLocationTemplateValue(String value)
    {
        if (columnType instanceof DateType) {
            long days = parseHiveDate(value);
            return LocalDate.ofEpochDay(days).format(dateFormat);
        }

        if (columnType instanceof TimestampType timestampType && timestampType.isShort()) {
            long epochMicros = parseHiveTimestamp(value);
            long epochSeconds = floorDiv(epochMicros, MICROSECONDS_PER_SECOND);
            int nanosOfSecond = floorMod(epochMicros, MICROSECONDS_PER_SECOND) * NANOSECONDS_PER_MICROSECOND;
            return LocalDateTime.ofEpochSecond(epochSeconds, nanosOfSecond, ZoneOffset.UTC).format(dateFormat);
        }

        return value;
    }

    private Instant adjustBoundToDateFormat(Instant value)
    {
        String formatted = LocalDateTime.ofInstant(value.with(ChronoField.MILLI_OF_SECOND, 0), UTC_TIME_ZONE_ID)
                .format(dateFormat);
        try {
            return parse(formatted);
        }
        catch (DateTimeParseException e) {
            throw new InvalidProjectionException(formatted, e.getMessage());
        }
    }

    private String formatValue(Instant current)
    {
        if (columnType instanceof DateType) {
            return LocalDate.ofEpochDay(MILLISECONDS.toDays(current.toEpochMilli())).format(HIVE_DATE_FORMATTER);
        }

        if (columnType instanceof TimestampType timestampType && timestampType.isShort()) {
            long epochMicros = MILLISECONDS.toMicros(current.toEpochMilli());
            long epochSeconds = floorDiv(epochMicros, MICROSECONDS_PER_SECOND);
            int nanosOfSecond = floorMod(epochMicros, MICROSECONDS_PER_SECOND) * NANOSECONDS_PER_MICROSECOND;
            return LocalDateTime.ofEpochSecond(epochSeconds, nanosOfSecond, ZoneOffset.UTC).format(HIVE_TIMESTAMP_FORMATTER);
        }

        LocalDateTime localDateTime = LocalDateTime.ofInstant(current, UTC_TIME_ZONE_ID);
        return localDateTime.format(dateFormat);
    }

    // TODO: remove once we support write custom format partition projection
    public void checkWriteSupported()
    {
        String formatted = toPartitionLocationTemplateValue(formatValue(Instant.now()));
        if (columnType instanceof DateType) {
            try {
                HIVE_DATE_PARSER.parseLocalDateTime(formatted);
            }
            catch (IllegalArgumentException e) {
                throw new TrinoException(
                        NOT_SUPPORTED,
                        format("Writing to date partition projection column '%s' with format '%s' is not supported", columnName, dateFormatPattern));
            }
        }

        if (columnType instanceof TimestampType timestampType && timestampType.isShort()) {
            try {
                HIVE_TIMESTAMP_PARSER.parseLocalDateTime(formatted);
            }
            catch (IllegalArgumentException e) {
                throw new TrinoException(
                        NOT_SUPPORTED,
                        format("Writing to date partition projection column '%s' with format '%s' is not supported", columnName, dateFormatPattern));
            }
        }
    }

    private boolean isValueInDomain(Optional<Domain> valueDomain, Instant value, String formattedValue)
    {
        if (valueDomain.isEmpty() || valueDomain.get().isAll()) {
            return true;
        }
        Domain domain = valueDomain.get();
        Type type = domain.getType();
        if (type instanceof VarcharType) {
            return domain.contains(singleValue(type, utf8Slice(formattedValue)));
        }
        if (type instanceof DateType) {
            return domain.contains(singleValue(type, MILLISECONDS.toDays(value.toEpochMilli())));
        }
        if (type instanceof TimestampType timestampType && timestampType.isShort()) {
            return domain.contains(singleValue(type, MILLISECONDS.toMicros(value.toEpochMilli())));
        }
        throw new InvalidProjectionException(columnName, type);
    }

    private static ChronoUnit resolveDefaultChronoUnit(String columnName, String dateFormatPattern)
    {
        String datePatternWithoutText = dateFormatPattern.replaceAll("'.*?'", "");
        if (datePatternWithoutText.contains("S") || datePatternWithoutText.contains("s")
                || datePatternWithoutText.contains("m") || datePatternWithoutText.contains("H")) {
            // When the provided dates are at single-day or single-month precision.
            throw new InvalidProjectionException(
                    columnName,
                    format(
                            "Property: '%s' needs to be set when provided '%s' is less that single-day precision. Interval defaults to 1 day or 1 month, respectively. Otherwise, interval is required",
                            COLUMN_PROJECTION_INTERVAL_UNIT,
                            COLUMN_PROJECTION_FORMAT));
        }
        if (datePatternWithoutText.contains("d")) {
            return DAYS;
        }
        return MONTHS;
    }

    private Instant parseDateRangerBound(String columnName, String value, String dateFormatPattern)
    {
        Matcher matcher = DATE_RANGE_BOUND_EXPRESSION_PATTERN.matcher(value);
        if (matcher.matches()) {
            String operator = matcher.group(2);
            String multiplierString = matcher.group(3);
            String unitString = matcher.group(4);
            if (nonNull(operator) && nonNull(multiplierString) && nonNull(unitString)) {
                unitString = unitString.toUpperCase(ENGLISH);
                int multiplier = Integer.parseInt(multiplierString);
                boolean increment = operator.charAt(0) == '+';
                ChronoUnit unit = ChronoUnit.valueOf(unitString + "S");
                return Instant.now().plus(increment ? multiplier : -multiplier, unit);
            }
            if (value.trim().equals("NOW")) {
                return Instant.now();
            }
            throw invalidRangeProperty(columnName, dateFormatPattern, Optional.of("Invalid expression"));
        }

        try {
            return parse(value);
        }
        catch (DateTimeParseException e) {
            throw invalidRangeProperty(columnName, dateFormatPattern, Optional.of(e.getMessage()));
        }
    }

    private Instant parse(String value)
            throws DateTimeParseException
    {
        TemporalAccessor parsed = dateFormat.parse(value);
        if (parsed.query(TemporalQueries.localDate()) != null && parsed.query(TemporalQueries.localTime()) == null) {
            return LocalDate.from(parsed).atStartOfDay().toInstant(UTC);
        }
        return LocalDateTime.from(parsed).toInstant(UTC);
    }

    private static TrinoException invalidRangeProperty(String columnName, String dateFormatPattern, Optional<String> errorDetail)
    {
        throw new InvalidProjectionException(
                columnName,
                format(
                        "Property: '%s' needs to be a list of 2 valid dates formatted as '%s' or '%s' that are sequential%s",
                        COLUMN_PROJECTION_RANGE,
                        dateFormatPattern,
                        DATE_RANGE_BOUND_EXPRESSION_PATTERN.pattern(),
                        errorDetail.map(error -> ": " + error).orElse("")));
    }
}
