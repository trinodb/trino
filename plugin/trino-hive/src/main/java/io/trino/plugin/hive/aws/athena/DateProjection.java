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
package io.trino.plugin.hive.aws.athena;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.TrinoException;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.DateType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.COLUMN_PROJECTION_FORMAT;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.COLUMN_PROJECTION_INTERVAL;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.COLUMN_PROJECTION_INTERVAL_UNIT;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.COLUMN_PROJECTION_RANGE;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.getProjectionPropertyRequiredValue;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.getProjectionPropertyValue;
import static io.trino.spi.predicate.Domain.singleValue;
import static java.lang.String.format;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.time.temporal.ChronoUnit.MONTHS;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;
import static java.util.TimeZone.getTimeZone;
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
    private final DateFormat dateFormat;
    private final Supplier<Instant> leftBound;
    private final Supplier<Instant> rightBound;
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

        String dateFormatPattern = getProjectionPropertyRequiredValue(
                columnName,
                columnProperties,
                COLUMN_PROJECTION_FORMAT,
                String::valueOf);

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

        SimpleDateFormat dateFormat = new SimpleDateFormat(dateFormatPattern);
        dateFormat.setLenient(false);
        dateFormat.setTimeZone(getTimeZone(UTC_TIME_ZONE_ID));
        this.dateFormat = requireNonNull(dateFormat, "dateFormatPattern is null");

        leftBound = parseDateRangerBound(columnName, range.get(0), dateFormat);
        rightBound = parseDateRangerBound(columnName, range.get(1), dateFormat);
        if (!leftBound.get().isBefore(rightBound.get())) {
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

        Instant leftBound = adjustBoundToDateFormat(this.leftBound.get());
        Instant rightBound = adjustBoundToDateFormat(this.rightBound.get());

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

    private Instant adjustBoundToDateFormat(Instant value)
    {
        String formatted = formatValue(value.with(ChronoField.MILLI_OF_SECOND, 0));
        try {
            return dateFormat.parse(formatted).toInstant();
        }
        catch (ParseException e) {
            throw new InvalidProjectionException(formatted, e.getMessage());
        }
    }

    private String formatValue(Instant current)
    {
        return dateFormat.format(new Date(current.toEpochMilli()));
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
        if (type instanceof TimestampType && ((TimestampType) type).isShort()) {
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

    private static Supplier<Instant> parseDateRangerBound(String columnName, String value, SimpleDateFormat dateFormat)
    {
        Matcher matcher = DATE_RANGE_BOUND_EXPRESSION_PATTERN.matcher(value);
        if (matcher.matches()) {
            String operator = matcher.group(2);
            String multiplierString = matcher.group(3);
            String unitString = matcher.group(4);
            if (nonNull(operator) && nonNull(multiplierString) && nonNull(unitString)) {
                unitString = unitString.toUpperCase(Locale.ENGLISH);
                return new DateExpressionBound(
                        Integer.parseInt(multiplierString),
                        ChronoUnit.valueOf(unitString + "S"),
                        operator.charAt(0) == '+');
            }
            if (value.trim().equals("NOW")) {
                Instant now = Instant.now();
                return () -> now;
            }
            throw invalidRangeProperty(columnName, dateFormat.toPattern(), Optional.of("Invalid expression"));
        }

        Instant dateBound;
        try {
            dateBound = dateFormat.parse(value).toInstant();
        }
        catch (ParseException e) {
            throw invalidRangeProperty(columnName, dateFormat.toPattern(), Optional.of(e.getMessage()));
        }
        return () -> dateBound;
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

    private record DateExpressionBound(int multiplier, ChronoUnit unit, boolean increment)
            implements Supplier<Instant>
    {
        @Override
        public Instant get()
        {
            return Instant.now().plus(increment ? multiplier : -multiplier, unit);
        }
    }
}
