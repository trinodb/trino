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
package io.trino.plugin.hive.aws.athena.projection;

import com.google.common.collect.ImmutableList;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.DateType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.text.DateFormat;
import java.text.ParseException;
import java.time.Instant;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.hive.aws.athena.projection.DateProjectionFactory.UTC_TIME_ZONE_ID;
import static io.trino.spi.predicate.Domain.singleValue;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class DateProjection
        extends Projection
{
    private final DateFormat dateFormat;
    private final Supplier<Instant> leftBound;
    private final Supplier<Instant> rightBound;
    private final int interval;
    private final ChronoUnit intervalUnit;

    public DateProjection(String columnName, DateFormat dateFormat, Supplier<Instant> leftBound, Supplier<Instant> rightBound, int interval, ChronoUnit intervalUnit)
    {
        super(columnName);
        this.dateFormat = requireNonNull(dateFormat, "dateFormatPattern is null");
        this.leftBound = requireNonNull(leftBound, "leftBound is null");
        this.rightBound = requireNonNull(rightBound, "rightBound is null");
        this.interval = interval;
        this.intervalUnit = requireNonNull(intervalUnit, "intervalUnit is null");
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
            throw invalidProjectionException(formatted, e.getMessage());
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
        throw unsupportedProjectionColumnTypeException(type);
    }
}
