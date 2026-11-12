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

package io.trino.plugin.faker;

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.Duration.succinctNanos;
import static io.airlift.units.Duration.timeUnitToString;
import static io.trino.plugin.faker.ColumnInfo.ALLOWED_VALUES_PROPERTY;
import static io.trino.plugin.faker.ColumnInfo.DISTRIBUTION_PROPERTY;
import static io.trino.plugin.faker.ColumnInfo.GENERATOR_PROPERTY;
import static io.trino.plugin.faker.ColumnInfo.MAX_PROPERTY;
import static io.trino.plugin.faker.ColumnInfo.MIN_PROPERTY;
import static io.trino.plugin.faker.ColumnInfo.NULL_PROBABILITY_PROPERTY;
import static io.trino.plugin.faker.ColumnInfo.REFERENCES_PROPERTY;
import static io.trino.plugin.faker.ColumnInfo.STEP_PROPERTY;
import static io.trino.plugin.faker.Distribution.NORMAL;
import static io.trino.plugin.faker.Distribution.UNIFORM;
import static io.trino.plugin.faker.Distribution.ZIPF;
import static io.trino.plugin.faker.FakerPageSource.POWERS_OF_TEN;
import static io.trino.plugin.faker.PropertyValues.propertyValue;
import static io.trino.spi.StandardErrorCode.INVALID_COLUMN_PROPERTY;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_DAY;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public record FakerColumnHandle(
        int columnIndex,
        String name,
        Type type,
        double nullProbability,
        String generator,
        Domain domain,
        ValueSet step,
        Optional<ReferencedColumn> referencedColumn,
        Distribution distribution,
        List<String> rankedValues)
        implements ColumnHandle
{
    public FakerColumnHandle
    {
        requireNonNull(name, "name is null");
        requireNonNull(type, "type is null");
        requireNonNull(domain, "domain is null");
        requireNonNull(step, "step is null");
        requireNonNull(referencedColumn, "referencedColumn is null");
        checkState(step.isNone() || step.isSingleValue(), "step must be a single value");
        requireNonNull(distribution, "distribution is null");
        rankedValues = ImmutableList.copyOf(requireNonNull(rankedValues, "rankedValues is null"));
    }

    public static FakerColumnHandle of(int columnId, ColumnMetadata column, double defaultNullProbability)
    {
        double nullProbability = 0;
        if (column.isNullable()) {
            nullProbability = (double) column.getProperties().getOrDefault(NULL_PROBABILITY_PROPERTY, defaultNullProbability);
        }
        String generator = (String) column.getProperties().get(GENERATOR_PROPERTY);
        if (generator != null && !supportsGenerator(column.getType())) {
            throw new TrinoException(INVALID_COLUMN_PROPERTY, "The `%s` property cannot be set for ROW, ARRAY, MAP, or JSON columns".formatted(GENERATOR_PROPERTY));
        }
        if (generator != null && (column.getProperties().get(MIN_PROPERTY) != null || column.getProperties().get(MAX_PROPERTY) != null || column.getProperties().get(STEP_PROPERTY) != null)) {
            throw new TrinoException(INVALID_COLUMN_PROPERTY, "The `%s` property cannot be set together with `%s`, `%s`, and `%s` properties".formatted(GENERATOR_PROPERTY, MIN_PROPERTY, MAX_PROPERTY, STEP_PROPERTY));
        }
        Object min = propertyValue(column, MIN_PROPERTY);
        Object max = propertyValue(column, MAX_PROPERTY);
        Domain domain = Domain.all(column.getType());
        if (min != null || max != null) {
            if (isCharacterColumn(column)) {
                throw new TrinoException(INVALID_COLUMN_PROPERTY, "The `%s` and `%s` properties cannot be set for CHAR, VARCHAR or VARBINARY columns".formatted(MIN_PROPERTY, MAX_PROPERTY));
            }
            domain = Domain.create(ValueSet.ofRanges(range(column.getType(), min, max)), false);
        }
        Object allowedValues = propertyValue(column, ALLOWED_VALUES_PROPERTY);

        if (allowedValues != null) {
            if (((Collection<?>) allowedValues).isEmpty()) {
                throw new TrinoException(INVALID_COLUMN_PROPERTY, "The `%s` property must contain at least one value".formatted(ALLOWED_VALUES_PROPERTY));
            }
            if (min != null || max != null || generator != null) {
                throw new TrinoException(INVALID_COLUMN_PROPERTY, "The `%s` property cannot be set together with `%s`, `%s`, and `%s` properties".formatted(ALLOWED_VALUES_PROPERTY, MIN_PROPERTY, MAX_PROPERTY, GENERATOR_PROPERTY));
            }
            domain = Domain.create(ValueSet.copyOf(column.getType(), (Collection<?>) allowedValues), false);
        }
        ValueSet step = stepValue(column);

        if (column.getProperties().containsKey(REFERENCES_PROPERTY) && (min != null || max != null || !step.isNone() || allowedValues != null || generator != null)) {
            throw new TrinoException(INVALID_COLUMN_PROPERTY, "The `%s` property cannot be set together with `%s`, `%s`, `%s`, `%s`, and `%s` properties".formatted(REFERENCES_PROPERTY, MIN_PROPERTY, MAX_PROPERTY, STEP_PROPERTY, ALLOWED_VALUES_PROPERTY, GENERATOR_PROPERTY));
        }

        Distribution distribution = (Distribution) column.getProperties().getOrDefault(DISTRIBUTION_PROPERTY, UNIFORM);
        checkDistribution(column, distribution, min, max, allowedValues != null);
        List<String> rankedValues = ImmutableList.of();
        if (distribution == ZIPF && allowedValues != null) {
            // Zipf ranks allowed values in the order they were listed, but the domain sorts them, so keep the listed literals
            rankedValues = ((Collection<?>) column.getProperties().get(ALLOWED_VALUES_PROPERTY)).stream()
                    .map(String.class::cast)
                    .collect(toImmutableList());
        }

        return new FakerColumnHandle(
                columnId,
                column.getName(),
                column.getType(),
                nullProbability,
                generator,
                domain,
                step,
                Optional.empty(),
                distribution,
                rankedValues);
    }

    // Generated text is parsed like other property values, which is not supported for these types
    private static boolean supportsGenerator(Type type)
    {
        return !(type instanceof RowType || type instanceof ArrayType || type instanceof MapType || type.getBaseName().equals(StandardTypes.JSON));
    }

    private static void checkDistribution(ColumnMetadata column, Distribution distribution, Object min, Object max, boolean hasAllowedValues)
    {
        if (distribution == UNIFORM) {
            return;
        }
        String name = distribution.name().toLowerCase(ENGLISH);
        if (column.getProperties().get(STEP_PROPERTY) != null || column.getProperties().get(GENERATOR_PROPERTY) != null) {
            throw new TrinoException(INVALID_COLUMN_PROPERTY, "The `%s` distribution cannot be used together with the `%s` or `%s` properties".formatted(name, STEP_PROPERTY, GENERATOR_PROPERTY));
        }
        if (distribution == ZIPF && hasAllowedValues) {
            return;
        }
        if (!isDistributionSupported(column.getType(), distribution)) {
            if (distribution == ZIPF) {
                throw new TrinoException(INVALID_COLUMN_PROPERTY, "The `%s` distribution can only be used for %s columns together with the `%s` property".formatted(name, column.getType().getDisplayName(), ALLOWED_VALUES_PROPERTY));
            }
            throw new TrinoException(INVALID_COLUMN_PROPERTY, "The `%s` distribution cannot be used for %s columns".formatted(name, column.getType().getDisplayName()));
        }
        if (hasAllowedValues) {
            throw new TrinoException(INVALID_COLUMN_PROPERTY, "The `%s` distribution cannot be used together with the `%s` property".formatted(name, ALLOWED_VALUES_PROPERTY));
        }
        if (min == null || max == null) {
            if (distribution == ZIPF) {
                throw new TrinoException(INVALID_COLUMN_PROPERTY, "The `%s` distribution requires the `%s` and `%s` properties, or the `%s` property".formatted(name, MIN_PROPERTY, MAX_PROPERTY, ALLOWED_VALUES_PROPERTY));
            }
            throw new TrinoException(INVALID_COLUMN_PROPERTY, "The `%s` distribution requires the `%s` and `%s` properties".formatted(name, MIN_PROPERTY, MAX_PROPERTY));
        }
        // the mean and standard deviation of an infinite range are not finite
        if (distribution == NORMAL && !(isFinite(column.getType(), min) && isFinite(column.getType(), max))) {
            throw new TrinoException(INVALID_COLUMN_PROPERTY, "The `%s` distribution requires finite `%s` and `%s` values".formatted(name, MIN_PROPERTY, MAX_PROPERTY));
        }
    }

    private static boolean isFinite(Type type, Object value)
    {
        return switch (value) {
            case Long longValue when REAL.equals(type) -> Float.isFinite(intBitsToFloat(toIntExact(longValue)));
            case Double doubleValue -> Double.isFinite(doubleValue);
            default -> true;
        };
    }

    private static boolean isDistributionSupported(Type type, Distribution distribution)
    {
        if (BIGINT.equals(type) || INTEGER.equals(type) || SMALLINT.equals(type) || TINYINT.equals(type) || DATE.equals(type) || type instanceof TimestampType) {
            return true;
        }
        // Zipf ranks values by their distance from the min value in units of the type, but floating point types
        // have no fixed unit, and the unit of decimals depends on their scale, so most values would be crammed
        // into a tiny fraction of the range
        return distribution == NORMAL && (REAL.equals(type) || DOUBLE.equals(type) || type instanceof DecimalType);
    }

    private static boolean isCharacterColumn(ColumnMetadata column)
    {
        return column.getType() instanceof CharType || column.getType() instanceof VarcharType || column.getType() instanceof VarbinaryType;
    }

    private static ValueSet stepValue(ColumnMetadata column)
    {
        Type type = column.getType();
        if (column.getProperties().get(STEP_PROPERTY) == null) {
            return ValueSet.none(type);
        }
        if (isCharacterColumn(column)) {
            throw new TrinoException(INVALID_COLUMN_PROPERTY, "The `%s` property cannot be set for CHAR, VARCHAR or VARBINARY columns".formatted(STEP_PROPERTY));
        }
        if (!isSequenceSupported(type)) {
            throw new TrinoException(INVALID_COLUMN_PROPERTY, "The `%s` property cannot be set for %s columns".formatted(STEP_PROPERTY, type.getDisplayName()));
        }
        Object step = propertyValue(column, STEP_PROPERTY);
        boolean isDateTime = DATE.equals(type) || type instanceof TimestampType || type instanceof TimestampWithTimeZoneType || type instanceof TimeType || type instanceof TimeWithTimeZoneType;
        // date and time steps are durations in nanoseconds
        Type stepType = isDateTime ? BIGINT : type;
        if (!isPositive(stepType, step)) {
            throw new TrinoException(INVALID_COLUMN_PROPERTY, "The `%s` property must be greater than zero and finite".formatted(STEP_PROPERTY));
        }
        // a step shorter than the precision of the type generates repeated values
        if (isDateTime && (long) step < minimumStepNanos(type)) {
            Duration minimumStep = succinctNanos(minimumStepNanos(type));
            throw new TrinoException(INVALID_COLUMN_PROPERTY, "The `%s` property for a %s column must be at least %d%s".formatted(
                    STEP_PROPERTY, type.getDisplayName(), Math.round(minimumStep.getValue()), timeUnitToString(minimumStep.getUnit())));
        }
        return ValueSet.of(stepType, step);
    }

    private static long minimumStepNanos(Type type)
    {
        if (DATE.equals(type)) {
            return NANOSECONDS_PER_DAY;
        }
        int precision = switch (type) {
            case TimestampType timestampType -> timestampType.getPrecision();
            case TimestampWithTimeZoneType timestampWithTimeZoneType -> timestampWithTimeZoneType.getPrecision();
            case TimeType timeType -> timeType.getPrecision();
            case TimeWithTimeZoneType timeWithTimeZoneType -> timeWithTimeZoneType.getPrecision();
            default -> throw new VerifyException("Unexpected type: " + type);
        };
        return precision >= 9 ? 1 : POWERS_OF_TEN[9 - precision];
    }

    private static boolean isPositive(Type type, Object value)
    {
        return switch (value) {
            case Long longValue when REAL.equals(type) -> Float.isFinite(intBitsToFloat(toIntExact(longValue))) && intBitsToFloat(toIntExact(longValue)) > 0;
            case Long longValue -> longValue > 0;
            case Double doubleValue -> Double.isFinite(doubleValue) && doubleValue > 0;
            case Int128 int128Value -> !int128Value.isNegative() && !int128Value.isZero();
            default -> throw new VerifyException("Unexpected step value: " + value);
        };
    }

    private static boolean isSequenceSupported(Type type)
    {
        return BIGINT.equals(type) || INTEGER.equals(type) || SMALLINT.equals(type) || TINYINT.equals(type) ||
                type instanceof DecimalType || REAL.equals(type) || DOUBLE.equals(type) ||
                IntervalValues.isInterval(type) ||
                DATE.equals(type) || type instanceof TimestampType || type instanceof TimestampWithTimeZoneType || type instanceof TimeType || type instanceof TimeWithTimeZoneType;
    }

    private static Range range(Type type, Object min, Object max)
    {
        requireNonNull(type, "type is null");
        if (min == null && max == null) {
            return Range.all(type);
        }
        if (max == null) {
            return Range.greaterThanOrEqual(type, min);
        }
        if (min == null) {
            return Range.lessThanOrEqual(type, max);
        }
        return Range.range(type, min, true, max, true);
    }

    public Object parseGeneratedValue(String value)
    {
        try {
            return Literal.parse(value, type);
        }
        catch (RuntimeException e) {
            throw new TrinoException(INVALID_COLUMN_PROPERTY, "The `%s` property of column `%s` produced `%s`, which is not a valid %s value".formatted(GENERATOR_PROPERTY, name, value, type.getDisplayName()), e);
        }
    }

    public FakerColumnHandle withNullProbability(double nullProbability)
    {
        return new FakerColumnHandle(columnIndex, name, type, nullProbability, generator, domain, step, referencedColumn, distribution, rankedValues);
    }

    public FakerColumnHandle withDomain(Domain domain)
    {
        return new FakerColumnHandle(columnIndex, name, type, nullProbability, generator, domain, step, referencedColumn, distribution, rankedValues);
    }

    public FakerColumnHandle withStep(ValueSet step)
    {
        return new FakerColumnHandle(columnIndex, name, type, nullProbability, generator, domain, step, referencedColumn, distribution, rankedValues);
    }

    public FakerColumnHandle withReferencedColumn(ReferencedColumn referencedColumn)
    {
        return new FakerColumnHandle(columnIndex, name, type, nullProbability, generator, domain, step, Optional.of(referencedColumn), distribution, rankedValues);
    }
}
