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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.math.LongMath;
import io.airlift.slice.Slice;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.DateType;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;

import java.time.Instant;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.base.expression.ConnectorExpressions.and;
import static io.trino.plugin.base.expression.ConnectorExpressions.extractConjuncts;
import static io.trino.spi.expression.StandardFunctions.CAST_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static java.lang.Math.toIntExact;
import static java.math.RoundingMode.UNNECESSARY;
import static java.time.ZoneOffset.UTC;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class ConstraintExtractor
{
    private ConstraintExtractor() {}

    public static ExtractionResult extractTupleDomain(Constraint constraint)
    {
        TupleDomain<IcebergColumnHandle> result = constraint.getSummary()
                .transformKeys(IcebergColumnHandle.class::cast);
        ImmutableList.Builder<ConnectorExpression> remainingExpressions = ImmutableList.builder();
        for (ConnectorExpression conjunct : extractConjuncts(constraint.getExpression())) {
            Optional<TupleDomain<IcebergColumnHandle>> converted = toTupleDomain(conjunct, constraint.getAssignments());
            if (converted.isEmpty()) {
                remainingExpressions.add(conjunct);
            }
            else {
                result = result.intersect(converted.get());
                if (result.isNone()) {
                    return new ExtractionResult(TupleDomain.none(), Constant.TRUE);
                }
            }
        }
        return new ExtractionResult(result, and(remainingExpressions.build()));
    }

    private static Optional<TupleDomain<IcebergColumnHandle>> toTupleDomain(ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        if (expression instanceof Call call) {
            return toTupleDomain(call, assignments);
        }
        return Optional.empty();
    }

    private static Optional<TupleDomain<IcebergColumnHandle>> toTupleDomain(Call call, Map<String, ColumnHandle> assignments)
    {
        if (call.getArguments().size() == 2) {
            ConnectorExpression firstArgument = call.getArguments().get(0);
            ConnectorExpression secondArgument = call.getArguments().get(1);

            // Note: CanonicalizeExpressionRewriter ensures that constants are the second comparison argument.

            if (firstArgument instanceof Call firstAsCall && firstAsCall.getFunctionName().equals(CAST_FUNCTION_NAME) &&
                    secondArgument instanceof Constant constant &&
                    // if type do no match, this cannot be a comparison function
                    firstArgument.getType().equals(secondArgument.getType())) {
                return unwrapCastInComparison(
                        call.getFunctionName(),
                        getOnlyElement(firstAsCall.getArguments()),
                        constant,
                        assignments);
            }

            if (firstArgument instanceof Call firstAsCall &&
                    firstAsCall.getFunctionName().equals(new FunctionName("date_trunc")) && firstAsCall.getArguments().size() == 2 &&
                    firstAsCall.getArguments().get(0) instanceof Constant unit &&
                    secondArgument instanceof Constant constant &&
                    // if type do no match, this cannot be a comparison function
                    firstArgument.getType().equals(secondArgument.getType())) {
                return unwrapDateTruncInComparison(
                        call.getFunctionName(),
                        unit,
                        firstAsCall.getArguments().get(1),
                        constant,
                        assignments);
            }

            if (firstArgument instanceof Call firstAsCall &&
                    firstAsCall.getFunctionName().equals(new FunctionName("year")) &&
                    firstAsCall.getArguments().size() == 1 &&
                    getOnlyElement(firstAsCall.getArguments()).getType() instanceof TimestampWithTimeZoneType &&
                    secondArgument instanceof Constant constant &&
                    // if types do no match, this cannot be a comparison function
                    firstArgument.getType().equals(secondArgument.getType())) {
                return unwrapYearInTimestampTzComparison(
                        call.getFunctionName(),
                        getOnlyElement(firstAsCall.getArguments()),
                        constant,
                        assignments);
            }
        }

        return Optional.empty();
    }

    private static Optional<TupleDomain<IcebergColumnHandle>> unwrapCastInComparison(
            // upon invocation, we don't know if this really is a comparison
            FunctionName functionName,
            ConnectorExpression castSource,
            Constant constant,
            Map<String, ColumnHandle> assignments)
    {
        if (!(castSource instanceof Variable sourceVariable)) {
            // Engine unwraps casts in comparisons in UnwrapCastInComparison. Within a connector we can do more than
            // engine only for source columns. We cannot draw many conclusions for intermediate expressions without
            // knowing them well.
            return Optional.empty();
        }

        if (constant.getValue() == null) {
            // Comparisons with NULL should be simplified by the engine
            return Optional.empty();
        }

        IcebergColumnHandle column = resolve(sourceVariable, assignments);
        if (column.getType() instanceof TimestampWithTimeZoneType sourceType) {
            // Iceberg supports only timestamp(6) with time zone
            checkArgument(sourceType.getPrecision() == 6, "Unexpected type: %s", column.getType());

            if (constant.getType() == DateType.DATE) {
                return unwrapTimestampTzToDateCast(column, functionName, (long) constant.getValue())
                        .map(domain -> TupleDomain.withColumnDomains(ImmutableMap.of(column, domain)));
            }
            // TODO support timestamp constant
        }

        return Optional.empty();
    }

    private static Optional<Domain> unwrapTimestampTzToDateCast(IcebergColumnHandle column, FunctionName functionName, long date)
    {
        Type type = column.getType();
        checkArgument(type.equals(TIMESTAMP_TZ_MICROS), "Column of unexpected type %s: %s", type, column);

        // Verify no overflow. Date values must be in integer range.
        verify(date <= Integer.MAX_VALUE, "Date value out of range: %s", date);

        // In Iceberg, timestamp with time zone values are all in UTC

        LongTimestampWithTimeZone startOfDate = LongTimestampWithTimeZone.fromEpochMillisAndFraction(date * MILLISECONDS_PER_DAY, 0, UTC_KEY);
        LongTimestampWithTimeZone startOfNextDate = LongTimestampWithTimeZone.fromEpochMillisAndFraction((date + 1) * MILLISECONDS_PER_DAY, 0, UTC_KEY);

        return createDomain(functionName, type, startOfDate, startOfNextDate);
    }

    private static Optional<Domain> unwrapYearInTimestampTzComparison(FunctionName functionName, Type type, Constant constant)
    {
        checkArgument(constant.getValue() != null, "Unexpected constant: %s", constant);
        checkArgument(type.equals(TIMESTAMP_TZ_MICROS), "Unexpected type: %s", type);

        int year = toIntExact((Long) constant.getValue());
        ZonedDateTime periodStart = ZonedDateTime.of(year, 1, 1, 0, 0, 0, 0, UTC);
        ZonedDateTime periodEnd = periodStart.plusYears(1);

        LongTimestampWithTimeZone start = LongTimestampWithTimeZone.fromEpochSecondsAndFraction(periodStart.toEpochSecond(), 0, UTC_KEY);
        LongTimestampWithTimeZone end = LongTimestampWithTimeZone.fromEpochSecondsAndFraction(periodEnd.toEpochSecond(), 0, UTC_KEY);

        return createDomain(functionName, type, start, end);
    }

    private static Optional<Domain> createDomain(FunctionName functionName, Type type, LongTimestampWithTimeZone startOfDate, LongTimestampWithTimeZone startOfNextDate)
    {
        if (functionName.equals(EQUAL_OPERATOR_FUNCTION_NAME)) {
            return Optional.of(Domain.create(ValueSet.ofRanges(Range.range(type, startOfDate, true, startOfNextDate, false)), false));
        }
        if (functionName.equals(NOT_EQUAL_OPERATOR_FUNCTION_NAME)) {
            return Optional.of(Domain.create(ValueSet.ofRanges(Range.lessThan(type, startOfDate), Range.greaterThanOrEqual(type, startOfNextDate)), false));
        }
        if (functionName.equals(LESS_THAN_OPERATOR_FUNCTION_NAME)) {
            return Optional.of(Domain.create(ValueSet.ofRanges(Range.lessThan(type, startOfDate)), false));
        }
        if (functionName.equals(LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME)) {
            return Optional.of(Domain.create(ValueSet.ofRanges(Range.lessThan(type, startOfNextDate)), false));
        }
        if (functionName.equals(GREATER_THAN_OPERATOR_FUNCTION_NAME)) {
            return Optional.of(Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(type, startOfNextDate)), false));
        }
        if (functionName.equals(GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME)) {
            return Optional.of(Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(type, startOfDate)), false));
        }
        if (functionName.equals(IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME)) {
            return Optional.of(Domain.create(ValueSet.ofRanges(Range.lessThan(type, startOfDate), Range.greaterThanOrEqual(type, startOfNextDate)), true));
        }

        return Optional.empty();
    }

    private static Optional<TupleDomain<IcebergColumnHandle>> unwrapDateTruncInComparison(
            // upon invocation, we don't know if this really is a comparison
            FunctionName functionName,
            Constant unit,
            ConnectorExpression dateTruncSource,
            Constant constant,
            Map<String, ColumnHandle> assignments)
    {
        if (!(dateTruncSource instanceof Variable sourceVariable)) {
            // Engine unwraps date_trunc in comparisons in UnwrapDateTruncInComparison. Within a connector we can do more than
            // engine only for source columns. We cannot draw many conclusions for intermediate expressions without
            // knowing them well.
            return Optional.empty();
        }

        if (unit.getValue() == null) {
            return Optional.empty();
        }

        if (constant.getValue() == null) {
            // Comparisons with NULL should be simplified by the engine
            return Optional.empty();
        }

        IcebergColumnHandle column = resolve(sourceVariable, assignments);
        if (column.getType() instanceof TimestampWithTimeZoneType type) {
            // Iceberg supports only timestamp(6) with time zone
            checkArgument(type.getPrecision() == 6, "Unexpected type: %s", column.getType());
            verify(constant.getType().equals(type), "This method should not be invoked when type mismatch (i.e. surely not a comparison)");

            return unwrapDateTruncInComparison(((Slice) unit.getValue()).toStringUtf8(), functionName, constant)
                    .map(domain -> TupleDomain.withColumnDomains(ImmutableMap.of(column, domain)));
        }

        return Optional.empty();
    }

    private static Optional<Domain> unwrapDateTruncInComparison(String unit, FunctionName functionName, Constant constant)
    {
        Type type = constant.getType();
        checkArgument(constant.getValue() != null, "Unexpected constant: %s", constant);
        checkArgument(type.equals(TIMESTAMP_TZ_MICROS), "Unexpected type: %s", type);

        // Normalized to UTC because for comparisons the zone is irrelevant
        ZonedDateTime dateTime = Instant.ofEpochMilli(((LongTimestampWithTimeZone) constant.getValue()).getEpochMillis())
                .plusNanos(LongMath.divide(((LongTimestampWithTimeZone) constant.getValue()).getPicosOfMilli(), PICOSECONDS_PER_NANOSECOND, UNNECESSARY))
                .atZone(UTC);

        ZonedDateTime periodStart;
        ZonedDateTime nextPeriodStart;
        switch (unit.toLowerCase(ENGLISH)) {
            case "hour" -> {
                periodStart = ZonedDateTime.of(dateTime.toLocalDate(), LocalTime.of(dateTime.getHour(), 0), UTC);
                nextPeriodStart = periodStart.plusHours(1);
            }
            case "day" -> {
                periodStart = dateTime.toLocalDate().atStartOfDay().atZone(UTC);
                nextPeriodStart = periodStart.plusDays(1);
            }
            case "month" -> {
                periodStart = dateTime.toLocalDate().withDayOfMonth(1).atStartOfDay().atZone(UTC);
                nextPeriodStart = periodStart.plusMonths(1);
            }
            case "year" -> {
                periodStart = dateTime.toLocalDate().withMonth(1).withDayOfMonth(1).atStartOfDay().atZone(UTC);
                nextPeriodStart = periodStart.plusYears(1);
            }
            default -> {
                return Optional.empty();
            }
        }
        boolean constantAtPeriodStart = dateTime.equals(periodStart);

        LongTimestampWithTimeZone start = LongTimestampWithTimeZone.fromEpochSecondsAndFraction(periodStart.toEpochSecond(), 0, UTC_KEY);
        LongTimestampWithTimeZone end = LongTimestampWithTimeZone.fromEpochSecondsAndFraction(nextPeriodStart.toEpochSecond(), 0, UTC_KEY);

        if (functionName.equals(EQUAL_OPERATOR_FUNCTION_NAME)) {
            if (!constantAtPeriodStart) {
                return Optional.of(Domain.none(type));
            }
            return Optional.of(Domain.create(ValueSet.ofRanges(Range.range(type, start, true, end, false)), false));
        }
        if (functionName.equals(NOT_EQUAL_OPERATOR_FUNCTION_NAME)) {
            if (!constantAtPeriodStart) {
                return Optional.of(Domain.notNull(type));
            }
            return Optional.of(Domain.create(ValueSet.ofRanges(Range.lessThan(type, start), Range.greaterThanOrEqual(type, end)), false));
        }
        if (functionName.equals(IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME)) {
            if (!constantAtPeriodStart) {
                return Optional.of(Domain.all(type));
            }
            return Optional.of(Domain.create(ValueSet.ofRanges(Range.lessThan(type, start), Range.greaterThanOrEqual(type, end)), true));
        }
        if (functionName.equals(LESS_THAN_OPERATOR_FUNCTION_NAME)) {
            if (constantAtPeriodStart) {
                return Optional.of(Domain.create(ValueSet.ofRanges(Range.lessThan(type, start)), false));
            }
            return Optional.of(Domain.create(ValueSet.ofRanges(Range.lessThan(type, end)), false));
        }
        if (functionName.equals(LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME)) {
            return Optional.of(Domain.create(ValueSet.ofRanges(Range.lessThan(type, end)), false));
        }
        if (functionName.equals(GREATER_THAN_OPERATOR_FUNCTION_NAME)) {
            return Optional.of(Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(type, end)), false));
        }
        if (functionName.equals(GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME)) {
            if (constantAtPeriodStart) {
                return Optional.of(Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(type, start)), false));
            }
            return Optional.of(Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(type, end)), false));
        }
        return Optional.empty();
    }

    private static Optional<TupleDomain<IcebergColumnHandle>> unwrapYearInTimestampTzComparison(
            // upon invocation, we don't know if this really is a comparison
            FunctionName functionName,
            ConnectorExpression yearSource,
            Constant constant,
            Map<String, ColumnHandle> assignments)
    {
        if (!(yearSource instanceof Variable sourceVariable)) {
            // Engine unwraps year in comparisons in UnwrapYearInComparison. Within a connector we can do more than
            // engine only for source columns. We cannot draw many conclusions for intermediate expressions without
            // knowing them well.
            return Optional.empty();
        }

        if (constant.getValue() == null) {
            // Comparisons with NULL should be simplified by the engine
            return Optional.empty();
        }

        IcebergColumnHandle column = resolve(sourceVariable, assignments);
        if (column.getType() instanceof TimestampWithTimeZoneType type) {
            // Iceberg supports only timestamp(6) with time zone
            checkArgument(type.getPrecision() == 6, "Unexpected type: %s", column.getType());

            return unwrapYearInTimestampTzComparison(functionName, type, constant)
                    .map(domain -> TupleDomain.withColumnDomains(ImmutableMap.of(column, domain)));
        }

        return Optional.empty();
    }

    private static IcebergColumnHandle resolve(Variable variable, Map<String, ColumnHandle> assignments)
    {
        ColumnHandle columnHandle = assignments.get(variable.getName());
        checkArgument(columnHandle != null, "No assignment for %s", variable);
        return (IcebergColumnHandle) columnHandle;
    }

    public record ExtractionResult(TupleDomain<IcebergColumnHandle> tupleDomain, ConnectorExpression remainingExpression)
    {
        public ExtractionResult
        {
            requireNonNull(tupleDomain, "tupleDomain is null");
            requireNonNull(remainingExpression, "remainingExpression is null");
        }
    }
}
