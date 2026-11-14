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
package io.trino.plugin.base.filter;

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
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.base.expression.ConnectorExpressions.and;
import static io.trino.plugin.base.expression.ConnectorExpressions.extractConjuncts;
import static io.trino.plugin.base.expression.ConnectorExpressions.extractDisjuncts;
import static io.trino.spi.expression.StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.BETWEEN_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.CAST_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IDENTICAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IN_PREDICATE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.OR_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampWithTimeZoneType.MAX_SHORT_PRECISION;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static java.lang.Math.toIntExact;
import static java.math.RoundingMode.UNNECESSARY;
import static java.time.DayOfWeek.MONDAY;
import static java.time.ZoneOffset.UTC;
import static java.time.temporal.TemporalAdjusters.previousOrSame;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * Some expressions involving the TIMESTAMP WITH TIME ZONE type can be optimized when the time zone is known.
 * It is not possible in the engine, but can be possible in the connector if the connector follows some
 * convention regarding time zones. In some connectors, like the Delta Lake connector, or the Iceberg connector,
 * all values of TIMESTAMP WITH TIME ZONE type are represented using the UTC time zone.
 */
public final class UtcConstraintExtractor
{
    // A compared date or year (year() returns bigint) can lie beyond what TIMESTAMP WITH TIME ZONE holds. Its
    // representation limits epoch millis to 52 bits (DateTimeEncoding), about 71 thousand years either side of 1970.
    // Building a range outside it would throw or overflow epoch millis, so such a comparison is left in the remaining
    // expression.
    private static final long MAX_PACKED_EPOCH_MILLIS = (1L << 51) - 1;
    // Days whose start and the start of the next day both fit
    private static final long MIN_UNWRAPPABLE_EPOCH_DAY = -(MAX_PACKED_EPOCH_MILLIS / MILLISECONDS_PER_DAY);
    private static final long MAX_UNWRAPPABLE_EPOCH_DAY = MAX_PACKED_EPOCH_MILLIS / MILLISECONDS_PER_DAY - 1;
    // Years whose first day and the first day of the next year both fit
    private static final int MIN_UNWRAPPABLE_YEAR = LocalDate.ofEpochDay(MIN_UNWRAPPABLE_EPOCH_DAY).getYear() + 1;
    private static final int MAX_UNWRAPPABLE_YEAR = LocalDate.ofEpochDay(MAX_UNWRAPPABLE_EPOCH_DAY + 1).getYear() - 1;
    // Whole seconds that fit, for date_trunc period bounds
    private static final long MAX_UNWRAPPABLE_EPOCH_SECOND = MAX_PACKED_EPOCH_MILLIS / MILLISECONDS_PER_SECOND;

    private UtcConstraintExtractor() {}

    public static ExtractionResult extractTupleDomain(Constraint constraint)
    {
        TupleDomain<ColumnHandle> result = constraint.getSummary();
        ImmutableList.Builder<ConnectorExpression> remainingExpressions = ImmutableList.builder();
        for (ConnectorExpression conjunct : extractConjuncts(constraint.getExpression())) {
            Optional<TupleDomain<ColumnHandle>> converted = toTupleDomain(conjunct, constraint.getAssignments());
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

    private static Optional<TupleDomain<ColumnHandle>> toTupleDomain(ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        if (expression instanceof Call call) {
            if (OR_FUNCTION_NAME.equals(call.getFunctionName())) {
                return disjunctsToTupleDomain(call, assignments);
            }
            return toTupleDomain(call, assignments);
        }
        return Optional.empty();
    }

    private static Optional<TupleDomain<ColumnHandle>> disjunctsToTupleDomain(Call orCall, Map<String, ColumnHandle> assignments)
    {
        List<ConnectorExpression> disjuncts = extractDisjuncts(orCall);
        ImmutableList.Builder<TupleDomain<ColumnHandle>> tupleDomains = ImmutableList.builder();
        for (ConnectorExpression disjunct : disjuncts) {
            Optional<TupleDomain<ColumnHandle>> converted = conjunctsToTupleDomain(disjunct, assignments);
            if (converted.isEmpty()) {
                // If any disjunct can't be fully converted, we can't handle the entire OR
                return Optional.empty();
            }
            tupleDomains.add(converted.get());
        }

        return TupleDomain.strictUnion(tupleDomains.build());
    }

    private static Optional<TupleDomain<ColumnHandle>> conjunctsToTupleDomain(ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        List<ConnectorExpression> conjuncts = extractConjuncts(expression);
        TupleDomain<ColumnHandle> result = TupleDomain.all();
        for (ConnectorExpression conjunct : conjuncts) {
            Optional<TupleDomain<ColumnHandle>> converted = toTupleDomain(conjunct, assignments);
            if (converted.isEmpty()) {
                return Optional.empty();
            }
            result = result.intersect(converted.get());
            if (result.isNone()) {
                return Optional.of(TupleDomain.none());
            }
        }
        return Optional.of(result);
    }

    private static Optional<TupleDomain<ColumnHandle>> toTupleDomain(Call call, Map<String, ColumnHandle> assignments)
    {
        if (BETWEEN_FUNCTION_NAME.equals(call.getFunctionName()) && call.getArguments().size() == 3) {
            // value BETWEEN min AND max <=> value >= min AND value <= max. Decompose and reuse the
            // comparison handling below (e.g. cast / date_trunc / year unwrapping for the value).
            ConnectorExpression value = call.getArguments().get(0);
            ConnectorExpression min = call.getArguments().get(1);
            ConnectorExpression max = call.getArguments().get(2);
            Optional<TupleDomain<ColumnHandle>> low = toTupleDomain(new Call(BOOLEAN, GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(value, min)), assignments);
            Optional<TupleDomain<ColumnHandle>> high = toTupleDomain(new Call(BOOLEAN, LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(value, max)), assignments);
            if (low.isPresent() && high.isPresent()) {
                return Optional.of(low.get().intersect(high.get()));
            }
            return Optional.empty();
        }

        if (IN_PREDICATE_FUNCTION_NAME.equals(call.getFunctionName()) && call.getArguments().size() == 2 &&
                call.getArguments().get(1) instanceof Call valueList &&
                ARRAY_CONSTRUCTOR_FUNCTION_NAME.equals(valueList.getFunctionName()) &&
                !valueList.getArguments().isEmpty()) {
            // value IN (c1, ..., cn) <=> value = c1 OR ... OR value = cn. Decompose and reuse the comparison
            // handling below, like BETWEEN above. The engine leaves the list alone when it cannot unwrap the value
            // itself, e.g. CAST(timestamp_tz AS date), so without this an IN prunes nothing even though each of
            // its equalities would.
            ConnectorExpression value = call.getArguments().get(0);
            ImmutableList.Builder<TupleDomain<ColumnHandle>> itemDomains = ImmutableList.builder();
            for (ConnectorExpression item : valueList.getArguments()) {
                Optional<TupleDomain<ColumnHandle>> itemDomain = toTupleDomain(new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(value, item)), assignments);
                if (itemDomain.isEmpty()) {
                    // If any item can't be converted (e.g. a null or a non-constant item), we can't handle the entire IN
                    return Optional.empty();
                }
                itemDomains.add(itemDomain.get());
            }
            return TupleDomain.strictUnion(itemDomains.build());
        }

        if (call.getArguments().size() == 2) {
            ConnectorExpression firstArgument = call.getArguments().get(0);
            ConnectorExpression secondArgument = call.getArguments().get(1);
            FunctionName functionName = call.getFunctionName();

            // The order of a comparison's operands is not part of the connector expression contract, so
            // normalize to <expression> <operator> <constant>. In particular, the engine canonicalizes
            // greater-than comparisons to less-than forms with flipped operands, putting the constant first.
            if (firstArgument instanceof Constant && !(secondArgument instanceof Constant)) {
                ConnectorExpression swappedArgument = firstArgument;
                firstArgument = secondArgument;
                secondArgument = swappedArgument;
                functionName = flipComparison(functionName);
            }

            if (firstArgument instanceof Call firstAsCall && firstAsCall.getFunctionName().equals(CAST_FUNCTION_NAME) &&
                    secondArgument instanceof Constant constant &&
                    // if type do no match, this cannot be a comparison function
                    firstArgument.getType().equals(secondArgument.getType())) {
                return unwrapCastInComparison(
                        functionName,
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
                        functionName,
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
                        functionName,
                        getOnlyElement(firstAsCall.getArguments()),
                        constant,
                        assignments);
            }
        }

        return Optional.empty();
    }

    private static FunctionName flipComparison(FunctionName functionName)
    {
        if (functionName.equals(LESS_THAN_OPERATOR_FUNCTION_NAME)) {
            return GREATER_THAN_OPERATOR_FUNCTION_NAME;
        }
        if (functionName.equals(LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME)) {
            return GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
        }
        if (functionName.equals(GREATER_THAN_OPERATOR_FUNCTION_NAME)) {
            return LESS_THAN_OPERATOR_FUNCTION_NAME;
        }
        if (functionName.equals(GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME)) {
            return LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
        }
        // EQUAL and NOT_EQUAL are symmetric under operand swap
        return functionName;
    }

    private static Optional<TupleDomain<ColumnHandle>> unwrapCastInComparison(
            // upon invocation, we don't know if this really is a comparison
            FunctionName functionName,
            ConnectorExpression castSource,
            Constant constant,
            Map<String, ColumnHandle> assignments)
    {
        if (!(castSource instanceof Variable sourceVariable)) {
            // Engine unwraps casts through function preimages. Within a connector we can do more than
            // engine only for source columns. We cannot draw many conclusions for intermediate expressions without
            // knowing them well.
            return Optional.empty();
        }

        if (constant.getValue() == null) {
            // Comparisons with NULL should be simplified by the engine
            return Optional.empty();
        }

        ColumnHandle column = resolve(sourceVariable, assignments);
        if (sourceVariable.getType() instanceof TimestampWithTimeZoneType columnType) {
            if (constant.getType() == DateType.DATE) {
                return unwrapTimestampTzToDateCast(columnType, functionName, (long) constant.getValue())
                        .map(domain -> TupleDomain.withColumnDomains(ImmutableMap.of(column, domain)));
            }
            // TODO support timestamp constant
        }

        return Optional.empty();
    }

    private static Optional<Domain> unwrapTimestampTzToDateCast(Type columnType, FunctionName functionName, long date)
    {
        // Verify no overflow. Date values must be in integer range.
        verify(date <= Integer.MAX_VALUE, "Date value out of range: %s", date);
        if (date < MIN_UNWRAPPABLE_EPOCH_DAY || date > MAX_UNWRAPPABLE_EPOCH_DAY) {
            return Optional.empty();
        }

        Object startOfDate;
        Object startOfNextDate;
        int precision = ((TimestampWithTimeZoneType) columnType).getPrecision();
        if (precision <= MAX_SHORT_PRECISION) {
            startOfDate = packDateTimeWithZone(date * MILLISECONDS_PER_DAY, UTC_KEY);
            startOfNextDate = packDateTimeWithZone((date + 1) * MILLISECONDS_PER_DAY, UTC_KEY);
        }
        else {
            startOfDate = LongTimestampWithTimeZone.fromEpochMillisAndFraction(date * MILLISECONDS_PER_DAY, 0, UTC_KEY);
            startOfNextDate = LongTimestampWithTimeZone.fromEpochMillisAndFraction((date + 1) * MILLISECONDS_PER_DAY, 0, UTC_KEY);
        }

        return createDomain(functionName, columnType, startOfDate, startOfNextDate);
    }

    private static Optional<Domain> unwrapYearInTimestampTzComparison(FunctionName functionName, Type type, Constant constant)
    {
        checkArgument(constant.getValue() != null, "Unexpected constant: %s", constant);

        long year = (Long) constant.getValue();
        if (year < MIN_UNWRAPPABLE_YEAR || year > MAX_UNWRAPPABLE_YEAR) {
            return Optional.empty();
        }
        ZonedDateTime periodStart = ZonedDateTime.of(toIntExact(year), 1, 1, 0, 0, 0, 0, UTC);
        ZonedDateTime periodEnd = periodStart.plusYears(1);

        Object start;
        Object end;
        int precision = ((TimestampWithTimeZoneType) type).getPrecision();
        if (precision <= MAX_SHORT_PRECISION) {
            start = packDateTimeWithZone(periodStart.toEpochSecond() * MILLISECONDS_PER_SECOND, UTC_KEY);
            end = packDateTimeWithZone(periodEnd.toEpochSecond() * MILLISECONDS_PER_SECOND, UTC_KEY);
        }
        else {
            start = LongTimestampWithTimeZone.fromEpochSecondsAndFraction(periodStart.toEpochSecond(), 0, UTC_KEY);
            end = LongTimestampWithTimeZone.fromEpochSecondsAndFraction(periodEnd.toEpochSecond(), 0, UTC_KEY);
        }

        return createDomain(functionName, type, start, end);
    }

    private static Optional<Domain> createDomain(FunctionName functionName, Type type, Object startOfDate, Object startOfNextDate)
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
        if (functionName.equals(IDENTICAL_OPERATOR_FUNCTION_NAME)) {
            return Optional.of(Domain.create(ValueSet.ofRanges(Range.range(type, startOfDate, true, startOfNextDate, false)), false));
        }

        return Optional.empty();
    }

    private static Optional<TupleDomain<ColumnHandle>> unwrapDateTruncInComparison(
            // upon invocation, we don't know if this really is a comparison
            FunctionName functionName,
            Constant unit,
            ConnectorExpression dateTruncSource,
            Constant constant,
            Map<String, ColumnHandle> assignments)
    {
        if (!(dateTruncSource instanceof Variable sourceVariable)) {
            // Engine unwraps date_trunc through function preimages. Within a connector we can do more than
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

        ColumnHandle column = resolve(sourceVariable, assignments);
        if (sourceVariable.getType() instanceof TimestampWithTimeZoneType type) {
            verify(constant.getType().equals(type), "This method should not be invoked when type mismatch (i.e. surely not a comparison)");

            return unwrapDateTruncInComparison(((Slice) unit.getValue()).toStringUtf8(), functionName, constant)
                    .map(domain -> TupleDomain.withColumnDomains(ImmutableMap.of(column, domain)));
        }

        return Optional.empty();
    }

    private static boolean isUnwrappablePeriodBound(ZonedDateTime periodBound)
    {
        // Period bounds are whole seconds, so their epoch millis are the epoch second times 1000
        long epochSecond = periodBound.toEpochSecond();
        return epochSecond >= -MAX_UNWRAPPABLE_EPOCH_SECOND && epochSecond <= MAX_UNWRAPPABLE_EPOCH_SECOND;
    }

    private static Optional<Domain> unwrapDateTruncInComparison(String unit, FunctionName functionName, Constant constant)
    {
        Type type = constant.getType();
        checkArgument(constant.getValue() != null, "Unexpected constant: %s", constant);

        ZonedDateTime dateTime;
        int precision = ((TimestampWithTimeZoneType) type).getPrecision();
        if (precision <= MAX_SHORT_PRECISION) {
            // Normalized to UTC because for comparisons the zone is irrelevant
            dateTime = Instant.ofEpochMilli(unpackMillisUtc((long) constant.getValue()))
                    .atZone(UTC);
        }
        else {
            if (precision > 9) {
                return Optional.empty();
            }
            // Normalized to UTC because for comparisons the zone is irrelevant
            dateTime = Instant.ofEpochMilli(((LongTimestampWithTimeZone) constant.getValue()).getEpochMillis())
                    .plusNanos(LongMath.divide(((LongTimestampWithTimeZone) constant.getValue()).getPicosOfMilli(), PICOSECONDS_PER_NANOSECOND, UNNECESSARY))
                    .atZone(UTC);
        }

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
            case "week" -> {
                periodStart = dateTime.toLocalDate().with(previousOrSame(MONDAY)).atStartOfDay().atZone(UTC);
                nextPeriodStart = periodStart.plusWeeks(1);
            }
            case "month" -> {
                periodStart = dateTime.toLocalDate().withDayOfMonth(1).atStartOfDay().atZone(UTC);
                nextPeriodStart = periodStart.plusMonths(1);
            }
            case "quarter" -> {
                int firstMonthOfQuarter = ((dateTime.getMonthValue() - 1) / 3) * 3 + 1;
                periodStart = dateTime.toLocalDate().withMonth(firstMonthOfQuarter).withDayOfMonth(1).atStartOfDay().atZone(UTC);
                nextPeriodStart = periodStart.plusMonths(3);
            }
            case "year" -> {
                periodStart = dateTime.toLocalDate().withMonth(1).withDayOfMonth(1).atStartOfDay().atZone(UTC);
                nextPeriodStart = periodStart.plusYears(1);
            }
            default -> {
                return Optional.empty();
            }
        }
        // As for dates and years, give up on a period that runs past what TIMESTAMP WITH TIME ZONE holds, e.g. when the
        // constant is in the last period, the next period starts after the largest value
        if (!isUnwrappablePeriodBound(periodStart) || !isUnwrappablePeriodBound(nextPeriodStart)) {
            return Optional.empty();
        }
        boolean constantAtPeriodStart = dateTime.equals(periodStart);

        Object start;
        Object end;
        if (precision <= MAX_SHORT_PRECISION) {
            start = packDateTimeWithZone(periodStart.toEpochSecond() * MILLISECONDS_PER_SECOND, UTC_KEY);
            end = packDateTimeWithZone(nextPeriodStart.toEpochSecond() * MILLISECONDS_PER_SECOND, UTC_KEY);
        }
        else {
            start = LongTimestampWithTimeZone.fromEpochSecondsAndFraction(periodStart.toEpochSecond(), 0, UTC_KEY);
            end = LongTimestampWithTimeZone.fromEpochSecondsAndFraction(nextPeriodStart.toEpochSecond(), 0, UTC_KEY);
        }

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
        if (functionName.equals(IDENTICAL_OPERATOR_FUNCTION_NAME)) {
            if (!constantAtPeriodStart) {
                return Optional.of(Domain.none(type));
            }
            return Optional.of(Domain.create(ValueSet.ofRanges(Range.range(type, start, true, end, false)), false));
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

    private static Optional<TupleDomain<ColumnHandle>> unwrapYearInTimestampTzComparison(
            // upon invocation, we don't know if this really is a comparison
            FunctionName functionName,
            ConnectorExpression yearSource,
            Constant constant,
            Map<String, ColumnHandle> assignments)
    {
        if (!(yearSource instanceof Variable sourceVariable)) {
            // Engine unwraps year through function preimages. Within a connector we can do more than
            // engine only for source columns. We cannot draw many conclusions for intermediate expressions without
            // knowing them well.
            return Optional.empty();
        }

        if (constant.getValue() == null) {
            // Comparisons with NULL should be simplified by the engine
            return Optional.empty();
        }

        ColumnHandle column = resolve(sourceVariable, assignments);
        if (sourceVariable.getType() instanceof TimestampWithTimeZoneType type) {
            return unwrapYearInTimestampTzComparison(functionName, type, constant)
                    .map(domain -> TupleDomain.withColumnDomains(ImmutableMap.of(column, domain)));
        }

        return Optional.empty();
    }

    private static ColumnHandle resolve(Variable variable, Map<String, ColumnHandle> assignments)
    {
        ColumnHandle columnHandle = assignments.get(variable.getName());
        checkArgument(columnHandle != null, "No assignment for %s", variable);
        return columnHandle;
    }

    public record ExtractionResult(TupleDomain<ColumnHandle> tupleDomain, ConnectorExpression remainingExpression)
    {
        public ExtractionResult
        {
            requireNonNull(tupleDomain, "tupleDomain is null");
            requireNonNull(remainingExpression, "remainingExpression is null");
        }
    }
}
