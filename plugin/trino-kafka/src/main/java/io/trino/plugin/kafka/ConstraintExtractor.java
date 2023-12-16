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
package io.trino.plugin.kafka;

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
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

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
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static java.lang.Math.toIntExact;
import static java.math.RoundingMode.UNNECESSARY;
import static java.time.ZoneOffset.UTC;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * This class is used to extract and manipulate constraint related data.
 * It provides static functions to handle different operations,
 * including transforming constraints and expressions, managing conjuncts,
 * converting and intersecting tuple domains, and handling different types of calls.
 * <p>
 * It has several private static helper methods for dealing with various cases
 * of comparisons and conversions, such as timestamp to date casts and date truncations.
 * Specialized handling for processing Connector specific timestamp handling is also included.
 * <p>
 * This class ends with a nested record ExtractionResult,
 * which groups a TupleDomain with a remaining ConnnectorExpression,
 * representing the result of an extraction process.
 * <p>
 * This class is declared as final, and it also has a private constructor,
 * which means it cannot be instantiated or sub-classed.
 * All of its methods are static and can be accessed directly from the class.
 * <p>
 * we should do some unit-tests here.
 */
public final class ConstraintExtractor
{
    private ConstraintExtractor() {}

    /**
     * Extracts the tuple domain and remaining expressions from a constraint.
     *
     * @param <C> the type of the column handle
     * @param constraint the constraint to extract from
     * @param columnTypeProvider provides the column types for the given column handles
     * @return an ExtractionResult object containing the extracted tuple domain and remaining expressions
     */
    public static <C extends ColumnHandle> ExtractionResult<C> extractTupleDomain(Constraint constraint, ColumnTypeProvider<C> columnTypeProvider)
    {
        TupleDomain<C> result = constraint.getSummary()
                .transformKeys(key -> (C) key);
        ImmutableList.Builder<ConnectorExpression> remainingExpressions = ImmutableList.builder();
        for (ConnectorExpression conjunct : extractConjuncts(constraint.getExpression())) {
            Optional<TupleDomain<C>> converted = toTupleDomain(conjunct, constraint.getAssignments(), columnTypeProvider);
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

    /**
     * Converts a ConnectorExpression to TupleDomain.
     *
     * @param <C> the type of the column handle
     * @param expression the ConnectorExpression to convert
     * @param assignments a map of column assignments
     * @param columnTypeProvider provides the column types for the given column handles
     * @return an Optional containing the converted TupleDomain if the expression is a Call,
     *         otherwise an empty Optional
     */
    private static <C> Optional<TupleDomain<C>> toTupleDomain(ConnectorExpression expression, Map<String, ColumnHandle> assignments, ColumnTypeProvider<C> columnTypeProvider)
    {
        if (expression instanceof Call call) {
            return toTupleDomain(call, assignments, columnTypeProvider);
        }
        return Optional.empty();
    }

    /**
     * Attempts to convert a 'Call' expression into a 'TupleDomain', if applicable. This method handles
     * specific patterns of 'Call' expressions, particularly those involving 'cast', 'date_trunc', and 'year'
     * functions, and attempts to transform them into a 'TupleDomain' representation.
     *
     * @param <C> the generic type parameter representing the column handle type.
     * @param call the 'Call' expression to be converted. It represents a function call in the query.
     * @param assignments a mapping of string identifiers to 'ColumnHandle' objects, used to resolve column references.
     * @param columnTypeProvider a provider for column types based on column handles, used to determine data types for columns.
     * @return an 'Optional' containing the 'TupleDomain' if conversion is applicable and successful, otherwise an empty 'Optional'.
     *
     * The method first checks if the 'Call' expression has exactly two arguments, a pattern common in binary functions
     * like comparisons. It then examines the type and nature of these arguments:
     *
     * - If the first argument is a 'cast' function call and the second is a constant, and both arguments have the same type,
     *   it attempts to unwrap this cast comparison into a 'TupleDomain'.
     * - If the first argument is a 'date_trunc' function call and the second is a constant, with both having the same type,
     *   it tries to unwrap this date truncation comparison.
     * - If the first argument is a 'year' function call on a 'TimestampWithTimeZoneType' and the second is a constant,
     *   with both having the same type, it unwraps this year comparison.
     *
     * If none of these patterns match, or if the arguments' types do not align in a way that permits meaningful comparison,
     * the method returns an empty 'Optional', indicating that no 'TupleDomain' representation is applicable for the given 'Call'.
     */
    private static <C> Optional<TupleDomain<C>> toTupleDomain(Call call, Map<String, ColumnHandle> assignments, ColumnTypeProvider<C> columnTypeProvider)
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
                        assignments,
                        columnTypeProvider);
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
                        assignments,
                        columnTypeProvider);
            }

            if (firstArgument instanceof Call firstAsCall &&
                    firstAsCall.getFunctionName().equals(new FunctionName("year")) &&
                    firstAsCall.getArguments().size() == 1 &&
                    (getOnlyElement(firstAsCall.getArguments()).getType() instanceof TimestampWithTimeZoneType ||
                            getOnlyElement(firstAsCall.getArguments()).getType() instanceof TimestampType) &&
                    secondArgument instanceof Constant constant &&
                    // if types do no match, this cannot be a comparison function
                    firstArgument.getType().equals(secondArgument.getType())) {
                return unwrapYearInTimestampTzComparison(
                        call.getFunctionName(),
                        getOnlyElement(firstAsCall.getArguments()),
                        constant,
                        assignments,
                        columnTypeProvider);
            }
        }

        return Optional.empty();
    }

    /**
     * Attempts to unwrap a cast operation within a comparison expression and convert it into a TupleDomain.
     * This method is specifically designed to handle cases where a column is cast to a different type and then compared against a constant value.
     *
     * @param <C> The generic type parameter representing the column handle type.
     * @param functionName The name of the function representing the comparison operation.
     * @param castSource The expression that is being cast, typically a column or a variable in the query.
     * @param constant The constant value being compared against after the cast operation.
     * @param assignments A map of string identifiers to column handles, used for resolving column references in expressions.
     * @param columnTypeProvider A provider for column types based on column handles, used to determine data types for columns.
     * @return An Optional containing the TupleDomain if the cast can be successfully unwrapped into a meaningful comparison, otherwise an empty Optional.
     *
     * The method first checks if the castSource is a variable and the constant is not null, as the method is designed to work primarily with source columns and non-null constants.
     * It then delegates to the processCastComparison method for further processing based on the column type.
     */
    private static <C> Optional<TupleDomain<C>> unwrapCastInComparison(
            // upon invocation, we don't know if this really is a comparison
            FunctionName functionName,
            ConnectorExpression castSource,
            Constant constant,
            Map<String, ColumnHandle> assignments,
            ColumnTypeProvider<C> columnTypeProvider)
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

        C column = resolve(sourceVariable, assignments);
        return processCastComparison(column, functionName, constant, columnTypeProvider);
    }

    /**
     * Processes the cast comparison by determining the type of the column and performing the appropriate unwrapping logic.
     *
     * @param <C> The generic type parameter representing the column handle type.
     * @param column The column handle, representing a column in the query.
     * @param functionName The name of the comparison function being used in the expression.
     * @param constant The constant value being compared.
     * @param columnTypeProvider A provider for column types based on column handles.
     * @return An Optional containing the TupleDomain if the comparison can be successfully unwrapped, otherwise an empty Optional.
     *
     * The method checks the type of the column (TimestampWithTimeZoneType or TimestampType) and calls the respective unwrapping method.
     * It handles specific precision requirements for timestamps (6 for with timezone and 3 for without timezone).
     * If the column type does not match these specific types or if other checks fail, the method returns an empty Optional.
     */
    private static <C> Optional<TupleDomain<C>> processCastComparison(
            C column,
            FunctionName functionName,
            Constant constant,
            ColumnTypeProvider<C> columnTypeProvider)
    {
        if (constant.getValue() == null || constant.getType() != DateType.DATE) {
            return Optional.empty();
        }

        return columnTypeProvider.getType(column)
                .flatMap(type -> {
                    // Connector supports timestamp(6) with time zone
                    if (type instanceof TimestampWithTimeZoneType tztType && tztType.getPrecision() == 6) {
                        return unwrapTimestampTzToDateCast(column, functionName, (long) constant.getValue(), columnTypeProvider)
                                .map(domain -> TupleDomain.withColumnDomains(ImmutableMap.of(column, domain)));
                    }
                    else if (type instanceof TimestampType tsType && tsType.getPrecision() == 3) {
                        return unwrapTimestampToDateCast(column, functionName, (long) constant.getValue(), columnTypeProvider)
                                .map(domain -> TupleDomain.withColumnDomains(ImmutableMap.of(column, domain)));
                    }
                    return Optional.<TupleDomain<C>>empty();
                });
    }

    /**
     * Unwraps a cast operation from timestamp with timezone to date within a comparison expression and converts it into a Domain.
     * This method specifically handles cases where a timestamp with timezone column is cast to a date and then compared against
     * a constant date value.
     *
     * @param <C> Generic type representing the column handle.
     * @param column The column handle, representing a column that is cast from timestamp with timezone to date.
     * @param functionName The name of the comparison function being used.
     * @param date The date value in epoch days.
     * @param columnTypeProvider A provider for column types based on column handles.
     * @return An Optional containing the Domain if the cast and comparison can be successfully unwrapped, otherwise an empty Optional.
     */
    private static <C> Optional<Domain> unwrapTimestampTzToDateCast(
            C column, FunctionName functionName, long date, ColumnTypeProvider<C> columnTypeProvider)
    {
        return unwrapTimestampCastToDomain(
                column,
                functionName,
                date,
                columnTypeProvider,
                d -> LongTimestampWithTimeZone.fromEpochMillisAndFraction(d * MILLISECONDS_PER_DAY, 0, UTC_KEY),
                type -> type.equals(TIMESTAMP_TZ_MICROS));
    }

    /**
     * Unwraps a cast operation from timestamp to date within a comparison expression and converts it into a Domain.
     * This method specifically handles cases where a timestamp column (without timezone) is cast to a date and then compared against
     * a constant date value.
     *
     * @param <C> Generic type representing the column handle.
     * @param column The column handle, representing a column that is cast from timestamp to date.
     * @param functionName The name of the comparison function being used.
     * @param date The date value in epoch days.
     * @param columnTypeProvider A provider for column types based on column handles.
     * @return An Optional containing the Domain if the cast and comparison can be successfully unwrapped, otherwise an empty Optional.
     */
    private static <C> Optional<Domain> unwrapTimestampToDateCast(
            C column, FunctionName functionName, long date, ColumnTypeProvider<C> columnTypeProvider)
    {
        return unwrapTimestampCastToDomain(
                column,
                functionName,
                date,
                columnTypeProvider,
                d -> d * MICROSECONDS_PER_DAY,
                type -> type.equals(TIMESTAMP_MILLIS));
    }

    /**
     * Generalized method for unwrapping a timestamp cast to a domain. This method is used to create a domain
     * from a timestamp value, taking into account different types of timestamp representations.
     *
     * @param <C> Generic type representing the column handle.
     * @param <T> Generic type representing the timestamp type.
     * @param column The column handle involved in the comparison.
     * @param functionName The name of the function representing the comparison operation.
     * @param date The date value in epoch days to be used in the comparison.
     * @param columnTypeProvider Provides the column types based on column handles.
     * @param timestampCreator A function to create a timestamp object from epoch milliseconds.
     * @param typeValidator A predicate to validate if the provided type is acceptable for the operation.
     * @return An Optional containing the Domain if it can be successfully created, otherwise an empty Optional.
     */
    private static <C, T> Optional<Domain> unwrapTimestampCastToDomain(
            C column,
            FunctionName functionName,
            long date,
            ColumnTypeProvider<C> columnTypeProvider,
            Function<Long, T> timestampCreator,
            Predicate<Type> typeValidator)
    {
        Type type = columnTypeProvider.getType(column).orElseThrow();
        checkArgument(typeValidator.test(type), "Column of unexpected type %s: %s", type, column);

        // Verify no overflow. Date values must be in integer range.
        verify(date <= Integer.MAX_VALUE, "Date value out of range: %s", date);

        T startOfDate = timestampCreator.apply(date);
        T startOfNextDate = timestampCreator.apply((date + 1));

        return createDomain(functionName, type, startOfDate, startOfNextDate);
    }

    /**
     * Unwraps the year component from a given timestamp with timezone comparison.
     *
     * @param functionName The name of the function being executed.
     * @param type The type of the comparison.
     * @param constant The constant value being compared.
     * @param timestampCalculator The function to calculate the timestamp from the year component.
     * @param typeValidator The predicate to validate the type.
     * @param <T> The type of the timestamp.
     * @return An Optional containing the Domain object created from the calculated start and end timestamps.
     * @throws IllegalArgumentException if the constant value is null or if the type is not valid.
     */
    private static <T> Optional<Domain> unwrapYearInTimestampTzComparison(FunctionName functionName, Type type, Constant constant, Function<ZonedDateTime, T> timestampCalculator, Predicate<Type> typeValidator)
    {
        checkArgument(constant.getValue() != null, "Unexpected constant: %s", constant);
        checkArgument(typeValidator.test(type), "Unexpected type: %s", type);

        int year = toIntExact((Long) constant.getValue());
        ZonedDateTime periodStart = ZonedDateTime.of(year, 1, 1, 0, 0, 0, 0, UTC);
        ZonedDateTime periodEnd = periodStart.plusYears(1);

        T start = timestampCalculator.apply(periodStart);
        T end = timestampCalculator.apply(periodEnd);

        return createDomain(functionName, type, start, end);
    }

    /**
     * Creates a Domain based on the given parameters.
     *
     * @param functionName      the name of the function
     * @param type              the Type object representing the type of the Domain
     * @param startOfDate       the start of the date range
     * @param startOfNextDate   the start of the next date range
     * @param <T>               the type of the startOfDate and startOfNextDate parameters
     * @return an Optional object containing the created Domain, or an empty Optional if the function name is not supported
     */
    private static <T> Optional<Domain> createDomain(FunctionName functionName, Type type, T startOfDate, T startOfNextDate)
    {
        Map<FunctionName, DomainCreator<T>> domainCreators = Map.of(
                EQUAL_OPERATOR_FUNCTION_NAME, (t, s, e) -> Optional.of(Domain.create(ValueSet.ofRanges(Range.range(t, s, true, e, false)), false)),
                NOT_EQUAL_OPERATOR_FUNCTION_NAME, (t, s, e) -> Optional.of(Domain.create(ValueSet.ofRanges(Range.lessThan(t, s), Range.greaterThanOrEqual(t, e)), false)),
                LESS_THAN_OPERATOR_FUNCTION_NAME, (t, s, e) -> Optional.of(Domain.create(ValueSet.ofRanges(Range.lessThan(t, s)), false)),
                LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, (t, s, e) -> Optional.of(Domain.create(ValueSet.ofRanges(Range.lessThan(t, e)), false)),
                GREATER_THAN_OPERATOR_FUNCTION_NAME, (t, s, e) -> Optional.of(Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(t, e)), false)),
                GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, (t, s, e) -> Optional.of(Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(t, s)), false)),
                IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME, (t, s, e) -> Optional.of(Domain.create(ValueSet.ofRanges(Range.lessThan(t, s), Range.greaterThanOrEqual(t, e)), true)));

        return Optional.ofNullable(domainCreators.get(functionName))
                .map(creator -> creator.create(type, startOfDate, startOfNextDate))
                .orElse(Optional.empty());
    }

    /**
     * This functional interface represents a domain creator which is used to create a domain object.
     *
     * @param <T> the type of the start and end values
     */
    @FunctionalInterface
    private interface DomainCreator<T>
    {
        Optional<Domain> create(Type type, T start, T end);
    }

    /**
     * Unwraps the date_trunc function in a comparison operation.
     *
     * @param functionName The name of the function.
     * @param unit The unit of time used in the truncation.
     * @param dateTruncSource The source of the date_trunc function.
     * @param constant The constant value being compared.
     * @param assignments A map of column assignments.
     * @param columnTypeProvider A provider for column types.
     * @param <C> The column type.
     * @return An Optional TupleDomain that represents the unwrapped comparison. Returns Optional.empty() if unable to unwrap the date_trunc function.
     */
    private static <C> Optional<TupleDomain<C>> unwrapDateTruncInComparison(
            // upon invocation, we don't know if this really is a comparison
            FunctionName functionName,
            Constant unit,
            ConnectorExpression dateTruncSource,
            Constant constant,
            Map<String, ColumnHandle> assignments,
            ColumnTypeProvider<C> columnTypeProvider)
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

        C column = resolve(sourceVariable, assignments);
        return columnTypeProvider.getType(column)
                .flatMap(
                        columnType -> {
                            // Connector supports only timestamp(6) with time zone
                            if (columnType instanceof TimestampWithTimeZoneType tztType && tztType.getPrecision() == 6 && constant.getType().equals(tztType)) {
                                //checkArgument(tztType.getPrecision() == 6, "Unexpected type: %s", columnType);
                                //verify(constant.getType().equals(tztType), "This method should not be invoked when type mismatch (i.e. surely not a comparison)");

                                return unwrapDateTruncInComparison(((Slice) unit.getValue()).toStringUtf8(),
                                        functionName,
                                        constant,
                                        ConstraintExtractor::convertToLongTimestampWithTimeZone)
                                        .map(domain -> TupleDomain.withColumnDomains(ImmutableMap.of(column, domain)));
                            }
                            else if (columnType instanceof TimestampType tsType && tsType.getPrecision() == 3 && constant.getType().equals(tsType)) {
                                return unwrapDateTruncInComparison(((Slice) unit.getValue()).toStringUtf8(),
                                        functionName,
                                        constant,
                                        zonedDateTime -> zonedDateTime.toEpochSecond() * MICROSECONDS_PER_SECOND)
                                        .map(domain -> TupleDomain.withColumnDomains(ImmutableMap.of(column, domain)));
                            }
                            return Optional.<TupleDomain<C>>empty();
                        });
    }

    /**
     * Unwraps a date truncation comparison to create a domain.
     * This method processes a comparison where a date-time value has been truncated to a specific unit
     * (e.g., hour, day, month, year) and then compared to a constant value.
     *
     * @param unit The unit of date truncation (e.g., "hour", "day", "month", "year").
     * @param functionName The name of the function representing the comparison operation.
     * @param constant The constant value used in the comparison.
     * @param timestampCalculator A function to convert ZonedDateTime to a specific timestamp type.
     * @param <T> The type of the timestamp (e.g., LongTimestampWithTimeZone, Long).
     * @return An Optional containing the created Domain if the comparison can be unwrapped successfully, otherwise an empty Optional.
     *
     * The method first validates the constant for the specified timestamp type.
     * Then, it converts the constant to a ZonedDateTime and calculates the start and end of the period based on the truncation unit.
     * The start and end times are converted to the required timestamp type using the provided timestampCalculator.
     * Finally, it invokes 'createDomainBasedOnFunction' to create the appropriate domain for the comparison function,
     * considering whether the constant corresponds to the start of the calculated period.
     */
    private static <T> Optional<Domain> unwrapDateTruncInComparison(String unit,
                                                                    FunctionName functionName,
                                                                    Constant constant,
                                                                    Function<ZonedDateTime, T> timestampCalculator)
    {
        // Validate constant for specified timestamp type
        if (!isValidConstant(constant)) {
            return Optional.empty();
        }

        // Convert constant to ZonedDateTime
        Optional<ZonedDateTime> dateTime = getZonedDateTimeFromConstant(constant);
        if (dateTime.isEmpty()) {
            return Optional.empty();
        }

        // Calculate period interval based on the unit
        Optional<PeriodInterval> periodInterval = calculatePeriodInterval(unit, dateTime.get());
        if (periodInterval.isEmpty()) {
            return Optional.empty();
        }

        // Convert start and end of period to the required timestamp type
        T startTimestamp = timestampCalculator.apply(periodInterval.orElseThrow().start);
        T endTimestamp = timestampCalculator.apply(periodInterval.orElseThrow().end);
        boolean isConstantAtPeriodStart = dateTime.get().equals(periodInterval.get().start);

        // Create the domain based on the function and the type of comparison
        return createDomainBasedOnFunction(functionName, constant.getType(),
                startTimestamp, endTimestamp, isConstantAtPeriodStart);
    }

    private static Optional<ZonedDateTime> getZonedDateTimeFromConstant(Constant constant)
    {
        final Map<Type, Function<Constant, ZonedDateTime>> typeToConverter = Map.of(
                TIMESTAMP_TZ_MICROS, c -> {
                    LongTimestampWithTimeZone timestamp = (LongTimestampWithTimeZone) c.getValue();
                    return Instant.ofEpochMilli(timestamp.getEpochMillis())
                            .plusNanos(LongMath.divide(timestamp.getPicosOfMilli(), PICOSECONDS_PER_NANOSECOND, UNNECESSARY))
                            .atZone(UTC);
                },
                TIMESTAMP_MILLIS, c -> {
                    Long timestamp = (Long) c.getValue();
                    return Instant.ofEpochMilli(timestamp / MICROSECONDS_PER_MILLISECOND)
                            .plusNanos(timestamp % MICROSECONDS_PER_MILLISECOND * NANOSECONDS_PER_MICROSECOND)
                            .atZone(UTC);
                });
        // add more converter functions
        return Optional.ofNullable(typeToConverter.get(constant.getType()))
                .map(converter -> converter.apply(constant));
    }

    /**
     * Validates if the constant is a valid timestamp of the specified type.
     *
     * @param constant The constant to be validated.
     * @return True if the constant is a valid timestamp of the specified type, otherwise false.
     */
    private static boolean isValidConstant(Constant constant)
    {
        //checkArgument(constant.getValue() != null, "Unexpected constant: %s", constant);
        //checkArgument(constant.getType().equals(TIMESTAMP_TZ_MICROS), "Unexpected type: %s", constant.getType());
        Map<Type, Predicate<Object>> typeValidators = Map.of(
                TIMESTAMP_TZ_MICROS, value -> value instanceof LongTimestampWithTimeZone,
                TIMESTAMP_MILLIS, value -> value instanceof Long);
        // Add more timestamp types and their corresponding validation logic here
        return Optional.ofNullable(typeValidators.get(constant.getType()))
                .map(validator -> validator.test(constant.getValue()))
                .orElse(false);
    }

    private static Optional<PeriodInterval> calculatePeriodInterval(String unit, ZonedDateTime dateTime)
    {
        return switch (unit.toLowerCase(ENGLISH)) {
            case "hour" -> Optional.of(new PeriodInterval(dateTime, dateTime.plusHours(1)));
            case "day" -> Optional.of(new PeriodInterval(dateTime.toLocalDate().atStartOfDay(UTC), dateTime.plusDays(1)));
            case "month" -> Optional.of(new PeriodInterval(dateTime.toLocalDate().withDayOfMonth(1).atStartOfDay(UTC), dateTime.plusMonths(1)));
            case "year" -> Optional.of(new PeriodInterval(dateTime.toLocalDate().withDayOfMonth(1).withMonth(1).atStartOfDay(UTC), dateTime.plusYears(1)));
            default -> Optional.empty();
        };
    }

    private static LongTimestampWithTimeZone convertToLongTimestampWithTimeZone(ZonedDateTime zonedDateTime)
    {
        return LongTimestampWithTimeZone.fromEpochSecondsAndFraction(zonedDateTime.toEpochSecond(), 0, UTC_KEY);
    }

    /**
     * Creates a domain for a given comparison function based on the type, start, end, and a boolean flag.
     * This method utilizes a map of function names to domain creators, selecting the appropriate domain logic
     * based on the specified comparison function.
     *
     * @param functionName The name of the comparison function to be used.
     * @param type The data type of the domain.
     * @param start The start timestamp of the domain.
     * @param end The end timestamp of the domain.
     * @param isConstantAtPeriodStart A boolean flag indicating if the constant is at the start of the period.
     * @return An Optional containing the created Domain if a matching domain creator is found; otherwise, an empty Optional.
     */
    private static <T> Optional<Domain> createDomainBasedOnFunction(FunctionName functionName, Type type,
                                                                T start, T end,
                                                                boolean isConstantAtPeriodStart)
    {
        Map<FunctionName, ComparisonDomainCreator<T>> domainCreators = Map.of(
                EQUAL_OPERATOR_FUNCTION_NAME, (t, s, e, atStart) -> atStart
                        ? Optional.of(Domain.create(ValueSet.ofRanges(Range.range(t, s, true, e, false)), false))
                        : Optional.of(Domain.none(t)),
                NOT_EQUAL_OPERATOR_FUNCTION_NAME, (t, s, e, atStart) -> atStart
                        ? Optional.of(Domain.create(ValueSet.ofRanges(Range.lessThan(t, s), Range.greaterThanOrEqual(t, e)), false))
                        : Optional.of(Domain.notNull(t)),
                IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME, (t, s, e, atStart) -> atStart
                        ? Optional.of(Domain.create(ValueSet.ofRanges(Range.lessThan(t, s), Range.greaterThanOrEqual(t, e)), true))
                        : Optional.of(Domain.all(t)),
                LESS_THAN_OPERATOR_FUNCTION_NAME, (t, s, e, atStart) -> Optional.of(Domain.create(ValueSet.ofRanges(Range.lessThan(t, atStart ? s : e)), false)),
                LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, (t, s, e, atStart) -> Optional.of(Domain.create(ValueSet.ofRanges(Range.lessThan(t, e)), false)),
                GREATER_THAN_OPERATOR_FUNCTION_NAME, (t, s, e, atStart) -> Optional.of(Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(t, e)), false)),
                GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, (t, s, e, atStart) -> Optional.of(Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(t, atStart ? s : e)), false)));

        return Optional.ofNullable(domainCreators.get(functionName))
                .map(creator -> creator.create(type, start, end, isConstantAtPeriodStart))
                .orElse(Optional.empty());
    }

    /**
     * Represents a functional interface for creating a comparison domain.
     */
    @FunctionalInterface
    private interface ComparisonDomainCreator<T>
    {
        Optional<Domain> create(Type type, T start, T end, boolean isConstantAtPeriodStart);
    }

    /**
     * Represents a time interval defined by a start and end {@link ZonedDateTime}.
     */
    private static class PeriodInterval
    {
        ZonedDateTime start;
        ZonedDateTime end;

        PeriodInterval(ZonedDateTime start, ZonedDateTime end)
        {
            this.start = start;
            this.end = end;
        }
    }

    /**
     * Unwraps the year in a comparison of TIMESTAMP WITH TIME ZONE type column with a constant year value.
     *
     * @param functionName     The name of the function that is invoking this method
     * @param yearSource       The expression representing the year source column
     * @param constant         The constant year value
     * @param assignments      The assignments for the columns
     * @param columnTypeProvider The provider for the column types
     * @param <C>              The column handle type
     * @return An Optional containing the unwrapped year in the comparison, or an empty Optional if the year cannot be unwrapped
     */
    private static <C> Optional<TupleDomain<C>> unwrapYearInTimestampTzComparison(
            // upon invocation, we don't know if this really is a comparison
            FunctionName functionName,
            ConnectorExpression yearSource,
            Constant constant,
            Map<String, ColumnHandle> assignments,
            ColumnTypeProvider<C> columnTypeProvider)
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

        C column = resolve(sourceVariable, assignments);

        return columnTypeProvider.getType(column)
                .flatMap(type -> {
                    if (type instanceof TimestampWithTimeZoneType ttzType && ttzType.getPrecision() == 6) {
                        // Connector supports only timestamp(6) with time zone
                        //checkArgument(ttzType.getPrecision() == 6, "Unexpected type: %s", type);
                        return unwrapYearInTimestampTzComparison(functionName, type, constant, ConstraintExtractor::convertToLongTimestampWithTimeZone, t -> t.equals(TIMESTAMP_TZ_MICROS))
                                .map(domain -> TupleDomain.withColumnDomains(ImmutableMap.of(column, domain)));
                    }
                    else if (type instanceof TimestampType tsType && tsType.getPrecision() == 3) {
                        return unwrapYearInTimestampTzComparison(functionName, type, constant, zonedDateTime -> zonedDateTime.toEpochSecond() * MICROSECONDS_PER_SECOND, t -> t.equals(TIMESTAMP_MILLIS))
                                .map(domain -> TupleDomain.withColumnDomains(ImmutableMap.of(column, domain)));
                    }
                    return Optional.<TupleDomain<C>>empty();
                });
    }

    /**
     * Resolves a Variable to its corresponding ColumnHandle assignment.
     *
     * @param <C> the type of the ColumnHandle
     * @param variable the Variable to resolve
     * @param assignments the map of assignments where the Variable must be present
     * @return the corresponding ColumnHandle assignment for the Variable
     * @throws IllegalArgumentException if no assignment is found for the Variable
     */
    private static <C> C resolve(Variable variable, Map<String, ColumnHandle> assignments)
    {
        ColumnHandle columnHandle = assignments.get(variable.getName());
        checkArgument(columnHandle != null, "No assignment for %s", variable);
        return (C) columnHandle;
    }

    /**
     * This record class, `ExtractionResult`, represents the result of an operation
     * that extracts a tuple domain and the remaining expressions from some particular constraint.
     * It encapsulates both the tuple domain and the remaining connector expression after the extraction.
     *
     * @param <C> A generic type that extends `ColumnHandle`.
     *           It represents the type of the column handle that the `TupleDomain` consists of.
     *
     * @record
     */
    public record ExtractionResult<C extends ColumnHandle>(TupleDomain<C> tupleDomain, ConnectorExpression remainingExpression)
    {
        /**
         * This is a constructor for the `ExtractionResult`. It checks that both `tupleDomain` and
         * `remainingExpression` are not null before initializing.
         *
         * If either `tupleDomain` or `remainingExpression` is null, it throws a `NullPointerException`.
         */
        public ExtractionResult
        {
            requireNonNull(tupleDomain, "tupleDomain is null");
            requireNonNull(remainingExpression, "remainingExpression is null");
        }
    }
}
