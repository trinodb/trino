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
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.TestingColumnHandle;
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
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.Map;
import java.util.Set;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.base.filter.UtcConstraintExtractor.extractTupleDomain;
import static io.trino.spi.expression.StandardFunctions.CAST_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;

public class TestUtcConstraintExtractor
{
    private static final ColumnHandle A_BIGINT = new TestingColumnHandle("a_bigint");

    @Test
    public void testExtractSummary()
    {
        assertThat(extract(
                new Constraint(
                        TupleDomain.withColumnDomains(Map.of(A_BIGINT, Domain.singleValue(BIGINT, 1L))),
                        Constant.TRUE,
                        Map.of(),
                        values -> {
                            throw new AssertionError("should not be called");
                        },
                        Set.of(A_BIGINT))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_BIGINT, Domain.singleValue(BIGINT, 1L))));
    }

    /**
     * Test equivalent of {@code io.trino.sql.planner.iterative.rule.UnwrapCastInComparison} for {@link TimestampWithTimeZoneType}.
     * {@code UnwrapCastInComparison} handles {@link DateType} and {@link TimestampType}, but cannot handle
     * {@link TimestampWithTimeZoneType}. Such unwrap would not be monotonic. If we know
     * that {@link TimestampWithTimeZoneType} is always in UTC zone (point in time, with no time zone information),
     * we can unwrap.
     */
    @Test
    public void testExtractTimestampTzMillisDateComparison()
    {
        String timestampTzColumnSymbol = "timestamp_tz_symbol";
        TimestampWithTimeZoneType columnType = TIMESTAMP_TZ_MILLIS;
        ColumnHandle columnHandle = new TestingColumnHandle(timestampTzColumnSymbol);

        ConnectorExpression castOfColumn = new Call(DATE, CAST_FUNCTION_NAME, ImmutableList.of(new Variable(timestampTzColumnSymbol, columnType)));

        LocalDate someDate = LocalDate.of(2005, 9, 10);
        ConnectorExpression someDateExpression = new Constant(someDate.toEpochDay(), DATE);

        long startOfDateUtcEpochMillis = someDate.atStartOfDay().toEpochSecond(UTC) * MILLISECONDS_PER_SECOND;
        long startOfDateUtc = timestampTzMillisFromEpochMillis(startOfDateUtcEpochMillis);
        long startOfNextDateUtc = timestampTzMillisFromEpochMillis(startOfDateUtcEpochMillis + MILLISECONDS_PER_DAY);

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(castOfColumn, someDateExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, domain(Range.range(columnType, startOfDateUtc, true, startOfNextDateUtc, false)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, NOT_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(castOfColumn, someDateExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, domain(
                        Range.lessThan(columnType, startOfDateUtc),
                        Range.greaterThanOrEqual(columnType, startOfNextDateUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, LESS_THAN_OPERATOR_FUNCTION_NAME, ImmutableList.of(castOfColumn, someDateExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, domain(Range.lessThan(columnType, startOfDateUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(castOfColumn, someDateExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, domain(Range.lessThan(columnType, startOfNextDateUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, GREATER_THAN_OPERATOR_FUNCTION_NAME, ImmutableList.of(castOfColumn, someDateExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, domain(Range.greaterThanOrEqual(columnType, startOfNextDateUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(castOfColumn, someDateExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, domain(Range.greaterThanOrEqual(columnType, startOfDateUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME, ImmutableList.of(castOfColumn, someDateExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, Domain.create(
                        ValueSet.ofRanges(
                                Range.lessThan(columnType, startOfDateUtc),
                                Range.greaterThanOrEqual(columnType, startOfNextDateUtc)),
                        true))));
    }

    /**
     * Test equivalent of {@code io.trino.sql.planner.iterative.rule.UnwrapCastInComparison} for {@link TimestampWithTimeZoneType}.
     * {@code UnwrapCastInComparison} handles {@link DateType} and {@link TimestampType}, but cannot handle
     * {@link TimestampWithTimeZoneType}. Such unwrap would not be monotonic. If we know
     * that {@link TimestampWithTimeZoneType} is always in UTC zone (point in time, with no time zone information),
     * we can unwrap.
     */
    @Test
    public void testExtractTimestampTzMicrosDateComparison()
    {
        String timestampTzColumnSymbol = "timestamp_tz_symbol";
        TimestampWithTimeZoneType columnType = TIMESTAMP_TZ_MICROS;
        ColumnHandle columnHandle = new TestingColumnHandle(timestampTzColumnSymbol);

        ConnectorExpression castOfColumn = new Call(DATE, CAST_FUNCTION_NAME, ImmutableList.of(new Variable(timestampTzColumnSymbol, columnType)));

        LocalDate someDate = LocalDate.of(2005, 9, 10);
        ConnectorExpression someDateExpression = new Constant(someDate.toEpochDay(), DATE);

        long startOfDateUtcEpochMillis = someDate.atStartOfDay().toEpochSecond(UTC) * MILLISECONDS_PER_SECOND;
        LongTimestampWithTimeZone startOfDateUtc = timestampTzMicrosFromEpochMillis(startOfDateUtcEpochMillis);
        LongTimestampWithTimeZone startOfNextDateUtc = timestampTzMicrosFromEpochMillis(startOfDateUtcEpochMillis + MILLISECONDS_PER_DAY);

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(castOfColumn, someDateExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, domain(Range.range(columnType, startOfDateUtc, true, startOfNextDateUtc, false)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, NOT_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(castOfColumn, someDateExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, domain(
                        Range.lessThan(columnType, startOfDateUtc),
                        Range.greaterThanOrEqual(columnType, startOfNextDateUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, LESS_THAN_OPERATOR_FUNCTION_NAME, ImmutableList.of(castOfColumn, someDateExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, domain(Range.lessThan(columnType, startOfDateUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(castOfColumn, someDateExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, domain(Range.lessThan(columnType, startOfNextDateUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, GREATER_THAN_OPERATOR_FUNCTION_NAME, ImmutableList.of(castOfColumn, someDateExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, domain(Range.greaterThanOrEqual(columnType, startOfNextDateUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(castOfColumn, someDateExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, domain(Range.greaterThanOrEqual(columnType, startOfDateUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME, ImmutableList.of(castOfColumn, someDateExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, Domain.create(
                        ValueSet.ofRanges(
                                Range.lessThan(columnType, startOfDateUtc),
                                Range.greaterThanOrEqual(columnType, startOfNextDateUtc)),
                        true))));
    }

    /**
     * Test equivalent of {@code io.trino.sql.planner.iterative.rule.UnwrapDateTruncInComparison} for {@link TimestampWithTimeZoneType}.
     * {@code UnwrapDateTruncInComparison} handles {@link DateType} and {@link TimestampType}, but cannot handle
     * {@link TimestampWithTimeZoneType}. Such unwrap would not be monotonic. If we know
     * that {@link TimestampWithTimeZoneType} is always in UTC zone (point in time, with no time zone information),
     * we can unwrap.
     */
    @Test
    public void testExtractDateTruncTimestampTzMillisComparison()
    {
        String timestampTzColumnSymbol = "timestamp_tz_symbol";
        TimestampWithTimeZoneType columnType = TIMESTAMP_TZ_MILLIS;
        ColumnHandle columnHandle = new TestingColumnHandle(timestampTzColumnSymbol);

        ConnectorExpression truncateToDay = new Call(
                columnType,
                new FunctionName("date_trunc"),
                ImmutableList.of(
                        new Constant(utf8Slice("day"), createVarcharType(17)),
                        new Variable(timestampTzColumnSymbol, columnType)));

        LocalDate someDate = LocalDate.of(2005, 9, 10);
        ConnectorExpression someMidnightExpression = new Constant(
                timestampTzMillisFromEpochMillis(someDate.toEpochDay() * MILLISECONDS_PER_DAY),
                columnType);
        ConnectorExpression someMiddayExpression = new Constant(
                timestampTzMillisFromEpochMillis(someDate.toEpochDay() * MILLISECONDS_PER_DAY + MILLISECONDS_PER_DAY / 2),
                columnType);

        long startOfDateUtcEpochMillis = someDate.atStartOfDay().toEpochSecond(UTC) * MILLISECONDS_PER_SECOND;
        long startOfDateUtc = timestampTzMillisFromEpochMillis(startOfDateUtcEpochMillis);
        long startOfNextDateUtc = timestampTzMillisFromEpochMillis(startOfDateUtcEpochMillis + MILLISECONDS_PER_DAY);

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(truncateToDay, someMidnightExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, domain(Range.range(columnType, startOfDateUtc, true, startOfNextDateUtc, false)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(truncateToDay, someMiddayExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.none());

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, NOT_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(truncateToDay, someMidnightExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, domain(
                        Range.lessThan(columnType, startOfDateUtc),
                        Range.greaterThanOrEqual(columnType, startOfNextDateUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, LESS_THAN_OPERATOR_FUNCTION_NAME, ImmutableList.of(truncateToDay, someMidnightExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, domain(Range.lessThan(columnType, startOfDateUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(truncateToDay, someMidnightExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, domain(Range.lessThan(columnType, startOfNextDateUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, GREATER_THAN_OPERATOR_FUNCTION_NAME, ImmutableList.of(truncateToDay, someMidnightExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, domain(Range.greaterThanOrEqual(columnType, startOfNextDateUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(truncateToDay, someMidnightExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, domain(Range.greaterThanOrEqual(columnType, startOfDateUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME, ImmutableList.of(truncateToDay, someMidnightExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, Domain.create(
                        ValueSet.ofRanges(
                                Range.lessThan(columnType, startOfDateUtc),
                                Range.greaterThanOrEqual(columnType, startOfNextDateUtc)),
                        true))));
    }

    /**
     * Test equivalent of {@code io.trino.sql.planner.iterative.rule.UnwrapDateTruncInComparison} for {@link TimestampWithTimeZoneType}.
     * {@code UnwrapDateTruncInComparison} handles {@link DateType} and {@link TimestampType}, but cannot handle
     * {@link TimestampWithTimeZoneType}. Such unwrap would not be monotonic. If we know
     * that {@link TimestampWithTimeZoneType} is always in UTC zone (point in time, with no time zone information),
     * we can unwrap.
     */
    @Test
    public void testExtractDateTruncTimestampTzMicrosComparison()
    {
        String timestampTzColumnSymbol = "timestamp_tz_symbol";
        TimestampWithTimeZoneType columnType = TIMESTAMP_TZ_MICROS;
        ColumnHandle columnHandle = new TestingColumnHandle(timestampTzColumnSymbol);

        ConnectorExpression truncateToDay = new Call(
                columnType,
                new FunctionName("date_trunc"),
                ImmutableList.of(
                        new Constant(utf8Slice("day"), createVarcharType(17)),
                        new Variable(timestampTzColumnSymbol, columnType)));

        LocalDate someDate = LocalDate.of(2005, 9, 10);
        ConnectorExpression someMidnightExpression = new Constant(
                LongTimestampWithTimeZone.fromEpochMillisAndFraction(someDate.toEpochDay() * MILLISECONDS_PER_DAY, 0, UTC_KEY),
                columnType);
        ConnectorExpression someMiddayExpression = new Constant(
                LongTimestampWithTimeZone.fromEpochMillisAndFraction(someDate.toEpochDay() * MILLISECONDS_PER_DAY, PICOSECONDS_PER_MICROSECOND, UTC_KEY),
                columnType);

        long startOfDateUtcEpochMillis = someDate.atStartOfDay().toEpochSecond(UTC) * MILLISECONDS_PER_SECOND;
        LongTimestampWithTimeZone startOfDateUtc = timestampTzMicrosFromEpochMillis(startOfDateUtcEpochMillis);
        LongTimestampWithTimeZone startOfNextDateUtc = timestampTzMicrosFromEpochMillis(startOfDateUtcEpochMillis + MILLISECONDS_PER_DAY);

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(truncateToDay, someMidnightExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, domain(Range.range(columnType, startOfDateUtc, true, startOfNextDateUtc, false)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(truncateToDay, someMiddayExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.none());

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, NOT_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(truncateToDay, someMidnightExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, domain(
                        Range.lessThan(columnType, startOfDateUtc),
                        Range.greaterThanOrEqual(columnType, startOfNextDateUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, LESS_THAN_OPERATOR_FUNCTION_NAME, ImmutableList.of(truncateToDay, someMidnightExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, domain(Range.lessThan(columnType, startOfDateUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(truncateToDay, someMidnightExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, domain(Range.lessThan(columnType, startOfNextDateUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, GREATER_THAN_OPERATOR_FUNCTION_NAME, ImmutableList.of(truncateToDay, someMidnightExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, domain(Range.greaterThanOrEqual(columnType, startOfNextDateUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(truncateToDay, someMidnightExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, domain(Range.greaterThanOrEqual(columnType, startOfDateUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME, ImmutableList.of(truncateToDay, someMidnightExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, Domain.create(
                        ValueSet.ofRanges(
                                Range.lessThan(columnType, startOfDateUtc),
                                Range.greaterThanOrEqual(columnType, startOfNextDateUtc)),
                        true))));
    }

    /**
     * Test equivalent of {@code io.trino.sql.planner.iterative.rule.UnwrapYearInComparison} for {@link TimestampWithTimeZoneType}.
     * {@code UnwrapYearInComparison} handles {@link DateType} and {@link TimestampType}, but cannot handle
     * {@link TimestampWithTimeZoneType}. Such unwrap would not be monotonic. If we know
     * that {@link TimestampWithTimeZoneType} is always in UTC zone (point in time, with no time zone information),
     * we can unwrap.
     */
    @Test
    public void testExtractYearTimestampTzMicrosComparison()
    {
        String timestampTzColumnSymbol = "timestamp_tz_symbol";
        TimestampWithTimeZoneType columnType = TIMESTAMP_TZ_MICROS;
        TestingColumnHandle columnHandle = new TestingColumnHandle(timestampTzColumnSymbol);

        ConnectorExpression extractYear = new Call(
                BIGINT,
                new FunctionName("year"),
                ImmutableList.of(new Variable(timestampTzColumnSymbol, columnType)));

        LocalDate someDate = LocalDate.of(2005, 9, 10);
        ConnectorExpression yearExpression = new Constant(2005L, BIGINT);

        long startOfYearUtcEpochMillis = someDate.withDayOfYear(1).atStartOfDay().toEpochSecond(UTC) * MILLISECONDS_PER_SECOND;
        LongTimestampWithTimeZone startOfYearUtc = timestampTzMicrosFromEpochMillis(startOfYearUtcEpochMillis);
        LongTimestampWithTimeZone startOfNextDateUtc = timestampTzMicrosFromEpochMillis(someDate.plusYears(1).withDayOfYear(1).atStartOfDay().toEpochSecond(UTC) * MILLISECONDS_PER_SECOND);

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(extractYear, yearExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of((ColumnHandle) columnHandle, domain(Range.range(columnType, startOfYearUtc, true, startOfNextDateUtc, false)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, NOT_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(extractYear, yearExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of((ColumnHandle) columnHandle, domain(
                        Range.lessThan(columnType, startOfYearUtc),
                        Range.greaterThanOrEqual(columnType, startOfNextDateUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, LESS_THAN_OPERATOR_FUNCTION_NAME, ImmutableList.of(extractYear, yearExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of((ColumnHandle) columnHandle, domain(Range.lessThan(columnType, startOfYearUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(extractYear, yearExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of((ColumnHandle) columnHandle, domain(Range.lessThan(columnType, startOfNextDateUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, GREATER_THAN_OPERATOR_FUNCTION_NAME, ImmutableList.of(extractYear, yearExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of((ColumnHandle) columnHandle, domain(Range.greaterThanOrEqual(columnType, startOfNextDateUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(extractYear, yearExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of((ColumnHandle) columnHandle, domain(Range.greaterThanOrEqual(columnType, startOfYearUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME, ImmutableList.of(extractYear, yearExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of((ColumnHandle) columnHandle, Domain.create(
                        ValueSet.ofRanges(
                                Range.lessThan(columnType, startOfYearUtc),
                                Range.greaterThanOrEqual(columnType, startOfNextDateUtc)),
                        true))));
    }

    /**
     * Test equivalent of {@code io.trino.sql.planner.iterative.rule.UnwrapYearInComparison} for {@link TimestampWithTimeZoneType}.
     * {@code UnwrapYearInComparison} handles {@link DateType} and {@link TimestampType}, but cannot handle
     * {@link TimestampWithTimeZoneType}. Such unwrap would not be monotonic. If we know
     * that {@link TimestampWithTimeZoneType} is always in UTC zone (point in time, with no time zone information),
     * we can unwrap.
     */
    @Test
    public void testExtractYearTimestampTzMillisComparison()
    {
        String timestampTzColumnSymbol = "timestamp_tz_symbol";
        TimestampWithTimeZoneType columnType = TIMESTAMP_TZ_MILLIS;
        TestingColumnHandle columnHandle = new TestingColumnHandle(timestampTzColumnSymbol);

        ConnectorExpression extractYear = new Call(
                BIGINT,
                new FunctionName("year"),
                ImmutableList.of(new Variable(timestampTzColumnSymbol, columnType)));

        LocalDate someDate = LocalDate.of(2005, 9, 10);
        ConnectorExpression yearExpression = new Constant(2005L, BIGINT);

        long startOfYearUtcEpochMillis = someDate.withDayOfYear(1).atStartOfDay().toEpochSecond(UTC) * MILLISECONDS_PER_SECOND;
        long startOfYearUtc = timestampTzMillisFromEpochMillis(startOfYearUtcEpochMillis);
        long startOfNextDateUtc = timestampTzMillisFromEpochMillis(someDate.plusYears(1).withDayOfYear(1).atStartOfDay().toEpochSecond(UTC) * MILLISECONDS_PER_SECOND);

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(extractYear, yearExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of((ColumnHandle) columnHandle, domain(Range.range(columnType, startOfYearUtc, true, startOfNextDateUtc, false)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, NOT_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(extractYear, yearExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of((ColumnHandle) columnHandle, domain(
                        Range.lessThan(columnType, startOfYearUtc),
                        Range.greaterThanOrEqual(columnType, startOfNextDateUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, LESS_THAN_OPERATOR_FUNCTION_NAME, ImmutableList.of(extractYear, yearExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of((ColumnHandle) columnHandle, domain(Range.lessThan(columnType, startOfYearUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(extractYear, yearExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of((ColumnHandle) columnHandle, domain(Range.lessThan(columnType, startOfNextDateUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, GREATER_THAN_OPERATOR_FUNCTION_NAME, ImmutableList.of(extractYear, yearExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of((ColumnHandle) columnHandle, domain(Range.greaterThanOrEqual(columnType, startOfNextDateUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(extractYear, yearExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of((ColumnHandle) columnHandle, domain(Range.greaterThanOrEqual(columnType, startOfYearUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME, ImmutableList.of(extractYear, yearExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of((ColumnHandle) columnHandle, Domain.create(
                        ValueSet.ofRanges(
                                Range.lessThan(columnType, startOfYearUtc),
                                Range.greaterThanOrEqual(columnType, startOfNextDateUtc)),
                        true))));
    }

    @Test
    public void testIntersectSummaryAndExpressionExtraction()
    {
        String timestampTzColumnSymbol = "timestamp_tz_symbol";
        TimestampWithTimeZoneType columnType = TIMESTAMP_TZ_MICROS;
        TestingColumnHandle columnHandle = new TestingColumnHandle(timestampTzColumnSymbol);

        ConnectorExpression castOfColumn = new Call(DATE, CAST_FUNCTION_NAME, ImmutableList.of(new Variable(timestampTzColumnSymbol, columnType)));

        LocalDate someDate = LocalDate.of(2005, 9, 10);
        ConnectorExpression someDateExpression = new Constant(someDate.toEpochDay(), DATE);

        long startOfDateUtcEpochMillis = someDate.atStartOfDay().toEpochSecond(UTC) * MILLISECONDS_PER_SECOND;
        LongTimestampWithTimeZone startOfDateUtc = timestampTzMicrosFromEpochMillis(startOfDateUtcEpochMillis);
        LongTimestampWithTimeZone startOfNextDateUtc = timestampTzMicrosFromEpochMillis(startOfDateUtcEpochMillis + MILLISECONDS_PER_DAY);
        LongTimestampWithTimeZone startOfNextNextDateUtc = timestampTzMicrosFromEpochMillis(startOfDateUtcEpochMillis + MILLISECONDS_PER_DAY * 2);

        assertThat(extract(
                constraint(
                        TupleDomain.withColumnDomains(Map.of(columnHandle, domain(Range.lessThan(columnType, startOfNextNextDateUtc)))),
                        new Call(BOOLEAN, NOT_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(castOfColumn, someDateExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(
                        (ColumnHandle) columnHandle, domain(
                                Range.lessThan(columnType, startOfDateUtc),
                                Range.range(columnType, startOfNextDateUtc, true, startOfNextNextDateUtc, false)))));

        assertThat(extract(
                constraint(
                        TupleDomain.withColumnDomains(Map.of(columnHandle, domain(Range.lessThan(columnType, startOfNextDateUtc)))),
                        new Call(BOOLEAN, GREATER_THAN_OPERATOR_FUNCTION_NAME, ImmutableList.of(castOfColumn, someDateExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.none());

        assertThat(extract(
                constraint(
                        TupleDomain.withColumnDomains(Map.of(A_BIGINT, Domain.singleValue(BIGINT, 1L))),
                        new Call(BOOLEAN, GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(castOfColumn, someDateExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(
                        A_BIGINT, Domain.singleValue(BIGINT, 1L),
                        columnHandle, domain(Range.greaterThanOrEqual(columnType, startOfDateUtc)))));
    }

    private static TupleDomain<ColumnHandle> extract(Constraint constraint)
    {
        UtcConstraintExtractor.ExtractionResult result = extractTupleDomain(constraint);
        assertThat(result.remainingExpression())
                .isEqualTo(Constant.TRUE);
        return result.tupleDomain();
    }

    private static Constraint constraint(ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        return constraint(TupleDomain.all(), expression, assignments);
    }

    private static Constraint constraint(TupleDomain<ColumnHandle> summary, ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        return new Constraint(summary, expression, assignments);
    }

    private static long timestampTzMillisFromEpochMillis(long epochMillis)
    {
        return packDateTimeWithZone(epochMillis, UTC_KEY);
    }

    private static LongTimestampWithTimeZone timestampTzMicrosFromEpochMillis(long epochMillis)
    {
        return LongTimestampWithTimeZone.fromEpochMillisAndFraction(epochMillis, 0, UTC_KEY);
    }

    private static Domain domain(Range first, Range... rest)
    {
        return Domain.create(ValueSet.ofRanges(first, rest), false);
    }
}
