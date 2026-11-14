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
import io.trino.plugin.base.expression.ConnectorExpressions;
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
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.base.filter.UtcConstraintExtractor.extractTupleDomain;
import static io.trino.spi.expression.StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.CAST_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IDENTICAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IN_PREDICATE_FUNCTION_NAME;
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
import static java.time.DayOfWeek.MONDAY;
import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.time.temporal.TemporalAdjusters.previousOrSame;
import static org.assertj.core.api.Assertions.assertThat;

public class TestUtcConstraintExtractor
{
    record TestingColumnHandle(String name)
            implements ColumnHandle {}

    private static final ColumnHandle A_BIGINT = new TestingColumnHandle("a_bigint");

    @Test
    public void testExtractSummary()
    {
        assertThat(extract(
                new Constraint(
                        TupleDomain.withColumnDomains(Map.of(A_BIGINT, Domain.singleValue(BIGINT, 1L))),
                        Constant.TRUE,
                        Map.of())))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_BIGINT, Domain.singleValue(BIGINT, 1L))));
    }

    /// Test equivalent of `io.trino.sql.planner.ComparisonPreimages` for [TimestampWithTimeZoneType].
    /// `ComparisonPreimages` handles [DateType] and [TimestampType], but cannot handle
    /// [TimestampWithTimeZoneType]. Such unwrap would not be monotonic. If we know
    /// that [TimestampWithTimeZoneType] is always in UTC zone (point in time, with no time zone information),
    /// we can unwrap.
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
                        new Call(BOOLEAN, IDENTICAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(castOfColumn, someDateExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(
                        columnHandle, Domain.create(ValueSet.ofRanges(Range.range(columnType, startOfDateUtc, true, startOfNextDateUtc, false)), false))));
    }

    @Test
    public void testExtractTimestampTzDateComparisonWithConstantOnLeft()
    {
        // The engine canonicalizes greater-than comparisons to less-than forms with flipped operands, which
        // places the constant first. Operand order is not part of the connector expression contract, so these
        // must extract the same domain as the equivalent column-first comparison.
        String timestampTzColumnSymbol = "timestamp_tz_symbol";
        TimestampWithTimeZoneType columnType = TIMESTAMP_TZ_MILLIS;
        ColumnHandle columnHandle = new TestingColumnHandle(timestampTzColumnSymbol);

        ConnectorExpression castOfColumn = new Call(DATE, CAST_FUNCTION_NAME, ImmutableList.of(new Variable(timestampTzColumnSymbol, columnType)));

        LocalDate someDate = LocalDate.of(2005, 9, 10);
        ConnectorExpression someDateExpression = new Constant(someDate.toEpochDay(), DATE);

        long startOfDateUtcEpochMillis = someDate.atStartOfDay().toEpochSecond(UTC) * MILLISECONDS_PER_SECOND;
        long startOfDateUtc = timestampTzMillisFromEpochMillis(startOfDateUtcEpochMillis);
        long startOfNextDateUtc = timestampTzMillisFromEpochMillis(startOfDateUtcEpochMillis + MILLISECONDS_PER_DAY);

        // someDate < CAST(col)  <=>  CAST(col) > someDate
        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, LESS_THAN_OPERATOR_FUNCTION_NAME, ImmutableList.of(someDateExpression, castOfColumn)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, domain(Range.greaterThanOrEqual(columnType, startOfNextDateUtc)))));

        // someDate <= CAST(col)  <=>  CAST(col) >= someDate
        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(someDateExpression, castOfColumn)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, domain(Range.greaterThanOrEqual(columnType, startOfDateUtc)))));

        // EQUAL is symmetric: someDate = CAST(col)
        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(someDateExpression, castOfColumn)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, domain(Range.range(columnType, startOfDateUtc, true, startOfNextDateUtc, false)))));
    }

    @Test
    public void testExtractTimestampTzDateInPredicate()
    {
        String timestampTzColumnSymbol = "timestamp_tz_symbol";
        TimestampWithTimeZoneType columnType = TIMESTAMP_TZ_MICROS;
        ColumnHandle columnHandle = new TestingColumnHandle(timestampTzColumnSymbol);

        ConnectorExpression castOfColumn = new Call(DATE, CAST_FUNCTION_NAME, ImmutableList.of(new Variable(timestampTzColumnSymbol, columnType)));

        // date(ts) IN (date '2025-07-01', date '2025-10-15'): days that are not adjacent, which no single range can express
        LocalDate july = LocalDate.of(2025, 7, 1);
        LocalDate october = LocalDate.of(2025, 10, 15);
        long julyMillis = july.atStartOfDay().toEpochSecond(UTC) * MILLISECONDS_PER_SECOND;
        long octoberMillis = october.atStartOfDay().toEpochSecond(UTC) * MILLISECONDS_PER_SECOND;

        assertThat(extract(
                constraint(
                        in(castOfColumn, new Constant(july.toEpochDay(), DATE), new Constant(october.toEpochDay(), DATE)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(
                        columnHandle, domain(
                                Range.range(columnType, timestampTzMicrosFromEpochMillis(julyMillis), true, timestampTzMicrosFromEpochMillis(julyMillis + MILLISECONDS_PER_DAY), false),
                                Range.range(columnType, timestampTzMicrosFromEpochMillis(octoberMillis), true, timestampTzMicrosFromEpochMillis(octoberMillis + MILLISECONDS_PER_DAY), false)))));
    }

    @Test
    public void testExtractTimestampTzDateInPredicateWithUnconvertibleItem()
    {
        String timestampTzColumnSymbol = "timestamp_tz_symbol";
        TimestampWithTimeZoneType columnType = TIMESTAMP_TZ_MILLIS;
        ColumnHandle columnHandle = new TestingColumnHandle(timestampTzColumnSymbol);

        ConnectorExpression castOfColumn = new Call(DATE, CAST_FUNCTION_NAME, ImmutableList.of(new Variable(timestampTzColumnSymbol, columnType)));
        ConnectorExpression july = new Constant(LocalDate.of(2025, 7, 1).toEpochDay(), DATE);

        // A null item, or an item that is not a constant, can't be unwrapped, so the entire IN should be a remaining expression
        for (ConnectorExpression item : List.of(new Constant(null, DATE), new Variable("some_date", DATE))) {
            ConnectorExpression inExpression = in(castOfColumn, july, item);
            UtcConstraintExtractor.ExtractionResult result = extractTupleDomain(
                    constraint(inExpression, Map.of(timestampTzColumnSymbol, columnHandle, "some_date", new TestingColumnHandle("some_date"))));
            assertThat(result.tupleDomain())
                    .isEqualTo(TupleDomain.all());
            assertThat(result.remainingExpression())
                    .isEqualTo(inExpression);
        }
    }

    @Test
    public void testExtractTimestampTzDateInPredicateWithOutOfRangeDate()
    {
        // A date item no TIMESTAMP WITH TIME ZONE value falls on can't be turned into a range, so the entire IN should
        // be a remaining expression rather than an error
        String timestampTzColumnSymbol = "timestamp_tz_symbol";
        ColumnHandle columnHandle = new TestingColumnHandle(timestampTzColumnSymbol);
        ConnectorExpression startOf2024 = new Constant(LocalDate.of(2024, 1, 1).toEpochDay(), DATE);
        for (TimestampWithTimeZoneType columnType : List.of(TIMESTAMP_TZ_MILLIS, TIMESTAMP_TZ_MICROS)) {
            ConnectorExpression castOfColumn = new Call(DATE, CAST_FUNCTION_NAME, ImmutableList.of(new Variable(timestampTzColumnSymbol, columnType)));
            for (LocalDate date : List.of(LocalDate.of(100_000, 1, 1), LocalDate.of(-100_000, 1, 1))) {
                ConnectorExpression inExpression = in(castOfColumn, startOf2024, new Constant(date.toEpochDay(), DATE));
                UtcConstraintExtractor.ExtractionResult result = extractTupleDomain(
                        constraint(inExpression, Map.of(timestampTzColumnSymbol, columnHandle)));
                assertThat(result.tupleDomain())
                        .isEqualTo(TupleDomain.all());
                assertThat(result.remainingExpression())
                        .isEqualTo(inExpression);
            }
        }
    }

    @Test
    public void testExtractDateTruncTimestampTzInPredicate()
    {
        assertDateTruncTimestampTzInDomain(
                "week",
                LocalDate.of(2024, 1, 1),
                LocalDate.of(2024, 1, 8),
                LocalDate.of(2024, 1, 15),
                LocalDate.of(2024, 1, 22));
        assertDateTruncTimestampTzInDomain(
                "quarter",
                LocalDate.of(2024, 1, 1),
                LocalDate.of(2024, 4, 1),
                LocalDate.of(2024, 7, 1),
                LocalDate.of(2024, 10, 1));
    }

    private static void assertDateTruncTimestampTzInDomain(
            String unit,
            LocalDate firstPeriodStart,
            LocalDate firstPeriodEnd,
            LocalDate secondPeriodStart,
            LocalDate secondPeriodEnd)
    {
        String timestampTzColumnSymbol = "timestamp_tz_symbol";
        ColumnHandle columnHandle = new TestingColumnHandle(timestampTzColumnSymbol);
        for (TimestampWithTimeZoneType columnType : List.of(TIMESTAMP_TZ_MILLIS, TIMESTAMP_TZ_MICROS)) {
            ConnectorExpression truncate = new Call(
                    columnType,
                    new FunctionName("date_trunc"),
                    ImmutableList.of(
                            new Constant(utf8Slice(unit), createVarcharType(17)),
                            new Variable(timestampTzColumnSymbol, columnType)));
            Constant firstStart = timestampTzConstant(columnType, firstPeriodStart.toEpochDay() * MILLISECONDS_PER_DAY);
            Constant firstEnd = timestampTzConstant(columnType, firstPeriodEnd.toEpochDay() * MILLISECONDS_PER_DAY);
            Constant secondStart = timestampTzConstant(columnType, secondPeriodStart.toEpochDay() * MILLISECONDS_PER_DAY);
            Constant secondEnd = timestampTzConstant(columnType, secondPeriodEnd.toEpochDay() * MILLISECONDS_PER_DAY);

            assertThat(extract(constraint(in(truncate, firstStart, secondStart), Map.of(timestampTzColumnSymbol, columnHandle))))
                    .isEqualTo(TupleDomain.withColumnDomains(Map.of(
                            columnHandle, domain(
                                    Range.range(columnType, firstStart.getValue(), true, firstEnd.getValue(), false),
                                    Range.range(columnType, secondStart.getValue(), true, secondEnd.getValue(), false)))));
        }
    }

    @Test
    public void testExtractDateTruncTimestampTzInPredicateWithOutOfRangePeriod()
    {
        // date_trunc of a value near either end of TIMESTAMP WITH TIME ZONE gives a period that runs past it: the next
        // period starts after the largest value, or the period starts before the smallest one. Such an item can't be
        // turned into a range, so the entire IN should be a remaining expression rather than an error
        String timestampTzColumnSymbol = "timestamp_tz_symbol";
        ColumnHandle columnHandle = new TestingColumnHandle(timestampTzColumnSymbol);
        long maxEpochMillis = (1L << 51) - 1; // DateTimeEncoding packs epoch millis into 52 bits
        ZonedDateTime max = Instant.ofEpochMilli(maxEpochMillis).atZone(UTC); // 73326-09-11 20:14:45.247 UTC
        Map<String, ZonedDateTime> lastPeriodStarts = ImmutableMap.of(
                "hour", max.truncatedTo(HOURS),
                "day", max.truncatedTo(DAYS),
                "week", max.truncatedTo(DAYS).with(previousOrSame(MONDAY)),
                "month", max.truncatedTo(DAYS).withDayOfMonth(1),
                "quarter", max.truncatedTo(DAYS).withMonth(7).withDayOfMonth(1),
                "year", max.truncatedTo(DAYS).withDayOfYear(1));
        long startOf2024 = LocalDate.of(2024, 1, 1).toEpochDay() * MILLISECONDS_PER_DAY;
        for (TimestampWithTimeZoneType columnType : List.of(TIMESTAMP_TZ_MILLIS, TIMESTAMP_TZ_MICROS)) {
            for (Map.Entry<String, ZonedDateTime> lastPeriodStart : lastPeriodStarts.entrySet()) {
                ConnectorExpression truncate = new Call(
                        columnType,
                        new FunctionName("date_trunc"),
                        ImmutableList.of(
                                new Constant(utf8Slice(lastPeriodStart.getKey()), createVarcharType(17)),
                                new Variable(timestampTzColumnSymbol, columnType)));
                for (long edgeEpochMillis : List.of(lastPeriodStart.getValue().toInstant().toEpochMilli(), -maxEpochMillis - 1)) {
                    ConnectorExpression inExpression = in(
                            truncate,
                            timestampTzConstant(columnType, startOf2024),
                            timestampTzConstant(columnType, edgeEpochMillis));
                    UtcConstraintExtractor.ExtractionResult result = extractTupleDomain(
                            constraint(inExpression, Map.of(timestampTzColumnSymbol, columnHandle)));
                    assertThat(result.tupleDomain())
                            .isEqualTo(TupleDomain.all());
                    assertThat(result.remainingExpression())
                            .isEqualTo(inExpression);
                }
            }
        }
    }

    @Test
    public void testExtractYearTimestampTzInPredicate()
    {
        // The IN reuses every unwrapping its equalities get, not only the date cast
        String timestampTzColumnSymbol = "timestamp_tz_symbol";
        TimestampWithTimeZoneType columnType = TIMESTAMP_TZ_MILLIS;
        ColumnHandle columnHandle = new TestingColumnHandle(timestampTzColumnSymbol);

        ConnectorExpression extractYear = new Call(
                BIGINT,
                new FunctionName("year"),
                ImmutableList.of(new Variable(timestampTzColumnSymbol, columnType)));

        long startOf2024 = timestampTzMillisFromEpochMillis(LocalDate.of(2024, 1, 1).atStartOfDay().toEpochSecond(UTC) * MILLISECONDS_PER_SECOND);
        long startOf2025 = timestampTzMillisFromEpochMillis(LocalDate.of(2025, 1, 1).atStartOfDay().toEpochSecond(UTC) * MILLISECONDS_PER_SECOND);
        long startOf2026 = timestampTzMillisFromEpochMillis(LocalDate.of(2026, 1, 1).atStartOfDay().toEpochSecond(UTC) * MILLISECONDS_PER_SECOND);
        long startOf2027 = timestampTzMillisFromEpochMillis(LocalDate.of(2027, 1, 1).atStartOfDay().toEpochSecond(UTC) * MILLISECONDS_PER_SECOND);

        assertThat(extract(
                constraint(
                        in(extractYear, new Constant(2024L, BIGINT), new Constant(2026L, BIGINT)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(
                        columnHandle, domain(
                                Range.range(columnType, startOf2024, true, startOf2025, false),
                                Range.range(columnType, startOf2026, true, startOf2027, false)))));
    }

    @Test
    public void testExtractYearTimestampTzInPredicateWithOutOfRangeYear()
    {
        // year() returns bigint, so an item can be a year no TIMESTAMP WITH TIME ZONE value has. Such an item can't be
        // turned into a range, so the entire IN should be a remaining expression rather than an error
        String timestampTzColumnSymbol = "timestamp_tz_symbol";
        ColumnHandle columnHandle = new TestingColumnHandle(timestampTzColumnSymbol);
        for (TimestampWithTimeZoneType columnType : List.of(TIMESTAMP_TZ_MILLIS, TIMESTAMP_TZ_MICROS)) {
            ConnectorExpression extractYear = new Call(
                    BIGINT,
                    new FunctionName("year"),
                    ImmutableList.of(new Variable(timestampTzColumnSymbol, columnType)));
            // 584556074 is a valid java.time year whose start cannot be represented in epoch millis
            for (long year : List.of(2_147_483_648L, 1_000_000_000L, 584_556_074L, 100_000L, -100_000L)) {
                ConnectorExpression inExpression = in(extractYear, new Constant(2024L, BIGINT), new Constant(year, BIGINT));
                UtcConstraintExtractor.ExtractionResult result = extractTupleDomain(
                        constraint(inExpression, Map.of(timestampTzColumnSymbol, columnHandle)));
                assertThat(result.tupleDomain())
                        .isEqualTo(TupleDomain.all());
                assertThat(result.remainingExpression())
                        .isEqualTo(inExpression);
            }
        }
    }

    /// Test equivalent of `io.trino.sql.planner.ComparisonPreimages` for [TimestampWithTimeZoneType].
    /// `ComparisonPreimages` handles [DateType] and [TimestampType], but cannot handle
    /// [TimestampWithTimeZoneType]. Such unwrap would not be monotonic. If we know
    /// that [TimestampWithTimeZoneType] is always in UTC zone (point in time, with no time zone information),
    /// we can unwrap.
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
                        new Call(BOOLEAN, IDENTICAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(castOfColumn, someDateExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(
                        columnHandle, Domain.create(ValueSet.ofRanges(Range.range(columnType, startOfDateUtc, true, startOfNextDateUtc, false)), false))));
    }

    @Test
    public void testExtractTimestampTzDateOrComparison()
    {
        String timestampTzColumnSymbol = "timestamp_tz_symbol";
        TimestampWithTimeZoneType columnType = TIMESTAMP_TZ_MILLIS;
        ColumnHandle columnHandle = new TestingColumnHandle(timestampTzColumnSymbol);

        ConnectorExpression castOfColumn = new Call(DATE, CAST_FUNCTION_NAME, ImmutableList.of(new Variable(timestampTzColumnSymbol, columnType)));

        // date(c2_ts) BETWEEN date'2025-07-01' AND date'2025-07-31' OR date(c2_ts) BETWEEN date'2025-10-01' AND date'2025-10-31'
        LocalDate julyStart = LocalDate.of(2025, 7, 1);
        LocalDate julyEnd = LocalDate.of(2025, 7, 31);
        LocalDate octoberStart = LocalDate.of(2025, 10, 1);
        LocalDate octoberEnd = LocalDate.of(2025, 10, 31);

        long julyStartMillis = julyStart.atStartOfDay().toEpochSecond(UTC) * MILLISECONDS_PER_SECOND;
        long julyEndNextDayMillis = julyEnd.plusDays(1).atStartOfDay().toEpochSecond(UTC) * MILLISECONDS_PER_SECOND;
        long octoberStartMillis = octoberStart.atStartOfDay().toEpochSecond(UTC) * MILLISECONDS_PER_SECOND;
        long octoberEndNextDayMillis = octoberEnd.plusDays(1).atStartOfDay().toEpochSecond(UTC) * MILLISECONDS_PER_SECOND;

        // Build: date(c2_ts) >= date'2025-07-01' AND date(c2_ts) <= date'2025-07-31'
        ConnectorExpression julyGe = new Call(BOOLEAN, GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(castOfColumn, new Constant(julyStart.toEpochDay(), DATE)));
        ConnectorExpression julyLe = new Call(BOOLEAN, LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(castOfColumn, new Constant(julyEnd.toEpochDay(), DATE)));
        ConnectorExpression julyBetween = ConnectorExpressions.and(julyGe, julyLe);

        // Build: date(c2_ts) >= date'2025-10-01' AND date(c2_ts) <= date'2025-10-31'
        ConnectorExpression octoberGe = new Call(BOOLEAN, GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(castOfColumn, new Constant(octoberStart.toEpochDay(), DATE)));
        ConnectorExpression octoberLe = new Call(BOOLEAN, LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(castOfColumn, new Constant(octoberEnd.toEpochDay(), DATE)));
        ConnectorExpression octoberBetween = ConnectorExpressions.and(octoberGe, octoberLe);

        // Build: julyBetween OR octoberBetween
        ConnectorExpression orExpression = ConnectorExpressions.or(julyBetween, octoberBetween);

        assertThat(extract(
                constraint(orExpression, Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(
                        columnHandle, domain(
                                Range.range(columnType, timestampTzMillisFromEpochMillis(julyStartMillis), true, timestampTzMillisFromEpochMillis(julyEndNextDayMillis), false),
                                Range.range(columnType, timestampTzMillisFromEpochMillis(octoberStartMillis), true, timestampTzMillisFromEpochMillis(octoberEndNextDayMillis), false)))));
    }

    @Test
    public void testExtractTimestampTzDateOrWithUnconvertibleDisjunct()
    {
        String timestampTzColumnSymbol = "timestamp_tz_symbol";
        TimestampWithTimeZoneType columnType = TIMESTAMP_TZ_MILLIS;
        ColumnHandle columnHandle = new TestingColumnHandle(timestampTzColumnSymbol);

        ConnectorExpression castOfColumn = new Call(DATE, CAST_FUNCTION_NAME, ImmutableList.of(new Variable(timestampTzColumnSymbol, columnType)));

        LocalDate julyStart = LocalDate.of(2025, 7, 1);

        ConnectorExpression julyGe = new Call(BOOLEAN, GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(castOfColumn, new Constant(julyStart.toEpochDay(), DATE)));
        // The second disjunct is something that can't be converted
        ConnectorExpression unknownExpression = new Variable("some_other", BOOLEAN);
        ConnectorExpression orExpression = ConnectorExpressions.or(julyGe, unknownExpression);

        // When one disjunct can't be converted, the entire OR should be a remaining expression
        UtcConstraintExtractor.ExtractionResult result = extractTupleDomain(
                constraint(orExpression, Map.of(timestampTzColumnSymbol, columnHandle, "some_other", new TestingColumnHandle("some_other"))));
        assertThat(result.tupleDomain())
                .isEqualTo(TupleDomain.all());
        assertThat(result.remainingExpression())
                .isEqualTo(orExpression);
    }

    @Test
    public void testExtractTimestampTzDateOrMultipleColumns()
    {
        // OR across different columns: (date(ts1) >= X) OR (date(ts2) >= Y)
        // columnWiseUnion would be a superset of the strict union, so we must NOT convert this
        String ts1Symbol = "ts1_symbol";
        String ts2Symbol = "ts2_symbol";
        TimestampWithTimeZoneType columnType = TIMESTAMP_TZ_MILLIS;
        ColumnHandle ts1Handle = new TestingColumnHandle(ts1Symbol);
        ColumnHandle ts2Handle = new TestingColumnHandle(ts2Symbol);

        ConnectorExpression castTs1 = new Call(DATE, CAST_FUNCTION_NAME, ImmutableList.of(new Variable(ts1Symbol, columnType)));
        ConnectorExpression castTs2 = new Call(DATE, CAST_FUNCTION_NAME, ImmutableList.of(new Variable(ts2Symbol, columnType)));

        LocalDate someDate = LocalDate.of(2025, 7, 1);
        ConnectorExpression someDateExpression = new Constant(someDate.toEpochDay(), DATE);

        ConnectorExpression ts1Ge = new Call(BOOLEAN, GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(castTs1, someDateExpression));
        ConnectorExpression ts2Ge = new Call(BOOLEAN, GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(castTs2, someDateExpression));
        ConnectorExpression orExpression = ConnectorExpressions.or(ts1Ge, ts2Ge);

        // Multi-column OR should remain as a remaining expression since columnWiseUnion is not exact
        UtcConstraintExtractor.ExtractionResult result = extractTupleDomain(
                constraint(orExpression, Map.of(ts1Symbol, ts1Handle, ts2Symbol, ts2Handle)));
        assertThat(result.tupleDomain())
                .isEqualTo(TupleDomain.all());
        assertThat(result.remainingExpression())
                .isEqualTo(orExpression);
    }

    /// Test equivalent of `io.trino.sql.planner.ComparisonPreimages` for [TimestampWithTimeZoneType].
    /// `ComparisonPreimages` handles [DateType] and [TimestampType], but cannot handle
    /// [TimestampWithTimeZoneType]. Such unwrap would not be monotonic. If we know
    /// that [TimestampWithTimeZoneType] is always in UTC zone (point in time, with no time zone information),
    /// we can unwrap.
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
                        new Call(BOOLEAN, IDENTICAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(truncateToDay, someMidnightExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(
                        columnHandle, Domain.create(ValueSet.ofRanges(Range.range(columnType, startOfDateUtc, true, startOfNextDateUtc, false)), false))));
    }

    @Test
    public void testExtractDateTruncTimestampTzMillisSupportedUnitComparison()
    {
        String timestampTzColumnSymbol = "timestamp_tz_symbol";
        TimestampWithTimeZoneType columnType = TIMESTAMP_TZ_MILLIS;
        ColumnHandle columnHandle = new TestingColumnHandle(timestampTzColumnSymbol);

        assertDateTruncTimestampTzMillisDomain(
                "week",
                LocalDate.of(2024, 1, 1),
                LocalDate.of(2024, 1, 8),
                columnType,
                timestampTzColumnSymbol,
                columnHandle);

        assertDateTruncTimestampTzMillisDomain(
                "month",
                LocalDate.of(2024, 4, 1),
                LocalDate.of(2024, 5, 1),
                columnType,
                timestampTzColumnSymbol,
                columnHandle);

        assertDateTruncTimestampTzMillisDomain(
                "quarter",
                LocalDate.of(2024, 4, 1),
                LocalDate.of(2024, 7, 1),
                columnType,
                timestampTzColumnSymbol,
                columnHandle);

        assertDateTruncTimestampTzMillisDomain(
                "year",
                LocalDate.of(2024, 1, 1),
                LocalDate.of(2025, 1, 1),
                columnType,
                timestampTzColumnSymbol,
                columnHandle);
    }

    private static void assertDateTruncTimestampTzMillisDomain(
            String unit,
            LocalDate periodStartDate,
            LocalDate nextPeriodStartDate,
            TimestampWithTimeZoneType columnType,
            String timestampTzColumnSymbol,
            ColumnHandle columnHandle)
    {
        ConnectorExpression truncate = new Call(
                columnType,
                new FunctionName("date_trunc"),
                ImmutableList.of(
                        new Constant(utf8Slice(unit), createVarcharType(17)),
                        new Variable(timestampTzColumnSymbol, columnType)));

        long periodStartMillis = periodStartDate.atStartOfDay().toEpochSecond(UTC) * MILLISECONDS_PER_SECOND;
        long nextPeriodStartMillis = nextPeriodStartDate.atStartOfDay().toEpochSecond(UTC) * MILLISECONDS_PER_SECOND;
        long periodStart = timestampTzMillisFromEpochMillis(periodStartMillis);
        long nextPeriodStart = timestampTzMillisFromEpochMillis(nextPeriodStartMillis);

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(
                                truncate,
                                new Constant(periodStart, columnType))),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, domain(Range.range(columnType, periodStart, true, nextPeriodStart, false)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(
                                truncate,
                                new Constant(timestampTzMillisFromEpochMillis(periodStartMillis + MILLISECONDS_PER_DAY), columnType))),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.none());

        assertThat(extract(constraint(new Call(BOOLEAN, NOT_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(truncate, new Constant(periodStart, columnType))), Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, domain(Range.lessThan(columnType, periodStart), Range.greaterThanOrEqual(columnType, nextPeriodStart)))));
        assertThat(extract(constraint(new Call(BOOLEAN, LESS_THAN_OPERATOR_FUNCTION_NAME, ImmutableList.of(truncate, new Constant(periodStart, columnType))), Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, domain(Range.lessThan(columnType, periodStart)))));
        assertThat(extract(constraint(new Call(BOOLEAN, LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(truncate, new Constant(periodStart, columnType))), Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, domain(Range.lessThan(columnType, nextPeriodStart)))));
        assertThat(extract(constraint(new Call(BOOLEAN, GREATER_THAN_OPERATOR_FUNCTION_NAME, ImmutableList.of(truncate, new Constant(periodStart, columnType))), Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, domain(Range.greaterThanOrEqual(columnType, nextPeriodStart)))));
        assertThat(extract(constraint(new Call(BOOLEAN, GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(truncate, new Constant(periodStart, columnType))), Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, domain(Range.greaterThanOrEqual(columnType, periodStart)))));
        assertThat(extract(constraint(new Call(BOOLEAN, IDENTICAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(truncate, new Constant(periodStart, columnType))), Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, Domain.create(ValueSet.ofRanges(Range.range(columnType, periodStart, true, nextPeriodStart, false)), false))));
    }

    /// Test equivalent of `io.trino.sql.planner.ComparisonPreimages` for [TimestampWithTimeZoneType].
    /// `ComparisonPreimages` handles [DateType] and [TimestampType], but cannot handle
    /// [TimestampWithTimeZoneType]. Such unwrap would not be monotonic. If we know
    /// that [TimestampWithTimeZoneType] is always in UTC zone (point in time, with no time zone information),
    /// we can unwrap.
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
                        new Call(BOOLEAN, IDENTICAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(truncateToDay, someMidnightExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(
                        columnHandle, Domain.create(ValueSet.ofRanges(Range.range(columnType, startOfDateUtc, true, startOfNextDateUtc, false)), false))));
    }

    @Test
    public void testExtractDateTruncTimestampTzMicrosSupportedUnitComparison()
    {
        String timestampTzColumnSymbol = "timestamp_tz_symbol";
        TimestampWithTimeZoneType columnType = TIMESTAMP_TZ_MICROS;
        ColumnHandle columnHandle = new TestingColumnHandle(timestampTzColumnSymbol);

        assertDateTruncTimestampTzMicrosDomain(
                "week",
                LocalDate.of(2024, 1, 1),
                LocalDate.of(2024, 1, 8),
                columnType,
                timestampTzColumnSymbol,
                columnHandle);

        assertDateTruncTimestampTzMicrosDomain(
                "month",
                LocalDate.of(2024, 4, 1),
                LocalDate.of(2024, 5, 1),
                columnType,
                timestampTzColumnSymbol,
                columnHandle);

        assertDateTruncTimestampTzMicrosDomain(
                "quarter",
                LocalDate.of(2024, 4, 1),
                LocalDate.of(2024, 7, 1),
                columnType,
                timestampTzColumnSymbol,
                columnHandle);

        assertDateTruncTimestampTzMicrosDomain(
                "year",
                LocalDate.of(2024, 1, 1),
                LocalDate.of(2025, 1, 1),
                columnType,
                timestampTzColumnSymbol,
                columnHandle);
    }

    private static void assertDateTruncTimestampTzMicrosDomain(
            String unit,
            LocalDate periodStartDate,
            LocalDate nextPeriodStartDate,
            TimestampWithTimeZoneType columnType,
            String timestampTzColumnSymbol,
            ColumnHandle columnHandle)
    {
        ConnectorExpression truncate = new Call(
                columnType,
                new FunctionName("date_trunc"),
                ImmutableList.of(
                        new Constant(utf8Slice(unit), createVarcharType(17)),
                        new Variable(timestampTzColumnSymbol, columnType)));

        long periodStartMillis = periodStartDate.atStartOfDay().toEpochSecond(UTC) * MILLISECONDS_PER_SECOND;
        long nextPeriodStartMillis = nextPeriodStartDate.atStartOfDay().toEpochSecond(UTC) * MILLISECONDS_PER_SECOND;
        LongTimestampWithTimeZone periodStart = timestampTzMicrosFromEpochMillis(periodStartMillis);
        LongTimestampWithTimeZone nextPeriodStart = timestampTzMicrosFromEpochMillis(nextPeriodStartMillis);

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(
                                truncate,
                                new Constant(periodStart, columnType))),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, domain(Range.range(columnType, periodStart, true, nextPeriodStart, false)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(
                                truncate,
                                new Constant(LongTimestampWithTimeZone.fromEpochMillisAndFraction(periodStartMillis, PICOSECONDS_PER_MICROSECOND, UTC_KEY), columnType))),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.none());

        assertThat(extract(constraint(new Call(BOOLEAN, NOT_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(truncate, new Constant(periodStart, columnType))), Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, domain(Range.lessThan(columnType, periodStart), Range.greaterThanOrEqual(columnType, nextPeriodStart)))));
        assertThat(extract(constraint(new Call(BOOLEAN, LESS_THAN_OPERATOR_FUNCTION_NAME, ImmutableList.of(truncate, new Constant(periodStart, columnType))), Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, domain(Range.lessThan(columnType, periodStart)))));
        assertThat(extract(constraint(new Call(BOOLEAN, LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(truncate, new Constant(periodStart, columnType))), Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, domain(Range.lessThan(columnType, nextPeriodStart)))));
        assertThat(extract(constraint(new Call(BOOLEAN, GREATER_THAN_OPERATOR_FUNCTION_NAME, ImmutableList.of(truncate, new Constant(periodStart, columnType))), Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, domain(Range.greaterThanOrEqual(columnType, nextPeriodStart)))));
        assertThat(extract(constraint(new Call(BOOLEAN, GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(truncate, new Constant(periodStart, columnType))), Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, domain(Range.greaterThanOrEqual(columnType, periodStart)))));
        assertThat(extract(constraint(new Call(BOOLEAN, IDENTICAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(truncate, new Constant(periodStart, columnType))), Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(columnHandle, Domain.create(ValueSet.ofRanges(Range.range(columnType, periodStart, true, nextPeriodStart, false)), false))));
    }

    /// Test equivalent of `io.trino.sql.planner.ComparisonPreimages` for [TimestampWithTimeZoneType].
    /// `ComparisonPreimages` handles [DateType] and [TimestampType], but cannot handle
    /// [TimestampWithTimeZoneType]. Such unwrap would not be monotonic. If we know
    /// that [TimestampWithTimeZoneType] is always in UTC zone (point in time, with no time zone information),
    /// we can unwrap.
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
                        new Call(BOOLEAN, IDENTICAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(extractYear, yearExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(
                        columnHandle, Domain.create(ValueSet.ofRanges(Range.range(columnType, startOfYearUtc, true, startOfNextDateUtc, false)), false))));
    }

    /// Test equivalent of `io.trino.sql.planner.ComparisonPreimages` for [TimestampWithTimeZoneType].
    /// `ComparisonPreimages` handles [DateType] and [TimestampType], but cannot handle
    /// [TimestampWithTimeZoneType]. Such unwrap would not be monotonic. If we know
    /// that [TimestampWithTimeZoneType] is always in UTC zone (point in time, with no time zone information),
    /// we can unwrap.
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
                        new Call(BOOLEAN, IDENTICAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(extractYear, yearExpression)),
                        Map.of(timestampTzColumnSymbol, columnHandle))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(
                        columnHandle, Domain.create(ValueSet.ofRanges(Range.range(columnType, startOfYearUtc, true, startOfNextDateUtc, false)), false))));
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

    private static Constant timestampTzConstant(TimestampWithTimeZoneType type, long epochMillis)
    {
        if (type == TIMESTAMP_TZ_MILLIS) {
            return new Constant(timestampTzMillisFromEpochMillis(epochMillis), type);
        }
        return new Constant(timestampTzMicrosFromEpochMillis(epochMillis), type);
    }

    private static Domain domain(Range first, Range... rest)
    {
        return Domain.create(ValueSet.ofRanges(first, rest), false);
    }

    private static ConnectorExpression in(ConnectorExpression value, ConnectorExpression... items)
    {
        return new Call(
                BOOLEAN,
                IN_PREDICATE_FUNCTION_NAME,
                ImmutableList.of(value, new Call(new ArrayType(value.getType()), ARRAY_CONSTRUCTOR_FUNCTION_NAME, ImmutableList.copyOf(items))));
    }
}
