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
import io.trino.sql.planner.iterative.rule.UnwrapCastInComparison;
import io.trino.sql.planner.iterative.rule.UnwrapDateTruncInComparison;
import io.trino.sql.planner.iterative.rule.UnwrapYearInComparison;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.iceberg.ColumnIdentity.primitiveColumnIdentity;
import static io.trino.plugin.iceberg.ConstraintExtractor.extractTupleDomain;
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
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;

public class TestConstraintExtractor
{
    private static final AtomicInteger nextColumnId = new AtomicInteger(1);

    private static final IcebergColumnHandle A_BIGINT = newPrimitiveColumn(BIGINT);
    private static final IcebergColumnHandle A_TIMESTAMP_TZ = newPrimitiveColumn(TIMESTAMP_TZ_MICROS);

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
     * Test equivalent of {@link UnwrapCastInComparison} for {@link TimestampWithTimeZoneType}.
     * {@link UnwrapCastInComparison} handles {@link DateType} and {@link TimestampType}, but cannot handle
     * {@link TimestampWithTimeZoneType}. Such unwrap would not be monotonic. Within Iceberg, we know
     * that {@link TimestampWithTimeZoneType} is always in UTC zone (point in time, with no time zone information),
     * so we can unwrap.
     */
    @Test
    public void testExtractTimestampTzDateComparison()
    {
        String timestampTzColumnSymbol = "timestamp_tz_symbol";
        ConnectorExpression castOfColumn = new Call(DATE, CAST_FUNCTION_NAME, ImmutableList.of(new Variable(timestampTzColumnSymbol, TIMESTAMP_TZ_MICROS)));

        LocalDate someDate = LocalDate.of(2005, 9, 10);
        ConnectorExpression someDateExpression = new Constant(someDate.toEpochDay(), DATE);

        long startOfDateUtcEpochMillis = someDate.atStartOfDay().toEpochSecond(UTC) * MILLISECONDS_PER_SECOND;
        LongTimestampWithTimeZone startOfDateUtc = timestampTzFromEpochMillis(startOfDateUtcEpochMillis);
        LongTimestampWithTimeZone startOfNextDateUtc = timestampTzFromEpochMillis(startOfDateUtcEpochMillis + MILLISECONDS_PER_DAY);

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(castOfColumn, someDateExpression)),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, domain(Range.range(TIMESTAMP_TZ_MICROS, startOfDateUtc, true, startOfNextDateUtc, false)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, NOT_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(castOfColumn, someDateExpression)),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, domain(
                        Range.lessThan(TIMESTAMP_TZ_MICROS, startOfDateUtc),
                        Range.greaterThanOrEqual(TIMESTAMP_TZ_MICROS, startOfNextDateUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, LESS_THAN_OPERATOR_FUNCTION_NAME, ImmutableList.of(castOfColumn, someDateExpression)),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, domain(Range.lessThan(TIMESTAMP_TZ_MICROS, startOfDateUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(castOfColumn, someDateExpression)),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, domain(Range.lessThan(TIMESTAMP_TZ_MICROS, startOfNextDateUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, GREATER_THAN_OPERATOR_FUNCTION_NAME, ImmutableList.of(castOfColumn, someDateExpression)),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, domain(Range.greaterThanOrEqual(TIMESTAMP_TZ_MICROS, startOfNextDateUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(castOfColumn, someDateExpression)),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, domain(Range.greaterThanOrEqual(TIMESTAMP_TZ_MICROS, startOfDateUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME, ImmutableList.of(castOfColumn, someDateExpression)),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, Domain.create(
                        ValueSet.ofRanges(
                                Range.lessThan(TIMESTAMP_TZ_MICROS, startOfDateUtc),
                                Range.greaterThanOrEqual(TIMESTAMP_TZ_MICROS, startOfNextDateUtc)),
                        true))));
    }

    /**
     * Test equivalent of {@link UnwrapDateTruncInComparison} for {@link TimestampWithTimeZoneType}.
     * {@link UnwrapDateTruncInComparison} handles {@link DateType} and {@link TimestampType}, but cannot handle
     * {@link TimestampWithTimeZoneType}. Such unwrap would not be monotonic. Within Iceberg, we know
     * that {@link TimestampWithTimeZoneType} is always in UTC zone (point in time, with no time zone information),
     * so we can unwrap.
     */
    @Test
    public void testExtractDateTruncTimestampTzComparison()
    {
        String timestampTzColumnSymbol = "timestamp_tz_symbol";
        ConnectorExpression truncateToDay = new Call(
                TIMESTAMP_TZ_MICROS,
                new FunctionName("date_trunc"),
                ImmutableList.of(
                        new Constant(utf8Slice("day"), createVarcharType(17)),
                        new Variable(timestampTzColumnSymbol, TIMESTAMP_TZ_MICROS)));

        LocalDate someDate = LocalDate.of(2005, 9, 10);
        ConnectorExpression someMidnightExpression = new Constant(
                LongTimestampWithTimeZone.fromEpochMillisAndFraction(someDate.toEpochDay() * MILLISECONDS_PER_DAY, 0, UTC_KEY),
                TIMESTAMP_TZ_MICROS);
        ConnectorExpression someMiddayExpression = new Constant(
                LongTimestampWithTimeZone.fromEpochMillisAndFraction(someDate.toEpochDay() * MILLISECONDS_PER_DAY, PICOSECONDS_PER_MICROSECOND, UTC_KEY),
                TIMESTAMP_TZ_MICROS);

        long startOfDateUtcEpochMillis = someDate.atStartOfDay().toEpochSecond(UTC) * MILLISECONDS_PER_SECOND;
        LongTimestampWithTimeZone startOfDateUtc = timestampTzFromEpochMillis(startOfDateUtcEpochMillis);
        LongTimestampWithTimeZone startOfNextDateUtc = timestampTzFromEpochMillis(startOfDateUtcEpochMillis + MILLISECONDS_PER_DAY);

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(truncateToDay, someMidnightExpression)),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, domain(Range.range(TIMESTAMP_TZ_MICROS, startOfDateUtc, true, startOfNextDateUtc, false)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(truncateToDay, someMiddayExpression)),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.none());

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, NOT_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(truncateToDay, someMidnightExpression)),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, domain(
                        Range.lessThan(TIMESTAMP_TZ_MICROS, startOfDateUtc),
                        Range.greaterThanOrEqual(TIMESTAMP_TZ_MICROS, startOfNextDateUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, LESS_THAN_OPERATOR_FUNCTION_NAME, ImmutableList.of(truncateToDay, someMidnightExpression)),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, domain(Range.lessThan(TIMESTAMP_TZ_MICROS, startOfDateUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(truncateToDay, someMidnightExpression)),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, domain(Range.lessThan(TIMESTAMP_TZ_MICROS, startOfNextDateUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, GREATER_THAN_OPERATOR_FUNCTION_NAME, ImmutableList.of(truncateToDay, someMidnightExpression)),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, domain(Range.greaterThanOrEqual(TIMESTAMP_TZ_MICROS, startOfNextDateUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(truncateToDay, someMidnightExpression)),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, domain(Range.greaterThanOrEqual(TIMESTAMP_TZ_MICROS, startOfDateUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME, ImmutableList.of(truncateToDay, someMidnightExpression)),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, Domain.create(
                        ValueSet.ofRanges(
                                Range.lessThan(TIMESTAMP_TZ_MICROS, startOfDateUtc),
                                Range.greaterThanOrEqual(TIMESTAMP_TZ_MICROS, startOfNextDateUtc)),
                        true))));
    }

    /**
     * Test equivalent of {@link UnwrapYearInComparison} for {@link TimestampWithTimeZoneType}.
     * {@link UnwrapYearInComparison} handles {@link DateType} and {@link TimestampType}, but cannot handle
     * {@link TimestampWithTimeZoneType}. Such unwrap would not be monotonic. Within Iceberg, we know
     * that {@link TimestampWithTimeZoneType} is always in UTC zone (point in time, with no time zone information),
     * so we can unwrap.
     */
    @Test
    public void testExtractYearTimestampTzComparison()
    {
        String timestampTzColumnSymbol = "timestamp_tz_symbol";
        ConnectorExpression extractYear = new Call(
                BIGINT,
                new FunctionName("year"),
                ImmutableList.of(new Variable(timestampTzColumnSymbol, TIMESTAMP_TZ_MICROS)));

        LocalDate someDate = LocalDate.of(2005, 9, 10);
        ConnectorExpression yearExpression = new Constant(2005L, BIGINT);

        long startOfYearUtcEpochMillis = someDate.withDayOfYear(1).atStartOfDay().toEpochSecond(UTC) * MILLISECONDS_PER_SECOND;
        LongTimestampWithTimeZone startOfYearUtc = timestampTzFromEpochMillis(startOfYearUtcEpochMillis);
        LongTimestampWithTimeZone startOfNextDateUtc = timestampTzFromEpochMillis(someDate.plusYears(1).withDayOfYear(1).atStartOfDay().toEpochSecond(UTC) * MILLISECONDS_PER_SECOND);

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(extractYear, yearExpression)),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, domain(Range.range(TIMESTAMP_TZ_MICROS, startOfYearUtc, true, startOfNextDateUtc, false)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, NOT_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(extractYear, yearExpression)),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, domain(
                        Range.lessThan(TIMESTAMP_TZ_MICROS, startOfYearUtc),
                        Range.greaterThanOrEqual(TIMESTAMP_TZ_MICROS, startOfNextDateUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, LESS_THAN_OPERATOR_FUNCTION_NAME, ImmutableList.of(extractYear, yearExpression)),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, domain(Range.lessThan(TIMESTAMP_TZ_MICROS, startOfYearUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(extractYear, yearExpression)),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, domain(Range.lessThan(TIMESTAMP_TZ_MICROS, startOfNextDateUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, GREATER_THAN_OPERATOR_FUNCTION_NAME, ImmutableList.of(extractYear, yearExpression)),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, domain(Range.greaterThanOrEqual(TIMESTAMP_TZ_MICROS, startOfNextDateUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(extractYear, yearExpression)),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, domain(Range.greaterThanOrEqual(TIMESTAMP_TZ_MICROS, startOfYearUtc)))));

        assertThat(extract(
                constraint(
                        new Call(BOOLEAN, IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME, ImmutableList.of(extractYear, yearExpression)),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, Domain.create(
                        ValueSet.ofRanges(
                                Range.lessThan(TIMESTAMP_TZ_MICROS, startOfYearUtc),
                                Range.greaterThanOrEqual(TIMESTAMP_TZ_MICROS, startOfNextDateUtc)),
                        true))));
    }

    @Test
    public void testIntersectSummaryAndExpressionExtraction()
    {
        String timestampTzColumnSymbol = "timestamp_tz_symbol";
        ConnectorExpression castOfColumn = new Call(DATE, CAST_FUNCTION_NAME, ImmutableList.of(new Variable(timestampTzColumnSymbol, TIMESTAMP_TZ_MICROS)));

        LocalDate someDate = LocalDate.of(2005, 9, 10);
        ConnectorExpression someDateExpression = new Constant(someDate.toEpochDay(), DATE);

        long startOfDateUtcEpochMillis = someDate.atStartOfDay().toEpochSecond(UTC) * MILLISECONDS_PER_SECOND;
        LongTimestampWithTimeZone startOfDateUtc = timestampTzFromEpochMillis(startOfDateUtcEpochMillis);
        LongTimestampWithTimeZone startOfNextDateUtc = timestampTzFromEpochMillis(startOfDateUtcEpochMillis + MILLISECONDS_PER_DAY);
        LongTimestampWithTimeZone startOfNextNextDateUtc = timestampTzFromEpochMillis(startOfDateUtcEpochMillis + MILLISECONDS_PER_DAY * 2);

        assertThat(extract(
                constraint(
                        TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, domain(Range.lessThan(TIMESTAMP_TZ_MICROS, startOfNextNextDateUtc)))),
                        new Call(BOOLEAN, NOT_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(castOfColumn, someDateExpression)),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(
                        A_TIMESTAMP_TZ, domain(
                                Range.lessThan(TIMESTAMP_TZ_MICROS, startOfDateUtc),
                                Range.range(TIMESTAMP_TZ_MICROS, startOfNextDateUtc, true, startOfNextNextDateUtc, false)))));

        assertThat(extract(
                constraint(
                        TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, domain(Range.lessThan(TIMESTAMP_TZ_MICROS, startOfNextDateUtc)))),
                        new Call(BOOLEAN, GREATER_THAN_OPERATOR_FUNCTION_NAME, ImmutableList.of(castOfColumn, someDateExpression)),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.none());

        assertThat(extract(
                constraint(
                        TupleDomain.withColumnDomains(Map.of(A_BIGINT, Domain.singleValue(BIGINT, 1L))),
                        new Call(BOOLEAN, GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(castOfColumn, someDateExpression)),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(
                        A_BIGINT, Domain.singleValue(BIGINT, 1L),
                        A_TIMESTAMP_TZ, domain(Range.greaterThanOrEqual(TIMESTAMP_TZ_MICROS, startOfDateUtc)))));
    }

    private static IcebergColumnHandle newPrimitiveColumn(Type type)
    {
        int id = nextColumnId.getAndIncrement();
        return new IcebergColumnHandle(
                primitiveColumnIdentity(id, "column_" + id),
                type,
                ImmutableList.of(),
                type,
                Optional.empty());
    }

    private static TupleDomain<IcebergColumnHandle> extract(Constraint constraint)
    {
        ConstraintExtractor.ExtractionResult result = extractTupleDomain(constraint);
        assertThat(result.remainingExpression())
                .isEqualTo(Constant.TRUE);
        return result.tupleDomain();
    }

    private static Constraint constraint(ConnectorExpression expression, Map<String, IcebergColumnHandle> assignments)
    {
        return constraint(TupleDomain.all(), expression, assignments);
    }

    private static Constraint constraint(TupleDomain<ColumnHandle> summary, ConnectorExpression expression, Map<String, IcebergColumnHandle> assignments)
    {
        return new Constraint(summary, expression, ImmutableMap.copyOf(assignments));
    }

    private static LongTimestampWithTimeZone timestampTzFromEpochMillis(long epochMillis)
    {
        return LongTimestampWithTimeZone.fromEpochMillisAndFraction(epochMillis, 0, UTC_KEY);
    }

    private static Domain domain(Range first, Range... rest)
    {
        return Domain.create(ValueSet.ofRanges(first, rest), false);
    }
}
