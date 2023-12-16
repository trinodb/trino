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

import com.google.common.collect.ImmutableMap;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.DateType;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.sql.planner.ConnectorExpressionTranslator;
import io.trino.sql.planner.LiteralEncoder;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.iterative.rule.UnwrapCastInComparison;
import io.trino.sql.planner.iterative.rule.UnwrapDateTruncInComparison;
import io.trino.sql.planner.iterative.rule.UnwrapYearInComparison;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.SymbolReference;
import io.trino.transaction.NoOpTransactionManager;
import io.trino.transaction.TransactionId;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.plugin.kafka.ConstraintExtractor.extractTupleDomain;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.planner.TypeAnalyzer.createTestingTypeAnalyzer;
import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.IS_DISTINCT_FROM;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.NOT_EQUAL;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;

public class TestConstraintExtractor
{
    private static final LiteralEncoder LITERAL_ENCODER = new LiteralEncoder(PLANNER_CONTEXT);

    private static final AtomicInteger nextColumnId = new AtomicInteger(1);

    private static final KafkaColumnHandle A_BIGINT = newPrimitiveColumn(BIGINT);
    private static final KafkaColumnHandle A_TIMESTAMP_TZ = newPrimitiveColumn(TIMESTAMP_TZ_MICROS);
    private static final KafkaColumnHandle A_TIMESTAMP_MILL = newPrimitiveColumn(TIMESTAMP_MILLIS);

    /**
     * Tests the extraction of a summary from a given constraint.
     * This test checks that the extraction process correctly identifies and isolates the tuple domain from a constraint,
     * which represents a set of conditions applied on the query.
     *
     * The test involves creating a constraint with a predefined tuple domain and ensuring
     * that the extracted tuple domain matches the one defined in the constraint.
     *
     * @implNote This test assumes that the constraint's tuple domain is simple and consists of a single domain for a BIGINT column.
     *           The test validates that the extraction process accurately retrieves this domain without invoking any additional value filtering logic.
     */
    @Test
    public void testExtractSummary()
    {
        // Creating a constraint with a single-value domain for a BIGINT column
        assertThat(extract(
                new Constraint(
                        // Tuple domain with a single value for the BIGINT column
                        TupleDomain.withColumnDomains(Map.of(A_BIGINT, Domain.singleValue(BIGINT, 1L))),
                        // The constant expression indicating no additional filtering
                        Constant.TRUE,
                        // Empty map for column assignments
                        Map.of(),
                        // A function that should not be called during the test
                        values -> {
                            throw new AssertionError("should not be called");
                        },
                        // The set of columns involved in the constraint
                        Set.of(A_BIGINT))))
                // Verifying that the extracted tuple domain matches the predefined domain
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_BIGINT, Domain.singleValue(BIGINT, 1L))));
    }

    /**
     * Test equivalent of {@link UnwrapCastInComparison} for {@link TimestampWithTimeZoneType}.
     * {@link UnwrapCastInComparison} handles {@link DateType} and {@link TimestampType}, but cannot handle
     * {@link TimestampWithTimeZoneType}. Such unwrap would not be monotonic. Within Connector, we know
     * that {@link TimestampWithTimeZoneType} is always in UTC zone (point in time, with no time zone information),
     * so we can unwrap.
     */
    @Test
    public void testExtractTimestampTzDateComparison()
    {
        String timestampTzColumnSymbol = "timestamp_tz_symbol";
        Cast castOfColumn = new Cast(new SymbolReference(timestampTzColumnSymbol), toSqlType(DATE));

        LocalDate someDate = LocalDate.of(2005, 9, 10);
        Expression someDateExpression = LITERAL_ENCODER.toExpression(someDate.toEpochDay(), DATE);

        long startOfDateUtcEpochMillis = someDate.atStartOfDay().toEpochSecond(UTC) * MILLISECONDS_PER_SECOND;
        LongTimestampWithTimeZone startOfDateUtc = timestampTzFromEpochMillis(startOfDateUtcEpochMillis);
        LongTimestampWithTimeZone startOfNextDateUtc = timestampTzFromEpochMillis(startOfDateUtcEpochMillis + MILLISECONDS_PER_DAY);

        assertThat(extract(
                constraint(
                        new ComparisonExpression(EQUAL, castOfColumn, someDateExpression),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, domain(Range.range(TIMESTAMP_TZ_MICROS, startOfDateUtc, true, startOfNextDateUtc, false)))));

        assertThat(extract(
                constraint(
                        new ComparisonExpression(NOT_EQUAL, castOfColumn, someDateExpression),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, domain(
                        Range.lessThan(TIMESTAMP_TZ_MICROS, startOfDateUtc),
                        Range.greaterThanOrEqual(TIMESTAMP_TZ_MICROS, startOfNextDateUtc)))));

        assertThat(extract(
                constraint(
                        new ComparisonExpression(LESS_THAN, castOfColumn, someDateExpression),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, domain(Range.lessThan(TIMESTAMP_TZ_MICROS, startOfDateUtc)))));

        assertThat(extract(
                constraint(
                        new ComparisonExpression(LESS_THAN_OR_EQUAL, castOfColumn, someDateExpression),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, domain(Range.lessThan(TIMESTAMP_TZ_MICROS, startOfNextDateUtc)))));

        assertThat(extract(
                constraint(
                        new ComparisonExpression(GREATER_THAN, castOfColumn, someDateExpression),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, domain(Range.greaterThanOrEqual(TIMESTAMP_TZ_MICROS, startOfNextDateUtc)))));

        assertThat(extract(
                constraint(
                        new ComparisonExpression(GREATER_THAN_OR_EQUAL, castOfColumn, someDateExpression),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, domain(Range.greaterThanOrEqual(TIMESTAMP_TZ_MICROS, startOfDateUtc)))));

        assertThat(extract(
                constraint(
                        new ComparisonExpression(IS_DISTINCT_FROM, castOfColumn, someDateExpression),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, Domain.create(
                        ValueSet.ofRanges(
                                Range.lessThan(TIMESTAMP_TZ_MICROS, startOfDateUtc),
                                Range.greaterThanOrEqual(TIMESTAMP_TZ_MICROS, startOfNextDateUtc)),
                        true))));
    }

    /**
     * Tests the extraction process for comparisons involving a timestamp and a date.
     * This test focuses on verifying the correct handling of scenarios where a timestamp (represented in milliseconds) is compared against a specific date.
     *
     * Different comparison expressions like equal, not equal, less than, etc., are evaluated
     * to ensure accurate processing of timestamp to date comparisons.
     *
     * @implNote The timestamp is represented in milliseconds, and comparisons are made against a date cast from the timestamp.
     */
    @Test
    public void testExtractTimestampDateComparison()
    {
        // Symbol representing the timestamp column in the query
        String timestampColumnSymbol = "timestamp_symbol";
        // Casting the timestamp column to a date type
        Cast castOfColumn = new Cast(new SymbolReference(timestampColumnSymbol), toSqlType(DATE));

        // Specific date for comparison
        LocalDate someDate = LocalDate.of(2005, 9, 10);
        // Expression representing the specific date
        Expression someDateExpression = LITERAL_ENCODER.toExpression(someDate.toEpochDay(), DATE);

        // Calculating the start of the date and the start of the next day in microseconds
        long startOfDateUtcEpochMillis = someDate.atStartOfDay().toEpochSecond(UTC) * MICROSECONDS_PER_SECOND;
        Long startOfDate = startOfDateUtcEpochMillis;
        Long startOfNextDate = (startOfDateUtcEpochMillis + MICROSECONDS_PER_DAY);

        // Testing EQUAL comparison
        assertThat(extract(
                constraint(
                        new ComparisonExpression(EQUAL, castOfColumn, someDateExpression),
                        Map.of(timestampColumnSymbol, A_TIMESTAMP_MILL))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_MILL, domain(Range.range(TIMESTAMP_MILLIS, startOfDate, true, startOfNextDate, false)))));

        assertThat(extract(
                constraint(
                        new ComparisonExpression(NOT_EQUAL, castOfColumn, someDateExpression),
                        Map.of(timestampColumnSymbol, A_TIMESTAMP_MILL))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_MILL, domain(
                        Range.lessThan(TIMESTAMP_MILLIS, startOfDate),
                        Range.greaterThanOrEqual(TIMESTAMP_MILLIS, startOfNextDate)))));

        assertThat(extract(
                constraint(
                        new ComparisonExpression(LESS_THAN, castOfColumn, someDateExpression),
                        Map.of(timestampColumnSymbol, A_TIMESTAMP_MILL))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_MILL, domain(Range.lessThan(TIMESTAMP_MILLIS, startOfDate)))));

        assertThat(extract(
                constraint(
                        new ComparisonExpression(LESS_THAN_OR_EQUAL, castOfColumn, someDateExpression),
                        Map.of(timestampColumnSymbol, A_TIMESTAMP_MILL))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_MILL, domain(Range.lessThan(TIMESTAMP_MILLIS, startOfNextDate)))));

        assertThat(extract(
                constraint(
                        new ComparisonExpression(GREATER_THAN, castOfColumn, someDateExpression),
                        Map.of(timestampColumnSymbol, A_TIMESTAMP_MILL))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_MILL, domain(Range.greaterThanOrEqual(TIMESTAMP_MILLIS, startOfNextDate)))));

        assertThat(extract(
                constraint(
                        new ComparisonExpression(GREATER_THAN_OR_EQUAL, castOfColumn, someDateExpression),
                        Map.of(timestampColumnSymbol, A_TIMESTAMP_MILL))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_MILL, domain(Range.greaterThanOrEqual(TIMESTAMP_MILLIS, startOfDate)))));

        assertThat(extract(
                constraint(
                        new ComparisonExpression(IS_DISTINCT_FROM, castOfColumn, someDateExpression),
                        Map.of(timestampColumnSymbol, A_TIMESTAMP_MILL))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_MILL, Domain.create(
                        ValueSet.ofRanges(
                                Range.lessThan(TIMESTAMP_MILLIS, startOfDate),
                                Range.greaterThanOrEqual(TIMESTAMP_MILLIS, startOfNextDate)),
                        true))));
    }

    /**
     * Test equivalent of {@link UnwrapDateTruncInComparison} for {@link TimestampWithTimeZoneType}.
     * {@link UnwrapDateTruncInComparison} handles {@link DateType} and {@link TimestampType}, but cannot handle
     * {@link TimestampWithTimeZoneType}. Such unwrap would not be monotonic. Within Connector, we know
     * that {@link TimestampWithTimeZoneType} is always in UTC zone (point in time, with no time zone information),
     * so we can unwrap.
     */
    @Test
    public void testExtractDateTruncTimestampTzComparison()
    {
        String timestampTzColumnSymbol = "timestamp_tz_symbol";
        FunctionCall truncateToDay = new FunctionCall(
                PLANNER_CONTEXT.getMetadata().resolveBuiltinFunction("date_trunc", fromTypes(VARCHAR, TIMESTAMP_TZ_MICROS)).toQualifiedName(),
                List.of(
                        LITERAL_ENCODER.toExpression(utf8Slice("day"), createVarcharType(17)),
                        new SymbolReference(timestampTzColumnSymbol)));

        LocalDate someDate = LocalDate.of(2005, 9, 10);
        Expression someMidnightExpression = LITERAL_ENCODER.toExpression(
                LongTimestampWithTimeZone.fromEpochMillisAndFraction(someDate.toEpochDay() * MILLISECONDS_PER_DAY, 0, UTC_KEY),
                TIMESTAMP_TZ_MICROS);
        Expression someMiddayExpression = LITERAL_ENCODER.toExpression(
                LongTimestampWithTimeZone.fromEpochMillisAndFraction(someDate.toEpochDay() * MILLISECONDS_PER_DAY, PICOSECONDS_PER_MICROSECOND, UTC_KEY),
                TIMESTAMP_TZ_MICROS);

        long startOfDateUtcEpochMillis = someDate.atStartOfDay().toEpochSecond(UTC) * MILLISECONDS_PER_SECOND;
        LongTimestampWithTimeZone startOfDateUtc = timestampTzFromEpochMillis(startOfDateUtcEpochMillis);
        LongTimestampWithTimeZone startOfNextDateUtc = timestampTzFromEpochMillis(startOfDateUtcEpochMillis + MILLISECONDS_PER_DAY);

        assertThat(extract(
                constraint(
                        new ComparisonExpression(EQUAL, truncateToDay, someMidnightExpression),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, domain(Range.range(TIMESTAMP_TZ_MICROS, startOfDateUtc, true, startOfNextDateUtc, false)))));

        assertThat(extract(
                constraint(
                        new ComparisonExpression(EQUAL, truncateToDay, someMiddayExpression),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.none());

        assertThat(extract(
                constraint(
                        new ComparisonExpression(NOT_EQUAL, truncateToDay, someMidnightExpression),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, domain(
                        Range.lessThan(TIMESTAMP_TZ_MICROS, startOfDateUtc),
                        Range.greaterThanOrEqual(TIMESTAMP_TZ_MICROS, startOfNextDateUtc)))));

        assertThat(extract(
                constraint(
                        new ComparisonExpression(LESS_THAN, truncateToDay, someMidnightExpression),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, domain(Range.lessThan(TIMESTAMP_TZ_MICROS, startOfDateUtc)))));

        assertThat(extract(
                constraint(
                        new ComparisonExpression(LESS_THAN_OR_EQUAL, truncateToDay, someMidnightExpression),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, domain(Range.lessThan(TIMESTAMP_TZ_MICROS, startOfNextDateUtc)))));

        assertThat(extract(
                constraint(
                        new ComparisonExpression(GREATER_THAN, truncateToDay, someMidnightExpression),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, domain(Range.greaterThanOrEqual(TIMESTAMP_TZ_MICROS, startOfNextDateUtc)))));

        assertThat(extract(
                constraint(
                        new ComparisonExpression(GREATER_THAN_OR_EQUAL, truncateToDay, someMidnightExpression),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, domain(Range.greaterThanOrEqual(TIMESTAMP_TZ_MICROS, startOfDateUtc)))));

        assertThat(extract(
                constraint(
                        new ComparisonExpression(IS_DISTINCT_FROM, truncateToDay, someMidnightExpression),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, Domain.create(
                        ValueSet.ofRanges(
                                Range.lessThan(TIMESTAMP_TZ_MICROS, startOfDateUtc),
                                Range.greaterThanOrEqual(TIMESTAMP_TZ_MICROS, startOfNextDateUtc)),
                        true))));
    }

    /**
     * Tests the extraction process for comparisons involving date truncation on a timestamp.
     * This test verifies the handling of scenarios where a timestamp (represented in milliseconds) is truncated to a specific date unit (e.g., day)
     * and then compared against a specific timestamp value.
     *
     * Different comparison expressions like equal, not equal, less than, etc., are evaluated
     * to ensure correct processing of date truncation logic in timestamp comparisons.
     *
     * @implNote The timestamp is represented in milliseconds, and the date truncation is simulated using the 'date_trunc' function.
     */
    @Test
    public void testExtractDateTruncTimestampComparison()
    {
        // Symbol representing the timestamp column in the query
        String timestampColumnSymbol = "timestamp_symbol";
        // Truncating the timestamp column to the day
        FunctionCall truncateToDay = new FunctionCall(
                PLANNER_CONTEXT.getMetadata().resolveBuiltinFunction("date_trunc", fromTypes(VARCHAR, TIMESTAMP_MILLIS)).toQualifiedName(),
                List.of(
                        LITERAL_ENCODER.toExpression(utf8Slice("day"), createVarcharType(17)),
                        new SymbolReference(timestampColumnSymbol)));

        // Specific date for comparison
        LocalDate someDate = LocalDate.of(2005, 9, 10);
        // Expressions representing midnight and midday timestamps for the specific date
        Expression someMidnightExpression = LITERAL_ENCODER.toExpression(someDate.toEpochDay() * MICROSECONDS_PER_DAY, TIMESTAMP_MILLIS);
        Expression someMiddayExpression = LITERAL_ENCODER.toExpression(someDate.toEpochDay() * MICROSECONDS_PER_DAY + PICOSECONDS_PER_MICROSECOND, TIMESTAMP_MILLIS);

        // Calculating start of the day and the start of the next day in microseconds
        long startOfDateUtcEpochMillis = someDate.atStartOfDay().toEpochSecond(UTC) * MICROSECONDS_PER_SECOND;
        Long startOfDateUtc = startOfDateUtcEpochMillis;
        Long startOfNextDateUtc = (startOfDateUtcEpochMillis + MICROSECONDS_PER_DAY);

        // Testing EQUAL comparison with exact midnight
        assertThat(extract(
                constraint(
                        new ComparisonExpression(EQUAL, truncateToDay, someMidnightExpression),
                        Map.of(timestampColumnSymbol, A_TIMESTAMP_MILL))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_MILL, domain(Range.range(TIMESTAMP_MILLIS, startOfDateUtc, true, startOfNextDateUtc, false)))));

        // Testing EQUAL comparison with midday, expecting no matches
        assertThat(extract(
                constraint(
                        new ComparisonExpression(EQUAL, truncateToDay, someMiddayExpression),
                        Map.of(timestampColumnSymbol, A_TIMESTAMP_MILL))))
                .isEqualTo(TupleDomain.none());

        // Other comparison scenarios...
        assertThat(extract(
                constraint(
                        new ComparisonExpression(NOT_EQUAL, truncateToDay, someMidnightExpression),
                        Map.of(timestampColumnSymbol, A_TIMESTAMP_MILL))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_MILL, domain(
                        Range.lessThan(TIMESTAMP_MILLIS, startOfDateUtc),
                        Range.greaterThanOrEqual(TIMESTAMP_MILLIS, startOfNextDateUtc)))));

        assertThat(extract(
                constraint(
                        new ComparisonExpression(LESS_THAN, truncateToDay, someMidnightExpression),
                        Map.of(timestampColumnSymbol, A_TIMESTAMP_MILL))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_MILL, domain(Range.lessThan(TIMESTAMP_MILLIS, startOfDateUtc)))));

        assertThat(extract(
                constraint(
                        new ComparisonExpression(LESS_THAN_OR_EQUAL, truncateToDay, someMidnightExpression),
                        Map.of(timestampColumnSymbol, A_TIMESTAMP_MILL))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_MILL, domain(Range.lessThan(TIMESTAMP_MILLIS, startOfNextDateUtc)))));

        assertThat(extract(
                constraint(
                        new ComparisonExpression(GREATER_THAN, truncateToDay, someMidnightExpression),
                        Map.of(timestampColumnSymbol, A_TIMESTAMP_MILL))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_MILL, domain(Range.greaterThanOrEqual(TIMESTAMP_MILLIS, startOfNextDateUtc)))));

        assertThat(extract(
                constraint(
                        new ComparisonExpression(GREATER_THAN_OR_EQUAL, truncateToDay, someMidnightExpression),
                        Map.of(timestampColumnSymbol, A_TIMESTAMP_MILL))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_MILL, domain(Range.greaterThanOrEqual(TIMESTAMP_MILLIS, startOfDateUtc)))));

        assertThat(extract(
                constraint(
                        new ComparisonExpression(IS_DISTINCT_FROM, truncateToDay, someMidnightExpression),
                        Map.of(timestampColumnSymbol, A_TIMESTAMP_MILL))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_MILL, Domain.create(
                        ValueSet.ofRanges(
                                Range.lessThan(TIMESTAMP_MILLIS, startOfDateUtc),
                                Range.greaterThanOrEqual(TIMESTAMP_MILLIS, startOfNextDateUtc)),
                        true))));
    }

    /**
     * Test equivalent of {@link UnwrapYearInComparison} for {@link TimestampWithTimeZoneType}.
     * {@link UnwrapYearInComparison} handles {@link DateType} and {@link TimestampType}, but cannot handle
     * {@link TimestampWithTimeZoneType}. Such unwrap would not be monotonic. Within Connector, we know
     * that {@link TimestampWithTimeZoneType} is always in UTC zone (point in time, with no time zone information),
     * so we can unwrap.
     */
    @Test
    public void testExtractYearTimestampTzComparison()
    {
        String timestampTzColumnSymbol = "timestamp_tz_symbol";
        FunctionCall extractYear = new FunctionCall(
                PLANNER_CONTEXT.getMetadata().resolveBuiltinFunction("year", fromTypes(TIMESTAMP_TZ_MICROS)).toQualifiedName(),
                List.of(new SymbolReference(timestampTzColumnSymbol)));

        LocalDate someDate = LocalDate.of(2005, 9, 10);
        Expression yearExpression = LITERAL_ENCODER.toExpression(2005L, BIGINT);

        long startOfYearUtcEpochMillis = someDate.withDayOfYear(1).atStartOfDay().toEpochSecond(UTC) * MILLISECONDS_PER_SECOND;
        LongTimestampWithTimeZone startOfYearUtc = timestampTzFromEpochMillis(startOfYearUtcEpochMillis);
        LongTimestampWithTimeZone startOfNextDateUtc = timestampTzFromEpochMillis(someDate.plusYears(1).withDayOfYear(1).atStartOfDay().toEpochSecond(UTC) * MILLISECONDS_PER_SECOND);

        assertThat(extract(
                constraint(
                        new ComparisonExpression(EQUAL, extractYear, yearExpression),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, domain(Range.range(TIMESTAMP_TZ_MICROS, startOfYearUtc, true, startOfNextDateUtc, false)))));

        assertThat(extract(
                constraint(
                        new ComparisonExpression(NOT_EQUAL, extractYear, yearExpression),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, domain(
                        Range.lessThan(TIMESTAMP_TZ_MICROS, startOfYearUtc),
                        Range.greaterThanOrEqual(TIMESTAMP_TZ_MICROS, startOfNextDateUtc)))));

        assertThat(extract(
                constraint(
                        new ComparisonExpression(LESS_THAN, extractYear, yearExpression),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, domain(Range.lessThan(TIMESTAMP_TZ_MICROS, startOfYearUtc)))));

        assertThat(extract(
                constraint(
                        new ComparisonExpression(LESS_THAN_OR_EQUAL, extractYear, yearExpression),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, domain(Range.lessThan(TIMESTAMP_TZ_MICROS, startOfNextDateUtc)))));

        assertThat(extract(
                constraint(
                        new ComparisonExpression(GREATER_THAN, extractYear, yearExpression),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, domain(Range.greaterThanOrEqual(TIMESTAMP_TZ_MICROS, startOfNextDateUtc)))));

        assertThat(extract(
                constraint(
                        new ComparisonExpression(GREATER_THAN_OR_EQUAL, extractYear, yearExpression),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, domain(Range.greaterThanOrEqual(TIMESTAMP_TZ_MICROS, startOfYearUtc)))));

        assertThat(extract(
                constraint(
                        new ComparisonExpression(IS_DISTINCT_FROM, extractYear, yearExpression),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, Domain.create(
                        ValueSet.ofRanges(
                                Range.lessThan(TIMESTAMP_TZ_MICROS, startOfYearUtc),
                                Range.greaterThanOrEqual(TIMESTAMP_TZ_MICROS, startOfNextDateUtc)),
                        true))));
    }

    /**
     * Tests the extraction process for comparisons involving the extraction of the year from a timestamp.
     * This test simulates the scenario where the year part of a timestamp (represented in milliseconds)
     * is extracted and compared against a constant year value.
     *
     * The test evaluates various comparison expressions, such as equal, not equal, less than, etc.,
     * to ensure that the extraction logic correctly interprets and processes these expressions.
     *
     * @implNote The timestamp is represented in milliseconds, and the year extraction is simulated using the 'year' function.
     */
    @Test
    public void testExtractYearTimestampComparison()
    {
        // Symbol representing the timestamp column in the query
        String timestampColumnSymbol = "timestamp_symbol";
        // Extracting the year part from the timestamp column
        FunctionCall extractYear = new FunctionCall(
                PLANNER_CONTEXT.getMetadata().resolveBuiltinFunction("year", fromTypes(TIMESTAMP_MILLIS)).toQualifiedName(),
                List.of(new SymbolReference(timestampColumnSymbol)));

        // A specific year for comparison
        LocalDate someDate = LocalDate.of(2005, 9, 10);
        // Expression representing the specific year
        Expression yearExpression = LITERAL_ENCODER.toExpression(2005L, BIGINT);

        // Calculating start of the year and the start of the next year in microseconds
        Long startOfYear = someDate.withDayOfYear(1).atStartOfDay().toEpochSecond(UTC) * MICROSECONDS_PER_SECOND;
        Long startOfNextYear = someDate.plusYears(1).withDayOfYear(1).atStartOfDay().toEpochSecond(UTC) * MICROSECONDS_PER_SECOND;

        // Testing EQUAL comparison
        assertThat(extract(
                constraint(
                        new ComparisonExpression(EQUAL, extractYear, yearExpression),
                        Map.of(timestampColumnSymbol, A_TIMESTAMP_MILL))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_MILL, domain(Range.range(TIMESTAMP_MILLIS, startOfYear, true, startOfNextYear, false)))));

        assertThat(extract(
                constraint(
                        new ComparisonExpression(NOT_EQUAL, extractYear, yearExpression),
                        Map.of(timestampColumnSymbol, A_TIMESTAMP_MILL))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_MILL, domain(
                        Range.lessThan(TIMESTAMP_MILLIS, startOfYear),
                        Range.greaterThanOrEqual(TIMESTAMP_MILLIS, startOfNextYear)))));

        assertThat(extract(
                constraint(
                        new ComparisonExpression(LESS_THAN, extractYear, yearExpression),
                        Map.of(timestampColumnSymbol, A_TIMESTAMP_MILL))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_MILL, domain(Range.lessThan(TIMESTAMP_MILLIS, startOfYear)))));

        assertThat(extract(
                constraint(
                        new ComparisonExpression(LESS_THAN_OR_EQUAL, extractYear, yearExpression),
                        Map.of(timestampColumnSymbol, A_TIMESTAMP_MILL))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_MILL, domain(Range.lessThan(TIMESTAMP_MILLIS, startOfNextYear)))));

        assertThat(extract(
                constraint(
                        new ComparisonExpression(GREATER_THAN, extractYear, yearExpression),
                        Map.of(timestampColumnSymbol, A_TIMESTAMP_MILL))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_MILL, domain(Range.greaterThanOrEqual(TIMESTAMP_MILLIS, startOfNextYear)))));

        assertThat(extract(
                constraint(
                        new ComparisonExpression(GREATER_THAN_OR_EQUAL, extractYear, yearExpression),
                        Map.of(timestampColumnSymbol, A_TIMESTAMP_MILL))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_MILL, domain(Range.greaterThanOrEqual(TIMESTAMP_MILLIS, startOfYear)))));

        assertThat(extract(
                constraint(
                        new ComparisonExpression(IS_DISTINCT_FROM, extractYear, yearExpression),
                        Map.of(timestampColumnSymbol, A_TIMESTAMP_MILL))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_MILL, Domain.create(
                        ValueSet.ofRanges(
                                Range.lessThan(TIMESTAMP_MILLIS, startOfYear),
                                Range.greaterThanOrEqual(TIMESTAMP_MILLIS, startOfNextYear)),
                        true))));
    }

    @Test
    public void testIntersectSummaryAndExpressionExtraction()
    {
        String timestampTzColumnSymbol = "timestamp_tz_symbol";
        Cast castOfColumn = new Cast(new SymbolReference(timestampTzColumnSymbol), toSqlType(DATE));

        LocalDate someDate = LocalDate.of(2005, 9, 10);
        Expression someDateExpression = LITERAL_ENCODER.toExpression(someDate.toEpochDay(), DATE);

        long startOfDateUtcEpochMillis = someDate.atStartOfDay().toEpochSecond(UTC) * MILLISECONDS_PER_SECOND;
        LongTimestampWithTimeZone startOfDateUtc = timestampTzFromEpochMillis(startOfDateUtcEpochMillis);
        LongTimestampWithTimeZone startOfNextDateUtc = timestampTzFromEpochMillis(startOfDateUtcEpochMillis + MILLISECONDS_PER_DAY);
        LongTimestampWithTimeZone startOfNextNextDateUtc = timestampTzFromEpochMillis(startOfDateUtcEpochMillis + MILLISECONDS_PER_DAY * 2);

        assertThat(extract(
                constraint(
                        TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, domain(Range.lessThan(TIMESTAMP_TZ_MICROS, startOfNextNextDateUtc)))),
                        new ComparisonExpression(NOT_EQUAL, castOfColumn, someDateExpression),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(
                        A_TIMESTAMP_TZ, domain(
                                Range.lessThan(TIMESTAMP_TZ_MICROS, startOfDateUtc),
                                Range.range(TIMESTAMP_TZ_MICROS, startOfNextDateUtc, true, startOfNextNextDateUtc, false)))));

        assertThat(extract(
                constraint(
                        TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_TZ, domain(Range.lessThan(TIMESTAMP_TZ_MICROS, startOfNextDateUtc)))),
                        new ComparisonExpression(GREATER_THAN, castOfColumn, someDateExpression),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.none());

        assertThat(extract(
                constraint(
                        TupleDomain.withColumnDomains(Map.of(A_BIGINT, Domain.singleValue(BIGINT, 1L))),
                        new ComparisonExpression(GREATER_THAN_OR_EQUAL, castOfColumn, someDateExpression),
                        Map.of(timestampTzColumnSymbol, A_TIMESTAMP_TZ))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(
                        A_BIGINT, Domain.singleValue(BIGINT, 1L),
                        A_TIMESTAMP_TZ, domain(Range.greaterThanOrEqual(TIMESTAMP_TZ_MICROS, startOfDateUtc)))));
    }

    /**
     * Tests the intersection of summary and expression for timestamp comparisons.
     * This method evaluates how the extraction process handles various comparison expressions
     * involving a timestamp column and a constant date.
     *
     * The test scenarios include:
     * - Comparing a timestamp column not equal to a specific date
     * - Comparing a timestamp column greater than a specific date
     * - Comparing a timestamp column greater than or equal to a specific date
     *
     * The method simulates different conditions using the TupleDomain summary and a ComparisonExpression.
     * It ensures that the TupleDomain extracted from these conditions correctly represents the expected result.
     *
     * @implNote This test uses a timestamp column represented in milliseconds.
     */
    @Test
    public void testIntersectSummaryAndExpressionTimestampExtraction()
    {
        // Symbol representing the timestamp column in the query
        String timestampColumnSymbol = "timestamp_symbol";
        // Casting the timestamp column to DATE type for comparison
        Cast castOfColumn = new Cast(new SymbolReference(timestampColumnSymbol), toSqlType(DATE));

        // A specific date for comparison
        LocalDate someDate = LocalDate.of(2005, 9, 10);
        // Expression representing the specific date
        Expression someDateExpression = LITERAL_ENCODER.toExpression(someDate.toEpochDay(), DATE);

        // Calculating start of the date, next date, and the date after in microseconds
        long startOfDateUtcEpochMillis = someDate.atStartOfDay().toEpochSecond(UTC) * MICROSECONDS_PER_SECOND;
        Long startOfDate = startOfDateUtcEpochMillis;
        Long startOfNextDate = startOfDateUtcEpochMillis + MICROSECONDS_PER_DAY;
        Long startOfNextNextDate = startOfDateUtcEpochMillis + MICROSECONDS_PER_DAY * 2;

        // Testing NOT_EQUAL comparison
        assertThat(extract(
                constraint(
                        TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_MILL, domain(Range.lessThan(TIMESTAMP_MILLIS, startOfNextNextDate)))),
                        new ComparisonExpression(NOT_EQUAL, castOfColumn, someDateExpression),
                        Map.of(timestampColumnSymbol, A_TIMESTAMP_MILL))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(
                        A_TIMESTAMP_MILL, domain(
                                Range.lessThan(TIMESTAMP_MILLIS, startOfDate),
                                Range.range(TIMESTAMP_MILLIS, startOfNextDate, true, startOfNextNextDate, false)))));

        // Testing GREATER_THAN comparison
        assertThat(extract(
                constraint(
                        TupleDomain.withColumnDomains(Map.of(A_TIMESTAMP_MILL, domain(Range.lessThan(TIMESTAMP_MILLIS, startOfNextDate)))),
                        new ComparisonExpression(GREATER_THAN, castOfColumn, someDateExpression),
                        Map.of(timestampColumnSymbol, A_TIMESTAMP_MILL))))
                .isEqualTo(TupleDomain.none());

        // Testing GREATER_THAN_OR_EQUAL comparison
        assertThat(extract(
                constraint(
                        TupleDomain.withColumnDomains(Map.of(A_BIGINT, Domain.singleValue(BIGINT, 1L))),
                        new ComparisonExpression(GREATER_THAN_OR_EQUAL, castOfColumn, someDateExpression),
                        Map.of(timestampColumnSymbol, A_TIMESTAMP_MILL))))
                .isEqualTo(TupleDomain.withColumnDomains(Map.of(
                        A_BIGINT, Domain.singleValue(BIGINT, 1L),
                        A_TIMESTAMP_MILL, domain(Range.greaterThanOrEqual(TIMESTAMP_MILLIS, startOfDate)))));
    }

    private static KafkaColumnHandle newPrimitiveColumn(Type type)
    {
        //
        int id = nextColumnId.getAndIncrement();
        return new KafkaColumnHandle(
                "column_" + id,
                type,
                null,
                null,
                null,
                false,
                false,
                true);
    }

    private static TupleDomain<KafkaColumnHandle> extract(Constraint constraint)
    {
        ConstraintExtractor.ExtractionResult result = extractTupleDomain(constraint, KafkaColumnHandle::getColumnTypeIfInternal);
        assertThat(result.remainingExpression())
                .isEqualTo(Constant.TRUE);
        return result.tupleDomain();
    }

    private static Constraint constraint(Expression expression, Map<String, KafkaColumnHandle> assignments)
    {
        return constraint(TupleDomain.all(), expression, assignments);
    }

    private static Constraint constraint(TupleDomain<ColumnHandle> summary, Expression expression, Map<String, KafkaColumnHandle> assignments)
    {
        Map<String, Type> symbolTypes = assignments.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().getType()));
        ConnectorExpression connectorExpression = connectorExpression(expression, symbolTypes);
        return new Constraint(summary, connectorExpression, ImmutableMap.copyOf(assignments));
    }

    private static ConnectorExpression connectorExpression(Expression expression, Map<String, Type> symbolTypes)
    {
        return ConnectorExpressionTranslator.translate(
                        TEST_SESSION.beginTransactionId(TransactionId.create(), new NoOpTransactionManager(), new AllowAllAccessControl()),
                        expression,
                        TypeProvider.viewOf(symbolTypes.entrySet().stream()
                                .collect(toImmutableMap(entry -> new Symbol(entry.getKey()), Map.Entry::getValue))),
                        PLANNER_CONTEXT,
                        createTestingTypeAnalyzer(PLANNER_CONTEXT))
                .orElseThrow(() -> new RuntimeException("Translation to ConnectorExpression failed for: " + expression));
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
