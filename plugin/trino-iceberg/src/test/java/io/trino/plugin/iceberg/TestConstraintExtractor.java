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
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SymbolReference;
import io.trino.transaction.NoOpTransactionManager;
import io.trino.transaction.TransactionId;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.plugin.iceberg.ColumnIdentity.primitiveColumnIdentity;
import static io.trino.plugin.iceberg.ConstraintExtractor.extractTupleDomain;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.VarcharType.createVarcharType;
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
        Cast castOfColumn = new Cast(new SymbolReference(timestampTzColumnSymbol), toSqlType(DATE));

        LocalDate someDate = LocalDate.of(2005, 9, 10);
        Expression someDateExpression = LITERAL_ENCODER.toExpression(TEST_SESSION, someDate.toEpochDay(), DATE);

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
        FunctionCall truncateToDay = new FunctionCall(
                QualifiedName.of("date_trunc"),
                List.of(
                        LITERAL_ENCODER.toExpression(TEST_SESSION, utf8Slice("day"), createVarcharType(17)),
                        new SymbolReference(timestampTzColumnSymbol)));

        LocalDate someDate = LocalDate.of(2005, 9, 10);
        Expression someMidnightExpression = LITERAL_ENCODER.toExpression(
                TEST_SESSION,
                LongTimestampWithTimeZone.fromEpochMillisAndFraction(someDate.toEpochDay() * MILLISECONDS_PER_DAY, 0, UTC_KEY),
                TIMESTAMP_TZ_MICROS);
        Expression someMiddayExpression = LITERAL_ENCODER.toExpression(
                TEST_SESSION,
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
        FunctionCall extractYear = new FunctionCall(
                QualifiedName.of("year"),
                List.of(new SymbolReference(timestampTzColumnSymbol)));

        LocalDate someDate = LocalDate.of(2005, 9, 10);
        Expression yearExpression = LITERAL_ENCODER.toExpression(
                TEST_SESSION,
                2005L,
                BIGINT);

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

    @Test
    public void testIntersectSummaryAndExpressionExtraction()
    {
        String timestampTzColumnSymbol = "timestamp_tz_symbol";
        Cast castOfColumn = new Cast(new SymbolReference(timestampTzColumnSymbol), toSqlType(DATE));

        LocalDate someDate = LocalDate.of(2005, 9, 10);
        Expression someDateExpression = LITERAL_ENCODER.toExpression(TEST_SESSION, someDate.toEpochDay(), DATE);

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

    private static Constraint constraint(Expression expression, Map<String, IcebergColumnHandle> assignments)
    {
        return constraint(TupleDomain.all(), expression, assignments);
    }

    private static Constraint constraint(TupleDomain<ColumnHandle> summary, Expression expression, Map<String, IcebergColumnHandle> assignments)
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
