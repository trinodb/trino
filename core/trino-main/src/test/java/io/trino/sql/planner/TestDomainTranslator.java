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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Between;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.In;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Not;
import io.trino.sql.planner.DomainTranslator.ExtractionResult;
import io.trino.type.LikePattern;
import io.trino.type.LikePatternType;
import io.trino.util.DateTimeUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.Comparison.Operator.IS_DISTINCT_FROM;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.Comparison.Operator.NOT_EQUAL;
import static io.trino.sql.ir.IrUtils.and;
import static io.trino.sql.ir.IrUtils.or;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.type.ColorType.COLOR;
import static io.trino.type.LikeFunctions.LIKE_FUNCTION_NAME;
import static io.trino.type.LikeFunctions.LIKE_PATTERN_FUNCTION_NAME;
import static io.trino.type.Reals.toReal;
import static java.lang.String.format;
import static java.math.BigDecimal.ONE;
import static java.math.BigDecimal.TWO;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestDomainTranslator
{
    private static final Symbol C_BIGINT = new Symbol(BIGINT, "c_bigint");
    private static final Symbol C_DOUBLE = new Symbol(DOUBLE, "c_double");
    private static final Symbol C_VARCHAR = new Symbol(VARCHAR, "c_varchar");
    private static final Symbol C_BOOLEAN = new Symbol(BOOLEAN, "c_boolean");
    private static final Symbol C_BIGINT_1 = new Symbol(BIGINT, "c_bigint_1");
    private static final Symbol C_DOUBLE_1 = new Symbol(DOUBLE, "c_double_1");
    private static final Symbol C_VARCHAR_1 = new Symbol(VARCHAR, "c_varchar_1");
    private static final Symbol C_BOOLEAN_1 = new Symbol(BOOLEAN, "c_boolean_1");
    private static final Symbol C_TIMESTAMP = new Symbol(createTimestampType(3), "c_timestamp");
    private static final Symbol C_DATE = new Symbol(DATE, "c_date");
    private static final Symbol C_COLOR = new Symbol(COLOR, "c_color");
    private static final Symbol C_HYPER_LOG_LOG = new Symbol(HYPER_LOG_LOG, "c_hyper_log_log");
    private static final Symbol C_INTEGER = new Symbol(INTEGER, "c_integer");
    private static final Symbol C_INTEGER_1 = new Symbol(INTEGER, "c_integer_1");
    private static final Symbol C_CHAR = new Symbol(createCharType(10), "c_char");
    private static final Symbol C_DECIMAL_21_3 = new Symbol(createDecimalType(21, 3), "c_decimal_21_3");
    private static final Symbol C_DECIMAL_21_3_1 = new Symbol(createDecimalType(21, 3), "c_decimal_21_3_1");
    private static final Symbol C_DECIMAL_12_2 = new Symbol(createDecimalType(12, 2), "c_decimal_12_2");
    private static final Symbol C_DECIMAL_6_1 = new Symbol(createDecimalType(6, 1), "c_decimal_6_1");
    private static final Symbol C_DECIMAL_6_1_1 = new Symbol(createDecimalType(6, 1), "c_decimal_6_1_1");
    private static final Symbol C_SMALLINT = new Symbol(SMALLINT, "c_smallint");
    private static final Symbol C_TINYINT = new Symbol(TINYINT, "c_tinyint");
    private static final Symbol C_REAL = new Symbol(REAL, "c_real");
    private static final Symbol C_REAL_1 = new Symbol(REAL, "c_real_1");

    private static final long TIMESTAMP_VALUE = new DateTime(2013, 3, 30, 1, 5, 0, 0, DateTimeZone.UTC).getMillis();
    private static final long DATE_VALUE = TimeUnit.MILLISECONDS.toDays(new DateTime(2001, 1, 22, 0, 0, 0, 0, DateTimeZone.UTC).getMillis());
    private static final long COLOR_VALUE_1 = 1;
    private static final long COLOR_VALUE_2 = 2;

    private TestingFunctionResolution functionResolution;

    @BeforeAll
    public void setup()
    {
        functionResolution = new TestingFunctionResolution();
    }

    @AfterAll
    public void tearDown()
    {
        functionResolution = null;
    }

    @Test
    public void testNoneRoundTrip()
    {
        TupleDomain<Symbol> tupleDomain = TupleDomain.none();
        ExtractionResult result = fromPredicate(toPredicate(tupleDomain));
        assertThat(result.getRemainingExpression()).isEqualTo(TRUE);
        assertThat(result.getTupleDomain()).isEqualTo(tupleDomain);
    }

    @Test
    public void testAllRoundTrip()
    {
        TupleDomain<Symbol> tupleDomain = TupleDomain.all();
        ExtractionResult result = fromPredicate(toPredicate(tupleDomain));
        assertThat(result.getRemainingExpression()).isEqualTo(TRUE);
        assertThat(result.getTupleDomain()).isEqualTo(tupleDomain);
    }

    @Test
    public void testRoundTrip()
    {
        TupleDomain<Symbol> tupleDomain = tupleDomain(ImmutableMap.<Symbol, Domain>builder()
                .put(C_BIGINT, Domain.singleValue(BIGINT, 1L))
                .put(C_DOUBLE, Domain.onlyNull(DOUBLE))
                .put(C_VARCHAR, Domain.notNull(VARCHAR))
                .put(C_BOOLEAN, Domain.singleValue(BOOLEAN, true))
                .put(C_BIGINT_1, Domain.singleValue(BIGINT, 2L))
                .put(C_DOUBLE_1, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(DOUBLE, 1.1), Range.equal(DOUBLE, 2.0), Range.range(DOUBLE, 3.0, false, 3.5, true)), true))
                .put(C_VARCHAR_1, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(VARCHAR, utf8Slice("2013-01-01")), Range.greaterThan(VARCHAR, utf8Slice("2013-10-01"))), false))
                .put(C_TIMESTAMP, Domain.singleValue(TIMESTAMP_MILLIS, TIMESTAMP_VALUE))
                .put(C_DATE, Domain.singleValue(DATE, DATE_VALUE))
                .put(C_COLOR, Domain.singleValue(COLOR, COLOR_VALUE_1))
                .put(C_HYPER_LOG_LOG, Domain.notNull(HYPER_LOG_LOG))
                .buildOrThrow());

        assertPredicateTranslates(toPredicate(tupleDomain), tupleDomain);
    }

    @Test
    public void testInOptimization()
    {
        Domain testDomain = Domain.create(
                ValueSet.all(BIGINT)
                        .subtract(ValueSet.ofRanges(
                                Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L), Range.equal(BIGINT, 3L))), false);

        TupleDomain<Symbol> tupleDomain = tupleDomain(C_BIGINT, testDomain);
        assertThat(toPredicate(tupleDomain)).isEqualTo(not(in(C_BIGINT, ImmutableList.of(1L, 2L, 3L))));

        testDomain = Domain.create(
                ValueSet.ofRanges(
                        Range.lessThan(BIGINT, 4L)).intersect(
                        ValueSet.all(BIGINT)
                                .subtract(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L), Range.equal(BIGINT, 3L)))), false);

        tupleDomain = tupleDomain(C_BIGINT, testDomain);
        assertThat(toPredicate(tupleDomain)).isEqualTo(and(lessThan(C_BIGINT, bigintLiteral(4L)), not(in(C_BIGINT, ImmutableList.of(1L, 2L, 3L)))));

        testDomain = Domain.create(ValueSet.ofRanges(
                        Range.range(BIGINT, 1L, true, 3L, true),
                        Range.range(BIGINT, 5L, true, 7L, true),
                        Range.range(BIGINT, 9L, true, 11L, true)),
                false);

        tupleDomain = tupleDomain(C_BIGINT, testDomain);
        assertThat(toPredicate(tupleDomain)).isEqualTo(or(between(C_BIGINT, bigintLiteral(1L), bigintLiteral(3L)), between(C_BIGINT, bigintLiteral(5L), bigintLiteral(7L)), between(C_BIGINT, bigintLiteral(9L), bigintLiteral(11L))));

        testDomain = Domain.create(
                ValueSet.ofRanges(
                                Range.lessThan(BIGINT, 4L))
                        .intersect(ValueSet.all(BIGINT)
                                .subtract(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L), Range.equal(BIGINT, 3L))))
                        .union(ValueSet.ofRanges(Range.range(BIGINT, 7L, true, 9L, true))), false);

        tupleDomain = tupleDomain(C_BIGINT, testDomain);
        assertThat(toPredicate(tupleDomain)).isEqualTo(or(and(lessThan(C_BIGINT, bigintLiteral(4L)), not(in(C_BIGINT, ImmutableList.of(1L, 2L, 3L)))), between(C_BIGINT, bigintLiteral(7L), bigintLiteral(9L))));

        testDomain = Domain.create(
                ValueSet.ofRanges(Range.lessThan(BIGINT, 4L))
                        .intersect(ValueSet.all(BIGINT)
                                .subtract(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L), Range.equal(BIGINT, 3L))))
                        .union(ValueSet.ofRanges(Range.range(BIGINT, 7L, false, 9L, false), Range.range(BIGINT, 11L, false, 13L, false))), false);

        tupleDomain = tupleDomain(C_BIGINT, testDomain);
        assertThat(toPredicate(tupleDomain)).isEqualTo(or(
                and(lessThan(C_BIGINT, bigintLiteral(4L)), not(in(C_BIGINT, ImmutableList.of(1L, 2L, 3L)))),
                and(greaterThan(C_BIGINT, bigintLiteral(7L)), lessThan(C_BIGINT, bigintLiteral(9L))),
                and(greaterThan(C_BIGINT, bigintLiteral(11L)), lessThan(C_BIGINT, bigintLiteral(13L)))));
    }

    @Test
    public void testToPredicateNone()
    {
        TupleDomain<Symbol> tupleDomain = tupleDomain(ImmutableMap.<Symbol, Domain>builder()
                .put(C_BIGINT, Domain.singleValue(BIGINT, 1L))
                .put(C_DOUBLE, Domain.onlyNull(DOUBLE))
                .put(C_VARCHAR, Domain.notNull(VARCHAR))
                .put(C_BOOLEAN, Domain.none(BOOLEAN))
                .buildOrThrow());

        assertThat(toPredicate(tupleDomain)).isEqualTo(FALSE);
    }

    @Test
    public void testToPredicateAllIgnored()
    {
        TupleDomain<Symbol> tupleDomain = tupleDomain(ImmutableMap.<Symbol, Domain>builder()
                .put(C_BIGINT, Domain.singleValue(BIGINT, 1L))
                .put(C_DOUBLE, Domain.onlyNull(DOUBLE))
                .put(C_VARCHAR, Domain.notNull(VARCHAR))
                .put(C_BOOLEAN, Domain.all(BOOLEAN))
                .buildOrThrow());

        ExtractionResult result = fromPredicate(toPredicate(tupleDomain));
        assertThat(result.getRemainingExpression()).isEqualTo(TRUE);
        assertThat(result.getTupleDomain()).isEqualTo(tupleDomain(ImmutableMap.<Symbol, Domain>builder()
                .put(C_BIGINT, Domain.singleValue(BIGINT, 1L))
                .put(C_DOUBLE, Domain.onlyNull(DOUBLE))
                .put(C_VARCHAR, Domain.notNull(VARCHAR))
                .buildOrThrow()));
    }

    @Test
    public void testToPredicate()
    {
        TupleDomain<Symbol> tupleDomain;

        tupleDomain = tupleDomain(C_BIGINT, Domain.notNull(BIGINT));
        assertThat(toPredicate(tupleDomain)).isEqualTo(isNotNull(C_BIGINT));

        tupleDomain = tupleDomain(C_BIGINT, Domain.onlyNull(BIGINT));
        assertThat(toPredicate(tupleDomain)).isEqualTo(isNull(C_BIGINT));

        tupleDomain = tupleDomain(C_BIGINT, Domain.none(BIGINT));
        assertThat(toPredicate(tupleDomain)).isEqualTo(FALSE);

        tupleDomain = tupleDomain(C_BIGINT, Domain.all(BIGINT));
        assertThat(toPredicate(tupleDomain)).isEqualTo(TRUE);

        tupleDomain = tupleDomain(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 1L)), false));
        assertThat(toPredicate(tupleDomain)).isEqualTo(greaterThan(C_BIGINT, bigintLiteral(1L)));

        tupleDomain = tupleDomain(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 1L)), false));
        assertThat(toPredicate(tupleDomain)).isEqualTo(greaterThanOrEqual(C_BIGINT, bigintLiteral(1L)));

        tupleDomain = tupleDomain(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 1L)), false));
        assertThat(toPredicate(tupleDomain)).isEqualTo(lessThan(C_BIGINT, bigintLiteral(1L)));

        tupleDomain = tupleDomain(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 0L, false, 1L, true)), false));
        assertThat(toPredicate(tupleDomain)).isEqualTo(and(greaterThan(C_BIGINT, bigintLiteral(0L)), lessThanOrEqual(C_BIGINT, bigintLiteral(1L))));

        tupleDomain = tupleDomain(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 1L)), false));
        assertThat(toPredicate(tupleDomain)).isEqualTo(lessThanOrEqual(C_BIGINT, bigintLiteral(1L)));

        tupleDomain = tupleDomain(C_BIGINT, Domain.singleValue(BIGINT, 1L));
        assertThat(toPredicate(tupleDomain)).isEqualTo(equal(C_BIGINT, bigintLiteral(1L)));

        tupleDomain = tupleDomain(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), false));
        assertThat(toPredicate(tupleDomain)).isEqualTo(in(C_BIGINT, ImmutableList.of(1L, 2L)));

        tupleDomain = tupleDomain(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 1L)), true));
        assertThat(toPredicate(tupleDomain)).isEqualTo(or(isNull(C_BIGINT), lessThan(C_BIGINT, bigintLiteral(1L))));

        tupleDomain = tupleDomain(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1), true));
        assertThat(toPredicate(tupleDomain)).isEqualTo(or(isNull(C_COLOR), equal(C_COLOR, colorLiteral(COLOR_VALUE_1))));

        tupleDomain = tupleDomain(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1).complement(), true));
        assertThat(toPredicate(tupleDomain)).isEqualTo(or(isNull(C_COLOR), not(equal(C_COLOR, colorLiteral(COLOR_VALUE_1)))));

        tupleDomain = tupleDomain(C_HYPER_LOG_LOG, Domain.onlyNull(HYPER_LOG_LOG));
        assertThat(toPredicate(tupleDomain)).isEqualTo(isNull(C_HYPER_LOG_LOG));

        tupleDomain = tupleDomain(C_HYPER_LOG_LOG, Domain.notNull(HYPER_LOG_LOG));
        assertThat(toPredicate(tupleDomain)).isEqualTo(isNotNull(C_HYPER_LOG_LOG));
    }

    @Test
    public void testToPredicateWithRangeOptimisation()
    {
        TupleDomain<Symbol> tupleDomain;

        tupleDomain = tupleDomain(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 1L), Range.lessThan(BIGINT, 1L)), false));
        assertThat(toPredicate(tupleDomain)).isEqualTo(notEqual(C_BIGINT, bigintLiteral(1L)));

        tupleDomain = tupleDomain(C_BIGINT, Domain.create(
                ValueSet.ofRanges(
                        Range.lessThan(BIGINT, 0L),
                        Range.range(BIGINT, 0L, false, 1L, false),
                        Range.greaterThan(BIGINT, 1L)),
                false));
        assertThat(toPredicate(tupleDomain)).isEqualTo(not(in(C_BIGINT, ImmutableList.of(0L, 1L))));

        tupleDomain = tupleDomain(C_BIGINT, Domain.create(
                ValueSet.ofRanges(
                        Range.lessThan(BIGINT, 0L),
                        Range.range(BIGINT, 0L, false, 1L, false),
                        Range.greaterThan(BIGINT, 2L)),
                false));
        assertThat(toPredicate(tupleDomain)).isEqualTo(or(and(lessThan(C_BIGINT, bigintLiteral(1L)), notEqual(C_BIGINT, bigintLiteral(0L))), greaterThan(C_BIGINT, bigintLiteral(2L))));

        // floating point types: do not coalesce ranges when range "all" would be introduced
        tupleDomain = tupleDomain(C_REAL, Domain.create(ValueSet.ofRanges(Range.greaterThan(REAL, 0L), Range.lessThan(REAL, 0L)), false));
        assertThat(toPredicate(tupleDomain)).isEqualTo(or(lessThan(C_REAL, realLiteral(0.0f)), greaterThan(C_REAL, realLiteral(0.0f))));

        tupleDomain = tupleDomain(C_REAL, Domain.create(
                ValueSet.ofRanges(
                        Range.lessThan(REAL, 0L),
                        Range.range(REAL, 0L, false, toReal(1F), false),
                        Range.greaterThan(REAL, toReal(1F))),
                false));
        assertThat(toPredicate(tupleDomain)).isEqualTo(or(
                lessThan(C_REAL, realLiteral(0.0f)),
                and(greaterThan(C_REAL, realLiteral(0.0f)), lessThan(C_REAL, realLiteral(1.0f))),
                greaterThan(C_REAL, realLiteral(1.0f))));

        tupleDomain = tupleDomain(C_REAL, Domain.create(
                ValueSet.ofRanges(
                        Range.lessThan(REAL, 0L),
                        Range.range(REAL, 0L, false, toReal(1F), false),
                        Range.greaterThan(REAL, toReal(2F))),
                false));
        assertThat(toPredicate(tupleDomain)).isEqualTo(or(and(lessThan(C_REAL, realLiteral(1.0f)), notEqual(C_REAL, realLiteral(0.0f))), greaterThan(C_REAL, realLiteral(2.0f))));

        tupleDomain = tupleDomain(C_DOUBLE, Domain.create(
                ValueSet.ofRanges(
                        Range.lessThan(DOUBLE, 0.0),
                        Range.range(DOUBLE, 0.0, false, 1.0, false),
                        Range.range(DOUBLE, 2.0, false, 3.0, false),
                        Range.greaterThan(DOUBLE, 3.0)),
                false));
        assertThat(toPredicate(tupleDomain)).isEqualTo(or(
                and(lessThan(C_DOUBLE, doubleLiteral(1)), notEqual(C_DOUBLE, doubleLiteral(0))),
                and(greaterThan(C_DOUBLE, doubleLiteral(2)), notEqual(C_DOUBLE, doubleLiteral(3)))));
    }

    @Test
    public void testFromUnknownPredicate()
    {
        assertUnsupportedPredicate(unprocessableExpression1(C_BIGINT));
        assertUnsupportedPredicate(not(unprocessableExpression1(C_BIGINT)));
    }

    @Test
    public void testFromAndPredicate()
    {
        Expression originalPredicate = and(
                and(greaterThan(C_BIGINT, bigintLiteral(1L)), unprocessableExpression1(C_BIGINT)),
                and(lessThan(C_BIGINT, bigintLiteral(5L)), unprocessableExpression2(C_BIGINT)));
        ExtractionResult result = fromPredicate(originalPredicate);
        assertThat(result.getRemainingExpression()).isEqualTo(and(unprocessableExpression1(C_BIGINT), unprocessableExpression2(C_BIGINT)));
        assertThat(result.getTupleDomain()).isEqualTo(tupleDomain(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 1L, false, 5L, false)), false)));

        // Test complements
        assertUnsupportedPredicate(not(and(
                and(greaterThan(C_BIGINT, bigintLiteral(1L)), unprocessableExpression1(C_BIGINT)),
                and(lessThan(C_BIGINT, bigintLiteral(5L)), unprocessableExpression2(C_BIGINT)))));

        originalPredicate = not(and(
                not(and(greaterThan(C_BIGINT, bigintLiteral(1L)), unprocessableExpression1(C_BIGINT))),
                not(and(lessThan(C_BIGINT, bigintLiteral(5L)), unprocessableExpression2(C_BIGINT)))));
        result = fromPredicate(originalPredicate);
        assertThat(result.getRemainingExpression()).isEqualTo(originalPredicate);
        assertThat(result.getTupleDomain()).isEqualTo(tupleDomain(C_BIGINT, Domain.notNull(BIGINT)));
    }

    @Test
    public void testFromOrPredicate()
    {
        Expression originalPredicate = or(
                and(greaterThan(C_BIGINT, bigintLiteral(1L)), unprocessableExpression1(C_BIGINT)),
                and(lessThan(C_BIGINT, bigintLiteral(5L)), unprocessableExpression2(C_BIGINT)));
        ExtractionResult result = fromPredicate(originalPredicate);
        assertThat(result.getRemainingExpression()).isEqualTo(originalPredicate);
        assertThat(result.getTupleDomain()).isEqualTo(tupleDomain(C_BIGINT, Domain.notNull(BIGINT)));

        originalPredicate = or(
                and(equal(C_BIGINT, bigintLiteral(1L)), unprocessableExpression1(C_BIGINT)),
                and(equal(C_BIGINT, bigintLiteral(2L)), unprocessableExpression2(C_BIGINT)));
        result = fromPredicate(originalPredicate);
        assertThat(result.getRemainingExpression()).isEqualTo(originalPredicate);
        assertThat(result.getTupleDomain()).isEqualTo(tupleDomain(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), false)));

        originalPredicate = or(
                and(lessThan(C_BIGINT, bigintLiteral(20L)), unprocessableExpression1(C_BIGINT)),
                and(greaterThan(C_BIGINT, bigintLiteral(10L)), unprocessableExpression2(C_BIGINT)));
        result = fromPredicate(originalPredicate);
        assertThat(result.getRemainingExpression()).isEqualTo(originalPredicate);
        assertThat(result.getTupleDomain()).isEqualTo(tupleDomain(C_BIGINT, Domain.create(ValueSet.all(BIGINT), false)));

        // Same unprocessableExpression means that we can do more extraction
        // If both sides are operating on the same single symbol
        originalPredicate = or(
                and(equal(C_BIGINT, bigintLiteral(1L)), unprocessableExpression1(C_BIGINT)),
                and(equal(C_BIGINT, bigintLiteral(2L)), unprocessableExpression1(C_BIGINT)));
        result = fromPredicate(originalPredicate);
        assertThat(result.getRemainingExpression()).isEqualTo(unprocessableExpression1(C_BIGINT));
        assertThat(result.getTupleDomain()).isEqualTo(tupleDomain(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), false)));

        // And not if they have different symbols
        assertUnsupportedPredicate(or(
                and(equal(C_BIGINT, bigintLiteral(1L)), unprocessableExpression1(C_BIGINT)),
                and(equal(C_DOUBLE, doubleLiteral(2.0)), unprocessableExpression1(C_BIGINT))));

        // Domain union implicitly adds NaN as an accepted value
        // The original predicate is returned as the RemainingExpression
        // (even if left and right unprocessableExpressions are the same)
        originalPredicate = or(
                greaterThan(C_DOUBLE, doubleLiteral(2.0)),
                lessThan(C_DOUBLE, doubleLiteral(5.0)));
        result = fromPredicate(originalPredicate);
        assertThat(result.getRemainingExpression()).isEqualTo(originalPredicate);
        assertThat(result.getTupleDomain()).isEqualTo(tupleDomain(C_DOUBLE, Domain.notNull(DOUBLE)));

        originalPredicate = or(
                greaterThan(C_REAL, realLiteral(2.0f)),
                lessThan(C_REAL, realLiteral(5.0f)),
                isNull(C_REAL));
        result = fromPredicate(originalPredicate);
        assertThat(result.getRemainingExpression()).isEqualTo(originalPredicate);
        assertThat(result.getTupleDomain()).isEqualTo(TupleDomain.all());

        originalPredicate = or(
                and(greaterThan(C_DOUBLE, doubleLiteral(2.0)), unprocessableExpression1(C_DOUBLE)),
                and(lessThan(C_DOUBLE, doubleLiteral(5.0)), unprocessableExpression1(C_DOUBLE)));
        result = fromPredicate(originalPredicate);
        assertThat(result.getRemainingExpression()).isEqualTo(originalPredicate);
        assertThat(result.getTupleDomain()).isEqualTo(tupleDomain(C_DOUBLE, Domain.notNull(DOUBLE)));

        originalPredicate = or(
                and(greaterThan(C_REAL, realLiteral(2.0f)), unprocessableExpression1(C_REAL)),
                and(lessThan(C_REAL, realLiteral(5.0f)), unprocessableExpression1(C_REAL)));
        result = fromPredicate(originalPredicate);
        assertThat(result.getRemainingExpression()).isEqualTo(originalPredicate);
        assertThat(result.getTupleDomain()).isEqualTo(tupleDomain(C_REAL, Domain.notNull(REAL)));

        // We can make another optimization if one side is the super set of the other side
        originalPredicate = or(
                and(greaterThan(C_BIGINT, bigintLiteral(1L)), greaterThan(C_DOUBLE, doubleLiteral(1.0)), unprocessableExpression1(C_BIGINT)),
                and(greaterThan(C_BIGINT, bigintLiteral(2L)), greaterThan(C_DOUBLE, doubleLiteral(2.0)), unprocessableExpression1(C_BIGINT)));
        result = fromPredicate(originalPredicate);
        assertThat(result.getRemainingExpression()).isEqualTo(unprocessableExpression1(C_BIGINT));
        assertThat(result.getTupleDomain()).isEqualTo(tupleDomain(
                C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 1L)), false),
                C_DOUBLE, Domain.create(ValueSet.ofRanges(Range.greaterThan(DOUBLE, 1.0)), false)));

        // We can't make those inferences if the unprocessableExpressions are non-deterministic
        originalPredicate = or(
                and(equal(C_BIGINT, bigintLiteral(1L)), randPredicate(C_BIGINT, BIGINT)),
                and(equal(C_BIGINT, bigintLiteral(2L)), randPredicate(C_BIGINT, BIGINT)));
        result = fromPredicate(originalPredicate);
        assertThat(result.getRemainingExpression()).isEqualTo(originalPredicate);
        assertThat(result.getTupleDomain()).isEqualTo(tupleDomain(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), false)));

        // Test complements
        originalPredicate = not(or(
                and(greaterThan(C_BIGINT, bigintLiteral(1L)), unprocessableExpression1(C_BIGINT)),
                and(lessThan(C_BIGINT, bigintLiteral(5L)), unprocessableExpression2(C_BIGINT))));
        result = fromPredicate(originalPredicate);
        assertThat(result.getRemainingExpression()).isEqualTo(and(
                not(and(greaterThan(C_BIGINT, bigintLiteral(1L)), unprocessableExpression1(C_BIGINT))),
                not(and(lessThan(C_BIGINT, bigintLiteral(5L)), unprocessableExpression2(C_BIGINT)))));
        assertThat(result.getTupleDomain().isAll()).isTrue();

        originalPredicate = not(or(
                not(and(greaterThan(C_BIGINT, bigintLiteral(1L)), unprocessableExpression1(C_BIGINT))),
                not(and(lessThan(C_BIGINT, bigintLiteral(5L)), unprocessableExpression2(C_BIGINT)))));
        result = fromPredicate(originalPredicate);
        assertThat(result.getRemainingExpression()).isEqualTo(and(unprocessableExpression1(C_BIGINT), unprocessableExpression2(C_BIGINT)));
        assertThat(result.getTupleDomain()).isEqualTo(tupleDomain(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 1L, false, 5L, false)), false)));
    }

    @Test
    public void testFromSingleBooleanReference()
    {
        Expression originalPredicate = C_BOOLEAN.toSymbolReference();
        ExtractionResult result = fromPredicate(originalPredicate);
        assertThat(result.getTupleDomain()).isEqualTo(tupleDomain(C_BOOLEAN, Domain.create(ValueSet.ofRanges(Range.equal(BOOLEAN, true)), false)));
        assertThat(result.getRemainingExpression()).isEqualTo(TRUE);

        originalPredicate = not(C_BOOLEAN.toSymbolReference());
        result = fromPredicate(originalPredicate);
        assertThat(result.getTupleDomain()).isEqualTo(tupleDomain(C_BOOLEAN, Domain.create(ValueSet.ofRanges(Range.equal(BOOLEAN, true)).complement(), false)));
        assertThat(result.getRemainingExpression()).isEqualTo(TRUE);

        originalPredicate = and(C_BOOLEAN.toSymbolReference(), C_BOOLEAN_1.toSymbolReference());
        result = fromPredicate(originalPredicate);
        Domain domain = Domain.create(ValueSet.ofRanges(Range.equal(BOOLEAN, true)), false);
        assertThat(result.getTupleDomain()).isEqualTo(tupleDomain(C_BOOLEAN, domain, C_BOOLEAN_1, domain));
        assertThat(result.getRemainingExpression()).isEqualTo(TRUE);

        originalPredicate = or(C_BOOLEAN.toSymbolReference(), C_BOOLEAN_1.toSymbolReference());
        result = fromPredicate(originalPredicate);
        assertThat(result.getTupleDomain()).isEqualTo(TupleDomain.all());
        assertThat(result.getRemainingExpression()).isEqualTo(originalPredicate);

        originalPredicate = not(and(C_BOOLEAN.toSymbolReference(), C_BOOLEAN_1.toSymbolReference()));
        result = fromPredicate(originalPredicate);
        assertThat(result.getTupleDomain()).isEqualTo(TupleDomain.all());
        assertThat(result.getRemainingExpression()).isEqualTo(originalPredicate);
    }

    @Test
    public void testFromNotPredicate()
    {
        assertUnsupportedPredicate(not(and(equal(C_BIGINT, bigintLiteral(1L)), unprocessableExpression1(C_BIGINT))));
        assertUnsupportedPredicate(not(unprocessableExpression1(C_BIGINT)));

        assertPredicateIsAlwaysFalse(not(TRUE));

        assertPredicateTranslates(
                not(equal(C_BIGINT, bigintLiteral(1L))),
                tupleDomain(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 1L), Range.greaterThan(BIGINT, 1L)), false)));
    }

    @Test
    public void testFromUnprocessableComparison()
    {
        assertUnsupportedPredicate(comparison(GREATER_THAN, unprocessableExpression1(C_BIGINT), unprocessableExpression2(C_BIGINT)));
        assertUnsupportedPredicate(not(comparison(GREATER_THAN, unprocessableExpression1(C_BIGINT), unprocessableExpression2(C_BIGINT))));
    }

    @Test
    public void testFromBasicComparisons()
    {
        // Test out the extraction of all basic comparisons
        assertPredicateTranslates(
                greaterThan(C_BIGINT, bigintLiteral(2L)),
                tupleDomain(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 2L)), false)));

        assertPredicateTranslates(
                greaterThanOrEqual(C_BIGINT, bigintLiteral(2L)),
                tupleDomain(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 2L)), false)));

        assertPredicateTranslates(
                lessThan(C_BIGINT, bigintLiteral(2L)),
                tupleDomain(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L)), false)));

        assertPredicateTranslates(
                lessThanOrEqual(C_BIGINT, bigintLiteral(2L)),
                tupleDomain(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 2L)), false)));

        assertPredicateTranslates(
                equal(C_BIGINT, bigintLiteral(2L)),
                tupleDomain(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), false)));

        assertPredicateTranslates(
                notEqual(C_BIGINT, bigintLiteral(2L)),
                tupleDomain(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L), Range.greaterThan(BIGINT, 2L)), false)));

        assertPredicateTranslates(
                isDistinctFrom(C_BIGINT, bigintLiteral(2L)),
                tupleDomain(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L), Range.greaterThan(BIGINT, 2L)), true)));

        assertPredicateTranslates(
                equal(C_COLOR, colorLiteral(COLOR_VALUE_1)),
                tupleDomain(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1), false)));

        assertPredicateTranslates(
                in(C_COLOR, ImmutableList.of(colorLiteral(COLOR_VALUE_1), colorLiteral(COLOR_VALUE_2))),
                tupleDomain(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1, COLOR_VALUE_2), false)));

        assertPredicateTranslates(
                isDistinctFrom(C_COLOR, colorLiteral(COLOR_VALUE_1)),
                tupleDomain(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1).complement(), true)));

        // Test complement
        assertPredicateTranslates(
                not(greaterThan(C_BIGINT, bigintLiteral(2L))),
                tupleDomain(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 2L)), false)));

        assertPredicateTranslates(
                not(greaterThanOrEqual(C_BIGINT, bigintLiteral(2L))),
                tupleDomain(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L)), false)));

        assertPredicateTranslates(
                not(lessThan(C_BIGINT, bigintLiteral(2L))),
                tupleDomain(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 2L)), false)));

        assertPredicateTranslates(
                not(lessThanOrEqual(C_BIGINT, bigintLiteral(2L))),
                tupleDomain(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 2L)), false)));

        assertPredicateTranslates(
                not(equal(C_BIGINT, bigintLiteral(2L))),
                tupleDomain(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L), Range.greaterThan(BIGINT, 2L)), false)));

        assertPredicateTranslates(
                not(notEqual(C_BIGINT, bigintLiteral(2L))),
                tupleDomain(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), false)));

        assertPredicateTranslates(
                not(isDistinctFrom(C_BIGINT, bigintLiteral(2L))),
                tupleDomain(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), false)));

        assertPredicateTranslates(
                not(equal(C_COLOR, colorLiteral(COLOR_VALUE_1))),
                tupleDomain(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1).complement(), false)));

        assertPredicateTranslates(
                not(in(C_COLOR, ImmutableList.of(colorLiteral(COLOR_VALUE_1), colorLiteral(COLOR_VALUE_2)))),
                tupleDomain(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1, COLOR_VALUE_2).complement(), false)));

        assertPredicateTranslates(
                not(isDistinctFrom(C_COLOR, colorLiteral(COLOR_VALUE_1))),
                tupleDomain(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1), false)));
    }

    @Test
    public void testFromFlippedBasicComparisons()
    {
        // Test out the extraction of all basic comparisons where the reference literal ordering is flipped
        assertPredicateTranslates(
                comparison(GREATER_THAN, bigintLiteral(2L), C_BIGINT.toSymbolReference()),
                tupleDomain(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L)), false)));

        assertPredicateTranslates(
                comparison(GREATER_THAN_OR_EQUAL, bigintLiteral(2L), C_BIGINT.toSymbolReference()),
                tupleDomain(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 2L)), false)));

        assertPredicateTranslates(
                comparison(LESS_THAN, bigintLiteral(2L), C_BIGINT.toSymbolReference()),
                tupleDomain(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 2L)), false)));

        assertPredicateTranslates(
                comparison(LESS_THAN_OR_EQUAL, bigintLiteral(2L), C_BIGINT.toSymbolReference()),
                tupleDomain(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 2L)), false)));

        assertPredicateTranslates(comparison(EQUAL, bigintLiteral(2L), C_BIGINT.toSymbolReference()),
                tupleDomain(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), false)));

        assertPredicateTranslates(comparison(EQUAL, colorLiteral(COLOR_VALUE_1), C_COLOR.toSymbolReference()),
                tupleDomain(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1), false)));

        assertPredicateTranslates(comparison(NOT_EQUAL, bigintLiteral(2L), C_BIGINT.toSymbolReference()),
                tupleDomain(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L), Range.greaterThan(BIGINT, 2L)), false)));

        assertPredicateTranslates(
                comparison(NOT_EQUAL, colorLiteral(COLOR_VALUE_1), C_COLOR.toSymbolReference()),
                tupleDomain(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1).complement(), false)));

        assertPredicateTranslates(comparison(IS_DISTINCT_FROM, bigintLiteral(2L), C_BIGINT.toSymbolReference()),
                tupleDomain(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L), Range.greaterThan(BIGINT, 2L)), true)));

        assertPredicateTranslates(
                comparison(IS_DISTINCT_FROM, colorLiteral(COLOR_VALUE_1), C_COLOR.toSymbolReference()),
                tupleDomain(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1).complement(), true)));

        assertPredicateTranslates(
                comparison(IS_DISTINCT_FROM, nullLiteral(BIGINT), C_BIGINT.toSymbolReference()),
                tupleDomain(C_BIGINT, Domain.notNull(BIGINT)));
    }

    @Test
    public void testFromBasicComparisonsWithNulls()
    {
        // Test out the extraction of all basic comparisons with null literals
        assertPredicateIsAlwaysFalse(greaterThan(C_BIGINT, nullLiteral(BIGINT)));

        assertPredicateTranslates(
                greaterThan(C_VARCHAR, nullLiteral(VARCHAR)),
                tupleDomain(C_VARCHAR, Domain.create(ValueSet.none(VARCHAR), false)));

        assertPredicateIsAlwaysFalse(greaterThanOrEqual(C_BIGINT, nullLiteral(BIGINT)));
        assertPredicateIsAlwaysFalse(lessThan(C_BIGINT, nullLiteral(BIGINT)));
        assertPredicateIsAlwaysFalse(lessThanOrEqual(C_BIGINT, nullLiteral(BIGINT)));
        assertPredicateIsAlwaysFalse(equal(C_BIGINT, nullLiteral(BIGINT)));
        assertPredicateIsAlwaysFalse(equal(C_COLOR, nullLiteral(COLOR)));
        assertPredicateIsAlwaysFalse(notEqual(C_BIGINT, nullLiteral(BIGINT)));
        assertPredicateIsAlwaysFalse(notEqual(C_COLOR, nullLiteral(COLOR)));

        assertPredicateTranslates(
                isDistinctFrom(C_BIGINT, nullLiteral(BIGINT)),
                tupleDomain(C_BIGINT, Domain.notNull(BIGINT)));

        assertPredicateTranslates(
                isDistinctFrom(C_COLOR, nullLiteral(COLOR)),
                tupleDomain(C_COLOR, Domain.notNull(COLOR)));

        // Test complements
        assertPredicateIsAlwaysFalse(not(greaterThan(C_BIGINT, nullLiteral(BIGINT))));
        assertPredicateIsAlwaysFalse(not(greaterThanOrEqual(C_BIGINT, nullLiteral(BIGINT))));
        assertPredicateIsAlwaysFalse(not(lessThan(C_BIGINT, nullLiteral(BIGINT))));
        assertPredicateIsAlwaysFalse(not(lessThanOrEqual(C_BIGINT, nullLiteral(BIGINT))));
        assertPredicateIsAlwaysFalse(not(equal(C_BIGINT, nullLiteral(BIGINT))));
        assertPredicateIsAlwaysFalse(not(equal(C_COLOR, nullLiteral(COLOR))));
        assertPredicateIsAlwaysFalse(not(notEqual(C_BIGINT, nullLiteral(BIGINT))));
        assertPredicateIsAlwaysFalse(not(notEqual(C_COLOR, nullLiteral(COLOR))));

        assertPredicateTranslates(
                not(isDistinctFrom(C_BIGINT, nullLiteral(BIGINT))),
                tupleDomain(C_BIGINT, Domain.onlyNull(BIGINT)));

        assertPredicateTranslates(
                not(isDistinctFrom(C_COLOR, nullLiteral(COLOR))),
                tupleDomain(C_COLOR, Domain.onlyNull(COLOR)));
    }

    @Test
    public void testFromBasicComparisonsWithNaN()
    {
        Expression nanDouble = new Constant(DOUBLE, Double.NaN);

        assertPredicateIsAlwaysFalse(equal(C_DOUBLE, nanDouble));
        assertPredicateIsAlwaysFalse(greaterThan(C_DOUBLE, nanDouble));
        assertPredicateIsAlwaysFalse(greaterThanOrEqual(C_DOUBLE, nanDouble));
        assertPredicateIsAlwaysFalse(lessThan(C_DOUBLE, nanDouble));
        assertPredicateIsAlwaysFalse(lessThanOrEqual(C_DOUBLE, nanDouble));
        assertPredicateTranslates(notEqual(C_DOUBLE, nanDouble), tupleDomain(C_DOUBLE, Domain.notNull(DOUBLE)));
        assertUnsupportedPredicate(isDistinctFrom(C_DOUBLE, nanDouble));

        assertPredicateTranslates(not(equal(C_DOUBLE, nanDouble)), tupleDomain(C_DOUBLE, Domain.notNull(DOUBLE)));
        assertPredicateTranslates(not(greaterThan(C_DOUBLE, nanDouble)), tupleDomain(C_DOUBLE, Domain.notNull(DOUBLE)));
        assertPredicateTranslates(not(greaterThanOrEqual(C_DOUBLE, nanDouble)), tupleDomain(C_DOUBLE, Domain.notNull(DOUBLE)));
        assertPredicateTranslates(not(lessThan(C_DOUBLE, nanDouble)), tupleDomain(C_DOUBLE, Domain.notNull(DOUBLE)));
        assertPredicateTranslates(not(lessThanOrEqual(C_DOUBLE, nanDouble)), tupleDomain(C_DOUBLE, Domain.notNull(DOUBLE)));
        assertPredicateIsAlwaysFalse(not(notEqual(C_DOUBLE, nanDouble)));
        assertUnsupportedPredicate(not(isDistinctFrom(C_DOUBLE, nanDouble)));

        Expression nanReal = new Constant(REAL, toReal(Float.NaN));

        assertPredicateIsAlwaysFalse(equal(C_REAL, nanReal));
        assertPredicateIsAlwaysFalse(greaterThan(C_REAL, nanReal));
        assertPredicateIsAlwaysFalse(greaterThanOrEqual(C_REAL, nanReal));
        assertPredicateIsAlwaysFalse(lessThan(C_REAL, nanReal));
        assertPredicateIsAlwaysFalse(lessThanOrEqual(C_REAL, nanReal));
        assertPredicateTranslates(notEqual(C_REAL, nanReal), tupleDomain(C_REAL, Domain.notNull(REAL)));
        assertUnsupportedPredicate(isDistinctFrom(C_REAL, nanReal));

        assertPredicateTranslates(not(equal(C_REAL, nanReal)), tupleDomain(C_REAL, Domain.notNull(REAL)));
        assertPredicateTranslates(not(greaterThan(C_REAL, nanReal)), tupleDomain(C_REAL, Domain.notNull(REAL)));
        assertPredicateTranslates(not(greaterThanOrEqual(C_REAL, nanReal)), tupleDomain(C_REAL, Domain.notNull(REAL)));
        assertPredicateTranslates(not(lessThan(C_REAL, nanReal)), tupleDomain(C_REAL, Domain.notNull(REAL)));
        assertPredicateTranslates(not(lessThanOrEqual(C_REAL, nanReal)), tupleDomain(C_REAL, Domain.notNull(REAL)));
        assertPredicateIsAlwaysFalse(not(notEqual(C_REAL, nanReal)));
        assertUnsupportedPredicate(not(isDistinctFrom(C_REAL, nanReal)));
    }

    @Test
    public void testFromCoercionComparisonsWithNaN()
    {
        Expression nanDouble = new Constant(DOUBLE, Double.NaN);

        assertPredicateIsAlwaysFalse(equal(cast(C_TINYINT, DOUBLE), nanDouble));
        assertPredicateIsAlwaysFalse(equal(cast(C_SMALLINT, DOUBLE), nanDouble));
        assertPredicateIsAlwaysFalse(equal(cast(C_INTEGER, DOUBLE), nanDouble));
    }

    @Test
    public void testNonImplicitCastOnSymbolSide()
    {
        // we expect TupleDomain.all here().
        // see comment in DomainTranslator.Visitor.visitComparisonExpression()
        assertUnsupportedPredicate(equal(
                cast(C_TIMESTAMP, DATE),
                new Constant(DATE, DATE_VALUE)));
        assertUnsupportedPredicate(equal(
                cast(C_DECIMAL_12_2, BIGINT),
                bigintLiteral(135L)));
    }

    @Test
    public void testNoSaturatedFloorCastFromUnsupportedApproximateDomain()
    {
        assertUnsupportedPredicate(equal(
                cast(C_DECIMAL_12_2, DOUBLE),
                new Constant(DOUBLE, 12345.56)));

        assertUnsupportedPredicate(equal(
                cast(C_BIGINT, DOUBLE),
                new Constant(DOUBLE, 12345.56)));

        assertUnsupportedPredicate(equal(
                cast(C_BIGINT, REAL),
                new Constant(REAL, toReal(12345.56f))));

        assertUnsupportedPredicate(equal(
                cast(C_INTEGER, REAL),
                new Constant(REAL, toReal(12345.56f))));
    }

    @Test
    public void testPredicateWithVarcharCastToDate()
    {
        // =
        assertPredicateDerives(
                equal(cast(C_VARCHAR, DATE), new Constant(DATE, (long) DateTimeUtils.parseDate("2005-9-10"))),
                tupleDomain(C_VARCHAR, Domain.create(ValueSet.ofRanges(
                                Range.lessThan(VARCHAR, utf8Slice("1")),
                                Range.range(VARCHAR, utf8Slice("2005-09-10"), true, utf8Slice("2005-09-11"), false),
                                Range.range(VARCHAR, utf8Slice("2005-9-10"), true, utf8Slice("2005-9-11"), false),
                                Range.greaterThan(VARCHAR, utf8Slice("9"))),
                        false)));
        // = with day ending with 9
        assertPredicateDerives(
                equal(cast(C_VARCHAR, DATE), new Constant(DATE, (long) DateTimeUtils.parseDate("2005-09-09"))),
                tupleDomain(C_VARCHAR, Domain.create(ValueSet.ofRanges(
                                Range.lessThan(VARCHAR, utf8Slice("1")),
                                Range.range(VARCHAR, utf8Slice("2005-09-09"), true, utf8Slice("2005-09-0:"), false),
                                Range.range(VARCHAR, utf8Slice("2005-09-9"), true, utf8Slice("2005-09-:"), false),
                                Range.range(VARCHAR, utf8Slice("2005-9-09"), true, utf8Slice("2005-9-0:"), false),
                                Range.range(VARCHAR, utf8Slice("2005-9-9"), true, utf8Slice("2005-9-:"), false),
                                Range.greaterThan(VARCHAR, utf8Slice("9"))),
                        false)));
        assertPredicateDerives(
                equal(cast(C_VARCHAR, DATE), new Constant(DATE, (long) DateTimeUtils.parseDate("2005-09-19"))),
                tupleDomain(C_VARCHAR, Domain.create(ValueSet.ofRanges(
                                Range.lessThan(VARCHAR, utf8Slice("1")),
                                Range.range(VARCHAR, utf8Slice("2005-09-19"), true, utf8Slice("2005-09-1:"), false),
                                Range.range(VARCHAR, utf8Slice("2005-9-19"), true, utf8Slice("2005-9-1:"), false),
                                Range.greaterThan(VARCHAR, utf8Slice("9"))),
                        false)));

        // !=
        assertPredicateDerives(
                notEqual(cast(C_VARCHAR, DATE), new Constant(DATE, (long) DateTimeUtils.parseDate("2005-9-10"))),
                tupleDomain(C_VARCHAR, Domain.create(ValueSet.ofRanges(
                                Range.lessThan(VARCHAR, utf8Slice("2005-09-10")),
                                Range.range(VARCHAR, utf8Slice("2005-09-11"), true, utf8Slice("2005-9-10"), false),
                                Range.greaterThanOrEqual(VARCHAR, utf8Slice("2005-9-11"))),
                        false)));

        // != with single-digit day
        assertUnsupportedPredicate(
                notEqual(cast(C_VARCHAR, DATE), new Constant(DATE, (long) DateTimeUtils.parseDate("2005-9-2"))));
        // != with day ending with 9
        assertUnsupportedPredicate(
                notEqual(cast(C_VARCHAR, DATE), new Constant(DATE, (long) DateTimeUtils.parseDate("2005-09-09"))));
        assertPredicateDerives(
                notEqual(cast(C_VARCHAR, DATE), new Constant(DATE, (long) DateTimeUtils.parseDate("2005-09-19"))),
                tupleDomain(C_VARCHAR, Domain.create(ValueSet.ofRanges(
                                Range.lessThan(VARCHAR, utf8Slice("2005-09-19")),
                                Range.range(VARCHAR, utf8Slice("2005-09-1:"), true, utf8Slice("2005-9-19"), false),
                                Range.greaterThanOrEqual(VARCHAR, utf8Slice("2005-9-1:"))),
                        false)));

        // <
        assertPredicateDerives(
                lessThan(cast(C_VARCHAR, DATE), new Constant(DATE, (long) DateTimeUtils.parseDate("2005-9-10"))),
                tupleDomain(C_VARCHAR, Domain.create(ValueSet.ofRanges(
                                Range.lessThan(VARCHAR, utf8Slice("2006")),
                                Range.greaterThan(VARCHAR, utf8Slice("9"))),
                        false)));

        // >
        assertPredicateDerives(
                greaterThan(cast(C_VARCHAR, DATE), new Constant(DATE, (long) DateTimeUtils.parseDate("2005-9-10"))),
                tupleDomain(C_VARCHAR, Domain.create(ValueSet.ofRanges(
                                Range.lessThan(VARCHAR, utf8Slice("1")),
                                Range.greaterThan(VARCHAR, utf8Slice("2004"))),
                        false)));

        // Regression test for https://github.com/trinodb/trino/issues/14954
        assertPredicateTranslates(
                greaterThan(new Constant(DATE, (long) DateTimeUtils.parseDate("2001-01-31")), cast(C_VARCHAR, DATE)),
                tupleDomain(
                        C_VARCHAR,
                        Domain.create(ValueSet.ofRanges(
                                        Range.lessThan(VARCHAR, utf8Slice("2002")),
                                        Range.greaterThan(VARCHAR, utf8Slice("9"))),
                                false)),
                greaterThan(new Constant(DATE, (long) DateTimeUtils.parseDate("2001-01-31")), cast(C_VARCHAR, DATE)));

        // BETWEEN
        assertPredicateTranslates(
                between(cast(C_VARCHAR, DATE), new Constant(DATE, (long) DateTimeUtils.parseDate("2001-01-31")), new Constant(DATE, (long) DateTimeUtils.parseDate("2005-09-10"))),
                tupleDomain(C_VARCHAR, Domain.create(ValueSet.ofRanges(
                                Range.lessThan(VARCHAR, utf8Slice("1")),
                                Range.range(VARCHAR, utf8Slice("2000"), false, utf8Slice("2006"), false),
                                Range.greaterThan(VARCHAR, utf8Slice("9"))),
                        false)),
                and(
                        greaterThanOrEqual(cast(C_VARCHAR, DATE), new Constant(DATE, (long) DateTimeUtils.parseDate("2001-01-31"))),
                        lessThanOrEqual(cast(C_VARCHAR, DATE), new Constant(DATE, (long) DateTimeUtils.parseDate("2005-09-10")))));

        // Regression test for https://github.com/trinodb/trino/issues/14954
        assertPredicateTranslates(
                between(new Constant(DATE, (long) DateTimeUtils.parseDate("2001-01-31")), cast(C_VARCHAR, DATE), cast(C_VARCHAR_1, DATE)),
                tupleDomain(
                        C_VARCHAR,
                        Domain.create(ValueSet.ofRanges(
                                        Range.lessThan(VARCHAR, utf8Slice("2002")),
                                        Range.greaterThan(VARCHAR, utf8Slice("9"))),
                                false),
                        C_VARCHAR_1,
                        Domain.create(ValueSet.ofRanges(
                                        Range.lessThan(VARCHAR, utf8Slice("1")),
                                        Range.greaterThan(VARCHAR, utf8Slice("2000"))),
                                false)),
                and(
                        greaterThanOrEqual(new Constant(DATE, (long) DateTimeUtils.parseDate("2001-01-31")), cast(C_VARCHAR, DATE)),
                        lessThanOrEqual(new Constant(DATE, (long) DateTimeUtils.parseDate("2001-01-31")), cast(C_VARCHAR_1, DATE))));
    }

    @Test
    public void testFromUnprocessableInPredicate()
    {
        assertUnsupportedPredicate(new In(unprocessableExpression1(C_BIGINT), ImmutableList.of(TRUE)));
        assertUnsupportedPredicate(new In(C_BOOLEAN.toSymbolReference(), ImmutableList.of(unprocessableExpression1(C_BOOLEAN))));
        assertUnsupportedPredicate(
                new In(C_BOOLEAN.toSymbolReference(), ImmutableList.of(TRUE, unprocessableExpression1(C_BOOLEAN))));
        assertPredicateTranslates(
                not(new In(C_BOOLEAN.toSymbolReference(), ImmutableList.of(unprocessableExpression1(C_BOOLEAN)))),
                tupleDomain(C_BOOLEAN, Domain.notNull(BOOLEAN)),
                not(equal(C_BOOLEAN, unprocessableExpression1(C_BOOLEAN))));
    }

    @Test
    public void testInPredicateWithBoolean()
    {
        testInPredicate(C_BOOLEAN, C_BOOLEAN_1, BOOLEAN, false, true);
    }

    @Test
    public void testInPredicateWithInteger()
    {
        testInPredicate(C_INTEGER, C_INTEGER_1, INTEGER, 1L, 2L);
    }

    @Test
    public void testInPredicateWithBigint()
    {
        testInPredicate(C_BIGINT, C_BIGINT_1, BIGINT, 1L, 2L);
    }

    @Test
    public void testInPredicateWithReal()
    {
        testInPredicateWithFloatingPoint(C_REAL, C_REAL_1, REAL, toReal(1), toReal(2), toReal(Float.NaN));
    }

    @Test
    public void testInPredicateWithDouble()
    {
        testInPredicateWithFloatingPoint(C_DOUBLE, C_DOUBLE_1, DOUBLE, 1., 2., Double.NaN);
    }

    @Test
    public void testInPredicateWithShortDecimal()
    {
        testInPredicate(C_DECIMAL_6_1, C_DECIMAL_6_1_1, createDecimalType(6, 1), 10L, 20L);
    }

    @Test
    public void testInPredicateWithLongDecimal()
    {
        testInPredicate(
                C_DECIMAL_21_3,
                C_DECIMAL_21_3_1,
                createDecimalType(21, 3),
                Decimals.encodeScaledValue(ONE, 3),
                Decimals.encodeScaledValue(TWO, 3));
    }

    @Test
    public void testInPredicateWithVarchar()
    {
        testInPredicate(
                C_VARCHAR,
                C_VARCHAR_1,
                VARCHAR,
                utf8Slice("first"),
                utf8Slice("second"));
    }

    private void testInPredicate(Symbol symbol, Symbol symbol2, Type type, Object one, Object two)
    {
        Expression oneExpression = new Constant(type, one);
        Expression twoExpression = new Constant(type, two);
        Expression nullExpression = new Constant(type, null);
        Expression otherSymbol = symbol2.toSymbolReference();

        // IN, single value
        assertPredicateTranslates(
                in(symbol, List.of(oneExpression)),
                tupleDomain(symbol, Domain.singleValue(type, one)));

        // IN, two values
        assertPredicateTranslates(
                in(symbol, List.of(oneExpression, twoExpression)),
                tupleDomain(symbol, Domain.multipleValues(type, List.of(one, two))));

        // IN, with null
        assertPredicateIsAlwaysFalse(
                in(symbol, List.of(nullExpression)));
        assertPredicateTranslates(
                in(symbol, List.of(oneExpression, nullExpression, twoExpression)),
                tupleDomain(symbol, Domain.multipleValues(type, List.of(one, two))));

        // IN, with expression
        assertUnsupportedPredicate(
                in(symbol, List.of(otherSymbol)));
        assertUnsupportedPredicate(
                in(symbol, List.of(oneExpression, otherSymbol, twoExpression)));
        assertUnsupportedPredicate(
                in(symbol, List.of(oneExpression, otherSymbol, twoExpression, nullExpression)));

        // NOT IN, single value
        assertPredicateTranslates(
                not(in(symbol, List.of(oneExpression))),
                tupleDomain(symbol, Domain.create(ValueSet.ofRanges(Range.lessThan(type, one), Range.greaterThan(type, one)), false)));

        // NOT IN, two values
        assertPredicateTranslates(
                not(in(symbol, List.of(oneExpression, twoExpression))),
                tupleDomain(symbol, Domain.create(
                        ValueSet.ofRanges(
                                Range.lessThan(type, one),
                                Range.range(type, one, false, two, false),
                                Range.greaterThan(type, two)),
                        false)));

        // NOT IN, with null
        assertPredicateIsAlwaysFalse(
                not(in(symbol, List.of(nullExpression))));
        assertPredicateTranslates(
                not(in(symbol, List.of(oneExpression, nullExpression, twoExpression))),
                TupleDomain.none(),
                TRUE);

        // NOT IN, with expression
        assertPredicateTranslates(
                not(in(symbol, List.of(otherSymbol))),
                tupleDomain(symbol, Domain.notNull(type)),
                not(equal(symbol, otherSymbol)));
        assertPredicateTranslates(
                not(in(symbol, List.of(oneExpression, otherSymbol, twoExpression))),
                tupleDomain(symbol, Domain.create(
                        ValueSet.ofRanges(
                                Range.lessThan(type, one),
                                Range.range(type, one, false, two, false),
                                Range.greaterThan(type, two)),
                        false)),
                not(equal(symbol, otherSymbol)));
        assertPredicateIsAlwaysFalse(
                not(in(symbol, List.of(oneExpression, otherSymbol, twoExpression, nullExpression))));
    }

    private void testInPredicateWithFloatingPoint(Symbol symbol, Symbol symbol2, Type type, Object one, Object two, Object nan)
    {
        Expression oneExpression = new Constant(type, one);
        Expression twoExpression = new Constant(type, two);
        Expression nanExpression = new Constant(type, nan);
        Expression nullExpression = new Constant(type, null);
        Expression otherSymbol = symbol2.toSymbolReference();

        // IN, single value
        assertPredicateTranslates(
                in(symbol, List.of(oneExpression)),
                tupleDomain(symbol, Domain.singleValue(type, one)));

        // IN, two values
        assertPredicateTranslates(
                in(symbol, List.of(oneExpression, twoExpression)),
                tupleDomain(symbol, Domain.multipleValues(type, List.of(one, two))));

        // IN, with null
        assertPredicateIsAlwaysFalse(
                in(symbol, List.of(nullExpression)));
        assertPredicateTranslates(
                in(symbol, List.of(oneExpression, nullExpression, twoExpression)),
                tupleDomain(symbol, Domain.multipleValues(type, List.of(one, two))));

        // IN, with NaN
        assertPredicateIsAlwaysFalse(
                in(symbol, List.of(nanExpression)));
        assertPredicateTranslates(
                in(symbol, List.of(oneExpression, nanExpression, twoExpression)),
                tupleDomain(symbol, Domain.multipleValues(type, List.of(one, two))));

        // IN, with null and NaN
        assertPredicateIsAlwaysFalse(
                in(symbol, List.of(nanExpression, nullExpression)));
        assertPredicateTranslates(
                in(symbol, List.of(oneExpression, nanExpression, twoExpression, nullExpression)),
                tupleDomain(symbol, Domain.multipleValues(type, List.of(one, two))));

        // IN, with expression
        assertUnsupportedPredicate(
                in(symbol, List.of(otherSymbol)));
        assertUnsupportedPredicate(
                in(symbol, List.of(oneExpression, otherSymbol, twoExpression)));
        assertUnsupportedPredicate(
                in(symbol, List.of(oneExpression, otherSymbol, twoExpression, nanExpression)));
        assertUnsupportedPredicate(
                in(symbol, List.of(oneExpression, otherSymbol, twoExpression, nullExpression)));
        assertUnsupportedPredicate(
                in(symbol, List.of(oneExpression, otherSymbol, nanExpression, twoExpression, nullExpression)));

        // NOT IN, single value
        assertPredicateTranslates(
                not(in(symbol, List.of(oneExpression))),
                tupleDomain(symbol, Domain.notNull(type)),
                not(equal(symbol, oneExpression)));

        // NOT IN, two values
        assertPredicateTranslates(
                not(in(symbol, List.of(oneExpression, twoExpression))),
                tupleDomain(symbol, Domain.notNull(type)),
                not(in(symbol, List.of(oneExpression, twoExpression))));

        // NOT IN, with null
        assertPredicateIsAlwaysFalse(
                not(in(symbol, List.of(nullExpression))));
        assertPredicateIsAlwaysFalse(
                not(in(symbol, List.of(oneExpression, nullExpression, twoExpression))));

        // NOT IN, with NaN
        assertPredicateTranslates(
                not(in(symbol, List.of(nanExpression))),
                tupleDomain(symbol, Domain.notNull(type)));
        assertPredicateTranslates(
                not(in(symbol, List.of(oneExpression, nanExpression, twoExpression))),
                tupleDomain(symbol, Domain.notNull(type)),
                not(in(symbol, List.of(oneExpression, twoExpression))));

        // NOT IN, with null and NaN
        assertPredicateIsAlwaysFalse(
                not(in(symbol, List.of(nanExpression, nullExpression))));
        assertPredicateIsAlwaysFalse(
                not(in(symbol, List.of(oneExpression, nanExpression, twoExpression, nullExpression))));

        // NOT IN, with expression
        assertPredicateTranslates(
                not(in(symbol, List.of(otherSymbol))),
                tupleDomain(symbol, Domain.notNull(type)),
                not(equal(symbol, otherSymbol)));
        assertPredicateTranslates(
                not(in(symbol, List.of(oneExpression, otherSymbol, twoExpression))),
                tupleDomain(symbol, Domain.notNull(type)),
                not(in(symbol, List.of(oneExpression, otherSymbol, twoExpression))));
        assertPredicateTranslates(
                not(in(symbol, List.of(oneExpression, otherSymbol, twoExpression, nanExpression))),
                tupleDomain(symbol, Domain.notNull(type)),
                not(in(symbol, List.of(oneExpression, otherSymbol, twoExpression))));
        assertPredicateIsAlwaysFalse(
                not(in(symbol, List.of(oneExpression, otherSymbol, twoExpression, nullExpression))));
        assertPredicateIsAlwaysFalse(
                not(in(symbol, List.of(oneExpression, otherSymbol, nanExpression, twoExpression, nullExpression))));
    }

    @Test
    public void testInPredicateWithEquitableType()
    {
        assertPredicateTranslates(
                in(C_COLOR, ImmutableList.of(colorLiteral(COLOR_VALUE_1))),
                tupleDomain(C_COLOR, Domain.singleValue(COLOR, COLOR_VALUE_1)));

        assertPredicateTranslates(
                in(C_COLOR, ImmutableList.of(colorLiteral(COLOR_VALUE_1), colorLiteral(COLOR_VALUE_2))),
                tupleDomain(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1, COLOR_VALUE_2), false)));

        assertPredicateTranslates(
                not(in(C_COLOR, ImmutableList.of(colorLiteral(COLOR_VALUE_1), colorLiteral(COLOR_VALUE_2)))),
                tupleDomain(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1, COLOR_VALUE_2).complement(), false)));
    }

    @Test
    public void testFromInPredicateWithCastsAndNulls()
    {
        assertPredicateIsAlwaysFalse(new In(
                C_BIGINT.toSymbolReference(),
                ImmutableList.of(new Constant(BIGINT, null))));

        assertUnsupportedPredicate(not(new In(
                cast(C_SMALLINT, BIGINT),
                ImmutableList.of(new Constant(BIGINT, null)))));
    }

    @Test
    public void testFromBetweenPredicate()
    {
        assertPredicateTranslates(
                between(C_BIGINT, bigintLiteral(1L), bigintLiteral(2L)),
                tupleDomain(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 1L, true, 2L, true)), false)));

        assertPredicateIsAlwaysFalse(between(C_BIGINT, bigintLiteral(1L), nullLiteral(BIGINT)));

        // Test complements
        assertPredicateTranslates(
                not(between(C_BIGINT, bigintLiteral(1L), bigintLiteral(2L))),
                tupleDomain(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 1L), Range.greaterThan(BIGINT, 2L)), false)));

        assertPredicateTranslates(
                not(between(C_BIGINT, bigintLiteral(1L), nullLiteral(BIGINT))),
                tupleDomain(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 1L)), false)));
    }

    @Test
    public void testFromIsNullPredicate()
    {
        assertPredicateTranslates(
                isNull(C_BIGINT),
                tupleDomain(C_BIGINT, Domain.onlyNull(BIGINT)));

        assertPredicateTranslates(
                isNull(C_HYPER_LOG_LOG),
                tupleDomain(C_HYPER_LOG_LOG, Domain.onlyNull(HYPER_LOG_LOG)));

        assertPredicateTranslates(
                not(isNull(C_BIGINT)),
                tupleDomain(C_BIGINT, Domain.notNull(BIGINT)));

        assertPredicateTranslates(
                not(isNull(C_HYPER_LOG_LOG)),
                tupleDomain(C_HYPER_LOG_LOG, Domain.notNull(HYPER_LOG_LOG)));
    }

    @Test
    public void testFromIsNotNullPredicate()
    {
        assertPredicateTranslates(
                isNotNull(C_BIGINT),
                tupleDomain(C_BIGINT, Domain.notNull(BIGINT)));

        assertPredicateTranslates(
                isNotNull(C_HYPER_LOG_LOG),
                tupleDomain(C_HYPER_LOG_LOG, Domain.notNull(HYPER_LOG_LOG)));

        assertPredicateTranslates(
                not(isNotNull(C_BIGINT)),
                tupleDomain(C_BIGINT, Domain.onlyNull(BIGINT)));

        assertPredicateTranslates(
                not(isNotNull(C_HYPER_LOG_LOG)),
                tupleDomain(C_HYPER_LOG_LOG, Domain.onlyNull(HYPER_LOG_LOG)));
    }

    @Test
    public void testFromBooleanLiteralPredicate()
    {
        assertPredicateIsAlwaysTrue(TRUE);
        assertPredicateIsAlwaysFalse(not(TRUE));
        assertPredicateIsAlwaysFalse(FALSE);
        assertPredicateIsAlwaysTrue(not(FALSE));
    }

    @Test
    public void testFromNullLiteralPredicate()
    {
        assertPredicateIsAlwaysFalse(nullLiteral(BOOLEAN));
        assertPredicateIsAlwaysFalse(not(nullLiteral(BOOLEAN)));
    }

    @Test
    public void testConjunctExpression()
    {
        Expression expression = and(
                comparison(GREATER_THAN, C_DOUBLE.toSymbolReference(), doubleLiteral(0)),
                comparison(GREATER_THAN, C_BIGINT.toSymbolReference(), bigintLiteral(0)));
        assertPredicateTranslates(
                expression,
                tupleDomain(
                        C_DOUBLE, Domain.create(ValueSet.ofRanges(Range.greaterThan(DOUBLE, .0)), false),
                        C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 0L)), false)));

        assertThat(toPredicate(fromPredicate(expression).getTupleDomain())).isEqualTo(and(
                comparison(GREATER_THAN, C_BIGINT.toSymbolReference(), bigintLiteral(0)),
                comparison(GREATER_THAN, C_DOUBLE.toSymbolReference(), doubleLiteral(0))));
    }

    @Test
    public void testMultipleCoercionsOnSymbolSide()
    {
        assertPredicateTranslates(
                comparison(GREATER_THAN, cast(cast(C_SMALLINT, REAL), DOUBLE), doubleLiteral(3.7)),
                tupleDomain(C_SMALLINT, Domain.create(ValueSet.ofRanges(Range.greaterThan(SMALLINT, 3L)), false)));
    }

    @Test
    public void testLikePredicate()
    {
        Type varcharType = createUnboundedVarcharType();

        // constant
        testSimpleComparison(
                like(C_VARCHAR, "abc"),
                C_VARCHAR,
                Domain.multipleValues(varcharType, ImmutableList.of(utf8Slice("abc"))));

        // starts with pattern
        assertUnsupportedPredicate(like(C_VARCHAR, "_def"));
        assertUnsupportedPredicate(like(C_VARCHAR, "%def"));

        // _ pattern (unless escaped)
        testSimpleComparison(
                like(C_VARCHAR, "abc_def"),
                C_VARCHAR,
                like(C_VARCHAR, "abc_def"),
                Domain.create(ValueSet.ofRanges(Range.range(varcharType, utf8Slice("abc"), true, utf8Slice("abd"), false)), false));

        testSimpleComparison(
                like(C_VARCHAR, "abc\\_def"),
                C_VARCHAR,
                like(C_VARCHAR, "abc\\_def"),
                Domain.create(ValueSet.ofRanges(Range.range(varcharType, utf8Slice("abc\\"), true, utf8Slice("abc]"), false)), false));

        testSimpleComparison(
                like(C_VARCHAR, "abc\\_def", '\\'),
                C_VARCHAR,
                Domain.multipleValues(varcharType, ImmutableList.of(utf8Slice("abc_def"))));

        testSimpleComparison(
                like(C_VARCHAR, "abc\\_def_", '\\'),
                C_VARCHAR,
                like(C_VARCHAR, "abc\\_def_", '\\'),
                Domain.create(ValueSet.ofRanges(Range.range(varcharType, utf8Slice("abc_def"), true, utf8Slice("abc_deg"), false)), false));

        testSimpleComparison(
                like(C_VARCHAR, "abc^_def_", '^'),
                C_VARCHAR,
                like(C_VARCHAR, "abc^_def_", '^'),
                Domain.create(ValueSet.ofRanges(Range.range(varcharType, utf8Slice("abc_def"), true, utf8Slice("abc_deg"), false)), false));

        // % pattern (unless escaped)
        testSimpleComparison(
                like(C_VARCHAR, "abc%"),
                C_VARCHAR,
                like(C_VARCHAR, "abc%"),
                Domain.create(ValueSet.ofRanges(Range.range(varcharType, utf8Slice("abc"), true, utf8Slice("abd"), false)), false));

        testSimpleComparison(
                like(C_VARCHAR, "abc%def"),
                C_VARCHAR,
                like(C_VARCHAR, "abc%def"),
                Domain.create(ValueSet.ofRanges(Range.range(varcharType, utf8Slice("abc"), true, utf8Slice("abd"), false)), false));

        testSimpleComparison(
                like(C_VARCHAR, "abc\\%def"),
                C_VARCHAR,
                like(C_VARCHAR, "abc\\%def"),
                Domain.create(ValueSet.ofRanges(Range.range(varcharType, utf8Slice("abc\\"), true, utf8Slice("abc]"), false)), false));

        testSimpleComparison(
                like(C_VARCHAR, "abc\\%def", '\\'),
                C_VARCHAR,
                Domain.multipleValues(varcharType, ImmutableList.of(utf8Slice("abc%def"))));

        testSimpleComparison(
                like(C_VARCHAR, "abc\\%def_", '\\'),
                C_VARCHAR,
                like(C_VARCHAR, "abc\\%def_", '\\'),
                Domain.create(ValueSet.ofRanges(Range.range(varcharType, utf8Slice("abc%def"), true, utf8Slice("abc%deg"), false)), false));

        testSimpleComparison(
                like(C_VARCHAR, "abc^%def_", '^'),
                C_VARCHAR,
                like(C_VARCHAR, "abc^%def_", '^'),
                Domain.create(ValueSet.ofRanges(Range.range(varcharType, utf8Slice("abc%def"), true, utf8Slice("abc%deg"), false)), false));

        // non-ASCII literal
        testSimpleComparison(
                like(C_VARCHAR, "abc\u007f\u0123\udbfe"),
                C_VARCHAR,
                Domain.multipleValues(varcharType, ImmutableList.of(utf8Slice("abc\u007f\u0123\udbfe"))));

        // non-ASCII prefix
        testSimpleComparison(
                like(C_VARCHAR, "abc\u0123\ud83d\ude80def~\u007f\u00ff\u0123\uccf0%"),
                C_VARCHAR,
                like(C_VARCHAR, "abc\u0123\ud83d\ude80def~\u007f\u00ff\u0123\uccf0%"),
                Domain.create(
                        ValueSet.ofRanges(Range.range(varcharType,
                                utf8Slice("abc\u0123\ud83d\ude80def~\u007f\u00ff\u0123\uccf0"), true,
                                utf8Slice("abc\u0123\ud83d\ude80def\u007f"), false)),
                        false));

        // dynamic escape
        assertUnsupportedPredicate(like(C_VARCHAR, stringLiteral("abc\\_def"), C_VARCHAR_1.toSymbolReference()));

        // negation with literal
        testSimpleComparison(
                not(like(C_VARCHAR, "abcdef")),
                C_VARCHAR,
                Domain.create(ValueSet.ofRanges(
                                Range.lessThan(varcharType, utf8Slice("abcdef")),
                                Range.greaterThan(varcharType, utf8Slice("abcdef"))),
                        false));

        testSimpleComparison(
                not(like(C_VARCHAR, "abc\\_def", '\\')),
                C_VARCHAR,
                Domain.create(ValueSet.ofRanges(
                                Range.lessThan(varcharType, utf8Slice("abc_def")),
                                Range.greaterThan(varcharType, utf8Slice("abc_def"))),
                        false));

        // negation with pattern
        assertUnsupportedPredicate(not(like(C_VARCHAR, "abc\\_def")));
    }

    @Test
    public void testStartsWithFunction()
    {
        Type varcharType = createUnboundedVarcharType();

        // constant
        testSimpleComparison(
                startsWith(C_VARCHAR, stringLiteral("abc")),
                C_VARCHAR,
                startsWith(C_VARCHAR, stringLiteral("abc")),
                Domain.create(ValueSet.ofRanges(Range.range(varcharType, utf8Slice("abc"), true, utf8Slice("abd"), false)), false));

        testSimpleComparison(
                startsWith(C_VARCHAR, stringLiteral("_abc")),
                C_VARCHAR,
                startsWith(C_VARCHAR, stringLiteral("_abc")),
                Domain.create(ValueSet.ofRanges(Range.range(varcharType, utf8Slice("_abc"), true, utf8Slice("_abd"), false)), false));

        // empty
        assertUnsupportedPredicate(startsWith(C_VARCHAR, stringLiteral("")));
        // complement
        assertUnsupportedPredicate(not(startsWith(C_VARCHAR, stringLiteral("abc"))));

        // non-ASCII
        testSimpleComparison(
                startsWith(C_VARCHAR, stringLiteral("abc\u0123\ud83d\ude80def~\u007f\u00ff\u0123\uccf0")),
                C_VARCHAR,
                startsWith(C_VARCHAR, stringLiteral("abc\u0123\ud83d\ude80def~\u007f\u00ff\u0123\uccf0")),
                Domain.create(
                        ValueSet.ofRanges(Range.range(varcharType,
                                utf8Slice("abc\u0123\ud83d\ude80def~\u007f\u00ff\u0123\uccf0"), true,
                                utf8Slice("abc\u0123\ud83d\ude80def\u007f"), false)),
                        false));
    }

    @Test
    public void testUnsupportedFunctions()
    {
        assertUnsupportedPredicate(new Call(
                functionResolution.resolveFunction("length", fromTypes(VARCHAR)),
                ImmutableList.of(C_VARCHAR.toSymbolReference())));
        assertUnsupportedPredicate(new Call(
                functionResolution.resolveFunction("replace", fromTypes(VARCHAR, VARCHAR)),
                ImmutableList.of(C_VARCHAR.toSymbolReference(), stringLiteral("abc"))));
    }

    @Test
    public void testCharComparedToVarcharExpression()
    {
        Type charType = createCharType(10);
        // varchar literal is coerced to column (char) type
        testSimpleComparison(equal(C_CHAR, new Constant(charType, utf8Slice("abc"))), C_CHAR, Range.equal(charType, Slices.utf8Slice("abc")));

        // both sides got coerced to char(11)
        charType = createCharType(11);
        assertUnsupportedPredicate(equal(cast(C_CHAR, charType), cast(stringLiteral("abc12345678"), charType)));
    }

    private void assertPredicateIsAlwaysTrue(Expression expression)
    {
        assertPredicateTranslates(expression, TupleDomain.all(), TRUE);
    }

    private void assertPredicateIsAlwaysFalse(Expression expression)
    {
        assertPredicateTranslates(expression, TupleDomain.none(), TRUE);
    }

    private void assertUnsupportedPredicate(Expression expression)
    {
        assertPredicateTranslates(expression, TupleDomain.all(), expression);
    }

    private void assertPredicateTranslates(Expression expression, TupleDomain<Symbol> tupleDomain)
    {
        assertPredicateTranslates(expression, tupleDomain, TRUE);
    }

    private void assertPredicateDerives(Expression expression, TupleDomain<Symbol> tupleDomain)
    {
        assertPredicateTranslates(expression, tupleDomain, expression);
    }

    private void assertPredicateTranslates(Expression expression, TupleDomain<Symbol> tupleDomain, Expression remainingExpression)
    {
        ExtractionResult result = fromPredicate(expression);
        assertThat(result.getTupleDomain()).isEqualTo(tupleDomain);
        assertThat(result.getRemainingExpression()).isEqualTo(remainingExpression);
    }

    private void assertNoFullPushdown(Expression expression)
    {
        ExtractionResult result = fromPredicate(expression);
        assertThat(result.getRemainingExpression())
                .isNotEqualTo(TRUE);
    }

    private ExtractionResult fromPredicate(Expression originalPredicate)
    {
        return DomainTranslator.getExtractionResult(functionResolution.getPlannerContext(), TEST_SESSION, originalPredicate);
    }

    private Expression toPredicate(TupleDomain<Symbol> tupleDomain)
    {
        return DomainTranslator.toPredicate(tupleDomain);
    }

    private static Expression unprocessableExpression1(Symbol symbol)
    {
        return comparison(GREATER_THAN, symbol.toSymbolReference(), symbol.toSymbolReference());
    }

    private static Expression unprocessableExpression2(Symbol symbol)
    {
        return comparison(LESS_THAN, symbol.toSymbolReference(), symbol.toSymbolReference());
    }

    private Expression randPredicate(Symbol symbol, Type type)
    {
        Call rand = functionResolution
                .functionCallBuilder("rand")
                .build();
        return comparison(GREATER_THAN, symbol.toSymbolReference(), cast(rand, type));
    }

    private static Comparison equal(Symbol symbol, Expression expression)
    {
        return equal(symbol.toSymbolReference(), expression);
    }

    private static Comparison notEqual(Symbol symbol, Expression expression)
    {
        return notEqual(symbol.toSymbolReference(), expression);
    }

    private static Comparison greaterThan(Symbol symbol, Expression expression)
    {
        return greaterThan(symbol.toSymbolReference(), expression);
    }

    private static Comparison greaterThanOrEqual(Symbol symbol, Expression expression)
    {
        return greaterThanOrEqual(symbol.toSymbolReference(), expression);
    }

    private static Comparison lessThan(Symbol symbol, Expression expression)
    {
        return lessThan(symbol.toSymbolReference(), expression);
    }

    private static Comparison lessThanOrEqual(Symbol symbol, Expression expression)
    {
        return lessThanOrEqual(symbol.toSymbolReference(), expression);
    }

    private static Comparison isDistinctFrom(Symbol symbol, Expression expression)
    {
        return isDistinctFrom(symbol.toSymbolReference(), expression);
    }

    private Call like(Symbol symbol, String pattern)
    {
        return new Call(
                functionResolution.resolveFunction(LIKE_FUNCTION_NAME, fromTypes(VARCHAR, LikePatternType.LIKE_PATTERN)),
                ImmutableList.of(symbol.toSymbolReference(), new Constant(LikePatternType.LIKE_PATTERN, LikePattern.compile(pattern, Optional.empty()))));
    }

    private Call like(Symbol symbol, Expression pattern, Expression escape)
    {
        Call likePattern = new Call(
                functionResolution.resolveFunction(LIKE_PATTERN_FUNCTION_NAME, fromTypes(VARCHAR, VARCHAR)),
                ImmutableList.of(pattern, escape));
        return new Call(
                functionResolution.resolveFunction(LIKE_FUNCTION_NAME, fromTypes(VARCHAR, LikePatternType.LIKE_PATTERN)),
                ImmutableList.of(symbol.toSymbolReference(), likePattern));
    }

    private Call like(Symbol symbol, String pattern, Character escape)
    {
        return new Call(
                functionResolution.resolveFunction(LIKE_FUNCTION_NAME, fromTypes(VARCHAR, LikePatternType.LIKE_PATTERN)),
                ImmutableList.of(symbol.toSymbolReference(), new Constant(LikePatternType.LIKE_PATTERN, LikePattern.compile(pattern, Optional.of(escape)))));
    }

    private Call startsWith(Symbol symbol, Expression expression)
    {
        return new Call(
                functionResolution.resolveFunction("starts_with", fromTypes(VARCHAR, VARCHAR)),
                ImmutableList.of(symbol.toSymbolReference(), expression));
    }

    private static Expression isNotNull(Symbol symbol)
    {
        return isNotNull(symbol.toSymbolReference());
    }

    private static IsNull isNull(Symbol symbol)
    {
        return new IsNull(symbol.toSymbolReference());
    }

    private In in(Symbol symbol, List<?> values)
    {
        return in(symbol.toSymbolReference(), symbol.type(), values);
    }

    private static Between between(Symbol symbol, Expression min, Expression max)
    {
        return new Between(symbol.toSymbolReference(), min, max);
    }

    private static Expression isNotNull(Expression expression)
    {
        return new Not(new IsNull(expression));
    }

    private In in(Expression expression, Type type, List<?> values)
    {
        return new In(
                expression,
                values.stream()
                        .map(value -> value instanceof Expression valueExpression ?
                                valueExpression :
                                new Constant(type, value))
                        .collect(toImmutableList()));
    }

    private static Between between(Expression expression, Expression min, Expression max)
    {
        return new Between(expression, min, max);
    }

    private static Comparison equal(Expression left, Expression right)
    {
        return comparison(EQUAL, left, right);
    }

    private static Comparison notEqual(Expression left, Expression right)
    {
        return comparison(NOT_EQUAL, left, right);
    }

    private static Comparison greaterThan(Expression left, Expression right)
    {
        return comparison(GREATER_THAN, left, right);
    }

    private static Comparison greaterThanOrEqual(Expression left, Expression right)
    {
        return comparison(GREATER_THAN_OR_EQUAL, left, right);
    }

    private static Comparison lessThan(Expression left, Expression expression)
    {
        return comparison(LESS_THAN, left, expression);
    }

    private static Comparison lessThanOrEqual(Expression left, Expression right)
    {
        return comparison(LESS_THAN_OR_EQUAL, left, right);
    }

    private static Comparison isDistinctFrom(Expression left, Expression right)
    {
        return comparison(IS_DISTINCT_FROM, left, right);
    }

    private static Not not(Expression expression)
    {
        return new Not(expression);
    }

    private static Comparison comparison(Comparison.Operator operator, Expression expression1, Expression expression2)
    {
        return new Comparison(operator, expression1, expression2);
    }

    private static Constant bigintLiteral(long value)
    {
        return new Constant(BIGINT, value);
    }

    private static Constant doubleLiteral(double value)
    {
        return new Constant(DOUBLE, value);
    }

    private static Expression realLiteral(float value)
    {
        return new Constant(REAL, toReal(value));
    }

    private static Constant stringLiteral(String value)
    {
        return new Constant(VARCHAR, utf8Slice(value));
    }

    private static Expression nullLiteral(Type type)
    {
        return new Constant(type, null);
    }

    private static Expression cast(Symbol symbol, Type type)
    {
        return cast(symbol.toSymbolReference(), type);
    }

    private static Expression cast(Expression expression, Type type)
    {
        return new Cast(expression, type);
    }

    private Expression colorLiteral(long value)
    {
        return new Constant(COLOR, value);
    }

    private void testSimpleComparison(Expression expression, Symbol symbol, Range expectedDomainRange)
    {
        testSimpleComparison(expression, symbol, Domain.create(ValueSet.ofRanges(expectedDomainRange), false));
    }

    private void testSimpleComparison(Expression expression, Symbol symbol, Domain expectedDomain)
    {
        testSimpleComparison(expression, symbol, TRUE, expectedDomain);
    }

    private void testSimpleComparison(Expression expression, Symbol symbol, Expression expectedRemainingExpression, Domain expectedDomain)
    {
        ExtractionResult result = fromPredicate(expression);
        assertThat(result.getRemainingExpression()).isEqualTo(expectedRemainingExpression);
        TupleDomain<Symbol> actual = result.getTupleDomain();
        TupleDomain<Symbol> expected = tupleDomain(symbol, expectedDomain);
        if (!actual.equals(expected)) {
            fail(format("for comparison [%s] expected [%s] but found [%s]", expression.toString(), expected.toString(SESSION), actual.toString(SESSION)));
        }
    }

    private static <T> TupleDomain<T> tupleDomain(T key, Domain domain)
    {
        return tupleDomain(Map.of(key, domain));
    }

    private static <T> TupleDomain<T> tupleDomain(T key1, Domain domain1, T key2, Domain domain2)
    {
        return tupleDomain(Map.of(key1, domain1, key2, domain2));
    }

    private static <T> TupleDomain<T> tupleDomain(Map<T, Domain> domains)
    {
        return TupleDomain.withColumnDomains(domains);
    }

    private static class NumericValues<T>
    {
        private final Symbol column;
        private final Type type;
        private final T min;
        private final T integerNegative;
        private final T fractionalNegative;
        private final T integerPositive;
        private final T fractionalPositive;
        private final T max;

        private NumericValues(Symbol column, T min, T integerNegative, T fractionalNegative, T integerPositive, T fractionalPositive, T max)
        {
            this.column = requireNonNull(column, "column is null");
            this.type = requireNonNull(column.type(), "type for column not found: " + column);
            this.min = requireNonNull(min, "min is null");
            this.integerNegative = requireNonNull(integerNegative, "integerNegative is null");
            this.fractionalNegative = requireNonNull(fractionalNegative, "fractionalNegative is null");
            this.integerPositive = requireNonNull(integerPositive, "integerPositive is null");
            this.fractionalPositive = requireNonNull(fractionalPositive, "fractionalPositive is null");
            this.max = requireNonNull(max, "max is null");
        }

        public Symbol getColumn()
        {
            return column;
        }

        public Type getType()
        {
            return type;
        }

        public T getMin()
        {
            return min;
        }

        public T getIntegerNegative()
        {
            return integerNegative;
        }

        public T getFractionalNegative()
        {
            return fractionalNegative;
        }

        public T getIntegerPositive()
        {
            return integerPositive;
        }

        public T getFractionalPositive()
        {
            return fractionalPositive;
        }

        public T getMax()
        {
            return max;
        }

        public boolean isFractional()
        {
            return type == DOUBLE || type == REAL || (type instanceof DecimalType && ((DecimalType) type).getScale() > 0);
        }

        public boolean isTypeWithNaN()
        {
            return type instanceof DoubleType || type instanceof RealType;
        }
    }
}
