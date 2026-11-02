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
import io.trino.Session;
import io.trino.connector.CatalogServiceProvider;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.FunctionPreimages;
import io.trino.metadata.GlobalFunctionCatalog;
import io.trino.metadata.LanguageFunctionProvider;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.function.DomainProjection;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.ScalarFunctionImplementation;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;
import io.trino.sql.InterpretedFunctionInvoker;
import io.trino.tracing.TracingMetadata;
import io.trino.type.CharVarcharCoercion;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.tracing.Tracing.noopTracer;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.SystemSessionProperties.LEGACY_VARCHAR_TO_CHAR_COERCION;
import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.spi.predicate.Domain.multipleValues;
import static io.trino.spi.predicate.Range.greaterThan;
import static io.trino.spi.predicate.Range.greaterThanOrEqual;
import static io.trino.spi.predicate.Range.lessThan;
import static io.trino.spi.predicate.Range.lessThanOrEqual;
import static io.trino.spi.predicate.Range.range;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.type.Reals.toReal;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDomainPreimages
{
    @Test
    public void testNone()
    {
        assertThat(castPreimage(Domain.none(BIGINT), INTEGER)).isEqualTo(Domain.none(INTEGER));
    }

    @Test
    public void testAll()
    {
        assertThat(castPreimage(Domain.all(BIGINT), INTEGER)).isEqualTo(Domain.all(INTEGER));
    }

    @Test
    public void testOnlyNull()
    {
        assertThat(castPreimage(Domain.onlyNull(BIGINT), INTEGER)).isEqualTo(Domain.onlyNull(INTEGER));
    }

    @Test
    public void testCoercedValueSameAsOriginal()
    {
        assertThat(castPreimage(multipleValues(BIGINT, ImmutableList.of(1L, 10000L, -2000L)), SMALLINT)).isEqualTo(multipleValues(SMALLINT, ImmutableList.of(1L, 10000L, -2000L)));

        Domain original = Domain.create(
                ValueSet.ofRanges(
                        lessThan(DOUBLE, 0.0),
                        range(DOUBLE, 0.0, false, 1.0, false),
                        range(DOUBLE, 2.0, true, 3.0, true),
                        greaterThan(DOUBLE, 4.0)),
                true);
        assertThat(castPreimage(original, REAL)).isEqualTo(Domain.create(
                ValueSet.ofRanges(
                        range(REAL, toReal(Float.NEGATIVE_INFINITY), true, toReal(0.0f), false),
                        range(REAL, toReal(0.0f), false, toReal(1.0f), false),
                        range(REAL, toReal(2.0f), true, toReal(3.0f), true),
                        range(REAL, toReal(4.0f), false, toReal(Float.POSITIVE_INFINITY), true)),
                true));
    }

    @Test
    public void testOutsideTargetTypeRange()
    {
        assertThat(castPreimage(multipleValues(BIGINT, ImmutableList.of(1L, 10000000000L, -2000L)), SMALLINT)).isEqualTo(multipleValues(SMALLINT, ImmutableList.of(1L, -2000L)));

        assertThat(castPreimage(
                Domain.create(
                        ValueSet.ofRanges(range(DOUBLE, 0.0, true, ((double) Float.MAX_VALUE) * 10, true)),
                        true),
                REAL)).isEqualTo(Domain.create(
                ValueSet.ofRanges(range(REAL, toReal(0.0f), true, toReal(Float.POSITIVE_INFINITY), false)),
                true));

        // low below and high above target type range
        assertThat(castPreimage(
                Domain.create(
                        ValueSet.ofRanges(
                                range(DOUBLE, ((double) Float.MAX_VALUE) * -2, true, ((double) Float.MAX_VALUE) * 10, true)),
                        true),
                REAL)).isEqualTo(Domain.create(ValueSet.ofRanges(range(REAL, toReal(Float.NEGATIVE_INFINITY), false, toReal(Float.POSITIVE_INFINITY), false)), true));

        assertThat(castPreimage(
                Domain.create(
                        ValueSet.ofRanges(
                                range(DOUBLE, Double.NEGATIVE_INFINITY, true, Double.POSITIVE_INFINITY, true)),
                        true),
                REAL)).isEqualTo(Domain.create(
                ValueSet.ofRanges(
                        range(REAL, toReal(Float.NEGATIVE_INFINITY), true, toReal(Float.POSITIVE_INFINITY), true)),
                true));

        assertThat(castPreimage(
                Domain.create(
                        ValueSet.ofRanges(
                                range(BIGINT, ((long) Integer.MAX_VALUE) * -2, false, ((long) Integer.MAX_VALUE) * 10, false)),
                        true),
                INTEGER)).isEqualTo(Domain.create(ValueSet.all(INTEGER), true));

        assertThat(castPreimage(
                Domain.create(
                        ValueSet.ofRanges(
                                range(DOUBLE, Double.NEGATIVE_INFINITY, true, Double.POSITIVE_INFINITY, true)),
                        true),
                INTEGER)).isEqualTo(Domain.create(ValueSet.all(INTEGER), true));

        // Low and high below target type range
        assertThat(castPreimage(
                Domain.create(
                        ValueSet.ofRanges(
                                range(BIGINT, ((long) Integer.MAX_VALUE) * -4, false, ((long) Integer.MAX_VALUE) * -2, false)),
                        false),
                INTEGER)).isEqualTo(Domain.none(INTEGER));

        assertThat(castPreimage(
                Domain.create(
                        ValueSet.ofRanges(
                                range(DOUBLE, ((double) Float.MAX_VALUE) * -4, true, ((double) Float.MAX_VALUE) * -2, true)),
                        true),
                REAL)).isEqualTo(Domain.onlyNull(REAL));

        // Low and high above target type range
        assertThat(castPreimage(
                Domain.create(
                        ValueSet.ofRanges(
                                range(BIGINT, ((long) Integer.MAX_VALUE) * 2, false, ((long) Integer.MAX_VALUE) * 4, false)),
                        false),
                INTEGER)).isEqualTo(Domain.none(INTEGER));

        assertThat(castPreimage(
                Domain.create(
                        ValueSet.ofRanges(
                                range(DOUBLE, ((double) Float.MAX_VALUE) * 2, true, ((double) Float.MAX_VALUE) * 4, true)),
                        true),
                REAL)).isEqualTo(Domain.onlyNull(REAL));

        // A finite lower bound excludes negative infinity and NaN.
        assertThat(castPreimage(
                Domain.create(
                        ValueSet.ofRanges(
                                greaterThanOrEqual(DOUBLE, ((double) Float.MAX_VALUE) * -4),
                                range(DOUBLE, 0.0, true, 1.0, true)),
                        true),
                REAL)).isEqualTo(Domain.create(ValueSet.ofRanges(range(REAL, toReal(Float.NEGATIVE_INFINITY), false, toReal(Float.POSITIVE_INFINITY), true)), true));
    }

    @Test
    public void testTruncatedCoercedValue()
    {
        assertThat(castPreimage(
                Domain.create(
                        ValueSet.ofRanges(
                                range(createDecimalType(6, 3), 123456L, true, 234567L, false)),
                        true),
                createDecimalType(6, 1))).isEqualTo(Domain.create(
                ValueSet.ofRanges(range(createDecimalType(6, 1), 1235L, true, 2346L, false)),
                true));
    }

    @Test
    public void testIntegerRoundingFibers()
    {
        for (Type inputType : List.of(INTEGER, BIGINT)) {
            for (Type resultType : List.of(REAL, DOUBLE)) {
                long threshold = resultType.equals(REAL) || inputType.equals(INTEGER) ? 1L << 24 : 1L << 53;
                Type.Range bounds = inputType.getRange().orElseThrow();
                for (long boundary : List.of(-threshold, threshold, (long) bounds.getMin(), (long) bounds.getMax())) {
                    Object value = floatingValue(resultType, boundary);
                    for (Domain result : boundaryDomains(resultType, value)) {
                        Domain input = castPreimage(result, inputType);
                        for (long offset = -4; offset <= 4; offset++) {
                            BigInteger candidate = BigInteger.valueOf(boundary).add(BigInteger.valueOf(offset));
                            if (candidate.compareTo(BigInteger.valueOf((long) bounds.getMin())) < 0 || candidate.compareTo(BigInteger.valueOf((long) bounds.getMax())) > 0) {
                                continue;
                            }
                            assertThat(input.includesNullableValue(candidate.longValueExact()))
                                    .describedAs("%s through %s -> %s for %s", result, inputType, resultType, candidate)
                                    .isEqualTo(result.includesNullableValue(floatingValue(resultType, candidate.longValueExact())));
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testDecimalBoundaries()
    {
        // Exercise both native decimal representations, widening scale, reduced integer
        // precision, and the inverse direction of the retired decimal-to-integer operator.
        for (List<Type> types : List.<List<Type>>of(
                List.of(createDecimalType(6, 1), createDecimalType(9, 3)),
                List.of(createDecimalType(6, 1), createDecimalType(22, 3)),
                List.of(createDecimalType(19, 2), createDecimalType(22, 4)),
                List.of(createDecimalType(19, 2), createDecimalType(18, 2)),
                List.of(INTEGER, createDecimalType(12, 2)),
                List.of(BIGINT, createDecimalType(38, 3)))) {
            Type source = types.getFirst();
            DecimalType target = (DecimalType) types.getLast();
            var cast = PLANNER_CONTEXT.getMetadata().getCoercion(getCharVarcharCoercion(TEST_SESSION), source, target);
            var invoker = new InterpretedFunctionInvoker(PLANNER_CONTEXT.getFunctionManager());
            List<Object> inputs = new ArrayList<>();
            for (long value : List.of(-10001L, -10000L, -9999L, -1L, 0L, 1L, 9999L, 10000L, 10001L)) {
                inputs.add(source instanceof DecimalType decimal && !decimal.isShort() ? Int128.valueOf(value) : value);
            }
            for (Object input : inputs) {
                Object output = invoker.invoke(cast, TEST_SESSION.toConnectorSession(), input);
                BigInteger unscaled = target.isShort() ? BigInteger.valueOf((long) output) : ((Int128) output).toBigInteger();
                for (long offset : List.of(-1L, 0L, 1L)) {
                    BigInteger boundary = unscaled.add(BigInteger.valueOf(offset));
                    Object value = target.isShort() ? boundary.longValueExact() : Int128.valueOf(boundary);
                    for (Domain result : boundaryDomains(target, value)) {
                        Domain projected = castPreimage(result, source);
                        assertThat(projected.isNullAllowed()).isEqualTo(result.isNullAllowed());
                        for (Object candidate : inputs) {
                            assertThat(projected.includesNullableValue(candidate))
                                    .describedAs("%s through %s -> %s for %s", result, source, target, candidate)
                                    .isEqualTo(result.includesNullableValue(invoker.invoke(cast, TEST_SESSION.toConnectorSession(), candidate)));
                        }
                    }
                }
            }
        }
    }

    private static List<Domain> boundaryDomains(Type type, Object value)
    {
        return List.of(
                Domain.singleValue(type, value),
                Domain.create(ValueSet.ofRanges(lessThan(type, value)), true),
                Domain.create(ValueSet.ofRanges(lessThanOrEqual(type, value)), false),
                Domain.create(ValueSet.ofRanges(greaterThan(type, value)), false),
                Domain.create(ValueSet.ofRanges(greaterThanOrEqual(type, value)), true));
    }

    @Test
    public void testFloatingPointInfinities()
    {
        for (double value : List.of(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY)) {
            assertThat(castPreimage(Domain.singleValue(DOUBLE, value), REAL))
                    .isEqualTo(Domain.singleValue(REAL, toReal((float) value)));
        }
    }

    @Test
    public void testRealPointAndRangeMembership()
    {
        List<Double> values = List.of(
                Double.NEGATIVE_INFINITY,
                -Double.MAX_VALUE,
                (double) -Float.MAX_VALUE,
                -1.1,
                -1.0,
                -Double.MIN_VALUE,
                -0.0,
                0.0,
                Double.MIN_VALUE,
                (double) Float.MIN_VALUE,
                1.0,
                1.1,
                (double) Float.MAX_VALUE,
                Double.MAX_VALUE,
                Double.POSITIVE_INFINITY);
        List<Float> inputs = List.of(
                Float.NEGATIVE_INFINITY,
                -Float.MAX_VALUE,
                -1.1f,
                -1.0f,
                -Float.MIN_VALUE,
                -0.0f,
                0.0f,
                Float.MIN_VALUE,
                1.0f,
                1.1f,
                Float.MAX_VALUE,
                Float.POSITIVE_INFINITY,
                Float.NaN);
        List<Domain> domains = new ArrayList<>();
        domains.add(Domain.all(DOUBLE));
        domains.add(Domain.none(DOUBLE));
        domains.add(Domain.onlyNull(DOUBLE));
        domains.add(multipleValues(DOUBLE, values, true));
        for (double value : values) {
            domains.addAll(boundaryDomains(DOUBLE, value));
        }
        for (Domain domain : domains) {
            for (Domain result : List.of(domain, domain.complement())) {
                Domain projected = castPreimage(result, REAL);
                assertThat(projected.isNullAllowed()).isEqualTo(result.isNullAllowed());
                for (float input : inputs) {
                    assertThat(projected.includesNullableValue(toReal(input)))
                            .describedAs("%s through REAL -> DOUBLE for %s", result, input)
                            .isEqualTo(result.includesNullableValue((double) input));
                }
            }
        }
    }

    @Test
    public void testLargeRealPointDomain()
    {
        List<Object> resultValues = new ArrayList<>();
        List<Object> inputValues = new ArrayList<>();
        for (int value = 0; value < 10_000; value++) {
            resultValues.add((double) value);
            resultValues.add(value + 0.1);
            inputValues.add(toReal(value));
        }
        for (boolean nullAllowed : List.of(false, true)) {
            assertThat(castPreimage(multipleValues(DOUBLE, resultValues, nullAllowed), REAL))
                    .isEqualTo(multipleValues(REAL, inputValues, nullAllowed));
        }
    }

    private static Object floatingValue(Type type, long value)
    {
        if (type.equals(REAL)) {
            return toReal((float) value);
        }
        return (double) value;
    }

    @Test
    public void testNaNDomains()
    {
        for (boolean nullAllowed : List.of(false, true)) {
            Domain nan = Domain.singleValue(DOUBLE, Double.NaN, nullAllowed);
            Domain realNaN = Domain.singleValue(REAL, toReal(Float.NaN), nullAllowed);
            assertThat(castPreimage(nan, REAL)).isEqualTo(realNaN);
            assertThat(castPreimage(nan.complement(), REAL)).isEqualTo(realNaN.complement());
            Domain mixed = nan.union(Domain.create(ValueSet.ofRanges(range(DOUBLE, 1.0, true, 3.0, true)), nullAllowed));
            Domain expected = realNaN.union(Domain.create(ValueSet.ofRanges(range(REAL, toReal(1), true, toReal(3), true)), nullAllowed));
            assertThat(castPreimage(mixed, REAL)).isEqualTo(expected);
            assertThat(castPreimage(mixed.complement(), REAL)).isEqualTo(expected.complement());
            for (Type integral : List.of(INTEGER, BIGINT)) {
                assertThat(castPreimage(nan, integral)).isEqualTo(nullAllowed ? Domain.onlyNull(integral) : Domain.none(integral));
                assertThat(castPreimage(mixed, integral)).isEqualTo(Domain.create(ValueSet.ofRanges(range(integral, 1L, true, 3L, true)), nullAllowed));
            }
        }
    }

    @Test
    public void testLargeNumericDomain()
    {
        List<Object> resultValues = new ArrayList<>();
        List<Object> inputValues = new ArrayList<>();
        for (int i = 0; i < 20_000; i++) {
            long value = 2L * i;
            resultValues.add((double) value);
            inputValues.add(value);
        }
        resultValues.add(Double.MAX_VALUE);
        assertThat(castPreimage(multipleValues(DOUBLE, resultValues, true), INTEGER))
                .isEqualTo(multipleValues(INTEGER, inputValues, true));
    }

    @Test
    public void testLargeCharacterDomains()
    {
        List<Object> values = new ArrayList<>();
        for (int i = 0; i < 20_000; i++) {
            values.add(utf8Slice(Integer.toString(10_000 + i)));
        }
        Type source = createCharType(5);
        Domain result = multipleValues(createVarcharType(5), values, true);
        Domain expected = multipleValues(source, values, true);
        Session legacySession = Session.builder(TEST_SESSION)
                .setSystemProperty(LEGACY_VARCHAR_TO_CHAR_COERCION, "true")
                .build();
        for (Session session : List.of(TEST_SESSION, legacySession)) {
            assertThat(castPreimage(session, result, source)).isEqualTo(expected);
            assertThat(castPreimage(session, result.complement(), source)).isEqualTo(expected.complement());
        }
    }

    @Test
    public void testUnsupportedCast()
    {
        assertThat(castPreimage(Domain.singleValue(INTEGER, 10L), BIGINT)).isEqualTo(Domain.all(BIGINT));
    }

    @Test
    public void testVarcharToChar()
    {
        Domain values = multipleValues(createVarcharType(10), ImmutableList.of(utf8Slice("I"), utf8Slice("P")));
        assertThat(castPreimage(values, createCharType(10))).isEqualTo(multipleValues(createCharType(10), ImmutableList.of(utf8Slice("I"), utf8Slice("P"))));
        // Ordering through the cast is not order-preserving, so the preimage is a superset bounded by the char floor
        assertThat(castPreimage(Domain.create(ValueSet.ofRanges(lessThan(createVarcharType(10), utf8Slice("P"))), false), createCharType(10)))
                .isEqualTo(Domain.create(ValueSet.ofRanges(lessThan(createCharType(10), utf8Slice("P"))), false));
        // A value extending the bound with a character below the space sorts below it as a char
        assertThat(castPreimage(Domain.create(ValueSet.ofRanges(greaterThan(createVarcharType(10), utf8Slice("abc"))), false), createCharType(5)))
                .isEqualTo(Domain.create(ValueSet.ofRanges(greaterThan(createCharType(5), utf8Slice("abb"))), false));
    }

    @Test
    public void testTruncatingVarcharToChar()
    {
        // CAST(varchar(10) AS char(3)) truncates, so every string mapped to a value starts with it
        assertThat(castPreimage(Domain.singleValue(createCharType(3), utf8Slice("ab")), createVarcharType(10)))
                .isEqualTo(Domain.create(ValueSet.ofRanges(range(createVarcharType(10), utf8Slice("ab"), true, utf8Slice("ac"), false)), false));
        // A value longer than the source cannot be produced
        assertThat(castPreimage(Domain.singleValue(createCharType(5), utf8Slice("abcde")), createVarcharType(3)))
                .isEqualTo(Domain.none(createVarcharType(3)));
    }

    @Test
    public void testVarcharToDate()
    {
        Type varchar = createVarcharType(20);
        long date = LocalDate.of(2005, 9, 10).toEpochDay();
        // The common spellings of one date, plus strings starting with whitespace, a sign, or a leading zero
        assertThat(castPreimage(Domain.singleValue(DATE, date), varchar)).isEqualTo(Domain.create(ValueSet.ofRanges(
                lessThan(varchar, utf8Slice("1")),
                range(varchar, utf8Slice("2005-09-10"), true, utf8Slice("2005-09-11"), false),
                range(varchar, utf8Slice("2005-9-10"), true, utf8Slice("2005-9-11"), false),
                greaterThanOrEqual(varchar, utf8Slice(":"))), false));

        // A bounded range includes the strings of every year it touches
        Domain decade = castPreimage(Domain.create(ValueSet.ofRanges(range(DATE, LocalDate.of(2000, 1, 1).toEpochDay(), true, LocalDate.of(2010, 12, 31).toEpochDay(), true)), false), varchar);
        assertThat(decade).isEqualTo(Domain.create(ValueSet.ofRanges(
                lessThan(varchar, utf8Slice("1")),
                range(varchar, utf8Slice("2000-"), true, utf8Slice("2010."), false),
                greaterThanOrEqual(varchar, utf8Slice(":"))), false));
        // Years of different lengths sort apart, so a range across lengths includes each of them
        Domain acrossLengths = castPreimage(Domain.create(ValueSet.ofRanges(range(DATE, LocalDate.of(950, 1, 1).toEpochDay(), true, LocalDate.of(1005, 1, 1).toEpochDay(), true)), false), varchar);
        assertThat(acrossLengths.includesNullableValue(utf8Slice("999-12-31"))).isTrue();
        assertThat(acrossLengths.includesNullableValue(utf8Slice("1004-06-07"))).isTrue();
        assertThat(acrossLengths.includesNullableValue(utf8Slice("1999-01-01"))).isFalse();

        // String order does not follow year order across lengths: '305-01-01' sorts between '2299' and '2301'.
        // A range unbounded on either side would include nearly every string, so the provider declines it.
        assertThat(castPreimage(Domain.create(ValueSet.ofRanges(lessThan(DATE, LocalDate.of(2300, 1, 1).toEpochDay())), false), varchar))
                .isEqualTo(Domain.all(varchar));
        assertThat(castPreimage(Domain.create(ValueSet.ofRanges(greaterThan(DATE, LocalDate.of(2300, 1, 1).toEpochDay())), false), varchar))
                .isEqualTo(Domain.all(varchar));

        // Excluding a date removes only its spellings that cannot cast to another date
        Domain excluded = castPreimage(Domain.create(ValueSet.of(DATE, date).complement(), false), varchar);
        assertThat(excluded.includesNullableValue(utf8Slice("2005-09-10"))).isFalse();
        assertThat(excluded.includesNullableValue(utf8Slice("2005-09-11"))).isTrue();
        // A one-digit day is a prefix of other days, so it cannot be excluded
        long singleDigitDay = LocalDate.of(2005, 9, 1).toEpochDay();
        Domain excludedSingleDigit = castPreimage(Domain.create(ValueSet.of(DATE, singleDigitDay).complement(), false), varchar);
        assertThat(excludedSingleDigit.includesNullableValue(utf8Slice("2005-9-1"))).isTrue();
        assertThat(excludedSingleDigit.includesNullableValue(utf8Slice("2005-09-01"))).isFalse();

        // An excluded spelling longer than the source type is skipped: shortened to fit, it would also remove
        // '2005-09-1', which casts to 2005-09-01
        Domain excludedShort = castPreimage(Domain.create(ValueSet.of(DATE, date).complement(), false), createVarcharType(9));
        assertThat(excludedShort.includesNullableValue(utf8Slice("2005-09-1"))).isTrue();
        assertThat(excludedShort.includesNullableValue(utf8Slice("2005-9-10"))).isFalse();

        // The only value of varchar(0) never casts successfully, so only null membership remains
        for (Domain domain : List.of(
                Domain.singleValue(DATE, date),
                Domain.create(ValueSet.of(DATE, date), true),
                Domain.create(ValueSet.of(DATE, date).complement(), false),
                Domain.create(ValueSet.ofRanges(range(DATE, date, true, date + 400, true)), true))) {
            assertThat(castPreimage(domain, createVarcharType(0))).isEqualTo(Domain.create(ValueSet.none(createVarcharType(0)), domain.isNullAllowed()));
        }

        // A bound longer than the source type keeps every value of the type in range
        assertThat(castPreimage(Domain.singleValue(DATE, date), createVarcharType(9)).includesNullableValue(utf8Slice("2005-9-10"))).isTrue();
    }

    @Test
    public void testShapesResolveOnce()
    {
        AtomicInteger implementations = new AtomicInteger();
        AtomicInteger coercions = new AtomicInteger();
        AtomicInteger projections = new AtomicInteger();
        FunctionManager delegate = PLANNER_CONTEXT.getFunctionManager();
        GlobalFunctionCatalog unusedCatalog = new GlobalFunctionCatalog(
                () -> { throw new UnsupportedOperationException(); },
                () -> { throw new UnsupportedOperationException(); },
                () -> { throw new UnsupportedOperationException(); });
        FunctionManager functionManager = new FunctionManager(CatalogServiceProvider.fail(), unusedCatalog, LanguageFunctionProvider.DISABLED)
        {
            @Override
            public ScalarFunctionImplementation getScalarFunctionImplementation(ResolvedFunction function, InvocationConvention convention)
            {
                implementations.incrementAndGet();
                return delegate.getScalarFunctionImplementation(function, convention);
            }
        };
        Metadata metadata = new TracingMetadata(noopTracer(), PLANNER_CONTEXT.getMetadata())
        {
            @Override
            public ResolvedFunction getCoercion(CharVarcharCoercion charVarcharCoercion, Type fromType, Type toType)
            {
                coercions.incrementAndGet();
                return super.getCoercion(charVarcharCoercion, fromType, toType);
            }

            @Override
            public Optional<DomainProjection> getDomainProjection(Session session, ResolvedFunction function)
            {
                projections.incrementAndGet();
                return super.getDomainProjection(session, function);
            }
        };
        FunctionPreimages preimages = new FunctionPreimages(metadata, functionManager, PLANNER_CONTEXT.getTypeManager());

        for (long value = 0; value < 10; value++) {
            Domain domain = Domain.create(ValueSet.ofRanges(greaterThan(DOUBLE, value + 0.5)), false);
            assertThat(preimages.castPreimage(TEST_SESSION, domain, BIGINT))
                    .isEqualTo(Domain.create(ValueSet.ofRanges(greaterThanOrEqual(BIGINT, value + 1)), false));
        }
        int resolvedImplementations = implementations.get();
        int resolvedCoercions = coercions.get();
        assertThat(projections.get()).isEqualTo(1);
        assertThat(resolvedImplementations).isPositive();
        assertThat(resolvedCoercions).isPositive();

        // Another call with the same shape resolves nothing.
        preimages.castPreimage(TEST_SESSION, Domain.create(ValueSet.ofRanges(lessThan(DOUBLE, -3.5)), false), BIGINT);
        assertThat(projections.get()).isEqualTo(1);
        assertThat(implementations.get()).isEqualTo(resolvedImplementations);
        assertThat(coercions.get()).isEqualTo(resolvedCoercions);
    }

    private static Domain castPreimage(Domain domain, Type inputType)
    {
        return castPreimage(TEST_SESSION, domain, inputType);
    }

    private static Domain castPreimage(Session session, Domain domain, Type inputType)
    {
        return new FunctionPreimages(PLANNER_CONTEXT.getMetadata(), PLANNER_CONTEXT.getFunctionManager(), PLANNER_CONTEXT.getTypeManager())
                .castPreimage(session, domain, inputType);
    }
}
