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
import io.trino.metadata.FunctionPreimages;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;
import io.trino.sql.InterpretedFunctionInvoker;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
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
        assertThat(castPreimage(Domain.create(ValueSet.ofRanges(lessThan(createVarcharType(10), utf8Slice("P"))), false), createCharType(10)))
                .isEqualTo(Domain.all(createCharType(10)));
    }

    private static Domain castPreimage(Domain domain, Type inputType)
    {
        return castPreimage(TEST_SESSION, domain, inputType);
    }

    private static Domain castPreimage(Session session, Domain domain, Type inputType)
    {
        return new FunctionPreimages(PLANNER_CONTEXT.getMetadata(), PLANNER_CONTEXT.getFunctionManager(), PLANNER_CONTEXT.getTypeManager(), session)
                .castPreimage(domain, inputType);
    }
}
