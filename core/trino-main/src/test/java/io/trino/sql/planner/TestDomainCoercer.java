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
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.SessionTestUtils.TEST_SESSION;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDomainCoercer
{
    @Test
    public void testNone()
    {
        assertThat(applySaturatedCasts(Domain.none(BIGINT), INTEGER)).isEqualTo(Domain.none(INTEGER));
    }

    @Test
    public void testAll()
    {
        assertThat(applySaturatedCasts(Domain.all(BIGINT), INTEGER)).isEqualTo(Domain.all(INTEGER));
    }

    @Test
    public void testOnlyNull()
    {
        assertThat(applySaturatedCasts(Domain.onlyNull(BIGINT), INTEGER)).isEqualTo(Domain.onlyNull(INTEGER));
    }

    @Test
    public void testCoercedValueSameAsOriginal()
    {
        assertThat(applySaturatedCasts(multipleValues(BIGINT, ImmutableList.of(1L, 10000L, -2000L)), SMALLINT)).isEqualTo(multipleValues(SMALLINT, ImmutableList.of(1L, 10000L, -2000L)));

        Domain original = Domain.create(
                ValueSet.ofRanges(
                        lessThan(DOUBLE, 0.0),
                        range(DOUBLE, 0.0, false, 1.0, false),
                        range(DOUBLE, 2.0, true, 3.0, true),
                        greaterThan(DOUBLE, 4.0)),
                true);
        assertThat(applySaturatedCasts(original, REAL)).isEqualTo(Domain.create(
                ValueSet.ofRanges(
                        lessThan(REAL, toReal(0.0f)),
                        range(REAL, toReal(0.0f), false, toReal(1.0f), false),
                        range(REAL, toReal(2.0f), true, toReal(3.0f), true),
                        greaterThan(REAL, toReal(4.0f))),
                true));
    }

    @Test
    public void testOutsideTargetTypeRange()
    {
        assertThat(applySaturatedCasts(multipleValues(BIGINT, ImmutableList.of(1L, 10000000000L, -2000L)), SMALLINT)).isEqualTo(multipleValues(SMALLINT, ImmutableList.of(1L, -2000L)));

        assertThat(applySaturatedCasts(
                Domain.create(
                        ValueSet.ofRanges(range(DOUBLE, 0.0, true, ((double) Float.MAX_VALUE) * 10, true)),
                        true),
                REAL)).isEqualTo(Domain.create(
                ValueSet.ofRanges(range(REAL, toReal(0.0f), true, toReal(Float.MAX_VALUE), true)),
                true));

        // low below and high above target type range
        assertThat(applySaturatedCasts(
                Domain.create(
                        ValueSet.ofRanges(
                                range(DOUBLE, ((double) Float.MAX_VALUE) * -2, true, ((double) Float.MAX_VALUE) * 10, true)),
                        true),
                REAL)).isEqualTo(Domain.create(ValueSet.ofRanges(lessThanOrEqual(REAL, toReal(Float.MAX_VALUE))), true));

        assertThat(applySaturatedCasts(
                Domain.create(
                        ValueSet.ofRanges(
                                range(DOUBLE, Double.NEGATIVE_INFINITY, true, Double.POSITIVE_INFINITY, true)),
                        true),
                REAL)).isEqualTo(Domain.create(
                ValueSet.ofRanges(
                        lessThanOrEqual(REAL, toReal(Float.MAX_VALUE))),
                true));

        assertThat(applySaturatedCasts(
                Domain.create(
                        ValueSet.ofRanges(
                                range(BIGINT, ((long) Integer.MAX_VALUE) * -2, false, ((long) Integer.MAX_VALUE) * 10, false)),
                        true),
                INTEGER)).isEqualTo(Domain.create(ValueSet.ofRanges(lessThanOrEqual(INTEGER, (long) Integer.MAX_VALUE)), true));

        assertThat(applySaturatedCasts(
                Domain.create(
                        ValueSet.ofRanges(
                                range(DOUBLE, Double.NEGATIVE_INFINITY, true, Double.POSITIVE_INFINITY, true)),
                        true),
                INTEGER)).isEqualTo(Domain.create(ValueSet.ofRanges(lessThanOrEqual(INTEGER, (long) Integer.MAX_VALUE)), true));

        // Low and high below target type range
        assertThat(applySaturatedCasts(
                Domain.create(
                        ValueSet.ofRanges(
                                range(BIGINT, ((long) Integer.MAX_VALUE) * -4, false, ((long) Integer.MAX_VALUE) * -2, false)),
                        false),
                INTEGER)).isEqualTo(Domain.none(INTEGER));

        assertThat(applySaturatedCasts(
                Domain.create(
                        ValueSet.ofRanges(
                                range(DOUBLE, ((double) Float.MAX_VALUE) * -4, true, ((double) Float.MAX_VALUE) * -2, true)),
                        true),
                REAL)).isEqualTo(Domain.onlyNull(REAL));

        // Low and high above target type range
        assertThat(applySaturatedCasts(
                Domain.create(
                        ValueSet.ofRanges(
                                range(BIGINT, ((long) Integer.MAX_VALUE) * 2, false, ((long) Integer.MAX_VALUE) * 4, false)),
                        false),
                INTEGER)).isEqualTo(Domain.none(INTEGER));

        assertThat(applySaturatedCasts(
                Domain.create(
                        ValueSet.ofRanges(
                                range(DOUBLE, ((double) Float.MAX_VALUE) * 2, true, ((double) Float.MAX_VALUE) * 4, true)),
                        true),
                REAL)).isEqualTo(Domain.onlyNull(REAL));

        // all short-circuit
        assertThat(applySaturatedCasts(
                Domain.create(
                        ValueSet.ofRanges(
                                greaterThanOrEqual(DOUBLE, ((double) Float.MAX_VALUE) * -4),
                                range(DOUBLE, 0.0, true, 1.0, true)),
                        true),
                REAL)).isEqualTo(Domain.all(REAL));
    }

    @Test
    public void testTruncatedCoercedValue()
    {
        assertThat(applySaturatedCasts(
                Domain.create(
                        ValueSet.ofRanges(
                                range(createDecimalType(6, 3), 123456L, true, 234567L, false)),
                        true),
                createDecimalType(6, 1))).isEqualTo(Domain.create(
                ValueSet.ofRanges(range(createDecimalType(6, 1), 1234L, false, 2345L, true)),
                true));
    }

    @Test
    public void testUnsupportedCast()
    {
        assertThatThrownBy(() -> applySaturatedCasts(Domain.singleValue(INTEGER, 10L), BIGINT))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testVarcharToChar()
    {
        // Values without trailing spaces are representable in char and must be preserved.
        // Dynamic filters on CAST(char_column AS varchar) join keys rely on this translation;
        // dropping the values would prune matching rows.
        assertThat(applySaturatedCasts(
                multipleValues(createVarcharType(10), ImmutableList.of(utf8Slice("I"), utf8Slice("P"))),
                createCharType(10)))
                .isEqualTo(multipleValues(createCharType(10), ImmutableList.of(utf8Slice("I"), utf8Slice("P"))));

        // No char value casts back to a varchar with trailing spaces
        assertThat(applySaturatedCasts(
                multipleValues(createVarcharType(10), ImmutableList.of(utf8Slice("I "))),
                createCharType(10)))
                .isEqualTo(Domain.none(createCharType(10)));

        // Truncation to the char length
        assertThat(applySaturatedCasts(
                Domain.create(ValueSet.ofRanges(lessThanOrEqual(createVarcharType(10), utf8Slice("abcde"))), false),
                createCharType(3)))
                .isEqualTo(Domain.create(ValueSet.ofRanges(lessThanOrEqual(createCharType(3), utf8Slice("abc"))), false));

        // An upper bound with trailing spaces floors to the char value with the trailing spaces
        // trimmed, which is the greatest char value whose cast back does not exceed the bound.
        // Flooring below it (e.g. to '123' + U+001F) would exclude char '123', whose cast back
        // ('123') satisfies the original predicate.
        assertThat(applySaturatedCasts(
                Domain.create(ValueSet.ofRanges(lessThanOrEqual(createVarcharType(10), utf8Slice("123 "))), false),
                createCharType(4)))
                .isEqualTo(Domain.create(ValueSet.ofRanges(lessThanOrEqual(createCharType(4), utf8Slice("123"))), false));

        // A lower bound with trailing spaces has no exact char preimage, so the range is widened
        // to be exclusive of the floor
        assertThat(applySaturatedCasts(
                Domain.create(ValueSet.ofRanges(greaterThanOrEqual(createVarcharType(10), utf8Slice("123 "))), false),
                createCharType(4)))
                .isEqualTo(Domain.create(ValueSet.ofRanges(greaterThan(createCharType(4), utf8Slice("123"))), false));

        // CAST(char AS varchar) is not monotone around code points below U+0020 (char comparison
        // pads with spaces), so range bounds containing them cannot be translated and are widened
        // to unbounded
        assertThat(applySaturatedCasts(
                Domain.create(ValueSet.ofRanges(lessThanOrEqual(createVarcharType(10), utf8Slice("123\0"))), false),
                createCharType(4)))
                .isEqualTo(Domain.notNull(createCharType(4)));
        assertThat(applySaturatedCasts(
                Domain.create(ValueSet.ofRanges(range(createVarcharType(10), utf8Slice("a\0"), true, utf8Slice("bcd"), true)), false),
                createCharType(4)))
                .isEqualTo(Domain.create(ValueSet.ofRanges(lessThanOrEqual(createCharType(4), utf8Slice("bcd"))), false));

        // Single values do not rely on monotonicity: an exact char preimage exists, so the value
        // is preserved even with code points below U+0020
        assertThat(applySaturatedCasts(
                multipleValues(createVarcharType(10), ImmutableList.of(utf8Slice("12\0"))),
                createCharType(4)))
                .isEqualTo(multipleValues(createCharType(4), ImmutableList.of(utf8Slice("12\0"))));
    }

    private static Domain applySaturatedCasts(Domain domain, Type coercedValueType)
    {
        return DomainCoercer.applySaturatedCasts(
                PLANNER_CONTEXT.getMetadata(),
                PLANNER_CONTEXT.getFunctionManager(),
                PLANNER_CONTEXT.getTypeOperators(),
                TEST_SESSION,
                domain,
                coercedValueType);
    }
}
