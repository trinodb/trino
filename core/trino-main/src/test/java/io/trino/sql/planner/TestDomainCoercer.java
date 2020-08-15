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
import org.testng.annotations.Test;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.predicate.Domain.multipleValues;
import static io.trino.spi.predicate.Range.greaterThan;
import static io.trino.spi.predicate.Range.greaterThanOrEqual;
import static io.trino.spi.predicate.Range.lessThan;
import static io.trino.spi.predicate.Range.lessThanOrEqual;
import static io.trino.spi.predicate.Range.range;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static java.lang.Float.floatToIntBits;
import static org.testng.Assert.assertEquals;

public class TestDomainCoercer
{
    @Test
    public void testNone()
    {
        assertEquals(applySaturatedCasts(Domain.none(BIGINT), INTEGER), Domain.none(INTEGER));
    }

    @Test
    public void testAll()
    {
        assertEquals(applySaturatedCasts(Domain.all(BIGINT), INTEGER), Domain.all(INTEGER));
    }

    @Test
    public void testOnlyNull()
    {
        assertEquals(applySaturatedCasts(Domain.onlyNull(BIGINT), INTEGER), Domain.onlyNull(INTEGER));
    }

    @Test
    public void testCoercedValueSameAsOriginal()
    {
        assertEquals(
                applySaturatedCasts(multipleValues(BIGINT, ImmutableList.of(1L, 10000L, -2000L)), SMALLINT),
                multipleValues(SMALLINT, ImmutableList.of(1L, 10000L, -2000L)));

        Domain original = Domain.create(
                ValueSet.ofRanges(
                        lessThan(DOUBLE, 0.0),
                        range(DOUBLE, 0.0, false, 1.0, false),
                        range(DOUBLE, 2.0, true, 3.0, true),
                        greaterThan(DOUBLE, 4.0)),
                true);
        assertEquals(
                applySaturatedCasts(original, REAL),
                Domain.create(
                        ValueSet.ofRanges(
                                lessThan(REAL, (long) floatToIntBits(0.0f)),
                                range(REAL, (long) floatToIntBits(0.0f), false, (long) floatToIntBits(1.0f), false),
                                range(REAL, (long) floatToIntBits(2.0f), true, (long) floatToIntBits(3.0f), true),
                                greaterThan(REAL, (long) floatToIntBits(4.0f))),
                        true));
    }

    @Test
    public void testOutsideTargetTypeRange()
    {
        assertEquals(
                applySaturatedCasts(multipleValues(BIGINT, ImmutableList.of(1L, 10000000000L, -2000L)), SMALLINT),
                multipleValues(SMALLINT, ImmutableList.of(1L, -2000L)));

        assertEquals(
                applySaturatedCasts(
                        Domain.create(
                                ValueSet.ofRanges(range(DOUBLE, 0.0, true, ((double) Float.MAX_VALUE) * 10, true)),
                                true),
                        REAL),
                Domain.create(
                        ValueSet.ofRanges((range(REAL, (long) floatToIntBits(0.0f), true, (long) floatToIntBits(Float.MAX_VALUE), true))),
                        true));

        // low below and high above target type range
        assertEquals(
                applySaturatedCasts(
                        Domain.create(
                                ValueSet.ofRanges(
                                        range(DOUBLE, ((double) Float.MAX_VALUE) * -2, true, ((double) Float.MAX_VALUE) * 10, true)),
                                true),
                        REAL),
                Domain.create(ValueSet.ofRanges(lessThanOrEqual(REAL, (long) floatToIntBits(Float.MAX_VALUE))), true));

        assertEquals(
                applySaturatedCasts(
                        Domain.create(
                                ValueSet.ofRanges(
                                        range(DOUBLE, Double.NEGATIVE_INFINITY, true, Double.POSITIVE_INFINITY, true)),
                                true),
                        REAL),
                Domain.create(
                        ValueSet.ofRanges(
                                lessThanOrEqual(REAL, (long) floatToIntBits(Float.MAX_VALUE))),
                        true));

        assertEquals(
                applySaturatedCasts(
                        Domain.create(
                                ValueSet.ofRanges(
                                        range(BIGINT, ((long) Integer.MAX_VALUE) * -2, false, ((long) Integer.MAX_VALUE) * 10, false)),
                                true),
                        INTEGER),
                Domain.create(ValueSet.ofRanges(lessThanOrEqual(INTEGER, (long) Integer.MAX_VALUE)), true));

        assertEquals(
                applySaturatedCasts(
                        Domain.create(
                                ValueSet.ofRanges(
                                        range(DOUBLE, Double.NEGATIVE_INFINITY, true, Double.POSITIVE_INFINITY, true)),
                                true),
                        INTEGER),
                Domain.create(ValueSet.ofRanges(lessThanOrEqual(INTEGER, (long) Integer.MAX_VALUE)), true));

        // Low and high below target type range
        assertEquals(
                applySaturatedCasts(
                        Domain.create(
                                ValueSet.ofRanges(
                                        range(BIGINT, ((long) Integer.MAX_VALUE) * -4, false, ((long) Integer.MAX_VALUE) * -2, false)),
                                false),
                        INTEGER),
                Domain.none(INTEGER));

        assertEquals(
                applySaturatedCasts(
                        Domain.create(
                                ValueSet.ofRanges(
                                        range(DOUBLE, ((double) Float.MAX_VALUE) * -4, true, ((double) Float.MAX_VALUE) * -2, true)),
                                true),
                        REAL),
                Domain.onlyNull(REAL));

        // Low and high above target type range
        assertEquals(
                applySaturatedCasts(
                        Domain.create(
                                ValueSet.ofRanges(
                                        range(BIGINT, ((long) Integer.MAX_VALUE) * 2, false, ((long) Integer.MAX_VALUE) * 4, false)),
                                false),
                        INTEGER),
                Domain.none(INTEGER));

        assertEquals(
                applySaturatedCasts(
                        Domain.create(
                                ValueSet.ofRanges(
                                        range(DOUBLE, ((double) Float.MAX_VALUE) * 2, true, ((double) Float.MAX_VALUE) * 4, true)),
                                true),
                        REAL),
                Domain.onlyNull(REAL));

        // all short-circuit
        assertEquals(
                applySaturatedCasts(
                        Domain.create(
                                ValueSet.ofRanges(
                                        greaterThanOrEqual(DOUBLE, ((double) Float.MAX_VALUE) * -4),
                                        range(DOUBLE, 0.0, true, 1.0, true)),
                                true),
                        REAL),
                Domain.all(REAL));
    }

    @Test
    public void testTruncatedCoercedValue()
    {
        assertEquals(
                applySaturatedCasts(
                        Domain.create(
                                ValueSet.ofRanges(
                                        range(createDecimalType(6, 3), 123456L, true, 234567L, false)),
                                true),
                        createDecimalType(6, 1)),
                Domain.create(
                        ValueSet.ofRanges(range(createDecimalType(6, 1), 1234L, false, 2345L, true)),
                        true));
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testUnsupportedCast()
    {
        applySaturatedCasts(Domain.singleValue(INTEGER, 10L), BIGINT);
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
