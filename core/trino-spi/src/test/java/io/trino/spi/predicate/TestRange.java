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
package io.trino.spi.predicate;

import org.testng.annotations.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestRange
{
    @Test
    public void testInvertedBounds()
    {
        assertThatThrownBy(() -> Range.range(BIGINT, 1L, true, 0L, true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("low must be less than or equal to high");
    }

    /**
     * Test Range construction when low and high bounds are equal, but one of them is not inclusive.
     */
    @Test
    public void testSingleValueExclusive()
    {
        // (10, 10]
        assertThatThrownBy(() -> Range.range(BIGINT, 10L, false, 10L, true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("invalid bounds for single value range");

        // [10, 10)
        assertThatThrownBy(() -> Range.range(BIGINT, 10L, true, 10L, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("invalid bounds for single value range");

        // (10, 10)
        assertThatThrownBy(() -> Range.range(BIGINT, 10L, false, 10L, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("invalid bounds for single value range");
    }

    @Test
    public void testSingleValue()
    {
        assertTrue(Range.range(BIGINT, 1L, true, 1L, true).isSingleValue());
        assertFalse(Range.range(BIGINT, 1L, true, 2L, true).isSingleValue());
        assertTrue(Range.range(DOUBLE, 1.1, true, 1.1, true).isSingleValue());
        assertTrue(Range.range(VARCHAR, utf8Slice("a"), true, utf8Slice("a"), true).isSingleValue());
        assertTrue(Range.range(BOOLEAN, true, true, true, true).isSingleValue());
        assertFalse(Range.range(BOOLEAN, false, true, true, true).isSingleValue());
    }

    @Test
    public void testAllRange()
    {
        Range range = Range.all(BIGINT);

        assertTrue(range.isLowUnbounded());
        assertFalse(range.isLowInclusive());
        assertThatThrownBy(range::getLowBoundedValue)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("The range is low-unbounded");

        assertTrue(range.isHighUnbounded());
        assertFalse(range.isHighInclusive());
        assertThatThrownBy(range::getHighBoundedValue)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("The range is high-unbounded");

        assertFalse(range.isSingleValue());
        assertTrue(range.isAll());
        assertEquals(range.getType(), BIGINT);
    }

    @Test
    public void testGreaterThanRange()
    {
        Range range = Range.greaterThan(BIGINT, 1L);

        assertFalse(range.isLowUnbounded());
        assertFalse(range.isLowInclusive());
        assertEquals(range.getLowBoundedValue(), 1L);

        assertTrue(range.isHighUnbounded());
        assertFalse(range.isHighInclusive());
        assertThatThrownBy(range::getHighBoundedValue)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("The range is high-unbounded");

        assertFalse(range.isSingleValue());
        assertFalse(range.isAll());
        assertEquals(range.getType(), BIGINT);
    }

    @Test
    public void testGreaterThanOrEqualRange()
    {
        Range range = Range.greaterThanOrEqual(BIGINT, 1L);

        assertFalse(range.isLowUnbounded());
        assertTrue(range.isLowInclusive());
        assertEquals(range.getLowBoundedValue(), 1L);

        assertTrue(range.isHighUnbounded());
        assertFalse(range.isHighInclusive());
        assertThatThrownBy(range::getHighBoundedValue)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("The range is high-unbounded");

        assertFalse(range.isSingleValue());
        assertFalse(range.isAll());
        assertEquals(range.getType(), BIGINT);
    }

    @Test
    public void testLessThanRange()
    {
        Range range = Range.lessThan(BIGINT, 1L);

        assertTrue(range.isLowUnbounded());
        assertFalse(range.isLowInclusive());
        assertThatThrownBy(range::getLowBoundedValue)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("The range is low-unbounded");

        assertFalse(range.isHighUnbounded());
        assertFalse(range.isHighInclusive());
        assertEquals(range.getHighBoundedValue(), 1L);

        assertFalse(range.isSingleValue());
        assertFalse(range.isAll());
        assertEquals(range.getType(), BIGINT);
    }

    @Test
    public void testLessThanOrEqualRange()
    {
        Range range = Range.lessThanOrEqual(BIGINT, 1L);

        assertTrue(range.isLowUnbounded());
        assertFalse(range.isLowInclusive());
        assertThatThrownBy(range::getLowBoundedValue)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("The range is low-unbounded");

        assertFalse(range.isHighUnbounded());
        assertTrue(range.isHighInclusive());
        assertEquals(range.getHighBoundedValue(), 1L);

        assertFalse(range.isSingleValue());
        assertFalse(range.isAll());
        assertEquals(range.getType(), BIGINT);
    }

    @Test
    public void testEqualRange()
    {
        Range range = Range.equal(BIGINT, 1L);

        assertFalse(range.isLowUnbounded());
        assertTrue(range.isLowInclusive());
        assertEquals(range.getLowBoundedValue(), 1L);

        assertFalse(range.isHighUnbounded());
        assertTrue(range.isHighInclusive());
        assertEquals(range.getHighBoundedValue(), 1L);

        assertTrue(range.isSingleValue());
        assertFalse(range.isAll());
        assertEquals(range.getType(), BIGINT);
    }

    @Test
    public void testRange()
    {
        Range range = Range.range(BIGINT, 0L, false, 2L, true);
        assertFalse(range.isLowUnbounded());
        assertFalse(range.isLowInclusive());
        assertEquals(range.getLowBoundedValue(), 0L);

        assertFalse(range.isHighUnbounded());
        assertTrue(range.isHighInclusive());
        assertEquals(range.getHighBoundedValue(), 2L);

        assertFalse(range.isSingleValue());
        assertFalse(range.isAll());
        assertEquals(range.getType(), BIGINT);
    }

    @Test
    public void testGetSingleValue()
    {
        assertEquals(Range.equal(BIGINT, 0L).getSingleValue(), 0L);
        assertThatThrownBy(() -> Range.lessThan(BIGINT, 0L).getSingleValue())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Range does not have just a single value");
    }

    @Test
    public void testContains()
    {
        assertTrue(Range.all(BIGINT).contains(Range.all(BIGINT)));
        assertTrue(Range.all(BIGINT).contains(Range.equal(BIGINT, 0L)));
        assertTrue(Range.all(BIGINT).contains(Range.greaterThan(BIGINT, 0L)));
        assertTrue(Range.equal(BIGINT, 0L).contains(Range.equal(BIGINT, 0L)));
        assertFalse(Range.equal(BIGINT, 0L).contains(Range.greaterThan(BIGINT, 0L)));
        assertFalse(Range.equal(BIGINT, 0L).contains(Range.greaterThanOrEqual(BIGINT, 0L)));
        assertFalse(Range.equal(BIGINT, 0L).contains(Range.all(BIGINT)));
        assertTrue(Range.greaterThanOrEqual(BIGINT, 0L).contains(Range.greaterThan(BIGINT, 0L)));
        assertTrue(Range.greaterThan(BIGINT, 0L).contains(Range.greaterThan(BIGINT, 1L)));
        assertFalse(Range.greaterThan(BIGINT, 0L).contains(Range.lessThan(BIGINT, 0L)));
        assertTrue(Range.range(BIGINT, 0L, true, 2L, true).contains(Range.range(BIGINT, 1L, true, 2L, true)));
        assertFalse(Range.range(BIGINT, 0L, true, 2L, true).contains(Range.range(BIGINT, 1L, true, 3L, false)));
    }

    @Test
    public void testSpan()
    {
        assertEquals(Range.greaterThan(BIGINT, 1L).span(Range.lessThanOrEqual(BIGINT, 2L)), Range.all(BIGINT));
        assertEquals(Range.greaterThan(BIGINT, 2L).span(Range.lessThanOrEqual(BIGINT, 0L)), Range.all(BIGINT));
        assertEquals(Range.range(BIGINT, 1L, true, 3L, false).span(Range.equal(BIGINT, 2L)), Range.range(BIGINT, 1L, true, 3L, false));
        assertEquals(Range.range(BIGINT, 1L, true, 3L, false).span(Range.range(BIGINT, 2L, false, 10L, false)), Range.range(BIGINT, 1L, true, 10L, false));
        assertEquals(Range.greaterThan(BIGINT, 1L).span(Range.equal(BIGINT, 0L)), Range.greaterThanOrEqual(BIGINT, 0L));
        assertEquals(Range.greaterThan(BIGINT, 1L).span(Range.greaterThanOrEqual(BIGINT, 10L)), Range.greaterThan(BIGINT, 1L));
        assertEquals(Range.lessThan(BIGINT, 1L).span(Range.lessThanOrEqual(BIGINT, 1L)), Range.lessThanOrEqual(BIGINT, 1L));
        assertEquals(Range.all(BIGINT).span(Range.lessThanOrEqual(BIGINT, 1L)), Range.all(BIGINT));
    }

    @Test
    public void testOverlaps()
    {
        assertTrue(Range.greaterThan(BIGINT, 1L).overlaps(Range.lessThanOrEqual(BIGINT, 2L)));
        assertFalse(Range.greaterThan(BIGINT, 2L).overlaps(Range.lessThan(BIGINT, 2L)));
        assertTrue(Range.range(BIGINT, 1L, true, 3L, false).overlaps(Range.equal(BIGINT, 2L)));
        assertTrue(Range.range(BIGINT, 1L, true, 3L, false).overlaps(Range.range(BIGINT, 2L, false, 10L, false)));
        assertFalse(Range.range(BIGINT, 1L, true, 3L, false).overlaps(Range.range(BIGINT, 3L, true, 10L, false)));
        assertTrue(Range.range(BIGINT, 1L, true, 3L, true).overlaps(Range.range(BIGINT, 3L, true, 10L, false)));
        assertTrue(Range.all(BIGINT).overlaps(Range.equal(BIGINT, Long.MAX_VALUE)));
    }

    @Test
    public void testIntersect()
    {
        assertThat(Range.greaterThan(BIGINT, 1L).intersect(Range.lessThanOrEqual(BIGINT, 2L)))
                .contains(Range.range(BIGINT, 1L, false, 2L, true));
        assertThat(Range.range(BIGINT, 1L, true, 3L, false).intersect(Range.equal(BIGINT, 2L)))
                .contains(Range.equal(BIGINT, 2L));
        assertThat(Range.range(BIGINT, 1L, true, 3L, false).intersect(Range.range(BIGINT, 2L, false, 10L, false)))
                .contains(Range.range(BIGINT, 2L, false, 3L, false));
        assertThat(Range.range(BIGINT, 1L, true, 3L, true).intersect(Range.range(BIGINT, 3L, true, 10L, false)))
                .contains(Range.equal(BIGINT, 3L));
        assertThat(Range.all(BIGINT).intersect(Range.equal(BIGINT, Long.MAX_VALUE)))
                .contains(Range.equal(BIGINT, Long.MAX_VALUE));
    }

    @Test
    public void testExceptionalIntersect()
    {
        Range greaterThan2 = Range.greaterThan(BIGINT, 2L);
        Range lessThan2 = Range.lessThan(BIGINT, 2L);
        assertThat(greaterThan2.intersect(lessThan2))
                .isEmpty();

        Range range1To3Exclusive = Range.range(BIGINT, 1L, true, 3L, false);
        Range range3To10 = Range.range(BIGINT, 3L, true, 10L, false);
        assertThat(range1To3Exclusive.intersect(range3To10))
                .isEmpty();
    }
}
