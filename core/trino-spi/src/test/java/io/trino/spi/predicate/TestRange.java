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
import static org.testng.Assert.fail;

public class TestRange
{
    @Test
    public void testMismatchedTypes()
    {
        assertThatThrownBy(() -> new Range(Marker.exactly(BIGINT, 1L), Marker.exactly(VARCHAR, utf8Slice("a"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Marker types do not match: bigint vs varchar");
    }

    @Test
    public void testInvertedBounds()
    {
        assertThatThrownBy(() -> new Range(Marker.exactly(BIGINT, 1L), Marker.exactly(BIGINT, 0L)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("low must be less than or equal to high");

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
        // (10, 10] using Marker
        assertThatThrownBy(() -> new Range(Marker.above(BIGINT, 10L), Marker.exactly(BIGINT, 10L)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("invalid bounds for single value range");

        // [10, 10) using Marker
        assertThatThrownBy(() -> new Range(Marker.exactly(BIGINT, 10L), Marker.below(BIGINT, 10L)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("invalid bounds for single value range");

        // (10, 10) using Marker
        assertThatThrownBy(() -> new Range(Marker.above(BIGINT, 10L), Marker.below(BIGINT, 10L)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("invalid bounds for single value range");

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
    public void testLowerUnboundedOnly()
    {
        assertThatThrownBy(() -> new Range(Marker.lowerUnbounded(BIGINT), Marker.lowerUnbounded(BIGINT)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("high bound must be EXACTLY or BELOW");
    }

    @Test
    public void testUpperUnboundedOnly()
    {
        assertThatThrownBy(() -> new Range(Marker.upperUnbounded(BIGINT), Marker.upperUnbounded(BIGINT)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("low bound must be EXACTLY or ABOVE");
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
        assertEquals(range.getLow(), Marker.lowerUnbounded(BIGINT));
        assertEquals(range.getHigh(), Marker.upperUnbounded(BIGINT));
        assertFalse(range.isSingleValue());
        assertTrue(range.isAll());
        assertEquals(range.getType(), BIGINT);
        assertTrue(range.includes(Marker.lowerUnbounded(BIGINT)));
        assertTrue(range.includes(Marker.below(BIGINT, 1L)));
        assertTrue(range.includes(Marker.exactly(BIGINT, 1L)));
        assertTrue(range.includes(Marker.above(BIGINT, 1L)));
        assertTrue(range.includes(Marker.upperUnbounded(BIGINT)));
    }

    @Test
    public void testGreaterThanRange()
    {
        Range range = Range.greaterThan(BIGINT, 1L);
        assertEquals(range.getLow(), Marker.above(BIGINT, 1L));
        assertEquals(range.getHigh(), Marker.upperUnbounded(BIGINT));
        assertFalse(range.isSingleValue());
        assertFalse(range.isAll());
        assertEquals(range.getType(), BIGINT);
        assertFalse(range.includes(Marker.lowerUnbounded(BIGINT)));
        assertFalse(range.includes(Marker.exactly(BIGINT, 1L)));
        assertTrue(range.includes(Marker.exactly(BIGINT, 2L)));
        assertTrue(range.includes(Marker.upperUnbounded(BIGINT)));
    }

    @Test
    public void testGreaterThanOrEqualRange()
    {
        Range range = Range.greaterThanOrEqual(BIGINT, 1L);
        assertEquals(range.getLow(), Marker.exactly(BIGINT, 1L));
        assertEquals(range.getHigh(), Marker.upperUnbounded(BIGINT));
        assertFalse(range.isSingleValue());
        assertFalse(range.isAll());
        assertEquals(range.getType(), BIGINT);
        assertFalse(range.includes(Marker.lowerUnbounded(BIGINT)));
        assertFalse(range.includes(Marker.exactly(BIGINT, 0L)));
        assertTrue(range.includes(Marker.exactly(BIGINT, 1L)));
        assertTrue(range.includes(Marker.exactly(BIGINT, 2L)));
        assertTrue(range.includes(Marker.upperUnbounded(BIGINT)));
    }

    @Test
    public void testLessThanRange()
    {
        Range range = Range.lessThan(BIGINT, 1L);
        assertEquals(range.getLow(), Marker.lowerUnbounded(BIGINT));
        assertEquals(range.getHigh(), Marker.below(BIGINT, 1L));
        assertFalse(range.isSingleValue());
        assertFalse(range.isAll());
        assertEquals(range.getType(), BIGINT);
        assertTrue(range.includes(Marker.lowerUnbounded(BIGINT)));
        assertFalse(range.includes(Marker.exactly(BIGINT, 1L)));
        assertTrue(range.includes(Marker.exactly(BIGINT, 0L)));
        assertFalse(range.includes(Marker.upperUnbounded(BIGINT)));
    }

    @Test
    public void testLessThanOrEqualRange()
    {
        Range range = Range.lessThanOrEqual(BIGINT, 1L);
        assertEquals(range.getLow(), Marker.lowerUnbounded(BIGINT));
        assertEquals(range.getHigh(), Marker.exactly(BIGINT, 1L));
        assertFalse(range.isSingleValue());
        assertFalse(range.isAll());
        assertEquals(range.getType(), BIGINT);
        assertTrue(range.includes(Marker.lowerUnbounded(BIGINT)));
        assertFalse(range.includes(Marker.exactly(BIGINT, 2L)));
        assertTrue(range.includes(Marker.exactly(BIGINT, 1L)));
        assertTrue(range.includes(Marker.exactly(BIGINT, 0L)));
        assertFalse(range.includes(Marker.upperUnbounded(BIGINT)));
    }

    @Test
    public void testEqualRange()
    {
        Range range = Range.equal(BIGINT, 1L);
        assertEquals(range.getLow(), Marker.exactly(BIGINT, 1L));
        assertEquals(range.getHigh(), Marker.exactly(BIGINT, 1L));
        assertTrue(range.isSingleValue());
        assertFalse(range.isAll());
        assertEquals(range.getType(), BIGINT);
        assertFalse(range.includes(Marker.lowerUnbounded(BIGINT)));
        assertFalse(range.includes(Marker.exactly(BIGINT, 0L)));
        assertTrue(range.includes(Marker.exactly(BIGINT, 1L)));
        assertFalse(range.includes(Marker.exactly(BIGINT, 2L)));
        assertFalse(range.includes(Marker.upperUnbounded(BIGINT)));
    }

    @Test
    public void testRange()
    {
        Range range = Range.range(BIGINT, 0L, false, 2L, true);
        assertEquals(range.getLow(), Marker.above(BIGINT, 0L));
        assertEquals(range.getHigh(), Marker.exactly(BIGINT, 2L));
        assertFalse(range.isSingleValue());
        assertFalse(range.isAll());
        assertEquals(range.getType(), BIGINT);
        assertFalse(range.includes(Marker.lowerUnbounded(BIGINT)));
        assertFalse(range.includes(Marker.exactly(BIGINT, 0L)));
        assertTrue(range.includes(Marker.exactly(BIGINT, 1L)));
        assertTrue(range.includes(Marker.exactly(BIGINT, 2L)));
        assertFalse(range.includes(Marker.exactly(BIGINT, 3L)));
        assertFalse(range.includes(Marker.upperUnbounded(BIGINT)));
    }

    @Test
    public void testGetSingleValue()
    {
        assertEquals(Range.equal(BIGINT, 0L).getSingleValue(), 0L);
        try {
            Range.lessThan(BIGINT, 0L).getSingleValue();
            fail();
        }
        catch (IllegalStateException e) {
        }
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
