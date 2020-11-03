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
package io.prestosql.type;

import io.prestosql.operator.scalar.AbstractTestFunctions;
import io.prestosql.spi.type.TSRange;
import org.testng.annotations.Test;

import java.time.LocalDateTime;

import static io.prestosql.spi.StandardErrorCode.CONSTRAINT_VIOLATION;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.TSRangeType.TSRANGE_MILLIS;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.prestosql.testing.DateTimeTestingUtils.sqlTimestampOf;

public class TestTSRangeFunctions
        extends AbstractTestFunctions
{
    @Test
    public void testTSRangeConstructor()
    {
        assertFunction("tsrange (20,30,false,false)", TSRANGE_MILLIS, TSRange.fromString("(20,30)"));
        assertFunction("tsrange (20,30,false,true)", TSRANGE_MILLIS, TSRange.fromString("(20,30]"));
        assertFunction("tsrange (20,30,true,false)", TSRANGE_MILLIS, TSRange.fromString("[20,30)"));
        assertFunction("tsrange (20,30,true,true)", TSRANGE_MILLIS, TSRange.fromString("[20,30]"));
        // no bounds -> [a,b)
        assertFunction("tsrange (10,20)", TSRANGE_MILLIS, TSRange.fromString("[10,20)"));
    }

    @Test
    public void testTSRangeFromTimestampsConstructor()
    {
        assertFunction("tsrange '(2020-08-18 20:00:00,2020-08-18 21:00:00)'", TSRANGE_MILLIS, TSRange.fromString("(2020-08-18 20:00:00,2020-08-18 21:00:00)"));
        // postgres standard
        assertFunction("tsrange '(\"2020-08-18 20:00:00\",\"2020-08-18 21:00:00\")'", TSRANGE_MILLIS, TSRange.fromString("(2020-08-18 20:00:00,2020-08-18 21:00:00)"));
    }

    @Test
    public void testLower()
    {
        // there seems to be a bug in the projection of AbstractTestFunctions
        assertFunction("lower(tsrange '[2020-11-03 18:51:00.123,2020-11-03 21:54:00.456)')", TIMESTAMP_MILLIS,
                sqlTimestampOf(3, LocalDateTime.of(2020, 11, 3, 18, 51, 0, 123_000_000)));
        assertInvalidFunction("lower(tsrange 'empty')", CONSTRAINT_VIOLATION);
    }

    @Test
    public void testUpper()
    {
        // there seems to be a bug in the projection of AbstractTestFunctions
        assertFunction("upper(tsrange '[2020-11-03 18:51:00.123,2020-11-03 21:54:02.456)')", TIMESTAMP_MILLIS,
                sqlTimestampOf(3, LocalDateTime.of(2020, 11, 3, 21, 54, 2, 456_000_000)));
        assertInvalidFunction("upper(tsrange 'empty')", CONSTRAINT_VIOLATION);
    }

    @Test
    public void testLowerEpoch()
    {
        assertFunction("lowerEpoch(tsrange '(1,3)')", BIGINT, 1L);
        assertInvalidFunction("lowerEpoch(tsrange 'empty')", CONSTRAINT_VIOLATION);
    }

    @Test
    public void testUpperEpoch()
    {
        assertFunction("upperEpoch(tsrange '(1,3)')", BIGINT, 3L);
        assertInvalidFunction("upperEpoch(tsrange 'empty')", CONSTRAINT_VIOLATION);
    }

    @Test
    public void testLeftClosed()
    {
        assertFunction("lower_inc(tsrange '[1,3)')", BOOLEAN, true);
        assertFunction("lower_inc(tsrange '(1,3)')", BOOLEAN, false);
        assertInvalidFunction("lower_inc(tsrange 'empty')", CONSTRAINT_VIOLATION, "Empty range has no bounds");
    }

    @Test
    public void testStrictlyLeft()
    {
        assertFunction("strictlyLeft(tsrange '(1,3)', tsrange '(3,4)')", BOOLEAN, true);
        // show asymmetry a << b -> !(b << a)
        assertFunction("strictlyLeft(tsrange '(3,4)', tsrange '(1,3)')", BOOLEAN, false);
        assertFunction("strictlyLeft(tsrange '(1,3]', tsrange '(3,4)')", BOOLEAN, true);
        assertFunction("strictlyLeft(tsrange '(1,3)', tsrange '[3,4)')", BOOLEAN, true);
        assertFunction("strictlyLeft(tsrange '(1,3]', tsrange '[3,4)')", BOOLEAN, false);
        assertFunction("strictlyLeft(tsrange '(1,3)', tsrange '(2,4)')", BOOLEAN, false);
    }

    @Test
    public void testAdjacent()
    {
        assertFunction("adjacent(tsrange '(1,3]', tsrange '(3,4)')", BOOLEAN, true);
        assertFunction("adjacent(tsrange '(1,3)', tsrange '[3,4)')", BOOLEAN, true);
        // symmetric
        assertFunction("adjacent(tsrange '[3,4)', tsrange '(1,3)')", BOOLEAN, true);
        assertFunction("adjacent(tsrange '(1,3)', tsrange '(3,4)')", BOOLEAN, false);
        assertFunction("adjacent(tsrange '(1,3]', tsrange '[3,4)')", BOOLEAN, false);
        assertFunction("adjacent(tsrange '(1,4]', tsrange '[3,4]')", BOOLEAN, false);
    }

    @Test
    public void testIsEmpty()
    {
        assertFunction("isempty(tsrange '(1,3]')", BOOLEAN, false);
        assertFunction("isempty(tsrange 'empty')", BOOLEAN, true);
    }

    @Test
    public void testContainsTSRange()
    {
        assertFunction("containsTSRange(tsrange '(1,10]', tsrange '(1,10]')", BOOLEAN, true);
        assertFunction("containsTSRange(tsrange '(1,10]', tsrange '[2,9)')", BOOLEAN, true);
        assertFunction("containsTSRange(tsrange '(1,10]', tsrange 'empty')", BOOLEAN, true);
        assertFunction("containsTSRange(tsrange 'empty', tsrange 'empty')", BOOLEAN, true);
        assertFunction("containsTSRange(tsrange '(1,10]', tsrange '[1,10)')", BOOLEAN, false);
        assertFunction("containsTSRange(tsrange '[1,10)', tsrange '(1,10]')", BOOLEAN, false);
    }

    @Test
    public void testContainsElement()
    {
        assertFunction("containsElement(tsrange '(1,10]', 10)", BOOLEAN, true);
        assertFunction("containsElement(tsrange '(1,10]', 1)", BOOLEAN, false);
        assertFunction("containsElement(tsrange 'empty', 1)", BOOLEAN, false);
    }

    @Test
    public void testContainsTimestamp()
    {
        assertFunction("containsTimestamp(tsrange '[2020-11-03 20:00:00,2020-11-03 23:00:00]', timestamp '2020-11-03 21:00:00.123')", BOOLEAN, true);
        assertFunction("containsTimestamp(tsrange '[2020-11-03 20:00:00,2020-11-03 21:00:00.123]', timestamp '2020-11-03 21:00:00.123')", BOOLEAN, true);
        // upper is exclusive
        assertFunction("containsTimestamp(tsrange '[2020-11-03 20:00:00,2020-11-03 21:00:00.123)', timestamp '2020-11-03 21:00:00.123')", BOOLEAN, false);
        // empty never contains elements
        assertFunction("containsTimestamp(tsrange 'empty', timestamp '2020-11-03 21:00:00.123')", BOOLEAN, false);
    }

    @Test
    public void testOverlap()
    {
        assertFunction("overlap(tsrange '(2,4]', tsrange '(3,5]')", BOOLEAN, true);
        // symmetry
        assertFunction("overlap(tsrange '(3,5]', tsrange '(2,4]')", BOOLEAN, true);
        assertFunction("overlap(tsrange '[5,7]', tsrange '(3,5]')", BOOLEAN, true);
        assertFunction("overlap(tsrange '(3,5]', tsrange '[5,7]')", BOOLEAN, true);
        assertFunction("overlap(tsrange '(2,4]', tsrange '[4,5]')", BOOLEAN, true);
        // (2,4) * (3,5] = (3,4)
        assertFunction("overlap(tsrange '(2,4)', tsrange '(3,5]')", BOOLEAN, true);
        assertFunction("overlap(tsrange '(3,5)', tsrange '[5,7]')", BOOLEAN, false);
        assertFunction("overlap(tsrange '(4,5)', tsrange '(2,4)')", BOOLEAN, false);
        assertFunction("overlap(tsrange '(2,4)', tsrange '[4,5]')", BOOLEAN, false);
        assertFunction("overlap(tsrange '[20,30]', tsrange '[0,9]')", BOOLEAN, false);
        assertFunction("overlap(tsrange 'empty', tsrange '[0,9]')", BOOLEAN, false);
    }

    @Test
    public void testDoesNotExtendToTheRightOf()
    {
        assertFunction("doesNotExtendToTheRightOf(tsrange '(2,4]', tsrange '(3,5]')", BOOLEAN, true);
        assertFunction("doesNotExtendToTheRightOf(tsrange '(2,4]', tsrange '(4,5]')", BOOLEAN, true);
        assertFunction("doesNotExtendToTheRightOf(tsrange '(2,4)', tsrange '(3,4)')", BOOLEAN, true);
        assertFunction("doesNotExtendToTheRightOf(tsrange '(2,4]', tsrange '(3,4)')", BOOLEAN, false);
        assertFunction("doesNotExtendToTheRightOf(tsrange 'empty', tsrange '(3,5]')", BOOLEAN, false);
        assertFunction("doesNotExtendToTheRightOf(tsrange '[4,4]', tsrange 'empty')", BOOLEAN, false);
        assertFunction("doesNotExtendToTheRightOf(tsrange 'empty', tsrange 'empty')", BOOLEAN, false);
    }

    @Test
    public void testDoesNotExtendToTheLeftOf()
    {
        assertFunction("doesNotExtendToTheLeftOf(tsrange '(2,4]', tsrange '(2,5]')", BOOLEAN, true);
        assertFunction("doesNotExtendToTheLeftOf(tsrange '[2,4]', tsrange '[2,5]')", BOOLEAN, true);
        assertFunction("doesNotExtendToTheLeftOf(tsrange '(5,8)', tsrange '(3,4)')", BOOLEAN, true);
        assertFunction("doesNotExtendToTheLeftOf(tsrange '[3,4]', tsrange '(3,4)')", BOOLEAN, false);
        assertFunction("doesNotExtendToTheLeftOf(tsrange 'empty', tsrange '(2,5]')", BOOLEAN, false);
        assertFunction("doesNotExtendToTheLeftOf(tsrange '[4,4]', tsrange 'empty')", BOOLEAN, false);
        assertFunction("doesNotExtendToTheLeftOf(tsrange 'empty', tsrange 'empty')", BOOLEAN, false);
    }
}
