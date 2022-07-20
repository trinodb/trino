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
package io.trino.type;

import io.trino.operator.scalar.AbstractTestFunctions;
import io.trino.spi.type.TimeZoneKey;
import io.trino.testing.TestingSession;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import static io.trino.spi.function.OperatorType.INDETERMINATE;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static io.trino.util.DateTimeZoneIndex.getDateTimeZone;

public class TestTime
        extends AbstractTestFunctions
{
    protected static final TimeZoneKey TIME_ZONE_KEY = TestingSession.DEFAULT_TIME_ZONE_KEY;
    protected static final DateTimeZone DATE_TIME_ZONE = getDateTimeZone(TIME_ZONE_KEY);

    public TestTime()
    {
        super(testSessionBuilder()
                .setTimeZoneKey(TIME_ZONE_KEY)
                .build());
    }

    @Test
    public void testSubtract()
    {
        functionAssertions.assertFunctionString("TIME '14:15:16.432' - TIME '03:04:05.321'", INTERVAL_DAY_TIME, "0 11:11:11.111");

        functionAssertions.assertFunctionString("TIME '03:04:05.321' - TIME '14:15:16.432'", INTERVAL_DAY_TIME, "-0 11:11:11.111");
    }

    @Test
    public void testEqual()
    {
        assertFunction("TIME '03:04:05.321' = TIME '03:04:05.321'", BOOLEAN, true);

        assertFunction("TIME '03:04:05.321' = TIME '03:04:05.333'", BOOLEAN, false);
    }

    @Test
    public void testNotEqual()
    {
        assertFunction("TIME '03:04:05.321' <> TIME '03:04:05.333'", BOOLEAN, true);

        assertFunction("TIME '03:04:05.321' <> TIME '03:04:05.321'", BOOLEAN, false);
    }

    @Test
    public void testLessThan()
    {
        assertFunction("TIME '03:04:05.321' < TIME '03:04:05.333'", BOOLEAN, true);

        assertFunction("TIME '03:04:05.321' < TIME '03:04:05.321'", BOOLEAN, false);
        assertFunction("TIME '03:04:05.321' < TIME '03:04:05'", BOOLEAN, false);
    }

    @Test
    public void testLessThanOrEqual()
    {
        assertFunction("TIME '03:04:05.321' <= TIME '03:04:05.333'", BOOLEAN, true);
        assertFunction("TIME '03:04:05.321' <= TIME '03:04:05.321'", BOOLEAN, true);

        assertFunction("TIME '03:04:05.321' <= TIME '03:04:05'", BOOLEAN, false);
    }

    @Test
    public void testGreaterThan()
    {
        assertFunction("TIME '03:04:05.321' > TIME '03:04:05.111'", BOOLEAN, true);

        assertFunction("TIME '03:04:05.321' > TIME '03:04:05.321'", BOOLEAN, false);
        assertFunction("TIME '03:04:05.321' > TIME '03:04:05.333'", BOOLEAN, false);
    }

    @Test
    public void testGreaterThanOrEqual()
    {
        assertFunction("TIME '03:04:05.321' >= TIME '03:04:05.111'", BOOLEAN, true);
        assertFunction("TIME '03:04:05.321' >= TIME '03:04:05.321'", BOOLEAN, true);

        assertFunction("TIME '03:04:05.321' >= TIME '03:04:05.333'", BOOLEAN, false);
    }

    @Test
    public void testBetween()
    {
        assertFunction("TIME '03:04:05.321' between TIME '03:04:05.111' and TIME '03:04:05.333'", BOOLEAN, true);
        assertFunction("TIME '03:04:05.321' between TIME '03:04:05.321' and TIME '03:04:05.333'", BOOLEAN, true);
        assertFunction("TIME '03:04:05.321' between TIME '03:04:05.111' and TIME '03:04:05.321'", BOOLEAN, true);
        assertFunction("TIME '03:04:05.321' between TIME '03:04:05.321' and TIME '03:04:05.321'", BOOLEAN, true);

        assertFunction("TIME '03:04:05.321' between TIME '03:04:05.322' and TIME '03:04:05.333'", BOOLEAN, false);
        assertFunction("TIME '03:04:05.321' between TIME '03:04:05.311' and TIME '03:04:05.312'", BOOLEAN, false);
        assertFunction("TIME '03:04:05.321' between TIME '03:04:05.333' and TIME '03:04:05.111'", BOOLEAN, false);
    }

    @Test
    public void testCastToVarchar()
    {
        assertFunction("cast(TIME '03:04:05.321' as varchar)", VARCHAR, "03:04:05.321");
        assertFunction("cast(TIME '03:04:05' as varchar)", VARCHAR, "03:04:05");
        assertFunction("cast(TIME '03:04' as varchar)", VARCHAR, "03:04:00");
    }

    @Test
    public void testCastFromVarchar()
    {
        assertFunction("cast('03:04:05.321' as time) = TIME '03:04:05.321'", BOOLEAN, true);
        assertFunction("cast('03:04:05' as time) = TIME '03:04:05.000'", BOOLEAN, true);
        assertFunction("cast('03:04' as time) = TIME '03:04:00.000'", BOOLEAN, true);
    }

    @Test
    public void testIndeterminate()
    {
        assertOperator(INDETERMINATE, "cast(null as TIME)", BOOLEAN, true);
        assertOperator(INDETERMINATE, "TIME '00:00:00'", BOOLEAN, false);
    }
}
