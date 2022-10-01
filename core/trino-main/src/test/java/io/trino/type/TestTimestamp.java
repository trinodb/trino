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

import java.time.LocalDateTime;

import static io.trino.spi.function.OperatorType.INDETERMINATE;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.testing.DateTimeTestingUtils.sqlTimestampOf;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static io.trino.util.DateTimeZoneIndex.getDateTimeZone;

public class TestTimestamp
        extends AbstractTestFunctions
{
    private static final TimeZoneKey TIME_ZONE_KEY = TestingSession.DEFAULT_TIME_ZONE_KEY;
    private static final DateTimeZone DATE_TIME_ZONE = getDateTimeZone(TIME_ZONE_KEY);

    public TestTimestamp()
    {
        super(testSessionBuilder()
                .setTimeZoneKey(TIME_ZONE_KEY)
                .build());
    }

    @Test
    public void testCastFromVarcharContainingTimeZone()
    {
        assertFunction(
                "cast('2001-1-22 03:04:05.321 +07:09' as timestamp)",
                TIMESTAMP_MILLIS,
                sqlTimestampOf(3, LocalDateTime.of(2001, 1, 22, 3, 4, 5, 321_000_000)));
        assertFunction(
                "cast('2001-1-22 03:04:05 +07:09' as timestamp)",
                TIMESTAMP_MILLIS,
                sqlTimestampOf(3, LocalDateTime.of(2001, 1, 22, 3, 4, 5)));
        assertFunction(
                "cast('2001-1-22 03:04 +07:09' as timestamp)",
                TIMESTAMP_MILLIS,
                sqlTimestampOf(3, LocalDateTime.of(2001, 1, 22, 3, 4, 0)));
        assertFunction(
                "cast('2001-1-22 +07:09' as timestamp)",
                TIMESTAMP_MILLIS,
                sqlTimestampOf(3, LocalDateTime.of(2001, 1, 22, 0, 0, 0)));

        assertFunction(
                "cast('2001-1-22 03:04:05.321 Asia/Oral' as timestamp)",
                TIMESTAMP_MILLIS,
                sqlTimestampOf(3, LocalDateTime.of(2001, 1, 22, 3, 4, 5, 321_000_000)));
        assertFunction(
                "cast('2001-1-22 03:04:05 Asia/Oral' as timestamp)",
                TIMESTAMP_MILLIS,
                sqlTimestampOf(3, LocalDateTime.of(2001, 1, 22, 3, 4, 5)));
        assertFunction(
                "cast('2001-1-22 03:04 Asia/Oral' as timestamp)",
                TIMESTAMP_MILLIS,
                sqlTimestampOf(3, LocalDateTime.of(2001, 1, 22, 3, 4, 0)));
        assertFunction(
                "cast('2001-1-22 Asia/Oral' as timestamp)",
                TIMESTAMP_MILLIS,
                sqlTimestampOf(3, LocalDateTime.of(2001, 1, 22, 0, 0, 0)));
    }

    @Test
    public void testSubtract()
    {
        functionAssertions.assertFunctionString("TIMESTAMP '2017-03-30 14:15:16.432' - TIMESTAMP '2016-03-29 03:04:05.321'",
                INTERVAL_DAY_TIME,
                "366 11:11:11.111");

        functionAssertions.assertFunctionString("TIMESTAMP '2016-03-29 03:04:05.321' - TIMESTAMP '2017-03-30 14:15:16.432'",
                INTERVAL_DAY_TIME,
                "-366 11:11:11.111");
    }

    @Test
    public void testEqual()
    {
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' = TIMESTAMP '2001-1-22 03:04:05.321'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22' = TIMESTAMP '2001-1-22'", BOOLEAN, true);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' = TIMESTAMP '2001-1-22 03:04:05.333'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22' = TIMESTAMP '2001-1-11'", BOOLEAN, false);
    }

    @Test
    public void testNotEqual()
    {
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' <> TIMESTAMP '2001-1-22 03:04:05.333'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22' <> TIMESTAMP '2001-1-11'", BOOLEAN, true);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' <> TIMESTAMP '2001-1-22 03:04:05.321'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22' <> TIMESTAMP '2001-1-22'", BOOLEAN, false);
    }

    @Test
    public void testLessThan()
    {
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' < TIMESTAMP '2001-1-22 03:04:05.333'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22' < TIMESTAMP '2001-1-23'", BOOLEAN, true);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' < TIMESTAMP '2001-1-22 03:04:05.321'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' < TIMESTAMP '2001-1-22 03:04:05'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22' < TIMESTAMP '2001-1-22'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22' < TIMESTAMP '2001-1-20'", BOOLEAN, false);
    }

    @Test
    public void testLessThanOrEqual()
    {
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' <= TIMESTAMP '2001-1-22 03:04:05.333'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' <= TIMESTAMP '2001-1-22 03:04:05.321'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22' <= TIMESTAMP '2001-1-23'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22' <= TIMESTAMP '2001-1-22'", BOOLEAN, true);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' <= TIMESTAMP '2001-1-22 03:04:05'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22' <= TIMESTAMP '2001-1-20'", BOOLEAN, false);
    }

    @Test
    public void testGreaterThan()
    {
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' > TIMESTAMP '2001-1-22 03:04:05.111'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22' > TIMESTAMP '2001-1-11'", BOOLEAN, true);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' > TIMESTAMP '2001-1-22 03:04:05.321'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' > TIMESTAMP '2001-1-22 03:04:05.333'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22' > TIMESTAMP '2001-1-22'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22' > TIMESTAMP '2001-1-23'", BOOLEAN, false);
    }

    @Test
    public void testGreaterThanOrEqual()
    {
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' >= TIMESTAMP '2001-1-22 03:04:05.111'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' >= TIMESTAMP '2001-1-22 03:04:05.321'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22' >= TIMESTAMP '2001-1-11'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22' >= TIMESTAMP '2001-1-22'", BOOLEAN, true);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' >= TIMESTAMP '2001-1-22 03:04:05.333'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22' >= TIMESTAMP '2001-1-23'", BOOLEAN, false);
    }

    @Test
    public void testBetween()
    {
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' between TIMESTAMP '2001-1-22 03:04:05.111' and TIMESTAMP '2001-1-22 03:04:05.333'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' between TIMESTAMP '2001-1-22 03:04:05.321' and TIMESTAMP '2001-1-22 03:04:05.333'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' between TIMESTAMP '2001-1-22 03:04:05.111' and TIMESTAMP '2001-1-22 03:04:05.321'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' between TIMESTAMP '2001-1-22 03:04:05.321' and TIMESTAMP '2001-1-22 03:04:05.321'", BOOLEAN, true);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' between TIMESTAMP '2001-1-22 03:04:05.322' and TIMESTAMP '2001-1-22 03:04:05.333'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' between TIMESTAMP '2001-1-22 03:04:05.311' and TIMESTAMP '2001-1-22 03:04:05.312'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' between TIMESTAMP '2001-1-22 03:04:05.333' and TIMESTAMP '2001-1-22 03:04:05.111'", BOOLEAN, false);
    }

    @Test
    public void testGreatest()
    {
        assertFunction("greatest(TIMESTAMP '2013-03-30 01:05', TIMESTAMP '2012-03-30 01:05')",
                createTimestampType(0),
                sqlTimestampOf(0, 2013, 3, 30, 1, 5, 0, 0));
        assertFunction("greatest(TIMESTAMP '2013-03-30 01:05', TIMESTAMP '2012-03-30 01:05', TIMESTAMP '2012-05-01 01:05')",
                createTimestampType(0),
                sqlTimestampOf(0, 2013, 3, 30, 1, 5, 0, 0));
    }

    @Test
    public void testLeast()
    {
        assertFunction("least(TIMESTAMP '2013-03-30 01:05', TIMESTAMP '2012-03-30 01:05')",
                createTimestampType(0),
                sqlTimestampOf(0, 2012, 3, 30, 1, 5, 0, 0));
        assertFunction("least(TIMESTAMP '2013-03-30 01:05', TIMESTAMP '2012-03-30 01:05', TIMESTAMP '2012-05-01 01:05')",
                createTimestampType(0),
                sqlTimestampOf(0, 2012, 3, 30, 1, 5, 0, 0));
    }

    @Test
    public void testIndeterminate()
    {
        assertOperator(INDETERMINATE, "cast(null as TIMESTAMP)", BOOLEAN, true);
        assertOperator(INDETERMINATE, "TIMESTAMP '2012-03-30 01:05'", BOOLEAN, false);
    }
}
