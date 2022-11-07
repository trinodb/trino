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
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.TimeZoneKey;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static io.trino.spi.StandardErrorCode.INVALID_LITERAL;
import static io.trino.spi.function.OperatorType.INDETERMINATE;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.DateTimeTestingUtils.sqlTimeOf;
import static io.trino.testing.DateTimeTestingUtils.sqlTimestampOf;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static io.trino.util.DateTimeZoneIndex.getDateTimeZone;
import static org.joda.time.DateTimeZone.UTC;

public class TestTimestampWithTimeZone
        extends AbstractTestFunctions
{
    private static final TimeZoneKey TIME_ZONE_KEY = getTimeZoneKeyForOffset(6 * 60 + 9);
    private static final DateTimeZone DATE_TIME_ZONE = getDateTimeZone(TIME_ZONE_KEY);
    private static final TimeZoneKey WEIRD_TIME_ZONE_KEY = getTimeZoneKeyForOffset(7 * 60 + 9);
    private static final DateTimeZone WEIRD_ZONE = getDateTimeZone(WEIRD_TIME_ZONE_KEY);
    private static final TimeZoneKey BERLIN_TIME_ZONE_KEY = getTimeZoneKey("Europe/Berlin");
    private static final DateTimeZone BERLIN_ZONE = getDateTimeZone(BERLIN_TIME_ZONE_KEY);

    public TestTimestampWithTimeZone()
    {
        super(testSessionBuilder()
                .setTimeZoneKey(TIME_ZONE_KEY)
                .build());
    }

    @Test
    public void testLiteral()
    {
        assertFunction("TIMESTAMP '2001-01-02 03:04:05.321 +07:09'",
                createTimestampWithTimeZoneType(3),
                SqlTimestampWithTimeZone.newInstance(3, new DateTime(2001, 1, 2, 3, 4, 5, 321, WEIRD_ZONE).getMillis(), 0, WEIRD_TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-01-02 03:04:05 +07:09'",
                createTimestampWithTimeZoneType(0),
                SqlTimestampWithTimeZone.newInstance(0, new DateTime(2001, 1, 2, 3, 4, 5, 0, WEIRD_ZONE).getMillis(), 0, WEIRD_TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-01-02 03:04 +07:09'",
                createTimestampWithTimeZoneType(0),
                SqlTimestampWithTimeZone.newInstance(0, new DateTime(2001, 1, 2, 3, 4, 0, 0, WEIRD_ZONE).getMillis(), 0, WEIRD_TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-01-02 +07:09'",
                createTimestampWithTimeZoneType(0),
                SqlTimestampWithTimeZone.newInstance(0, new DateTime(2001, 1, 2, 0, 0, 0, 0, WEIRD_ZONE).getMillis(), 0, WEIRD_TIME_ZONE_KEY));

        assertFunction("TIMESTAMP '2001-1-2 3:4:5.321+07:09'",
                createTimestampWithTimeZoneType(3),
                SqlTimestampWithTimeZone.newInstance(3, new DateTime(2001, 1, 2, 3, 4, 5, 321, WEIRD_ZONE).getMillis(), 0, WEIRD_TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-1-2 3:4:5+07:09'",
                createTimestampWithTimeZoneType(0),
                SqlTimestampWithTimeZone.newInstance(0, new DateTime(2001, 1, 2, 3, 4, 5, 0, WEIRD_ZONE).getMillis(), 0, WEIRD_TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-1-2 3:4+07:09'",
                createTimestampWithTimeZoneType(0),
                SqlTimestampWithTimeZone.newInstance(0, new DateTime(2001, 1, 2, 3, 4, 0, 0, WEIRD_ZONE).getMillis(), 0, WEIRD_TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-1-2+07:09'",
                createTimestampWithTimeZoneType(0),
                SqlTimestampWithTimeZone.newInstance(0, new DateTime(2001, 1, 2, 0, 0, 0, 0, WEIRD_ZONE).getMillis(), 0, WEIRD_TIME_ZONE_KEY));

        assertFunction("TIMESTAMP '2001-01-02 03:04:05.321 Europe/Berlin'",
                createTimestampWithTimeZoneType(3),
                SqlTimestampWithTimeZone.newInstance(3, new DateTime(2001, 1, 2, 3, 4, 5, 321, BERLIN_ZONE).getMillis(), 0, BERLIN_TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-01-02 03:04:05 Europe/Berlin'",
                createTimestampWithTimeZoneType(0),
                SqlTimestampWithTimeZone.newInstance(0, new DateTime(2001, 1, 2, 3, 4, 5, 0, BERLIN_ZONE).getMillis(), 0, BERLIN_TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-01-02 03:04 Europe/Berlin'",
                createTimestampWithTimeZoneType(0),
                SqlTimestampWithTimeZone.newInstance(0, new DateTime(2001, 1, 2, 3, 4, 0, 0, BERLIN_ZONE).getMillis(), 0, BERLIN_TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-01-02 Europe/Berlin'",
                createTimestampWithTimeZoneType(0),
                SqlTimestampWithTimeZone.newInstance(0, new DateTime(2001, 1, 2, 0, 0, 0, 0, BERLIN_ZONE).getMillis(), 0, BERLIN_TIME_ZONE_KEY));

        assertFunction("TIMESTAMP '12001-01-02 03:04:05.321 Europe/Berlin'",
                createTimestampWithTimeZoneType(3),
                SqlTimestampWithTimeZone.newInstance(3, new DateTime(12001, 1, 2, 3, 4, 5, 321, BERLIN_ZONE).getMillis(), 0, BERLIN_TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '+12001-01-02 03:04:05.321 Europe/Berlin'",
                createTimestampWithTimeZoneType(3),
                SqlTimestampWithTimeZone.newInstance(3, new DateTime(12001, 1, 2, 3, 4, 5, 321, BERLIN_ZONE).getMillis(), 0, BERLIN_TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '-12001-01-02 03:04:05.321 Europe/Berlin'",
                createTimestampWithTimeZoneType(3),
                SqlTimestampWithTimeZone.newInstance(3, new DateTime(-12001, 1, 2, 3, 4, 5, 321, BERLIN_ZONE).getMillis(), 0, BERLIN_TIME_ZONE_KEY));

        // Overflow
        assertInvalidFunction("TIMESTAMP '123001-01-02 03:04:05.321 Europe/Berlin'", INVALID_LITERAL, "line 1:1: '123001-01-02 03:04:05.321 Europe/Berlin' is not a valid timestamp literal");
        assertInvalidFunction("TIMESTAMP '+123001-01-02 03:04:05.321 Europe/Berlin'", INVALID_LITERAL, "line 1:1: '+123001-01-02 03:04:05.321 Europe/Berlin' is not a valid timestamp literal");
        assertInvalidFunction("TIMESTAMP '-123001-01-02 03:04:05.321 Europe/Berlin'", INVALID_LITERAL, "line 1:1: '-123001-01-02 03:04:05.321 Europe/Berlin' is not a valid timestamp literal");
    }

    @Test
    public void testSubtract()
    {
        functionAssertions.assertFunctionString("TIMESTAMP '2017-03-30 14:15:16.432 +07:09' - TIMESTAMP '2016-03-29 03:04:05.321 +08:09'",
                INTERVAL_DAY_TIME,
                "366 12:11:11.111");

        functionAssertions.assertFunctionString("TIMESTAMP '2016-03-29 03:04:05.321 +08:09' - TIMESTAMP '2017-03-30 14:15:16.432 +07:09'",
                INTERVAL_DAY_TIME,
                "-366 12:11:11.111");
    }

    @Test
    public void testEqual()
    {
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' = TIMESTAMP '2001-1-22 03:04:05.321 +07:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' = TIMESTAMP '2001-1-22 02:04:05.321 +06:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' = TIMESTAMP '2001-1-22 02:04:05.321'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 +07:09' = TIMESTAMP '2001-1-22 +07:09'", BOOLEAN, true);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' = TIMESTAMP '2001-1-22 03:04:05.333 +07:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' = TIMESTAMP '2001-1-22 02:04:05.333 +06:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' = TIMESTAMP '2001-1-22 02:04:05.333'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 +07:09' = TIMESTAMP '2001-1-11 +07:09'", BOOLEAN, false);
    }

    @Test
    public void testNotEqual()
    {
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' <> TIMESTAMP '2001-1-22 03:04:05.333 +07:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' <> TIMESTAMP '2001-1-22 02:04:05.333 +06:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' <> TIMESTAMP '2001-1-22 02:04:05.333'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 +07:09' <> TIMESTAMP '2001-1-11 +07:09'", BOOLEAN, true);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' <> TIMESTAMP '2001-1-22 03:04:05.321 +07:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' <> TIMESTAMP '2001-1-22 02:04:05.321 +06:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' <> TIMESTAMP '2001-1-22 02:04:05.321'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 +07:09' <> TIMESTAMP '2001-1-22 +07:09'", BOOLEAN, false);
    }

    @Test
    public void testLessThan()
    {
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' < TIMESTAMP '2001-1-22 03:04:05.333 +07:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' < TIMESTAMP '2001-1-22 02:04:05.333 +06:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' < TIMESTAMP '2001-1-22 02:04:05.333'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 +07:09' < TIMESTAMP '2001-1-23 +07:09'", BOOLEAN, true);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' < TIMESTAMP '2001-1-22 03:04:05.321 +07:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' < TIMESTAMP '2001-1-22 02:04:05.321 +06:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' < TIMESTAMP '2001-1-22 02:04:05.321'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' < TIMESTAMP '2001-1-22 03:04:05 +07:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' < TIMESTAMP '2001-1-22 02:04:05 +06:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' < TIMESTAMP '2001-1-22 02:04:05'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 +07:09' < TIMESTAMP '2001-1-22 +07:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 +07:09' < TIMESTAMP '2001-1-20 +07:09'", BOOLEAN, false);
    }

    @Test
    public void testLessThanOrEqual()
    {
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' <= TIMESTAMP '2001-1-22 03:04:05.333 +07:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' <= TIMESTAMP '2001-1-22 02:04:05.333 +06:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' <= TIMESTAMP '2001-1-22 02:04:05.333'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' <= TIMESTAMP '2001-1-22 03:04:05.321 +07:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' <= TIMESTAMP '2001-1-22 02:04:05.321 +06:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' <= TIMESTAMP '2001-1-22 02:04:05.321'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 +07:09' <= TIMESTAMP '2001-1-23 +07:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 +07:09' <= TIMESTAMP '2001-1-22 +07:09'", BOOLEAN, true);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' <= TIMESTAMP '2001-1-22 03:04:05 +07:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' <= TIMESTAMP '2001-1-22 02:04:05 +06:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' <= TIMESTAMP '2001-1-22 02:04:05'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 +07:09' <= TIMESTAMP '2001-1-20 +07:09'", BOOLEAN, false);
    }

    @Test
    public void testGreaterThan()
    {
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' > TIMESTAMP '2001-1-22 03:04:05.111 +07:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' > TIMESTAMP '2001-1-22 02:04:05.111 +06:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' > TIMESTAMP '2001-1-22 02:04:05.111'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 +07:09' > TIMESTAMP '2001-1-11 +07:09'", BOOLEAN, true);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' > TIMESTAMP '2001-1-22 03:04:05.321 +07:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' > TIMESTAMP '2001-1-22 02:04:05.321 +06:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' > TIMESTAMP '2001-1-22 02:04:05.321'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' > TIMESTAMP '2001-1-22 03:04:05.333 +07:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' > TIMESTAMP '2001-1-22 02:04:05.333 +06:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' > TIMESTAMP '2001-1-22 02:04:05.333'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 +07:09' > TIMESTAMP '2001-1-22 +07:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 +07:09' > TIMESTAMP '2001-1-23 +07:09'", BOOLEAN, false);
    }

    @Test
    public void testGreaterThanOrEqual()
    {
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' >= TIMESTAMP '2001-1-22 03:04:05.111 +07:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' >= TIMESTAMP '2001-1-22 02:04:05.111 +06:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' >= TIMESTAMP '2001-1-22 02:04:05.111'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' >= TIMESTAMP '2001-1-22 03:04:05.321 +07:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' >= TIMESTAMP '2001-1-22 02:04:05.321 +06:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' >= TIMESTAMP '2001-1-22 02:04:05.321'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 +07:09' >= TIMESTAMP '2001-1-11 +07:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 +07:09' >= TIMESTAMP '2001-1-22 +07:09'", BOOLEAN, true);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' >= TIMESTAMP '2001-1-22 03:04:05.333 +07:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' >= TIMESTAMP '2001-1-22 02:04:05.333 +06:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' >= TIMESTAMP '2001-1-22 02:04:05.333'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 +07:09' >= TIMESTAMP '2001-1-23 +07:09'", BOOLEAN, false);
    }

    @Test
    public void testBetween()
    {
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' between TIMESTAMP '2001-1-22 03:04:05.111 +07:09' and TIMESTAMP '2001-1-22 03:04:05.333 +07:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' between TIMESTAMP '2001-1-22 02:04:05.111 +06:09' and TIMESTAMP '2001-1-22 02:04:05.333 +06:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' between TIMESTAMP '2001-1-22 02:04:05.111' and TIMESTAMP '2001-1-22 02:04:05.333'", BOOLEAN, true);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' between TIMESTAMP '2001-1-22 03:04:05.321 +07:09' and TIMESTAMP '2001-1-22 03:04:05.333 +07:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' between TIMESTAMP '2001-1-22 02:04:05.321 +06:09' and TIMESTAMP '2001-1-22 02:04:05.333 +06:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' between TIMESTAMP '2001-1-22 02:04:05.321' and TIMESTAMP '2001-1-22 02:04:05.333'", BOOLEAN, true);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' between TIMESTAMP '2001-1-22 03:04:05.111 +07:09' and TIMESTAMP '2001-1-22 03:04:05.321 +07:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' between TIMESTAMP '2001-1-22 02:04:05.111 +06:09' and TIMESTAMP '2001-1-22 02:04:05.321 +06:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' between TIMESTAMP '2001-1-22 02:04:05.111' and TIMESTAMP '2001-1-22 02:04:05.321'", BOOLEAN, true);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' between TIMESTAMP '2001-1-22 03:04:05.321 +07:09' and TIMESTAMP '2001-1-22 03:04:05.321 +07:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' between TIMESTAMP '2001-1-22 02:04:05.321 +06:09' and TIMESTAMP '2001-1-22 02:04:05.321 +06:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' between TIMESTAMP '2001-1-22 02:04:05.321' and TIMESTAMP '2001-1-22 02:04:05.321'", BOOLEAN, true);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' between TIMESTAMP '2001-1-22 03:04:05.322 +07:09' and TIMESTAMP '2001-1-22 03:04:05.333 +07:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' between TIMESTAMP '2001-1-22 02:04:05.322 +06:09' and TIMESTAMP '2001-1-22 02:04:05.333 +06:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' between TIMESTAMP '2001-1-22 02:04:05.322' and TIMESTAMP '2001-1-22 02:04:05.333'", BOOLEAN, false);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' between TIMESTAMP '2001-1-22 03:04:05.311 +07:09' and TIMESTAMP '2001-1-22 03:04:05.312 +07:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' between TIMESTAMP '2001-1-22 02:04:05.311 +06:09' and TIMESTAMP '2001-1-22 02:04:05.312 +06:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' between TIMESTAMP '2001-1-22 02:04:05.311' and TIMESTAMP '2001-1-22 02:04:05.312'", BOOLEAN, false);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' between TIMESTAMP '2001-1-22 03:04:05.333 +07:09' and TIMESTAMP '2001-1-22 03:04:05.111 +07:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' between TIMESTAMP '2001-1-22 02:04:05.333 +06:09' and TIMESTAMP '2001-1-22 02:04:05.111 +06:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' between TIMESTAMP '2001-1-22 02:04:05.333' and TIMESTAMP '2001-1-22 02:04:05.111'", BOOLEAN, false);
    }

    @Test
    public void testCastToDate()
    {
        long millis = new DateTime(2001, 1, 22, 0, 0, UTC).getMillis();
        assertFunction("cast(TIMESTAMP '2001-1-22 03:04:05.321 +07:09' as date)", DATE, new SqlDate((int) TimeUnit.MILLISECONDS.toDays(millis)));
    }

    @Test
    public void testCastToTime()
    {
        assertFunction("cast(TIMESTAMP '2001-1-22 03:04:05.321 +07:09' as time)",
                TIME_MILLIS,
                sqlTimeOf(3, 4, 5, 321));

        functionAssertions.assertFunctionString("cast(TIMESTAMP '2001-1-22 03:04:05.321 +07:09' as time)",
                TIME_MILLIS,
                "03:04:05.321");
    }

    @Test
    public void testCastToTimestamp()
    {
        assertFunction("cast(TIMESTAMP '2001-1-22 03:04:05.321 +07:09' as timestamp)",
                TIMESTAMP_MILLIS,
                sqlTimestampOf(3, 2001, 1, 22, 3, 4, 5, 321));

        // This TZ had switch in 2014, so if we test for 2014 and used unpacked value we would use wrong shift
        assertFunction("cast(TIMESTAMP '2001-1-22 03:04:05.321 Pacific/Bougainville' as timestamp)",
                TIMESTAMP_MILLIS,
                sqlTimestampOf(3, 2001, 1, 22, 3, 4, 5, 321));
    }

    @Test
    public void testCastToVarchar()
    {
        assertFunction("cast(TIMESTAMP '2001-1-22 03:04:05.321 +07:09' as varchar)", VARCHAR, "2001-01-22 03:04:05.321 +07:09");
        assertFunction("cast(TIMESTAMP '2001-1-22 03:04:05 +07:09' as varchar)", VARCHAR, "2001-01-22 03:04:05 +07:09");
        assertFunction("cast(TIMESTAMP '2001-1-22 03:04 +07:09' as varchar)", VARCHAR, "2001-01-22 03:04:00 +07:09");
        assertFunction("cast(TIMESTAMP '2001-1-22 +07:09' as varchar)", VARCHAR, "2001-01-22 00:00:00 +07:09");
    }

    @Test
    public void testCastFromVarchar()
    {
        assertFunction("cast('2001-1-22 03:04:05.321' as timestamp with time zone)",
                TIMESTAMP_TZ_MILLIS,
                SqlTimestampWithTimeZone.newInstance(3, new DateTime(2001, 1, 22, 3, 4, 5, 321, DATE_TIME_ZONE).getMillis(), 0, TIME_ZONE_KEY));
        assertFunction("cast('2001-1-22 03:04:05' as timestamp with time zone)",
                TIMESTAMP_TZ_MILLIS,
                SqlTimestampWithTimeZone.newInstance(3, new DateTime(2001, 1, 22, 3, 4, 5, 0, DATE_TIME_ZONE).getMillis(), 0, TIME_ZONE_KEY));
        assertFunction("cast('2001-1-22 03:04' as timestamp with time zone)",
                TIMESTAMP_TZ_MILLIS,
                SqlTimestampWithTimeZone.newInstance(3, new DateTime(2001, 1, 22, 3, 4, 0, 0, DATE_TIME_ZONE).getMillis(), 0, TIME_ZONE_KEY));
        assertFunction("cast('2001-1-22' as timestamp with time zone)",
                TIMESTAMP_TZ_MILLIS,
                SqlTimestampWithTimeZone.newInstance(3, new DateTime(2001, 1, 22, 0, 0, 0, 0, DATE_TIME_ZONE).getMillis(), 0, TIME_ZONE_KEY));

        assertFunction("cast('2001-1-22 03:04:05.321 +07:09' as timestamp with time zone)",
                TIMESTAMP_TZ_MILLIS,
                SqlTimestampWithTimeZone.newInstance(3, new DateTime(2001, 1, 22, 3, 4, 5, 321, WEIRD_ZONE).getMillis(), 0, WEIRD_TIME_ZONE_KEY));
        assertFunction("cast('2001-1-22 03:04:05 +07:09' as timestamp with time zone)",
                TIMESTAMP_TZ_MILLIS,
                SqlTimestampWithTimeZone.newInstance(3, new DateTime(2001, 1, 22, 3, 4, 5, 0, WEIRD_ZONE).getMillis(), 0, WEIRD_TIME_ZONE_KEY));
        assertFunction("cast('2001-1-22 03:04 +07:09' as timestamp with time zone)",
                TIMESTAMP_TZ_MILLIS,
                SqlTimestampWithTimeZone.newInstance(3, new DateTime(2001, 1, 22, 3, 4, 0, 0, WEIRD_ZONE).getMillis(), 0, WEIRD_TIME_ZONE_KEY));
        assertFunction("cast('2001-1-22 +07:09' as timestamp with time zone)",
                TIMESTAMP_TZ_MILLIS,
                SqlTimestampWithTimeZone.newInstance(3, new DateTime(2001, 1, 22, 0, 0, 0, 0, WEIRD_ZONE).getMillis(), 0, WEIRD_TIME_ZONE_KEY));

        assertFunction("cast('2001-1-22 03:04:05.321 Europe/Berlin' as timestamp with time zone)",
                TIMESTAMP_TZ_MILLIS,
                SqlTimestampWithTimeZone.newInstance(3, new DateTime(2001, 1, 22, 3, 4, 5, 321, BERLIN_ZONE).getMillis(), 0, BERLIN_TIME_ZONE_KEY));
        assertFunction("cast('2001-1-22 03:04:05 Europe/Berlin' as timestamp with time zone)",
                TIMESTAMP_TZ_MILLIS,
                SqlTimestampWithTimeZone.newInstance(3, new DateTime(2001, 1, 22, 3, 4, 5, 0, BERLIN_ZONE).getMillis(), 0, BERLIN_TIME_ZONE_KEY));
        assertFunction("cast('2001-1-22 03:04 Europe/Berlin' as timestamp with time zone)",
                TIMESTAMP_TZ_MILLIS,
                SqlTimestampWithTimeZone.newInstance(3, new DateTime(2001, 1, 22, 3, 4, 0, 0, BERLIN_ZONE).getMillis(), 0, BERLIN_TIME_ZONE_KEY));
        assertFunction("cast('2001-1-22 Europe/Berlin' as timestamp with time zone)",
                TIMESTAMP_TZ_MILLIS,
                SqlTimestampWithTimeZone.newInstance(3, new DateTime(2001, 1, 22, 0, 0, 0, 0, BERLIN_ZONE).getMillis(), 0, BERLIN_TIME_ZONE_KEY));

        assertFunction("cast('\n\t 2001-1-22 03:04:05.321 Europe/Berlin' as timestamp with time zone)",
                TIMESTAMP_TZ_MILLIS,
                SqlTimestampWithTimeZone.newInstance(3, new DateTime(2001, 1, 22, 3, 4, 5, 321, BERLIN_ZONE).getMillis(), 0, BERLIN_TIME_ZONE_KEY));
        assertFunction("cast('2001-1-22 03:04:05.321 Europe/Berlin \t\n' as timestamp with time zone)",
                TIMESTAMP_TZ_MILLIS,
                SqlTimestampWithTimeZone.newInstance(3, new DateTime(2001, 1, 22, 3, 4, 5, 321, BERLIN_ZONE).getMillis(), 0, BERLIN_TIME_ZONE_KEY));
        assertFunction("cast('\n\t 2001-1-22 03:04:05.321 Europe/Berlin \t\n' as timestamp with time zone)",
                TIMESTAMP_TZ_MILLIS,
                SqlTimestampWithTimeZone.newInstance(3, new DateTime(2001, 1, 22, 3, 4, 5, 321, BERLIN_ZONE).getMillis(), 0, BERLIN_TIME_ZONE_KEY));
    }

    @Test
    public void testGreatest()
    {
        assertFunction(
                "greatest(TIMESTAMP '2002-01-02 03:04:05.321 +07:09', TIMESTAMP '2001-01-02 01:04:05.321 +02:09', TIMESTAMP '2000-01-02 01:04:05.321 +02:09')",
                TIMESTAMP_TZ_MILLIS,
                SqlTimestampWithTimeZone.newInstance(3, new DateTime(2002, 1, 2, 3, 4, 5, 321, WEIRD_ZONE).getMillis(), 0, WEIRD_TIME_ZONE_KEY));
        assertFunction(
                "greatest(TIMESTAMP '2001-01-02 03:04:05.321 +07:09', TIMESTAMP '2001-01-02 04:04:05.321 +10:09')",
                TIMESTAMP_TZ_MILLIS,
                SqlTimestampWithTimeZone.newInstance(3, new DateTime(2001, 1, 2, 3, 4, 5, 321, WEIRD_ZONE).getMillis(), 0, WEIRD_TIME_ZONE_KEY));
    }

    @Test
    public void testLeast()
    {
        assertFunction(
                "least(TIMESTAMP '2001-01-02 03:04:05.321 +07:09', TIMESTAMP '2001-01-02 01:04:05.321 +02:09', TIMESTAMP '2002-01-02 01:04:05.321 +02:09')",
                TIMESTAMP_TZ_MILLIS,
                SqlTimestampWithTimeZone.newInstance(3, new DateTime(2001, 1, 2, 3, 4, 5, 321, WEIRD_ZONE).getMillis(), 0, WEIRD_TIME_ZONE_KEY));
        assertFunction(
                "least(TIMESTAMP '2001-01-02 03:04:05.321 +07:09', TIMESTAMP '2001-01-02 01:04:05.321 +02:09')",
                TIMESTAMP_TZ_MILLIS,
                SqlTimestampWithTimeZone.newInstance(3, new DateTime(2001, 1, 2, 3, 4, 5, 321, WEIRD_ZONE).getMillis(), 0, WEIRD_TIME_ZONE_KEY));
    }

    @Test
    public void testIndeterminate()
    {
        assertOperator(INDETERMINATE, "cast(null as TIMESTAMP WITH TIME ZONE)", BOOLEAN, true);
        assertOperator(INDETERMINATE, "TIMESTAMP '2001-01-02 01:04:05.321 +02:09'", BOOLEAN, false);
    }
}
