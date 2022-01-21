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
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.DateTimeTestingUtils.sqlTimestampOf;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.joda.time.DateTimeZone.UTC;

public class TestDateTimeOperators
        extends AbstractTestFunctions
{
    private static final TimeZoneKey TIME_ZONE_KEY = getTimeZoneKey("Europe/Berlin");
    private static final DateTimeZone WEIRD_TIME_ZONE = DateTimeZone.forOffsetHoursMinutes(5, 9);
    private static final TimeZoneKey WEIRD_TIME_ZONE_KEY = getTimeZoneKeyForOffset(5 * 60 + 9);

    public TestDateTimeOperators()
    {
        super(testSessionBuilder()
                .setTimeZoneKey(TIME_ZONE_KEY)
                .build());
    }

    @Test
    public void testTimeZoneGap()
    {
        // No time zone gap should be applied

        assertFunction(
                "TIMESTAMP '2013-03-31 00:05' + INTERVAL '1' hour",
                createTimestampType(3),
                sqlTimestampOf(3, 2013, 3, 31, 1, 5, 0, 0));
        session.toConnectorSession();
        assertFunction(
                "TIMESTAMP '2013-03-31 00:05' + INTERVAL '2' hour",
                createTimestampType(3),
                sqlTimestampOf(3, 2013, 3, 31, 2, 5, 0, 0));
        session.toConnectorSession();
        assertFunction(
                "TIMESTAMP '2013-03-31 00:05' + INTERVAL '3' hour",
                createTimestampType(3),
                sqlTimestampOf(3, 2013, 3, 31, 3, 5, 0, 0));

        assertFunction(
                "TIMESTAMP '2013-03-31 04:05' - INTERVAL '3' hour",
                createTimestampType(3),
                sqlTimestampOf(3, 2013, 3, 31, 1, 5, 0, 0));
        session.toConnectorSession();
        assertFunction(
                "TIMESTAMP '2013-03-31 03:05' - INTERVAL '2' hour",
                createTimestampType(3),
                sqlTimestampOf(3, 2013, 3, 31, 1, 5, 0, 0));
        session.toConnectorSession();
        assertFunction(
                "TIMESTAMP '2013-03-31 01:05' - INTERVAL '1' hour",
                createTimestampType(3),
                sqlTimestampOf(3, 2013, 3, 31, 0, 5, 0, 0));
    }

    @Test
    public void testDaylightTimeSaving()
    {
        assertFunction(
                "TIMESTAMP '2013-10-27 00:05' + INTERVAL '1' hour",
                createTimestampType(3),
                sqlTimestampOf(3, 2013, 10, 27, 1, 5, 0, 0));
        session.toConnectorSession();
        assertFunction(
                "TIMESTAMP '2013-10-27 00:05' + INTERVAL '2' hour",
                createTimestampType(3),
                sqlTimestampOf(3, 2013, 10, 27, 2, 5, 0, 0));

        assertFunction(
                "TIMESTAMP '2013-10-27 00:05' + INTERVAL '3' hour",
                createTimestampType(3),
                sqlTimestampOf(3, 2013, 10, 27, 3, 5, 0, 0));
        session.toConnectorSession();
        assertFunction(
                "TIMESTAMP '2013-10-27 00:05' + INTERVAL '4' hour",
                createTimestampType(3),
                sqlTimestampOf(3, 2013, 10, 27, 4, 5, 0, 0));

        assertFunction(
                "TIMESTAMP '2013-10-27 03:05' - INTERVAL '4' hour",
                createTimestampType(3),
                sqlTimestampOf(3, 2013, 10, 26, 23, 5, 0, 0));
        session.toConnectorSession();
        assertFunction(
                "TIMESTAMP '2013-10-27 02:05' - INTERVAL '2' hour",
                createTimestampType(3),
                sqlTimestampOf(3, 2013, 10, 27, 0, 5, 0, 0));
        session.toConnectorSession();
        assertFunction(
                "TIMESTAMP '2013-10-27 01:05' - INTERVAL '1' hour",
                createTimestampType(3),
                sqlTimestampOf(3, 2013, 10, 27, 0, 5, 0, 0));

        assertFunction(
                "TIMESTAMP '2013-10-27 03:05' - INTERVAL '1' hour",
                createTimestampType(3),
                sqlTimestampOf(3, 2013, 10, 27, 2, 5, 0, 0));
        session.toConnectorSession();
        assertFunction(
                "TIMESTAMP '2013-10-27 03:05' - INTERVAL '2' hour",
                createTimestampType(3),
                sqlTimestampOf(3, 2013, 10, 27, 1, 5, 0, 0));
    }

    @Test
    public void testDatePlusInterval()
    {
        assertFunction("DATE '2001-1-22' + INTERVAL '3' day", DATE, toDate(new DateTime(2001, 1, 25, 0, 0, 0, 0, UTC)));
        assertFunction("INTERVAL '3' day + DATE '2001-1-22'", DATE, toDate(new DateTime(2001, 1, 25, 0, 0, 0, 0, UTC)));
        assertFunction("DATE '2001-1-22' + INTERVAL '3' month", DATE, toDate(new DateTime(2001, 4, 22, 0, 0, 0, 0, UTC)));
        assertFunction("INTERVAL '3' month + DATE '2001-1-22'", DATE, toDate(new DateTime(2001, 4, 22, 0, 0, 0, 0, UTC)));
        assertFunction("DATE '2001-1-22' + INTERVAL '3' year", DATE, toDate(new DateTime(2004, 1, 22, 0, 0, 0, 0, UTC)));
        assertFunction("INTERVAL '3' year + DATE '2001-1-22'", DATE, toDate(new DateTime(2004, 1, 22, 0, 0, 0, 0, UTC)));

        assertInvalidFunction("DATE '2001-1-22' + INTERVAL '3' hour", "Cannot add hour, minutes or seconds to a date");
        assertInvalidFunction("INTERVAL '3' hour + DATE '2001-1-22'", "Cannot add hour, minutes or seconds to a date");
    }

    @Test
    public void testTimestampPlusInterval()
    {
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' + INTERVAL '3' hour",
                TIMESTAMP_MILLIS,
                sqlTimestampOf(3, 2001, 1, 22, 6, 4, 5, 321));
        assertFunction("INTERVAL '3' hour + TIMESTAMP '2001-1-22 03:04:05.321'",
                TIMESTAMP_MILLIS,
                sqlTimestampOf(3, 2001, 1, 22, 6, 4, 5, 321));
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' + INTERVAL '3' day",
                TIMESTAMP_MILLIS,
                sqlTimestampOf(3, 2001, 1, 25, 3, 4, 5, 321));
        assertFunction("INTERVAL '3' day + TIMESTAMP '2001-1-22 03:04:05.321'",
                TIMESTAMP_MILLIS,
                sqlTimestampOf(3, 2001, 1, 25, 3, 4, 5, 321));
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' + INTERVAL '3' month",
                TIMESTAMP_MILLIS,
                sqlTimestampOf(3, 2001, 4, 22, 3, 4, 5, 321));
        assertFunction("INTERVAL '3' month + TIMESTAMP '2001-1-22 03:04:05.321'",
                TIMESTAMP_MILLIS,
                sqlTimestampOf(3, 2001, 4, 22, 3, 4, 5, 321));
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' + INTERVAL '3' year",
                TIMESTAMP_MILLIS,
                sqlTimestampOf(3, 2004, 1, 22, 3, 4, 5, 321));
        assertFunction("INTERVAL '3' year + TIMESTAMP '2001-1-22 03:04:05.321'",
                TIMESTAMP_MILLIS,
                sqlTimestampOf(3, 2004, 1, 22, 3, 4, 5, 321));

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +05:09' + INTERVAL '3' hour",
                TIMESTAMP_TZ_MILLIS,
                SqlTimestampWithTimeZone.newInstance(3, new DateTime(2001, 1, 22, 6, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), 0, WEIRD_TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' hour + TIMESTAMP '2001-1-22 03:04:05.321 +05:09'",
                TIMESTAMP_TZ_MILLIS,
                SqlTimestampWithTimeZone.newInstance(3, new DateTime(2001, 1, 22, 6, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), 0, WEIRD_TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +05:09' + INTERVAL '3' day",
                TIMESTAMP_TZ_MILLIS,
                SqlTimestampWithTimeZone.newInstance(3, new DateTime(2001, 1, 25, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), 0, WEIRD_TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' day + TIMESTAMP '2001-1-22 03:04:05.321 +05:09'",
                TIMESTAMP_TZ_MILLIS,
                SqlTimestampWithTimeZone.newInstance(3, new DateTime(2001, 1, 25, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), 0, WEIRD_TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +05:09' + INTERVAL '3' month",
                TIMESTAMP_TZ_MILLIS,
                SqlTimestampWithTimeZone.newInstance(3, new DateTime(2001, 4, 22, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), 0, WEIRD_TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' month + TIMESTAMP '2001-1-22 03:04:05.321 +05:09'",
                TIMESTAMP_TZ_MILLIS,
                SqlTimestampWithTimeZone.newInstance(3, new DateTime(2001, 4, 22, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), 0, WEIRD_TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +05:09' + INTERVAL '3' year",
                TIMESTAMP_TZ_MILLIS,
                SqlTimestampWithTimeZone.newInstance(3, new DateTime(2004, 1, 22, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), 0, WEIRD_TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' year + TIMESTAMP '2001-1-22 03:04:05.321 +05:09'",
                TIMESTAMP_TZ_MILLIS,
                SqlTimestampWithTimeZone.newInstance(3, new DateTime(2004, 1, 22, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), 0, WEIRD_TIME_ZONE_KEY));
    }

    @Test
    public void testDateMinusInterval()
    {
        assertFunction("DATE '2001-1-22' - INTERVAL '3' day", DATE, toDate(new DateTime(2001, 1, 19, 0, 0, 0, 0, UTC)));

        assertInvalidFunction("DATE '2001-1-22' - INTERVAL '3' hour", "Cannot subtract hour, minutes or seconds from a date");
    }

    @Test
    public void testTimestampMinusInterval()
    {
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' - INTERVAL '3' day",
                TIMESTAMP_MILLIS,
                sqlTimestampOf(3, 2001, 1, 19, 3, 4, 5, 321));
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +05:09' - INTERVAL '3' day",
                TIMESTAMP_TZ_MILLIS,
                SqlTimestampWithTimeZone.newInstance(3, new DateTime(2001, 1, 19, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), 0, WEIRD_TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' - INTERVAL '3' month",
                TIMESTAMP_MILLIS,
                sqlTimestampOf(3, 2000, 10, 22, 3, 4, 5, 321));
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +05:09' - INTERVAL '3' month",
                TIMESTAMP_TZ_MILLIS,
                SqlTimestampWithTimeZone.newInstance(3, new DateTime(2000, 10, 22, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), 0, WEIRD_TIME_ZONE_KEY));
    }

    @Test
    public void testDateToTimestampCoercing()
    {
        assertFunction("date_format(DATE '2013-10-27', '%Y-%m-%d %H:%i:%s')", VARCHAR, "2013-10-27 00:00:00");

        assertFunction("DATE '2013-10-27' = TIMESTAMP '2013-10-27 00:00:00'", BOOLEAN, true);
        assertFunction("DATE '2013-10-27' < TIMESTAMP '2013-10-27 00:00:01'", BOOLEAN, true);
        assertFunction("DATE '2013-10-27' > TIMESTAMP '2013-10-26 23:59:59'", BOOLEAN, true);
    }

    @Test
    public void testDateToTimestampWithZoneCoercing()
    {
        assertFunction("DATE '2013-10-27' = TIMESTAMP '2013-10-27 00:00:00 Europe/Berlin'", BOOLEAN, true);
        assertFunction("DATE '2013-10-27' < TIMESTAMP '2013-10-27 00:00:01 Europe/Berlin'", BOOLEAN, true);
        assertFunction("DATE '2013-10-27' > TIMESTAMP '2013-10-26 23:59:59 Europe/Berlin'", BOOLEAN, true);
    }

    @Test
    public void testIsDistinctFrom()
    {
        assertFunction("CAST(NULL AS DATE) IS DISTINCT FROM CAST(NULL AS DATE)", BOOLEAN, false);
        assertFunction("DATE '2013-10-27' IS DISTINCT FROM TIMESTAMP '2013-10-27 00:00:00'", BOOLEAN, false);
        assertFunction("DATE '2013-10-27' IS DISTINCT FROM TIMESTAMP '2013-10-28 00:00:00'", BOOLEAN, true);
        assertFunction("NULL IS DISTINCT FROM DATE '2013-10-27'", BOOLEAN, true);
        assertFunction("DATE '2013-10-27' IS DISTINCT FROM NULL", BOOLEAN, true);
    }

    @Test
    public void testDateCastFromVarchar()
    {
        assertFunction("DATE '2013-02-02'", DATE, toDate(new DateTime(2013, 2, 2, 0, 0, 0, 0, UTC)));
        assertInvalidFunction("DATE '5881580-07-12'", INVALID_LITERAL, "line 1:1: '5881580-07-12' is not a valid date literal");
        assertInvalidFunction("DATE '392251590-07-12'", INVALID_LITERAL, "line 1:1: '392251590-07-12' is not a valid date literal");
    }

    @Test
    public void testDateCastToVarchar()
    {
        assertFunction("cast(DATE '2013-02-02' AS varchar)", VARCHAR, "2013-02-02");
        // according to the SQL standard, this literal is incorrect. The required format is 'YYYY-MM-DD'. https://github.com/trinodb/trino/issues/10677
        assertFunction("cast(DATE '13-2-2' AS varchar)", VARCHAR, "0013-02-02");
        assertFunction("cast(DATE '2013-02-02' AS varchar(50))", createVarcharType(50), "2013-02-02");
        assertFunction("cast(DATE '2013-02-02' AS varchar(10))", createVarcharType(10), "2013-02-02");
        assertInvalidCast("cast(DATE '2013-02-02' AS varchar(9))", "Value 2013-02-02 cannot be represented as varchar(9)");
    }

    private static SqlDate toDate(DateTime dateTime)
    {
        return new SqlDate((int) TimeUnit.MILLISECONDS.toDays(dateTime.getMillis()));
    }
}
