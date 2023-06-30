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
package io.trino.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.spi.type.TimeZoneKey;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.TestingConnectorSession;
import io.trino.testing.TestingSession;
import io.trino.type.SqlIntervalDayTime;
import org.joda.time.chrono.ISOChronology;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static io.trino.operator.scalar.DateTimeFunctions.currentDate;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.testng.Assert.assertEquals;

@TestInstance(PER_CLASS)
public class TestDateTimeFunctions
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testToIso8601ForTimestampWithoutTimeZone()
    {
        assertThat(assertions.function("to_iso8601", "TIMESTAMP '2001-08-22 03:04:05.321'"))
                .hasType(createVarcharType(26))
                .isEqualTo("2001-08-22T03:04:05.321");
    }

    @Test
    public void testCurrentDate()
    {
        Session session = Session.builder(assertions.getDefaultSession())
                .setStart(ZonedDateTime.of(2017, 4, 1, 12, 34, 56, 789, ZoneId.of("UTC")).toInstant())
                .build();

        // current date is the time at midnight in the session time zone
        assertThat(assertions.expression("current_date", session))
                .matches("DATE '2017-04-02'");
    }

    @Test
    public void testCurrentDateTimezone()
    {
        TimeZoneKey kievTimeZoneKey = getTimeZoneKey("Europe/Kiev");
        TimeZoneKey bahiaBanderasTimeZoneKey = getTimeZoneKey("America/Bahia_Banderas"); // The zone has 'gap' on 1970-01-01
        TimeZoneKey montrealTimeZoneKey = getTimeZoneKey("America/Montreal");
        long timeIncrement = TimeUnit.MINUTES.toMillis(53);
        // We expect UTC millis later on so we have to use UTC chronology
        for (long millis = ISOChronology.getInstanceUTC().getDateTimeMillis(2000, 6, 15, 0, 0, 0, 0);
                millis < ISOChronology.getInstanceUTC().getDateTimeMillis(2016, 6, 15, 0, 0, 0, 0);
                millis += timeIncrement) {
            Instant instant = Instant.ofEpochMilli(millis);
            assertCurrentDateAtInstant(kievTimeZoneKey, instant);
            assertCurrentDateAtInstant(bahiaBanderasTimeZoneKey, instant);
            assertCurrentDateAtInstant(montrealTimeZoneKey, instant);
            assertCurrentDateAtInstant(TestingSession.DEFAULT_TIME_ZONE_KEY, instant);
        }
    }

    private void assertCurrentDateAtInstant(TimeZoneKey timeZoneKey, Instant instant)
    {
        long expectedDays = epochDaysInZone(timeZoneKey, instant);
        TestingConnectorSession connectorSession = TestingConnectorSession.builder()
                .setStart(instant)
                .setTimeZoneKey(timeZoneKey)
                .build();
        long dateTimeCalculation = currentDate(connectorSession);
        assertEquals(dateTimeCalculation, expectedDays);
    }

    private static long epochDaysInZone(TimeZoneKey timeZoneKey, Instant instant)
    {
        return LocalDate.from(instant.atZone(timeZoneKey.getZoneId())).toEpochDay();
    }

    @Test
    public void testFromUnixTime()
    {
        assertThat(assertions.function("from_unixtime", "980172245"))
                .matches("TIMESTAMP '2001-01-22 03:04:05.000 Pacific/Apia'");

        assertThat(assertions.function("from_unixtime", "980172245.888"))
                .matches("TIMESTAMP '2001-01-22 03:04:05.888 Pacific/Apia'");
    }

    @Test
    public void testFromUnixTimeNanos()
    {
        // long
        assertThat(assertions.function("from_unixtime_nanos", "1234567890123456789"))
                .matches("TIMESTAMP '2009-02-13 12:31:30.123456789 Pacific/Apia'");

        assertThat(assertions.function("from_unixtime_nanos", "999999999"))
                .matches("TIMESTAMP '1969-12-31 13:00:00.999999999 Pacific/Apia'");

        assertThat(assertions.function("from_unixtime_nanos", "-1234567890123456789"))
                .matches("TIMESTAMP '1930-11-17 12:58:29.876543211 Pacific/Apia'");

        assertThat(assertions.function("from_unixtime_nanos", "-999999999"))
                .matches("TIMESTAMP '1969-12-31 12:59:59.000000001 Pacific/Apia'");

        // short decimal
        assertThat(assertions.function("from_unixtime_nanos", "DECIMAL '1234'"))
                .matches("TIMESTAMP '1969-12-31 13:00:00.000001234 Pacific/Apia'");

        assertThat(assertions.function("from_unixtime_nanos", "DECIMAL '1234.0'"))
                .matches("TIMESTAMP '1969-12-31 13:00:00.000001234 Pacific/Apia'");

        assertThat(assertions.function("from_unixtime_nanos", "DECIMAL '1234.499'"))
                .matches("TIMESTAMP '1969-12-31 13:00:00.000001234 Pacific/Apia'");

        assertThat(assertions.function("from_unixtime_nanos", "DECIMAL '1234.500'"))
                .matches("TIMESTAMP '1969-12-31 13:00:00.000001235 Pacific/Apia'");

        assertThat(assertions.function("from_unixtime_nanos", "DECIMAL '-1234'"))
                .matches("TIMESTAMP '1969-12-31 12:59:59.999998766 Pacific/Apia'");

        assertThat(assertions.function("from_unixtime_nanos", "DECIMAL '-1234.0'"))
                .matches("TIMESTAMP '1969-12-31 12:59:59.999998766 Pacific/Apia'");

        assertThat(assertions.function("from_unixtime_nanos", "DECIMAL '-1234.499'"))
                .matches("TIMESTAMP '1969-12-31 12:59:59.999998766 Pacific/Apia'");

        assertThat(assertions.function("from_unixtime_nanos", "DECIMAL '-1234.500'"))
                .matches("TIMESTAMP '1969-12-31 12:59:59.999998765 Pacific/Apia'");

        // long decimal
        assertThat(assertions.function("from_unixtime_nanos", "DECIMAL '12345678900123456789'"))
                .matches("TIMESTAMP '2361-03-22 08:15:00.123456789 Pacific/Apia'");

        assertThat(assertions.function("from_unixtime_nanos", "DECIMAL '12345678900123456789.000000'"))
                .matches("TIMESTAMP '2361-03-22 08:15:00.123456789 Pacific/Apia'");

        assertThat(assertions.function("from_unixtime_nanos", "DECIMAL '12345678900123456789.499'"))
                .matches("TIMESTAMP '2361-03-22 08:15:00.123456789 Pacific/Apia'");

        assertThat(assertions.function("from_unixtime_nanos", "DECIMAL '12345678900123456789.500'"))
                .matches("TIMESTAMP '2361-03-22 08:15:00.123456790 Pacific/Apia'");

        assertThat(assertions.function("from_unixtime_nanos", "DECIMAL '-12345678900123456789'"))
                .matches("TIMESTAMP '1578-10-13 17:18:03.876543211 Pacific/Apia'");

        assertThat(assertions.function("from_unixtime_nanos", "DECIMAL '-12345678900123456789.000000'"))
                .matches("TIMESTAMP '1578-10-13 17:18:03.876543211 Pacific/Apia'");

        assertThat(assertions.function("from_unixtime_nanos", "DECIMAL '-12345678900123456789.499'"))
                .matches("TIMESTAMP '1578-10-13 17:18:03.876543211 Pacific/Apia'");

        assertThat(assertions.function("from_unixtime_nanos", "DECIMAL '-12345678900123456789.500'"))
                .matches("TIMESTAMP '1578-10-13 17:18:03.876543210 Pacific/Apia'");
    }

    @Test
    public void testFromUnixTimeWithOffset()
    {
        assertThat(assertions.function("from_unixtime", "980172245", "1", "10"))
                .matches("TIMESTAMP '2001-01-22 15:14:05.000 +01:10'");

        // test invalid minute offsets
        assertTrinoExceptionThrownBy(() -> assertions.function("from_unixtime", "0", "1", "10000").evaluate())
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.function("from_unixtime", "0", "10000", "0").evaluate())
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.function("from_unixtime", "0", "-100", "100").evaluate())
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testFromUnixTimeWithTimeZone()
    {
        assertThat(assertions.function("from_unixtime", "7200", "'Asia/Shanghai'"))
                .matches("TIMESTAMP '1970-01-01 10:00:00.000 Asia/Shanghai'");

        assertThat(assertions.function("from_unixtime", "7200", "'Asia/Tokyo'"))
                .matches("TIMESTAMP '1970-01-01 11:00:00.000 Asia/Tokyo'");

        assertThat(assertions.function("from_unixtime", "7200", "'Europe/Kiev'"))
                .matches("TIMESTAMP '1970-01-01 05:00:00.000 Europe/Kiev'");

        assertThat(assertions.function("from_unixtime", "7200", "'America/New_York'"))
                .matches("TIMESTAMP '1969-12-31 21:00:00.000 America/New_York'");

        assertThat(assertions.function("from_unixtime", "7200", "'America/Chicago'"))
                .matches("TIMESTAMP '1969-12-31 20:00:00.000 America/Chicago'");

        assertThat(assertions.function("from_unixtime", "7200", "'America/Los_Angeles'"))
                .matches("TIMESTAMP '1969-12-31 18:00:00.000 America/Los_Angeles'");
    }

    @Test
    public void testDate()
    {
        assertThat(assertions.function("date", "'2001-08-22'"))
                .matches("DATE '2001-08-22'");

        assertThat(assertions.function("date", "TIMESTAMP '2001-08-22 03:04:05.321 +07:09'"))
                .matches("DATE '2001-08-22'");

        assertThat(assertions.function("date", "TIMESTAMP '2001-08-22 03:04:05.321'"))
                .matches("DATE '2001-08-22'");
    }

    @Test
    public void testFromISO8601()
    {
        assertThat(assertions.function("from_iso8601_timestamp", "'2001-08-22T03:04:05.321-11:00'"))
                .matches("TIMESTAMP '2001-08-22 03:04:05.321 -11:00'");

        assertThat(assertions.function("from_iso8601_timestamp", "'2001-08-22T03:04:05.321+07:09'"))
                .matches("TIMESTAMP '2001-08-22 03:04:05.321 +07:09'");

        assertThat(assertions.function("from_iso8601_date", "'2001-08-22'"))
                .matches("DATE '2001-08-22'");
    }

    @Test
    public void testFromIso8601Nanos()
    {
        assertThat(assertions.function("from_iso8601_timestamp_nanos", "'2001-08-22T12:34:56.123456789Z'"))
                .matches("TIMESTAMP '2001-08-22 12:34:56.123456789 UTC'");

        assertThat(assertions.function("from_iso8601_timestamp_nanos", "'2001-08-22T07:34:56.123456789-05:00'"))
                .matches("TIMESTAMP '2001-08-22 07:34:56.123456789 -05:00'");

        assertThat(assertions.function("from_iso8601_timestamp_nanos", "'2001-08-22T13:34:56.123456789+01:00'"))
                .matches("TIMESTAMP '2001-08-22 13:34:56.123456789 +01:00'");

        assertThat(assertions.function("from_iso8601_timestamp_nanos", "'2001-08-22T12:34:56.123Z'"))
                .matches("TIMESTAMP '2001-08-22 12:34:56.123000000 UTC'");

        // make sure that strings without a timezone are parsed in the session local time
        Session session = Session.builder(assertions.getDefaultSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(ZoneId.of("-0900").getId()))
                .build();

        assertThat(assertions.expression("from_iso8601_timestamp_nanos('2001-08-22T03:34:56.123456789')", session))
                .matches("TIMESTAMP '2001-08-22 03:34:56.123456789 -09:00'");
    }

    @Test
    public void testToIso8601()
    {
        assertThat(assertions.function("to_iso8601", "DATE '2001-08-22'"))
                .hasType(createVarcharType(16))
                .isEqualTo("2001-08-22");
    }

    @Test
    public void testTimeZone()
    {
        assertThat(assertions.expression("current_timezone()"))
                .hasType(VARCHAR)
                .isEqualTo("Pacific/Apia");
    }

    @Test
    public void testPartFunctions()
    {
        assertThat(assertions.function("millisecond", "INTERVAL '90061.234' SECOND"))
                .isEqualTo(234L);

        assertThat(assertions.function("second", "INTERVAL '90061.234' SECOND"))
                .isEqualTo(1L);

        assertThat(assertions.function("minute", "INTERVAL '90061.234' SECOND"))
                .isEqualTo(1L);

        assertThat(assertions.function("hour", "INTERVAL '90061.234' SECOND"))
                .isEqualTo(1L);
    }

    @Test
    public void testYearOfWeek()
    {
        assertThat(assertions.function("year_of_week", "DATE '2001-08-22'"))
                .isEqualTo(2001L);

        assertThat(assertions.function("yow", "DATE '2001-08-22'"))
                .isEqualTo(2001L);

        assertThat(assertions.function("year_of_week", "DATE '2005-01-02'"))
                .isEqualTo(2004L);

        assertThat(assertions.function("year_of_week", "DATE '2008-12-28'"))
                .isEqualTo(2008L);

        assertThat(assertions.function("year_of_week", "DATE '2008-12-29'"))
                .isEqualTo(2009L);

        assertThat(assertions.function("year_of_week", "DATE '2009-12-31'"))
                .isEqualTo(2009L);

        assertThat(assertions.function("year_of_week", "DATE '2010-01-03'"))
                .isEqualTo(2009L);
    }

    @Test
    public void testLastDayOfMonth()
    {
        assertThat(assertions.function("last_day_of_month", "DATE '2001-08-22'"))
                .matches("DATE '2001-08-31'");

        assertThat(assertions.function("last_day_of_month", "DATE '2019-08-01'"))
                .matches("DATE '2019-08-31'");

        assertThat(assertions.function("last_day_of_month", "DATE '2019-08-31'"))
                .matches("DATE '2019-8-31'");

        assertThat(assertions.expression("last_day_of_month(TIMESTAMP '2001-08-22 03:04:05.321')"))
                .matches("DATE '2001-08-31'");

        assertThat(assertions.function("last_day_of_month", "TIMESTAMP '2019-08-01 00:00:00.000'"))
                .matches("DATE '2019-8-31'");

        assertThat(assertions.function("last_day_of_month", "TIMESTAMP '2019-08-01 17:00:00.000'"))
                .matches("DATE '2019-8-31'");

        assertThat(assertions.function("last_day_of_month", "TIMESTAMP '2019-08-01 23:59:59.999'"))
                .matches("DATE '2019-8-31'");

        assertThat(assertions.function("last_day_of_month", "TIMESTAMP '2019-08-31 23:59:59.999'"))
                .matches("DATE '2019-8-31'");

        assertThat(assertions.function("last_day_of_month", "TIMESTAMP '2001-08-22 03:04:05.321 +07:09'"))
                .matches("DATE '2001-08-31'");

        ImmutableList.of("+05:45", "+00:00", "-05:45", "Asia/Tokyo", "Europe/London", "America/Los_Angeles", "America/Bahia_Banderas").forEach(timeZone -> {
            assertThat(assertions.function("last_day_of_month", "TIMESTAMP '2018-12-31 17:00:00.000 " + timeZone + "'"))
                    .matches("DATE '2018-12-31'");

            assertThat(assertions.function("last_day_of_month", "TIMESTAMP '2018-12-31 20:00:00.000 " + timeZone + "'"))
                    .matches("DATE '2018-12-31'");

            assertThat(assertions.function("last_day_of_month", "TIMESTAMP '2018-12-31 23:59:59.999 " + timeZone + "'"))
                    .matches("DATE '2018-12-31'");

            assertThat(assertions.function("last_day_of_month", "TIMESTAMP '2019-01-01 00:00:00.000 " + timeZone + "'"))
                    .matches("DATE '2019-1-31'");

            assertThat(assertions.function("last_day_of_month", "TIMESTAMP '2019-01-01 00:00:00.001 " + timeZone + "'"))
                    .matches("DATE '2019-1-31'");

            assertThat(assertions.function("last_day_of_month", "TIMESTAMP '2019-01-01 03:00:00.000 " + timeZone + "'"))
                    .matches("DATE '2019-1-31'");

            assertThat(assertions.function("last_day_of_month", "TIMESTAMP '2019-01-01 06:00:00.000 " + timeZone + "'"))
                    .matches("DATE '2019-1-31'");

            assertThat(assertions.function("last_day_of_month", "TIMESTAMP '2019-08-01 00:00:00.000 " + timeZone + "'"))
                    .matches("DATE '2019-8-31'");

            assertThat(assertions.function("last_day_of_month", "TIMESTAMP '2019-08-01 17:00:00.000 " + timeZone + "'"))
                    .matches("DATE '2019-8-31'");

            assertThat(assertions.function("last_day_of_month", "TIMESTAMP '2019-08-01 23:59:59.999 " + timeZone + "'"))
                    .matches("DATE '2019-8-31'");

            assertThat(assertions.function("last_day_of_month", "TIMESTAMP '2019-08-31 23:59:59.999 " + timeZone + "'"))
                    .matches("DATE '2019-8-31'");
        });
    }

    @Test
    public void testExtractFromInterval()
    {
        assertThat(assertions.expression("extract(second FROM INTERVAL '5' SECOND)"))
                .isEqualTo(5L);

        assertThat(assertions.expression("extract(second FROM INTERVAL '65' SECOND)"))
                .isEqualTo(5L);

        assertThat(assertions.expression("extract(minute FROM INTERVAL '4' MINUTE)"))
                .isEqualTo(4L);

        assertThat(assertions.expression("extract(minute FROM INTERVAL '64' MINUTE)"))
                .isEqualTo(4L);

        assertThat(assertions.expression("extract(minute FROM INTERVAL '247' SECOND)"))
                .isEqualTo(4L);

        assertThat(assertions.expression("extract(hour FROM INTERVAL '3' HOUR)"))
                .isEqualTo(3L);

        assertThat(assertions.expression("extract(hour FROM INTERVAL '27' HOUR)"))
                .isEqualTo(3L);

        assertThat(assertions.expression("extract(hour FROM INTERVAL '187' MINUTE)"))
                .isEqualTo(3L);

        assertThat(assertions.expression("extract(day FROM INTERVAL '2' DAY)"))
                .isEqualTo(2L);

        assertThat(assertions.expression("extract(day FROM INTERVAL '55' HOUR)"))
                .isEqualTo(2L);

        assertThat(assertions.expression("extract(month FROM INTERVAL '3' MONTH)"))
                .isEqualTo(3L);

        assertThat(assertions.expression("extract(month FROM INTERVAL '15' MONTH)"))
                .isEqualTo(3L);

        assertThat(assertions.expression("extract(year FROM INTERVAL '2' YEAR)"))
                .isEqualTo(2L);

        assertThat(assertions.expression("extract(year FROM INTERVAL '29' MONTH)"))
                .isEqualTo(2L);
    }

    @Test
    public void testTruncateDate()
    {
        assertThat(assertions.expression("date_trunc('day', DATE '2001-08-22')"))
                .matches("DATE '2001-08-22'");

        assertThat(assertions.expression("date_trunc('week', DATE '2001-08-22')"))
                .matches("DATE '2001-08-20'");

        assertThat(assertions.expression("date_trunc('month', DATE '2001-08-22')"))
                .matches("DATE '2001-08-01'");

        assertThat(assertions.expression("date_trunc('quarter', DATE '2001-08-22')"))
                .matches("DATE '2001-07-01'");

        assertThat(assertions.expression("date_trunc('year', DATE '2001-08-22')"))
                .matches("DATE '2001-01-01'");
    }

    @Test
    public void testAddFieldToDate()
    {
        assertThat(assertions.function("date_add", "'day'", "0", "DATE '2001-08-22'"))
                .matches("DATE '2001-08-22'");

        assertThat(assertions.function("date_add", "'day'", "3", "DATE '2001-08-22'"))
                .matches("DATE '2001-08-25'");

        assertThat(assertions.function("date_add", "'week'", "3", "DATE '2001-08-22'"))
                .matches("DATE '2001-09-12'");

        assertThat(assertions.function("date_add", "'month'", "3", "DATE '2001-08-22'"))
                .matches("DATE '2001-11-22'");

        assertThat(assertions.function("date_add", "'quarter'", "3", "DATE '2001-08-22'"))
                .matches("DATE '2002-05-22'");

        assertThat(assertions.function("date_add", "'year'", "3", "DATE '2001-08-22'"))
                .matches("DATE '2004-08-22'");
    }

    @Test
    public void testDateDiffDate()
    {
        assertThat(assertions.function("date_diff", "'day'", "DATE '1960-05-03'", "DATE '2001-08-22'"))
                .isEqualTo(15086L);

        assertThat(assertions.function("date_diff", "'week'", "DATE '1960-05-03'", "DATE '2001-08-22'"))
                .isEqualTo(2155L);

        assertThat(assertions.function("date_diff", "'month'", "DATE '1960-05-03'", "DATE '2001-08-22'"))
                .isEqualTo(495L);

        assertThat(assertions.function("date_diff", "'quarter'", "DATE '1960-05-03'", "DATE '2001-08-22'"))
                .isEqualTo(165L);

        assertThat(assertions.function("date_diff", "'year'", "DATE '1960-05-03'", "DATE '2001-08-22'"))
                .isEqualTo(41L);
    }

    @Test
    public void testParseDatetime()
    {
        // Modern date
        assertThat(assertions.function("parse_datetime", "'2020-08-18 03:04:05.678'", "'yyyy-MM-dd HH:mm:ss.SSS'"))
                .matches("TIMESTAMP '2020-08-18 03:04:05.678 Pacific/Apia'");

        // Before epoch
        assertThat(assertions.function("parse_datetime", "'1960/01/22 03:04'", "'yyyy/MM/dd HH:mm'"))
                .matches("TIMESTAMP '1960-01-22 03:04:00.000 Pacific/Apia'");

        // With named zone
        assertThat(assertions.function("parse_datetime", "'1960/01/22 03:04 Asia/Oral'", "'yyyy/MM/dd HH:mm ZZZZZ'"))
                .matches("TIMESTAMP '1960-01-22 03:04:00.000 Asia/Oral'");

        // With zone offset
        assertThat(assertions.function("parse_datetime", "'1960/01/22 03:04 +0500'", "'yyyy/MM/dd HH:mm Z'"))
                .matches("TIMESTAMP '1960-01-22 03:04:00.000 +05:00'");
    }

    @Test
    public void testDateFormat()
    {
        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321'", "'%a'"))
                .hasType(VARCHAR)
                .isEqualTo("Tue");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321'", "'%b'"))
                .hasType(VARCHAR)
                .isEqualTo("Jan");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321'", "'%c'"))
                .hasType(VARCHAR)
                .isEqualTo("1");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321'", "'%d'"))
                .hasType(VARCHAR)
                .isEqualTo("09");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321'", "'%e'"))
                .hasType(VARCHAR)
                .isEqualTo("9");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321'", "'%f'"))
                .hasType(VARCHAR)
                .isEqualTo("321000");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321'", "'%H'"))
                .hasType(VARCHAR)
                .isEqualTo("13");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321'", "'%h'"))
                .hasType(VARCHAR)
                .isEqualTo("01");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321'", "'%I'"))
                .hasType(VARCHAR)
                .isEqualTo("01");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321'", "'%i'"))
                .hasType(VARCHAR)
                .isEqualTo("04");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321'", "'%j'"))
                .hasType(VARCHAR)
                .isEqualTo("009");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321'", "'%k'"))
                .hasType(VARCHAR)
                .isEqualTo("13");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321'", "'%l'"))
                .hasType(VARCHAR)
                .isEqualTo("1");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321'", "'%M'"))
                .hasType(VARCHAR)
                .isEqualTo("January");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321'", "'%m'"))
                .hasType(VARCHAR)
                .isEqualTo("01");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321'", "'%p'"))
                .hasType(VARCHAR)
                .isEqualTo("PM");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321'", "'%r'"))
                .hasType(VARCHAR)
                .isEqualTo("01:04:05 PM");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321'", "'%S'"))
                .hasType(VARCHAR)
                .isEqualTo("05");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321'", "'%s'"))
                .hasType(VARCHAR)
                .isEqualTo("05");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321'", "'%T'"))
                .hasType(VARCHAR)
                .isEqualTo("13:04:05");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321'", "'%v'"))
                .hasType(VARCHAR)
                .isEqualTo("02");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321'", "'%W'"))
                .hasType(VARCHAR)
                .isEqualTo("Tuesday");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321'", "'%Y'"))
                .hasType(VARCHAR)
                .isEqualTo("2001");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321'", "'%y'"))
                .hasType(VARCHAR)
                .isEqualTo("01");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321'", "'%%'"))
                .hasType(VARCHAR)
                .isEqualTo("%");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321'", "'foo'"))
                .hasType(VARCHAR)
                .isEqualTo("foo");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321'", "'%g'"))
                .hasType(VARCHAR)
                .isEqualTo("g");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321'", "'%4'"))
                .hasType(VARCHAR)
                .isEqualTo("4");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321'", "'%x %v'"))
                .hasType(VARCHAR)
                .isEqualTo("2001 02");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321'", "'%Y年%m月%d日'"))
                .hasType(VARCHAR)
                .isEqualTo("2001年01月09日");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321 +07:09'", "'%a'"))
                .hasType(VARCHAR)
                .isEqualTo("Tue");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321 +07:09'", "'%b'"))
                .hasType(VARCHAR)
                .isEqualTo("Jan");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321 +07:09'", "'%c'"))
                .hasType(VARCHAR)
                .isEqualTo("1");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321 +07:09'", "'%d'"))
                .hasType(VARCHAR)
                .isEqualTo("09");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321 +07:09'", "'%e'"))
                .hasType(VARCHAR)
                .isEqualTo("9");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321 +07:09'", "'%f'"))
                .hasType(VARCHAR)
                .isEqualTo("321000");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321 +07:09'", "'%H'"))
                .hasType(VARCHAR)
                .isEqualTo("13");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321 +07:09'", "'%h'"))
                .hasType(VARCHAR)
                .isEqualTo("01");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321 +07:09'", "'%I'"))
                .hasType(VARCHAR)
                .isEqualTo("01");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321 +07:09'", "'%i'"))
                .hasType(VARCHAR)
                .isEqualTo("04");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321 +07:09'", "'%j'"))
                .hasType(VARCHAR)
                .isEqualTo("009");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321 +07:09'", "'%k'"))
                .hasType(VARCHAR)
                .isEqualTo("13");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321 +07:09'", "'%l'"))
                .hasType(VARCHAR)
                .isEqualTo("1");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321 +07:09'", "'%M'"))
                .hasType(VARCHAR)
                .isEqualTo("January");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321 +07:09'", "'%m'"))
                .hasType(VARCHAR)
                .isEqualTo("01");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321 +07:09'", "'%p'"))
                .hasType(VARCHAR)
                .isEqualTo("PM");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321 +07:09'", "'%r'"))
                .hasType(VARCHAR)
                .isEqualTo("01:04:05 PM");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321 +07:09'", "'%S'"))
                .hasType(VARCHAR)
                .isEqualTo("05");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321 +07:09'", "'%s'"))
                .hasType(VARCHAR)
                .isEqualTo("05");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321 +07:09'", "'%T'"))
                .hasType(VARCHAR)
                .isEqualTo("13:04:05");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321 +07:09'", "'%v'"))
                .hasType(VARCHAR)
                .isEqualTo("02");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321 +07:09'", "'%W'"))
                .hasType(VARCHAR)
                .isEqualTo("Tuesday");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321 +07:09'", "'%Y'"))
                .hasType(VARCHAR)
                .isEqualTo("2001");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321 +07:09'", "'%y'"))
                .hasType(VARCHAR)
                .isEqualTo("01");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321 +07:09'", "'%%'"))
                .hasType(VARCHAR)
                .isEqualTo("%");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321 +07:09'", "'foo'"))
                .hasType(VARCHAR)
                .isEqualTo("foo");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321 +07:09'", "'%g'"))
                .hasType(VARCHAR)
                .isEqualTo("g");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321 +07:09'", "'%4'"))
                .hasType(VARCHAR)
                .isEqualTo("4");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321 +07:09'", "'%x %v'"))
                .hasType(VARCHAR)
                .isEqualTo("2001 02");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.321 +07:09'", "'%Y年%m月%d日'"))
                .hasType(VARCHAR)
                .isEqualTo("2001年01月09日");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 13:04:05.32'", "'%f'"))
                .hasType(VARCHAR)
                .isEqualTo("320000");

        assertThat(assertions.function("date_format", "TIMESTAMP '2001-01-09 00:04:05.32'", "'%k'"))
                .hasType(VARCHAR)
                .isEqualTo("0");

        assertTrinoExceptionThrownBy(() -> assertions.function("date_format", "DATE '2001-01-09'", "'%D'").evaluate())
                .hasMessage("%D not supported in date format string");

        assertTrinoExceptionThrownBy(() -> assertions.function("date_format", "DATE '2001-01-09'", "'%U'").evaluate())
                .hasMessage("%U not supported in date format string");

        assertTrinoExceptionThrownBy(() -> assertions.function("date_format", "DATE '2001-01-09'", "'%u'").evaluate())
                .hasMessage("%u not supported in date format string");

        assertTrinoExceptionThrownBy(() -> assertions.function("date_format", "DATE '2001-01-09'", "'%V'").evaluate())
                .hasMessage("%V not supported in date format string");

        assertTrinoExceptionThrownBy(() -> assertions.function("date_format", "DATE '2001-01-09'", "'%w'").evaluate())
                .hasMessage("%w not supported in date format string");

        assertTrinoExceptionThrownBy(() -> assertions.function("date_format", "DATE '2001-01-09'", "'%X'").evaluate())
                .hasMessage("%X not supported in date format string");
    }

    @Test
    public void testDateParse()
    {
        assertThat(assertions.function("date_parse", "'2013'", "'%Y'"))
                .matches("TIMESTAMP '2013-01-01 00:00:00.000'");

        assertThat(assertions.function("date_parse", "'2013-05'", "'%Y-%m'"))
                .matches("TIMESTAMP '2013-05-01 00:00:00.000'");

        assertThat(assertions.function("date_parse", "'2013-05-17'", "'%Y-%m-%d'"))
                .matches("TIMESTAMP '2013-05-17 00:00:00.000'");

        assertThat(assertions.function("date_parse", "'2013-05-17 12:35:10'", "'%Y-%m-%d %h:%i:%s'"))
                .matches("TIMESTAMP '2013-05-17 00:35:10.000'");

        assertThat(assertions.function("date_parse", "'2013-05-17 12:35:10 PM'", "'%Y-%m-%d %h:%i:%s %p'"))
                .matches("TIMESTAMP '2013-05-17 12:35:10.000'");

        assertThat(assertions.function("date_parse", "'2013-05-17 12:35:10 AM'", "'%Y-%m-%d %h:%i:%s %p'"))
                .matches("TIMESTAMP '2013-05-17 00:35:10.000'");

        assertThat(assertions.function("date_parse", "'2013-05-17 00:35:10'", "'%Y-%m-%d %H:%i:%s'"))
                .matches("TIMESTAMP '2013-05-17 00:35:10.000'");

        assertThat(assertions.function("date_parse", "'2013-05-17 23:35:10'", "'%Y-%m-%d %H:%i:%s'"))
                .matches("TIMESTAMP '2013-05-17 23:35:10.000'");

        assertThat(assertions.function("date_parse", "'abc 2013-05-17 fff 23:35:10 xyz'", "'abc %Y-%m-%d fff %H:%i:%s xyz'"))
                .matches("TIMESTAMP '2013-05-17 23:35:10.000'");

        assertThat(assertions.function("date_parse", "'2013 14'", "'%Y %y'"))
                .matches("TIMESTAMP '2014-01-01 00:00:00.000'");

        assertThat(assertions.function("date_parse", "'1998 53'", "'%x %v'"))
                .matches("TIMESTAMP '1998-12-28 00:00:00.000'");

        assertThat(assertions.function("date_parse", "'1.1'", "'%s.%f'"))
                .matches("TIMESTAMP '1970-01-01 00:00:01.100'");

        assertThat(assertions.function("date_parse", "'1.01'", "'%s.%f'"))
                .matches("TIMESTAMP '1970-01-01 00:00:01.010'");

        assertThat(assertions.function("date_parse", "'1.2006'", "'%s.%f'"))
                .matches("TIMESTAMP '1970-01-01 00:00:01.200'");

        assertThat(assertions.function("date_parse", "'59.123456789'", "'%s.%f'"))
                .matches("TIMESTAMP '1970-01-01 00:00:59.123'");

        assertThat(assertions.function("date_parse", "'0'", "'%k'"))
                .matches("TIMESTAMP '1970-01-01 00:00:00.000'");

        assertThat(assertions.function("date_parse", "'28-JAN-16 11.45.46.421000 PM'", "'%d-%b-%y %l.%i.%s.%f %p'"))
                .matches("TIMESTAMP '2016-01-28 23:45:46.421'");

        assertThat(assertions.function("date_parse", "'11-DEC-70 11.12.13.456000 AM'", "'%d-%b-%y %l.%i.%s.%f %p'"))
                .matches("TIMESTAMP '1970-12-11 11:12:13.456'");

        assertThat(assertions.function("date_parse", "'31-MAY-69 04.59.59.999000 AM'", "'%d-%b-%y %l.%i.%s.%f %p'"))
                .matches("TIMESTAMP '2069-05-31 04:59:59.999'");

        assertTrinoExceptionThrownBy(() -> assertions.function("date_parse", "''", "'%D'").evaluate())
                .hasMessage("%D not supported in date format string");

        assertTrinoExceptionThrownBy(() -> assertions.function("date_parse", "''", "'%U'").evaluate())
                .hasMessage("%U not supported in date format string");

        assertTrinoExceptionThrownBy(() -> assertions.function("date_parse", "''", "'%u'").evaluate())
                .hasMessage("%u not supported in date format string");

        assertTrinoExceptionThrownBy(() -> assertions.function("date_parse", "''", "'%V'").evaluate())
                .hasMessage("%V not supported in date format string");

        assertTrinoExceptionThrownBy(() -> assertions.function("date_parse", "''", "'%w'").evaluate())
                .hasMessage("%w not supported in date format string");

        assertTrinoExceptionThrownBy(() -> assertions.function("date_parse", "''", "'%X'").evaluate())
                .hasMessage("%X not supported in date format string");

        assertTrinoExceptionThrownBy(() -> assertions.function("date_parse", "'3.0123456789'", "'%s.%f'").evaluate())
                .hasMessage("Invalid format: \"3.0123456789\" is malformed at \"9\"");

        assertTrinoExceptionThrownBy(() -> assertions.function("date_parse", "'1970-01-01'", "''").evaluate())
                .hasMessage("Both printing and parsing not supported");
    }

    @Test
    public void testLocale()
    {
        Session session = Session.builder(assertions.getDefaultSession())
                .setLocale(Locale.KOREAN)
                .build();

        assertThat(assertions.expression("date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%a')", session))
                .hasType(VARCHAR)
                .isEqualTo("화");

        assertThat(assertions.expression("date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%W')", session))
                .hasType(VARCHAR)
                .isEqualTo("화요일");

        assertThat(assertions.expression("date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%p')", session))
                .hasType(VARCHAR)
                .isEqualTo("오후");

        assertThat(assertions.expression("date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%r')", session))
                .hasType(VARCHAR)
                .isEqualTo("01:04:05 오후");

        assertThat(assertions.expression("date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%b')", session))
                .hasType(VARCHAR)
                .isEqualTo("1월");

        assertThat(assertions.expression("date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%M')", session))
                .hasType(VARCHAR)
                .isEqualTo("1월");

        assertThat(assertions.expression("format_datetime(TIMESTAMP '2001-01-09 13:04:05.321', 'EEE')", session))
                .hasType(VARCHAR)
                .isEqualTo("화");

        assertThat(assertions.expression("format_datetime(TIMESTAMP '2001-01-09 13:04:05.321', 'EEEE')", session))
                .hasType(VARCHAR)
                .isEqualTo("화요일");

        assertThat(assertions.expression("format_datetime(TIMESTAMP '2001-01-09 13:04:05.321', 'a')", session))
                .hasType(VARCHAR)
                .isEqualTo("오후");

        assertThat(assertions.expression("format_datetime(TIMESTAMP '2001-01-09 13:04:05.321', 'MMM')", session))
                .hasType(VARCHAR)
                .isEqualTo("1월");

        assertThat(assertions.expression("format_datetime(TIMESTAMP '2001-01-09 13:04:05.321', 'MMMM')", session))
                .hasType(VARCHAR)
                .isEqualTo("1월");

        assertThat(assertions.expression("date_parse('2013-05-17 12:35:10 오후', '%Y-%m-%d %h:%i:%s %p')", session))
                .matches("TIMESTAMP '2013-05-17 12:35:10.000'");

        assertThat(assertions.expression("date_parse('2013-05-17 12:35:10 오전', '%Y-%m-%d %h:%i:%s %p')", session))
                .matches("TIMESTAMP '2013-05-17 00:35:10.000'");

        assertThat(assertions.expression("parse_datetime('2013-05-17 12:35:10 오후', 'yyyy-MM-dd hh:mm:ss a')", session))
                .matches("TIMESTAMP '2013-05-17 12:35:10.000 Pacific/Apia'");

        assertThat(assertions.expression("parse_datetime('2013-05-17 12:35:10 오전', 'yyyy-MM-dd hh:mm:ss aaa')", session))
                .matches("TIMESTAMP '2013-05-17 00:35:10.000 Pacific/Apia'");
    }

    @Test
    public void testParseDuration()
    {
        assertThat(assertions.function("parse_duration", "'1234 ns'"))
                .hasType(INTERVAL_DAY_TIME)
                .isEqualTo(new SqlIntervalDayTime(0, 0, 0, 0, 0));

        assertThat(assertions.function("parse_duration", "'1234 us'"))
                .hasType(INTERVAL_DAY_TIME)
                .isEqualTo(new SqlIntervalDayTime(0, 0, 0, 0, 1));

        assertThat(assertions.function("parse_duration", "'1234 ms'"))
                .hasType(INTERVAL_DAY_TIME)
                .isEqualTo(new SqlIntervalDayTime(0, 0, 0, 1, 234));

        assertThat(assertions.function("parse_duration", "'1234 s'"))
                .hasType(INTERVAL_DAY_TIME)
                .isEqualTo(new SqlIntervalDayTime(0, 0, 20, 34, 0));

        assertThat(assertions.function("parse_duration", "'1234 m'"))
                .hasType(INTERVAL_DAY_TIME)
                .isEqualTo(new SqlIntervalDayTime(0, 20, 34, 0, 0));

        assertThat(assertions.function("parse_duration", "'1234 h'"))
                .hasType(INTERVAL_DAY_TIME)
                .isEqualTo(new SqlIntervalDayTime(51, 10, 0, 0, 0));

        assertThat(assertions.function("parse_duration", "'1234 d'"))
                .hasType(INTERVAL_DAY_TIME)
                .isEqualTo(new SqlIntervalDayTime(1234, 0, 0, 0, 0));

        assertThat(assertions.function("parse_duration", "'1234.567 ns'"))
                .hasType(INTERVAL_DAY_TIME)
                .isEqualTo(new SqlIntervalDayTime(0, 0, 0, 0, 0));

        assertThat(assertions.function("parse_duration", "'1234.567 ms'"))
                .hasType(INTERVAL_DAY_TIME)
                .isEqualTo(new SqlIntervalDayTime(0, 0, 0, 1, 235));

        assertThat(assertions.function("parse_duration", "'1234.567 s'"))
                .hasType(INTERVAL_DAY_TIME)
                .isEqualTo(new SqlIntervalDayTime(0, 0, 0, 1234, 567));

        assertThat(assertions.function("parse_duration", "'1234.567 m'"))
                .hasType(INTERVAL_DAY_TIME)
                .isEqualTo(new SqlIntervalDayTime(0, 20, 34, 34, 20));

        assertThat(assertions.function("parse_duration", "'1234.567 h'"))
                .hasType(INTERVAL_DAY_TIME)
                .isEqualTo(new SqlIntervalDayTime(51, 10, 34, 1, 200));

        assertThat(assertions.function("parse_duration", "'1234.567 d'"))
                .hasType(INTERVAL_DAY_TIME)
                .isEqualTo(new SqlIntervalDayTime(1234, 13, 36, 28, 800));

        // without space
        assertThat(assertions.function("parse_duration", "'1234ns'"))
                .hasType(INTERVAL_DAY_TIME)
                .isEqualTo(new SqlIntervalDayTime(0, 0, 0, 0, 0));

        assertThat(assertions.function("parse_duration", "'1234us'"))
                .hasType(INTERVAL_DAY_TIME)
                .isEqualTo(new SqlIntervalDayTime(0, 0, 0, 0, 1));

        assertThat(assertions.function("parse_duration", "'1234ms'"))
                .hasType(INTERVAL_DAY_TIME)
                .isEqualTo(new SqlIntervalDayTime(0, 0, 0, 1, 234));

        assertThat(assertions.function("parse_duration", "'1234s'"))
                .hasType(INTERVAL_DAY_TIME)
                .isEqualTo(new SqlIntervalDayTime(0, 0, 20, 34, 0));

        assertThat(assertions.function("parse_duration", "'1234m'"))
                .hasType(INTERVAL_DAY_TIME)
                .isEqualTo(new SqlIntervalDayTime(0, 20, 34, 0, 0));

        assertThat(assertions.function("parse_duration", "'1234h'"))
                .hasType(INTERVAL_DAY_TIME)
                .isEqualTo(new SqlIntervalDayTime(51, 10, 0, 0, 0));

        assertThat(assertions.function("parse_duration", "'1234d'"))
                .hasType(INTERVAL_DAY_TIME)
                .isEqualTo(new SqlIntervalDayTime(1234, 0, 0, 0, 0));

        assertThat(assertions.function("parse_duration", "'1234.567ns'"))
                .hasType(INTERVAL_DAY_TIME)
                .isEqualTo(new SqlIntervalDayTime(0, 0, 0, 0, 0));

        assertThat(assertions.function("parse_duration", "'1234.567ms'"))
                .hasType(INTERVAL_DAY_TIME)
                .isEqualTo(new SqlIntervalDayTime(0, 0, 0, 1, 235));

        assertThat(assertions.function("parse_duration", "'1234.567s'"))
                .hasType(INTERVAL_DAY_TIME)
                .isEqualTo(new SqlIntervalDayTime(0, 0, 0, 1234, 567));

        assertThat(assertions.function("parse_duration", "'1234.567m'"))
                .hasType(INTERVAL_DAY_TIME)
                .isEqualTo(new SqlIntervalDayTime(0, 20, 34, 34, 20));

        assertThat(assertions.function("parse_duration", "'1234.567h'"))
                .hasType(INTERVAL_DAY_TIME)
                .isEqualTo(new SqlIntervalDayTime(51, 10, 34, 1, 200));

        assertThat(assertions.function("parse_duration", "'1234.567d'"))
                .hasType(INTERVAL_DAY_TIME)
                .isEqualTo(new SqlIntervalDayTime(1234, 13, 36, 28, 800));

        // invalid function calls
        assertTrinoExceptionThrownBy(() -> assertions.function("parse_duration", "''").evaluate())
                .hasMessage("duration is empty");

        assertTrinoExceptionThrownBy(() -> assertions.function("parse_duration", "'1f'").evaluate())
                .hasMessage("Unknown time unit: f");

        assertTrinoExceptionThrownBy(() -> assertions.function("parse_duration", "'abc'").evaluate())
                .hasMessage("duration is not a valid data duration string: abc");
    }

    @Test
    public void testIntervalDayToSecondToMilliseconds()
    {
        assertThat(assertions.function("to_milliseconds", "parse_duration('1ns')"))
                .isEqualTo(0L);

        assertThat(assertions.function("to_milliseconds", "parse_duration('1ms')"))
                .isEqualTo(1L);

        assertThat(assertions.function("to_milliseconds", "parse_duration('1s')"))
                .isEqualTo(SECONDS.toMillis(1));

        assertThat(assertions.function("to_milliseconds", "parse_duration('1h')"))
                .isEqualTo(HOURS.toMillis(1));

        assertThat(assertions.function("to_milliseconds", "parse_duration('1d')"))
                .isEqualTo(DAYS.toMillis(1));
    }

    @Test
    public void testWithTimezone()
    {
        assertThat(assertions.function("with_timezone", "TIMESTAMP '2001-08-22 03:04:05.321'", "'UTC'"))
                .matches("TIMESTAMP '2001-08-22 03:04:05.321 UTC'");

        assertThat(assertions.function("with_timezone", "TIMESTAMP '2001-08-22 03:04:05.321'", "'+13'"))
                .matches("TIMESTAMP '2001-08-22 03:04:05.321 +13:00'");

        assertThat(assertions.function("with_timezone", "TIMESTAMP '2001-08-22 03:04:05.321'", "'-14'"))
                .matches("TIMESTAMP '2001-08-22 03:04:05.321 -14:00'");

        assertThat(assertions.function("with_timezone", "TIMESTAMP '2001-08-22 03:04:05.321'", "'+00:45'"))
                .matches("TIMESTAMP '2001-08-22 03:04:05.321 +00:45'");

        assertThat(assertions.function("with_timezone", "TIMESTAMP '2001-08-22 03:04:05.321'", "'Asia/Shanghai'"))
                .matches("TIMESTAMP '2001-08-22 03:04:05.321 Asia/Shanghai'");

        assertThat(assertions.function("with_timezone", "TIMESTAMP '2001-08-22 03:04:05.321'", "'America/New_York'"))
                .matches("TIMESTAMP '2001-08-22 03:04:05.321 America/New_York'");

        assertThat(assertions.function("with_timezone", "TIMESTAMP '2001-06-01 03:04:05.321'", "'America/Los_Angeles'"))
                .matches("TIMESTAMP '2001-06-01 03:04:05.321 America/Los_Angeles'");

        assertThat(assertions.function("with_timezone", "TIMESTAMP '2001-12-01 03:04:05.321'", "'America/Los_Angeles'"))
                .matches("TIMESTAMP '2001-12-01 03:04:05.321 America/Los_Angeles'");

        assertTrinoExceptionThrownBy(() -> assertions.function("with_timezone", "TIMESTAMP '2001-08-22 03:04:05.321'", "'invalidzoneid'").evaluate())
                .hasMessage("'invalidzoneid' is not a valid time zone");
    }
}
