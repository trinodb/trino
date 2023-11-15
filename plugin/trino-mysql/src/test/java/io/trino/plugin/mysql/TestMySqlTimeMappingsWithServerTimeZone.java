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
package io.trino.plugin.mysql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.spi.type.TimeZoneKey;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingSession;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.datatype.CreateAsSelectDataSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.datatype.SqlDataTypeTest;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TrinoSqlExecutor;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.Map;

import static io.trino.plugin.mysql.MySqlQueryRunner.createMySqlQueryRunner;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Default MySQL time zone is set to UTC. This is to test the date and time type mappings when the server has a different time zone.
 */
public class TestMySqlTimeMappingsWithServerTimeZone
        extends AbstractTestQueryFramework
{
    private TestingMySqlServer mySqlServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        mySqlServer = closeAfterClass(new TestingMySqlServer(ZoneId.of("Pacific/Apia")));
        return createMySqlQueryRunner(mySqlServer, ImmutableMap.of(), ImmutableMap.of(), ImmutableList.of());
    }

    @Test
    public void testDate()
    {
        testDate(UTC);
        testDate(ZoneId.systemDefault());
        // no DST in 1970, but has DST in later years (e.g. 2018)
        testDate(ZoneId.of("Europe/Vilnius"));
        // minutes offset change since 1970-01-01, no DST
        testDate(ZoneId.of("Asia/Kathmandu"));
        testDate(TestingSession.DEFAULT_TIME_ZONE_KEY.getZoneId());
    }

    private void testDate(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        SqlDataTypeTest.create()
                .addRoundTrip("date", "DATE '0001-01-01'", DATE, "DATE '0001-01-01'")
                .addRoundTrip("date", "DATE '1582-10-04'", DATE, "DATE '1582-10-04'") // before julian->gregorian switch
                .addRoundTrip("date", "DATE '1582-10-05'", DATE, "DATE '1582-10-05'") // begin julian->gregorian switch
                .addRoundTrip("date", "DATE '1582-10-14'", DATE, "DATE '1582-10-14'") // end julian->gregorian switch
                .addRoundTrip("date", "DATE '1952-04-03'", DATE, "DATE '1952-04-03'") // before epoch
                .addRoundTrip("date", "DATE '1970-01-01'", DATE, "DATE '1970-01-01'")
                .addRoundTrip("date", "DATE '1970-02-03'", DATE, "DATE '1970-02-03'")
                .addRoundTrip("date", "DATE '2017-07-01'", DATE, "DATE '2017-07-01'") // summer on northern hemisphere (possible DST)
                .addRoundTrip("date", "DATE '2017-01-01'", DATE, "DATE '2017-01-01'") // winter on northern hemisphere (possible DST on southern hemisphere)
                .addRoundTrip("date", "DATE '1983-04-01'", DATE, "DATE '1983-04-01'")
                .addRoundTrip("date", "DATE '1983-10-01'", DATE, "DATE '1983-10-01'")
                .addRoundTrip("date", "NULL", DATE, "CAST(NULL AS DATE)")
                .execute(getQueryRunner(), session, mysqlCreateAndInsert("tpch.test_date"))
                .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_date"))
                .execute(getQueryRunner(), session, trinoCreateAsSelect("test_date"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_date"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert("test_date"));
    }

    @Test
    public void testTimeFromMySql()
    {
        testTimeFromMySql(UTC);
        testTimeFromMySql(ZoneId.systemDefault());
        // no DST in 1970, but has DST in later years (e.g. 2018)
        testTimeFromMySql(ZoneId.of("Europe/Vilnius"));
        // minutes offset change since 1970-01-01, no DST
        testTimeFromMySql(ZoneId.of("Asia/Kathmandu"));
        testTimeFromMySql(TestingSession.DEFAULT_TIME_ZONE_KEY.getZoneId());
    }

    private void testTimeFromMySql(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        SqlDataTypeTest.create()
                // default precision in MySQL is 0
                .addRoundTrip("TIME", "TIME '00:00:00'", createTimeType(0), "TIME '00:00:00'")
                .addRoundTrip("TIME", "TIME '12:34:56'", createTimeType(0), "TIME '12:34:56'")
                .addRoundTrip("TIME", "TIME '23:59:59'", createTimeType(0), "TIME '23:59:59'")

                // maximal value for a precision
                .addRoundTrip("TIME(1)", "TIME '23:59:59.9'", createTimeType(1), "TIME '23:59:59.9'")
                .addRoundTrip("TIME(2)", "TIME '23:59:59.99'", createTimeType(2), "TIME '23:59:59.99'")
                .addRoundTrip("TIME(3)", "TIME '23:59:59.999'", createTimeType(3), "TIME '23:59:59.999'")
                .addRoundTrip("TIME(4)", "TIME '23:59:59.9999'", createTimeType(4), "TIME '23:59:59.9999'")
                .addRoundTrip("TIME(5)", "TIME '23:59:59.99999'", createTimeType(5), "TIME '23:59:59.99999'")
                .addRoundTrip("TIME(6)", "TIME '23:59:59.999999'", createTimeType(6), "TIME '23:59:59.999999'")

                .addRoundTrip("TIME", "NULL", createTimeType(0), "CAST(NULL AS TIME(0))")
                .execute(getQueryRunner(), session, mysqlCreateAndInsert("tpch.test_time"));
    }

    @Test
    public void testTimeFromTrino()
    {
        testTimeFromTrino(UTC);
        testTimeFromTrino(ZoneId.systemDefault());
        // no DST in 1970, but has DST in later years (e.g. 2018)
        testTimeFromTrino(ZoneId.of("Europe/Vilnius"));
        // minutes offset change since 1970-01-01, no DST
        testTimeFromTrino(ZoneId.of("Asia/Kathmandu"));
        testTimeFromTrino(TestingSession.DEFAULT_TIME_ZONE_KEY.getZoneId());
    }

    private void testTimeFromTrino(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        SqlDataTypeTest.create()
                // default precision in Trino is 3
                .addRoundTrip("TIME", "TIME '00:00:00'", createTimeType(3), "TIME '00:00:00.000'")
                .addRoundTrip("TIME", "TIME '12:34:56.123'", createTimeType(3), "TIME '12:34:56.123'")
                .addRoundTrip("TIME", "TIME '23:59:59.999'", createTimeType(3), "TIME '23:59:59.999'")

                // maximal value for a precision
                .addRoundTrip("TIME", "TIME '23:59:59'", createTimeType(3), "TIME '23:59:59.000'")
                .addRoundTrip("TIME(1)", "TIME '23:59:59.9'", createTimeType(1), "TIME '23:59:59.9'")
                .addRoundTrip("TIME(2)", "TIME '23:59:59.99'", createTimeType(2), "TIME '23:59:59.99'")
                .addRoundTrip("TIME(3)", "TIME '23:59:59.999'", createTimeType(3), "TIME '23:59:59.999'")
                .addRoundTrip("TIME(4)", "TIME '23:59:59.9999'", createTimeType(4), "TIME '23:59:59.9999'")
                .addRoundTrip("TIME(5)", "TIME '23:59:59.99999'", createTimeType(5), "TIME '23:59:59.99999'")
                .addRoundTrip("TIME(6)", "TIME '23:59:59.999999'", createTimeType(6), "TIME '23:59:59.999999'")

                // supported precisions
                .addRoundTrip("TIME '23:59:59.9'", "TIME '23:59:59.9'")
                .addRoundTrip("TIME '23:59:59.99'", "TIME '23:59:59.99'")
                .addRoundTrip("TIME '23:59:59.999'", "TIME '23:59:59.999'")
                .addRoundTrip("TIME '23:59:59.9999'", "TIME '23:59:59.9999'")
                .addRoundTrip("TIME '23:59:59.99999'", "TIME '23:59:59.99999'")
                .addRoundTrip("TIME '23:59:59.999999'", "TIME '23:59:59.999999'")

                // round down
                .addRoundTrip("TIME '00:00:00.0000001'", "TIME '00:00:00.000000'")
                .addRoundTrip("TIME '00:00:00.000000000001'", "TIME '00:00:00.000000'")
                .addRoundTrip("TIME '12:34:56.1234561'", "TIME '12:34:56.123456'")
                .addRoundTrip("TIME '23:59:59.9999994'", "TIME '23:59:59.999999'")
                .addRoundTrip("TIME '23:59:59.999999499999'", "TIME '23:59:59.999999'")

                // round down, maximal value
                .addRoundTrip("TIME '00:00:00.0000004'", "TIME '00:00:00.000000'")
                .addRoundTrip("TIME '00:00:00.00000049'", "TIME '00:00:00.000000'")
                .addRoundTrip("TIME '00:00:00.000000449'", "TIME '00:00:00.000000'")
                .addRoundTrip("TIME '00:00:00.0000004449'", "TIME '00:00:00.000000'")
                .addRoundTrip("TIME '00:00:00.00000044449'", "TIME '00:00:00.000000'")
                .addRoundTrip("TIME '00:00:00.000000444449'", "TIME '00:00:00.000000'")

                // round up, minimal value
                .addRoundTrip("TIME '00:00:00.0000005'", "TIME '00:00:00.000001'")
                .addRoundTrip("TIME '00:00:00.00000050'", "TIME '00:00:00.000001'")
                .addRoundTrip("TIME '00:00:00.000000500'", "TIME '00:00:00.000001'")
                .addRoundTrip("TIME '00:00:00.0000005000'", "TIME '00:00:00.000001'")
                .addRoundTrip("TIME '00:00:00.00000050000'", "TIME '00:00:00.000001'")
                .addRoundTrip("TIME '00:00:00.000000500000'", "TIME '00:00:00.000001'")

                // round up, maximal value
                .addRoundTrip("TIME '00:00:00.0000009'", "TIME '00:00:00.000001'")
                .addRoundTrip("TIME '00:00:00.00000099'", "TIME '00:00:00.000001'")
                .addRoundTrip("TIME '00:00:00.000000999'", "TIME '00:00:00.000001'")
                .addRoundTrip("TIME '00:00:00.0000009999'", "TIME '00:00:00.000001'")
                .addRoundTrip("TIME '00:00:00.00000099999'", "TIME '00:00:00.000001'")
                .addRoundTrip("TIME '00:00:00.000000999999'", "TIME '00:00:00.000001'")

                // round up to next day, minimal value
                .addRoundTrip("TIME '23:59:59.9999995'", "TIME '00:00:00.000000'")
                .addRoundTrip("TIME '23:59:59.99999950'", "TIME '00:00:00.000000'")
                .addRoundTrip("TIME '23:59:59.999999500'", "TIME '00:00:00.000000'")
                .addRoundTrip("TIME '23:59:59.9999995000'", "TIME '00:00:00.000000'")
                .addRoundTrip("TIME '23:59:59.99999950000'", "TIME '00:00:00.000000'")
                .addRoundTrip("TIME '23:59:59.999999500000'", "TIME '00:00:00.000000'")

                // round up to next day, maximal value
                .addRoundTrip("TIME '23:59:59.9999999'", "TIME '00:00:00.000000'")
                .addRoundTrip("TIME '23:59:59.99999999'", "TIME '00:00:00.000000'")
                .addRoundTrip("TIME '23:59:59.999999999'", "TIME '00:00:00.000000'")
                .addRoundTrip("TIME '23:59:59.9999999999'", "TIME '00:00:00.000000'")
                .addRoundTrip("TIME '23:59:59.99999999999'", "TIME '00:00:00.000000'")
                .addRoundTrip("TIME '23:59:59.999999999999'", "TIME '00:00:00.000000'")

                // null
                .addRoundTrip("TIME", "NULL", createTimeType(3), "CAST(NULL AS TIME(3))")
                .addRoundTrip("TIME(1)", "NULL", createTimeType(1), "CAST(NULL AS TIME(1))")
                .addRoundTrip("TIME(2)", "NULL", createTimeType(2), "CAST(NULL AS TIME(2))")
                .addRoundTrip("TIME(3)", "NULL", createTimeType(3), "CAST(NULL AS TIME(3))")
                .addRoundTrip("TIME(4)", "NULL", createTimeType(4), "CAST(NULL AS TIME(4))")
                .addRoundTrip("TIME(5)", "NULL", createTimeType(5), "CAST(NULL AS TIME(5))")
                .addRoundTrip("TIME(6)", "NULL", createTimeType(6), "CAST(NULL AS TIME(6))")

                .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_time"))
                .execute(getQueryRunner(), session, trinoCreateAsSelect("test_time"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_time"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert("test_time"));
    }

    /**
     * Read {@code DATETIME}s inserted by MySQL as Trino {@code TIMESTAMP}s
     */
    @Test
    public void testMySqlDatetimeType()
    {
        testMySqlDatetimeType(UTC);
        testMySqlDatetimeType(ZoneId.systemDefault());
        // no DST in 1970, but has DST in later years (e.g. 2018)
        testMySqlDatetimeType(ZoneId.of("Europe/Vilnius"));
        // minutes offset change since 1970-01-01, no DST
        testMySqlDatetimeType(ZoneId.of("Asia/Kathmandu"));
        testMySqlDatetimeType(TestingSession.DEFAULT_TIME_ZONE_KEY.getZoneId());
    }

    private void testMySqlDatetimeType(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        SqlDataTypeTest.create()
                // before epoch
                .addRoundTrip("datetime(3)", "TIMESTAMP '1958-01-01 13:18:03.123'", createTimestampType(3), "TIMESTAMP '1958-01-01 13:18:03.123'")
                // after epoch
                .addRoundTrip("datetime(3)", "TIMESTAMP '2019-03-18 10:01:17.987'", createTimestampType(3), "TIMESTAMP '2019-03-18 10:01:17.987'")
                // time doubled in JVM zone
                .addRoundTrip("datetime(3)", "TIMESTAMP '2018-10-28 01:33:17.456'", createTimestampType(3), "TIMESTAMP '2018-10-28 01:33:17.456'")
                // time double in Vilnius
                .addRoundTrip("datetime(3)", "TIMESTAMP '2018-10-28 03:33:33.333'", createTimestampType(3), "TIMESTAMP '2018-10-28 03:33:33.333'")
                // epoch
                .addRoundTrip("datetime(3)", "TIMESTAMP '1970-01-01 00:00:00.000'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.000'")
                .addRoundTrip("datetime(3)", "TIMESTAMP '1970-01-01 00:13:42.000'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:13:42.000'")
                .addRoundTrip("datetime(3)", "TIMESTAMP '2018-04-01 02:13:55.123'", createTimestampType(3), "TIMESTAMP '2018-04-01 02:13:55.123'")
                // time gap in Vilnius
                .addRoundTrip("datetime(3)", "TIMESTAMP '2018-03-25 03:17:17.000'", createTimestampType(3), "TIMESTAMP '2018-03-25 03:17:17.000'")
                // time gap in Kathmandu
                .addRoundTrip("datetime(3)", "TIMESTAMP '1986-01-01 00:13:07.000'", createTimestampType(3), "TIMESTAMP '1986-01-01 00:13:07.000'")

                // same as above but with higher precision
                .addRoundTrip("datetime(6)", "TIMESTAMP '1958-01-01 13:18:03.123456'", createTimestampType(6), "TIMESTAMP '1958-01-01 13:18:03.123456'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '2019-03-18 10:01:17.987654'", createTimestampType(6), "TIMESTAMP '2019-03-18 10:01:17.987654'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '2018-10-28 01:33:17.123456'", createTimestampType(6), "TIMESTAMP '2018-10-28 01:33:17.123456'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '2018-10-28 03:33:33.333333'", createTimestampType(6), "TIMESTAMP '2018-10-28 03:33:33.333333'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '1970-01-01 00:00:00.000000'", createTimestampType(6), "TIMESTAMP '1970-01-01 00:00:00.000000'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '1970-01-01 00:13:42.123456'", createTimestampType(6), "TIMESTAMP '1970-01-01 00:13:42.123456'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '2018-04-01 02:13:55.123456'", createTimestampType(6), "TIMESTAMP '2018-04-01 02:13:55.123456'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '2018-03-25 03:17:17.456789'", createTimestampType(6), "TIMESTAMP '2018-03-25 03:17:17.456789'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '1986-01-01 00:13:07.456789'", createTimestampType(6), "TIMESTAMP '1986-01-01 00:13:07.456789'")

                // test arbitrary time for all supported precisions
                .addRoundTrip("datetime(0)", "TIMESTAMP '1970-01-01 00:00:01'", createTimestampType(0), "TIMESTAMP '1970-01-01 00:00:01'")
                .addRoundTrip("datetime(1)", "TIMESTAMP '1970-01-01 00:00:01.1'", createTimestampType(1), "TIMESTAMP '1970-01-01 00:00:01.1'")
                .addRoundTrip("datetime(2)", "TIMESTAMP '1970-01-01 00:00:01.12'", createTimestampType(2), "TIMESTAMP '1970-01-01 00:00:01.12'")
                .addRoundTrip("datetime(3)", "TIMESTAMP '1970-01-01 00:00:01.123'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:01.123'")
                .addRoundTrip("datetime(4)", "TIMESTAMP '1970-01-01 00:00:01.1234'", createTimestampType(4), "TIMESTAMP '1970-01-01 00:00:01.1234'")
                .addRoundTrip("datetime(5)", "TIMESTAMP '1970-01-01 00:00:01.12345'", createTimestampType(5), "TIMESTAMP '1970-01-01 00:00:01.12345'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '1970-01-01 00:00:01.123456'", createTimestampType(6), "TIMESTAMP '1970-01-01 00:00:01.123456'")

                // negative epoch
                .addRoundTrip("datetime(6)", "TIMESTAMP '1969-12-31 23:59:59.999995'", createTimestampType(6), "TIMESTAMP '1969-12-31 23:59:59.999995'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '1969-12-31 23:59:59.999949'", createTimestampType(6), "TIMESTAMP '1969-12-31 23:59:59.999949'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '1969-12-31 23:59:59.999994'", createTimestampType(6), "TIMESTAMP '1969-12-31 23:59:59.999994'")

                // null
                .addRoundTrip("datetime(0)", "NULL", createTimestampType(0), "CAST(NULL AS TIMESTAMP(0))")
                .addRoundTrip("datetime(1)", "NULL", createTimestampType(1), "CAST(NULL AS TIMESTAMP(1))")
                .addRoundTrip("datetime(2)", "NULL", createTimestampType(2), "CAST(NULL AS TIMESTAMP(2))")
                .addRoundTrip("datetime(3)", "NULL", createTimestampType(3), "CAST(NULL AS TIMESTAMP(3))")
                .addRoundTrip("datetime(4)", "NULL", createTimestampType(4), "CAST(NULL AS TIMESTAMP(4))")
                .addRoundTrip("datetime(5)", "NULL", createTimestampType(5), "CAST(NULL AS TIMESTAMP(5))")
                .addRoundTrip("datetime(6)", "NULL", createTimestampType(6), "CAST(NULL AS TIMESTAMP(6))")

                .execute(getQueryRunner(), session, mysqlCreateAndInsert("tpch.test_datetime"));
    }

    /**
     * Read {@code TIMESTAMP}s inserted by MySQL as Trino {@code TIMESTAMP WITH TIME ZONE}s
     */
    @Test
    public void testTimestampFromMySql()
    {
        testTimestampFromMySql(UTC);
        testTimestampFromMySql(ZoneId.systemDefault());
        // no DST in 1970, but has DST in later years (e.g. 2018)
        testTimestampFromMySql(ZoneId.of("Europe/Vilnius"));
        // minutes offset change since 1970-01-01, no DST
        testTimestampFromMySql(ZoneId.of("Asia/Kathmandu"));
        testTimestampFromMySql(TestingSession.DEFAULT_TIME_ZONE_KEY.getZoneId());
    }

    private void testTimestampFromMySql(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        // Same as above but with inserts from MySQL - i.e. read path
        // Note the inserted timestamp values are using the server time zone, Pacific/Apia, and the expected timestamps are shifted to UTC time
        SqlDataTypeTest.create()
                // after epoch (MySQL's timestamp type doesn't support values <= epoch)
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2019-03-18 10:01:17.987'", createTimestampWithTimeZoneType(3), "TIMESTAMP '2019-03-17 20:01:17.987 UTC'")
                // time doubled in JVM zone
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2018-10-28 01:33:17.456'", createTimestampWithTimeZoneType(3), "TIMESTAMP '2018-10-27 11:33:17.456 UTC'")
                // time double in Vilnius
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2018-10-28 03:33:33.333'", createTimestampWithTimeZoneType(3), "TIMESTAMP '2018-10-27 13:33:33.333 UTC'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1970-01-01 00:13:42.000'", createTimestampWithTimeZoneType(3), "TIMESTAMP '1970-01-01 11:13:42.000 UTC'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2018-04-01 02:13:55.123'", createTimestampWithTimeZoneType(3), "TIMESTAMP '2018-03-31 12:13:55.123 UTC'")
                // time gap in Vilnius
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2018-03-25 03:17:17.000'", createTimestampWithTimeZoneType(3), "TIMESTAMP '2018-03-24 13:17:17.000 UTC'")
                // time gap in Kathmandu
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1986-01-01 00:13:07.000'", createTimestampWithTimeZoneType(3), "TIMESTAMP '1986-01-01 11:13:07.000 UTC'")

                // same as above but with higher precision
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2019-03-18 10:01:17.987654'", createTimestampWithTimeZoneType(6), "TIMESTAMP '2019-03-17 20:01:17.987654 UTC'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2018-10-28 01:33:17.456789'", createTimestampWithTimeZoneType(6), "TIMESTAMP '2018-10-27 11:33:17.456789 UTC'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2018-10-28 03:33:33.333333'", createTimestampWithTimeZoneType(6), "TIMESTAMP '2018-10-27 13:33:33.333333 UTC'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1970-01-01 00:13:42.000000'", createTimestampWithTimeZoneType(6), "TIMESTAMP '1970-01-01 11:13:42.000000 UTC'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2018-04-01 02:13:55.123456'", createTimestampWithTimeZoneType(6), "TIMESTAMP '2018-03-31 12:13:55.123456 UTC'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2018-03-25 03:17:17.000000'", createTimestampWithTimeZoneType(6), "TIMESTAMP '2018-03-24 13:17:17.000000 UTC'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1986-01-01 00:13:07.000000'", createTimestampWithTimeZoneType(6), "TIMESTAMP '1986-01-01 11:13:07.000000 UTC'")

                // test arbitrary time for all supported precisions
                .addRoundTrip("timestamp(0)", "TIMESTAMP '1970-01-01 00:00:01'", createTimestampWithTimeZoneType(0), "TIMESTAMP '1970-01-01 11:00:01 UTC'")
                .addRoundTrip("timestamp(1)", "TIMESTAMP '1970-01-01 00:00:01.1'", createTimestampWithTimeZoneType(1), "TIMESTAMP '1970-01-01 11:00:01.1 UTC'")
                .addRoundTrip("timestamp(1)", "TIMESTAMP '1970-01-01 00:00:01.9'", createTimestampWithTimeZoneType(1), "TIMESTAMP '1970-01-01 11:00:01.9 UTC'")
                .addRoundTrip("timestamp(2)", "TIMESTAMP '1970-01-01 00:00:01.12'", createTimestampWithTimeZoneType(2), "TIMESTAMP '1970-01-01 11:00:01.12 UTC'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1970-01-01 00:00:01.123'", createTimestampWithTimeZoneType(3), "TIMESTAMP '1970-01-01 11:00:01.123 UTC'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1970-01-01 00:00:01.999'", createTimestampWithTimeZoneType(3), "TIMESTAMP '1970-01-01 11:00:01.999 UTC'")
                .addRoundTrip("timestamp(4)", "TIMESTAMP '1970-01-01 00:00:01.1234'", createTimestampWithTimeZoneType(4), "TIMESTAMP '1970-01-01 11:00:01.1234 UTC'")
                .addRoundTrip("timestamp(5)", "TIMESTAMP '1970-01-01 00:00:01.12345'", createTimestampWithTimeZoneType(5), "TIMESTAMP '1970-01-01 11:00:01.12345 UTC'")
                .addRoundTrip("timestamp(1)", "TIMESTAMP '2020-09-27 12:34:56.1'", createTimestampWithTimeZoneType(1), "TIMESTAMP '2020-09-26 22:34:56.1 UTC'")
                .addRoundTrip("timestamp(1)", "TIMESTAMP '2020-09-27 12:34:56.9'", createTimestampWithTimeZoneType(1), "TIMESTAMP '2020-09-26 22:34:56.9 UTC'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2020-09-27 12:34:56.123'", createTimestampWithTimeZoneType(3), "TIMESTAMP '2020-09-26 22:34:56.123 UTC'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2020-09-27 12:34:56.999'", createTimestampWithTimeZoneType(3), "TIMESTAMP '2020-09-26 22:34:56.999 UTC'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2020-09-27 12:34:56.123456'", createTimestampWithTimeZoneType(6), "TIMESTAMP '2020-09-26 22:34:56.123456 UTC'")

                .execute(getQueryRunner(), session, mysqlCreateAndInsert("tpch.test_timestamp"));
    }

    @Test
    public void testTimestampFromTrino()
    {
        testTimestampFromTrino(UTC);
        testTimestampFromTrino(ZoneId.systemDefault());
        // no DST in 1970, but has DST in later years (e.g. 2018)
        testTimestampFromTrino(ZoneId.of("Europe/Vilnius"));
        // minutes offset change since 1970-01-01, no DST
        testTimestampFromTrino(ZoneId.of("Asia/Kathmandu"));
        testTimestampFromTrino(TestingSession.DEFAULT_TIME_ZONE_KEY.getZoneId());
    }

    private void testTimestampFromTrino(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        SqlDataTypeTest.create()
                // before epoch
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1958-01-01 13:18:03.123'", createTimestampType(3), "TIMESTAMP '1958-01-01 13:18:03.123'")
                // after epoch
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2019-03-18 10:01:17.987'", createTimestampType(3), "TIMESTAMP '2019-03-18 10:01:17.987'")
                // time doubled in JVM zone
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2018-10-28 01:33:17.456'", createTimestampType(3), "TIMESTAMP '2018-10-28 01:33:17.456'")
                // time double in Vilnius
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2018-10-28 03:33:33.333'", createTimestampType(3), "TIMESTAMP '2018-10-28 03:33:33.333'")
                // epoch
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1970-01-01 00:00:00.000'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.000'")
                .addRoundTrip("timestamp(0)", "TIMESTAMP '1970-01-01 00:13:42'", createTimestampType(0), "TIMESTAMP '1970-01-01 00:13:42'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1970-01-01 00:13:42.000'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:13:42.000'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1970-01-01 00:13:42.123456'", createTimestampType(6), "TIMESTAMP '1970-01-01 00:13:42.123456'")
                .addRoundTrip("timestamp(0)", "TIMESTAMP '2018-04-01 02:13:55'", createTimestampType(0), "TIMESTAMP '2018-04-01 02:13:55'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2018-04-01 02:13:55.123'", createTimestampType(3), "TIMESTAMP '2018-04-01 02:13:55.123'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2018-04-01 02:13:55.123456'", createTimestampType(6), "TIMESTAMP '2018-04-01 02:13:55.123456'")

                // time gap in Vilnius
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2018-03-25 03:17:17.123'", createTimestampType(3), "TIMESTAMP '2018-03-25 03:17:17.123'")
                // time gap in Kathmandu
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1986-01-01 00:13:07.123'", createTimestampType(3), "TIMESTAMP '1986-01-01 00:13:07.123'")

                // null
                .addRoundTrip("timestamp", "NULL", createTimestampType(3), "CAST(NULL AS TIMESTAMP(3))")
                .addRoundTrip("timestamp(0)", "NULL", createTimestampType(0), "CAST(NULL AS TIMESTAMP(0))")
                .addRoundTrip("timestamp(1)", "NULL", createTimestampType(1), "CAST(NULL AS TIMESTAMP(1))")
                .addRoundTrip("timestamp(2)", "NULL", createTimestampType(2), "CAST(NULL AS TIMESTAMP(2))")
                .addRoundTrip("timestamp(3)", "NULL", createTimestampType(3), "CAST(NULL AS TIMESTAMP(3))")
                .addRoundTrip("timestamp(4)", "NULL", createTimestampType(4), "CAST(NULL AS TIMESTAMP(4))")
                .addRoundTrip("timestamp(5)", "NULL", createTimestampType(5), "CAST(NULL AS TIMESTAMP(5))")
                .addRoundTrip("timestamp(6)", "NULL", createTimestampType(6), "CAST(NULL AS TIMESTAMP(6))")

                .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_timestamp"))
                .execute(getQueryRunner(), session, trinoCreateAsSelect("test_timestamp"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_timestamp"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert("test_timestamp"));
    }

    /**
     * Additional test supplementing {@link #testTimestampFromTrino} with values that do not necessarily round-trip, including
     * timestamp precision higher than expressible with {@code LocalDateTime}.
     *
     * @see #testTimestampFromTrino
     */
    @Test
    public void testTimestampCoercion()
    {
        SqlDataTypeTest.create()

                // precision 0 ends up as precision 0
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00'", "TIMESTAMP '1970-01-01 00:00:00'")

                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.1'", "TIMESTAMP '1970-01-01 00:00:00.1'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.9'", "TIMESTAMP '1970-01-01 00:00:00.9'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.123'", "TIMESTAMP '1970-01-01 00:00:00.123'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.123000'", "TIMESTAMP '1970-01-01 00:00:00.123000'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.999'", "TIMESTAMP '1970-01-01 00:00:00.999'")
                // max supported precision
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.123456'", "TIMESTAMP '1970-01-01 00:00:00.123456'")

                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.1'", "TIMESTAMP '2020-09-27 12:34:56.1'")
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.9'", "TIMESTAMP '2020-09-27 12:34:56.9'")
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.123'", "TIMESTAMP '2020-09-27 12:34:56.123'")
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.123000'", "TIMESTAMP '2020-09-27 12:34:56.123000'")
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.999'", "TIMESTAMP '2020-09-27 12:34:56.999'")
                // max supported precision
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.123456'", "TIMESTAMP '2020-09-27 12:34:56.123456'")

                // round down
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.1234561'", "TIMESTAMP '1970-01-01 00:00:00.123456'")

                // nanoc round up, end result rounds down
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.123456499'", "TIMESTAMP '1970-01-01 00:00:00.123456'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.123456499999'", "TIMESTAMP '1970-01-01 00:00:00.123456'")

                // round up
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.1234565'", "TIMESTAMP '1970-01-01 00:00:00.123457'")

                // max precision
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.111222333444'", "TIMESTAMP '1970-01-01 00:00:00.111222'")

                // round up to next second
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.9999995'", "TIMESTAMP '1970-01-01 00:00:01.000000'")

                // round up to next day
                .addRoundTrip("TIMESTAMP '1970-01-01 23:59:59.9999995'", "TIMESTAMP '1970-01-02 00:00:00.000000'")

                // negative epoch
                .addRoundTrip("TIMESTAMP '1969-12-31 23:59:59.9999995'", "TIMESTAMP '1970-01-01 00:00:00.000000'")
                .addRoundTrip("TIMESTAMP '1969-12-31 23:59:59.999999499999'", "TIMESTAMP '1969-12-31 23:59:59.999999'")
                .addRoundTrip("TIMESTAMP '1969-12-31 23:59:59.9999994'", "TIMESTAMP '1969-12-31 23:59:59.999999'")

                // CTAS with Trino, where the coercion is done by the connector
                .execute(getQueryRunner(), trinoCreateAsSelect("test_timestamp_coercion"))
                // INSERT with Trino, where the coercion is done by the engine
                .execute(getQueryRunner(), trinoCreateAndInsert("test_timestamp_coercion"));
    }

    @Test
    public void testTimestampWithTimeZoneFromTrinoUtc()
    {
        ZoneId sessionZone = UTC;
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        SqlDataTypeTest.create()
                // after epoch (MySQL's timestamp type doesn't support values <= epoch)
                .addRoundTrip("timestamp(3) WITH TIME ZONE", "TIMESTAMP '2019-03-18 10:01:17.987 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(3), "TIMESTAMP '2019-03-18 10:01:17.987 UTC'")
                // time doubled in JVM zone
                .addRoundTrip("timestamp(3) WITH TIME ZONE", "TIMESTAMP '2018-10-28 01:33:17.456 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(3), "TIMESTAMP '2018-10-28 01:33:17.456 UTC'")
                // time double in Vilnius
                .addRoundTrip("timestamp(3) WITH TIME ZONE", "TIMESTAMP '2018-10-28 03:33:33.333 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(3), "TIMESTAMP '2018-10-28 03:33:33.333 UTC'")
                .addRoundTrip("timestamp(3) WITH TIME ZONE", "TIMESTAMP '1970-01-01 00:13:42.000 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(3), "TIMESTAMP '1970-01-01 00:13:42.000 UTC'")
                .addRoundTrip("timestamp(3) WITH TIME ZONE", "TIMESTAMP '2018-04-01 02:13:55.123 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(3), "TIMESTAMP '2018-04-01 02:13:55.123 UTC'")
                // time gap in Vilnius
                .addRoundTrip("timestamp(3) WITH TIME ZONE", "TIMESTAMP '2018-03-25 03:17:17.000 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(3), "TIMESTAMP '2018-03-25 03:17:17.000 UTC'")
                // time gap in Kathmandu
                .addRoundTrip("timestamp(3) WITH TIME ZONE", "TIMESTAMP '1986-01-01 00:13:07.000 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(3), "TIMESTAMP '1986-01-01 00:13:07.000 UTC'")

                // same as above but with higher precision
                .addRoundTrip("timestamp(6) WITH TIME ZONE", "TIMESTAMP '2019-03-18 10:01:17.987654 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(6), "TIMESTAMP '2019-03-18 10:01:17.987654 UTC'")
                .addRoundTrip("timestamp(6) WITH TIME ZONE", "TIMESTAMP '2018-10-28 01:33:17.456789 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(6), "TIMESTAMP '2018-10-28 01:33:17.456789 UTC'")
                .addRoundTrip("timestamp(6) WITH TIME ZONE", "TIMESTAMP '2018-10-28 03:33:33.333333 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(6), "TIMESTAMP '2018-10-28 03:33:33.333333 UTC'")
                .addRoundTrip("timestamp(6) WITH TIME ZONE", "TIMESTAMP '1970-01-01 00:13:42.000000 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(6), "TIMESTAMP '1970-01-01 00:13:42.000000 UTC'")
                .addRoundTrip("timestamp(6) WITH TIME ZONE", "TIMESTAMP '2018-04-01 02:13:55.123456 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(6), "TIMESTAMP '2018-04-01 02:13:55.123456 UTC'")
                .addRoundTrip("timestamp(6) WITH TIME ZONE", "TIMESTAMP '2018-03-25 03:17:17.000000 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(6), "TIMESTAMP '2018-03-25 03:17:17.000000 UTC'")
                .addRoundTrip("timestamp(6) WITH TIME ZONE", "TIMESTAMP '1986-01-01 00:13:07.000000 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(6), "TIMESTAMP '1986-01-01 00:13:07.000000 UTC'")

                // test arbitrary time for all supported precisions
                .addRoundTrip("timestamp(0) WITH TIME ZONE", "TIMESTAMP '1970-01-01 00:00:01 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(0), "TIMESTAMP '1970-01-01 00:00:01 UTC'")
                .addRoundTrip("timestamp(1) WITH TIME ZONE", "TIMESTAMP '1970-01-01 00:00:01.1 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(1), "TIMESTAMP '1970-01-01 00:00:01.1 UTC'")
                .addRoundTrip("timestamp(1) WITH TIME ZONE", "TIMESTAMP '1970-01-01 00:00:01.9 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(1), "TIMESTAMP '1970-01-01 00:00:01.9 UTC'")
                .addRoundTrip("timestamp(2) WITH TIME ZONE", "TIMESTAMP '1970-01-01 00:00:01.12 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(2), "TIMESTAMP '1970-01-01 00:00:01.12 UTC'")
                .addRoundTrip("timestamp(3) WITH TIME ZONE", "TIMESTAMP '1970-01-01 00:00:01.123 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(3), "TIMESTAMP '1970-01-01 00:00:01.123 UTC'")
                .addRoundTrip("timestamp(3) WITH TIME ZONE", "TIMESTAMP '1970-01-01 00:00:01.999 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(3), "TIMESTAMP '1970-01-01 00:00:01.999 UTC'")
                .addRoundTrip("timestamp(4) WITH TIME ZONE", "TIMESTAMP '1970-01-01 00:00:01.1234 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(4), "TIMESTAMP '1970-01-01 00:00:01.1234 UTC'")
                .addRoundTrip("timestamp(5) WITH TIME ZONE", "TIMESTAMP '1970-01-01 00:00:01.12345 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(5), "TIMESTAMP '1970-01-01 00:00:01.12345 UTC'")
                .addRoundTrip("timestamp(1) WITH TIME ZONE", "TIMESTAMP '2020-09-27 12:34:56.1 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(1), "TIMESTAMP '2020-09-27 12:34:56.1 UTC'")
                .addRoundTrip("timestamp(1) WITH TIME ZONE", "TIMESTAMP '2020-09-27 12:34:56.9 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(1), "TIMESTAMP '2020-09-27 12:34:56.9 UTC'")
                .addRoundTrip("timestamp(3) WITH TIME ZONE", "TIMESTAMP '2020-09-27 12:34:56.123 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(3), "TIMESTAMP '2020-09-27 12:34:56.123 UTC'")
                .addRoundTrip("timestamp(3) WITH TIME ZONE", "TIMESTAMP '2020-09-27 12:34:56.999 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(3), "TIMESTAMP '2020-09-27 12:34:56.999 UTC'")
                .addRoundTrip("timestamp(6) WITH TIME ZONE", "TIMESTAMP '2020-09-27 12:34:56.123456 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(6), "TIMESTAMP '2020-09-27 12:34:56.123456 UTC'")

                .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_timestamp_with_time_zone"))
                .execute(getQueryRunner(), session, trinoCreateAsSelect("test_timestamp_with_time_zone"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_timestamp_with_time_zone"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert("test_timestamp_with_time_zone"));
    }

    @Test
    public void testTimestampWithTimeZoneFromTrinoDefaultTimeZone()
    {
        // Same as above, but insert time zone is default and read time zone is UTC
        ZoneId sessionZone = TestingSession.DEFAULT_TIME_ZONE_KEY.getZoneId();
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        SqlDataTypeTest.create()
                // after epoch (MySQL's timestamp type doesn't support values <= epoch)
                .addRoundTrip("timestamp(3) WITH TIME ZONE", "TIMESTAMP '2019-03-18 10:01:17.987 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(3), "TIMESTAMP '2019-03-17 20:01:17.987 UTC'")
                // time doubled in JVM zone
                .addRoundTrip("timestamp(3) WITH TIME ZONE", "TIMESTAMP '2018-10-28 01:33:17.456 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(3), "TIMESTAMP '2018-10-27 11:33:17.456 UTC'")
                .addRoundTrip("timestamp(3) WITH TIME ZONE", "TIMESTAMP '2018-10-28 03:33:33.333 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(3), "TIMESTAMP '2018-10-27 13:33:33.333 UTC'")
                .addRoundTrip("timestamp(3) WITH TIME ZONE", "TIMESTAMP '1970-01-01 00:13:42.000 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(3), "TIMESTAMP '1970-01-01 11:13:42.000 UTC'")
                .addRoundTrip("timestamp(3) WITH TIME ZONE", "TIMESTAMP '2018-04-01 02:13:55.123 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(3), "TIMESTAMP '2018-03-31 12:13:55.123 UTC'")
                // time gap in Vilnius
                .addRoundTrip("timestamp(3) WITH TIME ZONE", "TIMESTAMP '2018-03-25 03:17:17.000 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(3), "TIMESTAMP '2018-03-24 13:17:17.000 UTC'")
                // time gap in Kathmandu
                .addRoundTrip("timestamp(3) WITH TIME ZONE", "TIMESTAMP '1986-01-01 00:13:07.000 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(3), "TIMESTAMP '1986-01-01 11:13:07.000 UTC'")

                // same as above but with higher precision
                .addRoundTrip("timestamp(6) WITH TIME ZONE", "TIMESTAMP '2019-03-18 10:01:17.987654 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(6), "TIMESTAMP '2019-03-17 20:01:17.987654 UTC'")
                .addRoundTrip("timestamp(6) WITH TIME ZONE", "TIMESTAMP '2018-10-28 01:33:17.456789 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(6), "TIMESTAMP '2018-10-27 11:33:17.456789 UTC'")
                .addRoundTrip("timestamp(6) WITH TIME ZONE", "TIMESTAMP '2018-10-28 03:33:33.333333 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(6), "TIMESTAMP '2018-10-27 13:33:33.333333 UTC'")
                .addRoundTrip("timestamp(6) WITH TIME ZONE", "TIMESTAMP '1970-01-01 00:13:42.000000 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(6), "TIMESTAMP '1970-01-01 11:13:42.000000 UTC'")
                .addRoundTrip("timestamp(6) WITH TIME ZONE", "TIMESTAMP '2018-04-01 02:13:55.123456 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(6), "TIMESTAMP '2018-03-31 12:13:55.123456 UTC'")
                .addRoundTrip("timestamp(6) WITH TIME ZONE", "TIMESTAMP '2018-03-25 03:17:17.000000 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(6), "TIMESTAMP '2018-03-24 13:17:17.000000 UTC'")
                .addRoundTrip("timestamp(6) WITH TIME ZONE", "TIMESTAMP '1986-01-01 00:13:07.000000 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(6), "TIMESTAMP '1986-01-01 11:13:07.000000 UTC'")

                // test arbitrary time for all supported precisions
                .addRoundTrip("timestamp(0) WITH TIME ZONE", "TIMESTAMP '1970-01-01 00:00:01 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(0), "TIMESTAMP '1970-01-01 11:00:01 UTC'")
                .addRoundTrip("timestamp(1) WITH TIME ZONE", "TIMESTAMP '1970-01-01 00:00:01.1 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(1), "TIMESTAMP '1970-01-01 11:00:01.1 UTC'")
                .addRoundTrip("timestamp(1) WITH TIME ZONE", "TIMESTAMP '1970-01-01 00:00:01.9 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(1), "TIMESTAMP '1970-01-01 11:00:01.9 UTC'")
                .addRoundTrip("timestamp(2) WITH TIME ZONE", "TIMESTAMP '1970-01-01 00:00:01.12 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(2), "TIMESTAMP '1970-01-01 11:00:01.12 UTC'")
                .addRoundTrip("timestamp(3) WITH TIME ZONE", "TIMESTAMP '1970-01-01 00:00:01.123 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(3), "TIMESTAMP '1970-01-01 11:00:01.123 UTC'")
                .addRoundTrip("timestamp(3) WITH TIME ZONE", "TIMESTAMP '1970-01-01 00:00:01.999 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(3), "TIMESTAMP '1970-01-01 11:00:01.999 UTC'")
                .addRoundTrip("timestamp(4) WITH TIME ZONE", "TIMESTAMP '1970-01-01 00:00:01.1234 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(4), "TIMESTAMP '1970-01-01 11:00:01.1234 UTC'")
                .addRoundTrip("timestamp(5) WITH TIME ZONE", "TIMESTAMP '1970-01-01 00:00:01.12345 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(5), "TIMESTAMP '1970-01-01 11:00:01.12345 UTC'")
                .addRoundTrip("timestamp(1) WITH TIME ZONE", "TIMESTAMP '2020-09-27 12:34:56.1 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(1), "TIMESTAMP '2020-09-26 22:34:56.1 UTC'")
                .addRoundTrip("timestamp(1) WITH TIME ZONE", "TIMESTAMP '2020-09-27 12:34:56.9 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(1), "TIMESTAMP '2020-09-26 22:34:56.9 UTC'")
                .addRoundTrip("timestamp(3) WITH TIME ZONE", "TIMESTAMP '2020-09-27 12:34:56.123 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(3), "TIMESTAMP '2020-09-26 22:34:56.123 UTC'")
                .addRoundTrip("timestamp(3) WITH TIME ZONE", "TIMESTAMP '2020-09-27 12:34:56.999 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(3), "TIMESTAMP '2020-09-26 22:34:56.999 UTC'")
                .addRoundTrip("timestamp(6) WITH TIME ZONE", "TIMESTAMP '2020-09-27 12:34:56.123456 %s'".formatted(sessionZone), createTimestampWithTimeZoneType(6), "TIMESTAMP '2020-09-26 22:34:56.123456 UTC'")

                .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_timestamp_with_time_zone"))
                .execute(getQueryRunner(), session, trinoCreateAsSelect("test_timestamp_with_time_zone"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_timestamp_with_time_zone"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert("test_timestamp_with_time_zone"));
    }

    @Test
    public void testUnsupportedTimestampWithTimeZoneValues()
    {
        // The range for TIMESTAMP values is '1970-01-01 00:00:01.000000' to '2038-01-19 03:14:07.499999'
        try (TestTable table = new TestTable(mySqlServer::execute, "tpch.test_unsupported_timestamp", "(data TIMESTAMP)")) {
            // Verify MySQL writes -- the server timezone is set to Pacific/Apia, so we have to account for that when inserting into MySQL
            assertMySqlQueryFails(
                    "INSERT INTO " + table.getName() + " VALUES ('1969-12-31 13:00:00')",
                    "Data truncation: Incorrect datetime value: '1969-12-31 13:00:00' for column 'data' at row 1");
            assertMySqlQueryFails(
                    "INSERT INTO " + table.getName() + " VALUES ('2038-01-19 16:14:08')",
                    "Data truncation: Incorrect datetime value: '2038-01-19 16:14:08' for column 'data' at row 1");

            // Verify Trino writes
            assertQueryFails(
                    "INSERT INTO " + table.getName() + " VALUES (TIMESTAMP '1970-01-01 00:00:00 UTC')", // min - 1
                    "Failed to insert data: Data truncation: Incorrect datetime value: '1969-12-31 16:00:00' for column 'data' at row 1");
            assertQueryFails(
                    "INSERT INTO " + table.getName() + " VALUES (TIMESTAMP '2038-01-19 03:14:08 UTC')", // max + 1
                    "Failed to insert data: Data truncation: Incorrect datetime value: '2038-01-18 21:14:08' for column 'data' at row 1");
        }
    }

    /**
     * Additional test supplementing {@link #testTimestampWithTimeZoneFromTrinoUtc()} with values that do not necessarily round-trip.
     *
     * @see #testTimestampWithTimeZoneFromTrinoUtc
     */
    @Test
    public void testTimestampWithTimeZoneCoercion()
    {
        SqlDataTypeTest.create()
                // precision 0 ends up as precision 0
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:01 UTC'", "TIMESTAMP '1970-01-01 00:00:01 UTC'")

                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:01.1 UTC'", "TIMESTAMP '1970-01-01 00:00:01.1 UTC'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:01.9 UTC'", "TIMESTAMP '1970-01-01 00:00:01.9 UTC'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:01.123 UTC'", "TIMESTAMP '1970-01-01 00:00:01.123 UTC'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:01.123000 UTC'", "TIMESTAMP '1970-01-01 00:00:01.123000 UTC'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:01.999 UTC'", "TIMESTAMP '1970-01-01 00:00:01.999 UTC'")
                // max supported precision
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:01.123456 UTC'", "TIMESTAMP '1970-01-01 00:00:01.123456 UTC'")

                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.1 UTC'", "TIMESTAMP '2020-09-27 12:34:56.1 UTC'")
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.9 UTC'", "TIMESTAMP '2020-09-27 12:34:56.9 UTC'")
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.123 UTC'", "TIMESTAMP '2020-09-27 12:34:56.123 UTC'")
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.123000 UTC'", "TIMESTAMP '2020-09-27 12:34:56.123000 UTC'")
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.999 UTC'", "TIMESTAMP '2020-09-27 12:34:56.999 UTC'")
                // max supported precision
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.123456 UTC'", "TIMESTAMP '2020-09-27 12:34:56.123456 UTC'")

                // round down
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:01.1234561 UTC'", "TIMESTAMP '1970-01-01 00:00:01.123456 UTC'")

                // nanoc round up, end result rounds down
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:01.123456499 UTC'", "TIMESTAMP '1970-01-01 00:00:01.123456 UTC'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:01.123456499999 UTC'", "TIMESTAMP '1970-01-01 00:00:01.123456 UTC'")

                // round up
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:01.1234565 UTC'", "TIMESTAMP '1970-01-01 00:00:01.123457 UTC'")

                // max precision
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:01.111222333444 UTC'", "TIMESTAMP '1970-01-01 00:00:01.111222 UTC'")

                // round up to next second
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:01.9999995 UTC'", "TIMESTAMP '1970-01-01 00:00:02.000000 UTC'")

                // round up to next day
                .addRoundTrip("TIMESTAMP '1970-01-01 23:59:59.9999995 UTC'", "TIMESTAMP '1970-01-02 00:00:00.000000 UTC'")

                // negative epoch is not supported by MySQL TIMESTAMP

                // CTAS with Trino, where the coercion is done by the connector
                .execute(getQueryRunner(), trinoCreateAsSelect("test_timestamp_with_time_zone_coercion"))
                // INSERT with Trino, where the coercion is done by the engine
                .execute(getQueryRunner(), trinoCreateAndInsert("test_timestamp_with_time_zone_coercion"));
    }

    @Test
    public void testZeroTimestamp()
            throws Exception
    {
        String connectionUrl = mySqlServer.getJdbcUrl() + "&zeroDateTimeBehavior=convertToNull";

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(getSession()).build();
        queryRunner.installPlugin(new MySqlPlugin());
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("connection-url", connectionUrl)
                .put("connection-user", mySqlServer.getUsername())
                .put("connection-password", mySqlServer.getPassword())
                .buildOrThrow();
        queryRunner.createCatalog("mysql", "mysql", properties);

        try (Connection connection = DriverManager.getConnection(connectionUrl, mySqlServer.getUsername(), mySqlServer.getPassword());
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE tpch.test_zero_ts(col_dt datetime, col_ts timestamp)");
            statement.execute("SET sql_mode=''");
            statement.execute("INSERT INTO tpch.test_zero_ts(col_dt, col_ts) VALUES ('0000-00-00 00:00:00', '0000-00-00 00:00:00')");

            assertThat(queryRunner.execute("SELECT col_dt FROM test_zero_ts").getOnlyValue()).isNull();
            assertThat(queryRunner.execute("SELECT col_ts FROM test_zero_ts").getOnlyValue()).isNull();

            statement.execute("DROP TABLE tpch.test_zero_ts");
        }
    }

    private DataSetup trinoCreateAsSelect(String tableNamePrefix)
    {
        return trinoCreateAsSelect(getSession(), tableNamePrefix);
    }

    private DataSetup trinoCreateAsSelect(Session session, String tableNamePrefix)
    {
        return new CreateAsSelectDataSetup(new TrinoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    private DataSetup trinoCreateAndInsert(String tableNamePrefix)
    {
        return trinoCreateAndInsert(getSession(), tableNamePrefix);
    }

    private DataSetup trinoCreateAndInsert(Session session, String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(new TrinoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    private DataSetup mysqlCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(mySqlServer::execute, tableNamePrefix);
    }

    private void assertMySqlQueryFails(@Language("SQL") String sql, String expectedMessage)
    {
        assertThatThrownBy(() -> mySqlServer.execute(sql))
                .cause()
                .hasMessageContaining(expectedMessage);
    }
}
