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
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingSession;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.datatype.CreateAsSelectDataSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.datatype.SqlDataTypeTest;
import io.trino.testing.sql.TrinoSqlExecutor;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.time.ZoneId;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.mysql.MySqlQueryRunner.createMySqlQueryRunner;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static java.time.ZoneOffset.UTC;

public class TestMysqlTimestampMillisPrecisionMapping
        extends AbstractTestQueryFramework
{
    private TestingMySqlServer mySqlServer;

    private final ZoneId jvmZone = ZoneId.of("America/Bahia_Banderas");
    // no DST in 1970, but has DST in later years (e.g. 2018)
    private final ZoneId vilnius = ZoneId.of("Europe/Vilnius");

    @BeforeClass
    public void setUp()
    {
        checkState(jvmZone.getId().equals("America/Bahia_Banderas"), "This test assumes certain JVM time zone");
        LocalDate dateOfLocalTimeChangeForwardAtMidnightInJvmZone = LocalDate.of(1970, 1, 1);
        verify(jvmZone.getRules().getValidOffsets(dateOfLocalTimeChangeForwardAtMidnightInJvmZone.atStartOfDay()).isEmpty());

        LocalDate dateOfLocalTimeChangeForwardAtMidnightInSomeZone = LocalDate.of(1983, 4, 1);
        verify(vilnius.getRules().getValidOffsets(dateOfLocalTimeChangeForwardAtMidnightInSomeZone.atStartOfDay()).isEmpty());
        LocalDate dateOfLocalTimeChangeBackwardAtMidnightInSomeZone = LocalDate.of(1983, 10, 1);
        verify(vilnius.getRules().getValidOffsets(dateOfLocalTimeChangeBackwardAtMidnightInSomeZone.atStartOfDay().minusMinutes(1)).size() == 2);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        mySqlServer = closeAfterClass(new TestingMySqlServer());
        return createMySqlQueryRunner(mySqlServer, ImmutableMap.of(), ImmutableMap.of("mysql.enforce-millis-timestamp-precision", "true"), ImmutableList.of());
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    public void testMySqlDatetimeType(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        SqlDataTypeTest.create()
                // before epoch
                .addRoundTrip("datetime(3)", "TIMESTAMP '1958-01-01 13:18:03.123'", TIMESTAMP_MILLIS, "TIMESTAMP '1958-01-01 13:18:03.123'")
                // after epoch
                .addRoundTrip("datetime(3)", "TIMESTAMP '2019-03-18 10:01:17.987'", TIMESTAMP_MILLIS, "TIMESTAMP '2019-03-18 10:01:17.987'")
                // time doubled in JVM zone
                .addRoundTrip("datetime(3)", "TIMESTAMP '2018-10-28 01:33:17.456'", TIMESTAMP_MILLIS, "TIMESTAMP '2018-10-28 01:33:17.456'")
                // time double in Vilnius
                .addRoundTrip("datetime(3)", "TIMESTAMP '2018-10-28 03:33:33.333'", TIMESTAMP_MILLIS, "TIMESTAMP '2018-10-28 03:33:33.333'")
                // epoch
                .addRoundTrip("datetime(3)", "TIMESTAMP '1970-01-01 00:00:00.000'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:00.000'")
                .addRoundTrip("datetime(3)", "TIMESTAMP '1970-01-01 00:13:42.000'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:13:42.000'")
                .addRoundTrip("datetime(3)", "TIMESTAMP '2018-04-01 02:13:55.123'", TIMESTAMP_MILLIS, "TIMESTAMP '2018-04-01 02:13:55.123'")
                // time gap in Vilnius
                .addRoundTrip("datetime(3)", "TIMESTAMP '2018-03-25 03:17:17.000'", TIMESTAMP_MILLIS, "TIMESTAMP '2018-03-25 03:17:17.000'")
                // time gap in Kathmandu
                .addRoundTrip("datetime(3)", "TIMESTAMP '1986-01-01 00:13:07.000'", TIMESTAMP_MILLIS, "TIMESTAMP '1986-01-01 00:13:07.000'")

                // same as above but with higher precision
                .addRoundTrip("datetime(6)", "TIMESTAMP '1958-01-01 13:18:03.123000'", TIMESTAMP_MILLIS, "TIMESTAMP '1958-01-01 13:18:03.123'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '2019-03-18 10:01:17.987000'", TIMESTAMP_MILLIS, "TIMESTAMP '2019-03-18 10:01:17.987'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '2018-10-28 01:33:17.456000'", TIMESTAMP_MILLIS, "TIMESTAMP '2018-10-28 01:33:17.456'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '2018-10-28 03:33:33.333000'", TIMESTAMP_MILLIS, "TIMESTAMP '2018-10-28 03:33:33.333'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '1970-01-01 00:00:00.000000'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:00.000'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '1970-01-01 00:13:42.000000'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:13:42.000'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '2018-04-01 02:13:55.123000'", TIMESTAMP_MILLIS, "TIMESTAMP '2018-04-01 02:13:55.123'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '2018-03-25 03:17:17.000000'", TIMESTAMP_MILLIS, "TIMESTAMP '2018-03-25 03:17:17.000'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '1986-01-01 00:13:07.000000'", TIMESTAMP_MILLIS, "TIMESTAMP '1986-01-01 00:13:07.000'")

                // test arbitrary time for all supported precisions
                .addRoundTrip("datetime(0)", "TIMESTAMP '1970-01-01 00:00:01'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.000'")
                .addRoundTrip("datetime(1)", "TIMESTAMP '1970-01-01 00:00:01.1'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.100'")
                .addRoundTrip("datetime(2)", "TIMESTAMP '1970-01-01 00:00:01.12'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.120'")
                .addRoundTrip("datetime(3)", "TIMESTAMP '1970-01-01 00:00:01.123'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")
                .addRoundTrip("datetime(4)", "TIMESTAMP '1970-01-01 00:00:01.1234'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")
                .addRoundTrip("datetime(5)", "TIMESTAMP '1970-01-01 00:00:01.12345'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '1970-01-01 00:00:01.123456'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")

                // test rounding for precisions too high for MySQL
                .addRoundTrip("datetime(6)", "TIMESTAMP '1970-01-01 00:00:01.1234560'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '1970-01-01 00:00:01.12345649999'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '1970-01-01 00:00:01.1234565'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '1970-01-01 00:00:01.1234569'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")

                .execute(getQueryRunner(), session, mysqlCreateAndInsert("tpch.test_datetime"));

        SqlDataTypeTest.create()
                // precision 0 ends up as precision 0
                .addRoundTrip("datetime(0)", "TIMESTAMP '1970-01-01 00:00:01'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:01.000'")

                .addRoundTrip("datetime(1)", "TIMESTAMP '1970-01-01 00:00:00.1'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.100'")
                .addRoundTrip("datetime(1)", "TIMESTAMP '1970-01-01 00:00:00.9'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.900'")
                .addRoundTrip("datetime(3)", "TIMESTAMP '1970-01-01 00:00:00.123'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.123'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '1970-01-01 00:00:00.123000'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.123'")
                .addRoundTrip("datetime(3)", "TIMESTAMP '1970-01-01 00:00:00.999'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.999'")
                // max supported precision
                .addRoundTrip("datetime(6)", "TIMESTAMP '1970-01-01 00:00:00.123456'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.123'")

                .addRoundTrip("datetime(1)", "TIMESTAMP '2020-09-27 12:34:56.1'", createTimestampType(3), "TIMESTAMP '2020-09-27 12:34:56.100'")
                .addRoundTrip("datetime(1)", "TIMESTAMP '2020-09-27 12:34:56.9'", createTimestampType(3), "TIMESTAMP '2020-09-27 12:34:56.900'")
                .addRoundTrip("datetime(3)", "TIMESTAMP '2020-09-27 12:34:56.123'", createTimestampType(3), "TIMESTAMP '2020-09-27 12:34:56.123'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '2020-09-27 12:34:56.123000'", createTimestampType(3), "TIMESTAMP '2020-09-27 12:34:56.123'")
                .addRoundTrip("datetime(3)", "TIMESTAMP '2020-09-27 12:34:56.999'", createTimestampType(3), "TIMESTAMP '2020-09-27 12:34:56.999'")
                // max supported precision
                .addRoundTrip("datetime(6)", "TIMESTAMP '2020-09-27 12:34:56.123456'", createTimestampType(3), "TIMESTAMP '2020-09-27 12:34:56.123'")

                // round down
                .addRoundTrip("datetime(6)", "TIMESTAMP '1970-01-01 00:00:00.123451'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.123'")

                // nanos round up, end result rounds down
                .addRoundTrip("datetime(6)", "TIMESTAMP '1970-01-01 00:00:00.123499'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.123'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '1970-01-01 00:00:00.123399'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.123'")

                // round up
                .addRoundTrip("datetime(6)", "TIMESTAMP '1970-01-01 00:00:00.123999'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.124'")

                // max precision
                .addRoundTrip("datetime(6)", "TIMESTAMP '1970-01-01 00:00:00.123456'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.123'")

                // round up to next second
                .addRoundTrip("datetime(6)", "TIMESTAMP '1970-01-01 00:00:00.999999'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:01.000'")

                // round up to next day
                .addRoundTrip("datetime(6)", "TIMESTAMP '1970-01-01 23:59:59.999999'", createTimestampType(3), "TIMESTAMP '1970-01-02 00:00:00.000'")

                // negative epoch - round up
                .addRoundTrip("datetime(6)", "TIMESTAMP '1969-12-31 23:59:59.999995'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.000'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '1969-12-31 23:59:59.999949'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.000'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '1969-12-31 23:59:59.999994'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.000'")

                .execute(getQueryRunner(), session, mysqlCreateAndInsert("tpch.test_timestamp"));
    }

    /**
     * Read {@code TIMESTAMP}s inserted by MySQL as Trino {@code TIMESTAMP}s
     */
    @Test(dataProvider = "sessionZonesDataProvider")
    public void testTimestampFromMySql(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        // Same as above but with inserts from MySQL - i.e. read path
        SqlDataTypeTest.create()
                // before epoch (MySQL's timestamp type doesn't support values <= epoch)
                //.addRoundTrip("timestamp(3)", "TIMESTAMP '1958-01-01 13:18:03.123'", createTimestampType(3), "TIMESTAMP '1958-01-01 13:18:03.000'")
                // after epoch
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2019-03-18 10:01:17.987'", TIMESTAMP_MILLIS, "TIMESTAMP '2019-03-18 10:01:17.987'")
                // time doubled in JVM zone
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2018-10-28 01:33:17.456'", TIMESTAMP_MILLIS, "TIMESTAMP '2018-10-28 01:33:17.456'")
                // time double in Vilnius
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2018-10-28 03:33:33.333'", TIMESTAMP_MILLIS, "TIMESTAMP '2018-10-28 03:33:33.333'")
                // epoch (MySQL's timestamp type doesn't support values <= epoch)
                //.addRoundTrip("timestamp(3)", "TIMESTAMP '1970-01-01 00:00:00.000'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.000'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1970-01-01 00:13:42.000'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:13:42.000'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2018-04-01 02:13:55.123'", TIMESTAMP_MILLIS, "TIMESTAMP '2018-04-01 02:13:55.123'")
                // time gap in Vilnius
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2018-03-25 03:17:17.000'", TIMESTAMP_MILLIS, "TIMESTAMP '2018-03-25 03:17:17.000'")
                // time gap in Kathmandu
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1986-01-01 00:13:07.000'", TIMESTAMP_MILLIS, "TIMESTAMP '1986-01-01 00:13:07.000'")

                // same as above but with higher precision - note that currently anything other than timestamp(3) can only be inserted directly via MySQL
                // MySQL's timestamp type doesn't support values <= epoch
                //.addRoundTrip("timestamp(6)", "TIMESTAMP '1958-01-01 13:18:03.123000'", createTimestampType(3), "TIMESTAMP '1958-01-01 13:18:03.123'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2019-03-18 10:01:17.987000'", TIMESTAMP_MILLIS, "TIMESTAMP '2019-03-18 10:01:17.987'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2018-10-28 01:33:17.456000'", TIMESTAMP_MILLIS, "TIMESTAMP '2018-10-28 01:33:17.456'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2018-10-28 03:33:33.333000'", TIMESTAMP_MILLIS, "TIMESTAMP '2018-10-28 03:33:33.333'")
                // MySQL's timestamp type doesn't support values <= epoch
                //.addRoundTrip("timestamp(6)", "TIMESTAMP '1970-01-01 00:00:00.000000'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.000'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1970-01-01 00:13:42.000000'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:13:42.000'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2018-04-01 02:13:55.123000'", TIMESTAMP_MILLIS, "TIMESTAMP '2018-04-01 02:13:55.123'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2018-03-25 03:17:17.000000'", TIMESTAMP_MILLIS, "TIMESTAMP '2018-03-25 03:17:17.000'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1986-01-01 00:13:07.000000'", TIMESTAMP_MILLIS, "TIMESTAMP '1986-01-01 00:13:07.000'")

                // test arbitrary time for all supported precisions
                .addRoundTrip("timestamp(0)", "TIMESTAMP '1970-01-01 00:00:01'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.000'")
                .addRoundTrip("timestamp(1)", "TIMESTAMP '1970-01-01 00:00:01.1'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.100'")
                .addRoundTrip("timestamp(2)", "TIMESTAMP '1970-01-01 00:00:01.12'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.120'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1970-01-01 00:00:01.123'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")
                .addRoundTrip("timestamp(4)", "TIMESTAMP '1970-01-01 00:00:01.1234'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")
                .addRoundTrip("timestamp(5)", "TIMESTAMP '1970-01-01 00:00:01.12345'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1970-01-01 00:00:01.123456'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1970-01-01 00:00:01.1234560'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1970-01-01 00:00:01.12345649999'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1970-01-01 00:00:01.1234565'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1970-01-01 00:00:01.1234569'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")

                // precision 0 ends up as precision 0
                .addRoundTrip("timestamp(0)", "TIMESTAMP '1970-01-01 00:00:01'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.000'")

                .addRoundTrip("timestamp(1)", "TIMESTAMP '1970-01-01 00:00:01.1'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.100'")
                .addRoundTrip("timestamp(1)", "TIMESTAMP '1970-01-01 00:00:01.9'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.900'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1970-01-01 00:00:01.123'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1970-01-01 00:00:01.123000'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1970-01-01 00:00:01.999'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.999'")
                // max supported precision
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1970-01-01 00:00:01.123456'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")

                .addRoundTrip("timestamp(1)", "TIMESTAMP '2020-09-27 12:34:56.1'", TIMESTAMP_MILLIS, "TIMESTAMP '2020-09-27 12:34:56.100'")
                .addRoundTrip("timestamp(1)", "TIMESTAMP '2020-09-27 12:34:56.9'", TIMESTAMP_MILLIS, "TIMESTAMP '2020-09-27 12:34:56.900'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2020-09-27 12:34:56.123'", TIMESTAMP_MILLIS, "TIMESTAMP '2020-09-27 12:34:56.123'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2020-09-27 12:34:56.123000'", TIMESTAMP_MILLIS, "TIMESTAMP '2020-09-27 12:34:56.123'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2020-09-27 12:34:56.999'", TIMESTAMP_MILLIS, "TIMESTAMP '2020-09-27 12:34:56.999'")
                // max supported precision
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2020-09-27 12:34:56.123456'", TIMESTAMP_MILLIS, "TIMESTAMP '2020-09-27 12:34:56.123'")

                // round down
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1970-01-01 00:00:01.12345671'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")

                // nanos round up, end result rounds down
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1970-01-01 00:00:01.1234567499'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1970-01-01 00:00:01.123456749999'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")

                // round up
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1970-01-01 00:00:01.12345675'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")

                // max precision
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1970-01-01 00:00:01.111222333444'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.111'")

                // round up to next second
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1970-01-01 00:00:01.99999995'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:02.000'")

                // round up to next day
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1970-01-01 23:59:59.99999995'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-02 00:00:00.000'")

                .execute(getQueryRunner(), session, mysqlCreateAndInsert("tpch.test_timestamp"));
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    public void testTimestampFromTrino(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        SqlDataTypeTest.create()
                // before epoch
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1958-01-01 13:18:03.123'", TIMESTAMP_MILLIS, "TIMESTAMP '1958-01-01 13:18:03.123'")
                // after epoch
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2019-03-18 10:01:17.987'", TIMESTAMP_MILLIS, "TIMESTAMP '2019-03-18 10:01:17.987'")
                // time doubled in JVM zone
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2018-10-28 01:33:17.456'", TIMESTAMP_MILLIS, "TIMESTAMP '2018-10-28 01:33:17.456'")
                // time double in Vilnius
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2018-10-28 03:33:33.333'", TIMESTAMP_MILLIS, "TIMESTAMP '2018-10-28 03:33:33.333'")
                // epoch
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1970-01-01 00:00:00.000'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:00.000'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1970-01-01 00:13:42.000'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:13:42.000'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2018-04-01 02:13:55.123'", TIMESTAMP_MILLIS, "TIMESTAMP '2018-04-01 02:13:55.123'")
                // time gap in Vilnius
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2018-03-25 03:17:17.000'", TIMESTAMP_MILLIS, "TIMESTAMP '2018-03-25 03:17:17.000'")
                // time gap in Kathmandu
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1986-01-01 00:13:07.000'", TIMESTAMP_MILLIS, "TIMESTAMP '1986-01-01 00:13:07.000'")

                .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_timestamp"))
                .execute(getQueryRunner(), session, trinoCreateAsSelect("test_timestamp"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_timestamp"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert("test_timestamp"));
    }

    @DataProvider
    public Object[][] sessionZonesDataProvider()
    {
        return new Object[][] {
                {UTC},
                {ZoneId.systemDefault()},
                // no DST in 1970, but has DST in later years (e.g. 2018)
                {ZoneId.of("Europe/Vilnius")},
                // minutes offset change since 1970-01-01, no DST
                {ZoneId.of("Asia/Kathmandu")},
                {TestingSession.DEFAULT_TIME_ZONE_KEY.getZoneId()},
        };
    }

    private DataSetup mysqlCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(mySqlServer::execute, tableNamePrefix);
    }

    private DataSetup trinoCreateAndInsert(String tableNamePrefix)
    {
        return trinoCreateAndInsert(getSession(), tableNamePrefix);
    }

    private DataSetup trinoCreateAndInsert(Session session, String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(new TrinoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    private DataSetup trinoCreateAsSelect(String tableNamePrefix)
    {
        return trinoCreateAsSelect(getSession(), tableNamePrefix);
    }

    private DataSetup trinoCreateAsSelect(Session session, String tableNamePrefix)
    {
        return new CreateAsSelectDataSetup(new TrinoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }
}
