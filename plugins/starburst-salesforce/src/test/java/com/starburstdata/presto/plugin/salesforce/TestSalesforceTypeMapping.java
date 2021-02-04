/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.salesforce;

import com.google.common.collect.ImmutableList;
import com.starburstdata.presto.plugin.salesforce.testing.datatype.SalesforceCreateAndInsertDataSetup;
import com.starburstdata.presto.plugin.salesforce.testing.datatype.SalesforceCreateAsSelectDataSetup;
import com.starburstdata.presto.plugin.salesforce.testing.datatype.SqlDataTypeTest;
import io.trino.Session;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.VarcharType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingSession;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.sql.JdbcSqlExecutor;
import io.trino.testing.sql.TrinoSqlExecutor;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.trino.spi.type.TimeType.TIME_SECONDS;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static java.time.format.DateTimeFormatter.ISO_DATE;
import static java.util.Objects.requireNonNull;

public class TestSalesforceTypeMapping
        extends AbstractTestQueryFramework
{
    private final ZoneId jvmZone = ZoneId.systemDefault();

    // no DST in 1970, but has DST in later years (e.g. 2018)
    private final ZoneId vilnius = ZoneId.of("Europe/Vilnius");

    // minutes offset change since 1970-01-01, no DST
    private final ZoneId kathmandu = ZoneId.of("Asia/Kathmandu");

    private String jdbcUrl;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        // Create a SalesforceConfig to get the JDBC URL that we can forward to the testing classes
        // to reset the metadata cache between runs
        // Note that if the JDBC connection properties are different than what is used by the
        // QueryRunner than we may have issues with the metadata cache
        SalesforceConfig salesforceConfig = new SalesforceConfig()
                .setUser(requireNonNull(System.getProperty("salesforce.test.user1.username"), "salesforce.test.user1.username is not set"))
                .setPassword(requireNonNull(System.getProperty("salesforce.test.user1.password"), "salesforce.test.user1.password is not set"))
                .setSecurityToken(requireNonNull(System.getProperty("salesforce.test.user1.security-token"), "salesforce.test.user1.security-token is not set"))
                .setSandboxEnabled(true);

        this.jdbcUrl = SalesforceConnectionFactory.getConnectionUrl(salesforceConfig);
        return SalesforceQueryRunner.builder()
                .enableWrites() // Enable writes so we can create tables and write data to them
                .setTables(ImmutableList.of()) // No tables needed for type mapping tests
                .build();
    }

    @Test
    public void testDouble()
    {
        // Max numeric value is 18 precision 0 scale when creating double type
        SqlDataTypeTest.create()
                .addRoundTrip("double", "-1.0E17", DoubleType.DOUBLE, "-1.0E17")
                .addRoundTrip("double", "1.0E17", DoubleType.DOUBLE, "1.0E17")
                .addRoundTrip("double", "NULL", DoubleType.DOUBLE, "CAST(NULL as DOUBLE)")
                .execute(getQueryRunner(), trinoCreateAsSelect("trino_test_double"));
    }

    @Test
    public void testTrinoCreatedParameterizedVarchar()
    {
        varcharSqlDataTypeTest()
                .execute(getQueryRunner(), trinoCreateAsSelect("trino_test_param_varchar"));
    }

    @Test
    public void testSalesforceCreatedParameterizedVarchar()
    {
        varcharSqlDataTypeTest()
                .execute(getQueryRunner(), salesforceCreateAndInsert("salesforce.sf_test_param_varchar"));
    }

    private SqlDataTypeTest varcharSqlDataTypeTest()
    {
        // Maximum varchar length is 255 and you must specify a length
        return SqlDataTypeTest.create()
                .addRoundTrip("VARCHAR(10)", "'text_a'", VarcharType.createVarcharType(10), "CAST('text_a' AS VARCHAR(10))")
                .addRoundTrip("VARCHAR(255)", "'text_b'", VarcharType.createVarcharType(255), "CAST('text_b' AS VARCHAR(255))");
    }

    @Test
    public void testDate()
    {
        ZoneId jvmZone = ZoneId.systemDefault();
        checkState(jvmZone.getId().equals("America/Bahia_Banderas"), "This test assumes certain JVM time zone");
        LocalDate dateOfLocalTimeChangeForwardAtMidnightInJvmZone = LocalDate.of(1970, 1, 1);
        checkIsGap(jvmZone, dateOfLocalTimeChangeForwardAtMidnightInJvmZone.atStartOfDay());

        ZoneId someZone = ZoneId.of("Europe/Vilnius");
        LocalDate dateOfLocalTimeChangeForwardAtMidnightInSomeZone = LocalDate.of(1983, 4, 1);
        checkIsGap(someZone, dateOfLocalTimeChangeForwardAtMidnightInSomeZone.atStartOfDay());
        LocalDate dateOfLocalTimeChangeBackwardAtMidnightInSomeZone = LocalDate.of(1983, 10, 1);
        checkIsDoubled(someZone, dateOfLocalTimeChangeBackwardAtMidnightInSomeZone.atStartOfDay().minusMinutes(1));

        String dateOfLocalTimeChangeForwardAtMidnightInJvmZoneLiteral = format("DATE '%s'", ISO_DATE.format(dateOfLocalTimeChangeForwardAtMidnightInJvmZone));
        String dateOfLocalTimeChangeForwardAtMidnightInSomeZoneLiteral = format("DATE '%s'", ISO_DATE.format(dateOfLocalTimeChangeForwardAtMidnightInSomeZone));
        String dateOfLocalTimeChangeBackwardAtMidnightInSomeZoneLiteral = format("DATE '%s'", ISO_DATE.format(dateOfLocalTimeChangeBackwardAtMidnightInSomeZone));

        SqlDataTypeTest testCases = SqlDataTypeTest.create()
                .addRoundTrip("DATE", "DATE '1952-04-03'", DateType.DATE, "DATE '1952-04-03'") // before epoch
                .addRoundTrip("DATE", "DATE '1970-01-01'", DateType.DATE, "DATE '1970-01-01'")
                .addRoundTrip("DATE", "DATE '1970-02-03'", DateType.DATE, "DATE '1970-02-03'")
                .addRoundTrip("DATE", "DATE '2017-07-01'", DateType.DATE, "DATE '2017-07-01'") // summer on northern hemisphere (possible DST)
                .addRoundTrip("DATE", "DATE '2017-01-01'", DateType.DATE, "DATE '2017-01-01'") // winter on northern hemisphere (possible DST on southern hemisphere)
                .addRoundTrip("DATE", dateOfLocalTimeChangeForwardAtMidnightInJvmZoneLiteral, DateType.DATE, dateOfLocalTimeChangeForwardAtMidnightInJvmZoneLiteral)
                .addRoundTrip("DATE", dateOfLocalTimeChangeForwardAtMidnightInSomeZoneLiteral, DateType.DATE, dateOfLocalTimeChangeForwardAtMidnightInSomeZoneLiteral)
                .addRoundTrip("DATE", dateOfLocalTimeChangeBackwardAtMidnightInSomeZoneLiteral, DateType.DATE, dateOfLocalTimeChangeBackwardAtMidnightInSomeZoneLiteral);

        for (String timeZoneId : ImmutableList.of(UTC_KEY.getId(), jvmZone.getId(), someZone.getId())) {
            Session session = Session.builder(getQueryRunner().getDefaultSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(timeZoneId))
                    .build();
            testCases.execute(getQueryRunner(), session, salesforceCreateAndInsert("salesforce.test_date"));
            testCases.execute(getQueryRunner(), session, trinoCreateAsSelect("test_date"));
        }
    }

    @Test(dataProvider = "testTimestampDataProvider")
    public void testTime(ZoneId sessionZone)
    {
        LocalTime timeGapInJvmZone = LocalTime.of(0, 12, 34, 0 /*567_000_000*/);
        checkIsGap(jvmZone, timeGapInJvmZone.atDate(LocalDate.EPOCH));

        // Comment out nanoseconds, only second precision is supported
        SqlDataTypeTest tests = SqlDataTypeTest.create()
                .addRoundTrip("TIME(0)", "TIME '01:12:34'", TIME_SECONDS, "TIME '01:12:34'")
                .addRoundTrip("TIME(0)", "TIME '02:12:34'", TIME_SECONDS, "TIME '02:12:34'")
                .addRoundTrip("TIME(0)", "TIME '03:12:34'", TIME_SECONDS, "TIME '03:12:34'")
                .addRoundTrip("TIME(0)", "TIME '04:12:34'", TIME_SECONDS, "TIME '04:12:34'")
                .addRoundTrip("TIME(0)", "TIME '05:12:34'", TIME_SECONDS, "TIME '05:12:34'")
                .addRoundTrip("TIME(0)", "TIME '06:12:34'", TIME_SECONDS, "TIME '06:12:34'")
                .addRoundTrip("TIME(0)", "TIME '09:12:34'", TIME_SECONDS, "TIME '09:12:34'")
                .addRoundTrip("TIME(0)", "TIME '10:12:34'", TIME_SECONDS, "TIME '10:12:34'")
                .addRoundTrip("TIME(0)", "TIME '15:12:34'", TIME_SECONDS, "TIME '15:12:34'")
                .addRoundTrip("TIME(0)", "TIME '23:59:59'", TIME_SECONDS, "TIME '23:59:59'");

        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        // Only use CTAS for this as inserting with Salesforce JDBC wants the time literal as a java.sql.Date.valueOf string which is actually a date and not a time
        tests.execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_time"));
    }

    @Test(dataProvider = "testTimestampDataProvider")
    public void testTimestamp(ZoneId sessionZone)
    {
        SqlDataTypeTest tests = SqlDataTypeTest.create();

        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        tests.addRoundTrip("TIMESTAMP(0)", "TIMESTAMP '1958-01-01 13:18:03'", TIMESTAMP_SECONDS, "CAST(TIMESTAMP '1958-01-01 13:18:03' AS TIMESTAMP(0))");
        tests.addRoundTrip("TIMESTAMP(0)", "TIMESTAMP '2019-03-18 01:17:00'", TIMESTAMP_SECONDS, "CAST(TIMESTAMP '2019-03-18 01:17:00' AS TIMESTAMP(0))");
        tests.addRoundTrip("TIMESTAMP(0)", "TIMESTAMP '2018-10-28 01:33:17'", TIMESTAMP_SECONDS, "CAST(TIMESTAMP '2018-10-28 01:33:17' AS TIMESTAMP(0))");
        tests.addRoundTrip("TIMESTAMP(0)", "TIMESTAMP '2018-10-28 03:33:33'", TIMESTAMP_SECONDS, "CAST(TIMESTAMP '2018-10-28 03:33:33' AS TIMESTAMP(0))");
        // tests.addRoundTrip("TIMESTAMP(0)", "TIMESTAMP '1970-01-01 00:00:00'", TIMESTAMP_SECONDS, "CAST(TIMESTAMP '1970-01-01 00:00:00' AS TIMESTAMP(0))"); // "TIMESTAMP '1970-01-01 00:00:00'" also is a gap in JVM zone
        // tests.addRoundTrip("TIMESTAMP(0)", "TIMESTAMP '1970-01-01 00:13:42'", TIMESTAMP_SECONDS, "CAST(TIMESTAMP '1970-01-01 00:13:42' AS TIMESTAMP(0))"); // expected [1970-01-01T00:13:42] but found [1970-01-01T01:13:42]
        // tests.addRoundTrip("TIMESTAMP(0)", "TIMESTAMP '2018-04-01 02:13:55'", TIMESTAMP_SECONDS, "CAST(TIMESTAMP '2018-04-01 02:13:55' AS TIMESTAMP(0))"); // [2018-04-01T02:13:55] but found [2018-04-01T03:13:55]
        tests.addRoundTrip("TIMESTAMP(0)", "TIMESTAMP '2018-03-25 03:17:17'", TIMESTAMP_SECONDS, "CAST(TIMESTAMP '2018-03-25 03:17:17' AS TIMESTAMP(0))");
        tests.addRoundTrip("TIMESTAMP(0)", "TIMESTAMP '1986-01-01 00:13:07'", TIMESTAMP_SECONDS, "CAST(TIMESTAMP '1986-01-01 00:13:07' AS TIMESTAMP(0))");

        // Only use CTAS for this as inserting with Salesforce JDBC wants the time literal as a java.sql.Date.valueOf string which is actually a date and not a timestamp
        tests.execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_timestamp"));
    }

    @DataProvider
    public Object[][] testTimestampDataProvider()
    {
        return new Object[][] {
                {UTC},
                {jvmZone},
                {vilnius},
                {kathmandu},
                {ZoneId.of(TestingSession.DEFAULT_TIME_ZONE_KEY.getId())},
        };
    }

    private DataSetup trinoCreateAsSelect(String tableNamePrefix)
    {
        return trinoCreateAsSelect(getSession(), tableNamePrefix);
    }

    private DataSetup trinoCreateAsSelect(Session session, String tableNamePrefix)
    {
        return new SalesforceCreateAsSelectDataSetup(new TrinoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    private DataSetup salesforceCreateAndInsert(String tableNamePrefix)
    {
        return new SalesforceCreateAndInsertDataSetup(new JdbcSqlExecutor(jdbcUrl), tableNamePrefix);
    }

    private static void checkIsGap(ZoneId zone, LocalDateTime dateTime)
    {
        verify(isGap(zone, dateTime), "Expected %s to be a gap in %s", dateTime, zone);
    }

    private static boolean isGap(ZoneId zone, LocalDateTime dateTime)
    {
        return zone.getRules().getValidOffsets(dateTime).isEmpty();
    }

    private static void checkIsDoubled(ZoneId zone, LocalDateTime dateTime)
    {
        verify(zone.getRules().getValidOffsets(dateTime).size() == 2, "Expected %s to be doubled in %s", dateTime, zone);
    }
}
