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
import com.google.common.io.Resources;
import com.starburstdata.presto.plugin.salesforce.testing.datatype.SalesforceCreateAndInsertDataSetup;
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
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.starburstdata.presto.plugin.salesforce.SalesforceConfig.SalesforceAuthenticationType.OAUTH_JWT;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static java.time.format.DateTimeFormatter.ISO_DATE;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// Note that there are no CTAS-based type mapping tests in this class due to Salesforce custom object limits
// We instead use static test names with no random suffix to avoid creating and deleting many tables and hitting this limit, which caused builds to fail
// These tables are created if they don't exist and truncated when the test is closed as well as initially to ensure there is no data
// Custom objects can be deleted by hand in Salesforce using the Object Manager
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
        SalesforceOAuthJwtConfig oAuthConfig = new SalesforceOAuthJwtConfig()
                .setPkcs12CertificateSubject("*")
                .setPkcs12Path(Resources.getResource("salesforce-ca.p12").getPath())
                .setPkcs12Password(requireNonNull(System.getProperty("salesforce.test.user1.pkcs12.password"), "salesforce.test.user1.pkcs12.password is not set"))
                .setJwtIssuer("3MVG9OI03ecbG2Vr3NBmmhtNrcBp3Ywy2y0XHbRN_uGz_zYWqKozppyAOX27EWcrOH5HAib9Cd2i8E8g.rYD.")
                .setJwtSubject(requireNonNull(System.getProperty("salesforce.test.user1.jwt.subject"), "salesforce.test.user1.jwt.subject is not set"));

        SalesforceConfig config = new SalesforceConfig()
                .setAuthenticationType(OAUTH_JWT)
                .setSandboxEnabled(true);

        // Create a SalesforceConfig to get the JDBC URL that we can forward to the testing classes
        // to reset the metadata cache between runs
        // Note that if the JDBC connection properties are different than what is used by the
        // QueryRunner than we may have issues with the metadata cache
        this.jdbcUrl = new SalesforceModule.OAuthJwtConnectionUrlProvider(config, oAuthConfig).get();
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
                .execute(getQueryRunner(), salesforceCreateAndInsert("test_double"));
    }

    @Test
    public void testSalesforceCreatedParameterizedVarchar()
    {
        // Maximum varchar length is 255 and you must specify a length
        SqlDataTypeTest.create()
                .addRoundTrip("VARCHAR(10)", "'text_a'", VarcharType.createVarcharType(10), "CAST('text_a' AS VARCHAR(10))")
                .addRoundTrip("VARCHAR(255)", "'text_b'", VarcharType.createVarcharType(255), "CAST('text_b' AS VARCHAR(255))")
                .execute(getQueryRunner(), salesforceCreateAndInsert("test_param_varchar"));
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
            testCases.execute(getQueryRunner(), session, salesforceCreateAndInsert("test_date"));
        }

        // The min and max value in Salesforce
        SqlDataTypeTest.create()
                .addRoundTrip("DATE", "DATE '1700-01-01'", DateType.DATE, "DATE '1700-01-01'")
                .addRoundTrip("DATE", "DATE '4000-12-31'", DateType.DATE, "DATE '4000-12-31'")
                .execute(getQueryRunner(), salesforceCreateAndInsert("test_date"));

        // Verify the failure when the value is min - 1d or max + 1d
        assertThatThrownBy(() ->
                SqlDataTypeTest.create()
                        .addRoundTrip("DATE", "DATE '1699-12-31'", DateType.DATE, "DATE '1699-12-31'")
                        .execute(getQueryRunner(), salesforceCreateAndInsert("test_date")))
                .hasMessageContaining("INSERT INTO")
                .hasStackTraceContaining("invalid date");
        assertThatThrownBy(() ->
                SqlDataTypeTest.create()
                        .addRoundTrip("DATE", "DATE '4001-01-01'", DateType.DATE, "DATE '4001-01-01'")
                        .execute(getQueryRunner(), salesforceCreateAndInsert("test_date")))
                .hasMessageContaining("INSERT INTO")
                .hasStackTraceContaining("invalid date");
    }

    @Test(dataProvider = "testTimestampDataProvider")
    public void testTime(ZoneId sessionZone)
    {
        // TIME data types inserted via the Salesforce JDBC driver do not support the test time zone
        // When read back the times are +1 hour
        throw new SkipException("TIME data type tests are skipped");
    }

    @Test(dataProvider = "testTimestampDataProvider")
    public void testTimestamp(ZoneId sessionZone)
    {
        SqlDataTypeTest tests = SqlDataTypeTest.create();

        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        tests.addRoundTrip("TIMESTAMP", "TIMESTAMP '1958-01-01 13:18:03'", TIMESTAMP_SECONDS, "CAST(TIMESTAMP '1958-01-01 13:18:03' AS TIMESTAMP(0))");
        tests.addRoundTrip("TIMESTAMP", "TIMESTAMP '2019-03-18 01:17:00'", TIMESTAMP_SECONDS, "CAST(TIMESTAMP '2019-03-18 01:17:00' AS TIMESTAMP(0))");
        tests.addRoundTrip("TIMESTAMP", "TIMESTAMP '2018-10-28 01:33:17'", TIMESTAMP_SECONDS, "CAST(TIMESTAMP '2018-10-28 01:33:17' AS TIMESTAMP(0))");
        tests.addRoundTrip("TIMESTAMP", "TIMESTAMP '2018-10-28 03:33:33'", TIMESTAMP_SECONDS, "CAST(TIMESTAMP '2018-10-28 03:33:33' AS TIMESTAMP(0))");
        // tests.addRoundTrip("TIMESTAMP", "TIMESTAMP '1970-01-01 00:00:00'", TIMESTAMP_SECONDS, "CAST(TIMESTAMP '1970-01-01 00:00:00' AS TIMESTAMP(0))"); // "TIMESTAMP '1970-01-01 00:00:00'" also is a gap in JVM zone
        // tests.addRoundTrip("TIMESTAMP", "TIMESTAMP '1970-01-01 00:13:42'", TIMESTAMP_SECONDS, "CAST(TIMESTAMP '1970-01-01 00:13:42' AS TIMESTAMP(0))"); // expected [1970-01-01T00:13:42] but found [1970-01-01T01:13:42]
        // tests.addRoundTrip("TIMESTAMP", "TIMESTAMP '2018-04-01 02:13:55'", TIMESTAMP_SECONDS, "CAST(TIMESTAMP '2018-04-01 02:13:55' AS TIMESTAMP(0))"); // [2018-04-01T02:13:55] but found [2018-04-01T03:13:55]
        tests.addRoundTrip("TIMESTAMP", "TIMESTAMP '2018-03-25 03:17:17'", TIMESTAMP_SECONDS, "CAST(TIMESTAMP '2018-03-25 03:17:17' AS TIMESTAMP(0))");
        tests.addRoundTrip("TIMESTAMP", "TIMESTAMP '1986-01-01 00:13:07'", TIMESTAMP_SECONDS, "CAST(TIMESTAMP '1986-01-01 00:13:07' AS TIMESTAMP(0))");

        tests.execute(getQueryRunner(), session, salesforceCreateAndInsert("test_timestamp"));
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

    private DataSetup salesforceCreateAndInsert(String tableName)
    {
        return new SalesforceCreateAndInsertDataSetup(new JdbcSqlExecutor(jdbcUrl), jdbcUrl, tableName);
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
