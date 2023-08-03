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
package io.trino.plugin.firebolt;

import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingSession;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.datatype.CreateAsSelectDataSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.datatype.SqlDataTypeTest;
import io.trino.testing.sql.JdbcSqlExecutor;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TrinoSqlExecutor;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.ZoneId;
import java.util.Properties;

import static io.trino.plugin.firebolt.FireboltTestProperties.JDBC_ENDPOINT;
import static io.trino.plugin.firebolt.FireboltTestProperties.JDBC_PASSWORD;
import static io.trino.plugin.firebolt.FireboltTestProperties.JDBC_USER;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;

public class TestFireboltTypes
        extends AbstractTestQueryFramework
{
    private static String testSchema = "public";

    private static final String TEST_DATABASE = "trino_test";
    static final String JDBC_URL = "jdbc:firebolt://" + JDBC_ENDPOINT + TEST_DATABASE;
    private static final ZoneId testZone = TestingSession.DEFAULT_TIME_ZONE_KEY.getZoneId();
    private final ZoneId jvmZone = ZoneId.systemDefault();

    // using two non-JVM zones so that we don't need to worry what the backend's system zone is

    // no DST in 1970, but has DST in later years (e.g. 2018)
    private final ZoneId vilnius = ZoneId.of("Europe/Vilnius");

    // Size of offset changed since 1970-01-01, no DST
    private final ZoneId kathmandu = ZoneId.of("Asia/Kathmandu");

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return FireboltQueryRunner.createQueryRunner();
    }

    private static SqlExecutor getFireboltExecutor()
    {
        Properties properties = new Properties();
        properties.setProperty("user", JDBC_USER);
        properties.setProperty("password", JDBC_PASSWORD);
        return new JdbcSqlExecutor(JDBC_URL, properties);
    }

    private static DataSetup fireboltCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(getFireboltExecutor(), testSchema + "." + tableNamePrefix);
    }

    private DataSetup trinoCreateAsSelect(String tableNamePrefix)
    {
        return trinoCreateAsSelect(getQueryRunner().getDefaultSession(), tableNamePrefix);
    }

    private DataSetup trinoCreateAsSelect(Session session, String tableNamePrefix)
    {
        return new CreateAsSelectDataSetup(new TrinoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    @Test
    public void testBasicTypes()
    {
        // Assume that if these types work at all, they have standard semantics.
        SqlDataTypeTest.create()
                .addRoundTrip("boolean", "true", BOOLEAN, "true")
                .addRoundTrip("boolean", "false", BOOLEAN, "false")
                .addRoundTrip("bigint", "123456789012", BIGINT, "123456789012")
                .addRoundTrip("integer", "1234567890", INTEGER, "1234567890")
                .addRoundTrip("smallint", "32456", INTEGER, "INTEGER '32456'")
                .addRoundTrip("double", "123.45", DOUBLE, "DOUBLE '123.45'")
                // issues with dobule to real comparison
//                .addRoundTrip("real", "3.14", REAL, "REAL '3.14'")
                .addRoundTrip("tinyint", "5", INTEGER, "INTEGER '5'")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_basic_types"));
    }

    @DataProvider(name = "datetime_test_parameters")
    public Object[][] dataProviderForDatetimeTests()
    {
        return new Object[][] {
                {UTC},
                {jvmZone},
                {vilnius},
                {kathmandu},
                {testZone},
        };
    }

    @Test(dataProvider = "datetime_test_parameters")
    public void testDate(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(getTimeZoneKey(sessionZone.getId()))
                .build();
        SqlDataTypeTest.create()
                // Date precision issues
//                .addRoundTrip("date", "DATE '0001-01-01'", DATE, "DATE '0001-01-01'") // first day of AD
//                .addRoundTrip("date", "DATE '1500-01-01'", DATE, "DATE '1500-01-01'") // sometime before julian->gregorian switch
                .addRoundTrip("date", "DATE '1600-01-01'", DATE, "DATE '1600-01-01'") // long ago but after julian->gregorian switch
                .addRoundTrip("date", "DATE '1952-04-03'", DATE, "DATE '1952-04-03'") // before epoch
                .addRoundTrip("date", "DATE '1970-01-01'", DATE, "DATE '1970-01-01'")
                .addRoundTrip("date", "DATE '1970-02-03'", DATE, "DATE '1970-02-03'") // after epoch
                .addRoundTrip("date", "DATE '2017-07-01'", DATE, "DATE '2017-07-01'") // summer in northern hemisphere (possible DST)
                .addRoundTrip("date", "DATE '2017-01-01'", DATE, "DATE '2017-01-01'") // winter in northern hemisphere (possible DST in southern hemisphere)
                .addRoundTrip("date", "DATE '1970-01-01'", DATE, "DATE '1970-01-01'") // day of midnight gap in JVM
                .addRoundTrip("date", "DATE '1983-04-01'", DATE, "DATE '1983-04-01'") // day of midnight gap in Vilnius
                .addRoundTrip("date", "DATE '1983-10-01'", DATE, "DATE '1983-10-01'") // day after midnight setback in Vilnius
                .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_date"))
                .execute(getQueryRunner(), session, fireboltCreateAndInsert("test_date"));
    }

    private static SqlDataTypeTest timestampTypeTests(String inputType)
    {
        return SqlDataTypeTest.create()
                // Date precision issues
//                .addRoundTrip(inputType, "TIMESTAMP '0001-01-01 00:00:00.000000'", createTimestampType(6), "TIMESTAMP '0001-01-01 00:00:00.000000'") // first day of AD
//                .addRoundTrip(inputType, "TIMESTAMP '1500-01-01 00:00:00.000000'", createTimestampType(6), "TIMESTAMP '1500-01-01 00:00:00.000000'") // sometime before julian->gregorian switch
                .addRoundTrip(inputType, "TIMESTAMP '1600-01-01 00:00:00.000000'", createTimestampType(6), "TIMESTAMP '1600-01-01 00:00:00.000000'") // long ago but after julian->gregorian switch
                .addRoundTrip(inputType, "TIMESTAMP '1958-01-01 13:18:03.123456'", createTimestampType(6), "TIMESTAMP '1958-01-01 13:18:03.123456'") // before epoch
                .addRoundTrip(inputType, "TIMESTAMP '2019-03-18 10:09:17.987654'", createTimestampType(6), "TIMESTAMP '2019-03-18 10:09:17.987654'") // after epoch
                .addRoundTrip(inputType, "TIMESTAMP '2018-10-28 01:33:17.456789'", createTimestampType(6), "TIMESTAMP '2018-10-28 01:33:17.456789'") // time doubled in JVM
                .addRoundTrip(inputType, "TIMESTAMP '2018-10-28 03:33:33.333333'", createTimestampType(6), "TIMESTAMP '2018-10-28 03:33:33.333333'") // time doubled in Vilnius
                .addRoundTrip(inputType, "TIMESTAMP '1970-01-01 00:00:00.000000'", createTimestampType(6), "TIMESTAMP '1970-01-01 00:00:00.000000'") // time gap in JVM
                .addRoundTrip(inputType, "TIMESTAMP '2018-03-25 03:17:17.000000'", createTimestampType(6), "TIMESTAMP '2018-03-25 03:17:17.000000'") // time gap in Vilnius
                .addRoundTrip(inputType, "TIMESTAMP '1986-01-01 00:13:07.000000'", createTimestampType(6), "TIMESTAMP '1986-01-01 00:13:07.000000'") // time gap in Kathmandu
                // Full time precision
                .addRoundTrip(inputType, "TIMESTAMP '1969-12-31 23:59:59.999999'", createTimestampType(6), "TIMESTAMP '1969-12-31 23:59:59.999999'")
                .addRoundTrip(inputType, "TIMESTAMP '1970-01-01 00:00:00.999999'", createTimestampType(6), "TIMESTAMP '1970-01-01 00:00:00.999999'");
    }

    @Test(dataProvider = "datetime_test_parameters")
    public void testTimestamp(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(getTimeZoneKey(sessionZone.getId()))
                .build();
        // Firebolt doesn't allow timestamp precision to be specified
        timestampTypeTests("timestamp(6)")
                .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "timestamp_from_trino"));
        timestampTypeTests("timestamp")
                .execute(getQueryRunner(), session, fireboltCreateAndInsert("timestamp_from_jdbc"));
    }

    @Test
    public void testVarchar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("varchar(65535)", "'varchar max'", createUnboundedVarcharType(), "CAST('varchar max' AS varchar)")
                .addRoundTrip("varchar(8)", "'кирилиця'", createUnboundedVarcharType(), "CAST('кирилиця' AS varchar)")
                .addRoundTrip("varchar(16)", "'😂'", createUnboundedVarcharType(), "CAST('😂' AS varchar)")
                .addRoundTrip("varchar(10)", "'text_a'", createUnboundedVarcharType(), "CAST('text_a' AS varchar)")
                .addRoundTrip("varchar(255)", "'text_b'", createUnboundedVarcharType(), "CAST('text_b' AS varchar)")
                .addRoundTrip("varchar(4096)", "'char max'", createUnboundedVarcharType(), "CAST('char max' AS varchar)")
                .execute(getQueryRunner(), trinoCreateAsSelect("trino_test_varchar"))
                .execute(getQueryRunner(), fireboltCreateAndInsert("jdbc_test_varchar"));
    }

    @Test
    public void testChar()
    {
        // No char in Firebolt, char is translated to varchar
        SqlDataTypeTest.create()
                .addRoundTrip("char(10)", "'text_a'", createUnboundedVarcharType(), "CAST('text_a' AS varchar)")
                .addRoundTrip("char(255)", "'text_b'", createUnboundedVarcharType(), "CAST('text_b' AS varchar)")
                .addRoundTrip("char(4096)", "'char max'", createUnboundedVarcharType(), "CAST('char max' AS varchar)")
                .execute(getQueryRunner(), trinoCreateAsSelect("trino_test_char"))
                .execute(getQueryRunner(), fireboltCreateAndInsert("jdbc_test_char"));
    }

    /**
     * Get the named system property, throwing an exception if it is not set.
     */
    private static String requireSystemProperty(String property)
    {
        return requireNonNull(System.getProperty(property), property + " is not set");
    }
}
