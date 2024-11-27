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
package io.trino.plugin.databend;

import io.trino.Session;
import io.trino.spi.type.TimeZoneKey;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingSession;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.datatype.CreateAsSelectDataSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.datatype.SqlDataTypeTest;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TrinoSqlExecutor;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.time.LocalDate;
import java.time.ZoneId;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.type.JsonType.JSON;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestDatabendTypeMapping
        extends AbstractTestQueryFramework
{
    private TestingDatabendServer databendServer;

    private final ZoneId jvmZone = ZoneId.systemDefault();
    // no DST in 1970, but has DST in later years (e.g. 2018)
    private final ZoneId vilnius = ZoneId.of("Europe/Vilnius");

    @BeforeAll
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
        databendServer = closeAfterClass(new TestingDatabendServer());
        return DatabendQueryRunner.builder(databendServer)
                .build();
    }

    @Test
    public void testBoolean()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("boolean", "true", BOOLEAN, "BOOLEAN 'true'")
                .addRoundTrip("boolean", "false", BOOLEAN, "BOOLEAN 'false'")
                .addRoundTrip("boolean", "NULL", BOOLEAN, "NULL")
                .execute(getQueryRunner(), databendCreateAndInsert("tpch.test_boolean"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_boolean"));

        SqlDataTypeTest.create()
                .addRoundTrip("boolean", "1", BOOLEAN, "TINYINT '1'")
                .addRoundTrip("boolean", "0", BOOLEAN, "TINYINT '0'")
                .addRoundTrip("boolean", "NULL", BOOLEAN, "NULL")
                .execute(getQueryRunner(), trinoCreateAndInsert("test_boolean"));
    }

    @Test
    public void testTinyint()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("tinyint", "NULL", TINYINT, "CAST(NULL AS TINYINT)")
                .addRoundTrip("tinyint", "-128", TINYINT, "TINYINT '-128'") // min value in Databend and Trino
                .addRoundTrip("tinyint", "5", TINYINT, "TINYINT '5'")
                .addRoundTrip("tinyint", "127", TINYINT, "TINYINT '127'") // max value in Databend and Trino
                .execute(getQueryRunner(), databendCreateAndInsert("tpch.test_tinyint"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_tinyint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_tinyint"));
    }

    @Test
    public void testUnsupportedTinyint()
    {
        try (TestTable table = new TestTable(databendServer::execute, "tpch.test_unsupported_tinyint", "(data tinyint)")) {
            assertDatabendQueryFails(
                    "INSERT INTO " + table.getName() + " VALUES (-129)", // min - 1
                    "Data truncation: Out of range value for column 'data' at row 1");
            assertDatabendQueryFails(
                    "INSERT INTO " + table.getName() + " VALUES (128)", // max + 1
                    "Data truncation: Out of range value for column 'data' at row 1");
        }
    }

    @Test
    public void testSmallint()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("smallint", "NULL", SMALLINT, "CAST(NULL AS SMALLINT)")
                .addRoundTrip("smallint", "-32768", SMALLINT, "SMALLINT '-32768'") // min value in Databend and Trino
                .addRoundTrip("smallint", "32456", SMALLINT, "SMALLINT '32456'")
                .addRoundTrip("smallint", "32767", SMALLINT, "SMALLINT '32767'") // max value in Databend and Trino
                .execute(getQueryRunner(), databendCreateAndInsert("tpch.test_smallint"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_smallint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_smallint"));
    }

    @Test
    public void testUnsupportedSmallint()
    {
        try (TestTable table = new TestTable(databendServer::execute, "tpch.test_unsupported_smallint", "(data smallint)")) {
            assertDatabendQueryFails(
                    "INSERT INTO " + table.getName() + " VALUES (-32769)", // min - 1
                    "Data truncation: Out of range value for column 'data' at row 1");
            assertDatabendQueryFails(
                    format("INSERT INTO %s VALUES (32768)", table.getName()), // max + 1
                    "Data truncation: Out of range value for column 'data' at row 1");
        }
    }

    @Test
    public void testInteger()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("integer", "NULL", INTEGER, "CAST(NULL AS INTEGER)")
                .addRoundTrip("integer", "-2147483648", INTEGER, "-2147483648") // min value in Databend and Trino
                .addRoundTrip("integer", "1234567890", INTEGER, "1234567890")
                .addRoundTrip("integer", "2147483647", INTEGER, "2147483647") // max value in Databend and Trino
                .execute(getQueryRunner(), databendCreateAndInsert("tpch.test_int"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_int"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_int"));
    }

    @Test
    public void testUnsupportedInteger()
    {
        try (TestTable table = new TestTable(databendServer::execute, "tpch.test_unsupported_integer", "(data integer)")) {
            assertDatabendQueryFails(
                    "INSERT INTO " + table.getName() + " VALUES (-2147483649)", // min - 1
                    "Data truncation: Out of range value for column 'data' at row 1");
            assertDatabendQueryFails(
                    "INSERT INTO " + table.getName() + " VALUES (2147483648)", // max + 1
                    "Data truncation: Out of range value for column 'data' at row 1");
        }
    }

    @Test
    public void testBigint()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("bigint", "NULL", BIGINT, "CAST(NULL AS BIGINT)")
                .addRoundTrip("bigint", "-9223372036854775808", BIGINT, "-9223372036854775808") // min value in Databend and Trino
                .addRoundTrip("bigint", "123456789012", BIGINT, "123456789012")
                .addRoundTrip("bigint", "9223372036854775807", BIGINT, "9223372036854775807") // max value in Databend and Trino
                .execute(getQueryRunner(), databendCreateAndInsert("tpch.test_bigint"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_bigint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_bigint"));
    }

    @Test
    public void testUnsupportedBigint()
    {
        try (TestTable table = new TestTable(databendServer::execute, "tpch.test_unsupported_bigint", "(data bigint)")) {
            assertDatabendQueryFails(
                    "INSERT INTO " + table.getName() + " VALUES (-9223372036854775809)", // min - 1
                    "Data truncation: Out of range value for column 'data' at row 1");
            assertDatabendQueryFails(
                    "INSERT INTO " + table.getName() + " VALUES (9223372036854775808)", // max + 1
                    "Data truncation: Out of range value for column 'data' at row 1");
        }
    }

    @Test
    public void testTrinoCreatedParameterizedVarchar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("varchar(10)", "'text_a'", createVarcharType(255), "CAST('text_a' AS varchar(255))")
                .addRoundTrip("varchar(255)", "'text_b'", createVarcharType(255), "CAST('text_b' AS varchar(255))")
                .addRoundTrip("varchar(256)", "'text_c'", createVarcharType(65535), "CAST('text_c' AS varchar(65535))")
                .addRoundTrip("varchar(65535)", "'text_d'", createVarcharType(65535), "CAST('text_d' AS varchar(65535))")
                .addRoundTrip("varchar(65536)", "'text_e'", createVarcharType(16777215), "CAST('text_e' AS varchar(16777215))")
                .addRoundTrip("varchar(16777215)", "'text_f'", createVarcharType(16777215), "CAST('text_f' AS varchar(16777215))")
                .addRoundTrip("varchar(16777216)", "'text_g'", createUnboundedVarcharType(), "CAST('text_g' AS varchar)")
                .addRoundTrip("varchar(2147483646)", "'text_h'", createUnboundedVarcharType(), "CAST('text_h' AS varchar)")
                .addRoundTrip("varchar", "'unbounded'", createUnboundedVarcharType(), "CAST('unbounded' AS varchar)")
                .addRoundTrip("varchar(10)", "NULL", createVarcharType(255), "CAST(NULL AS varchar(255))")
                .addRoundTrip("varchar", "NULL", createUnboundedVarcharType(), "CAST(NULL AS varchar)")
                .execute(getQueryRunner(), trinoCreateAsSelect("trino_test_parameterized_varchar"))
                .execute(getQueryRunner(), trinoCreateAndInsert("trino_test_parameterized_varchar"));
    }

    @Test
    public void testDecimal()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("decimal(3, 0)", "CAST('193' AS decimal(3, 0))", createDecimalType(3, 0), "CAST('193' AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 0)", "CAST('19' AS decimal(3, 0))", createDecimalType(3, 0), "CAST('19' AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 0)", "CAST('-193' AS decimal(3, 0))", createDecimalType(3, 0), "CAST('-193' AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 1)", "CAST('10.0' AS decimal(3, 1))", createDecimalType(3, 1), "CAST('10.0' AS decimal(3, 1))")
                .addRoundTrip("decimal(3, 1)", "CAST('10.1' AS decimal(3, 1))", createDecimalType(3, 1), "CAST('10.1' AS decimal(3, 1))")
                .addRoundTrip("decimal(3, 1)", "CAST('-10.1' AS decimal(3, 1))", createDecimalType(3, 1), "CAST('-10.1' AS decimal(3, 1))")
                .addRoundTrip("decimal(4, 2)", "CAST('2' AS decimal(4, 2))", createDecimalType(4, 2), "CAST('2' AS decimal(4, 2))")
                .addRoundTrip("decimal(4, 2)", "CAST('2.3' AS decimal(4, 2))", createDecimalType(4, 2), "CAST('2.3' AS decimal(4, 2))")
                .addRoundTrip("decimal(24, 2)", "CAST('2' AS decimal(24, 2))", createDecimalType(24, 2), "CAST('2' AS decimal(24, 2))")
                .addRoundTrip("decimal(24, 2)", "CAST('2.3' AS decimal(24, 2))", createDecimalType(24, 2), "CAST('2.3' AS decimal(24, 2))")
                .addRoundTrip("decimal(24, 2)", "CAST('123456789.3' AS decimal(24, 2))", createDecimalType(24, 2), "CAST('123456789.3' AS decimal(24, 2))")
                .addRoundTrip("decimal(24, 4)", "CAST('12345678901234567890.31' AS decimal(24, 4))", createDecimalType(24, 4), "CAST('12345678901234567890.31' AS decimal(24, 4))")
                .addRoundTrip("decimal(30, 5)", "CAST('3141592653589793238462643.38327' AS decimal(30, 5))", createDecimalType(30, 5), "CAST('3141592653589793238462643.38327' AS decimal(30, 5))")
                .addRoundTrip("decimal(30, 5)", "CAST('-3141592653589793238462643.38327' AS decimal(30, 5))", createDecimalType(30, 5), "CAST('-3141592653589793238462643.38327' AS decimal(30, 5))")
                .addRoundTrip("decimal(38, 0)", "CAST('27182818284590452353602874713526624977' AS decimal(38, 0))", createDecimalType(38, 0), "CAST('27182818284590452353602874713526624977' AS decimal(38, 0))")
                .addRoundTrip("decimal(38, 0)", "CAST('-27182818284590452353602874713526624977' AS decimal(38, 0))", createDecimalType(38, 0), "CAST('-27182818284590452353602874713526624977' AS decimal(38, 0))")
                .addRoundTrip("decimal(3, 0)", "CAST(NULL AS decimal(3, 0))", createDecimalType(3, 0), "CAST(NULL AS decimal(3, 0))")
                .addRoundTrip("decimal(38, 0)", "CAST(NULL AS decimal(38, 0))", createDecimalType(38, 0), "CAST(NULL AS decimal(38, 0))")
                .execute(getQueryRunner(), databendCreateAndInsert("tpch.test_decimal"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_decimal"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_decimal"));
    }

    @Test
    public void testDecimalExceedingPrecisionMax()
    {
        testUnsupportedDataType("decimal(50,0)");
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
                .execute(getQueryRunner(), session, databendCreateAndInsert("tpch.test_date"))
                .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_date"))
                .execute(getQueryRunner(), session, trinoCreateAsSelect("test_date"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_date"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert("test_date"));
    }

    @Test
    public void testTimeFromDatabend()
    {
        testTimeFromDatabend(UTC);
        testTimeFromDatabend(ZoneId.systemDefault());
        // no DST in 1970, but has DST in later years (e.g. 2018)
        testTimeFromDatabend(ZoneId.of("Europe/Vilnius"));
        // minutes offset change since 1970-01-01, no DST
        testTimeFromDatabend(ZoneId.of("Asia/Kathmandu"));
        testTimeFromDatabend(TestingSession.DEFAULT_TIME_ZONE_KEY.getZoneId());
    }

    private void testTimeFromDatabend(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        SqlDataTypeTest.create()
                // default precision in Databend is 0
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
                .execute(getQueryRunner(), session, databendCreateAndInsert("tpch.test_time"));
    }

    @Test
    public void testDatabendDatetimeType()
    {
        testDatabendDatetimeType(UTC);
        testDatabendDatetimeType(ZoneId.systemDefault());
        // no DST in 1970, but has DST in later years (e.g. 2018)
        testDatabendDatetimeType(ZoneId.of("Europe/Vilnius"));
        // minutes offset change since 1970-01-01, no DST
        testDatabendDatetimeType(ZoneId.of("Asia/Kathmandu"));
        testDatabendDatetimeType(TestingSession.DEFAULT_TIME_ZONE_KEY.getZoneId());
    }

    private void testDatabendDatetimeType(ZoneId sessionZone)
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

                .execute(getQueryRunner(), session, databendCreateAndInsert("tpch.test_datetime"));
    }

    @Test
    public void testTimestampFromDatabend()
    {
        testTimestampFromDatabend(UTC);
        testTimestampFromDatabend(ZoneId.systemDefault());
        // no DST in 1970, but has DST in later years (e.g. 2018)
        testTimestampFromDatabend(ZoneId.of("Europe/Vilnius"));
        // minutes offset change since 1970-01-01, no DST
        testTimestampFromDatabend(ZoneId.of("Asia/Kathmandu"));
        testTimestampFromDatabend(TestingSession.DEFAULT_TIME_ZONE_KEY.getZoneId());
    }

    private void testTimestampFromDatabend(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        SqlDataTypeTest.create()
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2019-03-18 10:01:17.987'", createTimestampWithTimeZoneType(3), "TIMESTAMP '2019-03-18 10:01:17.987 UTC'")
                // time doubled in JVM zone
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2018-10-28 01:33:17.456'", createTimestampWithTimeZoneType(3), "TIMESTAMP '2018-10-28 01:33:17.456 UTC'")
                // time double in Vilnius
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2018-10-28 03:33:33.333'", createTimestampWithTimeZoneType(3), "TIMESTAMP '2018-10-28 03:33:33.333 UTC'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1970-01-01 00:13:42.000'", createTimestampWithTimeZoneType(3), "TIMESTAMP '1970-01-01 00:13:42.000 UTC'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2018-04-01 02:13:55.123'", createTimestampWithTimeZoneType(3), "TIMESTAMP '2018-04-01 02:13:55.123 UTC'")
                // time gap in Vilnius
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2018-03-25 03:17:17.000'", createTimestampWithTimeZoneType(3), "TIMESTAMP '2018-03-25 03:17:17.000 UTC'")
                // time gap in Kathmandu
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1986-01-01 00:13:07.000'", createTimestampWithTimeZoneType(3), "TIMESTAMP '1986-01-01 00:13:07.000 UTC'")

                // same as above but with higher precision
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2019-03-18 10:01:17.987654'", createTimestampWithTimeZoneType(6), "TIMESTAMP '2019-03-18 10:01:17.987654 UTC'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2018-10-28 01:33:17.456789'", createTimestampWithTimeZoneType(6), "TIMESTAMP '2018-10-28 01:33:17.456789 UTC'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2018-10-28 03:33:33.333333'", createTimestampWithTimeZoneType(6), "TIMESTAMP '2018-10-28 03:33:33.333333 UTC'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1970-01-01 00:13:42.000000'", createTimestampWithTimeZoneType(6), "TIMESTAMP '1970-01-01 00:13:42.000000 UTC'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2018-04-01 02:13:55.123456'", createTimestampWithTimeZoneType(6), "TIMESTAMP '2018-04-01 02:13:55.123456 UTC'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2018-03-25 03:17:17.000000'", createTimestampWithTimeZoneType(6), "TIMESTAMP '2018-03-25 03:17:17.000000 UTC'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1986-01-01 00:13:07.000000'", createTimestampWithTimeZoneType(6), "TIMESTAMP '1986-01-01 00:13:07.000000 UTC'")

                // test arbitrary time for all supported precisions
                .addRoundTrip("timestamp(0)", "TIMESTAMP '1970-01-01 00:00:01'", createTimestampWithTimeZoneType(0), "TIMESTAMP '1970-01-01 00:00:01 UTC'")
                .addRoundTrip("timestamp(1)", "TIMESTAMP '1970-01-01 00:00:01.1'", createTimestampWithTimeZoneType(1), "TIMESTAMP '1970-01-01 00:00:01.1 UTC'")
                .addRoundTrip("timestamp(1)", "TIMESTAMP '1970-01-01 00:00:01.9'", createTimestampWithTimeZoneType(1), "TIMESTAMP '1970-01-01 00:00:01.9 UTC'")
                .addRoundTrip("timestamp(2)", "TIMESTAMP '1970-01-01 00:00:01.12'", createTimestampWithTimeZoneType(2), "TIMESTAMP '1970-01-01 00:00:01.12 UTC'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1970-01-01 00:00:01.123'", createTimestampWithTimeZoneType(3), "TIMESTAMP '1970-01-01 00:00:01.123 UTC'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1970-01-01 00:00:01.999'", createTimestampWithTimeZoneType(3), "TIMESTAMP '1970-01-01 00:00:01.999 UTC'")
                .addRoundTrip("timestamp(4)", "TIMESTAMP '1970-01-01 00:00:01.1234'", createTimestampWithTimeZoneType(4), "TIMESTAMP '1970-01-01 00:00:01.1234 UTC'")
                .addRoundTrip("timestamp(5)", "TIMESTAMP '1970-01-01 00:00:01.12345'", createTimestampWithTimeZoneType(5), "TIMESTAMP '1970-01-01 00:00:01.12345 UTC'")
                .addRoundTrip("timestamp(1)", "TIMESTAMP '2020-09-27 12:34:56.1'", createTimestampWithTimeZoneType(1), "TIMESTAMP '2020-09-27 12:34:56.1 UTC'")
                .addRoundTrip("timestamp(1)", "TIMESTAMP '2020-09-27 12:34:56.9'", createTimestampWithTimeZoneType(1), "TIMESTAMP '2020-09-27 12:34:56.9 UTC'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2020-09-27 12:34:56.123'", createTimestampWithTimeZoneType(3), "TIMESTAMP '2020-09-27 12:34:56.123 UTC'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2020-09-27 12:34:56.999'", createTimestampWithTimeZoneType(3), "TIMESTAMP '2020-09-27 12:34:56.999 UTC'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2020-09-27 12:34:56.123456'", createTimestampWithTimeZoneType(6), "TIMESTAMP '2020-09-27 12:34:56.123456 UTC'")

                .execute(getQueryRunner(), session, databendCreateAndInsert("tpch.test_timestamp"));
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

    @Test
    public void testTimestampWithTimeZoneFromTrinoUtc()
    {
        ZoneId sessionZone = UTC;
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        SqlDataTypeTest.create()
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
        try (TestTable table = new TestTable(databendServer::execute, "tpch.test_unsupported_timestamp", "(data TIMESTAMP)")) {
            // Verify Databend writes
            assertDatabendQueryFails(
                    "INSERT INTO " + table.getName() + " VALUES ('1970-01-01 00:00:00')",
                    "Data truncation: Incorrect datetime value: '1970-01-01 00:00:00' for column 'data' at row 1");
            assertDatabendQueryFails(
                    "INSERT INTO " + table.getName() + " VALUES ('2038-01-19 03:14:08')",
                    "Data truncation: Incorrect datetime value: '2038-01-19 03:14:08' for column 'data' at row 1");

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

                // CTAS with Trino, where the coercion is done by the connector
                .execute(getQueryRunner(), trinoCreateAsSelect("test_timestamp_with_time_zone_coercion"))
                // INSERT with Trino, where the coercion is done by the engine
                .execute(getQueryRunner(), trinoCreateAndInsert("test_timestamp_with_time_zone_coercion"));
    }

    @Test
    public void testJson()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("json", "JSON '{}'", JSON, "JSON '{}'")
                .addRoundTrip("json", "NULL", JSON, "CAST(NULL AS JSON)")
                .addRoundTrip("json", "JSON 'null'", JSON, "JSON 'null'")
                .addRoundTrip("json", "JSON '123.4'", JSON, "JSON '123.4'")
                .addRoundTrip("json", "JSON '\"abc\"'", JSON, "JSON '\"abc\"'")
                .addRoundTrip("json", "JSON '\"text with '' apostrophes\"'", JSON, "JSON '\"text with '' apostrophes\"'")
                .addRoundTrip("json", "JSON '\"\"'", JSON, "JSON '\"\"'")
                .addRoundTrip("json", "JSON '{\"a\":1,\"b\":2}'", JSON, "JSON '{\"a\":1,\"b\":2}'")
                .addRoundTrip("json", "JSON '{\"a\":[1,2,3],\"b\":{\"aa\":11,\"bb\":[{\"a\":1,\"b\":2},{\"a\":0}]}}'", JSON, "JSON '{\"a\":[1,2,3],\"b\":{\"aa\":11,\"bb\":[{\"a\":1,\"b\":2},{\"a\":0}]}}'")
                .addRoundTrip("json", "JSON '[]'", JSON, "JSON '[]'")
                .execute(getQueryRunner(), trinoCreateAsSelect("trino_test_json"));

        SqlDataTypeTest.create()
                .addRoundTrip("json", "CAST('{}' AS JSON)", JSON, "JSON '{}'")
                .addRoundTrip("json", "NULL", JSON, "CAST(NULL AS JSON)")
                .addRoundTrip("json", "CAST('null' AS JSON)", JSON, "JSON 'null'")
                .addRoundTrip("json", "CAST('123.4' AS JSON)", JSON, "JSON '123.4'")
                .addRoundTrip("json", "CAST('\"abc\"' AS JSON)", JSON, "JSON '\"abc\"'")
                .addRoundTrip("json", "CAST('\"text with '' apostrophes\"' AS JSON)", JSON, "JSON '\"text with '' apostrophes\"'")
                .addRoundTrip("json", "CAST('\"\"' AS JSON)", JSON, "JSON '\"\"'")
                .addRoundTrip("json", "CAST('{\"a\":1,\"b\":2}' AS JSON)", JSON, "JSON '{\"a\":1,\"b\":2}'")
                .addRoundTrip("json", "CAST('{\"a\":[1,2,3],\"b\":{\"aa\":11,\"bb\":[{\"a\":1,\"b\":2},{\"a\":0}]}}' AS JSON)", JSON, "JSON '{\"a\":[1,2,3],\"b\":{\"aa\":11,\"bb\":[{\"a\":1,\"b\":2},{\"a\":0}]}}'")
                .addRoundTrip("json", "CAST('[]' AS JSON)", JSON, "JSON '[]'")
                .execute(getQueryRunner(), databendCreateAndInsert("tpch.databend_test_json"));
    }

    @Test
    public void testFloat()
    {
        // we are not testing Nan/-Infinity/+Infinity as those are not supported by Databend
        SqlDataTypeTest.create()
                .addRoundTrip("real", "3.14", REAL, "REAL '3.14'")
                .addRoundTrip("real", "10.3e0", REAL, "REAL '10.3e0'")
                .addRoundTrip("real", "NULL", REAL, "CAST(NULL AS REAL)")
                .execute(getQueryRunner(), trinoCreateAsSelect("trino_test_float"))
                .execute(getQueryRunner(), trinoCreateAndInsert("trino_test_float"));

        SqlDataTypeTest.create()
                .addRoundTrip("float", "3.14", REAL, "REAL '3.14'")
                .addRoundTrip("float", "10.3e0", REAL, "REAL '10.3e0'")
                .addRoundTrip("float", "NULL", REAL, "CAST(NULL AS REAL)")
                .execute(getQueryRunner(), databendCreateAndInsert("tpch.databend_test_float"));
    }

    @Test
    public void testDouble()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("double", "1.0E100", DOUBLE, "1.0E100")
                .addRoundTrip("double", "1.23456E12", DOUBLE, "1.23456E12")
                .addRoundTrip("double", "NULL", DOUBLE, "CAST(NULL AS DOUBLE)")
                .execute(getQueryRunner(), trinoCreateAsSelect("trino_test_double"))
                .execute(getQueryRunner(), trinoCreateAndInsert("trino_test_double"))
                .execute(getQueryRunner(), databendCreateAndInsert("tpch.databend_test_double"));
    }

    @Test
    public void testUnsignedTypes()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("TINYINT UNSIGNED", "255", SMALLINT, "SMALLINT '255'")
                .addRoundTrip("SMALLINT UNSIGNED", "65535", INTEGER, "65535")
                .addRoundTrip("INT UNSIGNED", "4294967295", BIGINT, "4294967295")
                .addRoundTrip("INTEGER UNSIGNED", "4294967295", BIGINT, "4294967295")
                .addRoundTrip("BIGINT UNSIGNED", "18446744073709551615", createDecimalType(20, 0), "DECIMAL '18446744073709551615'")
                .execute(getQueryRunner(), databendCreateAndInsert("tpch.databend_test_unsigned"));
    }

    private void testUnsupportedDataType(String databaseDataType)
    {
        SqlExecutor jdbcSqlExecutor = databendServer::execute;
        jdbcSqlExecutor.execute(format("CREATE TABLE tpch.test_unsupported_data_type(supported_column varchar(5), unsupported_column %s)", databaseDataType));
        try {
            assertQuery(
                    "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'tpch' AND TABLE_NAME = 'test_unsupported_data_type'",
                    "VALUES 'supported_column'"); // no 'unsupported_column'
        }
        finally {
            jdbcSqlExecutor.execute("DROP TABLE tpch.test_unsupported_data_type");
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

    private DataSetup databendCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(databendServer::execute, tableNamePrefix);
    }

    private void assertDatabendQueryFails(@Language("SQL") String sql, String expectedMessage)
    {
        assertThatThrownBy(() -> databendServer.execute(sql))
                .cause()
                .hasMessageContaining(expectedMessage);
    }
}
