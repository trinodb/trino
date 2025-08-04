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
package io.trino.plugin.exasol;

import io.trino.Session;
import io.trino.spi.type.TimeZoneKey;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingSession;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.datatype.SqlDataTypeTest;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.exasol.TestingExasolServer.TEST_SCHEMA;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Isolated
final class TestExasolTypeMapping
        extends AbstractTestQueryFramework
{
    private TestingExasolServer exasolServer;

    private static final ZoneId jvmZone = ZoneId.systemDefault();
    private static final LocalDateTime timeGapInJvmZone1 = LocalDateTime.of(1932, 4, 1, 0, 13, 42);
    private static final LocalDateTime timeGapInJvmZone2 = LocalDateTime.of(2018, 4, 1, 2, 13, 55, 123_000_000);
    private static final LocalDateTime timeDoubledInJvmZone = LocalDateTime.of(2018, 10, 28, 1, 33, 17, 456_000_000);

    // no DST in 1970, but has DST in later years (e.g. 2018)
    private static final ZoneId vilnius = ZoneId.of("Europe/Vilnius");
    private static final LocalDateTime timeGapInVilnius = LocalDateTime.of(2018, 3, 25, 3, 17, 17);
    private static final LocalDateTime timeDoubledInVilnius = LocalDateTime.of(2018, 10, 28, 3, 33, 33, 333_000_000);

    // minutes offset change since 1970-01-01, no DST
    private static final ZoneId kathmandu = ZoneId.of("Asia/Kathmandu");
    private static final LocalDateTime timeGapInKathmandu = LocalDateTime.of(1986, 1, 1, 0, 13, 7);

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        exasolServer = closeAfterClass(new TestingExasolServer());
        return ExasolQueryRunner.builder(exasolServer).build();
    }

    @BeforeAll
    void setUp()
    {
        checkState(jvmZone.getId().equals("America/Bahia_Banderas"), "This test assumes certain JVM time zone");
        checkIsGap(jvmZone, timeGapInJvmZone1);
        checkIsGap(jvmZone, timeGapInJvmZone2);
        checkIsDoubled(jvmZone, timeDoubledInJvmZone);

        LocalDate dateOfLocalTimeChangeForwardAtMidnightInSomeZone = LocalDate.of(1983, 4, 1);
        checkIsGap(vilnius, dateOfLocalTimeChangeForwardAtMidnightInSomeZone.atStartOfDay());
        LocalDate dateOfLocalTimeChangeBackwardAtMidnightInSomeZone = LocalDate.of(1983, 10, 1);
        checkIsDoubled(vilnius, dateOfLocalTimeChangeBackwardAtMidnightInSomeZone.atStartOfDay().minusMinutes(1));
        checkIsGap(vilnius, timeGapInVilnius);
        checkIsDoubled(vilnius, timeDoubledInVilnius);

        checkIsGap(kathmandu, timeGapInKathmandu);
    }

    @Test
    void testBoolean()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("boolean", "true", BOOLEAN)
                .addRoundTrip("boolean", "false", BOOLEAN)
                .addRoundTrip("boolean", "NULL", BOOLEAN, "CAST(NULL AS BOOLEAN)")
                .addRoundTrip("boolean", "UNKNOWN", BOOLEAN, "CAST(NULL AS BOOLEAN)")
                .execute(getQueryRunner(), exasolCreateAndInsert(TEST_SCHEMA + "." + "test_boolean"));
    }

    @Test
    void testIntegerMappings()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("tinyint", "0", createDecimalType(3, 0), "CAST(0 AS DECIMAL(3, 0))")
                .addRoundTrip("smallint", "0", createDecimalType(9, 0), "CAST(0 AS DECIMAL(9, 0))")
                .addRoundTrip("integer", "0", createDecimalType(18, 0), "CAST(0 AS DECIMAL(18, 0))")
                .addRoundTrip("int", "0", createDecimalType(18, 0), "CAST(0 AS DECIMAL(18, 0))")
                .addRoundTrip("numeric", "0", createDecimalType(18, 0), "CAST(0 AS DECIMAL(18, 0))")
                .addRoundTrip("dec", "0", createDecimalType(18, 0), "CAST(0 AS DECIMAL(18, 0))")
                .addRoundTrip("bigint", "0", createDecimalType(36, 0), "CAST(0 AS DECIMAL(36, 0))")
                .execute(getQueryRunner(), exasolCreateAndInsert(TEST_SCHEMA + "." + "integers"));
    }

    @Test
    void testNumberMappingsNativeQuery()
    {
        testNumberMappingsNativeQuery("TINYINT", "DECIMAL(3,0)");
        testNumberMappingsNativeQuery("SMALLINT", "DECIMAL(9,0)");
        testNumberMappingsNativeQuery("SHORTINT", "DECIMAL(9,0)");
        testNumberMappingsNativeQuery("INTEGER", "DECIMAL(16,0)");
        testNumberMappingsNativeQuery("BIGINT", "DECIMAL(36,0)");
        testNumberMappingsNativeQuery("DECIMAL(6,3)", "DECIMAL(6,3)");
        testNumberMappingsNativeQuery("DOUBLE", "DOUBLE");
    }

    private void testNumberMappingsNativeQuery(String exasolType, String expectedType)
    {
        assertQuery("SELECT * FROM TABLE(system.query(query => 'SELECT CAST(1 AS " + exasolType + ")'))",
                "VALUES CAST (1 AS " + expectedType + ")");
    }

    @Test
    void testDouble()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("double precision", "NULL", DOUBLE, "CAST(NULL AS double)")
                .addRoundTrip("double precision", "1.0E100", DOUBLE, "1.0E100")
                .addRoundTrip("double precision", "123.456E10", DOUBLE, "123.456E10")
                .addRoundTrip("double", "123.456E10", DOUBLE, "123.456E10")
                .addRoundTrip("float", "123.456E10", DOUBLE, "123.456E10")
                .addRoundTrip("number", "123.456E10", DOUBLE, "123.456E10")
                .addRoundTrip("real", "123.456E10", DOUBLE, "123.456E10")
                .execute(getQueryRunner(), exasolCreateAndInsert(TEST_SCHEMA + "." + "exasol_test_double"));
    }

    @Test
    void testDecimal()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("decimal(3, 0)", "CAST(NULL AS decimal(3, 0))", createDecimalType(3, 0), "CAST(NULL AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 0)", "CAST('193' AS decimal(3, 0))", createDecimalType(3, 0), "CAST('193' AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 0)", "CAST('19' AS decimal(3, 0))", createDecimalType(3, 0), "CAST('19' AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 0)", "CAST('-193' AS decimal(3, 0))", createDecimalType(3, 0), "CAST('-193' AS decimal(3, 0))")
                .addRoundTrip("decimal(4, 0)", "CAST('19' AS decimal(4, 0))", createDecimalType(4, 0), "CAST('19' AS decimal(4, 0))") // JDBC Type SMALLINT
                .addRoundTrip("decimal(5, 0)", "CAST('19' AS decimal(5, 0))", createDecimalType(5, 0), "CAST('19' AS decimal(5, 0))") // JDBC Type INTEGER
                .addRoundTrip("decimal(10, 0)", "CAST('19' AS decimal(10, 0))", createDecimalType(10, 0), "CAST('19' AS decimal(10, 0))") // JDBC Type BIGINT
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
                .addRoundTrip("decimal(36, 0)", "CAST(NULL AS decimal(36, 0))", createDecimalType(36, 0), "CAST(NULL AS decimal(36, 0))")
                .addRoundTrip("decimal(36, 0)", "CAST('999999999999999999999999999999999999' AS decimal(36, 0))", createDecimalType(36, 0), "CAST('999999999999999999999999999999999999' AS decimal(36, 0))")
                .addRoundTrip("decimal(36, 0)", "CAST('-999999999999999999999999999999999999' AS decimal(36, 0))", createDecimalType(36, 0), "CAST('-999999999999999999999999999999999999' AS decimal(36, 0))")
                .addRoundTrip("decimal(36, 36)", "CAST('0.27182818284590452353602874713526624977' AS decimal(36, 36))", createDecimalType(36, 36), "CAST('0.27182818284590452353602874713526624977' AS decimal(36, 36))")
                .addRoundTrip("decimal(36, 36)", "CAST('-0.27182818284590452353602874713526624977' AS decimal(36, 36))", createDecimalType(36, 36), "CAST('-0.27182818284590452353602874713526624977' AS decimal(36, 36))")
                .execute(getQueryRunner(), exasolCreateAndInsert(TEST_SCHEMA + "." + "test_decimal"));
    }

    @Test
    void testDecimal38NotSupported()
    {
        assertThatThrownBy(() ->
                SqlDataTypeTest.create()
                        .addRoundTrip("decimal(38, 0)", "CAST(NULL AS decimal(38, 0))", createDecimalType(38, 0), "CAST(NULL AS decimal(38, 0))")
                        .execute(getQueryRunner(), exasolCreateAndInsert(TEST_SCHEMA + "." + "test_decimal")))
                .cause().hasMessageStartingWith("illegal precision value: 38");
    }

    @Test
    void testCompareDecimal38()
    {
        try (TestTable testTable = new TestTable(
                exasolServer::execute,
                "tpch.test_exceeding_max_decimal",
                "(d_col decimal(36,0))",
                asList("123456789012345678901234567890123456", "-123456789012345678901234567890123456"))) {
            assertQuery(
                    format("SELECT d_col from %s where d_col = CAST('123456789012345678901234567890123456' AS decimal(38))", testTable.getName()),
                    "VALUES (123456789012345678901234567890123456)");
            assertQuery(
                    format("SELECT d_col from %s where d_col = CAST('-123456789012345678901234567890123456' AS decimal(38))", testTable.getName()),
                    "VALUES (-123456789012345678901234567890123456)");
        }
    }

    @Test
    void testChar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("char(10)", "NULL", createCharType(10), "CAST(NULL AS char(10))")
                .addRoundTrip("char(10)", "''", createCharType(10), "CAST(NULL AS char(10))")
                .addRoundTrip("char(6)", "'text_a'", createCharType(6), "CAST('text_a' AS char(6))")
                .addRoundTrip("char(5)", "'攻殻機動隊'", createCharType(5), "CAST('攻殻機動隊' AS char(5))")
                .addRoundTrip("char(1)", "'😂'", createCharType(1), "CAST('😂' AS char(1))")
                .execute(getQueryRunner(), exasolCreateAndInsert(TEST_SCHEMA + "." + "test_char"));
    }

    @Test
    void testVarchar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("varchar(10)", "NULL", createVarcharType(10), "CAST(NULL AS varchar(10))")
                .addRoundTrip("varchar(10)", "''", createVarcharType(10), "CAST(NULL AS varchar(10))")
                .addRoundTrip("varchar(10)", "'text_a'", createVarcharType(10), "CAST('text_a' AS varchar(10))")
                .addRoundTrip("varchar(255)", "'text_b'", createVarcharType(255), "CAST('text_b' AS varchar(255))")
                .addRoundTrip("varchar(65535)", "'text_d'", createVarcharType(65535), "CAST('text_d' AS varchar(65535))")
                .addRoundTrip("varchar(5)", "'攻殻機動隊'", createVarcharType(5), "CAST('攻殻機動隊' AS varchar(5))")
                .addRoundTrip("varchar(32)", "'攻殻機動隊'", createVarcharType(32), "CAST('攻殻機動隊' AS varchar(32))")
                .addRoundTrip("varchar(20000)", "'攻殻機動隊'", createVarcharType(20000), "CAST('攻殻機動隊' AS varchar(20000))")
                .addRoundTrip("varchar(1)", "'😂'", createVarcharType(1), "CAST('😂' AS varchar(1))")
                .addRoundTrip("varchar(77)", "'Ну, погоди!'", createVarcharType(77), "CAST('Ну, погоди!' AS varchar(77))")
                .addRoundTrip("varchar(2000000)", "'text_f'", createVarcharType(2000000), "CAST('text_f' AS varchar(2000000))") // too long for a char in Trino
                .execute(getQueryRunner(), exasolCreateAndInsert(TEST_SCHEMA + "." + "test_varchar"));
    }

    @Test
    void testDate()
    {
        testDate(UTC);
        testDate(jvmZone);
        // using two non-JVM zones so that we don't need to worry what Exasol system zone is
        testDate(vilnius);
        testDate(kathmandu);
        testDate(TestingSession.DEFAULT_TIME_ZONE_KEY.getZoneId());
    }

    private void testDate(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        SqlDataTypeTest.create()
                .addRoundTrip("date", "NULL", DATE, "CAST(NULL AS DATE)")
                .addRoundTrip("date", "DATE '0001-01-01'", DATE, "DATE '0001-01-01'") // min value in Exasol
                .addRoundTrip("date", "DATE '1582-09-30'", DATE, "DATE '1582-09-30'")
                .addRoundTrip("date", "DATE '1582-10-01'", DATE, "DATE '1582-10-01'")
                .addRoundTrip("date", "DATE '1582-10-02'", DATE, "DATE '1582-10-02'")
                .addRoundTrip("date", "DATE '1582-10-03'", DATE, "DATE '1582-10-03'")
                .addRoundTrip("date", "DATE '1582-10-04'", DATE, "DATE '1582-10-04'")
                .addRoundTrip("date", "DATE '1582-10-05'", DATE, "DATE '1582-10-15'") // Julian-Gregorian calendar cut-over
                .addRoundTrip("date", "DATE '1582-10-13'", DATE, "DATE '1582-10-15'") // Julian-Gregorian calendar cut-over
                .addRoundTrip("date", "DATE '1582-10-14'", DATE, "DATE '1582-10-15'")
                .addRoundTrip("date", "DATE '1582-10-15'", DATE, "DATE '1582-10-15'")
                .addRoundTrip("date", "DATE '1970-01-01'", DATE, "DATE '1970-01-01'")
                .addRoundTrip("date", "DATE '1970-02-03'", DATE, "DATE '1970-02-03'")
                .addRoundTrip("date", "DATE '2017-07-01'", DATE, "DATE '2017-07-01'") // summer on northern hemisphere (possible DST)
                .addRoundTrip("date", "DATE '2017-01-01'", DATE, "DATE '2017-01-01'") // winter on northern hemisphere (possible DST on southern hemisphere)
                .addRoundTrip("date", "DATE '1970-01-01'", DATE, "DATE '1970-01-01'") // change forward at midnight in JVM
                .addRoundTrip("date", "DATE '1983-04-01'", DATE, "DATE '1983-04-01'") // change forward at midnight in Vilnius
                .addRoundTrip("date", "DATE '1983-10-01'", DATE, "DATE '1983-10-01'") // change backward at midnight in Vilnius
                .addRoundTrip("date", "DATE '9999-12-31'", DATE, "DATE '9999-12-31'") // max value in Exasol
                .execute(getQueryRunner(), session, exasolCreateAndInsert(TEST_SCHEMA + "." + "test_date"));
    }

    // See for more details: https://docs.exasol.com/db/latest/sql_references/data_types/datatypedetails.htm
    @Test
    void testTimestamp()
    {
        testTimestamp(UTC);
        testTimestamp(jvmZone);
        // using two non-JVM zones so that we don't need to worry what Exasol system zone is
        testTimestamp(vilnius);
        testTimestamp(kathmandu);
        testTimestamp(TestingSession.DEFAULT_TIME_ZONE_KEY.getZoneId());
    }

    private void testTimestamp(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        SqlDataTypeTest.create()
                .addRoundTrip("timestamp", "NULL", createTimestampType(3), "CAST(NULL AS TIMESTAMP)")
                .addRoundTrip("timestamp", "TIMESTAMP '2019-03-18 10:01:17.987'", createTimestampType(3), "TIMESTAMP '2019-03-18 10:01:17.987'")
                .addRoundTrip("timestamp", "TIMESTAMP '2013-03-11 17:30:15.123'", createTimestampType(3), "TIMESTAMP '2013-03-11 17:30:15.123'")
                .addRoundTrip("timestamp", "TIMESTAMP '2018-10-28 01:33:17.456'", createTimestampType(3), "TIMESTAMP '2018-10-28 01:33:17.456'")
                .addRoundTrip("timestamp", "TIMESTAMP '2018-10-28 03:33:33.333'", createTimestampType(3), "TIMESTAMP '2018-10-28 03:33:33.333'")
                .addRoundTrip("timestamp", "TIMESTAMP '1970-01-01 00:13:42.000'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:13:42.000'")
                .addRoundTrip("timestamp", "TIMESTAMP '2018-04-01 03:13:55.123'", createTimestampType(3), "TIMESTAMP '2018-04-01 03:13:55.123'")
                .addRoundTrip("timestamp", "TIMESTAMP '2020-09-27 12:34:56.999'", createTimestampType(3), "TIMESTAMP '2020-09-27 12:34:56.999'")
                .addRoundTrip("timestamp", "TIMESTAMP '2018-03-25 03:17:17.000'", createTimestampType(3), "TIMESTAMP '2018-03-25 03:17:17.000'")
                .addRoundTrip("timestamp", "TIMESTAMP '1986-01-01 00:13:07.000'", createTimestampType(3), "TIMESTAMP '1986-01-01 00:13:07.000'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2013-03-11 17:30:15.123456'", createTimestampType(6), "TIMESTAMP '2013-03-11 17:30:15.123456'")
                .addRoundTrip("timestamp(9)", "TIMESTAMP '2013-03-11 17:30:15.123456789'", createTimestampType(9), "TIMESTAMP '2013-03-11 17:30:15.123456789'")
                .addRoundTrip("timestamp(1)", "TIMESTAMP '2016-08-19 19:28:05.0'", createTimestampType(1), "TIMESTAMP '2016-08-19 19:28:05.0'")
                .addRoundTrip("timestamp(2)", "TIMESTAMP '2016-08-19 19:28:05.01'", createTimestampType(2), "TIMESTAMP '2016-08-19 19:28:05.01'")
                .addRoundTrip("timestamp", "TIMESTAMP '3030-03-03 12:34:56.123'", createTimestampType(3), "TIMESTAMP '3030-03-03 12:34:56.123'")
                .addRoundTrip("timestamp(5)", "TIMESTAMP '3030-03-03 12:34:56.12345'", createTimestampType(5), "TIMESTAMP '3030-03-03 12:34:56.12345'")
                .addRoundTrip("timestamp(9)", "TIMESTAMP '3030-03-03 12:34:56.123456789'", createTimestampType(9), "TIMESTAMP '3030-03-03 12:34:56.123456789'")
                .addRoundTrip("timestamp", "TIMESTAMP '3030-03-03 12:34:56.123'", createTimestampType(3), "TIMESTAMP '3030-03-03 12:34:56.123'")
                .addRoundTrip("timestamp(5)", "TIMESTAMP '3030-03-03 12:34:56.12345'", createTimestampType(5), "TIMESTAMP '3030-03-03 12:34:56.12345'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '3030-03-03 12:34:56.123456'", createTimestampType(6), "TIMESTAMP '3030-03-03 12:34:56.123456'")
                .addRoundTrip("timestamp(7)", "TIMESTAMP '3030-03-03 12:34:56.1234567'", createTimestampType(7), "TIMESTAMP '3030-03-03 12:34:56.1234567'")
                .addRoundTrip("timestamp(8)", "TIMESTAMP '3030-03-03 12:34:56.12345678'", createTimestampType(8), "TIMESTAMP '3030-03-03 12:34:56.12345678'")
                .addRoundTrip("timestamp(9)", "TIMESTAMP '3030-03-03 12:34:56.123456789'", createTimestampType(9), "TIMESTAMP '3030-03-03 12:34:56.123456789'")
                .addRoundTrip("timestamp(0)", "TIMESTAMP '2017-07-01'", createTimestampType(0), "TIMESTAMP '2017-07-01'") // summer on northern hemisphere (possible DST)
                .addRoundTrip("timestamp(0)", "TIMESTAMP '2017-01-01'", createTimestampType(0), "TIMESTAMP '2017-01-01'") // winter on northern hemisphere (possible DST on southern hemisphere)
                .addRoundTrip("timestamp(0)", "TIMESTAMP '1970-01-01'", createTimestampType(0), "TIMESTAMP '1970-01-01'") // change forward at midnight in JVM
                .addRoundTrip("timestamp(0)", "TIMESTAMP '1983-04-01'", createTimestampType(0), "TIMESTAMP '1983-04-01'") // change forward at midnight in Vilnius
                .addRoundTrip("timestamp(0)", "TIMESTAMP '1983-10-01'", createTimestampType(0), "TIMESTAMP '1983-10-01'") // change backward at midnight in Vilnius
                .addRoundTrip("timestamp(0)", "TIMESTAMP '9999-12-31'", createTimestampType(0), "TIMESTAMP '9999-12-31'") // max value in Exasol
                //test cases for timestamp with zero precision and with non-zero seconds
                .addRoundTrip("timestamp(0)", "TIMESTAMP '2017-07-01 00:00:01'", createTimestampType(0), "TIMESTAMP '2017-07-01 00:00:01'") // summer on northern hemisphere (possible DST)
                .addRoundTrip("timestamp(0)", "TIMESTAMP '2017-01-01 00:00:02'", createTimestampType(0), "TIMESTAMP '2017-01-01 00:00:02'") // winter on northern hemisphere (possible DST on southern hemisphere)
                .addRoundTrip("timestamp(0)", "TIMESTAMP '1970-01-01 00:00:03'", createTimestampType(0), "TIMESTAMP '1970-01-01 00:00:03'") // change forward at midnight in JVM
                .addRoundTrip("timestamp(0)", "TIMESTAMP '1983-04-01 00:00:04'", createTimestampType(0), "TIMESTAMP '1983-04-01 00:00:04'") // change forward at midnight in Vilnius
                .addRoundTrip("timestamp(0)", "TIMESTAMP '1983-10-01 00:00:05'", createTimestampType(0), "TIMESTAMP '1983-10-01 00:00:05'") // change backward at midnight in Vilnius
                .addRoundTrip("timestamp(0)", "TIMESTAMP '9999-12-31 00:00:59'", createTimestampType(0), "TIMESTAMP '9999-12-31 00:00:59'") // max value in Exasol
                .execute(getQueryRunner(), session, exasolCreateAndInsert(TEST_SCHEMA + "." + "test_timestamp"));
    }

    @Test
    void testTimestampWithTimeZone()
    {
        testTimestampWithTimeZone(UTC);
        testTimestampWithTimeZone(jvmZone);
        // using two non-JVM zones so that we don't need to worry what Exasol system zone is
        testTimestampWithTimeZone(vilnius);
        testTimestampWithTimeZone(kathmandu);
        testTimestampWithTimeZone(TestingSession.DEFAULT_TIME_ZONE_KEY.getZoneId());
    }

    /**
     * <p>
     * Exasol {@code TIMESTAMP WITH LOCAL TIME ZONE} does <b>not</b> store any time zone information in the database.
     * In Trino, it is mapped to {@code TIMESTAMP WITH TIME ZONE} with the hardcoded JVM time zone
     * </p>
     *
     * <p>
     * This limitation imposes some rules and restrictions on the tests:
     * <ul>
     *   <li>Both {@code inputLiteral} and {@code expectedLiteral} are interpreted as JVM time zone strings.</li>
     *   <li>In the Trino testing environment, the JVM time zone is hardcoded to {@code America/Bahia_Banderas}.</li>
     *   <li>When saved {@code inputLiteral} is read by Trino, it must be equal to the {@code expectedLiteral}, interpreted as JVM time zone value</li>
     * </ul>
     * </p>
     * <p>
     * <b>See for more details:</b> https://docs.exasol.com/db/latest/sql_references/data_types/datatypedetails.htm
     * <p>
     */
    private void testTimestampWithTimeZone(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        SqlDataTypeTest.create()
                .addRoundTrip("timestamp with local time zone", "NULL", createTimestampWithTimeZoneType(3), "CAST(NULL AS TIMESTAMP WITH TIME ZONE)")

                // timestamp with precision 3 examples
                .addRoundTrip("timestamp with local time zone", timestampWithTimeZoneInput("2019-03-18 10:01:17.123"), createTimestampWithTimeZoneType(3), timestampWithTimeZoneExpected("2019-03-18 10:01:17.123"))
                .addRoundTrip("timestamp(3) with local time zone", timestampWithTimeZoneInput("2018-10-27 19:33:17.456"), createTimestampWithTimeZoneType(3), timestampWithTimeZoneExpected("2018-10-27 19:33:17.456"))
                .addRoundTrip("timestamp(3) with local time zone", timestampWithTimeZoneInput("2018-10-28 03:33:33.333"), createTimestampWithTimeZoneType(3), timestampWithTimeZoneExpected("2018-10-28 03:33:33.333"))
                .addRoundTrip("timestamp(3) with local time zone", timestampWithTimeZoneInput("1970-01-01 00:13:42.000"), createTimestampWithTimeZoneType(3), timestampWithTimeZoneExpected("1970-01-01 00:13:42.000"))

                .addRoundTrip("timestamp(3) with local time zone", timestampWithTimeZoneInput("2018-04-01 03:13:55.123"), createTimestampWithTimeZoneType(3), timestampWithTimeZoneExpected("2018-04-01 03:13:55.123"))
                .addRoundTrip("timestamp(3) with local time zone", timestampWithTimeZoneInput("2020-09-27 12:34:56.999"), createTimestampWithTimeZoneType(3), timestampWithTimeZoneExpected("2020-09-27 12:34:56.999"))
                .addRoundTrip("timestamp(3) with local time zone", timestampWithTimeZoneInput("2018-03-25 03:17:17.000"), createTimestampWithTimeZoneType(3), timestampWithTimeZoneExpected("2018-03-25 03:17:17.000"))
                .addRoundTrip("timestamp(3) with local time zone", timestampWithTimeZoneInput("1986-01-01 00:13:07.000"), createTimestampWithTimeZoneType(3), timestampWithTimeZoneExpected("1986-01-01 00:13:07.000"))

                // timestamp with precision 6-9 examples
                .addRoundTrip("timestamp(6) with local time zone", timestampWithTimeZoneInput("2019-03-18 10:01:17.987654"), createTimestampWithTimeZoneType(6), timestampWithTimeZoneExpected("2019-03-18 10:01:17.987654"))
                .addRoundTrip("timestamp(6) with local time zone", timestampWithTimeZoneInput("2018-10-28 01:33:17.456789"), createTimestampWithTimeZoneType(6), timestampWithTimeZoneExpected("2018-10-28 01:33:17.456789"))
                .addRoundTrip("timestamp(6) with local time zone", timestampWithTimeZoneInput("2018-10-28 03:33:33.333333"), createTimestampWithTimeZoneType(6), timestampWithTimeZoneExpected("2018-10-28 03:33:33.333333"))
                .addRoundTrip("timestamp(6) with local time zone", timestampWithTimeZoneInput("1970-01-01 00:13:42.000000"), createTimestampWithTimeZoneType(6), timestampWithTimeZoneExpected("1970-01-01 00:13:42.000000"))
                .addRoundTrip("timestamp(6) with local time zone", timestampWithTimeZoneInput("2018-04-01 03:13:55.123456"), createTimestampWithTimeZoneType(6), timestampWithTimeZoneExpected("2018-04-01 03:13:55.123456"))
                .addRoundTrip("timestamp(6) with local time zone", timestampWithTimeZoneInput("2018-03-25 03:17:17.000000"), createTimestampWithTimeZoneType(6), timestampWithTimeZoneExpected("2018-03-25 03:17:17.000000"))
                .addRoundTrip("timestamp(6) with local time zone", timestampWithTimeZoneInput("1986-01-01 00:13:07.000000"), createTimestampWithTimeZoneType(6), timestampWithTimeZoneExpected("1986-01-01 00:13:07.000000"))
                .addRoundTrip("timestamp(7) with local time zone", timestampWithTimeZoneInput("1986-01-01 00:13:07.1234567"), createTimestampWithTimeZoneType(7), timestampWithTimeZoneExpected("1986-01-01 00:13:07.1234567"))
                .addRoundTrip("timestamp(8) with local time zone", timestampWithTimeZoneInput("1986-01-01 00:13:07.12345678"), createTimestampWithTimeZoneType(8), timestampWithTimeZoneExpected("1986-01-01 00:13:07.12345678"))
                .addRoundTrip("timestamp(9) with local time zone", timestampWithTimeZoneInput("1986-01-01 00:13:07.123456789"), createTimestampWithTimeZoneType(9), timestampWithTimeZoneExpected("1986-01-01 00:13:07.123456789"))

                // tests for other precisions (0-5 and some 1's)
                .addRoundTrip("timestamp(0) with local time zone", timestampWithTimeZoneInput("1970-01-01 00:00:01"), createTimestampWithTimeZoneType(0), timestampWithTimeZoneExpected("1970-01-01 00:00:01"))
                .addRoundTrip("timestamp(1) with local time zone", timestampWithTimeZoneInput("1970-01-01 00:00:01.1"), createTimestampWithTimeZoneType(1), timestampWithTimeZoneExpected("1970-01-01 00:00:01.1"))
                .addRoundTrip("timestamp(1) with local time zone", timestampWithTimeZoneInput("1970-01-01 00:00:01.9"), createTimestampWithTimeZoneType(1), timestampWithTimeZoneExpected("1970-01-01 00:00:01.9"))
                .addRoundTrip("timestamp(2) with local time zone", timestampWithTimeZoneInput("1970-01-01 00:00:01.12"), createTimestampWithTimeZoneType(2), timestampWithTimeZoneExpected("1970-01-01 00:00:01.12"))
                .addRoundTrip("timestamp(3) with local time zone", timestampWithTimeZoneInput("1970-01-01 00:00:01.123"), createTimestampWithTimeZoneType(3), timestampWithTimeZoneExpected("1970-01-01 00:00:01.123"))
                .addRoundTrip("timestamp(3) with local time zone", timestampWithTimeZoneInput("1970-01-01 00:00:01.999"), createTimestampWithTimeZoneType(3), timestampWithTimeZoneExpected("1970-01-01 00:00:01.999"))
                .addRoundTrip("timestamp(4) with local time zone", timestampWithTimeZoneInput("1970-01-01 00:00:01.1234"), createTimestampWithTimeZoneType(4), timestampWithTimeZoneExpected("1970-01-01 00:00:01.1234"))
                .addRoundTrip("timestamp(5) with local time zone", timestampWithTimeZoneInput("1970-01-01 00:00:01.12345"), createTimestampWithTimeZoneType(5), timestampWithTimeZoneExpected("1970-01-01 00:00:01.12345"))
                .addRoundTrip("timestamp(1) with local time zone", timestampWithTimeZoneInput("2020-09-27 12:34:56.1"), createTimestampWithTimeZoneType(1), timestampWithTimeZoneExpected("2020-09-27 12:34:56.1"))
                .addRoundTrip("timestamp(1) with local time zone", timestampWithTimeZoneInput("2020-09-27 12:34:56.9"), createTimestampWithTimeZoneType(1), timestampWithTimeZoneExpected("2020-09-27 12:34:56.9"))
                .addRoundTrip("timestamp(3) with local time zone", timestampWithTimeZoneInput("2020-09-27 12:34:56.123"), createTimestampWithTimeZoneType(3), timestampWithTimeZoneExpected("2020-09-27 12:34:56.123"))
                .addRoundTrip("timestamp(3) with local time zone", timestampWithTimeZoneInput("2020-09-27 12:34:56.999"), createTimestampWithTimeZoneType(3), timestampWithTimeZoneExpected("2020-09-27 12:34:56.999"))
                .addRoundTrip("timestamp(6) with local time zone", timestampWithTimeZoneInput("2020-09-27 12:34:56.123456"), createTimestampWithTimeZoneType(6), timestampWithTimeZoneExpected("2020-09-27 12:34:56.123456"))

                //test cases for timestamp with zero precision and with non-zero seconds
                .addRoundTrip("timestamp(0) with local time zone", timestampWithTimeZoneInput("2017-07-01 00:00:01"), createTimestampWithTimeZoneType(0), timestampWithTimeZoneExpected("2017-07-01 00:00:01")) // summer on northern hemisphere (possible DST)
                .addRoundTrip("timestamp(0) with local time zone", timestampWithTimeZoneInput("2017-01-01 00:00:02"), createTimestampWithTimeZoneType(0), timestampWithTimeZoneExpected("2017-01-01 00:00:02")) // winter on northern hemisphere (possible DST on southern hemisphere)
                .addRoundTrip("timestamp(0) with local time zone", timestampWithTimeZoneInput("1970-01-01 00:00:03"), createTimestampWithTimeZoneType(0), timestampWithTimeZoneExpected("1970-01-01 00:00:03")) // change forward at midnight in JVM
                .addRoundTrip("timestamp(0) with local time zone", timestampWithTimeZoneInput("1983-04-01 00:00:04"), createTimestampWithTimeZoneType(0), timestampWithTimeZoneExpected("1983-04-01 00:00:04")) // change forward at midnight in Vilnius
                .addRoundTrip("timestamp(0) with local time zone", timestampWithTimeZoneInput("1983-10-01 00:00:05"), createTimestampWithTimeZoneType(0), timestampWithTimeZoneExpected("1983-10-01 00:00:05")) // change backward at midnight in Vilnius
                .addRoundTrip("timestamp(0) with local time zone", timestampWithTimeZoneInput("9999-12-31 00:00:59"), createTimestampWithTimeZoneType(0), timestampWithTimeZoneExpected("9999-12-31 00:00:59")) // max value in Exasol

                .execute(getQueryRunner(), session, exasolCreateAndInsert(TEST_SCHEMA + "." + "test_timestamp"));
    }

    private String timestampWithTimeZoneInput(String jvmTimeZoneString)
    {
        return "TIMESTAMP '%s'".formatted(jvmTimeZoneString);
    }

    private String timestampWithTimeZoneExpected(String jvmTimeZoneString)
    {
        return "TIMESTAMP '%s %s'".formatted(jvmTimeZoneString, jvmZone.getId());
    }

    @Test
    // See for more details: https://docs.exasol.com/db/latest/sql_references/data_types/datatypedetails.htm
    void testUnsupportedTimestampValues()
    {
        // Below minimum supported TIMESTAMP value (must be >= 0001-01-01)
        testUnsupportedInsertValue(
                "TIMESTAMP",
                "TIMESTAMP '10000-01-01 00:00:00.000000'",
                "data exception - invalid character value for cast; Value: '10000-01-01 00:00:00.000000'");

        // Above maximum supported TIMESTAMP value (must be <= 9999-12-31)
        testUnsupportedInsertValue(
                "TIMESTAMP",
                "TIMESTAMP '0000-12-31 23:59:59.999999'",
                "data exception - invalid date value; Value: '0000-12-31 23:59:59.999999'");

        // Exceeds TIMESTAMP maximum supported fractional seconds precision (9 digits)
        testUnsupportedInsertValue(
                "TIMESTAMP",
                "TIMESTAMP '2024-01-01 12:34:56.1234567890'",
                "data exception - invalid character value for cast; Value: '2024-01-01 12:34:56.1234567890'");

        // Negative precisions are not supported
        testUnsupportedDefinition(
                "TIMESTAMP(-1)",
                "syntax error, unexpected '-', expecting UNSIGNED_INTEGER");
    }

    @Test
    // See for more details: https://docs.exasol.com/db/latest/sql_references/data_types/datatypedetails.htm
    void testUnsupportedTimestampWithLocalTimeZoneValues()
    {
        testUnsupportedDstGapJvmTimeZoneValue("2018-04-01 02:13:55.123", 3);
        testUnsupportedDstGapJvmTimeZoneValue("2018-04-01 02:13:55.123456", 6);

        // Below minimum supported TIMESTAMP WITH LOCAL TIME ZONE value (must be >= 0001-01-01)
        testUnsupportedInsertValue(
                "TIMESTAMP WITH LOCAL TIME ZONE",
                "TIMESTAMP '10000-01-01 00:00:00.000000'",
                "data exception - invalid character value for cast; Value: '10000-01-01 00:00:00.000000'");

        // Above maximum supported TIMESTAMP WITH LOCAL TIME ZONE value (must be <= 9999-12-31)
        testUnsupportedInsertValue(
                "TIMESTAMP WITH LOCAL TIME ZONE",
                "TIMESTAMP '0000-12-31 23:59:59.999999'",
                "data exception - invalid date value; Value: '0000-12-31 23:59:59.999999'");

        // Exceeds TIMESTAMP WITH LOCAL TIME ZONE maximum supported fractional seconds precision (9 digits)
        testUnsupportedInsertValue(
                "TIMESTAMP WITH LOCAL TIME ZONE",
                "TIMESTAMP '2024-01-01 12:34:56.1234567890'",
                "data exception - invalid character value for cast; Value: '2024-01-01 12:34:56.1234567890'");

        // Negative precisions are not supported
        testUnsupportedDefinition(
                "TIMESTAMP(-1) WITH LOCAL TIME ZONE",
                "syntax error, unexpected '-', expecting UNSIGNED_INTEGER");
    }

    private void testUnsupportedDstGapJvmTimeZoneValue(String dstGapJvmTimeZoneString, int precision)
    {
        try (TestTable table = new TestTable(onRemoteDatabase(), TestingExasolServer.TEST_SCHEMA + ".timestamp_dst_gap",
                "(col TIMESTAMP(%d) WITH LOCAL TIME ZONE)".formatted(precision))) {
            // Exasol successfully resolves dst gap value and saves it to database
            onRemoteDatabase().execute(format("INSERT INTO %s (col) VALUES (%s)", table.getName(),
                    "TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS.FF%d')".formatted(dstGapJvmTimeZoneString, precision)));
            assertThat(exasolServer.getSingleResult(
                    "SELECT count(*) FROM %s WHERE col = TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS.FF%d')".formatted(table.getName(), dstGapJvmTimeZoneString, precision),
                    String.class))
                    .matches("1");

            //Trino query throws exception, because dst gap value is not valid for 'America/Bahia_Banderas' time zone
            assertThat(query(
                    "SELECT count(*) FROM %s WHERE col = TIMESTAMP '%s %s'".formatted(table.getName(), dstGapJvmTimeZoneString, jvmZone.getId())))
                    .failure().hasMessageEndingWith("America/Bahia_Banderas' is not a valid TIMESTAMP literal");
        }
    }

    // See for more details: https://docs.exasol.com/saas/microcontent/Resources/MicroContent/general/hash-data-type.htm
    @Test
    void testHashtype()
    {
        SqlDataTypeTest.create()
                // Null
                .addRoundTrip("hashtype", "NULL", VARBINARY, "from_hex(NULL)")

                // Explicit 16-byte
                .addRoundTrip("hashtype(16 byte)", "'550e8400-e29b-11d4-a716-446655440000'", VARBINARY, "from_hex('550e8400e29b11d4a716446655440000')")

                // Explicit 16-byte with curly brackets
                .addRoundTrip("hashtype(16 byte)", "'{550e8400-e29b-11d4-a716-446655440000}'", VARBINARY, "from_hex('550e8400e29b11d4a716446655440000')")

                // Explicit 32-byte
                .addRoundTrip("hashtype(32 byte)", "'00112233-44556677-8899AABB-CCDDEEFF-00112233-44556677-8899AABB-CCDDEEFF'", VARBINARY,
                        "from_hex('00112233445566778899AABBCCDDEEFF00112233445566778899AABBCCDDEEFF')")
                .addRoundTrip("hashtype(32 byte)", "'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF'", VARBINARY,
                        "from_hex('FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF')")

                // Boundary: minimum size (1 byte)
                .addRoundTrip("hashtype(1 byte)", "'FF'", VARBINARY, "from_hex('FF')")

                // Boundary: maximum size (1024 byte) - use repeated pattern
                .addRoundTrip("hashtype(1024 byte)", "'" + "AA".repeat(1024) + "'", VARBINARY, "from_hex('" + "AA".repeat(1024) + "')")

                .execute(getQueryRunner(), exasolCreateAndInsert(TEST_SCHEMA + "." + "test_hashtype_as_varbinary_mapping"));
    }

    // See for more details: https://docs.exasol.com/saas/microcontent/Resources/MicroContent/general/hash-data-type.htm
    @Test
    void testUnsupportedHashTypeDefinitions()
    {
        // Too few bytes (< 1)
        testUnsupportedDefinition(
                "HASHTYPE(0 BYTE)",
                "the given size of HASHTYPE is too small. A minimum of 1 bytes are required");

        // Too many bytes (> 1024)
        testUnsupportedDefinition(
                "HASHTYPE(1025 BYTE)",
                "the given size of HASHTYPE is too large. At most 1024 bytes are allowed");

        // Too few bits (< 8)
        testUnsupportedDefinition(
                "HASHTYPE(7 BIT)",
                "the given size of HASHTYPE is too small. A minimum of 8 bits are required");

        // Too many bits (> 8192)
        testUnsupportedDefinition(
                "HASHTYPE(8193 BIT)",
                "the given size of HASHTYPE is too large. At most 8192 bits are allowed");

        // Bits not divisible by 8
        testUnsupportedDefinition(
                "HASHTYPE(9 BIT)",
                "Bit size of HASHTYPE has to be a multiple of 8");
    }

    private void testUnsupportedDefinition(
            String exasolType,
            String expectedException)
    {
        String tableName = "test_unsupported_definition_" + randomNameSuffix();
        assertExasolSqlQueryFails(
                "CREATE TABLE %s.%s (col %s)".formatted(TEST_SCHEMA, tableName, exasolType),
                expectedException);
    }

    // See for more details: https://docs.exasol.com/saas/microcontent/Resources/MicroContent/general/hash-data-type.htm
    @Test
    void testUnsupportedHashTypeInsertValues()
    {
        // Invalid hex character
        testUnsupportedInsertValue(
                "HASHTYPE(4 BYTE)",
                "'GGGGGGGG'",
                "data exception - Invalid hash format");

        // Too short for declared size (expecting 4 bytes = 8 hex chars, got 6)
        testUnsupportedInsertValue(
                "HASHTYPE(4 BYTE)",
                "'AABBCC'",
                "data exception - Invalid hash format");

        // Too short for declared size (expecting 16 bytes = 32 hex chars, got 31)
        testUnsupportedInsertValue(
                "HASHTYPE(16 BYTE)",
                "'550e8400-e29b-11d4-a716-44665544000'",
                "data exception - Invalid hash format");

        // Too long for declared size (expecting 4 bytes = 8 hex chars, got 10)
        testUnsupportedInsertValue(
                "HASHTYPE(4 BYTE)",
                "'AABBCCDDEE'",
                "data exception - Invalid hash format");

        // Unexpected symbol inside
        testUnsupportedInsertValue(
                "HASHTYPE(4 BYTE)",
                "'AABB-CCZZ'",
                "data exception - Invalid hash format");

        // Parentheses instead of curly brackets
        testUnsupportedInsertValue(
                "HASHTYPE(4 BYTE)",
                "'(AABB-CCCC)'",
                "data exception - Invalid hash format");
    }

    private void testUnsupportedInsertValue(
            String exasolType,
            String inputLiteral,
            String expectedException)
    {
        try (TestTable table = new TestTable(onRemoteDatabase(), TEST_SCHEMA + ".test_unsupported_hashtype", "(col %s)".formatted(exasolType))) {
            assertExasolSqlQueryFails("INSERT INTO %s VALUES (%s)".formatted(table.getName(), inputLiteral), expectedException);
        }
    }

    @Test
    void testUnsupportedType()
    {
        testUnsupportedType("GEOMETRY", "'POINT(1 2)'");
        testUnsupportedType("INTERVAL YEAR TO MONTH", "'5-3'");
        testUnsupportedType("INTERVAL DAY TO SECOND", "'2 12:50:10.123'");
    }

    private void testUnsupportedType(String dataTypeName, String value)
    {
        try (TestTable table = new TestTable(onRemoteDatabase(), TestingExasolServer.TEST_SCHEMA + ".unsupported_type", format("(col %s)", dataTypeName))) {
            onRemoteDatabase().execute(format("INSERT INTO %s (col) VALUES (%s)", table.getName(), value));
            assertQueryFails("SELECT * FROM " + table.getName(), "Table '.*' has no supported columns.*");
            assertQueryFails(format("SELECT 1 FROM %s WHERE col = %s", table.getName(), value), "Table '.*' has no supported columns.*");
        }
    }

    private DataSetup exasolCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(exasolServer.getSqlExecutor(), tableNamePrefix);
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

    private void assertExasolSqlQueryFails(@Language("SQL") String sql, String expectedMessage)
    {
        assertThatThrownBy(() -> onRemoteDatabase().execute(sql)).cause()
                .hasMessageContaining(expectedMessage);
    }

    private SqlExecutor onRemoteDatabase()
    {
        return exasolServer::execute;
    }
}
