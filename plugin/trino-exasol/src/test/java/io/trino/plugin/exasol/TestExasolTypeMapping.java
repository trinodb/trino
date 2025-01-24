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
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.asList;
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

    @Test
    void testHashtype()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("hashtype", "NULL", createCharType(32), "CAST(NULL AS char(32))")
                .addRoundTrip("hashtype", "'550e8400-e29b-11d4-a716-446655440000'", createCharType(32), "CAST('550e8400e29b11d4a716446655440000' AS char(32))")
                .execute(getQueryRunner(), exasolCreateAndInsert(TEST_SCHEMA + "." + "test_hashtype"));
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

    private SqlExecutor onRemoteDatabase()
    {
        return exasolServer::execute;
    }
}
