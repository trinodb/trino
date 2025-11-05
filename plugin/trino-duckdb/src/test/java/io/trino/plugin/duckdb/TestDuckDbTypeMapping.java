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
package io.trino.plugin.duckdb;

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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.duckdb.TestingDuckDb.TPCH_SCHEMA;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.time.ZoneOffset.UTC;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
final class TestDuckDbTypeMapping
        extends AbstractTestQueryFramework
{
    private TestingDuckDb duckDb;

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

    public TestDuckDbTypeMapping()
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

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        duckDb = closeAfterClass(new TestingDuckDb());
        return DuckDbQueryRunner.builder(duckDb).build();
    }

    @Test
    void testBoolean()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("boolean", "true", BOOLEAN)
                .addRoundTrip("boolean", "false", BOOLEAN)
                .addRoundTrip("boolean", "NULL", BOOLEAN, "CAST(NULL AS BOOLEAN)")
                .execute(getQueryRunner(), duckDbCreateAndInsert("test_boolean"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_boolean"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_boolean"));
    }

    @Test
    void testTinyInt()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("tinyint", "tinyint '-128'", TINYINT)
                .addRoundTrip("tinyint", "tinyint '127'", TINYINT)
                .addRoundTrip("tinyint", "NULL", TINYINT, "CAST(NULL AS TINYINT)")
                .execute(getQueryRunner(), duckDbCreateAndInsert("test_tinyint"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_tinyint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_tinyint"));
    }

    @Test
    void testSmallInt()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("smallint", "smallint '-32768'", SMALLINT)
                .addRoundTrip("smallint", "smallint '32767'", SMALLINT)
                .addRoundTrip("smallint", "NULL", SMALLINT, "CAST(NULL AS SMALLINT)")
                .execute(getQueryRunner(), duckDbCreateAndInsert("test_smallint"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_smallint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_smallint"));
    }

    @Test
    void testInteger()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("integer", "-2147483648", INTEGER)
                .addRoundTrip("integer", "2147483647", INTEGER)
                .addRoundTrip("integer", "NULL", INTEGER, "CAST(NULL AS INTEGER)")
                .execute(getQueryRunner(), duckDbCreateAndInsert("test_integer"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_integer"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_integer"));
    }

    @Test
    void testBigint()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("bigint", "-9223372036854775808", BIGINT)
                .addRoundTrip("bigint", "9223372036854775807", BIGINT)
                .addRoundTrip("bigint", "NULL", BIGINT, "CAST(NULL AS BIGINT)")
                .execute(getQueryRunner(), duckDbCreateAndInsert("test_bigint"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_bigint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_bigint"));
    }

    @Test
    void testReal()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("real", "123.456E10", REAL, "REAL '123.456E10'")
                .addRoundTrip("real", "REAL 'Infinity'", REAL, "REAL 'Infinity'")
                .addRoundTrip("real", "REAL '-Infinity'", REAL, "REAL '-Infinity'")
                .addRoundTrip("real", "REAL 'NaN'", REAL, "REAL 'NaN'")
                .addRoundTrip("real", "NULL", REAL, "CAST(NULL AS real)")
                .execute(getQueryRunner(), duckDbCreateAndInsert("test_real"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_real"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_real"));
    }

    @Test
    void testDouble()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("double", "1.0E100", DOUBLE, "1.0E100")
                .addRoundTrip("double", "123.456E10", DOUBLE, "123.456E10")
                .addRoundTrip("double", "123.456E10", DOUBLE, "123.456E10")
                .addRoundTrip("double", "DOUBLE 'Infinity'", DOUBLE, "DOUBLE 'Infinity'")
                .addRoundTrip("double", "DOUBLE '-Infinity'", DOUBLE, "DOUBLE '-Infinity'")
                .addRoundTrip("double", "DOUBLE 'NaN'", DOUBLE, "DOUBLE 'NaN'")
                .addRoundTrip("double", "NULL", DOUBLE, "CAST(NULL AS double)")
                .execute(getQueryRunner(), duckDbCreateAndInsert("test_double"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_double"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_double"));
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
                .execute(getQueryRunner(), duckDbCreateAndInsert("test_decimal"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_decimal"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_decimal"));
    }

    @Test
    void testDecimalDefault()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("decimal", "CAST('123456789012345.123' AS decimal(18, 3))", createDecimalType(18, 3), "CAST('123456789012345.123' AS decimal(18, 3))")
                .execute(getQueryRunner(), duckDbCreateAndInsert("test_decimal"));
    }

    @Test
    void testChar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("char(10)", "NULL", VARCHAR, "CAST(NULL AS varchar)")
                .addRoundTrip("char(10)", "''", VARCHAR, "CAST('' AS varchar)")
                .addRoundTrip("char(6)", "'text_a'", VARCHAR, "CAST('text_a' AS varchar)")
                .addRoundTrip("char(5)", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("char(1)", "'😂'", VARCHAR, "CAST('😂' AS varchar)")
                .execute(getQueryRunner(), duckDbCreateAndInsert("test_char"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_char"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_char"));
    }

    @Test
    void testVarchar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("varchar(10)", "NULL", VARCHAR, "CAST(NULL AS varchar)")
                .addRoundTrip("varchar(10)", "''", VARCHAR, "CAST('' AS varchar)")
                .addRoundTrip("varchar(10)", "'text_a'", VARCHAR, "CAST('text_a' AS varchar)")
                .addRoundTrip("varchar(255)", "'text_b'", VARCHAR, "CAST('text_b' AS varchar)")
                .addRoundTrip("varchar(65535)", "'text_d'", VARCHAR, "CAST('text_d' AS varchar)")
                .addRoundTrip("varchar(5)", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("varchar(32)", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("varchar(20000)", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("varchar(1)", "'😂'", VARCHAR, "CAST('😂' AS varchar)")
                .addRoundTrip("varchar(77)", "'Ну, погоди!'", VARCHAR, "CAST('Ну, погоди!' AS varchar)")
                .addRoundTrip("varchar(2000000)", "'text_f'", VARCHAR, "CAST('text_f' AS varchar)") // too long for a char in Trino
                .execute(getQueryRunner(), duckDbCreateAndInsert("test_varchar"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_varchar"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_varchar"));
    }

    @Test
    void testDate()
    {
        testDate(UTC);
        testDate(jvmZone);
        // using two non-JVM zones so that we don't need to worry what DuckDB system zone is
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
                .addRoundTrip("date", "DATE '0001-01-01'", DATE, "DATE '0001-01-01'")
                .addRoundTrip("date", "DATE '1582-09-30'", DATE, "DATE '1582-09-30'")
                .addRoundTrip("date", "DATE '1582-10-01'", DATE, "DATE '1582-10-01'")
                .addRoundTrip("date", "DATE '1582-10-02'", DATE, "DATE '1582-10-02'")
                .addRoundTrip("date", "DATE '1582-10-03'", DATE, "DATE '1582-10-03'")
                .addRoundTrip("date", "DATE '1582-10-04'", DATE, "DATE '1582-10-04'")
                .addRoundTrip("date", "DATE '1582-10-05'", DATE, "DATE '1582-10-05'") // Julian-Gregorian calendar cut-over
                .addRoundTrip("date", "DATE '1582-10-13'", DATE, "DATE '1582-10-13'") // Julian-Gregorian calendar cut-over
                .addRoundTrip("date", "DATE '1582-10-14'", DATE, "DATE '1582-10-14'")
                .addRoundTrip("date", "DATE '1582-10-15'", DATE, "DATE '1582-10-15'")
                .addRoundTrip("date", "DATE '1970-01-01'", DATE, "DATE '1970-01-01'")
                .addRoundTrip("date", "DATE '1970-02-03'", DATE, "DATE '1970-02-03'")
                .addRoundTrip("date", "DATE '2017-07-01'", DATE, "DATE '2017-07-01'") // summer on northern hemisphere (possible DST)
                .addRoundTrip("date", "DATE '2017-01-01'", DATE, "DATE '2017-01-01'") // winter on northern hemisphere (possible DST on southern hemisphere)
                .addRoundTrip("date", "DATE '1970-01-01'", DATE, "DATE '1970-01-01'") // change forward at midnight in JVM
                .addRoundTrip("date", "DATE '1983-04-01'", DATE, "DATE '1983-04-01'") // change forward at midnight in Vilnius
                .addRoundTrip("date", "DATE '1983-10-01'", DATE, "DATE '1983-10-01'") // change backward at midnight in Vilnius
                .addRoundTrip("date", "DATE '9999-12-31'", DATE, "DATE '9999-12-31'")
                .execute(getQueryRunner(), session, duckDbCreateAndInsert("test_date"))
                .execute(getQueryRunner(), session, trinoCreateAsSelect("test_date"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert("test_date"));
    }

    private DataSetup duckDbCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(duckDb.getSqlExecutor(), TPCH_SCHEMA + "." + tableNamePrefix);
    }

    private DataSetup trinoCreateAsSelect(String tableNamePrefix)
    {
        return trinoCreateAsSelect(getSession(), TPCH_SCHEMA + "." + tableNamePrefix);
    }

    private DataSetup trinoCreateAsSelect(Session session, String tableNamePrefix)
    {
        return new CreateAsSelectDataSetup(new TrinoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    private DataSetup trinoCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(new TrinoSqlExecutor(getQueryRunner()), tableNamePrefix);
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
