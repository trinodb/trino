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
package io.trino.plugin.sqlite;

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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;

@Isolated
final class TestSqliteTypeMapping
        extends AbstractTestQueryFramework
{
    private TestingSqliteServer sqliteServer;

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
        sqliteServer = closeAfterClass(new TestingSqliteServer());
        return SqliteQueryRunner.builder(sqliteServer).build();
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
                .execute(getQueryRunner(), sqliteCreateAndInsert("test_boolean"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_boolean"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_boolean"));
    }

    @Test
    public void testTinyInt()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("tinyint", "-128", TINYINT, "TINYINT '-128'")
                .addRoundTrip("tinyint", "127", TINYINT, "TINYINT '127'")
                .addRoundTrip("tinyint", "NULL", TINYINT, "CAST(NULL AS TINYINT)")
                .execute(getQueryRunner(), sqliteCreateAndInsert("test_tinyint"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_tinyint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_tinyint"));
    }

    @Test
    public void testSmallInt()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("smallint", "-32768", SMALLINT, "SMALLINT '-32768'")
                .addRoundTrip("smallint", "32767", SMALLINT, "SMALLINT '32767'")
                .addRoundTrip("smallint", "NULL", SMALLINT, "CAST(NULL AS SMALLINT)")
                .execute(getQueryRunner(), sqliteCreateAndInsert("test_smallint"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_smallint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_smallint"));
    }

    @Test
    public void testInteger()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("integer", "-2147483648", INTEGER, "-2147483648")
                .addRoundTrip("integer", "2147483647", INTEGER, "2147483647")
                .addRoundTrip("integer", "NULL", INTEGER, "CAST(NULL AS INTEGER)")
                .execute(getQueryRunner(), sqliteCreateAndInsert("test_integer"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_integer"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_integer"));
    }

    @Test
    public void testInt()
    {
        // For max compatibility int will be converted to bigint
        SqlDataTypeTest.create()
                .addRoundTrip("int", "-2147483648", BIGINT, "BIGINT '-2147483648'")
                .addRoundTrip("int", "2147483647", BIGINT, "BIGINT '2147483647'")
                .addRoundTrip("int", "NULL", BIGINT, "CAST(NULL AS BIGINT)")
                .execute(getQueryRunner(), sqliteCreateAndInsert("test_int"));
    }

    @Test
    public void testBigInt()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("bigint", "-9223372036854775808", BIGINT, "-9223372036854775808")
                .addRoundTrip("bigint", "9223372036854775807", BIGINT, "9223372036854775807")
                .addRoundTrip("bigint", "NULL", BIGINT, "CAST(NULL AS BIGINT)")
                .execute(getQueryRunner(), sqliteCreateAndInsert("test_bigint"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_bigint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_bigint"));
    }

    @Test
    public void testDecimal()
    {
        // SQLite promises to preserve the 15 most significant digits of a floating point value.
        // see: https://sqlite.org/floatingpoint.html#floating_point_accuracy
        SqlDataTypeTest.create()
                .addRoundTrip("decimal(3, 0)", "NULL", createDecimalType(3, 0), "CAST(NULL AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 0)", "CAST('193' AS decimal(3, 0))", createDecimalType(3, 0), "CAST('193' AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 0)", "CAST('19' AS decimal(3, 0))", createDecimalType(3, 0), "CAST('19' AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 0)", "CAST('-193' AS decimal(3, 0))", createDecimalType(3, 0), "CAST('-193' AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 1)", "CAST('10.0' AS decimal(3, 1))", createDecimalType(3, 1), "CAST('10.0' AS decimal(3, 1))")
                .addRoundTrip("decimal(3, 1)", "CAST('10.1' AS decimal(3, 1))", createDecimalType(3, 1), "CAST('10.1' AS decimal(3, 1))")
                .addRoundTrip("decimal(3, 1)", "CAST('-10.1' AS decimal(3, 1))", createDecimalType(3, 1), "CAST('-10.1' AS decimal(3, 1))")
                .addRoundTrip("decimal(4, 2)", "CAST('2' AS decimal(4, 2))", createDecimalType(4, 2), "CAST('2' AS decimal(4, 2))")
                .addRoundTrip("decimal(4, 2)", "CAST('2.3' AS decimal(4, 2))", createDecimalType(4, 2), "CAST('2.3' AS decimal(4, 2))")
                .addRoundTrip("decimal(15, 2)", "CAST('2' AS decimal(15, 2))", createDecimalType(15, 2), "CAST('2' AS decimal(15, 2))")
                .addRoundTrip("decimal(15, 2)", "CAST('2.3' AS decimal(15, 2))", createDecimalType(15, 2), "CAST('2.3' AS decimal(15, 2))")
                .addRoundTrip("decimal(15, 2)", "CAST('1234567890123.12' AS decimal(15, 2))", createDecimalType(15, 2), "CAST('1234567890123.12' AS decimal(15, 2))")
                .addRoundTrip("decimal(15, 4)", "CAST('12345678901.1234' AS decimal(15, 4))", createDecimalType(15, 4), "CAST('12345678901.1234' AS decimal(15, 4))")
                .execute(getQueryRunner(), sqliteCreateAndInsert("test_decimal"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_decimal"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_decimal"));
    }

    @Test
    public void testFloat()
    {
        // I am not testing the NaN value because I haven't found how to do it with SQLite.
        SqlDataTypeTest.create()
                .addRoundTrip("real", "3.14", DOUBLE, "DOUBLE '3.14'")
                .addRoundTrip("real", "10.3e0", DOUBLE, "DOUBLE '10.3e0'")
                .addRoundTrip("real", "NULL", DOUBLE, "CAST(NULL AS DOUBLE)")
                .addRoundTrip("real", "3.1415927", DOUBLE, "DOUBLE '3.1415927'") // Overeagerly rounded by MariaDB to 3.14159
                .addRoundTrip("real", "1e999", DOUBLE, "DOUBLE 'Infinity'")
                .addRoundTrip("real", "-1e999", DOUBLE, "DOUBLE '-Infinity'")
                .execute(getQueryRunner(), sqliteCreateAndInsert("test_real"));

        SqlDataTypeTest.create()
                .addRoundTrip("float", "3.14", DOUBLE, "CAST('3.14' AS DOUBLE)")
                .addRoundTrip("float", "10.3e0", DOUBLE, "CAST('10.3e0' AS DOUBLE)")
                .addRoundTrip("float", "NULL", DOUBLE, "CAST(NULL AS DOUBLE)")
                .addRoundTrip("float", "3.1415927", DOUBLE, "DOUBLE '3.1415927'") // Overeagerly rounded by MariaDB to 3.14159
                .addRoundTrip("float", "1e999", DOUBLE, "DOUBLE 'Infinity'")
                .addRoundTrip("float", "-1e999", DOUBLE, "DOUBLE '-Infinity'")
                .execute(getQueryRunner(), sqliteCreateAndInsert("test_float"));
    }

    @Test
    void testIntegerMappings()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("tinyint", "0", TINYINT, "CAST(0 AS TINYINT)")
                .addRoundTrip("smallint", "0", SMALLINT, "CAST(0 AS SMALLINT)")
                .addRoundTrip("integer", "0", INTEGER, "CAST(0 AS INTEGER)")
                .addRoundTrip("bigint", "0", BIGINT, "CAST(0 AS BIGINT)")
                .execute(getQueryRunner(), sqliteCreateAndInsert("test_integers"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_integers"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_integers"));
    }

    @Test
    void testDouble()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("double", "NULL", DOUBLE, "CAST(NULL AS double)")
                .addRoundTrip("double", "1.0E100", DOUBLE, "1.0E100")
                .addRoundTrip("double", "123.456E10", DOUBLE, "123.456E10")
                .addRoundTrip("double", "123.456E10", DOUBLE, "123.456E10")
                .execute(getQueryRunner(), sqliteCreateAndInsert("test_double"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_double"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_double"));
    }

    @Test
    void testDoublePrecision()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("double precision", "NULL", DOUBLE, "CAST(NULL AS double)")
                .addRoundTrip("double precision", "1.0E100", DOUBLE, "1.0E100")
                .addRoundTrip("double precision", "123.456E10", DOUBLE, "123.456E10")
                .addRoundTrip("double precision", "123.456E10", DOUBLE, "123.456E10")
                .execute(getQueryRunner(), sqliteCreateAndInsert("test_double_precision"));
    }

    @Test
    void testChar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("char(10)", "NULL", createVarcharType(10), "CAST(NULL AS varchar(10))")
                .addRoundTrip("char(10)", "''", createVarcharType(10), "CAST('' AS varchar(10))")
                .addRoundTrip("char(6)", "'text_a'", createVarcharType(6), "CAST('text_a' AS varchar(6))")
                .addRoundTrip("char(15)", "'攻殻機動隊'", createVarcharType(15), "CAST('攻殻機動隊' AS varchar(15))")
                .addRoundTrip("char(4)", "'😂'", createVarcharType(4), "CAST('😂' AS varchar(4))")
                .execute(getQueryRunner(), sqliteCreateAndInsert("test_char"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_char"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_char"));
    }

    @Test
    void testVarchar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("varchar(10)", "NULL", createVarcharType(10), "CAST(NULL AS varchar(10))")
                .addRoundTrip("varchar(10)", "''", createVarcharType(10), "CAST('' AS varchar(10))")
                .addRoundTrip("varchar(10)", "'text_a'", createVarcharType(10), "CAST('text_a' AS varchar(10))")
                .addRoundTrip("varchar(255)", "'text_b'", createVarcharType(255), "CAST('text_b' AS varchar(255))")
                .addRoundTrip("varchar(10000)", "'text_d'", createVarcharType(10000), "CAST('text_d' AS varchar(10000))")
                .addRoundTrip("varchar(15)", "'攻殻機動隊'", createVarcharType(15), "CAST('攻殻機動隊' AS varchar(15))")
                .addRoundTrip("varchar(32)", "'攻殻機動隊'", createVarcharType(32), "CAST('攻殻機動隊' AS varchar(32))")
                .addRoundTrip("varchar(20000)", "'攻殻機動隊'", createVarcharType(20000), "CAST('攻殻機動隊' AS varchar(20000))")
                .addRoundTrip("varchar(4)", "'😂'", createVarcharType(4), "CAST('😂' AS varchar(4))")
                .addRoundTrip("varchar(77)", "'Ну, погоди!'", createVarcharType(77), "CAST('Ну, погоди!' AS varchar(77))")
                .addRoundTrip("varchar(32765)", "'text_f'", createVarcharType(32765), "CAST('text_f' AS varchar(32765))") // max varchar length in Sqlite
                .execute(getQueryRunner(), sqliteCreateAndInsert("test_varchar"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_varchar"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_varchar"));
    }

    @Test
    void testDate()
    {
        // SQLite doesn't support the DATE type, so we're using the date() function on a literal here.
        // As a result, this test loses some of its usefulness.
        testDate(UTC);
        testDate(jvmZone);
        // using two non-JVM zones so that we don't need to worry what Sqlite system zone is
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
                .addRoundTrip("date", "date('0001-01-01')", DATE, "DATE '0001-01-01'") // min value in Sqlite
                .addRoundTrip("date", "date('1582-09-30')", DATE, "DATE '1582-09-30'")
                .addRoundTrip("date", "date('1582-10-01')", DATE, "DATE '1582-10-01'")
                .addRoundTrip("date", "date('1582-10-02')", DATE, "DATE '1582-10-02'")
                .addRoundTrip("date", "date('1582-10-03')", DATE, "DATE '1582-10-03'")
                .addRoundTrip("date", "date('1582-10-04')", DATE, "DATE '1582-10-04'")
                .addRoundTrip("date", "date('1582-10-05')", DATE, "DATE '1582-10-05'") // Julian-Gregorian calendar cut-over
                .addRoundTrip("date", "date('1582-10-13')", DATE, "DATE '1582-10-13'") // Julian-Gregorian calendar cut-over
                .addRoundTrip("date", "date('1582-10-14')", DATE, "DATE '1582-10-14'")
                .addRoundTrip("date", "date('1582-10-15')", DATE, "DATE '1582-10-15'")
                .addRoundTrip("date", "date('1970-01-01')", DATE, "DATE '1970-01-01'")
                .addRoundTrip("date", "date('1970-02-03')", DATE, "DATE '1970-02-03'")
                .addRoundTrip("date", "date('2017-07-01')", DATE, "DATE '2017-07-01'") // summer on northern hemisphere (possible DST)
                .addRoundTrip("date", "date('2017-01-01')", DATE, "DATE '2017-01-01'") // winter on northern hemisphere (possible DST on southern hemisphere)
                .addRoundTrip("date", "date('1970-01-01')", DATE, "DATE '1970-01-01'") // change forward at midnight in JVM
                .addRoundTrip("date", "date('1983-04-01')", DATE, "DATE '1983-04-01'") // change forward at midnight in Vilnius
                .addRoundTrip("date", "date('1983-10-01')", DATE, "DATE '1983-10-01'") // change backward at midnight in Vilnius
                .addRoundTrip("date", "date('9999-12-31')", DATE, "DATE '9999-12-31'") // max value in Sqlite
                .execute(getQueryRunner(), session, sqliteCreateAndInsert("test_date"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_date"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_date"));
    }

    @Test
    void testUnsupportedType()
    {
        // SQLite is not strictly typed: it converts any unknown type to varchar,
        // but for Trino's purposes and if sqlite.use-type-affinity=false as default,
        // only recognized types will be supported.
        testUnsupportedType("GEOMETRY", "'POINT(1 2)'");
        testUnsupportedType("INTERVAL YEAR", "'5-3'");
        testUnsupportedType("INTERVAL DAY", "'2 12:50:10.123'");
    }

    private void testUnsupportedType(String dataTypeName, String value)
    {
        try (TestTable table = new TestTable(onRemoteDatabase(), "unsupported_type", format("(col %s)", dataTypeName))) {
            onRemoteDatabase().execute(format("INSERT INTO %s (col) VALUES (%s)", table.getName(), value));
            assertQueryFails("SELECT * FROM " + table.getName(), "Table '.*' has no supported columns.*");
            assertQueryFails(format("SELECT 1 FROM %s WHERE col = %s", table.getName(), value), "Table '.*' has no supported columns.*");
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

    private DataSetup sqliteCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(sqliteServer::execute, tableNamePrefix);
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
        return sqliteServer::execute;
    }
}
