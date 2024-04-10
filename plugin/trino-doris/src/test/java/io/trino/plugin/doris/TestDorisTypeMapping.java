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
package io.trino.plugin.doris;

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
import static io.trino.plugin.doris.DorisQueryRunner.createStarRocksQueryRunner;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestDorisTypeMapping
        extends AbstractTestQueryFramework
{
    private TestingDorisServer starRocksServer;

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

    private String tableDefinition(String tableDefinition, String distributionKey)
    {
        return tableDefinition + " DISTRIBUTED BY HASH(`%s`) PROPERTIES(\"replication_num\" = \"1\")".formatted(distributionKey);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        starRocksServer = closeAfterClass(new TestingDorisServer());
        return createStarRocksQueryRunner(starRocksServer, ImmutableMap.of(), ImmutableMap.of("starrocks.olap-default-replication-number", "1"), ImmutableList.of());
    }

    @Test
    public void testBoolean()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("boolean", "true", BOOLEAN, "true")
                .addRoundTrip("boolean", "false", BOOLEAN, "false")
                .addRoundTrip("boolean", "NULL", BOOLEAN, "CAST(NULL AS BOOLEAN)")
                .execute(getQueryRunner(), starRocksCreateAndInsert("tpch.test_boolean"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_boolean"));
    }

    @Test
    public void testTinyint()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("tinyint", "NULL", TINYINT, "CAST(NULL AS TINYINT)")
                .addRoundTrip("tinyint", "-128", TINYINT, "TINYINT '-128'") // min value in StarRocks and Trino
                .addRoundTrip("tinyint", "5", TINYINT, "TINYINT '5'")
                .addRoundTrip("tinyint", "127", TINYINT, "TINYINT '127'") // max value in StarRocks and Trino
                .execute(getQueryRunner(), starRocksCreateAndInsert("tpch.test_tinyint"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_tinyint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_tinyint"));
    }

    @Test
    public void testUnsupportedTinyint()
    {
        abort("StarRocks does not throw exception if inserts an out-of-range tinyint value, and instead inserts a null value");
    }

    @Test
    public void testSmallint()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("smallint", "NULL", SMALLINT, "CAST(NULL AS SMALLINT)")
                .addRoundTrip("smallint", "-32768", SMALLINT, "SMALLINT '-32768'") // min value in StarRocks and Trino
                .addRoundTrip("smallint", "32456", SMALLINT, "SMALLINT '32456'")
                .addRoundTrip("smallint", "32767", SMALLINT, "SMALLINT '32767'") // max value in StarRocks and Trino
                .execute(getQueryRunner(), starRocksCreateAndInsert("tpch.test_smallint"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_smallint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_smallint"));
    }

    @Test
    public void testUnsupportedSmallint()
    {
        abort("TODO. StarRocks does not throw exception if inserts an out-of-range smallint value, and instead inserts a null value");
    }

    @Test
    public void testInteger()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("integer", "NULL", INTEGER, "CAST(NULL AS INTEGER)")
                .addRoundTrip("integer", "0", INTEGER, "0")
                .addRoundTrip("integer", "1", INTEGER, "1")
                .addRoundTrip("integer", "-2147483648", INTEGER, "-2147483648") // min value in StarRocks and Trino
                .addRoundTrip("integer", "1234567890", INTEGER, "1234567890")
                .addRoundTrip("integer", "2147483647", INTEGER, "2147483647") // max value in StarRocks and Trino
                .execute(getQueryRunner(), starRocksCreateAndInsert("tpch.test_int"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_int"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_int"));
    }

    @Test
    public void testUnsupportedInteger()
    {
        abort("StarRocks does not throw exception if inserts an out-of-range integer value, and instead inserts a null value");
    }

    @Test
    public void testBigint()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("bigint", "NULL", BIGINT, "CAST(NULL AS BIGINT)")
                .addRoundTrip("bigint", "-9223372036854775808", BIGINT, "-9223372036854775808") // min value in StarRocks and Trino
                .addRoundTrip("bigint", "123456789012", BIGINT, "123456789012")
                .addRoundTrip("bigint", "9223372036854775807", BIGINT, "9223372036854775807") // max value in StarRocks and Trino
                .execute(getQueryRunner(), starRocksCreateAndInsert("tpch.test_bigint"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_bigint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_bigint"));
    }

    @Test
    public void testUnsupportedBigint()
    {
        try (TestTable table = new TestTable(starRocksServer::execute, "tpch.test_unsupported_bigint", tableDefinition("(data bigint)", "data"))) {
            assertStarRocksQueryFails(
                    "INSERT INTO " + table.getName() + " VALUES (-9223372036854775809)", // min - 1
                    "Number out of range[-9223372036854775809]. type: BIGINT");
            assertStarRocksQueryFails(
                    "INSERT INTO " + table.getName() + " VALUES (9223372036854775808)", // max + 1
                    "Number out of range[9223372036854775808]. type: BIGINT");
        }
    }

    @Test
    public void testChar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("char", "''", createCharType(1), "CAST('' AS char(1))")
                .addRoundTrip("char", "'a'", createCharType(1), "CAST('a' AS char(1))")
                .addRoundTrip("char(1)", "''", createCharType(1), "CAST('' AS char(1))")
                .addRoundTrip("char(1)", "'a'", createCharType(1), "CAST('a' AS char(1))")
                .addRoundTrip("char(8)", "'abc'", createCharType(8), "CAST('abc' AS char(8))")
                .addRoundTrip("char(8)", "'12345678'", createCharType(8), "CAST('12345678' AS char(8))")
                .addRoundTrip("char(255)", format("'%s'", "a".repeat(255)), createCharType(255), format("CAST('%s' AS char(255))", "a".repeat(255)))
                .addRoundTrip("char", "NULL", createCharType(1), "CAST(NULL AS char(1))")
                .addRoundTrip("char(255)", "NULL", createCharType(255), "CAST(NULL AS char(255))")
                .addRoundTrip("char(32)", "'攻殻機動隊'", createCharType(32), "CAST('攻殻機動隊' AS char(32))")
                .addRoundTrip("char(10)", "'😂'", createCharType(10), "CAST('😂' AS char(10))")
                .execute(getQueryRunner(), trinoCreateAsSelect("mysql_test_parameterized_char"))
                .execute(getQueryRunner(), trinoCreateAndInsert("mysql_test_parameterized_char"))
                .execute(getQueryRunner(), starRocksCreateAndInsert("tpch.mysql_test_parameterized_char"));
    }

    @Test
    public void testUnsupportedChar()
    {
        assertStarRocksQueryFails("CREATE TABLE tpch.test_unsupported_char " + tableDefinition("(x int, y char(256))", "x"), "Char size must be <= 255: 256");
        assertStarRocksQueryFails("CREATE TABLE tpch.test_unsupported_char " + tableDefinition("(x int, y char(500))", "x"), "Char size must be <= 255: 500");
        assertStarRocksQueryFails("CREATE TABLE tpch.test_unsupported_char " + tableDefinition("(x int, y char(1000))", "x"), "Char size must be <= 255: 1000");
        assertStarRocksQueryFails("CREATE TABLE tpch.test_unsupported_char " + tableDefinition("(x int, y char(1024))", "x"), "Char size must be <= 255: 1024");
        assertStarRocksQueryFails("CREATE TABLE tpch.test_unsupported_char " + tableDefinition("(x int, y char(12345))", "x"), "Char size must be <= 255: 12345");
    }

    @Test
    public void testVarchar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("varchar(1)", "''", createVarcharType(1), "CAST('' AS varchar(1))")
                .addRoundTrip("varchar(1)", "'a'", createVarcharType(1), "CAST('a' AS varchar(1))")
                .addRoundTrip("varchar(8)", "'abc'", createVarcharType(8), "CAST('abc' AS varchar(8))")
                .addRoundTrip("varchar(8)", "'12345678'", createVarcharType(8), "CAST('12345678' AS varchar(8))")
                .addRoundTrip("varchar(10)", "'NULL'", createVarcharType(10), "CAST('NULL' AS varchar(10))")
                .addRoundTrip("varchar(10)", "NULL", createVarcharType(10), "CAST(NULL AS varchar(10))")
                .addRoundTrip("varchar(10)", "'简体'", createVarcharType(10), "CAST('简体' AS varchar(10))")
                .addRoundTrip("varchar(10)", "'text_a'", createVarcharType(10), "CAST('text_a' AS varchar(10))")
                .addRoundTrip("varchar(255)", "'text_b'", createVarcharType(255), "CAST('text_b' AS varchar(255))")
                .addRoundTrip("varchar(255)", "'%s'".formatted("a".repeat(255)), createVarcharType(255), "CAST('%s' AS varchar(255))".formatted("a".repeat(255)))
                .addRoundTrip("varchar(32)", "'攻殻機動隊'", createVarcharType(32), "CAST('攻殻機動隊' AS varchar(32))")
                .addRoundTrip("varchar(10)", "'😂'", createVarcharType(10), "CAST('😂' AS varchar(10))")
                .addRoundTrip("varchar(77)", "'Ну, погоди!'", createVarcharType(77), "CAST('Ну, погоди!' AS varchar(77))")
                .addRoundTrip("varchar(256)", "'text_c'", createVarcharType(256), "CAST('text_c' AS varchar(256))")
                .addRoundTrip("varchar(65535)", "'text_d'", createVarcharType(65535), "CAST('text_d' AS varchar(65535))")
                .addRoundTrip("varchar(65536)", "'text_e'", createVarcharType(65536), "CAST('text_e' AS varchar(65536))")
                .addRoundTrip("varchar(20000)", "'攻殻機動隊'", createVarcharType(20000), "CAST('攻殻機動隊' AS varchar(20000))")
                .addRoundTrip("varchar(1048576)", "'text_f'", createVarcharType(1048576), "CAST('text_f' AS varchar(1048576))")
                .execute(getQueryRunner(), starRocksCreateAndInsert("tpch.test_varchar"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_varchar"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_varchar"));
    }

    @Test
    public void testUnsupportedVarchar()
    {
        assertStarRocksQueryFails("CREATE TABLE tpch.test_unsupported_varchar " + tableDefinition("(x int, y varchar(1048577))", "x"), "Varchar size must be <= 1048576: 1048577");
        assertStarRocksQueryFails("CREATE TABLE tpch.test_unsupported_varchar " + tableDefinition("(x int, y varchar(1048578))", "x"), "Varchar size must be <= 1048576: 1048578");
        assertStarRocksQueryFails("CREATE TABLE tpch.test_unsupported_varchar " + tableDefinition("(x int, y varchar(10485780))", "x"), "Varchar size must be <= 1048576: 10485780");
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
                .execute(getQueryRunner(), starRocksCreateAndInsert("tpch.test_decimal"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_decimal"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_decimal"));
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
                .execute(getQueryRunner(), session, starRocksCreateAndInsert("tpch.test_date"))
                .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_date"))
                .execute(getQueryRunner(), session, trinoCreateAsSelect("test_date"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_date"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert("test_date"));
    }

    @Test
    public void testUnsupportedDateRange()
    {
        testUnsupportedDateRange(UTC);
        testUnsupportedDateRange(ZoneId.systemDefault());
        // no DST in 1970, but has DST in later years (e.g. 2018)
        testUnsupportedDateRange(ZoneId.of("Europe/Vilnius"));
        // minutes offset change since 1970-01-01, no DST
        testUnsupportedDateRange(ZoneId.of("Asia/Kathmandu"));
        testUnsupportedDateRange(TestingSession.DEFAULT_TIME_ZONE_KEY.getZoneId());
    }

    private void testUnsupportedDateRange(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        assertQueryFails("CREATE TABLE test_unsupported_date_range_ctas (data) AS SELECT DATE '-1582-10-05'", "Date must be between 0000-01-01 and 9999-12-31 in StarRocks.*");
        assertUpdate("DROP TABLE IF EXISTS test_unsupported_date_range_ctas");

        ImmutableList<String> unsupportedDateValues = ImmutableList.of("-0001-01-01", "-1000-01-01", "10000-01-01", "19969-12-31");
        try (TestTable table = new TestTable(starRocksServer::execute, "tpch.test_unsupported_date_range", tableDefinition("(id int, data date)", "id"))) {
            for (int i = 0; i < unsupportedDateValues.size(); i++) {
                assertQueryFails(session, format("INSERT INTO %s VALUES(%d, DATE '%s')", table.getName(), i, unsupportedDateValues.get(i)), "Date must be between 0000-01-01 and 9999-12-31 in StarRocks.*");
            }
        }
    }

    @Test
    public void testFloat()
    {
        // we are not testing Nan/-Infinity/+Infinity as those are not supported by StarRocks
        SqlDataTypeTest.create()
                .addRoundTrip("bigint", "123456789012", BIGINT, "123456789012")
                .addRoundTrip("real", "3.14", REAL, "REAL '3.14'")
                .addRoundTrip("real", "10.3e0", REAL, "REAL '10.3e0'")
                .addRoundTrip("real", "NULL", REAL, "CAST(NULL AS REAL)")
                .addRoundTrip("real", "3.1415927", REAL, "REAL '3.1415927'")
                .execute(getQueryRunner(), trinoCreateAsSelect("trino_test_float"))
                .execute(getQueryRunner(), trinoCreateAndInsert("trino_test_float"));

        SqlDataTypeTest.create()
                .addRoundTrip("bigint", "123456789012", BIGINT, "123456789012")
                .addRoundTrip("float", "3.14", REAL, "REAL '3.14'")
                .addRoundTrip("float", "10.3e0", REAL, "REAL '10.3e0'")
                .addRoundTrip("float", "NULL", REAL, "CAST(NULL AS REAL)")
                .addRoundTrip("float", "3.1415927", REAL, "REAL '3.1415927'")
                .execute(getQueryRunner(), starRocksCreateAndInsert("tpch.starrocks_test_float"));
    }

    @Test
    public void testDouble()
    {
        // we are not testing Nan/-Infinity/+Infinity as those are not supported by StarRocks
        SqlDataTypeTest.create()
                .addRoundTrip("bigint", "123456789012", BIGINT, "123456789012")
                .addRoundTrip("double", "1.0E100", DOUBLE, "1.0E100")
                .addRoundTrip("double", "1.23456E12", DOUBLE, "1.23456E12")
                .addRoundTrip("double", "NULL", DOUBLE, "CAST(NULL AS DOUBLE)")
                .execute(getQueryRunner(), trinoCreateAsSelect("trino_test_double"))
                .execute(getQueryRunner(), trinoCreateAndInsert("trino_test_double"))
                .execute(getQueryRunner(), starRocksCreateAndInsert("tpch.starrocks_test_double"));
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

    private DataSetup starRocksCreateAndInsert(String tableNamePrefix)
    {
        return new DorisCreateAndInsertDataSetup(starRocksServer::execute, tableNamePrefix);
    }

    private void assertStarRocksQueryFails(@Language("SQL") String sql, String expectedMessage)
    {
        assertThatThrownBy(() -> starRocksServer.execute(sql))
                .cause()
                .hasMessageContaining(expectedMessage);
    }
}
