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
package io.trino.plugin.hsqldb;

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
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimeWithTimeZoneType.createTimeWithTimeZoneType;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/**
 * @see <a href="https://mariadb.com/kb/en/data-types/">MariaDB data types</a>
 */
@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestHsqlDbTypeMapping
        extends AbstractTestQueryFramework
{
    protected TestingHsqlDbServer server;

    private static final LocalDate EPOCH_DAY = LocalDate.ofEpochDay(0);
    private final ZoneId jvmZone = ZoneId.systemDefault();
    // no DST in 1970, but has DST in later years (e.g. 2018)
    private final ZoneId vilnius = ZoneId.of("Europe/Vilnius");
    // minutes offset change since 1970-01-01, no DST
    private final ZoneId kathmandu = ZoneId.of("Asia/Kathmandu");

    @Language("RegExp")
    private static final String NUMERIC_VALUE_OUT_OF_RANGE = "data exception: numeric value out of range";
    @Language("RegExp")
    private static final String INVALID_DATETIME_FORMAT = "data exception: invalid datetime format";

    @BeforeAll
    public void setUp()
    {
        checkState(jvmZone.getId().equals("America/Bahia_Banderas"), "This test assumes certain JVM time zone");
        checkIsGap(jvmZone, LocalDate.of(1970, 1, 1));
        checkIsGap(vilnius, LocalDate.of(1983, 4, 1));
        verify(vilnius.getRules().getValidOffsets(LocalDate.of(1983, 10, 1).atStartOfDay().minusMinutes(1)).size() == 2);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        server = closeAfterClass(new TestingHsqlDbServer());
        return HsqlDbQueryRunner.builder(server).build();
    }

    @Test
    public void testBoolean()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("BOOLEAN", "TRUE", BOOLEAN, "TRUE")
                .addRoundTrip("BOOLEAN", "FALSE", BOOLEAN, "FALSE")
                .addRoundTrip("BOOLEAN", "NULL", BOOLEAN, "CAST(NULL AS BOOLEAN)")
                .execute(getQueryRunner(), hsqlDbCreateAndInsert("test_boolean"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_boolean"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_boolean"));
    }

    @Test
    public void testTinyInt()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("TINYINT", "-128", TINYINT, "CAST(-128 AS TINYINT)")
                .addRoundTrip("TINYINT", "127", TINYINT, "CAST(127 AS TINYINT)")
                .addRoundTrip("TINYINT", "NULL", TINYINT, "CAST(NULL AS TINYINT)")
                .execute(getQueryRunner(), hsqlDbCreateAndInsert("test_tinyint"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_tinyint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_tinyint"));
    }

    @Test
    public void testUnsupportedTinyInt()
    {
        try (TestTable table = new TestTable(server::execute, "test_unsupported_tinyint", "(data tinyint)")) {
            assertHsqlDbQueryFails(
                    "INSERT INTO " + table.getName() + " VALUES (-129)", // min - 1
                    NUMERIC_VALUE_OUT_OF_RANGE);
            assertHsqlDbQueryFails(
                    "INSERT INTO " + table.getName() + " VALUES (128)", // max + 1
                    NUMERIC_VALUE_OUT_OF_RANGE);
        }
    }

    @Test
    public void testSmallInt()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("SMALLINT", "-32768", SMALLINT, "SMALLINT '-32768'")
                .addRoundTrip("SMALLINT", "32767", SMALLINT, "SMALLINT '32767'")
                .addRoundTrip("SMALLINT", "NULL", SMALLINT, "CAST(NULL AS SMALLINT)")
                .execute(getQueryRunner(), hsqlDbCreateAndInsert("test_smallint"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_smallint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_smallint"));
    }

    @Test
    public void testUnsupportedSmallint()
    {
        try (TestTable table = new TestTable(server::execute, "test_unsupported_smallint", "(data smallint)")) {
            assertHsqlDbQueryFails(
                    "INSERT INTO " + table.getName() + " VALUES (-32769)", // min - 1
                    NUMERIC_VALUE_OUT_OF_RANGE);
            assertHsqlDbQueryFails(
                    "INSERT INTO " + table.getName() + " VALUES (32768)", // max + 1
                    NUMERIC_VALUE_OUT_OF_RANGE);
        }
    }

    @Test
    public void testInteger()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("INTEGER", "-2147483648", INTEGER, "-2147483648")
                .addRoundTrip("INTEGER", "2147483647", INTEGER, "2147483647")
                .addRoundTrip("INTEGER", "NULL", INTEGER, "CAST(NULL AS INTEGER)")
                .execute(getQueryRunner(), hsqlDbCreateAndInsert("test_integer"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_integer"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_integer"));
    }

    @Test
    public void testUnsupportedInteger()
    {
        try (TestTable table = new TestTable(server::execute, "test_unsupported_integer", "(data integer)")) {
            assertHsqlDbQueryFails(
                    "INSERT INTO " + table.getName() + " VALUES (-2147483649)", // min - 1
                    NUMERIC_VALUE_OUT_OF_RANGE);
            assertHsqlDbQueryFails(
                    "INSERT INTO " + table.getName() + " VALUES (2147483648)", // max + 1
                    NUMERIC_VALUE_OUT_OF_RANGE);
        }
    }

    @Test
    public void testInt()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("INT", "-2147483648", INTEGER, "-2147483648")
                .addRoundTrip("INT", "2147483647", INTEGER, "2147483647")
                .addRoundTrip("INT", "NULL", INTEGER, "CAST(NULL AS INT)")
                .execute(getQueryRunner(), hsqlDbCreateAndInsert("test_int"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_int"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_int"));
    }

    @Test
    public void testUnsupportedInt()
    {
        try (TestTable table = new TestTable(server::execute, "test_unsupported_int", "(data int)")) {
            assertHsqlDbQueryFails(
                    "INSERT INTO " + table.getName() + " VALUES (-2147483649)", // min - 1
                    NUMERIC_VALUE_OUT_OF_RANGE);
            assertHsqlDbQueryFails(
                    "INSERT INTO " + table.getName() + " VALUES (2147483648)", // max + 1
                    NUMERIC_VALUE_OUT_OF_RANGE);
        }
    }

    @Test
    public void testBigInt()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("BIGINT", "-9223372036854775808", BIGINT, "-9223372036854775808")
                .addRoundTrip("BIGINT", "9223372036854775807", BIGINT, "9223372036854775807")
                .addRoundTrip("BIGINT", "NULL", BIGINT, "CAST(NULL AS BIGINT)")
                .execute(getQueryRunner(), hsqlDbCreateAndInsert("test_bigint"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_bigint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_bigint"));
    }

    @Test
    public void testUnsupportedBigInt()
    {
        try (TestTable table = new TestTable(server::execute, "test_unsupported_bigint", "(data bigint)")) {
            assertHsqlDbQueryFails(
                    "INSERT INTO " + table.getName() + " VALUES (-9223372036854775809)", // min - 1
                    NUMERIC_VALUE_OUT_OF_RANGE);
            assertHsqlDbQueryFails(
                    "INSERT INTO " + table.getName() + " VALUES (9223372036854775808)", // max + 1
                    NUMERIC_VALUE_OUT_OF_RANGE);
        }
    }

    @Test
    public void testDecimal()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("DECIMAL(3, 0)", "CAST(NULL AS DECIMAL(3, 0))", createDecimalType(3, 0), "CAST(NULL AS DECIMAL(3, 0))")
                .addRoundTrip("DECIMAL(3, 0)", "CAST('193' AS DECIMAL(3, 0))", createDecimalType(3, 0), "CAST('193' AS DECIMAL(3, 0))")
                .addRoundTrip("DECIMAL(3, 0)", "CAST('19' AS DECIMAL(3, 0))", createDecimalType(3, 0), "CAST('19' AS DECIMAL(3, 0))")
                .addRoundTrip("DECIMAL(3, 0)", "CAST('-193' AS DECIMAL(3, 0))", createDecimalType(3, 0), "CAST('-193' AS DECIMAL(3, 0))")
                .addRoundTrip("DECIMAL(3, 1)", "CAST('10.0' AS DECIMAL(3, 1))", createDecimalType(3, 1), "CAST('10.0' AS DECIMAL(3, 1))")
                .addRoundTrip("DECIMAL(3, 1)", "CAST('10.1' AS DECIMAL(3, 1))", createDecimalType(3, 1), "CAST('10.1' AS DECIMAL(3, 1))")
                .addRoundTrip("DECIMAL(3, 1)", "CAST('-10.1' AS DECIMAL(3, 1))", createDecimalType(3, 1), "CAST('-10.1' AS DECIMAL(3, 1))")
                .addRoundTrip("DECIMAL(4, 2)", "CAST('2' AS DECIMAL(4, 2))", createDecimalType(4, 2), "CAST('2' AS DECIMAL(4, 2))")
                .addRoundTrip("DECIMAL(4, 2)", "CAST('2.3' AS DECIMAL(4, 2))", createDecimalType(4, 2), "CAST('2.3' AS DECIMAL(4, 2))")
                .addRoundTrip("DECIMAL(24, 2)", "CAST('2' AS DECIMAL(24, 2))", createDecimalType(24, 2), "CAST('2' AS DECIMAL(24, 2))")
                .addRoundTrip("DECIMAL(24, 2)", "CAST('2.3' AS DECIMAL(24, 2))", createDecimalType(24, 2), "CAST('2.3' AS DECIMAL(24, 2))")
                .addRoundTrip("DECIMAL(24, 2)", "CAST('123456789.3' AS DECIMAL(24, 2))", createDecimalType(24, 2), "CAST('123456789.3' AS DECIMAL(24, 2))")
                .addRoundTrip("DECIMAL(24, 4)", "CAST('12345678901234567890.31' AS DECIMAL(24, 4))", createDecimalType(24, 4), "CAST('12345678901234567890.31' AS DECIMAL(24, 4))")
                .addRoundTrip("DECIMAL(30, 5)", "CAST('3141592653589793238462643.38327' AS DECIMAL(30, 5))", createDecimalType(30, 5), "CAST('3141592653589793238462643.38327' AS DECIMAL(30, 5))")
                .addRoundTrip("DECIMAL(30, 5)", "CAST('-3141592653589793238462643.38327' AS DECIMAL(30, 5))", createDecimalType(30, 5), "CAST('-3141592653589793238462643.38327' AS DECIMAL(30, 5))")
                .addRoundTrip("DECIMAL(38, 0)", "CAST(NULL AS DECIMAL(38, 0))", createDecimalType(38, 0), "CAST(NULL AS DECIMAL(38, 0))")
                .addRoundTrip("DECIMAL(38, 0)", "CAST('27182818284590452353602874713526624977' AS DECIMAL(38, 0))", createDecimalType(38, 0), "CAST('27182818284590452353602874713526624977' AS DECIMAL(38, 0))")
                .addRoundTrip("DECIMAL(38, 0)", "CAST('-27182818284590452353602874713526624977' AS DECIMAL(38, 0))", createDecimalType(38, 0), "CAST('-27182818284590452353602874713526624977' AS DECIMAL(38, 0))")
                .addRoundTrip("DECIMAL(38, 38)", "CAST('0.27182818284590452353602874713526624977' AS DECIMAL(38, 38))", createDecimalType(38, 38), "CAST('0.27182818284590452353602874713526624977' AS DECIMAL(38, 38))")
                .addRoundTrip("DECIMAL(38, 38)", "CAST('-0.27182818284590452353602874713526624977' AS DECIMAL(38, 38))", createDecimalType(38, 38), "CAST('-0.27182818284590452353602874713526624977' AS DECIMAL(38, 38))")
                .execute(getQueryRunner(), hsqlDbCreateAndInsert("test_decimal"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_decimal"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_decimal"));
    }

    @Test
    public void testFloat()
    {
        // we are not testing Nan/-Infinity/+Infinity as those are not supported by MariaDB
        SqlDataTypeTest.create()
                .addRoundTrip("FLOAT", "3.14", DOUBLE, "DOUBLE '3.14'")
                .addRoundTrip("FLOAT", "10.3e0", DOUBLE, "DOUBLE '10.3e0'")
                .addRoundTrip("FLOAT", "NULL", DOUBLE, "CAST(NULL AS DOUBLE)")
                .execute(getQueryRunner(), hsqlDbCreateAndInsert("test_float"));
                //.execute(getQueryRunner(), trinoCreateAsSelect("test_float"))
                //.execute(getQueryRunner(), trinoCreateAndInsert("test_float"));
    }

    @Test
    public void testDouble()
    {
        // we are not testing Nan/-Infinity/+Infinity as those are not supported by MariaDB
        SqlDataTypeTest.create()
                .addRoundTrip("DOUBLE", "3.14", DOUBLE, "CAST(3.14 AS DOUBLE)")
                .addRoundTrip("DOUBLE", "1.0E100", DOUBLE, "1.0E100")
                .addRoundTrip("DOUBLE", "1.23456E12", DOUBLE, "1.23456E12")
                .addRoundTrip("DOUBLE", "NULL", DOUBLE, "CAST(NULL AS DOUBLE)")
                .execute(getQueryRunner(), hsqlDbCreateAndInsert("test_double"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_double"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_double"));
    }

    @Test
    public void testVarchar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("VARCHAR(10)", "'text_a'", createVarcharType(10), "CAST('text_a' AS VARCHAR(10))")
                .addRoundTrip("VARCHAR(255)", "'text_b'", createVarcharType(255), "CAST('text_b' AS VARCHAR(255))")
                .addRoundTrip("VARCHAR(4001)", "'text_c'", createVarcharType(4001), "CAST('text_c' AS VARCHAR(4001))")
                .addRoundTrip("VARCHAR(5)", "CAST('攻殻機動隊' AS VARCHAR(5))", createVarcharType(5), "CAST('攻殻機動隊' AS VARCHAR(5))")
                .addRoundTrip("VARCHAR(32)", "CAST('攻殻機動隊' AS VARCHAR(32))", createVarcharType(32), "CAST('攻殻機動隊' AS VARCHAR(32))")
                .addRoundTrip("VARCHAR(20)", "CAST('😂' AS VARCHAR(20))", createVarcharType(20), "CAST('😂' AS VARCHAR(20))")
                .addRoundTrip("VARCHAR(77)", "CAST('Ну, погоди!' AS VARCHAR(77))", createVarcharType(77), "CAST('Ну, погоди!' AS VARCHAR(77))")
                .execute(getQueryRunner(), hsqlDbCreateAndInsert("test_varchar"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_varchar"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_varchar"));
    }

    @Test
    public void testUnboundedVarchar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("VARCHAR", "'text_a'", createVarcharType(32768), "CAST('text_a' AS VARCHAR(32768))")
                .addRoundTrip("VARCHAR", "'text_b'", createVarcharType(32768), "CAST('text_b' AS VARCHAR(32768))")
                .addRoundTrip("VARCHAR", "'text_d'", createVarcharType(32768), "CAST('text_d' AS VARCHAR(32768))")
                .addRoundTrip("VARCHAR", "'攻殻機動隊'", createVarcharType(32768), "CAST('攻殻機動隊' AS VARCHAR(32768))")
                .addRoundTrip("VARCHAR", "'攻殻機動隊'", createVarcharType(32768), "CAST('攻殻機動隊' AS VARCHAR(32768))")
                .addRoundTrip("VARCHAR", "'攻殻機動隊'", createVarcharType(32768), "CAST('攻殻機動隊' AS VARCHAR(32768))")
                .addRoundTrip("VARCHAR", "'😂'", createVarcharType(32768), "CAST('😂' AS VARCHAR(32768))")
                .addRoundTrip("VARCHAR", "'Ну, погоди!'", createVarcharType(32768), "CAST('Ну, погоди!' AS VARCHAR(32768))")
                .addRoundTrip("VARCHAR", "'text_f'", createVarcharType(32768), "CAST('text_f' AS VARCHAR(32768))")
                .execute(getQueryRunner(), hsqlDbCreateAndInsert("test_unbounded_varchar"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_unbounded_varchar"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_unbounded_varchar"));
    }

    @Test
    public void testCreatedParameterizedVarchar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("varchar(32)", "'e'", createVarcharType(32), "CAST('e' AS VARCHAR(32))")
                .addRoundTrip("varchar(15000)", "'f'", createVarcharType(15000), "CAST('f' AS VARCHAR(15000))")
                .execute(getQueryRunner(), hsqlDbCreateAndInsert("test_parameterized_varchar"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_parameterized_varchar"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_parameterized_varchar"));
    }

    @Test
    public void testCreatedParameterizedVarcharUnicode()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("varchar(5)", "'攻殻機動隊'", createVarcharType(5), "CAST('攻殻機動隊' AS VARCHAR(5))")
                .addRoundTrip("varchar(32)", "'攻殻機動隊'", createVarcharType(32), "CAST('攻殻機動隊' AS VARCHAR(32))")
                .addRoundTrip("varchar(20000)", "'攻殻機動隊'", createVarcharType(20000), "CAST('攻殻機動隊' AS VARCHAR(20000))")
                // FIXME: Why we need to put 2 as maximum length for passing this test (it fails with 1)?
                .addRoundTrip("varchar(2)", "'😂'", createVarcharType(2), "CAST('😂' AS VARCHAR(2))")
                .addRoundTrip("varchar(77)", "'Ну, погоди!'", createVarcharType(77), "CAST('Ну, погоди!' AS VARCHAR(77))")
                .execute(getQueryRunner(), hsqlDbCreateAndInsert("test_parameterized_varchar_unicode"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_parameterized_varchar_unicode"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_parameterized_varchar_unicode"));
    }

    @Test
    public void testParameterizedChar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("char", "''", createCharType(1), "CAST('' AS CHAR(1))")
                .addRoundTrip("char", "'a'", createCharType(1), "CAST('a' AS CHAR(1))")
                .addRoundTrip("char(1)", "''", createCharType(1), "CAST('' AS CHAR(1))")
                .addRoundTrip("char(1)", "'a'", createCharType(1), "CAST('a' AS CHAR(1))")
                .addRoundTrip("char(8)", "'abc'", createCharType(8), "CAST('abc' AS CHAR(8))")
                .addRoundTrip("char(8)", "'12345678'", createCharType(8), "CAST('12345678' AS CHAR(8))")
                .execute(getQueryRunner(), trinoCreateAsSelect("hsqldb_test_parameterized_char"))
                .execute(getQueryRunner(), hsqlDbCreateAndInsert("hsqldb_test_parameterized_char"));
    }

    @Test
    public void testHsqlDbParameterizedCharUnicode()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("char(1)", "'攻'", createCharType(1), "CAST('攻' AS CHAR(1))")
                .addRoundTrip("char(5)", "'攻殻'", createCharType(5), "CAST('攻殻' AS CHAR(5))")
                .addRoundTrip("char(5)", "'攻殻機動隊'", createCharType(5), "CAST('攻殻機動隊' AS CHAR(5))")
                // FIXME: Why we need to put 2 as maximum length for passing this test (it fails with 1)?
                .addRoundTrip("char(2)", "'😂'", createCharType(2), "CAST('😂' AS char(2))")
                .addRoundTrip("char(77)", "'Ну, погоди!'", createCharType(77), "CAST('Ну, погоди!' AS char(77))")
                .execute(getQueryRunner(), hsqlDbCreateAndInsert("hsqldb_test_parameterized_char"));
    }

    @Test
    public void testCharTrailingSpace()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("char(10)", "'test'", createCharType(10), "CAST('test' AS CHAR(10))")
                .addRoundTrip("char(10)", "'test  '", createCharType(10), "CAST('test' AS CHAR(10))")
                .addRoundTrip("char(10)", "'test        '", createCharType(10), "CAST('test' AS CHAR(10))")
                .execute(getQueryRunner(), hsqlDbCreateAndInsert("hsqldb_char_trailing_space"));
    }

    @Test
    public void testVarbinary()
    {
        varbinaryTestCases("varbinary(50)")
                .execute(getQueryRunner(), hsqlDbCreateAndInsert("test_varbinary"));

        //varbinaryTestCases("blob")
        //        .execute(getQueryRunner(), hsqlDbCreateAndInsert("test_varbinary"));

        //varbinaryTestCases("varbinary")
        //        .execute(getQueryRunner(), trinoCreateAsSelect("test_varbinary"));
    }

    private SqlDataTypeTest varbinaryTestCases(String insertType)
    {
        return SqlDataTypeTest.create()
                .addRoundTrip(insertType, "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip(insertType, "X''", VARBINARY, "X''")
                .addRoundTrip(insertType, "X'68656C6C6F'", VARBINARY, "to_utf8('hello')")
                .addRoundTrip(insertType, "X'5069C4996B6E6120C582C4856B61207720E69DB1E4BAACE983BD'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip(insertType, "X'4261672066756C6C206F6620F09F92B0'", VARBINARY, "to_utf8('Bag full of 💰')")
                .addRoundTrip(insertType, "X'0001020304050607080DF9367AA7000000'", VARBINARY, "X'0001020304050607080DF9367AA7000000'") // non-text
                .addRoundTrip(insertType, "X'000000000000'", VARBINARY, "X'000000000000'");
    }

    @Test
    public void testBinary()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("binary(18)", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("binary(18)", "X''", VARBINARY, "X'000000000000000000000000000000000000'")
                .addRoundTrip("binary(18)", "X'68656C6C6F'", VARBINARY, "to_utf8('hello') || X'00000000000000000000000000'")
                .addRoundTrip("binary(18)", "X'C582C4856B61207720E69DB1E4BAACE983BD'", VARBINARY, "to_utf8('łąka w 東京都')") // no trailing zeros
                .addRoundTrip("binary(18)", "X'4261672066756C6C206F6620F09F92B0'", VARBINARY, "to_utf8('Bag full of 💰') || X'0000'")
                .addRoundTrip("binary(18)", "X'0001020304050607080DF9367AA7000000'", VARBINARY, "X'0001020304050607080DF9367AA700000000'") // non-text prefix
                .addRoundTrip("binary(18)", "X'000000000000'", VARBINARY, "X'000000000000000000000000000000000000'")
                .execute(getQueryRunner(), hsqlDbCreateAndInsert("test_binary"));
    }

    /*@Test
    public void testUuid()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("uuid", "CAST ('00000000-0000-0000-0000-000000000000' AS UUID)", UUID)
                .addRoundTrip("uuid", "CAST ('123e4567-e89b-12d3-a456-426655440000' AS UUID)", UUID)
                //.execute(getQueryRunner(), hsqlDbCreateAndInsert("test_uuid"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_uuid"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_uuid"));
    }*/

    @Test
    public void testDate()
    {
        testDate(UTC);
        testDate(jvmZone);
        testDate(vilnius);
        testDate(kathmandu);
        testDate(TestingSession.DEFAULT_TIME_ZONE_KEY.getZoneId());
    }

    private void testDate(@NotNull ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        dateTest(Function.identity())
                .execute(getQueryRunner(), session, hsqlDbCreateAndInsert("test_date"));

        dateTest(inputLiteral -> format("DATE %s", inputLiteral))
                .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_date"))
                .execute(getQueryRunner(), session, trinoCreateAsSelect("test_date"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_date"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert("test_date"));
    }

    private SqlDataTypeTest dateTest(@NotNull Function<String, String> inputLiteralFactory)
    {
        // BC dates not supported by HsqlDB
        return SqlDataTypeTest.create()
                .addRoundTrip("date", "NULL", DATE, "CAST(NULL AS DATE)")
                // min date supported by HsqlDB
                .addRoundTrip("date", inputLiteralFactory.apply("'0001-01-01'"), DATE, "DATE '0001-01-01'")
                .addRoundTrip("date", inputLiteralFactory.apply("'0012-12-12'"), DATE, "DATE '0012-12-12'")
                // before julian->gregorian switch
                .addRoundTrip("date", inputLiteralFactory.apply("'1582-10-04'"), DATE, "DATE '1582-10-04'")
                // after julian->gregorian switch
                .addRoundTrip("date", inputLiteralFactory.apply("'1582-10-15'"), DATE, "DATE '1582-10-15'")
                // before epoch
                .addRoundTrip("date", inputLiteralFactory.apply("'1952-04-03'"), DATE, "DATE '1952-04-03'")
                .addRoundTrip("date", inputLiteralFactory.apply("'1970-01-01'"), DATE, "DATE '1970-01-01'")
                .addRoundTrip("date", inputLiteralFactory.apply("'1970-02-03'"), DATE, "DATE '1970-02-03'")
                // summer on northern hemisphere (possible DST)
                .addRoundTrip("date", inputLiteralFactory.apply("'2017-07-01'"), DATE, "DATE '2017-07-01'")
                // winter on northern hemisphere (possible DST on southern hemisphere)
                .addRoundTrip("date", inputLiteralFactory.apply("'2017-01-01'"), DATE, "DATE '2017-01-01'")
                .addRoundTrip("date", inputLiteralFactory.apply("'1983-04-01'"), DATE, "DATE '1983-04-01'")
                .addRoundTrip("date", inputLiteralFactory.apply("'1983-10-01'"), DATE, "DATE '1983-10-01'")
                // max date supported by HsqlDB
                .addRoundTrip("date", inputLiteralFactory.apply("'9999-12-31'"), DATE, "DATE '9999-12-31'");
    }

    @Test
    public void testUnsupportedDate()
    {
        // HsqlDB does not support negative dates
        String unsupportedMin = "'-0001-01-01'";
        // HsqlDB does not support > 4 digit years
        String unsupportedMax = "'11111-01-01'";
        // HsqlDB does not support dates during julian->gregorian switch
        String startSwitch = "'1582-10-05'";
        String middleSwitch = "'1582-10-10'";
        String endSwitch = "'1582-10-14'";
        String tableName = "test_date_unsupported" + randomNameSuffix();
        assertUpdate(format("CREATE TABLE %s (test_date date)", tableName));
        try {
            assertQueryFails(format("INSERT INTO %s VALUES (date %s)", tableName, unsupportedMin),
                    INVALID_DATETIME_FORMAT);
            assertQueryFails(format("INSERT INTO %s VALUES (date %s)", tableName, unsupportedMax),
                    INVALID_DATETIME_FORMAT);
            assertQueryFails(format("INSERT INTO %s VALUES (date %s)", tableName, startSwitch),
                    INVALID_DATETIME_FORMAT);
            assertQueryFails(format("INSERT INTO %s VALUES (date %s)", tableName, middleSwitch),
                    INVALID_DATETIME_FORMAT);
            assertQueryFails(format("INSERT INTO %s VALUES (date %s)", tableName, endSwitch),
                    INVALID_DATETIME_FORMAT);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    public void testTime()
    {
        testTime(UTC);
        testTime(jvmZone);
        testTime(vilnius);
        testTime(kathmandu);
        testDate(TestingSession.DEFAULT_TIME_ZONE_KEY.getZoneId());
    }

    private void testTime(@NotNull ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        commonTimeTest(Function.identity())
                .execute(getQueryRunner(), session, hsqlDbCreateAndInsert("test_time"));

        trinoTimeTest(inputLiteral -> format("TIME %s", inputLiteral))
                .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_time"))
                .execute(getQueryRunner(), session, trinoCreateAsSelect("test_time"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_time"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert("test_time"));
    }

    private SqlDataTypeTest commonTimeTest(@NotNull Function<String, String> inputLiteralFactory)
    {
        return SqlDataTypeTest.create()
                .addRoundTrip("time(0)", inputLiteralFactory.apply("'00:00:00'"), createTimeType(0), "TIME '00:00:00'")
                .addRoundTrip("time(9)", inputLiteralFactory.apply("'00:00:00.000000000'"), createTimeType(9), "TIME '00:00:00.000000000'")
                .addRoundTrip("time(9)", inputLiteralFactory.apply("'00:00:00.123456789'"), createTimeType(9), "TIME '00:00:00.123456789'")
                .addRoundTrip("time(0)", inputLiteralFactory.apply("'12:34:56'"), createTimeType(0), "TIME '12:34:56'")
                .addRoundTrip("time(9)", inputLiteralFactory.apply("'12:34:56.123456789'"), createTimeType(9), "TIME '12:34:56.123456789'")

                // maximal value for a precision
                .addRoundTrip("time(0)", inputLiteralFactory.apply("'23:59:59'"), createTimeType(0), "TIME '23:59:59'")
                .addRoundTrip("time(1)", inputLiteralFactory.apply("'23:59:59.9'"), createTimeType(1), "TIME '23:59:59.9'")
                .addRoundTrip("time(2)", inputLiteralFactory.apply("'23:59:59.99'"), createTimeType(2), "TIME '23:59:59.99'")
                .addRoundTrip("time(3)", inputLiteralFactory.apply("'23:59:59.999'"), createTimeType(3), "TIME '23:59:59.999'")
                .addRoundTrip("time(4)", inputLiteralFactory.apply("'23:59:59.9999'"), createTimeType(4), "TIME '23:59:59.9999'")
                .addRoundTrip("time(5)", inputLiteralFactory.apply("'23:59:59.99999'"), createTimeType(5), "TIME '23:59:59.99999'")
                .addRoundTrip("time(6)", inputLiteralFactory.apply("'23:59:59.999999'"), createTimeType(6), "TIME '23:59:59.999999'")
                .addRoundTrip("time(7)", inputLiteralFactory.apply("'23:59:59.9999999'"), createTimeType(7), "TIME '23:59:59.9999999'")
                .addRoundTrip("time(8)", inputLiteralFactory.apply("'23:59:59.99999999'"), createTimeType(8), "TIME '23:59:59.99999999'")
                .addRoundTrip("time(9)", inputLiteralFactory.apply("'23:59:59.999999999'"), createTimeType(9), "TIME '23:59:59.999999999'")

                .addRoundTrip("time(0)", inputLiteralFactory.apply("'00:00:00'"), createTimeType(0), "TIME '00:00:00'")
                .addRoundTrip("time(9)", inputLiteralFactory.apply("'00:00:00.000000000'"), createTimeType(9), "TIME '00:00:00.000000000'")
                .addRoundTrip("time(9)", inputLiteralFactory.apply("'00:00:00.123456789'"), createTimeType(9), "TIME '00:00:00.123456789'")
                .addRoundTrip("time(0)", inputLiteralFactory.apply("'12:34:56'"), createTimeType(0), "TIME '12:34:56'")
                .addRoundTrip("time(9)", inputLiteralFactory.apply("'12:34:56.123456789'"), createTimeType(9), "TIME '12:34:56.123456789'")

                // maximal value for a precision
                .addRoundTrip("time(0)", inputLiteralFactory.apply("'23:59:59'"), createTimeType(0), "TIME '23:59:59'")
                .addRoundTrip("time(1)", inputLiteralFactory.apply("'23:59:59.9'"), createTimeType(1), "TIME '23:59:59.9'")
                .addRoundTrip("time(2)", inputLiteralFactory.apply("'23:59:59.99'"), createTimeType(2), "TIME '23:59:59.99'")
                .addRoundTrip("time(3)", inputLiteralFactory.apply("'23:59:59.999'"), createTimeType(3), "TIME '23:59:59.999'")
                .addRoundTrip("time(4)", inputLiteralFactory.apply("'23:59:59.9999'"), createTimeType(4), "TIME '23:59:59.9999'")
                .addRoundTrip("time(5)", inputLiteralFactory.apply("'23:59:59.99999'"), createTimeType(5), "TIME '23:59:59.99999'")
                .addRoundTrip("time(6)", inputLiteralFactory.apply("'23:59:59.999999'"), createTimeType(6), "TIME '23:59:59.999999'")
                .addRoundTrip("time(7)", inputLiteralFactory.apply("'23:59:59.9999999'"), createTimeType(7), "TIME '23:59:59.9999999'")
                .addRoundTrip("time(8)", inputLiteralFactory.apply("'23:59:59.99999999'"), createTimeType(8), "TIME '23:59:59.99999999'")
                .addRoundTrip("time(9)", inputLiteralFactory.apply("'23:59:59.999999999'"), createTimeType(9), "TIME '23:59:59.999999999'");
    }

    private SqlDataTypeTest trinoTimeTest(Function<String, String> inputLiteralFactory)
    {
        return commonTimeTest(inputLiteralFactory)
                // round down
                .addRoundTrip(inputLiteralFactory.apply("'00:00:00.0000000001'"), "TIME '00:00:00.000000000'")
                .addRoundTrip(inputLiteralFactory.apply("'00:00:00.000000000001'"), "TIME '00:00:00.000000000'")

                // round down, maximal value
                .addRoundTrip(inputLiteralFactory.apply("'00:00:00.0000000004'"), "TIME '00:00:00.000000000'")
                .addRoundTrip(inputLiteralFactory.apply("'00:00:00.00000000049'"), "TIME '00:00:00.000000000'")
                .addRoundTrip(inputLiteralFactory.apply("'00:00:00.000000000449'"), "TIME '00:00:00.000000000'")

                // round up to next day, minimal value
                .addRoundTrip(inputLiteralFactory.apply("'23:59:59.9999999995'"), "TIME '00:00:00.000000000'")
                .addRoundTrip(inputLiteralFactory.apply("'23:59:59.99999999950'"), "TIME '00:00:00.000000000'")
                .addRoundTrip(inputLiteralFactory.apply("'23:59:59.999999999500'"), "TIME '00:00:00.000000000'")

                // round up to next day, maximal value
                .addRoundTrip(inputLiteralFactory.apply("'23:59:59.9999999999'"), "TIME '00:00:00.000000000'")
                .addRoundTrip(inputLiteralFactory.apply("'23:59:59.99999999999'"), "TIME '00:00:00.000000000'")
                .addRoundTrip(inputLiteralFactory.apply("'23:59:59.999999999999'"), "TIME '00:00:00.000000000'")

                // round down
                .addRoundTrip(inputLiteralFactory.apply("'23:59:59.999999999499'"), "TIME '23:59:59.999999999'");
    }

    @Test
    public void testUnsupportedTime()
    {
        // HsqlDB does not support negative hours
        String unsupportedNegativeHour = "'-01:00:00'";
        // HsqlDB does not support negative minutes
        String unsupportedNegativeMinute = "'00:-01:00'";
        // HsqlDB does not support negative second
        String unsupportedNegativeSecond = "'00:00:-01'";
        // HsqlDB does not support > 23 digit hours
        String unsupportedHour = "'24:00:00'";
        // HsqlDB does not support > 59 digit minutes
        String unsupportedMinute = "'00:60:00'";
        // HsqlDB does not support > 59 digit seconds
        String unsupportedSecond = "'00:00:60'";
        String tableName = "test_time_unsupported" + randomNameSuffix();
        String expectedMessage = "line 1:53: %s is not a valid TIME literal";
        assertUpdate(format("CREATE TABLE %s (test_time time)", tableName));
        try {
            assertQueryFails(format("INSERT INTO %s VALUES (time %s)", tableName, unsupportedNegativeHour),
                    format(expectedMessage, unsupportedNegativeHour));
            assertQueryFails(format("INSERT INTO %s VALUES (time %s)", tableName, unsupportedNegativeMinute),
                    format(expectedMessage, unsupportedNegativeMinute));
            assertQueryFails(format("INSERT INTO %s VALUES (time %s)", tableName, unsupportedNegativeSecond),
                    format(expectedMessage, unsupportedNegativeSecond));
            assertQueryFails(format("INSERT INTO %s VALUES (time %s)", tableName, unsupportedHour),
                    format(expectedMessage, unsupportedHour));
            assertQueryFails(format("INSERT INTO %s VALUES (time %s)", tableName, unsupportedMinute),
                    format(expectedMessage, unsupportedMinute));
            assertQueryFails(format("INSERT INTO %s VALUES (time %s)", tableName, unsupportedSecond),
                    format(expectedMessage, unsupportedSecond));
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    public void testTimeWithTimeZone()
    {
        testTimeWithTimeZone(UTC);
        testTimeWithTimeZone(jvmZone);
        testTimeWithTimeZone(vilnius);
        testTimeWithTimeZone(kathmandu);
        testTimeWithTimeZone(TestingSession.DEFAULT_TIME_ZONE_KEY.getZoneId());
    }

    private void testTimeWithTimeZone(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        commonTimeWithTimeZoneTest(inputLiteral -> format("TIME %s", inputLiteral))
                .execute(getQueryRunner(), session, hsqlDbCreateAndInsert("test_time_with_zone"));

        trinoTimeWithTimeZoneTest(inputLiteral -> format("TIME %s", inputLiteral))
                .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_time_with_zone"))
                .execute(getQueryRunner(), session, trinoCreateAsSelect("test_time_with_zone"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_time_with_zone"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert("test_time_with_zone"));
    }

    private SqlDataTypeTest commonTimeWithTimeZoneTest(Function<String, String> inputLiteralFactory)
    {
        return SqlDataTypeTest.create()
                .addRoundTrip("TIME(0) WITH TIME ZONE", inputLiteralFactory.apply("'20:08:08-08:00'"), createTimeWithTimeZoneType(0))
                .addRoundTrip("TIME(1) WITH TIME ZONE", inputLiteralFactory.apply("'20:08:08.1-08:00'"), createTimeWithTimeZoneType(1))
                .addRoundTrip("TIME(2) WITH TIME ZONE", inputLiteralFactory.apply("'20:08:08.12-08:00'"), createTimeWithTimeZoneType(2))
                .addRoundTrip("TIME(3) WITH TIME ZONE", inputLiteralFactory.apply("'13:29:38.123-01:00'"), createTimeWithTimeZoneType(3))
                .addRoundTrip("TIME(4) WITH TIME ZONE", inputLiteralFactory.apply("'13:29:38.1234-01:00'"), createTimeWithTimeZoneType(4))
                .addRoundTrip("TIME(5) WITH TIME ZONE", inputLiteralFactory.apply("'13:29:38.12345+02:00'"), createTimeWithTimeZoneType(5))
                .addRoundTrip("TIME(6) WITH TIME ZONE", inputLiteralFactory.apply("'13:29:38.123456+02:00'"), createTimeWithTimeZoneType(6))
                .addRoundTrip("TIME(7) WITH TIME ZONE", inputLiteralFactory.apply("'13:29:38.1234567+02:00'"), createTimeWithTimeZoneType(7))
                .addRoundTrip("TIME(8) WITH TIME ZONE", inputLiteralFactory.apply("'13:29:38.12345678+02:00'"), createTimeWithTimeZoneType(8))
                .addRoundTrip("TIME(9) WITH TIME ZONE", inputLiteralFactory.apply("'13:29:38.123456789+02:00'"), createTimeWithTimeZoneType(9));
    }

    private SqlDataTypeTest trinoTimeWithTimeZoneTest(Function<String, String> inputLiteralFactory)
    {
        return commonTimeWithTimeZoneTest(inputLiteralFactory)
                // round down
                .addRoundTrip(inputLiteralFactory.apply("'00:00:00.0000000001'"), "TIME '00:00:00.000000000'")
                .addRoundTrip(inputLiteralFactory.apply("'00:00:00.000000000001'"), "TIME '00:00:00.000000000'")

                // round down, maximal value
                .addRoundTrip(inputLiteralFactory.apply("'00:00:00.0000000004'"), "TIME '00:00:00.000000000'")
                .addRoundTrip(inputLiteralFactory.apply("'00:00:00.00000000049'"), "TIME '00:00:00.000000000'")
                .addRoundTrip(inputLiteralFactory.apply("'00:00:00.000000000449'"), "TIME '00:00:00.000000000'")

                // round up to next day, minimal value
                .addRoundTrip(inputLiteralFactory.apply("'23:59:59.9999999995'"), "TIME '00:00:00.000000000'")
                .addRoundTrip(inputLiteralFactory.apply("'23:59:59.99999999950'"), "TIME '00:00:00.000000000'")
                .addRoundTrip(inputLiteralFactory.apply("'23:59:59.999999999500'"), "TIME '00:00:00.000000000'")

                // round up to next day, maximal value
                .addRoundTrip(inputLiteralFactory.apply("'23:59:59.9999999999'"), "TIME '00:00:00.000000000'")
                .addRoundTrip(inputLiteralFactory.apply("'23:59:59.99999999999'"), "TIME '00:00:00.000000000'")
                .addRoundTrip(inputLiteralFactory.apply("'23:59:59.999999999999'"), "TIME '00:00:00.000000000'")

                // round down
                .addRoundTrip(inputLiteralFactory.apply("'23:59:59.999999999499'"), "TIME '23:59:59.999999999'");
    }

    @Test
    public void testUnsupportedTimeWithTimeZone()
    {
        // HsqlDB does not support negative hours
        String unsupportedNegativeHour = "'-01:00:00-8:00'";
        // HsqlDB does not support > 23 digit hours
        String unsupportedHour = "'24:00:00-4:00'";
        // HsqlDB does not support > 59 digit seconds
        String unsupportedSecond = "'00:00:60-2:00'";
        String tableName = "test_time_with_zone_unsupported" + randomNameSuffix();
        String expectedMessage = "line 1:63: %s is not a valid TIME literal";
        assertUpdate(format("CREATE TABLE %s (test_time time with time zone)", tableName));
        try {
            assertQueryFails(format("INSERT INTO %s VALUES (time %s)", tableName, unsupportedNegativeHour),
                    format(expectedMessage, unsupportedNegativeHour));
            assertQueryFails(format("INSERT INTO %s VALUES (time %s)", tableName, unsupportedHour),
                    format(expectedMessage, unsupportedHour));
            assertQueryFails(format("INSERT INTO %s VALUES (time %s)", tableName, unsupportedSecond),
                    format(expectedMessage, unsupportedSecond));
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    public void testTimestamp()
    {
        testTimestamp(UTC);
        testTimestamp(ZoneId.systemDefault());
        // using two non-JVM zones so that we don't need to worry what SQL Server system zone is
        // no DST in 1970, but has DST in later years (e.g. 2018)
        testTimestamp(ZoneId.of("Europe/Vilnius"));
        // minutes offset change since 1970-01-01, no DST
        testTimestamp(ZoneId.of("Asia/Kathmandu"));
        testTimestamp(TestingSession.DEFAULT_TIME_ZONE_KEY.getZoneId());
    }

    private void testTimestamp(ZoneId sessionZone)
    {
        SqlDataTypeTest tests = SqlDataTypeTest.create()

                // before epoch
                .addRoundTrip("TIMESTAMP(3)", "TIMESTAMP '1958-01-01 13:18:03.123'", createTimestampType(3))
                // after epoch
                .addRoundTrip("TIMESTAMP(3)", "TIMESTAMP '2019-03-18 10:01:17.987'", createTimestampType(3))
                // time doubled in JVM zone
                .addRoundTrip("TIMESTAMP(3)", "TIMESTAMP '2018-10-28 01:33:17.456'", createTimestampType(3))
                // time double in Vilnius
                .addRoundTrip("TIMESTAMP(3)", "TIMESTAMP '2018-10-28 03:33:33.333'", createTimestampType(3))
                // epoch
                .addRoundTrip("TIMESTAMP(3)", "TIMESTAMP '1970-01-01 00:00:00.000'", createTimestampType(3))
                // time gap in JVM zone
                .addRoundTrip("TIMESTAMP(3)", "TIMESTAMP '1970-01-01 00:13:42.000'", createTimestampType(3))
                .addRoundTrip("TIMESTAMP(3)", "TIMESTAMP '2018-04-01 02:13:55.123'", createTimestampType(3))
                // time gap in Vilnius
                .addRoundTrip("TIMESTAMP(3)", "TIMESTAMP '2018-03-25 03:17:17.000'", createTimestampType(3))
                // time gap in Kathmandu
                .addRoundTrip("TIMESTAMP(3)", "TIMESTAMP '1986-01-01 00:13:07.000'", createTimestampType(3))

                // same as above but with higher precision
                .addRoundTrip("TIMESTAMP(9)", "TIMESTAMP '1958-01-01 13:18:03.123000000'", createTimestampType(9))
                .addRoundTrip("TIMESTAMP(9)", "TIMESTAMP '2019-03-18 10:01:17.987000000'", createTimestampType(9))
                .addRoundTrip("TIMESTAMP(9)", "TIMESTAMP '2018-10-28 01:33:17.456000000'", createTimestampType(9))
                .addRoundTrip("TIMESTAMP(9)", "TIMESTAMP '2018-10-28 03:33:33.333000000'", createTimestampType(9))
                .addRoundTrip("TIMESTAMP(9)", "TIMESTAMP '1970-01-01 00:00:00.000000000'", createTimestampType(9))
                .addRoundTrip("TIMESTAMP(9)", "TIMESTAMP '1970-01-01 00:13:42.000000000'", createTimestampType(9))
                .addRoundTrip("TIMESTAMP(9)", "TIMESTAMP '2018-04-01 02:13:55.123000000'", createTimestampType(9))
                .addRoundTrip("TIMESTAMP(9)", "TIMESTAMP '2018-03-25 03:17:17.000000000'", createTimestampType(9))
                .addRoundTrip("TIMESTAMP(9)", "TIMESTAMP '1986-01-01 00:13:07.000000000'", createTimestampType(9))

                // test arbitrary time for all supported precisions
                .addRoundTrip("TIMESTAMP(0)", "TIMESTAMP '1970-01-01 00:00:00'", createTimestampType(0))
                .addRoundTrip("TIMESTAMP(1)", "TIMESTAMP '1970-01-01 00:00:00.1'", createTimestampType(1))
                .addRoundTrip("TIMESTAMP(2)", "TIMESTAMP '1970-01-01 00:00:00.12'", createTimestampType(2))
                .addRoundTrip("TIMESTAMP(3)", "TIMESTAMP '1970-01-01 00:00:00.123'", createTimestampType(3))
                .addRoundTrip("TIMESTAMP(4)", "TIMESTAMP '1970-01-01 00:00:00.1234'", createTimestampType(4))
                .addRoundTrip("TIMESTAMP(5)", "TIMESTAMP '1970-01-01 00:00:00.12345'", createTimestampType(5))
                .addRoundTrip("TIMESTAMP(6)", "TIMESTAMP '1970-01-01 00:00:00.123456'", createTimestampType(6))
                .addRoundTrip("TIMESTAMP(7)", "TIMESTAMP '1970-01-01 00:00:00.1234567'", createTimestampType(7))
                .addRoundTrip("TIMESTAMP(8)", "TIMESTAMP '1970-01-01 00:00:00.12345678'", createTimestampType(8))
                .addRoundTrip("TIMESTAMP(9)", "TIMESTAMP '1970-01-01 00:00:00.123456789'", createTimestampType(9))
                .addRoundTrip("TIMESTAMP(9)", "TIMESTAMP '1970-01-01 00:00:00.1234567890'", createTimestampType(9), "TIMESTAMP '1970-01-01 00:00:00.123456789'")
                .addRoundTrip("TIMESTAMP(9)", "TIMESTAMP '1970-01-01 00:00:00.123456789499'", createTimestampType(9), "TIMESTAMP '1970-01-01 00:00:00.123456789'")
                .addRoundTrip("TIMESTAMP(9)", "TIMESTAMP '1970-01-01 00:00:00.1234567895'", createTimestampType(9), "TIMESTAMP '1970-01-01 00:00:00.123456790'")
                .addRoundTrip("TIMESTAMP(9)", "TIMESTAMP '1970-01-01 00:00:00.1234567899'", createTimestampType(9), "TIMESTAMP '1970-01-01 00:00:00.123456790'")

                // before epoch with second fraction
                .addRoundTrip("TIMESTAMP(9)", "TIMESTAMP '1969-12-31 23:59:59.123000000'", createTimestampType(9))
                .addRoundTrip("TIMESTAMP(9)", "TIMESTAMP '1969-12-31 23:59:59.123456789'", createTimestampType(9))

                // precision 0 ends up as precision 0
                .addRoundTrip("TIMESTAMP(0)", "TIMESTAMP '1970-01-01 00:00:00'", createTimestampType(0))

                .addRoundTrip("TIMESTAMP(1)", "TIMESTAMP '1970-01-01 00:00:00.1'", createTimestampType(1))
                .addRoundTrip("TIMESTAMP(1)", "TIMESTAMP '1970-01-01 00:00:00.9'", createTimestampType(1))
                .addRoundTrip("TIMESTAMP(3)", "TIMESTAMP '1970-01-01 00:00:00.123'", createTimestampType(3))
                .addRoundTrip("TIMESTAMP(6)", "TIMESTAMP '1970-01-01 00:00:00.123000'", createTimestampType(6))
                .addRoundTrip("TIMESTAMP(3)", "TIMESTAMP '1970-01-01 00:00:00.999'", createTimestampType(3))
                // max supported precision
                .addRoundTrip("TIMESTAMP(9)", "TIMESTAMP '1970-01-01 00:00:00.123456789'", createTimestampType(9), "TIMESTAMP '1970-01-01 00:00:00.123456789'")

                .addRoundTrip("TIMESTAMP(1)", "TIMESTAMP '2020-09-27 12:34:56.1'", createTimestampType(1))
                .addRoundTrip("TIMESTAMP(1)", "TIMESTAMP '2020-09-27 12:34:56.9'", createTimestampType(1))
                .addRoundTrip("TIMESTAMP(3)", "TIMESTAMP '2020-09-27 12:34:56.123'", createTimestampType(3))
                .addRoundTrip("TIMESTAMP(6)", "TIMESTAMP '2020-09-27 12:34:56.123000'", createTimestampType(6))
                .addRoundTrip("TIMESTAMP(6)", "TIMESTAMP '2020-09-27 12:34:56.999999'", createTimestampType(6))
                // max supported precision
                .addRoundTrip("TIMESTAMP(9)", "TIMESTAMP '2020-09-27 12:34:56.123456789'", createTimestampType(9))

                // round down
                .addRoundTrip("TIMESTAMP(9)", "TIMESTAMP '1970-01-01 00:00:00.1234567891'", createTimestampType(9), "TIMESTAMP '1970-01-01 00:00:00.123456789'")

                // nanos round up, end result rounds down
                .addRoundTrip("TIMESTAMP(9)", "TIMESTAMP '1970-01-01 00:00:00.12345678949'", createTimestampType(9), "TIMESTAMP '1970-01-01 00:00:00.123456789'")
                .addRoundTrip("TIMESTAMP(9)", "TIMESTAMP '1970-01-01 00:00:00.123456789499'", createTimestampType(9), "TIMESTAMP '1970-01-01 00:00:00.123456789'")

                // round up
                .addRoundTrip("TIMESTAMP(9)", "TIMESTAMP '1970-01-01 00:00:00.1234567895'", createTimestampType(9), "TIMESTAMP '1970-01-01 00:00:00.123456790'")

                // max precision
                .addRoundTrip("TIMESTAMP(9)", "TIMESTAMP '1970-01-01 00:00:00.111222333444'", createTimestampType(9), "TIMESTAMP '1970-01-01 00:00:00.111222333'")

                // round up to next second
                .addRoundTrip("TIMESTAMP(9)", "TIMESTAMP '1970-01-01 00:00:00.9999999995'", createTimestampType(9), "TIMESTAMP '1970-01-01 00:00:01.000000000'")

                // round up to next day
                .addRoundTrip("TIMESTAMP(9)", "TIMESTAMP '1970-01-01 23:59:59.9999999995'", createTimestampType(9), "TIMESTAMP '1970-01-02 00:00:00.000000000'")

                // negative epoch
                .addRoundTrip("TIMESTAMP(9)", "TIMESTAMP '1969-12-31 23:59:59.9999999995'", createTimestampType(9), "TIMESTAMP '1970-01-01 00:00:00.000000000'")
                .addRoundTrip("TIMESTAMP(9)", "TIMESTAMP '1969-12-31 23:59:59.999999999499'", createTimestampType(9), "TIMESTAMP '1969-12-31 23:59:59.999999999'")
                .addRoundTrip("TIMESTAMP(9)", "TIMESTAMP '1969-12-31 23:59:59.9999999994'", createTimestampType(9), "TIMESTAMP '1969-12-31 23:59:59.999999999'");

        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        tests.execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_timestamp"));
        tests.execute(getQueryRunner(), session, trinoCreateAsSelect("test_timestamp"));
        tests.execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_timestamp"));
        tests.execute(getQueryRunner(), session, trinoCreateAndInsert("test_timestamp"));
    }

    @Test
    public void testHsqlDbTimestamp()
    {
        SqlDataTypeTest.create()
                // literal values with higher precision are NOT rounded and cause an error
                .addRoundTrip("TIMESTAMP(0)", "'1970-01-01 00:00:00'", createTimestampType(0), "TIMESTAMP '1970-01-01 00:00:00'")
                .addRoundTrip("TIMESTAMP(1)", "'1970-01-01 00:00:00.1'", createTimestampType(1), "TIMESTAMP '1970-01-01 00:00:00.1'")
                .addRoundTrip("TIMESTAMP(1)", "'1970-01-01 00:00:00.9'", createTimestampType(1), "TIMESTAMP '1970-01-01 00:00:00.9'")
                .addRoundTrip("TIMESTAMP(3)", "'1970-01-01 00:00:00.123'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.123'")
                .addRoundTrip("TIMESTAMP(6)", "'1970-01-01 00:00:00.123000'", createTimestampType(6), "TIMESTAMP '1970-01-01 00:00:00.123000'")
                .addRoundTrip("TIMESTAMP(3)", "'1970-01-01 00:00:00.999'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.999'")
                .addRoundTrip("TIMESTAMP(7)", "'1970-01-01 00:00:00.1234567'", createTimestampType(7), "TIMESTAMP '1970-01-01 00:00:00.1234567'")
                .addRoundTrip("TIMESTAMP(1)", "'2020-09-27 12:34:56.1'", createTimestampType(1), "TIMESTAMP '2020-09-27 12:34:56.1'")
                .addRoundTrip("TIMESTAMP(1)", "'2020-09-27 12:34:56.9'", createTimestampType(1), "TIMESTAMP '2020-09-27 12:34:56.9'")
                .addRoundTrip("TIMESTAMP(3)", "'2020-09-27 12:34:56.123'", createTimestampType(3), "TIMESTAMP '2020-09-27 12:34:56.123'")
                .addRoundTrip("TIMESTAMP(6)", "'2020-09-27 12:34:56.123000'", createTimestampType(6), "TIMESTAMP '2020-09-27 12:34:56.123000'")
                .addRoundTrip("TIMESTAMP(3)", "'2020-09-27 12:34:56.999'", createTimestampType(3), "TIMESTAMP '2020-09-27 12:34:56.999'")
                .addRoundTrip("TIMESTAMP(7)", "'2020-09-27 12:34:56.1234567'", createTimestampType(7), "TIMESTAMP '2020-09-27 12:34:56.1234567'")

                .addRoundTrip("TIMESTAMP(7)", "'1970-01-01 00:00:00'", createTimestampType(7), "TIMESTAMP '1970-01-01 00:00:00.0000000'")
                .addRoundTrip("TIMESTAMP(7)", "'1970-01-01 00:00:00.1'", createTimestampType(7), "TIMESTAMP '1970-01-01 00:00:00.1000000'")
                .addRoundTrip("TIMESTAMP(7)", "'1970-01-01 00:00:00.9'", createTimestampType(7), "TIMESTAMP '1970-01-01 00:00:00.9000000'")
                .addRoundTrip("TIMESTAMP(7)", "'1970-01-01 00:00:00.123'", createTimestampType(7), "TIMESTAMP '1970-01-01 00:00:00.1230000'")
                .addRoundTrip("TIMESTAMP(7)", "'1970-01-01 00:00:00.123000'", createTimestampType(7), "TIMESTAMP '1970-01-01 00:00:00.1230000'")
                .addRoundTrip("TIMESTAMP(7)", "'1970-01-01 00:00:00.999'", createTimestampType(7), "TIMESTAMP '1970-01-01 00:00:00.9990000'")
                .addRoundTrip("TIMESTAMP(7)", "'1970-01-01 00:00:00.1234567'", createTimestampType(7), "TIMESTAMP '1970-01-01 00:00:00.1234567'")
                .addRoundTrip("TIMESTAMP(7)", "'2020-09-27 12:34:56.1'", createTimestampType(7), "TIMESTAMP '2020-09-27 12:34:56.1000000'")
                .addRoundTrip("TIMESTAMP(7)", "'2020-09-27 12:34:56.9'", createTimestampType(7), "TIMESTAMP '2020-09-27 12:34:56.9000000'")
                .addRoundTrip("TIMESTAMP(7)", "'2020-09-27 12:34:56.123'", createTimestampType(7), "TIMESTAMP '2020-09-27 12:34:56.1230000'")
                .addRoundTrip("TIMESTAMP(7)", "'2020-09-27 12:34:56.123000'", createTimestampType(7), "TIMESTAMP '2020-09-27 12:34:56.1230000'")
                .addRoundTrip("TIMESTAMP(7)", "'2020-09-27 12:34:56.999'", createTimestampType(7), "TIMESTAMP '2020-09-27 12:34:56.9990000'")
                .addRoundTrip("TIMESTAMP(7)", "'2020-09-27 12:34:56.1234567'", createTimestampType(7), "TIMESTAMP '2020-09-27 12:34:56.1234567'")

                .execute(getQueryRunner(), hsqlDbCreateAndInsert("test_hsqldb_timestamp"));
    }

    @Test
    public void testTimestampWithZone()
    {
        testTimestamp(UTC);
        testTimestamp(ZoneId.systemDefault());
        // no DST in 1970, but has DST in later years (e.g. 2018)
        testTimestamp(ZoneId.of("Europe/Vilnius"));
        // minutes offset change since 1970-01-01, no DST
        testTimestamp(ZoneId.of("Asia/Kathmandu"));
        testTimestamp(TestingSession.DEFAULT_TIME_ZONE_KEY.getZoneId());
    }

    private void testTimestampWithZone(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        SqlDataTypeTest.create()
                // FIXME: Cant run there tests!!!
                //.addRoundTrip("timestamp '-290307-12-31 23:59:59.999'", "timestamp '-290307-12-31 23:59:59.999'") // min value
                .addRoundTrip("timestamp '1582-10-04 23:59:59.999+8:00'", "timestamp '1582-10-04 23:59:59.999+8:00'") // before julian->gregorian switch
                .addRoundTrip("timestamp '1582-10-05 00:00:00.000+8:00'", "timestamp '1582-10-05 00:00:00.000+8:00'") // begin julian->gregorian switch
                .addRoundTrip("timestamp '1582-10-14 23:59:59.999+8:00'", "timestamp '1582-10-14 23:59:59.999+8:00'") // end julian->gregorian switch
                .addRoundTrip("timestamp '1970-01-01 00:00:00.000+8:00'", "timestamp '1970-01-01 00:00:00.000+8:00'") // epoch
                .addRoundTrip("timestamp '1986-01-01 00:13:07.123+8:00'", "timestamp '1986-01-01 00:13:07.123+8:00'") // time gap in Kathmandu
                .addRoundTrip("timestamp '2018-03-25 03:17:17.123+8:00'", "timestamp '2018-03-25 03:17:17.123+8:00'") // time gap in Vilnius
                .addRoundTrip("timestamp '2018-10-28 01:33:17.456+8:00'", "timestamp '2018-10-28 01:33:17.456+8:00'") // time doubled in JVM zone
                .addRoundTrip("timestamp '2018-10-28 03:33:33.333+8:00'", "timestamp '2018-10-28 03:33:33.333+8:00'") // time double in Vilnius
                //.addRoundTrip("timestamp '294247-01-10 04:00:54.775'", "timestamp '294247-01-10 04:00:54.775'") // max value

                .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_timestamp"))
                .execute(getQueryRunner(), session, trinoCreateAsSelect("test_timestamp"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_timestamp"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert("test_timestamp"));
    }

    @Test
    public void testIncorrectTimestamp()
    {
        // XXX: The Timestamp supported range is '1000-01-01' to '9999-12-31'
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_incorrect_timestamp", "(dt TIMESTAMP)")) {
            assertHsqlDbQueryFails(
                    "INSERT INTO " + table.getName() + " VALUES (TIMESTAMP '999-01-01 00:00:00.000')",
                    INVALID_DATETIME_FORMAT);
            assertHsqlDbQueryFails(
                    "INSERT INTO " + table.getName() + " VALUES (TIMESTAMP '10000-12-31 03:14:08.000')",
                    "data exception: datetime field overflow");
        }
    }

    @Test
    public void testTimestampCoercion()
    {
        SqlDataTypeTest.create()

                // precision 0 ends up as precision 0
                .addRoundTrip("timestamp '1970-01-01 00:00:00'", "timestamp '1970-01-01 00:00:00'")

                .addRoundTrip("timestamp '1970-01-01 00:00:00.1'", "timestamp '1970-01-01 00:00:00.1'")
                .addRoundTrip("timestamp '1970-01-01 00:00:00.9'", "timestamp '1970-01-01 00:00:00.9'")
                .addRoundTrip("timestamp '1970-01-01 00:00:00.123'", "timestamp '1970-01-01 00:00:00.123'")
                .addRoundTrip("timestamp '1970-01-01 00:00:00.123000'", "timestamp '1970-01-01 00:00:00.123000'")
                .addRoundTrip("timestamp '1970-01-01 00:00:00.999'", "timestamp '1970-01-01 00:00:00.999'")
                // max supported precision
                .addRoundTrip("timestamp '1970-01-01 00:00:00.123456789'", "timestamp '1970-01-01 00:00:00.123456789'")

                .addRoundTrip("timestamp '2020-09-27 12:34:56.1'", "timestamp '2020-09-27 12:34:56.1'")
                .addRoundTrip("timestamp '2020-09-27 12:34:56.9'", "timestamp '2020-09-27 12:34:56.9'")
                .addRoundTrip("timestamp '2020-09-27 12:34:56.123'", "timestamp '2020-09-27 12:34:56.123'")
                .addRoundTrip("timestamp '2020-09-27 12:34:56.123000'", "timestamp '2020-09-27 12:34:56.123000'")
                .addRoundTrip("timestamp '2020-09-27 12:34:56.999'", "timestamp '2020-09-27 12:34:56.999'")
                // max supported precision
                .addRoundTrip("timestamp '2020-09-27 12:34:56.123456789'", "timestamp '2020-09-27 12:34:56.123456789'")

                // round down
                .addRoundTrip("TIMESTAMP(9)", "timestamp '1970-01-01 00:00:00.1234567894'", createTimestampType(9), "timestamp '1970-01-01 00:00:00.123456789'")

                // nanoc round up, end result rounds down
                .addRoundTrip("TIMESTAMP(9)", "timestamp '1970-01-01 00:00:00.1234567899'", createTimestampType(9), "timestamp '1970-01-01 00:00:00.123456790'")
                .addRoundTrip("TIMESTAMP(9)", "timestamp '1970-01-01 00:00:00.123456789999'", createTimestampType(9), "timestamp '1970-01-01 00:00:00.123456790'")

                // round up
                .addRoundTrip("TIMESTAMP(9)", "timestamp '1970-01-01 00:00:00.1234567895'", createTimestampType(9), "timestamp '1970-01-01 00:00:00.123456790'")

                // max precision
                .addRoundTrip("TIMESTAMP(9)", "timestamp '1970-01-01 00:00:00.111222333444'", createTimestampType(9), "timestamp '1970-01-01 00:00:00.111222333'")

                // round up to next second
                .addRoundTrip("TIMESTAMP(9)", "timestamp '1970-01-01 00:00:00.9999999995'", createTimestampType(9), "timestamp '1970-01-01 00:00:01.000000000'")

                // round up to next day
                .addRoundTrip("TIMESTAMP(9)", "timestamp '1970-01-01 23:59:59.9999999995'", createTimestampType(9), "timestamp '1970-01-02 00:00:00.000000000'")

                // negative epoch
                .addRoundTrip("TIMESTAMP(9)", "timestamp '1969-12-31 23:59:59.9999999995'", createTimestampType(9), "timestamp '1970-01-01 00:00:00.000000000'")
                .addRoundTrip("TIMESTAMP(9)", "timestamp '1969-12-31 23:59:59.999999999499'", createTimestampType(9), "timestamp '1969-12-31 23:59:59.999999999'")
                .addRoundTrip("TIMESTAMP(9)", "timestamp '1969-12-31 23:59:59.9999999994'", createTimestampType(9), "timestamp '1969-12-31 23:59:59.999999999'")

                .execute(getQueryRunner(), trinoCreateAsSelect("test_timestamp_coercion"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_timestamp_coercion"));
    }

    private SqlDataTypeTest timestampRoundTrips(String inputType)
    {
        return SqlDataTypeTest.create()
                // after epoch (MariaDb's timestamp type doesn't support values <= epoch)
                .addRoundTrip(inputType + "(3)", "TIMESTAMP '2019-03-18 10:01:17.987'", createTimestampType(3), "TIMESTAMP '2019-03-18 10:01:17.987'")
                // time doubled in JVM zone
                .addRoundTrip(inputType + "(3)", "TIMESTAMP '2018-10-28 01:33:17.456'", createTimestampType(3), "TIMESTAMP '2018-10-28 01:33:17.456'")
                // time double in Vilnius
                .addRoundTrip(inputType + "(3)", "TIMESTAMP '2018-10-28 03:33:33.333'", createTimestampType(3), "TIMESTAMP '2018-10-28 03:33:33.333'")
                .addRoundTrip(inputType + "(3)", "TIMESTAMP '1970-01-01 00:13:42.000'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:13:42.000'")
                .addRoundTrip(inputType + "(3)", "TIMESTAMP '2018-04-01 02:13:55.123'", createTimestampType(3), "TIMESTAMP '2018-04-01 02:13:55.123'")
                // time gap in Vilnius
                .addRoundTrip(inputType + "(3)", "TIMESTAMP '2018-03-25 03:17:17.000'", createTimestampType(3), "TIMESTAMP '2018-03-25 03:17:17.000'")
                // time gap in Kathmandu
                .addRoundTrip(inputType + "(3)", "TIMESTAMP '1986-01-01 00:13:07.000'", createTimestampType(3), "TIMESTAMP '1986-01-01 00:13:07.000'")
                // same as above but with higher precision
                .addRoundTrip(inputType + "(6)", "TIMESTAMP '2019-03-18 10:01:17.987654'", createTimestampType(6), "TIMESTAMP '2019-03-18 10:01:17.987654'")
                .addRoundTrip(inputType + "(6)", "TIMESTAMP '2018-10-28 01:33:17.456789'", createTimestampType(6), "TIMESTAMP '2018-10-28 01:33:17.456789'")
                .addRoundTrip(inputType + "(6)", "TIMESTAMP '2018-10-28 03:33:33.333333'", createTimestampType(6), "TIMESTAMP '2018-10-28 03:33:33.333333'")
                .addRoundTrip(inputType + "(6)", "TIMESTAMP '1970-01-01 00:13:42.000000'", createTimestampType(6), "TIMESTAMP '1970-01-01 00:13:42.000000'")
                .addRoundTrip(inputType + "(6)", "TIMESTAMP '2018-04-01 02:13:55.123456'", createTimestampType(6), "TIMESTAMP '2018-04-01 02:13:55.123456'")
                .addRoundTrip(inputType + "(6)", "TIMESTAMP '2018-03-25 03:17:17.000000'", createTimestampType(6), "TIMESTAMP '2018-03-25 03:17:17.000000'")
                .addRoundTrip(inputType + "(6)", "TIMESTAMP '1986-01-01 00:13:07.000000'", createTimestampType(6), "TIMESTAMP '1986-01-01 00:13:07.000000'")

                // test arbitrary time for all supported precisions
                .addRoundTrip(inputType + "(0)", "TIMESTAMP '1970-01-01 00:00:01'", createTimestampType(0), "TIMESTAMP '1970-01-01 00:00:01'")
                .addRoundTrip(inputType + "(1)", "TIMESTAMP '1970-01-01 00:00:01.1'", createTimestampType(1), "TIMESTAMP '1970-01-01 00:00:01.1'")
                .addRoundTrip(inputType + "(1)", "TIMESTAMP '1970-01-01 00:00:01.9'", createTimestampType(1), "TIMESTAMP '1970-01-01 00:00:01.9'")
                .addRoundTrip(inputType + "(2)", "TIMESTAMP '1970-01-01 00:00:01.12'", createTimestampType(2), "TIMESTAMP '1970-01-01 00:00:01.12'")
                .addRoundTrip(inputType + "(3)", "TIMESTAMP '1970-01-01 00:00:01.123'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:01.123'")
                .addRoundTrip(inputType + "(3)", "TIMESTAMP '1970-01-01 00:00:01.999'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:01.999'")
                .addRoundTrip(inputType + "(4)", "TIMESTAMP '1970-01-01 00:00:01.1234'", createTimestampType(4), "TIMESTAMP '1970-01-01 00:00:01.1234'")
                .addRoundTrip(inputType + "(5)", "TIMESTAMP '1970-01-01 00:00:01.12345'", createTimestampType(5), "TIMESTAMP '1970-01-01 00:00:01.12345'")
                .addRoundTrip(inputType + "(1)", "TIMESTAMP '2020-09-27 12:34:56.1'", createTimestampType(1), "TIMESTAMP '2020-09-27 12:34:56.1'")
                .addRoundTrip(inputType + "(1)", "TIMESTAMP '2020-09-27 12:34:56.9'", createTimestampType(1), "TIMESTAMP '2020-09-27 12:34:56.9'")
                .addRoundTrip(inputType + "(3)", "TIMESTAMP '2020-09-27 12:34:56.123'", createTimestampType(3), "TIMESTAMP '2020-09-27 12:34:56.123'")
                .addRoundTrip(inputType + "(3)", "TIMESTAMP '2020-09-27 12:34:56.999'", createTimestampType(3), "TIMESTAMP '2020-09-27 12:34:56.999'")
                .addRoundTrip(inputType + "(6)", "TIMESTAMP '2020-09-27 12:34:56.123456'", createTimestampType(6), "TIMESTAMP '2020-09-27 12:34:56.123456'")
                // null
                .addRoundTrip(inputType + "(0)", "NULL", createTimestampType(0), "CAST(NULL AS TIMESTAMP(0))")
                .addRoundTrip(inputType + "(1)", "NULL", createTimestampType(1), "CAST(NULL AS TIMESTAMP(1))")
                .addRoundTrip(inputType + "(2)", "NULL", createTimestampType(2), "CAST(NULL AS TIMESTAMP(2))")
                .addRoundTrip(inputType + "(3)", "NULL", createTimestampType(3), "CAST(NULL AS TIMESTAMP(3))")
                .addRoundTrip(inputType + "(4)", "NULL", createTimestampType(4), "CAST(NULL AS TIMESTAMP(4))")
                .addRoundTrip(inputType + "(5)", "NULL", createTimestampType(5), "CAST(NULL AS TIMESTAMP(5))")
                .addRoundTrip(inputType + "(6)", "NULL", createTimestampType(6), "CAST(NULL AS TIMESTAMP(6))");
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

    private DataSetup hsqlDbCreateAndInsert(String tableNamePrefix)
    {
        return new HsqlDbCreateAndInsertDataSetup(server::execute, tableNamePrefix);
    }

    private static boolean isGap(ZoneId zone, LocalDateTime dateTime)
    {
        return zone.getRules().getValidOffsets(dateTime).isEmpty();
    }

    private static void checkIsGap(ZoneId zone, LocalDateTime dateTime)
    {
        verify(isGap(zone, dateTime), "Expected %s to be a gap in %s", dateTime, zone);
    }

    private static void checkIsGap(ZoneId zone, LocalDate date)
    {
        verify(isGap(zone, date), "Expected %s to be a gap in %s", date, zone);
    }

    private static boolean isGap(ZoneId zone, LocalDate date)
    {
        return zone.getRules().getValidOffsets(date.atStartOfDay()).isEmpty();
    }

    private void assertHsqlDbQueryFails(@Language("SQL") String sql, String expectedMessage)
    {
        assertThatThrownBy(() -> server.execute(sql))
                .cause()
                .hasMessageContaining(expectedMessage);
    }
}
