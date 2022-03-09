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
package io.trino.plugin.mysql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.jdbc.UnsupportedTypeHandling;
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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.ZoneId;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.jdbc.DecimalConfig.DecimalMapping.ALLOW_OVERFLOW;
import static io.trino.plugin.jdbc.DecimalConfig.DecimalMapping.STRICT;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.DECIMAL_DEFAULT_SCALE;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.DECIMAL_MAPPING;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.DECIMAL_ROUNDING_MODE;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.UNSUPPORTED_TYPE_HANDLING;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.plugin.mysql.MySqlQueryRunner.createMySqlQueryRunner;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.type.JsonType.JSON;
import static java.lang.String.format;
import static java.math.RoundingMode.HALF_UP;
import static java.math.RoundingMode.UNNECESSARY;
import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestMySqlTypeMapping
        extends AbstractTestQueryFramework
{
    private TestingMySqlServer mySqlServer;

    private final ZoneId jvmZone = ZoneId.systemDefault();
    // no DST in 1970, but has DST in later years (e.g. 2018)
    private final ZoneId vilnius = ZoneId.of("Europe/Vilnius");

    @BeforeClass
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
        mySqlServer = closeAfterClass(new TestingMySqlServer());
        return createMySqlQueryRunner(mySqlServer, ImmutableMap.of(), ImmutableMap.of(), ImmutableList.of());
    }

    @Test
    public void testBit()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("bit", "b'1'", BOOLEAN, "true")
                .addRoundTrip("bit", "b'0'", BOOLEAN, "false")
                .addRoundTrip("bit", "NULL", BOOLEAN, "CAST(NULL AS BOOLEAN)")
                .execute(getQueryRunner(), mysqlCreateAndInsert("tpch.test_bit"));
    }

    @Test
    public void testBoolean()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("boolean", "true", TINYINT, "TINYINT '1'")
                .addRoundTrip("boolean", "false", TINYINT, "TINYINT '0'")
                .addRoundTrip("boolean", "NULL", TINYINT, "CAST(NULL AS TINYINT)")
                .execute(getQueryRunner(), mysqlCreateAndInsert("tpch.test_boolean"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_boolean"));

        SqlDataTypeTest.create()
                .addRoundTrip("boolean", "1", TINYINT, "TINYINT '1'")
                .addRoundTrip("boolean", "0", TINYINT, "TINYINT '0'")
                .addRoundTrip("boolean", "NULL", TINYINT, "CAST(NULL AS TINYINT)")
                .execute(getQueryRunner(), trinoCreateAndInsert("test_boolean"));
    }

    @Test
    public void testTinyint()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("tinyint", "NULL", TINYINT, "CAST(NULL AS TINYINT)")
                .addRoundTrip("tinyint", "-128", TINYINT, "TINYINT '-128'") // min value in MySQL and Trino
                .addRoundTrip("tinyint", "5", TINYINT, "TINYINT '5'")
                .addRoundTrip("tinyint", "127", TINYINT, "TINYINT '127'") // max value in MySQL and Trino
                .execute(getQueryRunner(), mysqlCreateAndInsert("tpch.test_tinyint"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_tinyint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_tinyint"));
    }

    @Test
    public void testUnsupportedTinyint()
    {
        try (TestTable table = new TestTable(mySqlServer::execute, "tpch.test_unsupported_tinyint", "(data tinyint)")) {
            assertMySqlQueryFails(
                    format("INSERT INTO %s VALUES (-129)", table.getName()), // min - 1
                    "Data truncation: Out of range value for column 'data' at row 1");
            assertMySqlQueryFails(
                    format("INSERT INTO %s VALUES (128)", table.getName()), // max + 1
                    "Data truncation: Out of range value for column 'data' at row 1");
        }
    }

    @Test
    public void testSmallint()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("smallint", "NULL", SMALLINT, "CAST(NULL AS SMALLINT)")
                .addRoundTrip("smallint", "-32768", SMALLINT, "SMALLINT '-32768'") // min value in MySQL and Trino
                .addRoundTrip("smallint", "32456", SMALLINT, "SMALLINT '32456'")
                .addRoundTrip("smallint", "32767", SMALLINT, "SMALLINT '32767'") // max value in MySQL and Trino
                .execute(getQueryRunner(), mysqlCreateAndInsert("tpch.test_smallint"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_smallint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_smallint"));
    }

    @Test
    public void testUnsupportedSmallint()
    {
        try (TestTable table = new TestTable(mySqlServer::execute, "tpch.test_unsupported_smallint", "(data smallint)")) {
            assertMySqlQueryFails(
                    format("INSERT INTO %s VALUES (-32769)", table.getName()), // min - 1
                    "Data truncation: Out of range value for column 'data' at row 1");
            assertMySqlQueryFails(
                    format("INSERT INTO %s VALUES (32768)", table.getName()), // max + 1
                    "Data truncation: Out of range value for column 'data' at row 1");
        }
    }

    @Test
    public void testInteger()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("integer", "NULL", INTEGER, "CAST(NULL AS INTEGER)")
                .addRoundTrip("integer", "-2147483648", INTEGER, "-2147483648") // min value in MySQL and Trino
                .addRoundTrip("integer", "1234567890", INTEGER, "1234567890")
                .addRoundTrip("integer", "2147483647", INTEGER, "2147483647") // max value in MySQL and Trino
                .execute(getQueryRunner(), mysqlCreateAndInsert("tpch.test_int"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_int"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_int"));
    }

    @Test
    public void testUnsupportedInteger()
    {
        try (TestTable table = new TestTable(mySqlServer::execute, "tpch.test_unsupported_integer", "(data integer)")) {
            assertMySqlQueryFails(
                    format("INSERT INTO %s VALUES (-2147483649)", table.getName()), // min - 1
                    "Data truncation: Out of range value for column 'data' at row 1");
            assertMySqlQueryFails(
                    format("INSERT INTO %s VALUES (2147483648)", table.getName()), // max + 1
                    "Data truncation: Out of range value for column 'data' at row 1");
        }
    }

    @Test
    public void testBigint()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("bigint", "NULL", BIGINT, "CAST(NULL AS BIGINT)")
                .addRoundTrip("bigint", "-9223372036854775808", BIGINT, "-9223372036854775808") // min value in MySQL and Trino
                .addRoundTrip("bigint", "123456789012", BIGINT, "123456789012")
                .addRoundTrip("bigint", "9223372036854775807", BIGINT, "9223372036854775807") // max value in MySQL and Trino
                .execute(getQueryRunner(), mysqlCreateAndInsert("tpch.test_bigint"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_bigint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_bigint"));
    }

    @Test
    public void testUnsupportedBigint()
    {
        try (TestTable table = new TestTable(mySqlServer::execute, "tpch.test_unsupported_bigint", "(data bigint)")) {
            assertMySqlQueryFails(
                    format("INSERT INTO %s VALUES (-9223372036854775809)", table.getName()), // min - 1
                    "Data truncation: Out of range value for column 'data' at row 1");
            assertMySqlQueryFails(
                    format("INSERT INTO %s VALUES (9223372036854775808)", table.getName()), // max + 1
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
    public void testMySqlCreatedParameterizedVarchar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("tinytext", "'a'", createVarcharType(255), "CAST('a' AS varchar(255))")
                .addRoundTrip("text", "'b'", createVarcharType(65535), "CAST('b' AS varchar(65535))")
                .addRoundTrip("mediumtext", "'c'", createVarcharType(16777215), "CAST('c' AS varchar(16777215))")
                .addRoundTrip("longtext", "'d'", createUnboundedVarcharType(), "CAST('d' AS varchar)")
                .addRoundTrip("varchar(32)", "'e'", createVarcharType(32), "CAST('e' AS varchar(32))")
                .addRoundTrip("varchar(15000)", "'f'", createVarcharType(15000), "CAST('f' AS varchar(15000))")
                .execute(getQueryRunner(), mysqlCreateAndInsert("tpch.mysql_test_parameterized_varchar"));
    }

    @Test
    public void testMySqlCreatedParameterizedVarcharUnicode()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("tinytext CHARACTER SET utf8", "'攻殻機動隊'", createVarcharType(255), "CAST('攻殻機動隊' AS varchar(255))")
                .addRoundTrip("text CHARACTER SET utf8", "'攻殻機動隊'", createVarcharType(65535), "CAST('攻殻機動隊' AS varchar(65535))")
                .addRoundTrip("mediumtext CHARACTER SET utf8", "'攻殻機動隊'", createVarcharType(16777215), "CAST('攻殻機動隊' AS varchar(16777215))")
                .addRoundTrip("longtext CHARACTER SET utf8", "'攻殻機動隊'", createUnboundedVarcharType(), "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("varchar(5) CHARACTER SET utf8", "'攻殻機動隊'", createVarcharType(5), "CAST('攻殻機動隊' AS varchar(5))")
                .addRoundTrip("varchar(32) CHARACTER SET utf8", "'攻殻機動隊'", createVarcharType(32), "CAST('攻殻機動隊' AS varchar(32))")
                .addRoundTrip("varchar(20000) CHARACTER SET utf8", "'攻殻機動隊'", createVarcharType(20000), "CAST('攻殻機動隊' AS varchar(20000))")
                .execute(getQueryRunner(), mysqlCreateAndInsert("tpch.mysql_test_parameterized_varchar_unicode"));
    }

    @Test
    public void testParameterizedChar()
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
                .execute(getQueryRunner(), trinoCreateAsSelect("mysql_test_parameterized_char"))
                .execute(getQueryRunner(), trinoCreateAndInsert("mysql_test_parameterized_char"))
                .execute(getQueryRunner(), mysqlCreateAndInsert("tpch.mysql_test_parameterized_char"));
    }

    @Test
    public void testMySqlCreatedParameterizedCharUnicode()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("char(1) CHARACTER SET utf8", "'攻'", createCharType(1), "CAST('攻' AS char(1))")
                .addRoundTrip("char(5) CHARACTER SET utf8", "'攻殻'", createCharType(5), "CAST('攻殻' AS char(5))")
                .addRoundTrip("char(5) CHARACTER SET utf8", "'攻殻機動隊'", createCharType(5), "CAST('攻殻機動隊' AS char(5))")
                .execute(getQueryRunner(), mysqlCreateAndInsert("tpch.mysql_test_parameterized_varchar"));
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
                .execute(getQueryRunner(), mysqlCreateAndInsert("tpch.test_decimal"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_decimal"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_decimal"));
    }

    @Test
    public void testDecimalExceedingPrecisionMax()
    {
        testUnsupportedDataType("decimal(50,0)");
    }

    @Test
    public void testDecimalExceedingPrecisionMaxWithExceedingIntegerValues()
    {
        try (TestTable testTable = new TestTable(
                mySqlServer::execute,
                "tpch.test_exceeding_max_decimal",
                "(d_col decimal(65,25))",
                asList("1234567890123456789012345678901234567890.123456789", "-1234567890123456789012345678901234567890.123456789"))) {
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 0),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_schema||'.'||table_name = '%s'", testTable.getName()),
                    "VALUES ('d_col', 'decimal(38,0)')");
            assertQueryFails(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 0),
                    "SELECT d_col FROM " + testTable.getName(),
                    "Rounding necessary");
            assertQueryFails(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 0),
                    "SELECT d_col FROM " + testTable.getName(),
                    "Decimal overflow");
            assertQuery(
                    sessionWithDecimalMappingStrict(CONVERT_TO_VARCHAR),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_schema||'.'||table_name = '%s'", testTable.getName()),
                    "VALUES ('d_col', 'varchar')");
            assertQuery(
                    sessionWithDecimalMappingStrict(CONVERT_TO_VARCHAR),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES ('1234567890123456789012345678901234567890.1234567890000000000000000'), ('-1234567890123456789012345678901234567890.1234567890000000000000000')");
        }
    }

    @Test
    public void testDecimalExceedingPrecisionMaxWithNonExceedingIntegerValues()
    {
        try (TestTable testTable = new TestTable(
                mySqlServer::execute,
                "tpch.test_exceeding_max_decimal",
                "(d_col decimal(60,20))",
                asList("123456789012345678901234567890.123456789012345", "-123456789012345678901234567890.123456789012345"))) {
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 0),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_schema||'.'||table_name = '%s'", testTable.getName()),
                    "VALUES ('d_col', 'decimal(38,0)')");
            assertQueryFails(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 0),
                    "SELECT d_col FROM " + testTable.getName(),
                    "Rounding necessary");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 0),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES (123456789012345678901234567890), (-123456789012345678901234567890)");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 8),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_schema||'.'||table_name = '%s'", testTable.getName()),
                    "VALUES ('d_col', 'decimal(38,8)')");
            assertQueryFails(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 8),
                    "SELECT d_col FROM " + testTable.getName(),
                    "Rounding necessary");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 8),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES (123456789012345678901234567890.12345679), (-123456789012345678901234567890.12345679)");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 22),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_schema||'.'||table_name = '%s'", testTable.getName()),
                    "VALUES ('d_col', 'decimal(38,20)')");
            assertQueryFails(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 20),
                    "SELECT d_col FROM " + testTable.getName(),
                    "Decimal overflow");
            assertQueryFails(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 9),
                    "SELECT d_col FROM " + testTable.getName(),
                    "Decimal overflow");
            assertQuery(
                    sessionWithDecimalMappingStrict(CONVERT_TO_VARCHAR),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_schema||'.'||table_name = '%s'", testTable.getName()),
                    "VALUES ('d_col', 'varchar')");
            assertQuery(
                    sessionWithDecimalMappingStrict(CONVERT_TO_VARCHAR),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES ('123456789012345678901234567890.12345678901234500000'), ('-123456789012345678901234567890.12345678901234500000')");
        }
    }

    @Test(dataProvider = "testDecimalExceedingPrecisionMaxProvider")
    public void testDecimalExceedingPrecisionMaxWithSupportedValues(int typePrecision, int typeScale)
    {
        try (TestTable testTable = new TestTable(
                mySqlServer::execute,
                "tpch.test_exceeding_max_decimal",
                format("(d_col decimal(%d,%d))", typePrecision, typeScale),
                asList("12.01", "-12.01", "123", "-123", "1.12345678", "-1.12345678"))) {
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 0),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_schema||'.'||table_name = '%s'", testTable.getName()),
                    "VALUES ('d_col', 'decimal(38,0)')");
            assertQueryFails(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 0),
                    "SELECT d_col FROM " + testTable.getName(),
                    "Rounding necessary");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 0),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES (12), (-12), (123), (-123), (1), (-1)");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 3),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_schema||'.'||table_name = '%s'", testTable.getName()),
                    "VALUES ('d_col', 'decimal(38,3)')");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 3),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES (12.01), (-12.01), (123), (-123), (1.123), (-1.123)");
            assertQueryFails(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 3),
                    "SELECT d_col FROM " + testTable.getName(),
                    "Rounding necessary");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 8),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_schema||'.'||table_name = '%s'", testTable.getName()),
                    "VALUES ('d_col', 'decimal(38,8)')");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 8),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES (12.01), (-12.01), (123), (-123), (1.12345678), (-1.12345678)");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 9),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES (12.01), (-12.01), (123), (-123), (1.12345678), (-1.12345678)");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 8),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES (12.01), (-12.01), (123), (-123), (1.12345678), (-1.12345678)");
        }
    }

    @DataProvider
    public Object[][] testDecimalExceedingPrecisionMaxProvider()
    {
        return new Object[][] {
                {40, 8},
                {50, 10},
        };
    }

    private Session sessionWithDecimalMappingAllowOverflow(RoundingMode roundingMode, int scale)
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("mysql", DECIMAL_MAPPING, ALLOW_OVERFLOW.name())
                .setCatalogSessionProperty("mysql", DECIMAL_ROUNDING_MODE, roundingMode.name())
                .setCatalogSessionProperty("mysql", DECIMAL_DEFAULT_SCALE, Integer.valueOf(scale).toString())
                .build();
    }

    private Session sessionWithDecimalMappingStrict(UnsupportedTypeHandling unsupportedTypeHandling)
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("mysql", DECIMAL_MAPPING, STRICT.name())
                .setCatalogSessionProperty("mysql", UNSUPPORTED_TYPE_HANDLING, unsupportedTypeHandling.name())
                .build();
    }

    @Test
    public void testVarbinary()
    {
        varbinaryTestCases("varbinary(50)")
                .execute(getQueryRunner(), mysqlCreateAndInsert("tpch.test_varbinary"));

        varbinaryTestCases("tinyblob")
                .execute(getQueryRunner(), mysqlCreateAndInsert("tpch.test_varbinary"));

        varbinaryTestCases("blob")
                .execute(getQueryRunner(), mysqlCreateAndInsert("tpch.test_varbinary"));

        varbinaryTestCases("mediumblob")
                .execute(getQueryRunner(), mysqlCreateAndInsert("tpch.test_varbinary"));

        varbinaryTestCases("longblob")
                .execute(getQueryRunner(), mysqlCreateAndInsert("tpch.test_varbinary"));

        varbinaryTestCases("varbinary")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_varbinary"));
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
                .execute(getQueryRunner(), mysqlCreateAndInsert("tpch.test_binary"));
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    public void testDate(ZoneId sessionZone)
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
                .execute(getQueryRunner(), session, mysqlCreateAndInsert("tpch.test_date"))
                .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_date"))
                .execute(getQueryRunner(), session, trinoCreateAsSelect("test_date"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_date"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert("test_date"));
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    public void testTimeFromMySql(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        SqlDataTypeTest.create()
                // default precision in MySQL is 0
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
                .execute(getQueryRunner(), session, mysqlCreateAndInsert("tpch.test_time"));
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    public void testTimeFromTrino(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        SqlDataTypeTest.create()
                // default precision in Trino is 3
                .addRoundTrip("TIME", "TIME '00:00:00'", createTimeType(3), "TIME '00:00:00.000'")
                .addRoundTrip("TIME", "TIME '12:34:56.123'", createTimeType(3), "TIME '12:34:56.123'")
                .addRoundTrip("TIME", "TIME '23:59:59.999'", createTimeType(3), "TIME '23:59:59.999'")

                // maximal value for a precision
                .addRoundTrip("TIME", "TIME '23:59:59'", createTimeType(3), "TIME '23:59:59.000'")
                .addRoundTrip("TIME(1)", "TIME '23:59:59.9'", createTimeType(1), "TIME '23:59:59.9'")
                .addRoundTrip("TIME(2)", "TIME '23:59:59.99'", createTimeType(2), "TIME '23:59:59.99'")
                .addRoundTrip("TIME(3)", "TIME '23:59:59.999'", createTimeType(3), "TIME '23:59:59.999'")
                .addRoundTrip("TIME(4)", "TIME '23:59:59.9999'", createTimeType(4), "TIME '23:59:59.9999'")
                .addRoundTrip("TIME(5)", "TIME '23:59:59.99999'", createTimeType(5), "TIME '23:59:59.99999'")
                .addRoundTrip("TIME(6)", "TIME '23:59:59.999999'", createTimeType(6), "TIME '23:59:59.999999'")

                // supported precisions
                .addRoundTrip("TIME '23:59:59.9'", "TIME '23:59:59.9'")
                .addRoundTrip("TIME '23:59:59.99'", "TIME '23:59:59.99'")
                .addRoundTrip("TIME '23:59:59.999'", "TIME '23:59:59.999'")
                .addRoundTrip("TIME '23:59:59.9999'", "TIME '23:59:59.9999'")
                .addRoundTrip("TIME '23:59:59.99999'", "TIME '23:59:59.99999'")
                .addRoundTrip("TIME '23:59:59.999999'", "TIME '23:59:59.999999'")

                // round down
                .addRoundTrip("TIME '00:00:00.0000001'", "TIME '00:00:00.000000'")
                .addRoundTrip("TIME '00:00:00.000000000001'", "TIME '00:00:00.000000'")
                .addRoundTrip("TIME '12:34:56.1234561'", "TIME '12:34:56.123456'")
                .addRoundTrip("TIME '23:59:59.9999994'", "TIME '23:59:59.999999'")
                .addRoundTrip("TIME '23:59:59.999999499999'", "TIME '23:59:59.999999'")

                // round down, maximal value
                .addRoundTrip("TIME '00:00:00.0000004'", "TIME '00:00:00.000000'")
                .addRoundTrip("TIME '00:00:00.00000049'", "TIME '00:00:00.000000'")
                .addRoundTrip("TIME '00:00:00.000000449'", "TIME '00:00:00.000000'")
                .addRoundTrip("TIME '00:00:00.0000004449'", "TIME '00:00:00.000000'")
                .addRoundTrip("TIME '00:00:00.00000044449'", "TIME '00:00:00.000000'")
                .addRoundTrip("TIME '00:00:00.000000444449'", "TIME '00:00:00.000000'")

                // round up, minimal value
                .addRoundTrip("TIME '00:00:00.0000005'", "TIME '00:00:00.000001'")
                .addRoundTrip("TIME '00:00:00.00000050'", "TIME '00:00:00.000001'")
                .addRoundTrip("TIME '00:00:00.000000500'", "TIME '00:00:00.000001'")
                .addRoundTrip("TIME '00:00:00.0000005000'", "TIME '00:00:00.000001'")
                .addRoundTrip("TIME '00:00:00.00000050000'", "TIME '00:00:00.000001'")
                .addRoundTrip("TIME '00:00:00.000000500000'", "TIME '00:00:00.000001'")

                // round up, maximal value
                .addRoundTrip("TIME '00:00:00.0000009'", "TIME '00:00:00.000001'")
                .addRoundTrip("TIME '00:00:00.00000099'", "TIME '00:00:00.000001'")
                .addRoundTrip("TIME '00:00:00.000000999'", "TIME '00:00:00.000001'")
                .addRoundTrip("TIME '00:00:00.0000009999'", "TIME '00:00:00.000001'")
                .addRoundTrip("TIME '00:00:00.00000099999'", "TIME '00:00:00.000001'")
                .addRoundTrip("TIME '00:00:00.000000999999'", "TIME '00:00:00.000001'")

                // round up to next day, minimal value
                .addRoundTrip("TIME '23:59:59.9999995'", "TIME '00:00:00.000000'")
                .addRoundTrip("TIME '23:59:59.99999950'", "TIME '00:00:00.000000'")
                .addRoundTrip("TIME '23:59:59.999999500'", "TIME '00:00:00.000000'")
                .addRoundTrip("TIME '23:59:59.9999995000'", "TIME '00:00:00.000000'")
                .addRoundTrip("TIME '23:59:59.99999950000'", "TIME '00:00:00.000000'")
                .addRoundTrip("TIME '23:59:59.999999500000'", "TIME '00:00:00.000000'")

                // round up to next day, maximal value
                .addRoundTrip("TIME '23:59:59.9999999'", "TIME '00:00:00.000000'")
                .addRoundTrip("TIME '23:59:59.99999999'", "TIME '00:00:00.000000'")
                .addRoundTrip("TIME '23:59:59.999999999'", "TIME '00:00:00.000000'")
                .addRoundTrip("TIME '23:59:59.9999999999'", "TIME '00:00:00.000000'")
                .addRoundTrip("TIME '23:59:59.99999999999'", "TIME '00:00:00.000000'")
                .addRoundTrip("TIME '23:59:59.999999999999'", "TIME '00:00:00.000000'")

                // null
                .addRoundTrip("TIME", "NULL", createTimeType(3), "CAST(NULL AS TIME(3))")
                .addRoundTrip("TIME(1)", "NULL", createTimeType(1), "CAST(NULL AS TIME(1))")
                .addRoundTrip("TIME(2)", "NULL", createTimeType(2), "CAST(NULL AS TIME(2))")
                .addRoundTrip("TIME(3)", "NULL", createTimeType(3), "CAST(NULL AS TIME(3))")
                .addRoundTrip("TIME(4)", "NULL", createTimeType(4), "CAST(NULL AS TIME(4))")
                .addRoundTrip("TIME(5)", "NULL", createTimeType(5), "CAST(NULL AS TIME(5))")
                .addRoundTrip("TIME(6)", "NULL", createTimeType(6), "CAST(NULL AS TIME(6))")

                .execute(getQueryRunner(), session, trinoCreateAsSelect("test_time"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_time"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert("test_time"));
    }

    /**
     * Read {@code DATATIME}s inserted by MySQL as Trino {@code TIMESTAMP}s
     */
    @Test(dataProvider = "sessionZonesDataProvider")
    public void testMySqlDatetimeType(ZoneId sessionZone)
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

                .execute(getQueryRunner(), session, mysqlCreateAndInsert("tpch.test_datetime"));
    }

    /**
     * Read {@code TIMESTAMP}s inserted by MySQL as Trino {@code TIMESTAMP}s
     */
    @Test(dataProvider = "sessionZonesDataProvider")
    public void testTimestampFromMySql(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        // Same as above but with inserts from MySQL - i.e. read path
        SqlDataTypeTest.create()
                // after epoch (MySQL's timestamp type doesn't support values <= epoch)
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2019-03-18 10:01:17.987'", createTimestampType(3), "TIMESTAMP '2019-03-18 10:01:17.987'")
                // time doubled in JVM zone
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2018-10-28 01:33:17.456'", createTimestampType(3), "TIMESTAMP '2018-10-28 01:33:17.456'")
                // time double in Vilnius
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2018-10-28 03:33:33.333'", createTimestampType(3), "TIMESTAMP '2018-10-28 03:33:33.333'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1970-01-01 00:13:42.000'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:13:42.000'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2018-04-01 02:13:55.123'", createTimestampType(3), "TIMESTAMP '2018-04-01 02:13:55.123'")
                // time gap in Vilnius
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2018-03-25 03:17:17.000'", createTimestampType(3), "TIMESTAMP '2018-03-25 03:17:17.000'")
                // time gap in Kathmandu
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1986-01-01 00:13:07.000'", createTimestampType(3), "TIMESTAMP '1986-01-01 00:13:07.000'")

                // same as above but with higher precision
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2019-03-18 10:01:17.987654'", createTimestampType(6), "TIMESTAMP '2019-03-18 10:01:17.987654'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2018-10-28 01:33:17.456789'", createTimestampType(6), "TIMESTAMP '2018-10-28 01:33:17.456789'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2018-10-28 03:33:33.333333'", createTimestampType(6), "TIMESTAMP '2018-10-28 03:33:33.333333'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1970-01-01 00:13:42.000000'", createTimestampType(6), "TIMESTAMP '1970-01-01 00:13:42.000000'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2018-04-01 02:13:55.123456'", createTimestampType(6), "TIMESTAMP '2018-04-01 02:13:55.123456'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2018-03-25 03:17:17.000000'", createTimestampType(6), "TIMESTAMP '2018-03-25 03:17:17.000000'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1986-01-01 00:13:07.000000'", createTimestampType(6), "TIMESTAMP '1986-01-01 00:13:07.000000'")

                // test arbitrary time for all supported precisions
                .addRoundTrip("timestamp(0)", "TIMESTAMP '1970-01-01 00:00:01'", createTimestampType(0), "TIMESTAMP '1970-01-01 00:00:01'")
                .addRoundTrip("timestamp(1)", "TIMESTAMP '1970-01-01 00:00:01.1'", createTimestampType(1), "TIMESTAMP '1970-01-01 00:00:01.1'")
                .addRoundTrip("timestamp(1)", "TIMESTAMP '1970-01-01 00:00:01.9'", createTimestampType(1), "TIMESTAMP '1970-01-01 00:00:01.9'")
                .addRoundTrip("timestamp(2)", "TIMESTAMP '1970-01-01 00:00:01.12'", createTimestampType(2), "TIMESTAMP '1970-01-01 00:00:01.12'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1970-01-01 00:00:01.123'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:01.123'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1970-01-01 00:00:01.999'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:01.999'")
                .addRoundTrip("timestamp(4)", "TIMESTAMP '1970-01-01 00:00:01.1234'", createTimestampType(4), "TIMESTAMP '1970-01-01 00:00:01.1234'")
                .addRoundTrip("timestamp(5)", "TIMESTAMP '1970-01-01 00:00:01.12345'", createTimestampType(5), "TIMESTAMP '1970-01-01 00:00:01.12345'")
                .addRoundTrip("timestamp(1)", "TIMESTAMP '2020-09-27 12:34:56.1'", createTimestampType(1), "TIMESTAMP '2020-09-27 12:34:56.1'")
                .addRoundTrip("timestamp(1)", "TIMESTAMP '2020-09-27 12:34:56.9'", createTimestampType(1), "TIMESTAMP '2020-09-27 12:34:56.9'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2020-09-27 12:34:56.123'", createTimestampType(3), "TIMESTAMP '2020-09-27 12:34:56.123'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2020-09-27 12:34:56.999'", createTimestampType(3), "TIMESTAMP '2020-09-27 12:34:56.999'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2020-09-27 12:34:56.123456'", createTimestampType(6), "TIMESTAMP '2020-09-27 12:34:56.123456'")

                .execute(getQueryRunner(), session, mysqlCreateAndInsert("tpch.test_timestamp"));
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    public void testTimestampFromTrino(ZoneId sessionZone)
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

    /**
     * Additional test supplementing {@link #testTimestampFromTrino} with values that do not necessarily round-trip, including
     * timestamp precision higher than expressible with {@code LocalDateTime}.
     *
     * @see #testTimestampFromTrino
     */
    @Test
    public void testTimestampCoercion()
    {
        SqlDataTypeTest.create()

                // precision 0 ends up as precision 0
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00'", "TIMESTAMP '1970-01-01 00:00:00'")

                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.1'", "TIMESTAMP '1970-01-01 00:00:00.1'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.9'", "TIMESTAMP '1970-01-01 00:00:00.9'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.123'", "TIMESTAMP '1970-01-01 00:00:00.123'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.123000'", "TIMESTAMP '1970-01-01 00:00:00.123000'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.999'", "TIMESTAMP '1970-01-01 00:00:00.999'")
                // max supported precision
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.123456'", "TIMESTAMP '1970-01-01 00:00:00.123456'")

                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.1'", "TIMESTAMP '2020-09-27 12:34:56.1'")
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.9'", "TIMESTAMP '2020-09-27 12:34:56.9'")
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.123'", "TIMESTAMP '2020-09-27 12:34:56.123'")
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.123000'", "TIMESTAMP '2020-09-27 12:34:56.123000'")
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.999'", "TIMESTAMP '2020-09-27 12:34:56.999'")
                // max supported precision
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.123456'", "TIMESTAMP '2020-09-27 12:34:56.123456'")

                // round down
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.1234561'", "TIMESTAMP '1970-01-01 00:00:00.123456'")

                // nanoc round up, end result rounds down
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.123456499'", "TIMESTAMP '1970-01-01 00:00:00.123456'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.123456499999'", "TIMESTAMP '1970-01-01 00:00:00.123456'")

                // round up
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.1234565'", "TIMESTAMP '1970-01-01 00:00:00.123457'")

                // max precision
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.111222333444'", "TIMESTAMP '1970-01-01 00:00:00.111222'")

                // round up to next second
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.9999995'", "TIMESTAMP '1970-01-01 00:00:01.000000'")

                // round up to next day
                .addRoundTrip("TIMESTAMP '1970-01-01 23:59:59.9999995'", "TIMESTAMP '1970-01-02 00:00:00.000000'")

                // negative epoch
                .addRoundTrip("TIMESTAMP '1969-12-31 23:59:59.9999995'", "TIMESTAMP '1970-01-01 00:00:00.000000'")
                .addRoundTrip("TIMESTAMP '1969-12-31 23:59:59.999999499999'", "TIMESTAMP '1969-12-31 23:59:59.999999'")
                .addRoundTrip("TIMESTAMP '1969-12-31 23:59:59.9999994'", "TIMESTAMP '1969-12-31 23:59:59.999999'")

                // CTAS with Trino, where the coercion is done by the connector
                .execute(getQueryRunner(), trinoCreateAsSelect("test_timestamp_coercion"))
                // INSERT with Trino, where the coercion is done by the engine
                .execute(getQueryRunner(), trinoCreateAndInsert("test_timestamp_coercion"));
    }

    @DataProvider
    public Object[][] sessionZonesDataProvider()
    {
        return new Object[][] {
                {UTC},
                {ZoneId.systemDefault()},
                // no DST in 1970, but has DST in later years (e.g. 2018)
                {ZoneId.of("Europe/Vilnius")},
                // minutes offset change since 1970-01-01, no DST
                {ZoneId.of("Asia/Kathmandu")},
                {ZoneId.of(TestingSession.DEFAULT_TIME_ZONE_KEY.getId())},
        };
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
                .execute(getQueryRunner(), mysqlCreateAndInsert("tpch.mysql_test_json"));
    }

    @Test
    public void testFloat()
    {
        // we are not testing Nan/-Infinity/+Infinity as those are not supported by MySQL
        SqlDataTypeTest.create()
                .addRoundTrip("real", "3.14", REAL, "REAL '3.14'")
                .addRoundTrip("real", "10.3e0", REAL, "REAL '10.3e0'")
                .addRoundTrip("real", "NULL", REAL, "CAST(NULL AS REAL)")
                // .addRoundTrip("real", "3.1415927", REAL, "REAL '3.1415927'") // Overeagerly rounded by mysql to 3.14159
                .execute(getQueryRunner(), trinoCreateAsSelect("trino_test_float"))
                .execute(getQueryRunner(), trinoCreateAndInsert("trino_test_float"));

        SqlDataTypeTest.create()
                .addRoundTrip("float", "3.14", REAL, "REAL '3.14'")
                .addRoundTrip("float", "10.3e0", REAL, "REAL '10.3e0'")
                .addRoundTrip("float", "NULL", REAL, "CAST(NULL AS REAL)")
                // .addRoundTrip("float", "3.1415927", REAL, "REAL '3.1415927'") // Overeagerly rounded by mysql to 3.14159
                .execute(getQueryRunner(), mysqlCreateAndInsert("tpch.mysql_test_float"));
    }

    @Test
    public void testDouble()
    {
        // we are not testing Nan/-Infinity/+Infinity as those are not supported by MySQL
        SqlDataTypeTest.create()
                .addRoundTrip("double", "1.0E100", DOUBLE, "1.0E100")
                .addRoundTrip("double", "1.23456E12", DOUBLE, "1.23456E12")
                .addRoundTrip("double", "NULL", DOUBLE, "CAST(NULL AS DOUBLE)")
                .execute(getQueryRunner(), trinoCreateAsSelect("trino_test_double"))
                .execute(getQueryRunner(), trinoCreateAndInsert("trino_test_double"))
                .execute(getQueryRunner(), mysqlCreateAndInsert("tpch.mysql_test_double"));
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
                .execute(getQueryRunner(), mysqlCreateAndInsert("tpch.mysql_test_unsigned"));
    }

    private void testUnsupportedDataType(String databaseDataType)
    {
        SqlExecutor jdbcSqlExecutor = mySqlServer::execute;
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

    private DataSetup mysqlCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(mySqlServer::execute, tableNamePrefix);
    }

    private void assertMySqlQueryFails(@Language("SQL") String sql, String expectedMessage)
    {
        assertThatThrownBy(() -> mySqlServer.execute(sql))
                .getCause()
                .hasMessageContaining(expectedMessage);
    }
}
