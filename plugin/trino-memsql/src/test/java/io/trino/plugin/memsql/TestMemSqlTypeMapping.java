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
package io.trino.plugin.memsql;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.plugin.jdbc.UnsupportedTypeHandling;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.VarcharType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.datatype.CreateAsSelectDataSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.datatype.DataType;
import io.trino.testing.datatype.DataTypeTest;
import io.trino.testing.datatype.SqlDataTypeTest;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TrinoSqlExecutor;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
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
import static io.trino.plugin.memsql.MemSqlClient.MEMSQL_VARCHAR_MAX_LENGTH;
import static io.trino.plugin.memsql.MemSqlQueryRunner.createMemSqlQueryRunner;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.datatype.DataType.dataType;
import static io.trino.testing.datatype.DataType.realDataType;
import static io.trino.type.JsonType.JSON;
import static java.lang.String.format;
import static java.math.RoundingMode.HALF_UP;
import static java.math.RoundingMode.UNNECESSARY;
import static java.util.Arrays.asList;

public class TestMemSqlTypeMapping
        extends AbstractTestQueryFramework
{
    private static final String CHARACTER_SET_UTF8 = "CHARACTER SET utf8";

    protected TestingMemSqlServer memSqlServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        memSqlServer = new TestingMemSqlServer();
        return createMemSqlQueryRunner(memSqlServer);
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        memSqlServer.close();
    }

    @Test
    public void testBasicTypes()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("bigint", "123456789012", BIGINT, "123456789012")
                .addRoundTrip("integer", "1234567890", INTEGER, "1234567890")
                .addRoundTrip("smallint", "32456", SMALLINT, "SMALLINT '32456'")
                .addRoundTrip("tinyint", "125", TINYINT, "TINYINT '125'")
                .addRoundTrip("double", "123.45", DOUBLE, "DOUBLE '123.45'")
                // TODO: Real doesn't work with SqlDataTypeTest (see below)
                // .addRoundTrip("real", "123.45", REAL, "REAL '123.45'")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_basic_types"));
    }

    @Test
    public void testFloat()
    {
        // we are not testing Nan/-Infinity/+Infinity as those are not supported by MemSQL
        DataTypeTest.create()
                .addRoundTrip(realDataType(), 3.14f)
                // TODO Overeagerly rounded by MemSQL to 3.14159
                // .addRoundTrip(realDataType(), 3.1415927f)
                .addRoundTrip(realDataType(), null)
                .execute(getQueryRunner(), trinoCreateAsSelect("trino_test_float"));

        DataTypeTest.create()
                .addRoundTrip(memSqlFloatDataType(), 3.14f)
                // TODO Overeagerly rounded by MemSQL to 3.14159
                // .addRoundTrip(floatType, 3.1415927f)
                .addRoundTrip(memSqlFloatDataType(), null)
                .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.memsql_test_float"));

        // TODO: Changing to SqlDataTypeTest with Real does not work: assertion in SqlDataTypeTest.verifyPredicate() fails

        // SqlDataTypeTest.create()
        //         .addRoundTrip("real", "3.14", REAL, "REAL '3.14'")
        //         // TODO Overeagerly rounded by MemSQL to 3.14159
        //         // .addRoundTrip("real", "3.1415927", REAL, "REAL '3.14159'")
        //         .addRoundTrip("real", "NULL", REAL, "CAST(NULL AS real)")
        //         .execute(getQueryRunner(), trinoCreateAsSelect("trino_test_float"));

        // SqlDataTypeTest.create()
        //         .addRoundTrip("float", "3.14", REAL, "REAL '3.14'")
        //         // TODO Overeagerly rounded by MemSQL to 3.14159
        //         // .addRoundTrip("float", "3.1415927", REAL, "REAL '3.14159'")
        //         .addRoundTrip("float", "NULL", REAL, "CAST(NULL AS real)")
        //         .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.memsql_test_float"));
    }

    @Test
    public void testDouble()
    {
        // we are not testing Nan/-Infinity/+Infinity as those are not supported by MemSQL
        SqlDataTypeTest.create()
                .addRoundTrip("double", "1.0E100", DOUBLE, "DOUBLE '1.0E100'")
                .addRoundTrip("double", "NULL", DOUBLE, "CAST(NULL AS double)")
                .execute(getQueryRunner(), trinoCreateAsSelect("trino_test_double"));

        SqlDataTypeTest.create()
                .addRoundTrip("double precision", "1.0E100", DOUBLE, "DOUBLE '1.0E100'")
                .addRoundTrip("double precision", "NULL", DOUBLE, "CAST(NULL AS double)")
                .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.memsql_test_double"));
    }

    @Test
    public void testUnsignedTypes()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("tinyint unsigned", "255", SMALLINT, "SMALLINT '255'")
                .addRoundTrip("smallint unsigned", "65535", INTEGER)
                .addRoundTrip("int unsigned", "4294967295", BIGINT)
                .addRoundTrip("integer unsigned", "4294967295", BIGINT)
                .addRoundTrip("bigint unsigned", "18446744073709551615", createDecimalType(20, 0), "CAST('18446744073709551615' AS decimal(20, 0))")
                .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.memsql_test_unsigned"));
    }

    @Test
    public void testMemsqlCreatedDecimal()
    {
        decimalTests()
                .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.test_decimal"));
    }

    @Test
    public void testTrinoCreatedDecimal()
    {
        decimalTests()
                .execute(getQueryRunner(), trinoCreateAsSelect("test_decimal"));
    }

    private SqlDataTypeTest decimalTests()
    {
        return SqlDataTypeTest.create()
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
                .addRoundTrip("decimal(38, 0)", "CAST('-27182818284590452353602874713526624977' AS decimal(38, 0))", createDecimalType(38, 0), "CAST('-27182818284590452353602874713526624977' AS decimal(38, 0))");
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
                memSqlServer::execute,
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
                memSqlServer::execute,
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
                memSqlServer::execute,
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
                .setCatalogSessionProperty("memsql", DECIMAL_MAPPING, ALLOW_OVERFLOW.name())
                .setCatalogSessionProperty("memsql", DECIMAL_ROUNDING_MODE, roundingMode.name())
                .setCatalogSessionProperty("memsql", DECIMAL_DEFAULT_SCALE, Integer.valueOf(scale).toString())
                .build();
    }

    private Session sessionWithDecimalMappingStrict(UnsupportedTypeHandling unsupportedTypeHandling)
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("memsql", DECIMAL_MAPPING, STRICT.name())
                .setCatalogSessionProperty("memsql", UNSUPPORTED_TYPE_HANDLING, unsupportedTypeHandling.name())
                .build();
    }

    @Test
    public void testTrinoCreatedParameterizedChar()
    {
        memSqlCharTypeTest()
                .execute(getQueryRunner(), trinoCreateAsSelect("memsql_test_parameterized_char"));
    }

    @Test
    public void testMemSqlCreatedParameterizedChar()
    {
        memSqlCharTypeTest()
                .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.memsql_test_parameterized_char"));
    }

    private SqlDataTypeTest memSqlCharTypeTest()
    {
        return SqlDataTypeTest.create()
                .addRoundTrip("char(1)", "''", createCharType(1), "CAST('' AS char(1))")
                .addRoundTrip("char(1)", "'a'", createCharType(1), "CAST('a' AS char(1))")
                .addRoundTrip("char(8)", "'abc'", createCharType(8), "CAST('abc' AS char(8))")
                .addRoundTrip("char(8)", "'12345678'", createCharType(8), "CAST('12345678' AS char(8))")
                .addRoundTrip("char(255)", String.format("'%s'", "a".repeat(255)), createCharType(255), String.format("CAST('%s' AS char(255))", "a".repeat(255)));
    }

    @Test
    public void testMemSqlCreatedParameterizedCharUnicode()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("char(1)", "'攻'", createCharType(1), "CAST('攻' AS char(1))")
                .addRoundTrip("char(5)", "'攻殻'", createCharType(5), "CAST('攻殻' AS char(5))")
                .addRoundTrip("char(5)", "'攻殻機動隊'", createCharType(5), "CAST('攻殻機動隊' AS char(5))")
                .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.memsql_test_parameterized_varchar"));
    }

    @Test
    public void testTrinoCreatedParameterizedVarchar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("varchar(10)", "'text_a'", createVarcharType(10), "CAST('text_a' AS varchar(10))")
                .addRoundTrip("varchar(255)", "'text_b'", createVarcharType(255), "CAST('text_b' AS varchar(255))")
                .addRoundTrip("varchar(256)", "'text_c'", createVarcharType(256), "CAST('text_c' AS varchar(256))")
                .addRoundTrip("varchar(" + MEMSQL_VARCHAR_MAX_LENGTH + ")", "'text_memsql_max'", createVarcharType(MEMSQL_VARCHAR_MAX_LENGTH), "CAST('text_memsql_max' AS varchar(" + MEMSQL_VARCHAR_MAX_LENGTH + "))")
                // types larger than max VARCHAR(n) for MemSQL get mapped to one of TEXT/MEDIUMTEXT/LONGTEXT
                .addRoundTrip("varchar(" + (MEMSQL_VARCHAR_MAX_LENGTH + 1) + ")", "'text_memsql_larger_than_max'", createVarcharType(65535), "CAST('text_memsql_larger_than_max' AS varchar(65535))")
                .addRoundTrip("varchar(65535)", "'text_d'", createVarcharType(65535), "CAST('text_d' AS varchar(65535))")
                .addRoundTrip("varchar(65536)", "'text_e'", createVarcharType(16777215), "CAST('text_e' AS varchar(16777215))")
                .addRoundTrip("varchar(16777215)", "'text_f'", createVarcharType(16777215), "CAST('text_f' AS varchar(16777215))")
                .addRoundTrip("varchar(16777216)", "'text_g'", createUnboundedVarcharType(), "CAST('text_g' AS varchar)")
                .addRoundTrip("varchar(" + VarcharType.MAX_LENGTH + ")", "'text_h'", createUnboundedVarcharType(), "CAST('text_h' AS varchar)")
                .addRoundTrip("varchar", "'unbounded'", createUnboundedVarcharType(), "CAST('unbounded' AS varchar)")
                .execute(getQueryRunner(), trinoCreateAsSelect("trino_test_parameterized_varchar"));
    }

    @Test
    public void testMemSqlCreatedParameterizedVarchar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("tinytext", "'a'", createVarcharType(255), "CAST('a' AS varchar(255))")
                .addRoundTrip("text", "'b'", createVarcharType(65535), "CAST('b' AS varchar(65535))")
                .addRoundTrip("mediumtext", "'c'", createVarcharType(16777215), "CAST('c' AS varchar(16777215))")
                .addRoundTrip("longtext", "'unbounded'", createUnboundedVarcharType(), "CAST('unbounded' AS varchar)")
                .addRoundTrip("varchar(32)", "'e'", createVarcharType(32), "CAST('e' AS varchar(32))")
                .addRoundTrip("varchar(15000)", "'f'", createVarcharType(15000), "CAST('f' AS varchar(15000))")
                .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.memsql_test_parameterized_varchar"));
    }

    @Test
    public void testMemSqlCreatedParameterizedVarcharUnicode()
    {
        String sampleUnicodeText = "'\u653b\u6bbb\u6a5f\u52d5\u968a'";
        SqlDataTypeTest.create()
                .addRoundTrip("tinytext " + CHARACTER_SET_UTF8, sampleUnicodeText, createVarcharType(255), "CAST(" + sampleUnicodeText + "AS varchar(255))")
                .addRoundTrip("text " + CHARACTER_SET_UTF8, sampleUnicodeText, createVarcharType(65535), "CAST(" + sampleUnicodeText + "AS varchar(65535))")
                .addRoundTrip("mediumtext " + CHARACTER_SET_UTF8, sampleUnicodeText, createVarcharType(16777215), "CAST(" + sampleUnicodeText + "AS varchar(16777215))")
                .addRoundTrip("longtext " + CHARACTER_SET_UTF8, sampleUnicodeText, createUnboundedVarcharType(), "CAST(" + sampleUnicodeText + "AS varchar)")
                .addRoundTrip("varchar(" + sampleUnicodeText.length() + ") " + CHARACTER_SET_UTF8, sampleUnicodeText,
                        createVarcharType(sampleUnicodeText.length()), "CAST(" + sampleUnicodeText + "AS varchar(" + sampleUnicodeText.length() + "))")
                .addRoundTrip("varchar(32) " + CHARACTER_SET_UTF8, sampleUnicodeText, createVarcharType(32), "CAST(" + sampleUnicodeText + "AS varchar(32))")
                .addRoundTrip("varchar(20000) " + CHARACTER_SET_UTF8, sampleUnicodeText, createVarcharType(20000), "CAST(" + sampleUnicodeText + "AS varchar(20000))")
                .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.memsql_test_parameterized_varchar_unicode"));
    }

    @Test
    public void testDate()
    {
        ZoneId jvmZone = ZoneId.systemDefault();
        checkState(jvmZone.getId().equals("America/Bahia_Banderas"), "This test assumes certain JVM time zone");

        ZoneId someZone = ZoneId.of("Europe/Vilnius");

        LocalDate dateOfLocalTimeChangeForwardAtMidnightInJvmZone = LocalDate.of(1970, 1, 1);
        verify(jvmZone.getRules().getValidOffsets(dateOfLocalTimeChangeForwardAtMidnightInJvmZone.atStartOfDay()).isEmpty());
        LocalDate dateOfLocalTimeChangeForwardAtMidnightInSomeZone = LocalDate.of(1983, 4, 1);
        verify(someZone.getRules().getValidOffsets(dateOfLocalTimeChangeForwardAtMidnightInSomeZone.atStartOfDay()).isEmpty());
        LocalDate dateOfLocalTimeChangeBackwardAtMidnightInSomeZone = LocalDate.of(1983, 10, 1);
        verify(someZone.getRules().getValidOffsets(dateOfLocalTimeChangeBackwardAtMidnightInSomeZone.atStartOfDay().minusMinutes(1)).size() == 2);

        for (String timeZoneId : ImmutableList.of(UTC_KEY.getId(), jvmZone.getId(), someZone.getId())) {
            Session session = Session.builder(getSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(timeZoneId))
                    .build();

            SqlDataTypeTest.create()
                    .addRoundTrip("date", "CAST('1952-04-03' AS date)", DATE, "DATE '1952-04-03'") // before epoch
                    .addRoundTrip("date", "CAST('1970-01-01' AS date)", DATE, "DATE '1970-01-01'")
                    .addRoundTrip("date", "CAST('1970-02-03' AS date)", DATE, "DATE '1970-02-03'")
                    .addRoundTrip("date", "CAST('2017-07-01' AS date)", DATE, "DATE '2017-07-01'") // summer on northern hemisphere (possible DST)
                    .addRoundTrip("date", "CAST('2017-01-01' AS date)", DATE, "DATE '2017-01-01'") // winter on northern hemisphere (possible DST on southern hemisphere)
                    .addRoundTrip("date", "CAST('" + dateOfLocalTimeChangeForwardAtMidnightInJvmZone.toString() + "' AS date)",
                            DATE, "DATE '" + dateOfLocalTimeChangeForwardAtMidnightInJvmZone.toString() + "'")
                    .addRoundTrip("date", "CAST('" + dateOfLocalTimeChangeForwardAtMidnightInSomeZone.toString() + "' AS date)",
                            DATE, "DATE '" + dateOfLocalTimeChangeForwardAtMidnightInSomeZone.toString() + "'")
                    .addRoundTrip("date", "CAST('" + dateOfLocalTimeChangeBackwardAtMidnightInSomeZone.toString() + "' AS date)",
                            DATE, "DATE '" + dateOfLocalTimeChangeBackwardAtMidnightInSomeZone.toString() + "'")
                    .execute(getQueryRunner(), session, memSqlCreateAndInsert("tpch.test_date"))
                    .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_date"))
                    .execute(getQueryRunner(), session, trinoCreateAsSelect(getSession(), "test_date"))
                    .execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_date"));
        }
    }

    @Test
    public void testDatetime()
    {
        // TODO (https://github.com/trinodb/trino/issues/5450) MemSQL datetime is not correctly read (see comment in StandardColumnMappings.timestampColumnMappingUsingSqlTimestamp)
        throw new SkipException("TODO");
    }

    @Test
    public void testTimestamp()
    {
        // TODO (https://github.com/trinodb/trino/issues/5450) MemSQL timestamp is not correctly read (see comment in StandardColumnMappings.timestampColumnMappingUsingSqlTimestamp)
        throw new SkipException("TODO");
    }

    @Test
    public void testJson()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("json", "json_parse('{}')", JSON, "JSON '{}'")
                .addRoundTrip("json", "null", JSON, "CAST(NULL AS json)")
                .addRoundTrip("json", "json_parse('null')", JSON, "JSON 'null'")
                .addRoundTrip("json", "123.4", JSON, "JSON '123.4'")
                .addRoundTrip("json", "'abc'", JSON, "JSON '\"abc\"'")
                .addRoundTrip("json", "'text with '' apostrophes'", JSON, "JSON '\"text with '' apostrophes\"'")
                .addRoundTrip("json", "''", JSON, "JSON '\"\"'")
                .addRoundTrip("json", "json_parse('{\"a\":1,\"b\":2}')", JSON, "JSON '{\"a\":1,\"b\":2}'")
                .addRoundTrip("json", "json_parse('{\"a\":[1,2,3],\"b\":{\"aa\":11,\"bb\":[{\"a\":1,\"b\":2},{\"a\":0}]}}')", JSON, "JSON '{\"a\":[1,2,3],\"b\":{\"aa\":11,\"bb\":[{\"a\":1,\"b\":2},{\"a\":0}]}}'")
                .addRoundTrip("json", "json_parse('[]')", JSON, "JSON '[]'")
                .execute(getQueryRunner(), trinoCreateAsSelect("trino_test_json"));

        // MemSQL doesn't support CAST to JSON but accepts string literals as JSON values
        SqlDataTypeTest.create()
                .addRoundTrip("json", "'{}'", JSON, "JSON '{}'")
                .addRoundTrip("json", "null", JSON, "CAST(NULL AS json)")
                .addRoundTrip("json", "'null'", JSON, "JSON 'null'")
                .addRoundTrip("json", "'123.4'", JSON, "JSON '123.4'")
                .addRoundTrip("json", "'\"abc\"'", JSON, "JSON '\"abc\"'")
                .addRoundTrip("json", "'\"text with '' apostrophes\"'", JSON, "JSON '\"text with '' apostrophes\"'")
                .addRoundTrip("json", "'\"\"'", JSON, "JSON '\"\"'")
                .addRoundTrip("json", "'{\"a\":1,\"b\":2}'", JSON, "JSON '{\"a\":1,\"b\":2}'")
                .addRoundTrip("json", "'{\"a\":[1,2,3],\"b\":{\"aa\":11,\"bb\":[{\"a\":1,\"b\":2},{\"a\":0}]}}'", JSON, "JSON '{\"a\":[1,2,3],\"b\":{\"aa\":11,\"bb\":[{\"a\":1,\"b\":2},{\"a\":0}]}}'")
                .addRoundTrip("json", "'[]'", JSON, "JSON '[]'")
                .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.mysql_test_json"));
    }

    private void testUnsupportedDataType(String databaseDataType)
    {
        SqlExecutor jdbcSqlExecutor = memSqlServer::execute;
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

    private DataSetup trinoCreateAndInsert(Session session, String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(new TrinoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    private DataSetup memSqlCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(memSqlServer::execute, tableNamePrefix);
    }

    private static DataType<Float> memSqlFloatDataType()
    {
        return dataType("float", REAL, Object::toString);
    }
}
