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
package io.trino.plugin.singlestore;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.jdbc.UnsupportedTypeHandling;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.VarcharType;
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
import static io.trino.plugin.singlestore.SingleStoreClient.MEMSQL_VARCHAR_MAX_LENGTH;
import static io.trino.plugin.singlestore.SingleStoreQueryRunner.createSingleStoreQueryRunner;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimeType.TIME_SECONDS;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
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

/**
 * @see <a href="https://docs.singlestore.com/db/latest/en/reference/sql-reference/data-types.html">SingleStore (MemSQL) data types</a>
 */
public class TestSingleStoreTypeMapping
        extends AbstractTestQueryFramework
{
    private static final String CHARACTER_SET_UTF8 = "CHARACTER SET utf8";

    protected TestingSingleStoreServer singleStoreServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        singleStoreServer = new TestingSingleStoreServer();
        return createSingleStoreQueryRunner(singleStoreServer, ImmutableMap.of(), ImmutableMap.of(), ImmutableList.of());
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        singleStoreServer.close();
    }

    @Test
    public void testBit()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("bit", "b'1'", BOOLEAN, "true")
                .addRoundTrip("bit", "b'0'", BOOLEAN, "false")
                .addRoundTrip("bit", "NULL", BOOLEAN, "CAST(NULL AS BOOLEAN)")
                .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.test_bit"));
    }

    @Test
    public void testBoolean()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("boolean", "true", TINYINT, "TINYINT '1'")
                .addRoundTrip("boolean", "false", TINYINT, "TINYINT '0'")
                .addRoundTrip("boolean", "NULL", TINYINT, "CAST(NULL AS TINYINT)")
                .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.test_boolean"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_boolean"));
    }

    @Test
    public void testTinyint()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("tinyint", "NULL", TINYINT, "CAST(NULL AS TINYINT)")
                .addRoundTrip("tinyint", "-128", TINYINT, "TINYINT '-128'") // min value in SingleStore and Trino
                .addRoundTrip("tinyint", "5", TINYINT, "TINYINT '5'")
                .addRoundTrip("tinyint", "127", TINYINT, "TINYINT '127'") // max value in SingleStore and Trino
                .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.test_tinyint"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_tinyint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_tinyint"));
    }

    @Test
    public void testUnsupportedTinyint()
    {
        // SingleStore stores incorrect results when the values are out of supported range. This test should be fixed when SingleStore changes the behavior.
        SqlDataTypeTest.create()
                .addRoundTrip("tinyint", "-129", TINYINT, "TINYINT '-128'")
                .addRoundTrip("tinyint", "128", TINYINT, "TINYINT '127'")
                .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.test_unsupported_tinyint"));
    }

    @Test
    public void testSmallint()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("smallint", "NULL", SMALLINT, "CAST(NULL AS SMALLINT)")
                .addRoundTrip("smallint", "-32768", SMALLINT, "SMALLINT '-32768'") // min value in SingleStore and Trino
                .addRoundTrip("smallint", "32456", SMALLINT, "SMALLINT '32456'")
                .addRoundTrip("smallint", "32767", SMALLINT, "SMALLINT '32767'") // max value in SingleStore and Trino
                .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.test_smallint"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_smallint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_smallint"));
    }

    @Test
    public void testUnsupportedSmallint()
    {
        // SingleStore stores incorrect results when the values are out of supported range. This test should be fixed when SingleStore changes the behavior.
        SqlDataTypeTest.create()
                .addRoundTrip("smallint", "-32769", SMALLINT, "SMALLINT '-32768'")
                .addRoundTrip("smallint", "32768", SMALLINT, "SMALLINT '32767'")
                .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.test_unsupported_smallint"));
    }

    @Test
    public void testInteger()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("integer", "NULL", INTEGER, "CAST(NULL AS INTEGER)")
                .addRoundTrip("integer", "-2147483648", INTEGER, "-2147483648") // min value in SingleStore and Trino
                .addRoundTrip("integer", "1234567890", INTEGER, "1234567890")
                .addRoundTrip("integer", "2147483647", INTEGER, "2147483647") // max value in SingleStore and Trino
                .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.test_int"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_int"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_int"));
    }

    @Test
    public void testUnsupportedInteger()
    {
        // SingleStore stores incorrect results when the values are out of supported range. This test should be fixed when SingleStore changes the behavior.
        SqlDataTypeTest.create()
                .addRoundTrip("integer", "-2147483649", INTEGER, "INTEGER '-2147483648'")
                .addRoundTrip("integer", "2147483648", INTEGER, "INTEGER '2147483647'")
                .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.test_unsupported_integer"));
    }

    @Test
    public void testBigint()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("bigint", "NULL", BIGINT, "CAST(NULL AS BIGINT)")
                .addRoundTrip("bigint", "-9223372036854775808", BIGINT, "-9223372036854775808") // min value in SingleStore and Trino
                .addRoundTrip("bigint", "123456789012", BIGINT, "123456789012")
                .addRoundTrip("bigint", "9223372036854775807", BIGINT, "9223372036854775807") // max value in SingleStore and Trino
                .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.test_bigint"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_bigint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_bigint"));
    }

    @Test
    public void testUnsupportedBigint()
    {
        // SingleStore stores incorrect results when the values are out of supported range. This test should be fixed when SingleStore changes the behavior.
        SqlDataTypeTest.create()
                .addRoundTrip("bigint", "-9223372036854775809", BIGINT, "BIGINT '-9223372036854775808'")
                .addRoundTrip("bigint", "9223372036854775808", BIGINT, "BIGINT '9223372036854775807'")
                .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.test_unsupported_bigint"));
    }

    @Test
    public void testFloat()
    {
        // we are not testing Nan/-Infinity/+Infinity as those are not supported by MemSQL
        SqlDataTypeTest.create()
                .addRoundTrip("real", "3.14", REAL, "REAL '3.14'")
                .addRoundTrip("real", "10.3e0", REAL, "REAL '10.3e0'")
                .addRoundTrip("real", "NULL", REAL, "CAST(NULL AS REAL)")
                // .addRoundTrip("real", "3.1415927", REAL, "REAL '3.1415927'") // Overeagerly rounded by MemSQL to 3.14159
                .execute(getQueryRunner(), trinoCreateAsSelect("trino_test_float"))
                .execute(getQueryRunner(), trinoCreateAndInsert("trino_test_float"));

        SqlDataTypeTest.create()
                .addRoundTrip("float", "3.14", REAL, "REAL '3.14'")
                .addRoundTrip("float", "10.3e0", REAL, "REAL '10.3e0'")
                .addRoundTrip("float", "NULL", REAL, "CAST(NULL AS REAL)")
                // .addRoundTrip("float", "3.1415927", REAL, "REAL '3.1415927'") // Overeagerly rounded by MemSQL to 3.14159
                .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.memsql_test_float"));
    }

    @Test
    public void testDouble()
    {
        // we are not testing Nan/-Infinity/+Infinity as those are not supported by MemSQL
        SqlDataTypeTest.create()
                .addRoundTrip("double", "1.0E100", DOUBLE, "DOUBLE '1.0E100'")
                .addRoundTrip("double", "123.456E10", DOUBLE, "DOUBLE '123.456E10'")
                .addRoundTrip("double", "NULL", DOUBLE, "CAST(NULL AS double)")
                .execute(getQueryRunner(), trinoCreateAsSelect("trino_test_double"))
                .execute(getQueryRunner(), trinoCreateAndInsert("trino_test_double"))
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
                .execute(getQueryRunner(), trinoCreateAsSelect("test_decimal"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_decimal"));
    }

    private SqlDataTypeTest decimalTests()
    {
        return SqlDataTypeTest.create()
                .addRoundTrip("decimal(3, 0)", "CAST(NULL AS decimal(3, 0))", createDecimalType(3, 0), "CAST(NULL AS decimal(3, 0))")
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
                .addRoundTrip("decimal(38, 0)", "CAST(NULL AS decimal(38, 0))", createDecimalType(38, 0), "CAST(NULL AS decimal(38, 0))")
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
                singleStoreServer::execute,
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
                singleStoreServer::execute,
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
                singleStoreServer::execute,
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
                .setCatalogSessionProperty("singlestore", DECIMAL_MAPPING, ALLOW_OVERFLOW.name())
                .setCatalogSessionProperty("singlestore", DECIMAL_ROUNDING_MODE, roundingMode.name())
                .setCatalogSessionProperty("singlestore", DECIMAL_DEFAULT_SCALE, Integer.valueOf(scale).toString())
                .build();
    }

    private Session sessionWithDecimalMappingStrict(UnsupportedTypeHandling unsupportedTypeHandling)
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("singlestore", DECIMAL_MAPPING, STRICT.name())
                .setCatalogSessionProperty("singlestore", UNSUPPORTED_TYPE_HANDLING, unsupportedTypeHandling.name())
                .build();
    }

    @Test
    public void testTrinoCreatedParameterizedChar()
    {
        memSqlCharTypeTest()
                .execute(getQueryRunner(), trinoCreateAsSelect("memsql_test_parameterized_char"))
                .execute(getQueryRunner(), trinoCreateAndInsert("memsql_test_parameterized_char"));
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
                .addRoundTrip("char(1)", "NULL", createCharType(1), "CAST(NULL AS char(1))")
                .addRoundTrip("char(1)", "''", createCharType(1), "CAST('' AS char(1))")
                .addRoundTrip("char(1)", "'a'", createCharType(1), "CAST('a' AS char(1))")
                .addRoundTrip("char(8)", "'abc'", createCharType(8), "CAST('abc' AS char(8))")
                .addRoundTrip("char(8)", "'12345678'", createCharType(8), "CAST('12345678' AS char(8))")
                .addRoundTrip("char(255)", format("'%s'", "a".repeat(255)), createCharType(255), format("CAST('%s' AS char(255))", "a".repeat(255)));
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
                .addRoundTrip("varchar(10)", "NULL", createVarcharType(10), "CAST(NULL AS varchar(10))")
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
                .execute(getQueryRunner(), trinoCreateAsSelect("trino_test_parameterized_varchar"))
                .execute(getQueryRunner(), trinoCreateAndInsert("trino_test_parameterized_varchar"));
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
        String sampleUnicodeLiteral = "'\u653b\u6bbb\u6a5f\u52d5\u968a'";
        SqlDataTypeTest.create()
                .addRoundTrip("tinytext " + CHARACTER_SET_UTF8, sampleUnicodeLiteral, createVarcharType(255), "CAST(" + sampleUnicodeLiteral + " AS varchar(255))")
                .addRoundTrip("text " + CHARACTER_SET_UTF8, sampleUnicodeLiteral, createVarcharType(65535), "CAST(" + sampleUnicodeLiteral + " AS varchar(65535))")
                .addRoundTrip("mediumtext " + CHARACTER_SET_UTF8, sampleUnicodeLiteral, createVarcharType(16777215), "CAST(" + sampleUnicodeLiteral + " AS varchar(16777215))")
                .addRoundTrip("longtext " + CHARACTER_SET_UTF8, sampleUnicodeLiteral, createUnboundedVarcharType(), "CAST(" + sampleUnicodeLiteral + " AS varchar)")
                .addRoundTrip("varchar(" + sampleUnicodeLiteral.length() + ") " + CHARACTER_SET_UTF8, sampleUnicodeLiteral,
                        createVarcharType(sampleUnicodeLiteral.length()), "CAST(" + sampleUnicodeLiteral + " AS varchar(" + sampleUnicodeLiteral.length() + "))")
                .addRoundTrip("varchar(32) " + CHARACTER_SET_UTF8, sampleUnicodeLiteral, createVarcharType(32), "CAST(" + sampleUnicodeLiteral + " AS varchar(32))")
                .addRoundTrip("varchar(20000) " + CHARACTER_SET_UTF8, sampleUnicodeLiteral, createVarcharType(20000), "CAST(" + sampleUnicodeLiteral + " AS varchar(20000))")
                // MemSQL version >= 7.5 supports utf8mb4, but older versions store an empty character for a 4 bytes character
                .addRoundTrip("varchar(1) " + CHARACTER_SET_UTF8, "'😂'", createVarcharType(1), "CAST('' AS varchar(1))")
                .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.memsql_test_parameterized_varchar_unicode"));
    }

    @Test
    public void testVarbinary()
    {
        varbinaryTestCases("varbinary(50)")
                .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.test_varbinary"));

        varbinaryTestCases("tinyblob")
                .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.test_varbinary"));

        varbinaryTestCases("blob")
                .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.test_varbinary"));

        varbinaryTestCases("mediumblob")
                .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.test_varbinary"));

        varbinaryTestCases("longblob")
                .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.test_varbinary"));

        varbinaryTestCases("varbinary")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_varbinary"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_varbinary"));
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
                .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.test_binary"));
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
                    .addRoundTrip("date", "CAST(NULL AS date)", DATE, "CAST(NULL AS date)")
                    .addRoundTrip("date", "CAST('0000-01-01' AS date)", DATE, "DATE '0000-01-01'")
                    .addRoundTrip("date", "CAST('0001-01-01' AS date)", DATE, "DATE '0001-01-01'")
                    .addRoundTrip("date", "CAST('1000-01-01' AS date)", DATE, "DATE '1000-01-01'") // min date in docs
                    .addRoundTrip("date", "CAST('1582-10-04' AS date)", DATE, "DATE '1582-10-04'") // before julian->gregorian switch
                    .addRoundTrip("date", "CAST('1582-10-05' AS date)", DATE, "DATE '1582-10-05'") // begin julian->gregorian switch
                    .addRoundTrip("date", "CAST('1582-10-14' AS date)", DATE, "DATE '1582-10-14'") // end julian->gregorian switch
                    .addRoundTrip("date", "CAST('1952-04-03' AS date)", DATE, "DATE '1952-04-03'") // before epoch
                    .addRoundTrip("date", "CAST('1970-01-01' AS date)", DATE, "DATE '1970-01-01'")
                    .addRoundTrip("date", "CAST('1970-02-03' AS date)", DATE, "DATE '1970-02-03'")
                    .addRoundTrip("date", "CAST('2017-07-01' AS date)", DATE, "DATE '2017-07-01'") // summer on northern hemisphere (possible DST)
                    .addRoundTrip("date", "CAST('2017-01-01' AS date)", DATE, "DATE '2017-01-01'") // winter on northern hemisphere (possible DST on southern hemisphere)
                    .addRoundTrip("date", "CAST('9999-12-31' AS date)", DATE, "DATE '9999-12-31'") // max value
                    .addRoundTrip("date", "CAST('" + dateOfLocalTimeChangeForwardAtMidnightInJvmZone.toString() + "' AS date)",
                            DATE, "DATE '" + dateOfLocalTimeChangeForwardAtMidnightInJvmZone.toString() + "'")
                    .addRoundTrip("date", "CAST('" + dateOfLocalTimeChangeForwardAtMidnightInSomeZone.toString() + "' AS date)",
                            DATE, "DATE '" + dateOfLocalTimeChangeForwardAtMidnightInSomeZone.toString() + "'")
                    .addRoundTrip("date", "CAST('" + dateOfLocalTimeChangeBackwardAtMidnightInSomeZone.toString() + "' AS date)",
                            DATE, "DATE '" + dateOfLocalTimeChangeBackwardAtMidnightInSomeZone.toString() + "'")
                    .execute(getQueryRunner(), session, memSqlCreateAndInsert("tpch.test_date"))
                    .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_date"))
                    .execute(getQueryRunner(), session, trinoCreateAsSelect("test_date"))
                    .execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_date"))
                    .execute(getQueryRunner(), session, trinoCreateAndInsert("test_date"));
        }
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    public void testTime(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(getTimeZoneKey(sessionZone.getId()))
                .build();

        SqlDataTypeTest.create()
                .addRoundTrip("time", "TIME '00:00:00'", TIME_MICROS, "TIME '00:00:00.000000'") // default to micro second (same as timestamp) in Trino
                .addRoundTrip("time(0)", "NULL", TIME_SECONDS, "CAST(NULL AS time(0))")
                .addRoundTrip("time(0)", "TIME '00:00:00'", TIME_SECONDS, "TIME '00:00:00'")
                .addRoundTrip("time(0)", "TIME '01:02:03'", TIME_SECONDS, "TIME '01:02:03'")
                .addRoundTrip("time(0)", "TIME '23:59:59'", TIME_SECONDS, "TIME '23:59:59'")
                .addRoundTrip("time(0)", "TIME '23:59:59.9'", TIME_SECONDS, "TIME '00:00:00'") // round by engine
                .addRoundTrip("time(6)", "NULL", TIME_MICROS, "CAST(NULL AS time(6))")
                .addRoundTrip("time(6)", "TIME '00:00:00'", TIME_MICROS, "TIME '00:00:00.000000'")
                .addRoundTrip("time(6)", "TIME '01:02:03'", TIME_MICROS, "TIME '01:02:03.000000'")
                .addRoundTrip("time(6)", "TIME '23:59:59'", TIME_MICROS, "TIME '23:59:59.000000'")
                .addRoundTrip("time(6)", "TIME '23:59:59.9'", TIME_MICROS, "TIME '23:59:59.900000'")
                .addRoundTrip("time(6)", "TIME '23:59:59.99'", TIME_MICROS, "TIME '23:59:59.990000'")
                .addRoundTrip("time(6)", "TIME '23:59:59.999'", TIME_MICROS, "TIME '23:59:59.999000'")
                .addRoundTrip("time(6)", "TIME '23:59:59.9999'", TIME_MICROS, "TIME '23:59:59.999900'")
                .addRoundTrip("time(6)", "TIME '23:59:59.99999'", TIME_MICROS, "TIME '23:59:59.999990'")
                .addRoundTrip("time(6)", "TIME '00:00:00.000000'", TIME_MICROS, "TIME '00:00:00.000000'")
                .addRoundTrip("time(6)", "TIME '01:02:03.123456'", TIME_MICROS, "TIME '01:02:03.123456'")
                .addRoundTrip("time(6)", "TIME '23:59:59.999999'", TIME_MICROS, "TIME '23:59:59.999999'")
                .addRoundTrip("time(6)", "TIME '00:00:00.000000'", TIME_MICROS, "TIME '00:00:00.000000'") // round by engine
                .execute(getQueryRunner(), session, trinoCreateAsSelect("test_time"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert("test_time"));

        SqlDataTypeTest.create()
                .addRoundTrip("time", "NULL", TIME_SECONDS, "CAST(NULL AS time(0))") // default to second in MemSQL
                .addRoundTrip("time", "'00:00:00'", TIME_SECONDS, "TIME '00:00:00'")
                .addRoundTrip("time", "'01:02:03'", TIME_SECONDS, "TIME '01:02:03'")
                .addRoundTrip("time", "'23:59:59'", TIME_SECONDS, "TIME '23:59:59'")
                .addRoundTrip("time", "'23:59:59.9'", TIME_SECONDS, "TIME '23:59:59'") // MemSQL ignores millis and stores only seconds in 'time' type
                .addRoundTrip("time(6)", "NULL", TIME_MICROS, "CAST(NULL AS time(6))")
                .addRoundTrip("time(6)", "'00:00:00'", TIME_MICROS, "TIME '00:00:00.000000'")
                .addRoundTrip("time(6)", "'01:02:03'", TIME_MICROS, "TIME '01:02:03.000000'")
                .addRoundTrip("time(6)", "'23:59:59'", TIME_MICROS, "TIME '23:59:59.000000'")
                .addRoundTrip("time(6)", "'23:59:59.9'", TIME_MICROS, "TIME '23:59:59.900000'")
                .addRoundTrip("time(6)", "'23:59:59.99'", TIME_MICROS, "TIME '23:59:59.990000'")
                .addRoundTrip("time(6)", "'23:59:59.999'", TIME_MICROS, "TIME '23:59:59.999000'")
                .addRoundTrip("time(6)", "'23:59:59.9999'", TIME_MICROS, "TIME '23:59:59.999900'")
                .addRoundTrip("time(6)", "'23:59:59.99999'", TIME_MICROS, "TIME '23:59:59.999990'")
                .addRoundTrip("time(6)", "'00:00:00.000000'", TIME_MICROS, "TIME '00:00:00.000000'")
                .addRoundTrip("time(6)", "'01:02:03.123456'", TIME_MICROS, "TIME '01:02:03.123456'")
                .addRoundTrip("time(6)", "'23:59:59.999999'", TIME_MICROS, "TIME '23:59:59.999999'")
                .addRoundTrip("time(6)", "'23:59:59.9999999'", TIME_MICROS, "TIME '23:59:59.999999'") // MemSQL ignores nanos and stores only micros in 'time(6)' type
                .execute(getQueryRunner(), session, memSqlCreateAndInsert("tpch.test_time"));
    }

    @Test(dataProvider = "unsupportedTimeDataProvider")
    public void testUnsupportedTime(String unsupportedTime)
    {
        // SingleStore stores incorrect results when the values are out of supported range. This test should be fixed when SingleStore changes the behavior
        try (TestTable table = new TestTable(singleStoreServer::execute, "tpch.test_unsupported_time", "(col time)", ImmutableList.of(format("'%s'", unsupportedTime)))) {
            assertQueryFails(
                    "SELECT * FROM " + table.getName(),
                    format("Supported Trino TIME type range is between 00:00:00 and 23:59:59.999999 but got %s", unsupportedTime));
        }

        try (TestTable table = new TestTable(singleStoreServer::execute, "tpch.test_unsupported_time", "(col time(6))", ImmutableList.of(format("'%s'", unsupportedTime)))) {
            assertQueryFails(
                    "SELECT * FROM " + table.getName(),
                    format("Supported Trino TIME type range is between 00:00:00 and 23:59:59.999999 but got %s.000000", unsupportedTime));
        }
    }

    @DataProvider
    public Object[][] unsupportedTimeDataProvider()
    {
        return new Object[][] {
                {"-838:59:59"}, // min value in MemSQL
                {"-00:00:01"},
                {"24:00:00"},
                {"838:59:59"}, // max value in MemSQL
        };
    }

    @Test(dataProvider = "unsupportedDateTimePrecisions")
    public void testUnsupportedTimePrecision(int precision)
    {
        // This test should be fixed if future MemSQL supports those precisions
        assertThatThrownBy(() -> singleStoreServer.execute(format("CREATE TABLE test_unsupported_timestamp_precision (col1 TIME(%s))", precision)))
                .hasMessageContaining("Feature 'TIME type with precision other than 0 or 6' is not supported by MemSQL.");
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    public void testDatetime(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(getTimeZoneKey(sessionZone.getId()))
                .build();

        // TODO (https://github.com/trinodb/trino/issues/5450) Fix DST handling
        SqlDataTypeTest.create()
                // before epoch
                .addRoundTrip("datetime", "CAST('1958-01-01 13:18:03' AS DATETIME)", createTimestampType(0), "TIMESTAMP '1958-01-01 13:18:03'")
                // after epoch
                .addRoundTrip("datetime", "CAST('2019-03-18 10:01:17' AS DATETIME)", createTimestampType(0), "TIMESTAMP '2019-03-18 10:01:17'")
                // time doubled in JVM zone
                .addRoundTrip("datetime", "CAST('2018-10-28 01:33:17' AS DATETIME)", createTimestampType(0), "TIMESTAMP '2018-10-28 01:33:17'")
                // time double in Vilnius
                .addRoundTrip("datetime", "CAST('2018-10-28 03:33:33' AS DATETIME)", createTimestampType(0), "TIMESTAMP '2018-10-28 03:33:33'")
                // epoch
//                .addRoundTrip("datetime", "CAST('1970-01-01 00:00:00' AS DATETIME)", createTimestampType(0), "TIMESTAMP '1970-01-01 00:00:00'")
//                .addRoundTrip("datetime", "CAST('1970-01-01 00:13:42' AS DATETIME)", createTimestampType(0), "TIMESTAMP '1970-01-01 00:13:42'")
//                .addRoundTrip("datetime", "CAST('2018-04-01 02:13:55' AS DATETIME)", createTimestampType(0), "TIMESTAMP '2018-04-01 02:13:55'")
                // time gap in Vilnius
                .addRoundTrip("datetime", "CAST('2018-03-25 03:17:17.000000' AS DATETIME)", createTimestampType(0), "TIMESTAMP '2018-03-25 03:17:17'")
                // time gap in Kathmandu
                .addRoundTrip("datetime", "CAST('1986-01-01 00:13:07.000000' AS DATETIME)", createTimestampType(0), "TIMESTAMP '1986-01-01 00:13:07'")

                // same as above but with higher precision
                .addRoundTrip("datetime(6)", "CAST('1958-01-01 13:18:03.123456' AS DATETIME(6))", createTimestampType(6), "TIMESTAMP '1958-01-01 13:18:03.123456'")
                .addRoundTrip("datetime(6)", "CAST('2019-03-18 10:01:17.987654' AS DATETIME(6))", createTimestampType(6), "TIMESTAMP '2019-03-18 10:01:17.987654'")
                .addRoundTrip("datetime(6)", "CAST('2018-10-28 01:33:17.456789' AS DATETIME(6))", createTimestampType(6), "TIMESTAMP '2018-10-28 01:33:17.456789'")
                .addRoundTrip("datetime(6)", "CAST('2018-10-28 03:33:33.333333' AS DATETIME(6))", createTimestampType(6), "TIMESTAMP '2018-10-28 03:33:33.333333'")
//                .addRoundTrip("datetime(6)", "CAST('1970-01-01 00:00:00.000000' AS DATETIME(6))", createTimestampType(6), "TIMESTAMP '1970-01-01 00:00:00.000000'")
//                .addRoundTrip("datetime(6)", "CAST('1970-01-01 00:13:42.000001' AS DATETIME(6))", createTimestampType(6), "TIMESTAMP '1970-01-01 00:13:42.000001'")
//                .addRoundTrip("datetime(6)", "CAST('2018-04-01 02:13:55.123456' AS DATETIME(6))", createTimestampType(6), "TIMESTAMP '2018-04-01 02:13:55.123456'")
                .addRoundTrip("datetime(6)", "CAST('2018-03-25 03:17:17.000000' AS DATETIME(6))", createTimestampType(6), "TIMESTAMP '2018-03-25 03:17:17.000000'")
                .addRoundTrip("datetime(6)", "CAST('1986-01-01 00:13:07.000000' AS DATETIME(6))", createTimestampType(6), "TIMESTAMP '1986-01-01 00:13:07.000000'")

                // negative epoch
                .addRoundTrip("datetime(6)", "CAST('1969-12-31 23:59:59.999995' AS DATETIME(6))", createTimestampType(6), "TIMESTAMP '1969-12-31 23:59:59.999995'")
                .addRoundTrip("datetime(6)", "CAST('1969-12-31 23:59:59.999949' AS DATETIME(6))", createTimestampType(6), "TIMESTAMP '1969-12-31 23:59:59.999949'")
                .addRoundTrip("datetime(6)", "CAST('1969-12-31 23:59:59.999994' AS DATETIME(6))", createTimestampType(6), "TIMESTAMP '1969-12-31 23:59:59.999994'")

                // min value in MemSQL
                .addRoundTrip("datetime", "CAST('1000-01-01 00:00:00' AS DATETIME)", createTimestampType(0), "TIMESTAMP '1000-01-01 00:00:00'")
                .addRoundTrip("datetime(6)", "CAST('1000-01-01 00:00:00.000000' AS DATETIME(6))", createTimestampType(6), "TIMESTAMP '1000-01-01 00:00:00.000000'")

                // max value in MemSQL
                .addRoundTrip("datetime", "CAST('9999-12-31 23:59:59' AS DATETIME)", createTimestampType(0), "TIMESTAMP '9999-12-31 23:59:59'")
                .addRoundTrip("datetime(6)", "CAST('9999-12-31 23:59:59.999999' AS DATETIME(6))", createTimestampType(6), "TIMESTAMP '9999-12-31 23:59:59.999999'")

                // null
                .addRoundTrip("datetime", "NULL", createTimestampType(0), "CAST(NULL AS TIMESTAMP(0))")
                .addRoundTrip("datetime(6)", "NULL", createTimestampType(6), "CAST(NULL AS TIMESTAMP(6))")

                .execute(getQueryRunner(), session, memSqlCreateAndInsert("tpch.test_datetime"));
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    public void testTimestamp(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(getTimeZoneKey(sessionZone.getId()))
                .build();

        // TODO (https://github.com/trinodb/trino/issues/5450) Fix DST handling
        SqlDataTypeTest.create()
                // before epoch doesn't exist because min timestamp value is 1970-01-01 in MemSQL
                // after epoch
                .addRoundTrip("timestamp", toTimestamp("2019-03-18 10:01:17"), createTimestampType(0), "TIMESTAMP '2019-03-18 10:01:17'")
                // time doubled in JVM zone
                .addRoundTrip("timestamp", toTimestamp("2018-10-28 01:33:17"), createTimestampType(0), "TIMESTAMP '2018-10-28 01:33:17'")
                // time double in Vilnius
                .addRoundTrip("timestamp", toTimestamp("2018-10-28 03:33:33"), createTimestampType(0), "TIMESTAMP '2018-10-28 03:33:33'")
                // epoch
//                .addRoundTrip("timestamp", toTimestamp("1970-01-01 00:00:00"), createTimestampType(0), "TIMESTAMP '1970-01-01 00:00:00'")
//                .addRoundTrip("timestamp", toTimestamp("1970-01-01 00:13:42"), createTimestampType(0), "TIMESTAMP '1970-01-01 00:13:42'")
//                .addRoundTrip("timestamp", toTimestamp("2018-04-01 02:13:55"), createTimestampType(0), "TIMESTAMP '2018-04-01 02:13:55'")
                // time gap in Vilnius
                .addRoundTrip("timestamp", toTimestamp("2018-03-25 03:17:17.000000"), createTimestampType(0), "TIMESTAMP '2018-03-25 03:17:17'")

                // same as above but with higher precision
                .addRoundTrip("timestamp(6)", toLongTimestamp("2019-03-18 10:01:17.987654"), createTimestampType(6), "TIMESTAMP '2019-03-18 10:01:17.987654'")
                .addRoundTrip("timestamp(6)", toLongTimestamp("2018-10-28 01:33:17.456789"), createTimestampType(6), "TIMESTAMP '2018-10-28 01:33:17.456789'")
                .addRoundTrip("timestamp(6)", toLongTimestamp("2018-10-28 03:33:33.333333"), createTimestampType(6), "TIMESTAMP '2018-10-28 03:33:33.333333'")
//                .addRoundTrip("timestamp(6)", toLongTimestamp("1970-01-01 00:00:00.000000"), createTimestampType(6), "TIMESTAMP '1970-01-01 00:00:00.000000'")
//                .addRoundTrip("timestamp(6)", toLongTimestamp("1970-01-01 00:13:42.000001"), createTimestampType(6), "TIMESTAMP '1970-01-01 00:13:42.000001'")
//                .addRoundTrip("timestamp(6)", toLongTimestamp("2018-04-01 02:13:55.123456"), createTimestampType(6), "TIMESTAMP '2018-04-01 02:13:55.123456'")
                .addRoundTrip("timestamp(6)", toLongTimestamp("2018-03-25 03:17:17.000000"), createTimestampType(6), "TIMESTAMP '2018-03-25 03:17:17.000000'")

                // min value in MemSQL
//                .addRoundTrip("timestamp", toTimestamp("1970-01-01 00:00:01"), createTimestampType(0), "TIMESTAMP '1970-01-01 00:00:01'")
//                .addRoundTrip("timestamp(6)", toLongTimestamp("1970-01-01 00:00:01.000000"), createTimestampType(6), "TIMESTAMP '1970-01-01 00:00:01.000000'")

                // max value in MemSQL
                .addRoundTrip("timestamp", toTimestamp("2038-01-19 03:14:07"), createTimestampType(0), "TIMESTAMP '2038-01-19 03:14:07'")
                .addRoundTrip("timestamp(6)", toLongTimestamp("2038-01-19 03:14:07.999999"), createTimestampType(6), "TIMESTAMP '2038-01-19 03:14:07.999999'")

                // null
                .addRoundTrip("timestamp", "NULL", createTimestampType(0), "CAST(NULL AS TIMESTAMP(0))")
                .addRoundTrip("timestamp(6)", "NULL", createTimestampType(6), "CAST(NULL AS TIMESTAMP(6))")

                .execute(getQueryRunner(), session, memSqlCreateAndInsert("tpch.test_timestamp"));
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    public void testTimestampWrite(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(getTimeZoneKey(sessionZone.getId()))
                .build();

        // TODO (https://github.com/trinodb/trino/issues/5450) Fix DST handling
        SqlDataTypeTest.create()
                // without precision
                .addRoundTrip("TIMESTAMP '2021-10-21 12:34:56.123456'", "TIMESTAMP '2021-10-21 12:34:56.123456'")
                // before epoch
                .addRoundTrip("timestamp(0)", "TIMESTAMP '1958-01-01 13:18:03'", createTimestampType(0), "TIMESTAMP '1958-01-01 13:18:03'")
                // after epoch
                .addRoundTrip("timestamp(0)", "TIMESTAMP '2019-03-18 10:01:17'", createTimestampType(0), "TIMESTAMP '2019-03-18 10:01:17'")
                // time doubled in JVM zone
                .addRoundTrip("timestamp(0)", "TIMESTAMP '2018-10-28 01:33:17'", createTimestampType(0), "TIMESTAMP '2018-10-28 01:33:17'")
                // time double in Vilnius
                .addRoundTrip("timestamp(0)", "TIMESTAMP '2018-10-28 03:33:33'", createTimestampType(0), "TIMESTAMP '2018-10-28 03:33:33'")
                // epoch
//                .addRoundTrip("timestamp(0)", "TIMESTAMP '1970-01-01 00:00:00'", createTimestampType(0), "TIMESTAMP '1970-01-01 00:00:00'")
//                .addRoundTrip("timestamp(0)", "TIMESTAMP '1970-01-01 00:13:42'", createTimestampType(0), "TIMESTAMP '1970-01-01 00:13:42'")
//                .addRoundTrip("timestamp(0)", "TIMESTAMP '2018-04-01 02:13:55'", createTimestampType(0), "TIMESTAMP '2018-04-01 02:13:55'")
                // time gap in Vilnius
                .addRoundTrip("timestamp(0)", "TIMESTAMP '2018-03-25 03:17:17.000000'", createTimestampType(0), "TIMESTAMP '2018-03-25 03:17:17'")
                // time gap in Kathmandu
                .addRoundTrip("timestamp(0)", "TIMESTAMP '1986-01-01 00:13:07.000000'", createTimestampType(0), "TIMESTAMP '1986-01-01 00:13:07'")

                // same as above but with higher precision
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1958-01-01 13:18:03.123456'", createTimestampType(6), "TIMESTAMP '1958-01-01 13:18:03.123456'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2019-03-18 10:01:17.987654'", createTimestampType(6), "TIMESTAMP '2019-03-18 10:01:17.987654'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2018-10-28 01:33:17.456789'", createTimestampType(6), "TIMESTAMP '2018-10-28 01:33:17.456789'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2018-10-28 03:33:33.333333'", createTimestampType(6), "TIMESTAMP '2018-10-28 03:33:33.333333'")
//                .addRoundTrip("timestamp(6)", "TIMESTAMP '1970-01-01 00:00:00.000000'", createTimestampType(6), "TIMESTAMP '1970-01-01 00:00:00.000000'")
//                .addRoundTrip("timestamp(6)", "TIMESTAMP '1970-01-01 00:13:42.000001'", createTimestampType(6), "TIMESTAMP '1970-01-01 00:13:42.000001'")
//                .addRoundTrip("timestamp(6)", "TIMESTAMP '2018-04-01 02:13:55.123456'", createTimestampType(6), "TIMESTAMP '2018-04-01 02:13:55.123456'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2018-03-25 03:17:17.000000'", createTimestampType(6), "TIMESTAMP '2018-03-25 03:17:17.000000'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1986-01-01 00:13:07.000000'", createTimestampType(6), "TIMESTAMP '1986-01-01 00:13:07.000000'")

                // negative epoch
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1969-12-31 23:59:59.999995'", createTimestampType(6), "TIMESTAMP '1969-12-31 23:59:59.999995'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1969-12-31 23:59:59.999949'", createTimestampType(6), "TIMESTAMP '1969-12-31 23:59:59.999949'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1969-12-31 23:59:59.999994'", createTimestampType(6), "TIMESTAMP '1969-12-31 23:59:59.999994'")

                // min value in MemSQL
                .addRoundTrip("timestamp(0)", "TIMESTAMP '1000-01-01 00:00:00'", createTimestampType(0), "TIMESTAMP '1000-01-01 00:00:00'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1000-01-01 00:00:00.000000'", createTimestampType(6), "TIMESTAMP '1000-01-01 00:00:00.000000'")

                // max value in MemSQL
                .addRoundTrip("timestamp(0)", "TIMESTAMP '9999-12-31 23:59:59'", createTimestampType(0), "TIMESTAMP '9999-12-31 23:59:59'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '9999-12-31 23:59:59.999999'", createTimestampType(6), "TIMESTAMP '9999-12-31 23:59:59.999999'")

                // null
                .addRoundTrip("timestamp(0)", "NULL", createTimestampType(0), "CAST(NULL AS TIMESTAMP(0))")
                .addRoundTrip("timestamp(6)", "NULL", createTimestampType(6), "CAST(NULL AS TIMESTAMP(6))")

                .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_datetime"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_datetime"));
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

    @Test(dataProvider = "unsupportedDateTimePrecisions")
    public void testUnsupportedDateTimePrecision(int precision)
    {
        // This test should be fixed if future MemSQL supports those precisions
        assertThatThrownBy(() -> singleStoreServer.execute(format("CREATE TABLE test_unsupported_timestamp_precision (col1 TIMESTAMP(%s))", precision)))
                .hasMessageContaining("Feature 'TIMESTAMP type with precision other than 0 or 6' is not supported by MemSQL.");

        assertThatThrownBy(() -> singleStoreServer.execute(format("CREATE TABLE test_unsupported_datetime_precision (col1 DATETIME(%s))", precision)))
                .hasMessageContaining("Feature 'DATETIME type with precision other than 0 or 6' is not supported by MemSQL.");
    }

    @DataProvider
    public Object[][] unsupportedDateTimePrecisions()
    {
        return new Object[][] {
                {1},
                {2},
                {3},
                {4},
                {5},
                {7},
                {8},
                {9},
        };
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
        SqlExecutor jdbcSqlExecutor = singleStoreServer::execute;
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

    private DataSetup memSqlCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(singleStoreServer::execute, tableNamePrefix);
    }

    private static String toTimestamp(String value)
    {
        return format("TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS')", value);
    }

    private static String toLongTimestamp(String value)
    {
        return format("TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS.FF6')", value);
    }
}
