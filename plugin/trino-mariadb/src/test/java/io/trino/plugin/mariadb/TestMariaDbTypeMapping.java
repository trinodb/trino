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
package io.trino.plugin.mariadb;

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
import static io.trino.plugin.mariadb.MariaDbQueryRunner.createMariaDbQueryRunner;
import static io.trino.spi.type.BigintType.BIGINT;
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
import static java.lang.String.format;
import static java.math.RoundingMode.HALF_UP;
import static java.math.RoundingMode.UNNECESSARY;
import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.asList;

/**
 * @see <a href="https://mariadb.com/kb/en/data-types/">MariaDB data types</a>
 */
public class TestMariaDbTypeMapping
        extends AbstractTestQueryFramework
{
    protected TestingMariaDbServer server;

    private final ZoneId jvmZone = ZoneId.systemDefault();
    // no DST in 1970, but has DST in later years (e.g. 2018)
    private final ZoneId vilnius = ZoneId.of("Europe/Vilnius");
    // minutes offset change since 1970-01-01, no DST
    private final ZoneId kathmandu = ZoneId.of("Asia/Kathmandu");

    @BeforeClass
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
        server = closeAfterClass(new TestingMariaDbServer());
        return createMariaDbQueryRunner(server, ImmutableMap.of(), ImmutableMap.of(), ImmutableList.of());
    }

    @Test
    public void testBoolean()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("boolean", "true", TINYINT, "TINYINT '1'")
                .addRoundTrip("boolean", "false", TINYINT, "TINYINT '0'")
                .addRoundTrip("boolean", "NULL", TINYINT, "CAST(NULL AS TINYINT)")
                .execute(getQueryRunner(), mariaDbCreateAndInsert("tpch.test_boolean"))
                .execute(getQueryRunner(), trinoCreateAsSelect("tpch.test_boolean"));

        SqlDataTypeTest.create()
                .addRoundTrip("tinyint(1)", "true", TINYINT, "TINYINT '1'")
                .addRoundTrip("tinyint(1)", "false", TINYINT, "TINYINT '0'")
                .addRoundTrip("tinyint(1)", "NULL", TINYINT, "CAST(NULL AS TINYINT)")
                .execute(getQueryRunner(), mariaDbCreateAndInsert("tpch.test_boolean"));
    }

    @Test
    public void testTinyInt()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("tinyint", "-128", TINYINT, "TINYINT '-128'")
                .addRoundTrip("tinyint", "127", TINYINT, "TINYINT '127'")
                .addRoundTrip("tinyint", "NULL", TINYINT, "CAST(NULL AS TINYINT)")
                .execute(getQueryRunner(), mariaDbCreateAndInsert("tpch.test_tinyint"))
                .execute(getQueryRunner(), trinoCreateAsSelect("tpch.test_tinyint"));
    }

    @Test
    public void testTinyIntUnsigned()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("tinyint unsigned", "0", SMALLINT, "SMALLINT '0'")
                .addRoundTrip("tinyint unsigned", "255", SMALLINT, "SMALLINT '255'")
                .addRoundTrip("tinyint unsigned", "NULL", SMALLINT, "CAST(NULL AS SMALLINT)")
                .execute(getQueryRunner(), mariaDbCreateAndInsert("tpch.test_tinyint"));
    }

    @Test
    public void testSmallInt()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("smallint", "-32768", SMALLINT, "SMALLINT '-32768'")
                .addRoundTrip("smallint", "32767", SMALLINT, "SMALLINT '32767'")
                .addRoundTrip("smallint", "NULL", SMALLINT, "CAST(NULL AS SMALLINT)")
                .execute(getQueryRunner(), mariaDbCreateAndInsert("tpch.test_smallint"))
                .execute(getQueryRunner(), trinoCreateAsSelect("tpch.test_smallint"));
    }

    @Test
    public void testSmallIntUnsigned()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("smallint unsigned", "0", INTEGER, "0")
                .addRoundTrip("smallint unsigned", "65535", INTEGER, "65535")
                .addRoundTrip("smallint unsigned", "NULL", INTEGER, "CAST(NULL AS INTEGER)")
                .execute(getQueryRunner(), mariaDbCreateAndInsert("tpch.test_smallint_unsigned"));
    }

    @Test
    public void testMediumInt()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("integer", "-8388608", INTEGER, "-8388608")
                .addRoundTrip("integer", "8388607", INTEGER, "8388607")
                .addRoundTrip("integer", "NULL", INTEGER, "CAST(NULL AS INTEGER)")
                .execute(getQueryRunner(), mariaDbCreateAndInsert("tpch.test_mediumint"))
                .execute(getQueryRunner(), trinoCreateAsSelect("tpch.test_mediumint"));
    }

    @Test
    public void testMediumIntUnsigned()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("mediumint unsigned", "0", INTEGER, "0")
                .addRoundTrip("mediumint unsigned", "16777215", INTEGER, "16777215")
                .addRoundTrip("mediumint unsigned", "NULL", INTEGER, "CAST(NULL AS INTEGER)")
                .execute(getQueryRunner(), mariaDbCreateAndInsert("tpch.test_mediumint_unsigned"));
    }

    @Test
    public void testInt()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("int", "-2147483648", INTEGER, "-2147483648")
                .addRoundTrip("int", "2147483647", INTEGER, "2147483647")
                .addRoundTrip("int", "NULL", INTEGER, "CAST(NULL AS INTEGER)")
                .execute(getQueryRunner(), mariaDbCreateAndInsert("tpch.test_integer"))
                .execute(getQueryRunner(), trinoCreateAsSelect("tpch.test_integer"));
    }

    @Test
    public void testIntUnsigned()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("int unsigned", "0", BIGINT, "BIGINT '0'")
                .addRoundTrip("int unsigned", "4294967295", BIGINT, "BIGINT '4294967295'")
                .addRoundTrip("int unsigned", "NULL", BIGINT, "CAST(NULL AS BIGINT)")
                .execute(getQueryRunner(), mariaDbCreateAndInsert("tpch.test_integer_unsigned"));
    }

    @Test
    public void testBigInt()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("bigint", "-9223372036854775808", BIGINT, "-9223372036854775808")
                .addRoundTrip("bigint", "9223372036854775807", BIGINT, "9223372036854775807")
                .addRoundTrip("bigint", "NULL", BIGINT, "CAST(NULL AS BIGINT)")
                .execute(getQueryRunner(), mariaDbCreateAndInsert("tpch.test_bigint"))
                .execute(getQueryRunner(), trinoCreateAsSelect("tpch.test_bigint"));
    }

    @Test
    public void testBigIntUnsigned()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("bigint unsigned", "0", createDecimalType(20, 0), "CAST('0' AS DECIMAL(20,0))")
                .addRoundTrip("bigint unsigned", "18446744073709551615", createDecimalType(20, 0), "DECIMAL '18446744073709551615'")
                .addRoundTrip("bigint unsigned", "NULL", createDecimalType(20, 0), "CAST(NULL AS DECIMAL(20,0))")
                .execute(getQueryRunner(), mariaDbCreateAndInsert("tpch.test_bigint_unsigned"));
    }

    @Test
    public void testTypeAliases()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("int1", "1", TINYINT, "TINYINT '1'")
                .addRoundTrip("int2", "2", SMALLINT, "SMALLINT '2'")
                .addRoundTrip("int3", "3", INTEGER, "3") // INT3 is a synonym for MEDIUMINT
                .addRoundTrip("int4", "4", INTEGER, "4") // INT4 is a synonym for INT
                .addRoundTrip("integer", "5", INTEGER, "5") // INTEGER is a synonym for INT
                .addRoundTrip("int8", "8", BIGINT, "BIGINT '8'") // INT8 is a synonym for BIGINT
                .execute(getQueryRunner(), mariaDbCreateAndInsert("tpch.test_synonym"));
    }

    @Test
    public void testDecimal()
    {
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
                .addRoundTrip("decimal(24, 2)", "CAST('2' AS decimal(24, 2))", createDecimalType(24, 2), "CAST('2' AS decimal(24, 2))")
                .addRoundTrip("decimal(24, 2)", "CAST('2.3' AS decimal(24, 2))", createDecimalType(24, 2), "CAST('2.3' AS decimal(24, 2))")
                .addRoundTrip("decimal(24, 2)", "CAST('123456789.3' AS decimal(24, 2))", createDecimalType(24, 2), "CAST('123456789.3' AS decimal(24, 2))")
                .addRoundTrip("decimal(24, 4)", "CAST('12345678901234567890.31' AS decimal(24, 4))", createDecimalType(24, 4), "CAST('12345678901234567890.31' AS decimal(24, 4))")
                .addRoundTrip("decimal(30, 5)", "CAST('3141592653589793238462643.38327' AS decimal(30, 5))", createDecimalType(30, 5), "CAST('3141592653589793238462643.38327' AS decimal(30, 5))")
                .addRoundTrip("decimal(30, 5)", "CAST('-3141592653589793238462643.38327' AS decimal(30, 5))", createDecimalType(30, 5), "CAST('-3141592653589793238462643.38327' AS decimal(30, 5))")
                .addRoundTrip("decimal(38, 0)", "CAST('27182818284590452353602874713526624977' AS decimal(38, 0))", createDecimalType(38, 0), "CAST('27182818284590452353602874713526624977' AS decimal(38, 0))")
                .addRoundTrip("decimal(38, 0)", "CAST('-27182818284590452353602874713526624977' AS decimal(38, 0))", createDecimalType(38, 0), "CAST('-27182818284590452353602874713526624977' AS decimal(38, 0))")
                .addRoundTrip("decimal(38, 0)", "CAST(NULL AS decimal(38, 0))", createDecimalType(38, 0), "CAST(NULL AS decimal(38, 0))")
                .execute(getQueryRunner(), mariaDbCreateAndInsert("tpch.test_decimal"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_decimal"));
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
                server::execute,
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
                server::execute,
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
                server::execute,
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
                .setCatalogSessionProperty("mariadb", DECIMAL_MAPPING, ALLOW_OVERFLOW.name())
                .setCatalogSessionProperty("mariadb", DECIMAL_ROUNDING_MODE, roundingMode.name())
                .setCatalogSessionProperty("mariadb", DECIMAL_DEFAULT_SCALE, Integer.valueOf(scale).toString())
                .build();
    }

    private Session sessionWithDecimalMappingStrict(UnsupportedTypeHandling unsupportedTypeHandling)
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("mariadb", DECIMAL_MAPPING, STRICT.name())
                .setCatalogSessionProperty("mariadb", UNSUPPORTED_TYPE_HANDLING, unsupportedTypeHandling.name())
                .build();
    }

    @Test
    public void testFloat()
    {
        // we are not testing Nan/-Infinity/+Infinity as those are not supported by MariaDB
        SqlDataTypeTest.create()
                .addRoundTrip("real", "3.14", REAL, "REAL '3.14'")
                .addRoundTrip("real", "10.3e0", REAL, "REAL '10.3e0'")
                .addRoundTrip("real", "NULL", REAL, "CAST(NULL AS REAL)")
                // .addRoundTrip("real", "3.1415927", REAL, "REAL '3.1415927'") // Overeagerly rounded by MariaDB to 3.14159
                .execute(getQueryRunner(), trinoCreateAsSelect("tpch.test_real"));

        SqlDataTypeTest.create()
                .addRoundTrip("float", "3.14", REAL, "REAL '3.14'")
                .addRoundTrip("float", "10.3e0", REAL, "REAL '10.3e0'")
                .addRoundTrip("float", "NULL", REAL, "CAST(NULL AS REAL)")
                // .addRoundTrip("real", "3.1415927", REAL, "REAL '3.1415927'") // Overeagerly rounded by MariaDB to 3.14159
                .execute(getQueryRunner(), mariaDbCreateAndInsert("tpch.test_float"));
    }

    @Test
    public void testDouble()
    {
        // we are not testing Nan/-Infinity/+Infinity as those are not supported by MariaDB
        SqlDataTypeTest.create()
                .addRoundTrip("double", "3.14", DOUBLE, "CAST(3.14 AS DOUBLE)")
                .addRoundTrip("double", "1.0E100", DOUBLE, "1.0E100")
                .addRoundTrip("double", "1.23456E12", DOUBLE, "1.23456E12")
                .addRoundTrip("double", "NULL", DOUBLE, "CAST(NULL AS DOUBLE)")
                .execute(getQueryRunner(), trinoCreateAsSelect("trino_test_double"))
                .execute(getQueryRunner(), mariaDbCreateAndInsert("tpch.test_double"));
    }

    @Test
    public void testTrinoCreatedParameterizedVarchar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("varchar(10)", "'text_a'", createVarcharType(255), "CAST('text_a' AS VARCHAR(255))")
                .addRoundTrip("varchar(255)", "'text_b'", createVarcharType(255), "CAST('text_b' AS VARCHAR(255))")
                .addRoundTrip("varchar(256)", "'text_c'", createVarcharType(65535), "CAST('text_c' AS VARCHAR(65535))")
                .addRoundTrip("varchar(65535)", "'text_d'", createVarcharType(65535), "CAST('text_d' AS VARCHAR(65535))")
                .addRoundTrip("varchar(65536)", "'text_e'", createVarcharType(16777215), "CAST('text_e' AS VARCHAR(16777215))")
                .addRoundTrip("varchar(16777215)", "'text_f'", createVarcharType(16777215), "CAST('text_f' AS VARCHAR(16777215))")
                .addRoundTrip("varchar(16777216)", "'text_g'", createUnboundedVarcharType(), "CAST('text_g' AS VARCHAR)")
                .addRoundTrip("varchar(2147483646)", "'text_h'", createUnboundedVarcharType(), "CAST('text_h' AS VARCHAR)")
                .addRoundTrip("varchar", "'unbounded'", createUnboundedVarcharType(), "CAST('unbounded' AS VARCHAR)")
                .execute(getQueryRunner(), trinoCreateAsSelect("trino_test_parameterized_varchar"));
    }

    @Test
    public void testMariaDbCreatedParameterizedVarchar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("tinytext", "'a'", createVarcharType(255), "CAST('a' AS VARCHAR(255))")
                .addRoundTrip("text", "'b'", createVarcharType(65535), "CAST('b' AS VARCHAR(65535))")
                .addRoundTrip("mediumtext", "'c'", createVarcharType(16777215), "CAST('c' AS VARCHAR(16777215))")
                .addRoundTrip("longtext", "'d'", createUnboundedVarcharType(), "CAST('d' AS VARCHAR)")
                .addRoundTrip("varchar(32)", "'e'", createVarcharType(32), "CAST('e' AS VARCHAR(32))")
                .addRoundTrip("varchar(15000)", "'f'", createVarcharType(15000), "CAST('f' AS VARCHAR(15000))")
                .execute(getQueryRunner(), mariaDbCreateAndInsert("tpch.mariadb_test_parameterized_varchar"));
    }

    @Test
    public void testMariaDbCreatedParameterizedVarcharUnicode()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("tinytext CHARACTER SET utf8", "'攻殻機動隊'", createVarcharType(255), "CAST('攻殻機動隊' AS VARCHAR(255))")
                .addRoundTrip("text CHARACTER SET utf8", "'攻殻機動隊'", createVarcharType(65535), "CAST('攻殻機動隊' AS VARCHAR(65535))")
                .addRoundTrip("mediumtext CHARACTER SET utf8", "'攻殻機動隊'", createVarcharType(16777215), "CAST('攻殻機動隊' AS VARCHAR(16777215))")
                .addRoundTrip("longtext CHARACTER SET utf8", "'攻殻機動隊'", createUnboundedVarcharType(), "CAST('攻殻機動隊' AS VARCHAR)")
                .addRoundTrip("varchar(5) CHARACTER SET utf8", "'攻殻機動隊'", createVarcharType(5), "CAST('攻殻機動隊' AS VARCHAR(5))")
                .addRoundTrip("varchar(32) CHARACTER SET utf8", "'攻殻機動隊'", createVarcharType(32), "CAST('攻殻機動隊' AS VARCHAR(32))")
                .addRoundTrip("varchar(20000) CHARACTER SET utf8", "'攻殻機動隊'", createVarcharType(20000), "CAST('攻殻機動隊' AS VARCHAR(20000))")
                .addRoundTrip("varchar(1) CHARACTER SET utf8mb4", "'😂'", createVarcharType(1), "CAST('😂' AS VARCHAR(1))")
                .addRoundTrip("varchar(77) CHARACTER SET utf8mb4", "'Ну, погоди!'", createVarcharType(77), "CAST('Ну, погоди!' AS VARCHAR(77))")
                .execute(getQueryRunner(), mariaDbCreateAndInsert("tpch.mariadb_test_parameterized_varchar_unicode"));
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
                .execute(getQueryRunner(), trinoCreateAsSelect("mariadb_test_parameterized_char"))
                .execute(getQueryRunner(), mariaDbCreateAndInsert("tpch.mariadb_test_parameterized_char"));
    }

    @Test
    public void testMariaDbParameterizedCharUnicode()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("char(1) CHARACTER SET utf8", "'攻'", createCharType(1), "CAST('攻' AS CHAR(1))")
                .addRoundTrip("char(5) CHARACTER SET utf8", "'攻殻'", createCharType(5), "CAST('攻殻' AS CHAR(5))")
                .addRoundTrip("char(5) CHARACTER SET utf8", "'攻殻機動隊'", createCharType(5), "CAST('攻殻機動隊' AS CHAR(5))")
                .addRoundTrip("char(1)", "'😂'", createCharType(1), "CAST('😂' AS char(1))")
                .addRoundTrip("char(77)", "'Ну, погоди!'", createCharType(77), "CAST('Ну, погоди!' AS char(77))")
                .execute(getQueryRunner(), mariaDbCreateAndInsert("tpch.mariadb_test_parameterized_char"));
    }

    @Test
    public void testCharTrailingSpace()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("char(10)", "'test'", createCharType(10), "CAST('test' AS CHAR(10))")
                .addRoundTrip("char(10)", "'test  '", createCharType(10), "CAST('test' AS CHAR(10))")
                .addRoundTrip("char(10)", "'test        '", createCharType(10), "CAST('test' AS CHAR(10))")
                .execute(getQueryRunner(), mariaDbCreateAndInsert("tpch.mariadb_char_trailing_space"));
    }

    @Test
    public void testVarbinary()
    {
        varbinaryTestCases("varbinary(50)")
                .execute(getQueryRunner(), mariaDbCreateAndInsert("tpch.test_varbinary"));

        varbinaryTestCases("tinyblob")
                .execute(getQueryRunner(), mariaDbCreateAndInsert("tpch.test_varbinary"));

        varbinaryTestCases("blob")
                .execute(getQueryRunner(), mariaDbCreateAndInsert("tpch.test_varbinary"));

        varbinaryTestCases("mediumblob")
                .execute(getQueryRunner(), mariaDbCreateAndInsert("tpch.test_varbinary"));

        varbinaryTestCases("longblob")
                .execute(getQueryRunner(), mariaDbCreateAndInsert("tpch.test_varbinary"));

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
                .execute(getQueryRunner(), mariaDbCreateAndInsert("tpch.test_binary"));
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    public void testDate(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        SqlDataTypeTest.create()
                .addRoundTrip("date", "NULL", DATE, "CAST(NULL AS DATE)")
                .addRoundTrip("date", "DATE '0001-01-01'", DATE, "DATE '0001-01-01'")
                .addRoundTrip("date", "DATE '0012-12-12'", DATE, "DATE '0012-12-12'")
                .addRoundTrip("date", "DATE '1000-01-01'", DATE, "DATE '1000-01-01'") // min supported date in MariaDB
                .addRoundTrip("date", "DATE '1500-01-01'", DATE, "DATE '1500-01-01'")
                .addRoundTrip("date", "DATE '1582-10-05'", DATE, "DATE '1582-10-05'") // begin julian->gregorian switch
                .addRoundTrip("date", "DATE '1582-10-14'", DATE, "DATE '1582-10-14'") // end julian->gregorian switch
                .addRoundTrip("date", "DATE '1952-04-03'", DATE, "DATE '1952-04-03'")
                .addRoundTrip("date", "DATE '1970-01-01'", DATE, "DATE '1970-01-01'")
                .addRoundTrip("date", "DATE '1970-02-03'", DATE, "DATE '1970-02-03'")
                .addRoundTrip("date", "DATE '1983-04-01'", DATE, "DATE '1983-04-01'")
                .addRoundTrip("date", "DATE '1983-10-01'", DATE, "DATE '1983-10-01'")
                .addRoundTrip("date", "DATE '2017-07-01'", DATE, "DATE '2017-07-01'") // summer on northern hemisphere (possible DST)
                .addRoundTrip("date", "DATE '2017-01-01'", DATE, "DATE '2017-01-01'") // winter on northern hemisphere (possible DST on southern hemisphere)
                .addRoundTrip("date", "DATE '9999-12-31'", DATE, "DATE '9999-12-31'") // max supported date in MariaDB
                .execute(getQueryRunner(), session, trinoCreateAsSelect("test_date"))
                .execute(getQueryRunner(), session, mariaDbCreateAndInsert("tpch.test_date"));
    }

    @Test
    public void testUnsupportedDate()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_negative_date", "(dt DATE)")) {
            assertQueryFails(format("INSERT INTO %s VALUES (DATE '-0001-01-01')", table.getName()), ".*Failed to insert data.*");
            assertQueryFails(format("INSERT INTO %s VALUES (DATE '10000-01-01')", table.getName()), ".*Failed to insert data.*");
        }
    }

    @Test
    public void testTimeFromMariaDb()
    {
        SqlDataTypeTest.create()
                // default precision in MariaDB is 0
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
                .execute(getQueryRunner(), mariaDbCreateAndInsert("tpch.test_time"));
    }

    @Test
    public void testTimeFromTrino()
    {
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

                .execute(getQueryRunner(), trinoCreateAsSelect("tpch.test_time"));
    }

    private void testUnsupportedDataType(String databaseDataType)
    {
        SqlExecutor jdbcSqlExecutor = server::execute;
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

    /**
     * Read {@code TIMESTAMP}s inserted by MariaDb as Trino {@code TIMESTAMP}s
     */
    @Test(dataProvider = "sessionZonesDataProvider")
    public void testTimestamp(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        SqlDataTypeTest.create()
                // after epoch (MariaDb's timestamp type doesn't support values <= epoch)
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
                // max value 2038-01-19 03:14:07
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2038-01-19 03:14:07.000'", createTimestampType(3), "TIMESTAMP '2038-01-19 03:14:07.000'")

                // same as above but with higher precision
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2019-03-18 10:01:17.987654'", createTimestampType(6), "TIMESTAMP '2019-03-18 10:01:17.987654'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2018-10-28 01:33:17.456789'", createTimestampType(6), "TIMESTAMP '2018-10-28 01:33:17.456789'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2018-10-28 03:33:33.333333'", createTimestampType(6), "TIMESTAMP '2018-10-28 03:33:33.333333'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1970-01-01 00:13:42.000000'", createTimestampType(6), "TIMESTAMP '1970-01-01 00:13:42.000000'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2018-04-01 02:13:55.123456'", createTimestampType(6), "TIMESTAMP '2018-04-01 02:13:55.123456'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2018-03-25 03:17:17.000000'", createTimestampType(6), "TIMESTAMP '2018-03-25 03:17:17.000000'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1986-01-01 00:13:07.000000'", createTimestampType(6), "TIMESTAMP '1986-01-01 00:13:07.000000'")
                // max value 2038-01-19 03:14:07
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2038-01-19 03:14:07.000000'", createTimestampType(6), "TIMESTAMP '2038-01-19 03:14:07.000000'")

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

                .execute(getQueryRunner(), session, mariaDbCreateAndInsert("tpch.test_timestamp"))
                .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_timestamp"))
                .execute(getQueryRunner(), session, trinoCreateAsSelect("test_timestamp"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_timestamp"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert("test_timestamp"));
    }

    @Test
    public void testIncorrectTimestamp()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_incorrect_timestamp", "(dt TIMESTAMP)")) {
            assertQueryFails(format("INSERT INTO %s VALUES (TIMESTAMP '1970-01-01 00:00:00.000')", table.getName()), ".*Failed to insert data.*");
            assertQueryFails(format("INSERT INTO %s VALUES (TIMESTAMP '2038-01-19 03:14:08.000')", table.getName()), ".*Failed to insert data.*");
        }
    }

    /**
     * Additional test supplementing {@link #testTimestamp} with values that do not necessarily round-trip, including
     * timestamp precision higher than expressible with {@code LocalDateTime}.
     *
     * @see #testTimestamp
     */
    @Test
    public void testTimestampCoercion()
    {
        SqlDataTypeTest.create()

                // precision 0 ends up as precision 0
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:01'", "TIMESTAMP '1970-01-01 00:00:01'")

                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:01.1'", "TIMESTAMP '1970-01-01 00:00:01.1'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:01.9'", "TIMESTAMP '1970-01-01 00:00:01.9'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:01.123'", "TIMESTAMP '1970-01-01 00:00:01.123'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:01.123000'", "TIMESTAMP '1970-01-01 00:00:01.123000'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:01.999'", "TIMESTAMP '1970-01-01 00:00:01.999'")
                // max supported precision
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:01.123456'", "TIMESTAMP '1970-01-01 00:00:01.123456'")

                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.1'", "TIMESTAMP '2020-09-27 12:34:56.1'")
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.9'", "TIMESTAMP '2020-09-27 12:34:56.9'")
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.123'", "TIMESTAMP '2020-09-27 12:34:56.123'")
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.123000'", "TIMESTAMP '2020-09-27 12:34:56.123000'")
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.999'", "TIMESTAMP '2020-09-27 12:34:56.999'")
                // max supported precision
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.123456'", "TIMESTAMP '2020-09-27 12:34:56.123456'")

                // round down
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.1234561'", "TIMESTAMP '2020-09-27 12:34:56.123456'")

                // nanoc round up, end result rounds down
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.123456499'", "TIMESTAMP '2020-09-27 12:34:56.123456'")
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.123456499999'", "TIMESTAMP '2020-09-27 12:34:56.123456'")

                // round up
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.1234565'", "TIMESTAMP '2020-09-27 12:34:56.123457'")

                // max precision
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.111222333444'", "TIMESTAMP '2020-09-27 12:34:56.111222'")

                // round up to next second
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.9999995'", "TIMESTAMP '2020-09-27 12:34:57.000000'")

                // round up to next day
                .addRoundTrip("TIMESTAMP '2020-09-27 23:59:59.9999995'", "TIMESTAMP '2020-09-28 00:00:00.000000'")

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
                {jvmZone},
                {vilnius},
                {kathmandu},
                {TestingSession.DEFAULT_TIME_ZONE_KEY.getZoneId()},
        };
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

    private DataSetup mariaDbCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(server::execute, tableNamePrefix);
    }

    private static void checkIsGap(ZoneId zone, LocalDate date)
    {
        verify(isGap(zone, date), "Expected %s to be a gap in %s", date, zone);
    }

    private static boolean isGap(ZoneId zone, LocalDate date)
    {
        return zone.getRules().getValidOffsets(date.atStartOfDay()).isEmpty();
    }
}
