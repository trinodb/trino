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
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.VarcharType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingSession;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.datatype.CreateAsSelectDataSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.datatype.DataType;
import io.trino.testing.datatype.DataTypeTest;
import io.trino.testing.datatype.SqlDataTypeTest;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TrinoSqlExecutor;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Objects;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.io.BaseEncoding.base16;
import static io.trino.plugin.jdbc.DecimalConfig.DecimalMapping.ALLOW_OVERFLOW;
import static io.trino.plugin.jdbc.DecimalConfig.DecimalMapping.STRICT;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.DECIMAL_DEFAULT_SCALE;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.DECIMAL_MAPPING;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.DECIMAL_ROUNDING_MODE;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.UNSUPPORTED_TYPE_HANDLING;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.plugin.mysql.MySqlQueryRunner.createMySqlQueryRunner;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.datatype.DataType.bigintDataType;
import static io.trino.testing.datatype.DataType.charDataType;
import static io.trino.testing.datatype.DataType.dataType;
import static io.trino.testing.datatype.DataType.dateDataType;
import static io.trino.testing.datatype.DataType.decimalDataType;
import static io.trino.testing.datatype.DataType.doubleDataType;
import static io.trino.testing.datatype.DataType.formatStringLiteral;
import static io.trino.testing.datatype.DataType.integerDataType;
import static io.trino.testing.datatype.DataType.realDataType;
import static io.trino.testing.datatype.DataType.smallintDataType;
import static io.trino.testing.datatype.DataType.stringDataType;
import static io.trino.testing.datatype.DataType.tinyintDataType;
import static io.trino.testing.datatype.DataType.varcharDataType;
import static io.trino.type.JsonType.JSON;
import static java.lang.String.format;
import static java.math.RoundingMode.HALF_UP;
import static java.math.RoundingMode.UNNECESSARY;
import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.asList;
import static java.util.function.Function.identity;

public class TestMySqlTypeMapping
        extends AbstractTestQueryFramework
{
    private static final String CHARACTER_SET_UTF8 = "CHARACTER SET utf8";

    private TestingMySqlServer mysqlServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        mysqlServer = closeAfterClass(new TestingMySqlServer());
        return createMySqlQueryRunner(mysqlServer, ImmutableMap.of(), ImmutableMap.of(), ImmutableList.of());
    }

    @Test
    public void testBasicTypes()
    {
        DataTypeTest.create()
                .addRoundTrip(bigintDataType(), 123_456_789_012L)
                .addRoundTrip(integerDataType(), 1_234_567_890)
                .addRoundTrip(smallintDataType(), (short) 32_456)
                .addRoundTrip(tinyintDataType(), (byte) 125)
                .addRoundTrip(doubleDataType(), 123.45d)
                .addRoundTrip(realDataType(), 123.45f)
                .execute(getQueryRunner(), trinoCreateAsSelect("test_basic_types"));
    }

    @Test
    public void testTrinoCreatedParameterizedVarchar()
    {
        DataTypeTest.create()
                .addRoundTrip(stringDataType("varchar(10)", createVarcharType(255)), "text_a")
                .addRoundTrip(stringDataType("varchar(255)", createVarcharType(255)), "text_b")
                .addRoundTrip(stringDataType("varchar(256)", createVarcharType(65535)), "text_c")
                .addRoundTrip(stringDataType("varchar(65535)", createVarcharType(65535)), "text_d")
                .addRoundTrip(stringDataType("varchar(65536)", createVarcharType(16777215)), "text_e")
                .addRoundTrip(stringDataType("varchar(16777215)", createVarcharType(16777215)), "text_f")
                .addRoundTrip(stringDataType("varchar(16777216)", createUnboundedVarcharType()), "text_g")
                .addRoundTrip(stringDataType("varchar(" + VarcharType.MAX_LENGTH + ")", createUnboundedVarcharType()), "text_h")
                .addRoundTrip(stringDataType("varchar", createUnboundedVarcharType()), "unbounded")
                .execute(getQueryRunner(), trinoCreateAsSelect("trino__test_parameterized_varchar"));
    }

    @Test
    public void testMySqlCreatedParameterizedVarchar()
    {
        DataTypeTest.create()
                .addRoundTrip(stringDataType("tinytext", createVarcharType(255)), "a")
                .addRoundTrip(stringDataType("text", createVarcharType(65535)), "b")
                .addRoundTrip(stringDataType("mediumtext", createVarcharType(16777215)), "c")
                .addRoundTrip(stringDataType("longtext", createUnboundedVarcharType()), "d")
                .addRoundTrip(varcharDataType(32), "e")
                .addRoundTrip(varcharDataType(15000), "f")
                .execute(getQueryRunner(), mysqlCreateAndInsert("tpch.mysql_test_parameterized_varchar"));
    }

    @Test
    public void testMySqlCreatedParameterizedVarcharUnicode()
    {
        String sampleUnicodeText = "\u653b\u6bbb\u6a5f\u52d5\u968a";
        DataTypeTest.create()
                .addRoundTrip(stringDataType("tinytext " + CHARACTER_SET_UTF8, createVarcharType(255)), sampleUnicodeText)
                .addRoundTrip(stringDataType("text " + CHARACTER_SET_UTF8, createVarcharType(65535)), sampleUnicodeText)
                .addRoundTrip(stringDataType("mediumtext " + CHARACTER_SET_UTF8, createVarcharType(16777215)), sampleUnicodeText)
                .addRoundTrip(stringDataType("longtext " + CHARACTER_SET_UTF8, createUnboundedVarcharType()), sampleUnicodeText)
                .addRoundTrip(varcharDataType(sampleUnicodeText.length(), CHARACTER_SET_UTF8), sampleUnicodeText)
                .addRoundTrip(varcharDataType(32, CHARACTER_SET_UTF8), sampleUnicodeText)
                .addRoundTrip(varcharDataType(20000, CHARACTER_SET_UTF8), sampleUnicodeText)
                .execute(getQueryRunner(), mysqlCreateAndInsert("tpch.mysql_test_parameterized_varchar_unicode"));
    }

    @Test
    public void testTrinoCreatedParameterizedChar()
    {
        mysqlCharTypeTest()
                .execute(getQueryRunner(), trinoCreateAsSelect("mysql_test_parameterized_char"));
    }

    @Test
    public void testMySqlCreatedParameterizedChar()
    {
        mysqlCharTypeTest()
                .execute(getQueryRunner(), mysqlCreateAndInsert("tpch.mysql_test_parameterized_char"));
    }

    private DataTypeTest mysqlCharTypeTest()
    {
        return DataTypeTest.create()
                .addRoundTrip(charDataType("char", 1), "")
                .addRoundTrip(charDataType("char", 1), "a")
                .addRoundTrip(charDataType(1), "")
                .addRoundTrip(charDataType(1), "a")
                .addRoundTrip(charDataType(8), "abc")
                .addRoundTrip(charDataType(8), "12345678")
                .addRoundTrip(charDataType(255), "a".repeat(255));
    }

    @Test
    public void testMySqlCreatedParameterizedCharUnicode()
    {
        DataTypeTest.create()
                .addRoundTrip(charDataType(1, CHARACTER_SET_UTF8), "\u653b")
                .addRoundTrip(charDataType(5, CHARACTER_SET_UTF8), "\u653b\u6bbb")
                .addRoundTrip(charDataType(5, CHARACTER_SET_UTF8), "\u653b\u6bbb\u6a5f\u52d5\u968a")
                .execute(getQueryRunner(), mysqlCreateAndInsert("tpch.mysql_test_parameterized_varchar"));
    }

    @Test
    public void testMysqlCreatedDecimal()
    {
        decimalTests()
                .execute(getQueryRunner(), mysqlCreateAndInsert("tpch.test_decimal"));
    }

    @Test
    public void testTrinoCreatedDecimal()
    {
        decimalTests()
                .execute(getQueryRunner(), trinoCreateAsSelect("test_decimal"));
    }

    private DataTypeTest decimalTests()
    {
        return DataTypeTest.create()
                .addRoundTrip(decimalDataType(3, 0), new BigDecimal("193"))
                .addRoundTrip(decimalDataType(3, 0), new BigDecimal("19"))
                .addRoundTrip(decimalDataType(3, 0), new BigDecimal("-193"))
                .addRoundTrip(decimalDataType(3, 1), new BigDecimal("10.0"))
                .addRoundTrip(decimalDataType(3, 1), new BigDecimal("10.1"))
                .addRoundTrip(decimalDataType(3, 1), new BigDecimal("-10.1"))
                .addRoundTrip(decimalDataType(4, 2), new BigDecimal("2"))
                .addRoundTrip(decimalDataType(4, 2), new BigDecimal("2.3"))
                .addRoundTrip(decimalDataType(24, 2), new BigDecimal("2"))
                .addRoundTrip(decimalDataType(24, 2), new BigDecimal("2.3"))
                .addRoundTrip(decimalDataType(24, 2), new BigDecimal("123456789.3"))
                .addRoundTrip(decimalDataType(24, 4), new BigDecimal("12345678901234567890.31"))
                .addRoundTrip(decimalDataType(30, 5), new BigDecimal("3141592653589793238462643.38327"))
                .addRoundTrip(decimalDataType(30, 5), new BigDecimal("-3141592653589793238462643.38327"))
                .addRoundTrip(decimalDataType(38, 0), new BigDecimal("27182818284590452353602874713526624977"))
                .addRoundTrip(decimalDataType(38, 0), new BigDecimal("-27182818284590452353602874713526624977"));
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
                mysqlServer::execute,
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
                mysqlServer::execute,
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
                mysqlServer::execute,
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

    @Test
    public void testDate()
    {
        // Note: there is identical test for PostgreSQL

        ZoneId jvmZone = ZoneId.systemDefault();
        checkState(jvmZone.getId().equals("America/Bahia_Banderas"), "This test assumes certain JVM time zone");
        LocalDate dateOfLocalTimeChangeForwardAtMidnightInJvmZone = LocalDate.of(1970, 1, 1);
        verify(jvmZone.getRules().getValidOffsets(dateOfLocalTimeChangeForwardAtMidnightInJvmZone.atStartOfDay()).isEmpty());

        ZoneId someZone = ZoneId.of("Europe/Vilnius");
        LocalDate dateOfLocalTimeChangeForwardAtMidnightInSomeZone = LocalDate.of(1983, 4, 1);
        verify(someZone.getRules().getValidOffsets(dateOfLocalTimeChangeForwardAtMidnightInSomeZone.atStartOfDay()).isEmpty());
        LocalDate dateOfLocalTimeChangeBackwardAtMidnightInSomeZone = LocalDate.of(1983, 10, 1);
        verify(someZone.getRules().getValidOffsets(dateOfLocalTimeChangeBackwardAtMidnightInSomeZone.atStartOfDay().minusMinutes(1)).size() == 2);

        DataTypeTest testCases = DataTypeTest.create()
                .addRoundTrip(dateDataType(), LocalDate.of(1952, 4, 3)) // before epoch
                .addRoundTrip(dateDataType(), LocalDate.of(1970, 1, 1))
                .addRoundTrip(dateDataType(), LocalDate.of(1970, 2, 3))
                .addRoundTrip(dateDataType(), LocalDate.of(2017, 7, 1)) // summer on northern hemisphere (possible DST)
                .addRoundTrip(dateDataType(), LocalDate.of(2017, 1, 1)) // winter on northern hemisphere (possible DST on southern hemisphere)
                .addRoundTrip(dateDataType(), dateOfLocalTimeChangeForwardAtMidnightInJvmZone)
                .addRoundTrip(dateDataType(), dateOfLocalTimeChangeForwardAtMidnightInSomeZone)
                .addRoundTrip(dateDataType(), dateOfLocalTimeChangeBackwardAtMidnightInSomeZone);

        for (String timeZoneId : ImmutableList.of(UTC_KEY.getId(), jvmZone.getId(), someZone.getId())) {
            Session session = Session.builder(getSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(timeZoneId))
                    .build();
            testCases.execute(getQueryRunner(), session, mysqlCreateAndInsert("tpch.test_date"));
            testCases.execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_date"));
            testCases.execute(getQueryRunner(), session, trinoCreateAsSelect(getSession(), "test_date"));
            testCases.execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_date"));
        }
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
                .addRoundTrip("datetime(3)", "TIMESTAMP '1958-01-01 13:18:03.123'", TIMESTAMP_MILLIS, "TIMESTAMP '1958-01-01 13:18:03.123'")
                // after epoch
                .addRoundTrip("datetime(3)", "TIMESTAMP '2019-03-18 10:01:17.987'", TIMESTAMP_MILLIS, "TIMESTAMP '2019-03-18 10:01:17.987'")
                // time doubled in JVM zone
                .addRoundTrip("datetime(3)", "TIMESTAMP '2018-10-28 01:33:17.456'", TIMESTAMP_MILLIS, "TIMESTAMP '2018-10-28 01:33:17.456'")
                // time double in Vilnius
                .addRoundTrip("datetime(3)", "TIMESTAMP '2018-10-28 03:33:33.333'", TIMESTAMP_MILLIS, "TIMESTAMP '2018-10-28 03:33:33.333'")
                // epoch
                .addRoundTrip("datetime(3)", "TIMESTAMP '1970-01-01 00:00:00.000'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:00.000'")
                .addRoundTrip("datetime(3)", "TIMESTAMP '1970-01-01 00:13:42.000'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:13:42.000'")
                .addRoundTrip("datetime(3)", "TIMESTAMP '2018-04-01 02:13:55.123'", TIMESTAMP_MILLIS, "TIMESTAMP '2018-04-01 02:13:55.123'")
                // time gap in Vilnius
                .addRoundTrip("datetime(3)", "TIMESTAMP '2018-03-25 03:17:17.000'", TIMESTAMP_MILLIS, "TIMESTAMP '2018-03-25 03:17:17.000'")
                // time gap in Kathmandu
                .addRoundTrip("datetime(3)", "TIMESTAMP '1986-01-01 00:13:07.000'", TIMESTAMP_MILLIS, "TIMESTAMP '1986-01-01 00:13:07.000'")

                // same as above but with higher precision
                .addRoundTrip("datetime(6)", "TIMESTAMP '1958-01-01 13:18:03.123000'", TIMESTAMP_MILLIS, "TIMESTAMP '1958-01-01 13:18:03.123'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '2019-03-18 10:01:17.987000'", TIMESTAMP_MILLIS, "TIMESTAMP '2019-03-18 10:01:17.987'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '2018-10-28 01:33:17.456000'", TIMESTAMP_MILLIS, "TIMESTAMP '2018-10-28 01:33:17.456'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '2018-10-28 03:33:33.333000'", TIMESTAMP_MILLIS, "TIMESTAMP '2018-10-28 03:33:33.333'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '1970-01-01 00:00:00.000000'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:00.000'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '1970-01-01 00:13:42.000000'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:13:42.000'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '2018-04-01 02:13:55.123000'", TIMESTAMP_MILLIS, "TIMESTAMP '2018-04-01 02:13:55.123'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '2018-03-25 03:17:17.000000'", TIMESTAMP_MILLIS, "TIMESTAMP '2018-03-25 03:17:17.000'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '1986-01-01 00:13:07.000000'", TIMESTAMP_MILLIS, "TIMESTAMP '1986-01-01 00:13:07.000'")

                // test arbitrary time for all supported precisions
                .addRoundTrip("datetime(0)", "TIMESTAMP '1970-01-01 00:00:01'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.000'")
                .addRoundTrip("datetime(1)", "TIMESTAMP '1970-01-01 00:00:01.1'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.100'")
                .addRoundTrip("datetime(2)", "TIMESTAMP '1970-01-01 00:00:01.12'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.120'")
                .addRoundTrip("datetime(3)", "TIMESTAMP '1970-01-01 00:00:01.123'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")
                .addRoundTrip("datetime(4)", "TIMESTAMP '1970-01-01 00:00:01.1234'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")
                .addRoundTrip("datetime(5)", "TIMESTAMP '1970-01-01 00:00:01.12345'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '1970-01-01 00:00:01.123456'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")

                // test rounding for precisions too high for MySQL
                .addRoundTrip("datetime(6)", "TIMESTAMP '1970-01-01 00:00:01.1234560'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '1970-01-01 00:00:01.12345649999'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '1970-01-01 00:00:01.1234565'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '1970-01-01 00:00:01.1234569'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")

                .execute(getQueryRunner(), session, mysqlCreateAndInsert("tpch.test_datetime"));

        SqlDataTypeTest.create()
                // TODO support higher precision timestamps (https://github.com/trinodb/trino/issues/6910)
                // before epoch with second fraction
                //.addRoundTrip("timestamp(7)", "TIMESTAMP '1969-12-31 23:59:59.1230000'", createTimestampType(7), "TIMESTAMP '1969-12-31 23:59:59.1230000'")
                //.addRoundTrip("timestamp(7)", "TIMESTAMP '1969-12-31 23:59:59.1234567'", createTimestampType(7), "TIMESTAMP '1969-12-31 23:59:59.1234567'")

                // precision 0 ends up as precision 0
                .addRoundTrip("datetime(0)", "TIMESTAMP '1970-01-01 00:00:01'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:01.000'")

                .addRoundTrip("datetime(1)", "TIMESTAMP '1970-01-01 00:00:00.1'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.100'")
                .addRoundTrip("datetime(1)", "TIMESTAMP '1970-01-01 00:00:00.9'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.900'")
                .addRoundTrip("datetime(3)", "TIMESTAMP '1970-01-01 00:00:00.123'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.123'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '1970-01-01 00:00:00.123000'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.123'")
                .addRoundTrip("datetime(3)", "TIMESTAMP '1970-01-01 00:00:00.999'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.999'")
                // max supported precision
                .addRoundTrip("datetime(6)", "TIMESTAMP '1970-01-01 00:00:00.123456'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.123'")

                .addRoundTrip("datetime(1)", "TIMESTAMP '2020-09-27 12:34:56.1'", createTimestampType(3), "TIMESTAMP '2020-09-27 12:34:56.100'")
                .addRoundTrip("datetime(1)", "TIMESTAMP '2020-09-27 12:34:56.9'", createTimestampType(3), "TIMESTAMP '2020-09-27 12:34:56.900'")
                .addRoundTrip("datetime(3)", "TIMESTAMP '2020-09-27 12:34:56.123'", createTimestampType(3), "TIMESTAMP '2020-09-27 12:34:56.123'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '2020-09-27 12:34:56.123000'", createTimestampType(3), "TIMESTAMP '2020-09-27 12:34:56.123'")
                .addRoundTrip("datetime(3)", "TIMESTAMP '2020-09-27 12:34:56.999'", createTimestampType(3), "TIMESTAMP '2020-09-27 12:34:56.999'")
                // max supported precision
                .addRoundTrip("datetime(6)", "TIMESTAMP '2020-09-27 12:34:56.123456'", createTimestampType(3), "TIMESTAMP '2020-09-27 12:34:56.123'")

                // round down
                .addRoundTrip("datetime(6)", "TIMESTAMP '1970-01-01 00:00:00.123451'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.123'")

                // nanos round up, end result rounds down
                .addRoundTrip("datetime(6)", "TIMESTAMP '1970-01-01 00:00:00.123499'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.123'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '1970-01-01 00:00:00.123399'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.123'")

                // round up
                .addRoundTrip("datetime(6)", "TIMESTAMP '1970-01-01 00:00:00.123999'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.124'")

                // max precision
                .addRoundTrip("datetime(6)", "TIMESTAMP '1970-01-01 00:00:00.123456'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.123'")

                // round up to next second
                .addRoundTrip("datetime(6)", "TIMESTAMP '1970-01-01 00:00:00.999999'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:01.000'")

                // round up to next day
                .addRoundTrip("datetime(6)", "TIMESTAMP '1970-01-01 23:59:59.999999'", createTimestampType(3), "TIMESTAMP '1970-01-02 00:00:00.000'")

                // negative epoch - round up
                .addRoundTrip("datetime(6)", "TIMESTAMP '1969-12-31 23:59:59.999995'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.000'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '1969-12-31 23:59:59.999949'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.000'")
                .addRoundTrip("datetime(6)", "TIMESTAMP '1969-12-31 23:59:59.999994'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.000'")

                .execute(getQueryRunner(), session, mysqlCreateAndInsert("tpch.test_timestamp"));
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
                // before epoch (MySQL's timestamp type doesn't support values <= epoch)
                //.addRoundTrip("timestamp(3)", "TIMESTAMP '1958-01-01 13:18:03.123'", createTimestampType(3), "TIMESTAMP '1958-01-01 13:18:03.000'")
                // after epoch
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2019-03-18 10:01:17.987'", TIMESTAMP_MILLIS, "TIMESTAMP '2019-03-18 10:01:17.987'")
                // time doubled in JVM zone
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2018-10-28 01:33:17.456'", TIMESTAMP_MILLIS, "TIMESTAMP '2018-10-28 01:33:17.456'")
                // time double in Vilnius
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2018-10-28 03:33:33.333'", TIMESTAMP_MILLIS, "TIMESTAMP '2018-10-28 03:33:33.333'")
                // epoch (MySQL's timestamp type doesn't support values <= epoch)
                //.addRoundTrip("timestamp(3)", "TIMESTAMP '1970-01-01 00:00:00.000'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.000'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1970-01-01 00:13:42.000'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:13:42.000'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2018-04-01 02:13:55.123'", TIMESTAMP_MILLIS, "TIMESTAMP '2018-04-01 02:13:55.123'")
                // time gap in Vilnius
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2018-03-25 03:17:17.000'", TIMESTAMP_MILLIS, "TIMESTAMP '2018-03-25 03:17:17.000'")
                // time gap in Kathmandu
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1986-01-01 00:13:07.000'", TIMESTAMP_MILLIS, "TIMESTAMP '1986-01-01 00:13:07.000'")

                // same as above but with higher precision - note that currently anything other than timestamp(3) can only be inserted directly via MySQL
                // MySQL's timestamp type doesn't support values <= epoch
                //.addRoundTrip("timestamp(6)", "TIMESTAMP '1958-01-01 13:18:03.123000'", createTimestampType(3), "TIMESTAMP '1958-01-01 13:18:03.123'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2019-03-18 10:01:17.987000'", TIMESTAMP_MILLIS, "TIMESTAMP '2019-03-18 10:01:17.987'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2018-10-28 01:33:17.456000'", TIMESTAMP_MILLIS, "TIMESTAMP '2018-10-28 01:33:17.456'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2018-10-28 03:33:33.333000'", TIMESTAMP_MILLIS, "TIMESTAMP '2018-10-28 03:33:33.333'")
                // MySQL's timestamp type doesn't support values <= epoch
                //.addRoundTrip("timestamp(6)", "TIMESTAMP '1970-01-01 00:00:00.000000'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.000'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1970-01-01 00:13:42.000000'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:13:42.000'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2018-04-01 02:13:55.123000'", TIMESTAMP_MILLIS, "TIMESTAMP '2018-04-01 02:13:55.123'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2018-03-25 03:17:17.000000'", TIMESTAMP_MILLIS, "TIMESTAMP '2018-03-25 03:17:17.000'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1986-01-01 00:13:07.000000'", TIMESTAMP_MILLIS, "TIMESTAMP '1986-01-01 00:13:07.000'")

                // test arbitrary time for all supported precisions
                .addRoundTrip("timestamp(0)", "TIMESTAMP '1970-01-01 00:00:01'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.000'")
                .addRoundTrip("timestamp(1)", "TIMESTAMP '1970-01-01 00:00:01.1'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.100'")
                .addRoundTrip("timestamp(2)", "TIMESTAMP '1970-01-01 00:00:01.12'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.120'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1970-01-01 00:00:01.123'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")
                .addRoundTrip("timestamp(4)", "TIMESTAMP '1970-01-01 00:00:01.1234'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")
                .addRoundTrip("timestamp(5)", "TIMESTAMP '1970-01-01 00:00:01.12345'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1970-01-01 00:00:01.123456'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1970-01-01 00:00:01.1234560'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1970-01-01 00:00:01.12345649999'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1970-01-01 00:00:01.1234565'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1970-01-01 00:00:01.1234569'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")

                // TODO support higher precision timestamps (https://github.com/trinodb/trino/issues/6910)
                // before epoch with second fraction
                //.addRoundTrip("timestamp(7)", "TIMESTAMP '1969-12-31 23:59:59.1230000'", createTimestampType(7), "TIMESTAMP '1969-12-31 23:59:59.1230000'")
                //.addRoundTrip("timestamp(7)", "TIMESTAMP '1969-12-31 23:59:59.1234567'", createTimestampType(7), "TIMESTAMP '1969-12-31 23:59:59.1234567'")

                // precision 0 ends up as precision 0
                .addRoundTrip("timestamp(0)", "TIMESTAMP '1970-01-01 00:00:01'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.000'")

                .addRoundTrip("timestamp(1)", "TIMESTAMP '1970-01-01 00:00:01.1'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.100'")
                .addRoundTrip("timestamp(1)", "TIMESTAMP '1970-01-01 00:00:01.9'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.900'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1970-01-01 00:00:01.123'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1970-01-01 00:00:01.123000'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1970-01-01 00:00:01.999'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.999'")
                // max supported precision
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1970-01-01 00:00:01.123456'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")

                .addRoundTrip("timestamp(1)", "TIMESTAMP '2020-09-27 12:34:56.1'", TIMESTAMP_MILLIS, "TIMESTAMP '2020-09-27 12:34:56.100'")
                .addRoundTrip("timestamp(1)", "TIMESTAMP '2020-09-27 12:34:56.9'", TIMESTAMP_MILLIS, "TIMESTAMP '2020-09-27 12:34:56.900'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2020-09-27 12:34:56.123'", TIMESTAMP_MILLIS, "TIMESTAMP '2020-09-27 12:34:56.123'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2020-09-27 12:34:56.123000'", TIMESTAMP_MILLIS, "TIMESTAMP '2020-09-27 12:34:56.123'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2020-09-27 12:34:56.999'", TIMESTAMP_MILLIS, "TIMESTAMP '2020-09-27 12:34:56.999'")
                // max supported precision
                .addRoundTrip("timestamp(6)", "TIMESTAMP '2020-09-27 12:34:56.123456'", TIMESTAMP_MILLIS, "TIMESTAMP '2020-09-27 12:34:56.123'")

                // round down
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1970-01-01 00:00:01.12345671'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")

                // nanos round up, end result rounds down
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1970-01-01 00:00:01.1234567499'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1970-01-01 00:00:01.123456749999'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")

                // round up
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1970-01-01 00:00:01.12345675'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.123'")

                // max precision
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1970-01-01 00:00:01.111222333444'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:01.111'")

                // round up to next second
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1970-01-01 00:00:01.99999995'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:02.000'")

                // round up to next day
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1970-01-01 23:59:59.99999995'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-02 00:00:00.000'")

                // negative epoch (MySQL's timestamp type doesn't support values <= epoch)
                //.addRoundTrip("TIMESTAMP '1969-12-31 23:59:59.99999995'", "TIMESTAMP '1970-01-01 00:00:00.000'")
                //.addRoundTrip("TIMESTAMP '1969-12-31 23:59:59.999999949999'", "TIMESTAMP '1969-12-31 23:59:59.999'")
                //.addRoundTrip("TIMESTAMP '1969-12-31 23:59:59.99999994'", "TIMESTAMP '1969-12-31 23:59:59.999'")

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
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1958-01-01 13:18:03.123'", TIMESTAMP_MILLIS, "TIMESTAMP '1958-01-01 13:18:03.123'")
                // after epoch
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2019-03-18 10:01:17.987'", TIMESTAMP_MILLIS, "TIMESTAMP '2019-03-18 10:01:17.987'")
                // time doubled in JVM zone
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2018-10-28 01:33:17.456'", TIMESTAMP_MILLIS, "TIMESTAMP '2018-10-28 01:33:17.456'")
                // time double in Vilnius
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2018-10-28 03:33:33.333'", TIMESTAMP_MILLIS, "TIMESTAMP '2018-10-28 03:33:33.333'")
                // epoch
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1970-01-01 00:00:00.000'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:00.000'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1970-01-01 00:13:42.000'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:13:42.000'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2018-04-01 02:13:55.123'", TIMESTAMP_MILLIS, "TIMESTAMP '2018-04-01 02:13:55.123'")
                // time gap in Vilnius
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2018-03-25 03:17:17.000'", TIMESTAMP_MILLIS, "TIMESTAMP '2018-03-25 03:17:17.000'")
                // time gap in Kathmandu
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1986-01-01 00:13:07.000'", TIMESTAMP_MILLIS, "TIMESTAMP '1986-01-01 00:13:07.000'")

                .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_timestamp"))
                .execute(getQueryRunner(), session, trinoCreateAsSelect(getSession(), "test_timestamp"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_timestamp"));
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
        jsonTestCases(jsonDataType(value -> "JSON " + formatStringLiteral(value)))
                .execute(getQueryRunner(), trinoCreateAsSelect("trino_test_json"));
        jsonTestCases(jsonDataType(value -> format("CAST(%s AS JSON)", formatStringLiteral(value))))
                .execute(getQueryRunner(), mysqlCreateAndInsert("tpch.mysql_test_json"));
    }

    private DataTypeTest jsonTestCases(DataType<String> jsonDataType)
    {
        return DataTypeTest.create()
                .addRoundTrip(jsonDataType, "{}")
                .addRoundTrip(jsonDataType, null)
                .addRoundTrip(jsonDataType, "null")
                .addRoundTrip(jsonDataType, "123.4")
                .addRoundTrip(jsonDataType, "\"abc\"")
                .addRoundTrip(jsonDataType, "\"text with ' apostrophes\"")
                .addRoundTrip(jsonDataType, "\"\"")
                .addRoundTrip(jsonDataType, "{\"a\":1,\"b\":2}")
                .addRoundTrip(jsonDataType, "{\"a\":[1,2,3],\"b\":{\"aa\":11,\"bb\":[{\"a\":1,\"b\":2},{\"a\":0}]}}")
                .addRoundTrip(jsonDataType, "[]");
    }

    @Test
    public void testFloat()
    {
        singlePrecisionFloatingPointTests(realDataType())
                .execute(getQueryRunner(), trinoCreateAsSelect("trino_test_float"));
        singlePrecisionFloatingPointTests(mysqlFloatDataType())
                .execute(getQueryRunner(), mysqlCreateAndInsert("tpch.mysql_test_float"));
    }

    @Test
    public void testDouble()
    {
        doublePrecisionFloatingPointTests(doubleDataType())
                .execute(getQueryRunner(), trinoCreateAsSelect("trino_test_double"));
        doublePrecisionFloatingPointTests(mysqlDoubleDataType())
                .execute(getQueryRunner(), mysqlCreateAndInsert("tpch.mysql_test_double"));
    }

    @Test
    public void testUnsignedTypes()
    {
        DataType<Short> mysqlUnsignedTinyInt = DataType.dataType("TINYINT UNSIGNED", SmallintType.SMALLINT, Objects::toString);
        DataType<Integer> mysqlUnsignedSmallInt = DataType.dataType("SMALLINT UNSIGNED", IntegerType.INTEGER, Objects::toString);
        DataType<Long> mysqlUnsignedInt = DataType.dataType("INT UNSIGNED", BigintType.BIGINT, Objects::toString);
        DataType<Long> mysqlUnsignedInteger = DataType.dataType("INTEGER UNSIGNED", BigintType.BIGINT, Objects::toString);
        DataType<BigDecimal> mysqlUnsignedBigint = DataType.dataType("BIGINT UNSIGNED", createDecimalType(20), Objects::toString);

        DataTypeTest.create()
                .addRoundTrip(mysqlUnsignedTinyInt, (short) 255)
                .addRoundTrip(mysqlUnsignedSmallInt, 65_535)
                .addRoundTrip(mysqlUnsignedInt, 4_294_967_295L)
                .addRoundTrip(mysqlUnsignedInteger, 4_294_967_295L)
                .addRoundTrip(mysqlUnsignedBigint, new BigDecimal("18446744073709551615"))
                .execute(getQueryRunner(), mysqlCreateAndInsert("tpch.mysql_test_unsigned"));
    }

    private static DataTypeTest singlePrecisionFloatingPointTests(DataType<Float> floatType)
    {
        // we are not testing Nan/-Infinity/+Infinity as those are not supported by MySQL
        return DataTypeTest.create()
                .addRoundTrip(floatType, 3.14f)
                // .addRoundTrip(floatType, 3.1415927f) // Overeagerly rounded by mysql to 3.14159
                .addRoundTrip(floatType, null);
    }

    private static DataTypeTest doublePrecisionFloatingPointTests(DataType<Double> doubleType)
    {
        // we are not testing Nan/-Infinity/+Infinity as those are not supported by MySQL
        return DataTypeTest.create()
                .addRoundTrip(doubleType, 1.0e100d)
                .addRoundTrip(doubleType, 123.456E10)
                .addRoundTrip(doubleType, null);
    }

    private void testUnsupportedDataType(String databaseDataType)
    {
        SqlExecutor jdbcSqlExecutor = mysqlServer::execute;
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

    private DataSetup mysqlCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(mysqlServer::execute, tableNamePrefix);
    }

    private static DataType<String> jsonDataType(Function<String, String> toLiteral)
    {
        return dataType(
                "json",
                JSON,
                toLiteral);
    }

    private static DataType<Float> mysqlFloatDataType()
    {
        return dataType("float", RealType.REAL, Object::toString);
    }

    private static DataType<Double> mysqlDoubleDataType()
    {
        return dataType("double precision", DoubleType.DOUBLE, Object::toString);
    }

    private static DataType<byte[]> mysqlBinaryDataType(String insertType)
    {
        return dataType(
                insertType,
                VARBINARY,
                bytes -> "X'" + base16().encode(bytes) + "'",
                DataType::binaryLiteral,
                identity());
    }
}
