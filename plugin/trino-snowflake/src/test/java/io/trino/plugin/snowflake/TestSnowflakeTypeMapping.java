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
package io.trino.plugin.snowflake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingSession;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.datatype.CreateAsSelectDataSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.datatype.SqlDataTypeTest;
import io.trino.testing.sql.TrinoSqlExecutor;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.time.ZoneId;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.snowflake.SnowflakeQueryRunner.createSnowflakeQueryRunner;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.time.ZoneOffset.UTC;

public class TestSnowflakeTypeMapping
        extends AbstractTestQueryFramework
{
    protected TestingSnowflakeServer snowflakeServer;

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
        snowflakeServer = new TestingSnowflakeServer();
        return createSnowflakeQueryRunner(
                snowflakeServer,
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableList.of());
    }

    @Test
    public void testBoolean()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("boolean", "true", BOOLEAN, "BOOLEAN '1'")
                .addRoundTrip("boolean", "false", BOOLEAN, "BOOLEAN '0'")
                .addRoundTrip("boolean", "NULL", BOOLEAN, "CAST(NULL AS BOOLEAN)")
                .execute(getQueryRunner(), snowflakeCreateAndInsert("tpch.test_boolean"))
                .execute(getQueryRunner(), trinoCreateAsSelect("tpch.test_boolean"));
    }

    @Test(dataProvider = "snowflakeIntegerTypeProvider")
    public void testInteger(String inputType)
    {
        SqlDataTypeTest.create()
                .addRoundTrip(inputType, "-9223372036854775808", BIGINT, "-9223372036854775808")
                .addRoundTrip(inputType, "9223372036854775807", BIGINT, "9223372036854775807")
                .addRoundTrip(inputType, "0", BIGINT, "CAST(0 AS BIGINT)")
                .addRoundTrip(inputType, "NULL", BIGINT, "CAST(NULL AS BIGINT)")
                .execute(getQueryRunner(), snowflakeCreateAndInsert("tpch.integer"));
    }

    @DataProvider
    public Object[][] snowflakeIntegerTypeProvider()
    {
        // INT , INTEGER , BIGINT , SMALLINT , TINYINT , BYTEINT, DECIMAL , NUMERIC are aliases for NUMBER(38, 0) in snowflake
        // https://docs.snowflake.com/en/sql-reference/data-types-numeric.html#int-integer-bigint-smallint-tinyint-byteint
        return new Object[][] {
                {"INT"},
                {"INTEGER"},
                {"BIGINT"},
                {"SMALLINT"},
                {"TINYINT"},
                {"BYTEINT"},
        };
    }

    @Test
    public void testDecimal()
    {
        // TODO: Still can't pass the test with a long value
        // Snowflake will convert DECIMAL to BIGINT, except DECIMAL with scale > 0
        // https://docs.snowflake.com/en/sql-reference/data-types-numeric.html#impact-of-precision-and-scale-on-storage-size
        SqlDataTypeTest.create()
                .addRoundTrip("decimal(3, 0)", "NULL", BIGINT, "CAST(NULL AS BIGINT)")
                .addRoundTrip("decimal(3, 0)", "CAST('193' AS decimal(3, 0))", BIGINT, "CAST('193' AS BIGINT)")
                .addRoundTrip("decimal(3, 0)", "CAST('19' AS decimal(3, 0))", BIGINT, "CAST('19' AS BIGINT)")
                .addRoundTrip("decimal(3, 0)", "CAST('-193' AS decimal(3, 0))", BIGINT, "CAST('-193' AS BIGINT)")
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
//                .addRoundTrip("decimal(38, 0)", "CAST('27182818284590452353602874713526624977' AS decimal(38, 0))", BIGINT, "CAST('27182818284590452353602874713526624977' AS BIGINT)")
//                .addRoundTrip("decimal(38, 0)", "CAST('-27182818284590452353602874713526624977' AS decimal(38, 0))", BIGINT, "CAST('-27182818284590452353602874713526624977' AS BIGINT)")
                .addRoundTrip("decimal(38, 0)", "CAST(NULL AS decimal(38, 0))", BIGINT, "CAST(NULL AS BIGINT)")
                .execute(getQueryRunner(), snowflakeCreateAndInsert("tpch.test_decimal"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_decimal"));
    }

    @Test
    public void testFloat()
    {
        // https://docs.snowflake.com/en/sql-reference/data-types-numeric.html#float-float4-float8
        SqlDataTypeTest.create()
                .addRoundTrip("real", "3.14", DOUBLE, "DOUBLE '3.14'")
                .addRoundTrip("real", "10.3e0", DOUBLE, "DOUBLE '10.3e0'")
                .addRoundTrip("real", "NULL", DOUBLE, "CAST(NULL AS DOUBLE)")
                .addRoundTrip("real", "CAST('NaN' AS DOUBLE)", DOUBLE, "nan()")
                .addRoundTrip("real", "CAST('Infinity' AS DOUBLE)", DOUBLE, "+infinity()")
                .addRoundTrip("real", "CAST('-Infinity' AS DOUBLE)", DOUBLE, "-infinity()")
                .execute(getQueryRunner(), trinoCreateAsSelect("tpch.test_real"));

        SqlDataTypeTest.create()
                .addRoundTrip("float", "3.14", DOUBLE, "DOUBLE '3.14'")
                .addRoundTrip("float", "10.3e0", DOUBLE, "DOUBLE '10.3e0'")
                .addRoundTrip("float", "NULL", DOUBLE, "CAST(NULL AS DOUBLE)")
                .addRoundTrip("float", "CAST('NaN' AS float)", DOUBLE, "nan()")
                .addRoundTrip("float", "CAST('Infinity' AS float)", DOUBLE, "+infinity()")
                .addRoundTrip("float", "CAST('-Infinity' AS float)", DOUBLE, "-infinity()")
                .execute(getQueryRunner(), snowflakeCreateAndInsert("tpch.test_float"));
    }

    @Test
    public void testDouble()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("double", "3.14", DOUBLE, "CAST(3.14 AS DOUBLE)")
                .addRoundTrip("double", "1.0E100", DOUBLE, "1.0E100")
                .addRoundTrip("double", "1.23456E12", DOUBLE, "1.23456E12")
                .addRoundTrip("double", "NULL", DOUBLE, "CAST(NULL AS DOUBLE)")
                .addRoundTrip("double", "CAST('NaN' AS DOUBLE)", DOUBLE, "nan()")
                .addRoundTrip("double", "CAST('Infinity' AS DOUBLE)", DOUBLE, "+infinity()")
                .addRoundTrip("double", "CAST('-Infinity' AS DOUBLE)", DOUBLE, "-infinity()")
                .execute(getQueryRunner(), trinoCreateAsSelect("trino_test_double"))
                .execute(getQueryRunner(), snowflakeCreateAndInsert("tpch.test_double"));
    }

    @Test
    public void testSnowflakeCreatedParameterizedVarchar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("text", "'b'", createVarcharType(16777216), "CAST('b' AS VARCHAR(16777216))")
                .addRoundTrip("varchar(32)", "'e'", createVarcharType(32), "CAST('e' AS VARCHAR(32))")
                .addRoundTrip("varchar(15000)", "'f'", createVarcharType(15000), "CAST('f' AS VARCHAR(15000))")
                .execute(getQueryRunner(), snowflakeCreateAndInsert("tpch.snowflake_test_parameterized_varchar"));
    }

    @Test
    public void testSnowflakeCreatedParameterizedVarcharUnicode()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("text collate \'utf8\'", "'攻殻機動隊'", createVarcharType(16777216), "CAST('攻殻機動隊' AS VARCHAR(16777216))")
                .addRoundTrip("varchar(5) collate \'utf8\'", "'攻殻機動隊'", createVarcharType(5), "CAST('攻殻機動隊' AS VARCHAR(5))")
                .addRoundTrip("varchar(32) collate \'utf8\'", "'攻殻機動隊'", createVarcharType(32), "CAST('攻殻機動隊' AS VARCHAR(32))")
                .addRoundTrip("varchar(20000) collate \'utf8\'", "'攻殻機動隊'", createVarcharType(20000), "CAST('攻殻機動隊' AS VARCHAR(20000))")
                .addRoundTrip("varchar(1) collate \'utf8mb4\'", "'😂'", createVarcharType(1), "CAST('😂' AS VARCHAR(1))")
                .addRoundTrip("varchar(77) collate \'utf8mb4\'", "'Ну, погоди!'", createVarcharType(77), "CAST('Ну, погоди!' AS VARCHAR(77))")
                .execute(getQueryRunner(), snowflakeCreateAndInsert("tpch.snowflake_test_parameterized_varchar_unicode"));
    }

    @Test
    public void testParameterizedChar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("char", "''", createVarcharType(1), "CAST('' AS varchar(1))")
                .addRoundTrip("char", "'a'", createVarcharType(1), "CAST('a' AS varchar(1))")
                .addRoundTrip("char(1)", "''", createVarcharType(1), "CAST('' AS varchar(1))")
                .addRoundTrip("char(1)", "'a'", createVarcharType(1), "CAST('a' AS varchar(1))")
                .addRoundTrip("char(8)", "'abc'", createVarcharType(8), "CAST('abc' AS varchar(8))")
                .addRoundTrip("char(8)", "'12345678'", createVarcharType(8), "CAST('12345678' AS varchar(8))")
                .execute(getQueryRunner(), trinoCreateAsSelect("snowflake_test_parameterized_char"))
                .execute(getQueryRunner(), snowflakeCreateAndInsert("tpch.snowflake_test_parameterized_char"));
    }

    @Test
    public void testSnowflakeParameterizedCharUnicode()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("char(1) collate \'utf8\'", "'攻'", createVarcharType(1), "CAST('攻' AS VARCHAR(1))")
                .addRoundTrip("char(5) collate \'utf8\'", "'攻殻'", createVarcharType(5), "CAST('攻殻' AS VARCHAR(5))")
                .addRoundTrip("char(5) collate \'utf8\'", "'攻殻機動隊'", createVarcharType(5), "CAST('攻殻機動隊' AS VARCHAR(5))")
                .addRoundTrip("char(1)", "'😂'", createVarcharType(1), "CAST('😂' AS VARCHAR(1))")
                .addRoundTrip("char(77)", "'Ну, погоди!'", createVarcharType(77), "CAST('Ну, погоди!' AS VARCHAR(77))")
                .execute(getQueryRunner(), snowflakeCreateAndInsert("tpch.snowflake_test_parameterized_char"));
    }

    @Test
    public void testBinary()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("binary(18)", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("binary(18)", "X''", VARBINARY, "X''")
                .addRoundTrip("binary(18)", "X'68656C6C6F'", VARBINARY, "to_utf8('hello')")
                .addRoundTrip("binary(18)", "X'C582C4856B61207720E69DB1E4BAACE983BD'", VARBINARY, "to_utf8('łąka w 東京都')") // no trailing zeros
                .addRoundTrip("binary(18)", "X'4261672066756C6C206F6620F09F92B0'", VARBINARY, "to_utf8('Bag full of 💰')")
                .addRoundTrip("binary(18)", "X'0001020304050607080DF9367AA7000000'", VARBINARY, "X'0001020304050607080DF9367AA7000000'") // non-text prefix
                .addRoundTrip("binary(18)", "X'000000000000'", VARBINARY, "X'000000000000'")
                .execute(getQueryRunner(), snowflakeCreateAndInsert("tpch.test_binary"));
    }

    @Test
    public void testVarbinary()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("varbinary", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("varbinary", "X''", VARBINARY, "X''")
                .addRoundTrip("varbinary", "X'68656C6C6F'", VARBINARY, "to_utf8('hello')")
                .addRoundTrip("varbinary", "X'5069C4996B6E6120C582C4856B61207720E69DB1E4BAACE983BD'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip("varbinary", "X'4261672066756C6C206F6620F09F92B0'", VARBINARY, "to_utf8('Bag full of 💰')")
                .addRoundTrip("varbinary", "X'0001020304050607080DF9367AA7000000'", VARBINARY, "X'0001020304050607080DF9367AA7000000'") // non-text
                .addRoundTrip("varbinary", "X'000000000000'", VARBINARY, "X'000000000000'")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_varbinary"))
                .execute(getQueryRunner(), snowflakeCreateAndInsert("tpch.test_varbinary"));
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    public void testDate(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(getTimeZoneKey(sessionZone.getId()))
                .build();

        SqlDataTypeTest.create()
                .addRoundTrip("date", "NULL", DATE, "CAST(NULL AS DATE)")
                // TODO: Expanding the min and max value of a test case by using the input of String
//                .addRoundTrip("date", "DATE '0000-01-01'", DATE, "DATE '0000-01-01'") // Min value for the input of string only.
//                .addRoundTrip("date", "DATE '99999-12-31'", DATE, "DATE '99999-12-31'") // Max value for the input of string only.
                .addRoundTrip("date", "DATE '0001-01-01'", DATE, "DATE '0001-01-01'") // Min value for the function Date.
                .addRoundTrip("date", "DATE '1582-10-05'", DATE, "DATE '1582-10-05'")
                .addRoundTrip("date", "DATE '1582-10-14'", DATE, "DATE '1582-10-14'")
                .addRoundTrip("date", "DATE '9999-12-31'", DATE, "DATE '9999-12-31'") // Max value for the function Date.
                .addRoundTrip("date", "DATE '1952-04-03'", DATE, "DATE '1952-04-03'")
                .addRoundTrip("date", "DATE '1970-01-01'", DATE, "DATE '1970-01-01'")
                .addRoundTrip("date", "DATE '1970-02-03'", DATE, "DATE '1970-02-03'")
                .addRoundTrip("date", "DATE '1970-01-01'", DATE, "DATE '1970-01-01'")
                .addRoundTrip("date", "DATE '1983-04-01'", DATE, "DATE '1983-04-01'")
                .addRoundTrip("date", "DATE '1983-10-01'", DATE, "DATE '1983-10-01'")
                .addRoundTrip("date", "DATE '2017-07-01'", DATE, "DATE '2017-07-01'") // summer on northern hemisphere (possible DST)
                .addRoundTrip("date", "DATE '2017-01-01'", DATE, "DATE '2017-01-01'") // winter on northern hemisphere (possible DST on southern hemisphere)
                .execute(getQueryRunner(), session, trinoCreateAsSelect("test_date"))
                .execute(getQueryRunner(), session, snowflakeCreateAndInsert("tpch.test_date"));
    }

    @DataProvider
    public Object[][] sessionZonesDataProvider()
    {
        return new Object[][] {
                {UTC},
                {jvmZone},
                {vilnius},
                {kathmandu},
                {ZoneId.of(TestingSession.DEFAULT_TIME_ZONE_KEY.getId())},
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

    private DataSetup trinoCreateAndInsert(Session session, String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(new TrinoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    private DataSetup snowflakeCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(snowflakeServer::execute, tableNamePrefix);
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
