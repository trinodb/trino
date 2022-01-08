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
package io.trino.plugin.phoenix;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.jdbc.UnsupportedTypeHandling;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingSession;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.datatype.CreateAsSelectDataSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.datatype.DataType;
import io.trino.testing.datatype.DataTypeTest;
import io.trino.testing.datatype.SqlDataTypeTest;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TrinoSqlExecutor;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.jdbc.DecimalConfig.DecimalMapping.ALLOW_OVERFLOW;
import static io.trino.plugin.jdbc.DecimalConfig.DecimalMapping.STRICT;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.DECIMAL_DEFAULT_SCALE;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.DECIMAL_MAPPING;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.DECIMAL_ROUNDING_MODE;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.UNSUPPORTED_TYPE_HANDLING;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.plugin.phoenix.PhoenixQueryRunner.createPhoenixQueryRunner;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.datatype.DataType.dataType;
import static java.lang.String.format;
import static java.math.RoundingMode.HALF_UP;
import static java.math.RoundingMode.UNNECESSARY;
import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * @see <a href="https://phoenix.apache.org/language/datatypes.html">Phoenix data types</a>
 */
public class TestPhoenixTypeMapping
        extends AbstractTestQueryFramework
{
    private TestingPhoenixServer phoenixServer;

    private final ZoneId jvmZone = ZoneId.systemDefault();
    // no DST in 1970, but has DST in later years (e.g. 2018)
    private final ZoneId vilnius = ZoneId.of("Europe/Vilnius");
    // minutes offset change since 1970-01-01, no DST
    private final ZoneId kathmandu = ZoneId.of("Asia/Kathmandu");

    @BeforeClass
    public void setUp()
    {
        checkState(jvmZone.getId().equals("America/Bahia_Banderas"), "This test assumes certain JVM time zone");
        LocalDate dateOfLocalTimeChangeForwardAtMidnightInJvmZone = LocalDate.of(1970, 1, 1);
        checkIsGap(jvmZone, dateOfLocalTimeChangeForwardAtMidnightInJvmZone.atStartOfDay());

        LocalDate dateOfLocalTimeChangeForwardAtMidnightInSomeZone = LocalDate.of(1983, 4, 1);
        checkIsGap(vilnius, dateOfLocalTimeChangeForwardAtMidnightInSomeZone.atStartOfDay());
        LocalDate dateOfLocalTimeChangeBackwardAtMidnightInSomeZone = LocalDate.of(1983, 10, 1);
        checkIsDoubled(vilnius, dateOfLocalTimeChangeBackwardAtMidnightInSomeZone.atStartOfDay().minusMinutes(1));

        checkIsGap(kathmandu, LocalDate.of(1986, 1, 1).atStartOfDay());
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        phoenixServer = TestingPhoenixServer.getInstance();
        return createPhoenixQueryRunner(phoenixServer, ImmutableMap.of(), ImmutableList.of());
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        TestingPhoenixServer.shutDown();
    }

    @Test
    public void testBasicTypes()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("boolean", "true", BOOLEAN, "true")
                .addRoundTrip("boolean", "false", BOOLEAN, "false")
                .addRoundTrip("bigint", "123456789012", BIGINT, "123456789012")
                .addRoundTrip("integer", "1234567890", INTEGER, "1234567890")
                .addRoundTrip("smallint", "32456", SMALLINT, "SMALLINT '32456'")
                .addRoundTrip("tinyint", "5", TINYINT, "TINYINT '5'")
                .addRoundTrip("double", "123.45", DOUBLE, "DOUBLE '123.45'")
                .addRoundTrip("real", "123.45", REAL, "REAL '123.45'")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_basic_types"));
    }

    @Test
    public void testBoolean()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("boolean", "true", BOOLEAN, "true")
                .addRoundTrip("boolean", "false", BOOLEAN, "false")
                .addRoundTrip("boolean", "NULL", BOOLEAN, "CAST(NULL AS BOOLEAN)")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_boolean"))

                .addRoundTrip("integer primary key", "1", INTEGER, "1")
                .execute(getQueryRunner(), phoenixCreateAndInsert("tpch.test_boolean"));
    }

    @Test
    public void testTinyInt()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("tinyint", "-128", TINYINT, "TINYINT '-128'") // min value in Phoenix
                .addRoundTrip("tinyint", "0", TINYINT, "TINYINT '0'")
                .addRoundTrip("tinyint", "127", TINYINT, "TINYINT '127'") // max value in Phoenix
                .addRoundTrip("tinyint", "NULL", TINYINT, "CAST(NULL AS TINYINT)")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_tinyint"))

                .addRoundTrip("integer primary key", "1", INTEGER, "1")
                .execute(getQueryRunner(), phoenixCreateAndInsert("tpch.test_tinyint"));
    }

    @Test
    public void testUnsignedTinyInt()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("unsigned_tinyint", "0", TINYINT, "TINYINT '0'") // min value in Phoenix
                .addRoundTrip("unsigned_tinyint", "127", TINYINT, "TINYINT '127'") // max value in Phoenix
                .addRoundTrip("unsigned_tinyint", "NULL", TINYINT, "CAST(NULL AS TINYINT)")
                .addRoundTrip("integer primary key", "1", INTEGER, "1")
                .execute(getQueryRunner(), phoenixCreateAndInsert("tpch.test_unsigned_tinyint"));
    }

    @Test
    public void testSmallInt()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("smallint", "-32768", SMALLINT, "SMALLINT '-32768'") // min value in Phoenix
                .addRoundTrip("smallint", "0", SMALLINT, "SMALLINT '0'")
                .addRoundTrip("smallint", "32767", SMALLINT, "SMALLINT '32767'") // max value in Phoenix
                .addRoundTrip("smallint", "NULL", SMALLINT, "CAST(NULL AS SMALLINT)")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_smallint"))

                .addRoundTrip("integer primary key", "1", INTEGER, "1")
                .execute(getQueryRunner(), phoenixCreateAndInsert("tpch.test_smallint"));
    }

    @Test
    public void testUnsignedSmallInt()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("unsigned_smallint", "0", SMALLINT, "SMALLINT '0'") // min value in Phoenix
                .addRoundTrip("unsigned_smallint", "32767", SMALLINT, "SMALLINT '32767'") // max value in Phoenix
                .addRoundTrip("unsigned_smallint", "NULL", SMALLINT, "CAST(NULL AS SMALLINT)")
                .addRoundTrip("integer primary key", "1", INTEGER, "1")
                .execute(getQueryRunner(), phoenixCreateAndInsert("tpch.test_unsigned_smallint"));
    }

    @Test
    public void testInteger()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("integer", "-2147483648", INTEGER, "-2147483648") // min value in Phoenix
                .addRoundTrip("integer", "0", INTEGER, "0")
                .addRoundTrip("integer", "2147483647", INTEGER, "2147483647") // max value in Phoenix
                .addRoundTrip("integer", "NULL", INTEGER, "CAST(NULL AS INTEGER)")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_integer"))

                .addRoundTrip("integer primary key", "1", INTEGER, "1")
                .execute(getQueryRunner(), phoenixCreateAndInsert("tpch.test_integer"));
    }

    @Test
    public void testUnsignedInt()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("unsigned_int", "0", INTEGER, "0") // min value in Phoenix
                .addRoundTrip("unsigned_int", "2147483647", INTEGER, "2147483647") // max value in Phoenix
                .addRoundTrip("unsigned_int", "NULL", INTEGER, "CAST(NULL AS INTEGER)")
                .addRoundTrip("integer primary key", "1", INTEGER, "1")
                .execute(getQueryRunner(), phoenixCreateAndInsert("tpch.test_unsigned_int"));
    }

    @Test
    public void testBigInt()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("bigint", "-9223372036854775808", BIGINT, "-9223372036854775808") // min value in Phoenix
                .addRoundTrip("bigint", "0", BIGINT, "BIGINT '0'")
                .addRoundTrip("bigint", "9223372036854775807", BIGINT, "9223372036854775807") // max value in Phoenix
                .addRoundTrip("bigint", "NULL", BIGINT, "CAST(NULL AS BIGINT)")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_bigint"))

                .addRoundTrip("integer primary key", "1", INTEGER, "1")
                .execute(getQueryRunner(), phoenixCreateAndInsert("tpch.test_bigint"));
    }

    @Test
    public void testUnsignedLong()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("unsigned_long", "0", BIGINT, "BIGINT '0'") // min value in Phoenix
                .addRoundTrip("unsigned_long", "9223372036854775807", BIGINT, "BIGINT '9223372036854775807'") // max value in Phoenix
                .addRoundTrip("unsigned_long", "NULL", BIGINT, "CAST(NULL AS BIGINT)")
                .addRoundTrip("integer primary key", "1", INTEGER, "1")
                .execute(getQueryRunner(), phoenixCreateAndInsert("tpch.test_unsigned_long"));
    }

    @Test
    public void testFloat()
    {
        // Not testing Nan/-Infinity/+Infinity as those are not supported by Phoenix
        SqlDataTypeTest.create()
                .addRoundTrip("real", "REAL '-3.402823466E38'", REAL, "REAL '-3.402823466E38'") // min value in Phoenix
                .addRoundTrip("real", "REAL '0.0'", REAL, "REAL '0.0'")
                .addRoundTrip("real", "REAL '123.456E10'", REAL, "REAL '123.456E10'")
                .addRoundTrip("real", "REAL '3.402823466E38'", REAL, "REAL '3.402823466E38'") // max value in Phoenix
                .addRoundTrip("real", "NULL", REAL, "CAST(NULL AS REAL)")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_float"));

        SqlDataTypeTest.create()
                .addRoundTrip("float", "-3.402823466E38", REAL, "REAL '-3.402823466E38'") // min value in Phoenix
                .addRoundTrip("float", "0.0", REAL, "REAL '0.0'")
                .addRoundTrip("float", "123.456E10", REAL, "REAL '123.456E10'")
                .addRoundTrip("float", "3.402823466E38", REAL, "REAL '3.402823466E38'") // max value in Phoenix
                .addRoundTrip("float", "NULL", REAL, "CAST(NULL AS REAL)")
                .addRoundTrip("integer primary key", "1", INTEGER, "1")
                .execute(getQueryRunner(), phoenixCreateAndInsert("tpch.test_float"));
    }

    @Test
    public void testUnsignedFloat()
    {
        // Not testing Nan/-Infinity/+Infinity as those are not supported by Phoenix
        SqlDataTypeTest.create()
                .addRoundTrip("unsigned_float", "0.0", REAL, "REAL '0.0'") // min value in Phoenix
                .addRoundTrip("unsigned_float", "123.456E10", REAL, "REAL '123.456E10'")
                .addRoundTrip("unsigned_float", "3.402823466E38", REAL, "REAL '3.402823466E38'") // max value in Phoenix
                .addRoundTrip("unsigned_float", "NULL", REAL, "CAST(NULL AS REAL)")
                .addRoundTrip("integer primary key", "1", INTEGER, "1")
                .execute(getQueryRunner(), phoenixCreateAndInsert("tpch.test_unsigned_float"));
    }

    @Test
    public void testDouble()
    {
        // Not testing Nan/-Infinity/+Infinity as those are not supported by Phoenix
        SqlDataTypeTest.create()
                .addRoundTrip("double", "-1.7976931348623158E308", DOUBLE, "DOUBLE '-1.7976931348623158E308'") // min value in Phoenix
                .addRoundTrip("double", "0.0", DOUBLE, "DOUBLE '0.0'")
                .addRoundTrip("double", "1.0E100", DOUBLE, "DOUBLE '1.0E100'")
                .addRoundTrip("double", "123.456E10", DOUBLE, "DOUBLE '123.456E10'")
                .addRoundTrip("double", "1.7976931348623158E308", DOUBLE, "DOUBLE '1.7976931348623158E308'") // max value in Phoenix
                .addRoundTrip("double", "NULL", DOUBLE, "CAST(NULL AS DOUBLE)")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_double"))

                .addRoundTrip("integer primary key", "1", INTEGER, "1")
                .execute(getQueryRunner(), phoenixCreateAndInsert("tpch.test_double"));
    }

    @Test
    public void testUnsignedDouble()
    {
        // Not testing Nan/-Infinity/+Infinity as those are not supported by Phoenix
        SqlDataTypeTest.create()
                .addRoundTrip("unsigned_double", "0.0", DOUBLE, "DOUBLE '0.0'") // min value in Phoenix
                .addRoundTrip("unsigned_double", "1.0E100", DOUBLE, "DOUBLE '1.0E100'")
                .addRoundTrip("unsigned_double", "123.456E10", DOUBLE, "DOUBLE '123.456E10'")
                .addRoundTrip("unsigned_double", "1.7976931348623158E308", DOUBLE, "DOUBLE '1.7976931348623158E308'") // max value in Phoenix
                .addRoundTrip("unsigned_double", "NULL", DOUBLE, "CAST(NULL AS DOUBLE)")

                .addRoundTrip("integer primary key", "1", INTEGER, "1")
                .execute(getQueryRunner(), phoenixCreateAndInsert("tpch.test_unsigned_double"));
    }

    @Test
    public void testVarchar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("varchar(10)", "'text_a'", createVarcharType(10), "CAST('text_a' AS VARCHAR(10))")
                .addRoundTrip("varchar(255)", "'text_b'", createVarcharType(255), "CAST('text_b' AS VARCHAR(255))")
                .addRoundTrip("varchar(65535)", "'text_d'", createVarcharType(65535), "CAST('text_d' AS VARCHAR(65535))")
                .addRoundTrip("varchar(10485760)", "'text_f'", createVarcharType(10485760), "CAST('text_f' AS VARCHAR(10485760))")
                .addRoundTrip("varchar", "'unbounded'", VARCHAR, "VARCHAR 'unbounded'")
                .addRoundTrip("varchar(10)", "NULL", createVarcharType(10), "CAST(NULL AS VARCHAR(10))")
                .addRoundTrip("varchar", "NULL", VARCHAR, "CAST(NULL AS VARCHAR)")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_varchar"))

                .addRoundTrip("integer primary key", "1", INTEGER, "1")
                .execute(getQueryRunner(), phoenixCreateAndInsert("tpch.test_varchar"));
    }

    @Test
    public void testChar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("char(10)", "'text_a'", createCharType(10), "CAST('text_a' AS CHAR(10))")
                .addRoundTrip("char(255)", "'text_b'", createCharType(255), "CAST('text_b' AS CHAR(255))")
                .addRoundTrip("char(65536)", "'text_e'", createCharType(CharType.MAX_LENGTH), "CAST('text_e' AS CHAR(65536))")
                .addRoundTrip("char(10)", "NULL", createCharType(10), "CAST(NULL AS CHAR(10))")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_char"))

                .addRoundTrip("integer primary key", "1", INTEGER, "1")
                .execute(getQueryRunner(), phoenixCreateAndInsert("tpch.test_char"));
    }

    @Test
    public void testBinary()
    {
        // Not testing max length (2147483647) because it leads to 'Requested array size exceeds VM limit'
        SqlDataTypeTest.create()
                .addRoundTrip("binary(1)", "NULL", VARBINARY, "X'00'") // NULL stored as zeros
                .addRoundTrip("binary(10)", "DECODE('', 'HEX')", VARBINARY, "X'00000000000000000000'") // empty stored as zeros
                .addRoundTrip("binary(5)", "DECODE('68656C6C6F', 'HEX')", VARBINARY, "to_utf8('hello')")
                .addRoundTrip("binary(26)", "DECODE('5069C4996B6E6120C582C4856B61207720E69DB1E4BAACE983BD', 'HEX')", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip("binary(16)", "DECODE('4261672066756C6C206F6620F09F92B0', 'HEX')", VARBINARY, "to_utf8('Bag full of 💰')")
                .addRoundTrip("binary(17)", "DECODE('0001020304050607080DF9367AA7000000', 'HEX')", VARBINARY, "X'0001020304050607080DF9367AA7000000'") // non-text
                .addRoundTrip("binary(6)", "DECODE('000000000000', 'HEX')", VARBINARY, "X'000000000000'")
                .addRoundTrip("integer primary key", "1", INTEGER, "1")
                .execute(getQueryRunner(), phoenixCreateAndInsert("tpch.test_binary"));

        // Verify 'IS NULL' doesn't get rows where the value is X'00...' padded in Phoenix
        try (TestTable table = new TestTable(new PhoenixSqlExecutor(phoenixServer.getJdbcUrl()), "tpch.test_binary", "(null_binary binary(1), empty_binary binary(10), pk integer primary key)", ImmutableList.of("NULL, DECODE('', 'HEX'), 1"))) {
            assertQueryReturnsEmptyResult(format("SELECT * FROM %s WHERE null_binary IS NULL", table.getName()));
            assertQueryReturnsEmptyResult(format("SELECT * FROM %s WHERE empty_binary IS NULL", table.getName()));
        }
    }

    @Test
    public void testVarbinary()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("varbinary", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("varbinary", "X''", VARBINARY, "CAST(NULL AS varbinary)") // empty stored as NULL
                .addRoundTrip("varbinary", "X'68656C6C6F'", VARBINARY, "to_utf8('hello')")
                .addRoundTrip("varbinary", "X'5069C4996B6E6120C582C4856B61207720E69DB1E4BAACE983BD'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip("varbinary", "X'4261672066756C6C206F6620F09F92B0'", VARBINARY, "to_utf8('Bag full of 💰')")
                .addRoundTrip("varbinary", "X'0001020304050607080DF9367AA7000000'", VARBINARY, "X'0001020304050607080DF9367AA7000000'") // non-text
                .addRoundTrip("varbinary", "X'000000000000'", VARBINARY, "X'000000000000'")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_varbinary"));

        SqlDataTypeTest.create()
                .addRoundTrip("integer primary key", "1", INTEGER, "1")
                .addRoundTrip("varbinary", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("varbinary", "DECODE('', 'HEX')", VARBINARY, "CAST(NULL AS varbinary)") // empty stored as NULL
                .addRoundTrip("varbinary", "DECODE('68656C6C6F', 'HEX')", VARBINARY, "to_utf8('hello')")
                .addRoundTrip("varbinary", "DECODE('5069C4996B6E6120C582C4856B61207720E69DB1E4BAACE983BD', 'HEX')", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip("varbinary", "DECODE('4261672066756C6C206F6620F09F92B0', 'HEX')", VARBINARY, "to_utf8('Bag full of 💰')")
                .addRoundTrip("varbinary", "DECODE('0001020304050607080DF9367AA7000000', 'HEX')", VARBINARY, "X'0001020304050607080DF9367AA7000000'") // non-text
                .addRoundTrip("varbinary", "DECODE('000000000000', 'HEX')", VARBINARY, "X'000000000000'")
                .execute(getQueryRunner(), phoenixCreateAndInsert("tpch.test_varbinary"));
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
                .addRoundTrip("decimal(3, 2)", "CAST('3.14' AS decimal(3, 2))", createDecimalType(3, 2), "CAST('3.14' AS decimal(3, 2))")
                .addRoundTrip("decimal(4, 2)", "CAST('2' AS decimal(4, 2))", createDecimalType(4, 2), "CAST('2' AS decimal(4, 2))")
                .addRoundTrip("decimal(4, 2)", "CAST('2.3' AS decimal(4, 2))", createDecimalType(4, 2), "CAST('2.3' AS decimal(4, 2))")
                .addRoundTrip("decimal(24, 2)", "CAST('2' AS decimal(24, 2))", createDecimalType(24, 2), "CAST('2' AS decimal(24, 2))")
                .addRoundTrip("decimal(24, 2)", "CAST('2.3' AS decimal(24, 2))", createDecimalType(24, 2), "CAST('2.3' AS decimal(24, 2))")
                .addRoundTrip("decimal(24, 2)", "CAST('123456789.3' AS decimal(24, 2))", createDecimalType(24, 2), "CAST('123456789.3' AS decimal(24, 2))")
                .addRoundTrip("decimal(24, 4)", "CAST('12345678901234567890.31' AS decimal(24, 4))", createDecimalType(24, 4), "CAST('12345678901234567890.31' AS decimal(24, 4))")
                .addRoundTrip("decimal(24, 23)", "CAST('3.12345678901234567890123' AS decimal(24, 23))", createDecimalType(24, 23), "CAST('3.12345678901234567890123' AS decimal(24, 23))")
                .addRoundTrip("decimal(30, 5)", "CAST('3141592653589793238462643.38327' AS decimal(30, 5))", createDecimalType(30, 5), "CAST('3141592653589793238462643.38327' AS decimal(30, 5))")
                .addRoundTrip("decimal(30, 5)", "CAST('-3141592653589793238462643.38327' AS decimal(30, 5))", createDecimalType(30, 5), "CAST('-3141592653589793238462643.38327' AS decimal(30, 5))")
                .addRoundTrip("decimal(38, 0)", "CAST('27182818284590452353602874713526624977' AS decimal(38, 0))", createDecimalType(38, 0), "CAST('27182818284590452353602874713526624977' AS decimal(38, 0))")
                .addRoundTrip("decimal(38, 0)", "CAST('-27182818284590452353602874713526624977' AS decimal(38, 0))", createDecimalType(38, 0), "CAST('-27182818284590452353602874713526624977' AS decimal(38, 0))")
                .addRoundTrip("decimal(3, 0)", "NULL", createDecimalType(3, 0), "CAST(NULL AS decimal(3, 0))")
                .addRoundTrip("decimal(38, 0)", "CAST(NULL AS decimal(38, 0))", createDecimalType(38, 0), "CAST(NULL AS decimal(38, 0))")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_decimal"));

        SqlDataTypeTest.create()
                .addRoundTrip("decimal(3, 0)", "CAST(193 AS decimal(3, 0))", createDecimalType(3, 0), "CAST('193' AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 0)", "CAST(19 AS decimal(3, 0))", createDecimalType(3, 0), "CAST('19' AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 0)", "CAST(-193 AS decimal(3, 0))", createDecimalType(3, 0), "CAST('-193' AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 1)", "CAST(10.0 AS decimal(3, 1))", createDecimalType(3, 1), "CAST('10.0' AS decimal(3, 1))")
                .addRoundTrip("decimal(3, 1)", "CAST(10.1 AS decimal(3, 1))", createDecimalType(3, 1), "CAST('10.1' AS decimal(3, 1))")
                .addRoundTrip("decimal(3, 1)", "CAST(-10.1 AS decimal(3, 1))", createDecimalType(3, 1), "CAST('-10.1' AS decimal(3, 1))")
                .addRoundTrip("decimal(3, 2)", "CAST(3.14 AS decimal(3, 2))", createDecimalType(3, 2), "CAST('3.14' AS decimal(3, 2))")
                .addRoundTrip("decimal(4, 2)", "CAST(2 AS decimal(4, 2))", createDecimalType(4, 2), "CAST('2' AS decimal(4, 2))")
                .addRoundTrip("decimal(4, 2)", "CAST(2.3 AS decimal(4, 2))", createDecimalType(4, 2), "CAST('2.3' AS decimal(4, 2))")
                .addRoundTrip("decimal(24, 2)", "CAST(2 AS decimal(24, 2))", createDecimalType(24, 2), "CAST('2' AS decimal(24, 2))")
                .addRoundTrip("decimal(24, 2)", "CAST(2.3 AS decimal(24, 2))", createDecimalType(24, 2), "CAST('2.3' AS decimal(24, 2))")
                .addRoundTrip("decimal(24, 2)", "CAST(123456789.3 AS decimal(24, 2))", createDecimalType(24, 2), "CAST('123456789.3' AS decimal(24, 2))")
                .addRoundTrip("decimal(24, 4)", "CAST(12345678901234567890.31 AS decimal(24, 4))", createDecimalType(24, 4), "CAST('12345678901234567890.31' AS decimal(24, 4))")
                .addRoundTrip("decimal(24, 23)", "CAST(3.12345678901234567890123 AS decimal(24, 23))", createDecimalType(24, 23), "CAST('3.12345678901234567890123' AS decimal(24, 23))")
                .addRoundTrip("decimal(30, 5)", "CAST(3141592653589793238462643.38327 AS decimal(30, 5))", createDecimalType(30, 5), "CAST('3141592653589793238462643.38327' AS decimal(30, 5))")
                .addRoundTrip("decimal(30, 5)", "CAST(-3141592653589793238462643.38327 AS decimal(30, 5))", createDecimalType(30, 5), "CAST('-3141592653589793238462643.38327' AS decimal(30, 5))")
                .addRoundTrip("decimal(38, 0)", "CAST(27182818284590452353602874713526624977 AS decimal(38, 0))", createDecimalType(38, 0), "CAST('27182818284590452353602874713526624977' AS decimal(38, 0))")
                .addRoundTrip("decimal(38, 0)", "CAST(-27182818284590452353602874713526624977 AS decimal(38, 0))", createDecimalType(38, 0), "CAST('-27182818284590452353602874713526624977' AS decimal(38, 0))")
                .addRoundTrip("decimal(3, 0)", "CAST(NULL AS decimal(3, 0))", createDecimalType(3, 0), "CAST(NULL AS decimal(3, 0))")
                .addRoundTrip("decimal(38, 0)", "CAST(NULL AS decimal(38, 0))", createDecimalType(38, 0), "CAST(NULL AS decimal(38, 0))")
                .addRoundTrip("integer primary key", "1", INTEGER, "1")
                .execute(getQueryRunner(), phoenixCreateAndInsert("tpch.test_decimal"));
    }

    @Test
    public void testDecimalUnspecifiedPrecision()
    {
        PhoenixSqlExecutor phoenixSqlExecutor = new PhoenixSqlExecutor(phoenixServer.getJdbcUrl());
        try (TestTable testTable = new TestTable(
                phoenixSqlExecutor,
                "tpch.test_var_decimal",
                "(pk bigint primary key, d_col decimal)",
                asList("1, 1.12", "2, 123456.789", "3, -1.12", "4, -123456.789"))) {
            assertQueryFails(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 0),
                    "SELECT d_col FROM " + testTable.getName(),
                    "Rounding necessary");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 0),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES (1), (123457), (-1), (-123457)");
            assertQueryFails(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 1),
                    "SELECT d_col FROM " + testTable.getName(),
                    "Rounding necessary");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 1),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES (1.1), (123456.8), (-1.1), (-123456.8)");
            assertQueryFails(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 2),
                    "SELECT d_col FROM " + testTable.getName(),
                    "Rounding necessary");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 2),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES (1.12), (123456.79), (-1.12), (-123456.79)");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 3),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES (1.12), (123456.789), (-1.12), (-123456.789)");
            assertQueryFails(
                    sessionWithDecimalMappingStrict(CONVERT_TO_VARCHAR),
                    "SELECT d_col FROM " + testTable.getName(),
                    "Rounding necessary");
        }
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    public void testDate(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(getTimeZoneKey(sessionZone.getId()))
                .build();

        SqlDataTypeTest.create()
                .addRoundTrip("date", "DATE '-5877641-06-23'", DATE, "DATE '-5877641-06-23'") // min value in Trino
                .addRoundTrip("date", "DATE '-0001-01-01'", DATE, "DATE '-0001-01-01'")
                .addRoundTrip("date", "DATE '0001-01-01'", DATE, "DATE '0001-01-01'")
                .addRoundTrip("date", "DATE '1582-10-04'", DATE, "DATE '1582-10-04'")
                .addRoundTrip("date", "DATE '1582-10-05'", DATE, "DATE '1582-10-15'") // begin julian->gregorian switch
                .addRoundTrip("date", "DATE '1582-10-14'", DATE, "DATE '1582-10-24'") // end julian->gregorian switch
                .addRoundTrip("date", "DATE '1582-10-15'", DATE, "DATE '1582-10-15'")
                .addRoundTrip("date", "DATE '1899-12-31'", DATE, "DATE '1899-12-31'")
                .addRoundTrip("date", "DATE '1900-01-01'", DATE, "DATE '1900-01-01'")
                .addRoundTrip("date", "DATE '1952-04-04'", DATE, "DATE '1952-04-04'") // before epoch
                .addRoundTrip("date", "DATE '1970-01-01'", DATE, "DATE '1970-01-01'")
                .addRoundTrip("date", "DATE '1970-02-03'", DATE, "DATE '1970-02-03'")
                .addRoundTrip("date", "DATE '2017-07-01'", DATE, "DATE '2017-07-01'") // summer on northern hemisphere (possible DST)
                .addRoundTrip("date", "DATE '2017-01-01'", DATE, "DATE '2017-01-01'") // winter on northern hemisphere (possible DST on southern hemisphere)
                .addRoundTrip("date", "DATE '1983-04-01'", DATE, "DATE '1983-04-01'")
                .addRoundTrip("date", "DATE '1983-10-01'", DATE, "DATE '1983-10-01'")
                .addRoundTrip("date", "DATE '9999-12-31'", DATE, "DATE '9999-12-31'")
                .addRoundTrip("date", "DATE '5881580-07-11'", DATE, "DATE '5881580-07-11'") // max value in Trino
                .addRoundTrip("date", "NULL", DATE, "CAST(NULL AS DATE)")
                .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_date"))
                .execute(getQueryRunner(), session, trinoCreateAsSelect(getSession(), "test_date"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_date"));

        SqlDataTypeTest.create()
                .addRoundTrip("date", "TO_DATE('5877642-06-23 BC', 'yyyy-MM-dd G', 'local')", DATE, "DATE '-5877641-06-23'") // min value in Trino
                .addRoundTrip("date", "TO_DATE('0002-01-01 BC', 'yyyy-MM-dd G', 'local')", DATE, "DATE '-0001-01-01'")
                .addRoundTrip("date", "TO_DATE('0001-01-01', 'yyyy-MM-dd', 'local')", DATE, "DATE '0001-01-01'")
                .addRoundTrip("date", "TO_DATE('1582-10-04', 'yyyy-MM-dd', 'local')", DATE, "DATE '1582-10-04'")
                .addRoundTrip("date", "TO_DATE('1582-10-05', 'yyyy-MM-dd', 'local')", DATE, "DATE '1582-10-15'") // begin julian->gregorian switch
                .addRoundTrip("date", "TO_DATE('1582-10-14', 'yyyy-MM-dd', 'local')", DATE, "DATE '1582-10-24'") // end julian->gregorian switch
                .addRoundTrip("date", "TO_DATE('1582-10-15', 'yyyy-MM-dd', 'local')", DATE, "DATE '1582-10-15'")
                .addRoundTrip("date", "TO_DATE('1899-12-31', 'yyyy-MM-dd', 'local')", DATE, "DATE '1899-12-31'")
                .addRoundTrip("date", "TO_DATE('1900-01-01', 'yyyy-MM-dd', 'local')", DATE, "DATE '1900-01-01'")
                .addRoundTrip("date", "TO_DATE('1952-04-04', 'yyyy-MM-dd', 'local')", DATE, "DATE '1952-04-04'") // before epoch
                .addRoundTrip("date", "TO_DATE('1970-01-01', 'yyyy-MM-dd', 'local')", DATE, "DATE '1970-01-01'")
                .addRoundTrip("date", "TO_DATE('1970-02-03', 'yyyy-MM-dd', 'local')", DATE, "DATE '1970-02-03'")
                .addRoundTrip("date", "TO_DATE('2017-07-01', 'yyyy-MM-dd', 'local')", DATE, "DATE '2017-07-01'") // summer on northern hemisphere (possible DST)
                .addRoundTrip("date", "TO_DATE('2017-01-01', 'yyyy-MM-dd', 'local')", DATE, "DATE '2017-01-01'") // winter on northern hemisphere (possible DST on southern hemisphere)
                .addRoundTrip("date", "TO_DATE('1983-04-01', 'yyyy-MM-dd', 'local')", DATE, "DATE '1983-04-01'")
                .addRoundTrip("date", "TO_DATE('1983-10-01', 'yyyy-MM-dd', 'local')", DATE, "DATE '1983-10-01'")
                .addRoundTrip("date", "TO_DATE('9999-12-31', 'yyyy-MM-dd', 'local')", DATE, "DATE '9999-12-31'")
                .addRoundTrip("date", "TO_DATE('5881580-07-11', 'yyyy-MM-dd', 'local')", DATE, "DATE '5881580-07-11'") // max value in Trino
                .addRoundTrip("date", "NULL", DATE, "CAST(NULL AS DATE)")
                .addRoundTrip("integer primary key", "1", INTEGER, "1")
                .execute(getQueryRunner(), session, phoenixCreateAndInsert("tpch.test_date"));
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    public void testUnsignedDate(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(getTimeZoneKey(sessionZone.getId()))
                .build();

        SqlDataTypeTest.create()
                .addRoundTrip("unsigned_date", "TO_DATE('1970-01-01', 'yyyy-MM-dd', 'local')", DATE, "DATE '1970-01-01'") // min value in Phoenix
                .addRoundTrip("unsigned_date", "TO_DATE('1970-02-03', 'yyyy-MM-dd', 'local')", DATE, "DATE '1970-02-03'")
                .addRoundTrip("unsigned_date", "TO_DATE('1983-04-01', 'yyyy-MM-dd', 'local')", DATE, "DATE '1983-04-01'")
                .addRoundTrip("unsigned_date", "TO_DATE('1983-10-01', 'yyyy-MM-dd', 'local')", DATE, "DATE '1983-10-01'")
                .addRoundTrip("unsigned_date", "TO_DATE('2017-07-01', 'yyyy-MM-dd', 'local')", DATE, "DATE '2017-07-01'") // summer on northern hemisphere (possible DST)
                .addRoundTrip("unsigned_date", "TO_DATE('2017-01-01', 'yyyy-MM-dd', 'local')", DATE, "DATE '2017-01-01'") // winter on northern hemisphere (possible DST on southern hemisphere)
                .addRoundTrip("unsigned_date", "TO_DATE('9999-12-31', 'yyyy-MM-dd', 'local')", DATE, "DATE '9999-12-31'")
                .addRoundTrip("unsigned_date", "TO_DATE('5881580-07-11', 'yyyy-MM-dd', 'local')", DATE, "DATE '5881580-07-11'") // max value in Trino
                .addRoundTrip("unsigned_date", "NULL", DATE, "CAST(NULL AS DATE)")
                .addRoundTrip("integer primary key", "1", INTEGER, "1")
                .execute(getQueryRunner(), session, phoenixCreateAndInsert("tpch.test_unsigned_date"));
    }

    @Test
    public void testArray()
    {
        // basic types
        SqlDataTypeTest.create()
                .addRoundTrip("ARRAY(boolean)", "ARRAY[true, false]", new ArrayType(BOOLEAN), "ARRAY[true, false]")
                .addRoundTrip("ARRAY(bigint)", "ARRAY[123456789012]", new ArrayType(BIGINT), "ARRAY[123456789012]")
                .addRoundTrip("ARRAY(integer)", "ARRAY[1, 2, 1234567890]", new ArrayType(INTEGER), "ARRAY[1, 2, 1234567890]")
                .addRoundTrip("ARRAY(smallint)", "ARRAY[32456]", new ArrayType(SMALLINT), "ARRAY[SMALLINT '32456']")
                .addRoundTrip("ARRAY(double)", "ARRAY[123.45]", new ArrayType(DOUBLE), "ARRAY[DOUBLE '123.45']")
                .addRoundTrip("ARRAY(real)", "ARRAY[123.45]", new ArrayType(REAL), "ARRAY[REAL '123.45']")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_array_basic"));

        SqlDataTypeTest.create()
                .addRoundTrip("ARRAY(date)", "ARRAY[DATE '1952-04-03']", new ArrayType(DATE), "ARRAY[DATE '1952-04-03']") // before epoch
                .addRoundTrip("ARRAY(date)", "ARRAY[DATE '1970-01-01']", new ArrayType(DATE), "ARRAY[DATE '1970-01-01']")
                .addRoundTrip("ARRAY(date)", "ARRAY[DATE '1970-02-03']", new ArrayType(DATE), "ARRAY[DATE '1970-02-03']")
                .addRoundTrip("ARRAY(date)", "ARRAY[DATE '2017-07-01']", new ArrayType(DATE), "ARRAY[DATE '2017-07-01']") // summer on northern hemisphere (possible DST)
                .addRoundTrip("ARRAY(date)", "ARRAY[DATE '2017-01-01']", new ArrayType(DATE), "ARRAY[DATE '2017-01-01']") // winter on northern hemisphere (possible DST on southern hemisphere)
                .addRoundTrip("ARRAY(date)", "ARRAY[DATE '1983-04-01']", new ArrayType(DATE), "ARRAY[DATE '1983-04-01']")
                .addRoundTrip("ARRAY(date)", "ARRAY[DATE '1983-10-01']", new ArrayType(DATE), "ARRAY[DATE '1983-10-01']")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_array_date"));

        SqlDataTypeTest.create()
                .addRoundTrip("date ARRAY", "ARRAY[TO_DATE('1952-04-03', 'yyyy-MM-dd', 'local')]", new ArrayType(DATE), "ARRAY[DATE '1952-04-03']") // before epoch
                .addRoundTrip("date ARRAY", "ARRAY[TO_DATE('1970-01-01', 'yyyy-MM-dd', 'local')]", new ArrayType(DATE), "ARRAY[DATE '1970-01-01']")
                .addRoundTrip("date ARRAY", "ARRAY[TO_DATE('1970-02-03', 'yyyy-MM-dd', 'local')]", new ArrayType(DATE), "ARRAY[DATE '1970-02-03']")
                .addRoundTrip("date ARRAY", "ARRAY[TO_DATE('2017-07-01', 'yyyy-MM-dd', 'local')]", new ArrayType(DATE), "ARRAY[DATE '2017-07-01']") // summer on northern hemisphere (possible DST)
                .addRoundTrip("date ARRAY", "ARRAY[TO_DATE('2017-01-01', 'yyyy-MM-dd', 'local')]", new ArrayType(DATE), "ARRAY[DATE '2017-01-01']") // winter on northern hemisphere (possible DST on southern hemisphere)
                .addRoundTrip("date ARRAY", "ARRAY[TO_DATE('1983-04-01', 'yyyy-MM-dd', 'local')]", new ArrayType(DATE), "ARRAY[DATE '1983-04-01']")
                .addRoundTrip("date ARRAY", "ARRAY[TO_DATE('1983-10-01', 'yyyy-MM-dd', 'local')]", new ArrayType(DATE), "ARRAY[DATE '1983-10-01']")
                .addRoundTrip("integer primary key", "1", INTEGER, "1")
                .execute(getQueryRunner(), phoenixCreateAndInsert("tpch.test_array_date"));

        SqlDataTypeTest.create()
                .addRoundTrip("ARRAY(decimal(3, 0))", "ARRAY[CAST('193' AS decimal(3, 0)), CAST('19' AS decimal(3, 0)), CAST('-193' AS decimal(3, 0))]", new ArrayType(createDecimalType(3, 0)), "ARRAY[CAST('193' AS decimal(3, 0)), CAST('19' AS decimal(3, 0)), CAST('-193' AS decimal(3, 0))]")
                .addRoundTrip("ARRAY(decimal(3, 1))", "ARRAY[CAST('10.0' AS decimal(3, 1)), CAST('10.1' AS decimal(3, 1)), CAST('-10.1' AS decimal(3, 1))]", new ArrayType(createDecimalType(3, 1)), "ARRAY[CAST('10.0' AS decimal(3, 1)), CAST('10.1' AS decimal(3, 1)), CAST('-10.1' AS decimal(3, 1))]")
                .addRoundTrip("ARRAY(decimal(4, 2))", "ARRAY[CAST('2' AS decimal(4, 2)), CAST('2.3' AS decimal(4, 2))]", new ArrayType(createDecimalType(4, 2)), "ARRAY[CAST('2' AS decimal(4, 2)), CAST('2.3' AS decimal(4, 2))]")
                .addRoundTrip("ARRAY(decimal(24, 2))", "ARRAY[CAST('2' AS decimal(24, 2)), CAST('2.3' AS decimal(24, 2)), CAST('123456789.3' AS decimal(24, 2))]", new ArrayType(createDecimalType(24, 2)), "ARRAY[CAST('2' AS decimal(24, 2)), CAST('2.3' AS decimal(24, 2)), CAST('123456789.3' AS decimal(24, 2))]")
                .addRoundTrip("ARRAY(decimal(24, 4))", "ARRAY[CAST('12345678901234567890.31' AS decimal(24, 4))]", new ArrayType(createDecimalType(24, 4)), "ARRAY[CAST('12345678901234567890.31' AS decimal(24, 4))]")
                .addRoundTrip("ARRAY(decimal(30, 5))", "ARRAY[CAST('3141592653589793238462643.38327' AS decimal(30, 5)), CAST('-3141592653589793238462643.38327' AS decimal(30, 5))]", new ArrayType(createDecimalType(30, 5)), "ARRAY[CAST('3141592653589793238462643.38327' AS decimal(30, 5)), CAST('-3141592653589793238462643.38327' AS decimal(30, 5))]")
                .addRoundTrip("ARRAY(decimal(38, 0))", "ARRAY[CAST('27182818284590452353602874713526624977' AS decimal(38, 0)), CAST('-27182818284590452353602874713526624977' AS decimal(38, 0))]", new ArrayType(createDecimalType(38, 0)), "ARRAY[CAST('27182818284590452353602874713526624977' AS decimal(38, 0)), CAST('-27182818284590452353602874713526624977' AS decimal(38, 0))]")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_array_decimal"));

        SqlDataTypeTest.create()
                .addRoundTrip("decimal(3, 0) ARRAY", "ARRAY[CAST(193 AS decimal(3, 0)), CAST(19 AS decimal(3, 0)), CAST(-193 AS decimal(3, 0))]", new ArrayType(createDecimalType(3, 0)), "ARRAY[CAST(193 AS decimal(3, 0)), CAST(19 AS decimal(3, 0)), CAST(-193 AS decimal(3, 0))]")
                .addRoundTrip("decimal(3, 1) ARRAY", "ARRAY[CAST(10.0 AS decimal(3, 1)), CAST(10.1 AS decimal(3, 1)), CAST(-10.1 AS decimal(3, 1))]", new ArrayType(createDecimalType(3, 1)), "ARRAY[CAST(10.0 AS decimal(3, 1)), CAST(10.1 AS decimal(3, 1)), CAST(-10.1 AS decimal(3, 1))]")
                .addRoundTrip("decimal(4, 2) ARRAY", "ARRAY[CAST(2 AS decimal(4, 2)), CAST(2.3 AS decimal(4, 2))]", new ArrayType(createDecimalType(4, 2)), "ARRAY[CAST(2 AS decimal(4, 2)), CAST(2.3 AS decimal(4, 2))]")
                .addRoundTrip("decimal(24, 2) ARRAY", "ARRAY[CAST(2 AS decimal(24, 2)), CAST(2.3 AS decimal(24, 2)), CAST(123456789.3 AS decimal(24, 2))]", new ArrayType(createDecimalType(24, 2)), "ARRAY[CAST(2 AS decimal(24, 2)), CAST(2.3 AS decimal(24, 2)), CAST(123456789.3 AS decimal(24, 2))]")
                .addRoundTrip("decimal(24, 4) ARRAY", "ARRAY[CAST(12345678901234567890.31 AS decimal(24, 4))]", new ArrayType(createDecimalType(24, 4)), "ARRAY[CAST(12345678901234567890.31 AS decimal(24, 4))]")
                .addRoundTrip("decimal(30, 5) ARRAY", "ARRAY[CAST(3141592653589793238462643.38327 AS decimal(30, 5)), CAST(-3141592653589793238462643.38327 AS decimal(30, 5))]", new ArrayType(createDecimalType(30, 5)), "ARRAY[CAST(3141592653589793238462643.38327 AS decimal(30, 5)), CAST(-3141592653589793238462643.38327 AS decimal(30, 5))]")
                .addRoundTrip("decimal(38, 0) ARRAY", "ARRAY[CAST(27182818284590452353602874713526624977 AS decimal(38, 0)), CAST(-27182818284590452353602874713526624977 AS decimal(38, 0))]", new ArrayType(createDecimalType(38, 0)), "ARRAY[CAST('27182818284590452353602874713526624977' AS decimal(38, 0)), CAST('-27182818284590452353602874713526624977' AS decimal(38, 0))]")
                .addRoundTrip("integer primary key", "1", INTEGER, "1")
                .execute(getQueryRunner(), phoenixCreateAndInsert("tpch.test_array_decimal"));

        // TODO (https://github.com/trinodb/trino/issues/10451) Migrate to SqlDataTypeTest after fixing predicate pushdown failure on array(char) type
        arrayStringDataTypeTest(TestPhoenixTypeMapping::arrayDataType, DataType::charDataType)
                .execute(getQueryRunner(), trinoCreateAsSelect("test_array_char"));
        arrayStringDataTypeTest(TestPhoenixTypeMapping::phoenixArrayDataType, DataType::charDataType)
                .addRoundTrip(primaryKey(), 1)
                .execute(getQueryRunner(), phoenixCreateAndInsert("tpch.test_array_char"));

        SqlDataTypeTest.create()
                .addRoundTrip("ARRAY(varchar(10))", "ARRAY['text_a']", new ArrayType(createVarcharType(10)), "ARRAY[CAST('text_a' AS varchar(10))]")
                .addRoundTrip("ARRAY(varchar(255))", "ARRAY['text_b']", new ArrayType(createVarcharType(255)), "ARRAY[CAST('text_b' AS varchar(255))]")
                .addRoundTrip("ARRAY(varchar(65535))", "ARRAY['text_d']", new ArrayType(createVarcharType(65535)), "ARRAY[CAST('text_d' AS varchar(65535))]")
                .addRoundTrip("ARRAY(varchar(10485760))", "ARRAY['text_f']", new ArrayType(createVarcharType(10485760)), "ARRAY[CAST('text_f' AS varchar(10485760))]")
                .addRoundTrip("ARRAY(varchar)", "ARRAY['unbounded']", new ArrayType(VARCHAR), "ARRAY[CAST('unbounded' AS varchar)]")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_array_varchar"));

        SqlDataTypeTest.create()
                .addRoundTrip("varchar(10) ARRAY", "ARRAY['text_a']", new ArrayType(createVarcharType(10)), "ARRAY[CAST('text_a' AS varchar(10))]")
                .addRoundTrip("varchar(255) ARRAY", "ARRAY['text_b']", new ArrayType(createVarcharType(255)), "ARRAY[CAST('text_b' AS varchar(255))]")
                .addRoundTrip("varchar(65535) ARRAY", "ARRAY['text_d']", new ArrayType(createVarcharType(65535)), "ARRAY[CAST('text_d' AS varchar(65535))]")
                .addRoundTrip("varchar(10485760) ARRAY", "ARRAY['text_f']", new ArrayType(createVarcharType(10485760)), "ARRAY[CAST('text_f' AS varchar(10485760))]")
                .addRoundTrip("varchar ARRAY", "ARRAY['unbounded']", new ArrayType(VARCHAR), "ARRAY[CAST('unbounded' AS varchar)]")
                .addRoundTrip("integer primary key", "1", INTEGER, "1")
                .execute(getQueryRunner(), phoenixCreateAndInsert("tpch.test_array_varchar"));
    }

    @Test
    public void testArrayNulls()
    {
        // Verify only SELECT instead of using SqlDataTypeTest because array comparison not supported for arrays with null elements
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_array_nulls", "(c1 ARRAY(boolean), c2 ARRAY(varchar), c3 ARRAY(varchar))", ImmutableList.of("(NULL, ARRAY[NULL], ARRAY['foo', NULL, 'bar', NULL])"))) {
            assertThat(query("SELECT c1 FROM " + table.getName())).matches("VALUES CAST(NULL AS ARRAY(boolean))");
            assertThat(query("SELECT c2 FROM " + table.getName())).matches("VALUES CAST(ARRAY[NULL] AS ARRAY(varchar))");
            assertThat(query("SELECT c3 FROM " + table.getName())).matches("VALUES CAST(ARRAY['foo', NULL, 'bar', NULL] AS ARRAY(varchar))");
        }
    }

    private DataTypeTest arrayStringDataTypeTest(Function<DataType<String>, DataType<List<String>>> arrayTypeFactory, Function<Integer, DataType<String>> dataTypeFactory)
    {
        return DataTypeTest.create()
                .addRoundTrip(arrayTypeFactory.apply(dataTypeFactory.apply(10)), asList("text_a"))
                .addRoundTrip(arrayTypeFactory.apply(dataTypeFactory.apply(255)), asList("text_b"))
                .addRoundTrip(arrayTypeFactory.apply(dataTypeFactory.apply(65535)), asList("text_d"));
    }

    private static <E> DataType<List<E>> arrayDataType(DataType<E> elementType)
    {
        return arrayDataType(elementType, format("ARRAY(%s)", elementType.getInsertType()));
    }

    private static <E> DataType<List<E>> phoenixArrayDataType(DataType<E> elementType)
    {
        return arrayDataType(elementType, elementType.getInsertType() + " ARRAY");
    }

    private static <E> DataType<List<E>> arrayDataType(DataType<E> elementType, String insertType)
    {
        return dataType(
                insertType,
                new ArrayType(elementType.getTrinoResultType()),
                valuesList -> "ARRAY" + valuesList.stream().map(elementType::toLiteral).collect(toList()),
                valuesList -> valuesList == null ? null : valuesList.stream().map(elementType::toTrinoQueryResult).collect(toList()));
    }

    @DataProvider
    public Object[][] sessionZonesDataProvider()
    {
        return new Object[][] {
                {UTC},
                {jvmZone},
                // using two non-JVM zones so that we don't need to worry what Phoenix system zone is
                {vilnius},
                {kathmandu},
                {ZoneId.of(TestingSession.DEFAULT_TIME_ZONE_KEY.getId())},
        };
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

    private DataType<Integer> primaryKey()
    {
        return dataType("integer primary key", INTEGER, Object::toString);
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

    private DataSetup phoenixCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(new PhoenixSqlExecutor(phoenixServer.getJdbcUrl()), tableNamePrefix);
    }

    private Session sessionWithDecimalMappingAllowOverflow(RoundingMode roundingMode, int scale)
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("phoenix", DECIMAL_MAPPING, ALLOW_OVERFLOW.name())
                .setCatalogSessionProperty("phoenix", DECIMAL_ROUNDING_MODE, roundingMode.name())
                .setCatalogSessionProperty("phoenix", DECIMAL_DEFAULT_SCALE, Integer.valueOf(scale).toString())
                .build();
    }

    private Session sessionWithDecimalMappingStrict(UnsupportedTypeHandling unsupportedTypeHandling)
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("phoenix", DECIMAL_MAPPING, STRICT.name())
                .setCatalogSessionProperty("phoenix", UNSUPPORTED_TYPE_HANDLING, unsupportedTypeHandling.name())
                .build();
    }
}
