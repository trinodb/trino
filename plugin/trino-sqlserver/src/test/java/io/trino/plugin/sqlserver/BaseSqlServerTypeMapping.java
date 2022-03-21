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
package io.trino.plugin.sqlserver;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.spi.type.TimeZoneKey;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.TestingSession;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.datatype.CreateAndTrinoInsertDataSetup;
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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
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
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class BaseSqlServerTypeMapping
        extends AbstractTestQueryFramework
{
    private final ZoneId jvmZone = ZoneId.systemDefault();
    private final LocalDateTime timeGapInJvmZone1 = LocalDateTime.of(1970, 1, 1, 0, 13, 42);
    private final LocalDateTime timeGapInJvmZone2 = LocalDateTime.of(2018, 4, 1, 2, 13, 55, 123_000_000);
    private final LocalDateTime timeDoubledInJvmZone = LocalDateTime.of(2018, 10, 28, 1, 33, 17, 456_000_000);

    // no DST in 1970, but has DST in later years (e.g. 2018)
    private final ZoneId vilnius = ZoneId.of("Europe/Vilnius");
    private final LocalDateTime timeGapInVilnius = LocalDateTime.of(2018, 3, 25, 3, 17, 17);
    private final LocalDateTime timeDoubledInVilnius = LocalDateTime.of(2018, 10, 28, 3, 33, 33, 333_000_000);

    // minutes offset change since 1970-01-01, no DST
    private final ZoneId kathmandu = ZoneId.of("Asia/Kathmandu");
    private final LocalDateTime timeGapInKathmandu = LocalDateTime.of(1986, 1, 1, 0, 13, 7);

    protected TestingSqlServer sqlServer;

    @BeforeClass
    public void setUp()
    {
        checkIsGap(jvmZone, timeGapInJvmZone1);
        checkIsGap(jvmZone, timeGapInJvmZone2);
        checkIsDoubled(jvmZone, timeDoubledInJvmZone);

        checkIsGap(vilnius, timeGapInVilnius);
        checkIsDoubled(vilnius, timeDoubledInVilnius);

        checkIsGap(kathmandu, timeGapInKathmandu);
    }

    @Test
    public void testTrinoBoolean()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("boolean", "null", BOOLEAN, "CAST(NULL AS BOOLEAN)")
                .addRoundTrip("boolean", "true", BOOLEAN)
                .addRoundTrip("boolean", "false", BOOLEAN)
                .execute(getQueryRunner(), trinoCreateAsSelect("test_boolean"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_boolean"));
    }

    @Test
    public void testSqlServerBit()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("bit", "null", BOOLEAN, "CAST(NULL AS BOOLEAN)")
                .addRoundTrip("bit", "1", BOOLEAN, "true")
                .addRoundTrip("bit", "0", BOOLEAN, "false")
                .execute(getQueryRunner(), sqlServerCreateAndInsert("test_bit"));
    }

    @Test
    public void testTinyint()
    {
        // Map SQL Server TINYINT to Trino SMALLINT because SQL Server TINYINT is actually "unsigned tinyint"
        SqlDataTypeTest.create()
                .addRoundTrip("tinyint", "NULL", SMALLINT, "CAST(NULL AS SMALLINT)")
                .addRoundTrip("tinyint", "0", SMALLINT, "SMALLINT '0'") // min value in SQL Server
                .addRoundTrip("tinyint", "5", SMALLINT, "SMALLINT '5'")
                .addRoundTrip("tinyint", "255", SMALLINT, "SMALLINT '255'") // max value in SQL Server
                .execute(getQueryRunner(), sqlServerCreateAndInsert("test_tinyint"))
                .execute(getQueryRunner(), sqlServerCreateAndTrinoInsert("test_tinyint"));
    }

    @Test
    public void testUnsupportedTinyint()
    {
        try (TestTable table = new TestTable(onRemoteDatabase(), "test_unsupported_tinyint", "(data tinyint)")) {
            assertSqlServerQueryFails(
                    format("INSERT INTO %s VALUES (-1)", table.getName()), // min - 1
                    "Arithmetic overflow error for data type tinyint, value = -1");
            assertSqlServerQueryFails(
                    format("INSERT INTO %s VALUES (256)", table.getName()), // max + 1
                    "Arithmetic overflow error for data type tinyint, value = 256.");
        }
    }

    @Test
    public void testSmallint()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("smallint", "NULL", SMALLINT, "CAST(NULL AS SMALLINT)")
                .addRoundTrip("smallint", "-32768", SMALLINT, "SMALLINT '-32768'") // min value in SQL Server and Trino
                .addRoundTrip("smallint", "32456", SMALLINT, "SMALLINT '32456'")
                .addRoundTrip("smallint", "32767", SMALLINT, "SMALLINT '32767'") // max value in SQL Server and Trino
                .execute(getQueryRunner(), sqlServerCreateAndInsert("test_smallint"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_smallint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_smallint"));
    }

    @Test
    public void testUnsupportedSmallint()
    {
        try (TestTable table = new TestTable(onRemoteDatabase(), "test_unsupported_smallint", "(data smallint)")) {
            assertSqlServerQueryFails(
                    format("INSERT INTO %s VALUES (-32769)", table.getName()), // min - 1
                    "Arithmetic overflow error for data type smallint, value = -32769.");
            assertSqlServerQueryFails(
                    format("INSERT INTO %s VALUES (32768)", table.getName()), // max + 1
                    "Arithmetic overflow error for data type smallint, value = 32768.");
        }
    }

    @Test
    public void testInteger()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("integer", "NULL", INTEGER, "CAST(NULL AS INTEGER)")
                .addRoundTrip("integer", "-2147483648", INTEGER, "-2147483648") // min value in SQL Server and Trino
                .addRoundTrip("integer", "1234567890", INTEGER, "1234567890")
                .addRoundTrip("integer", "2147483647", INTEGER, "2147483647") // max value in SQL Server and Trino
                .execute(getQueryRunner(), sqlServerCreateAndInsert("test_int"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_int"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_int"));
    }

    @Test
    public void testUnsupportedInteger()
    {
        try (TestTable table = new TestTable(onRemoteDatabase(), "test_unsupported_integer", "(data integer)")) {
            assertSqlServerQueryFails(
                    format("INSERT INTO %s VALUES (-2147483649)", table.getName()), // min - 1
                    "Arithmetic overflow error converting expression to data type int.");
            assertSqlServerQueryFails(
                    format("INSERT INTO %s VALUES (2147483648)", table.getName()), // max + 1
                    "Arithmetic overflow error converting expression to data type int.");
        }
    }

    @Test
    public void testBigint()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("bigint", "NULL", BIGINT, "CAST(NULL AS BIGINT)")
                .addRoundTrip("bigint", "-9223372036854775808", BIGINT, "-9223372036854775808") // min value in SQL Server and Trino
                .addRoundTrip("bigint", "123456789012", BIGINT, "123456789012")
                .addRoundTrip("bigint", "9223372036854775807", BIGINT, "9223372036854775807") // max value in SQL Server and Trino
                .execute(getQueryRunner(), sqlServerCreateAndInsert("test_bigint"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_bigint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_bigint"));
    }

    @Test
    public void testUnsupportedBigint()
    {
        try (TestTable table = new TestTable(onRemoteDatabase(), "test_unsupported_bigint", "(data bigint)")) {
            assertSqlServerQueryFails(
                    format("INSERT INTO %s VALUES (-9223372036854775809)", table.getName()), // min - 1
                    "Arithmetic overflow error converting expression to data type bigint.");
            assertSqlServerQueryFails(
                    format("INSERT INTO %s VALUES (9223372036854775808)", table.getName()), // max + 1
                    "Arithmetic overflow error converting expression to data type bigint.");
        }
    }

    @Test
    public void testReal()
    {
        // we are not testing Nan/-Infinity/+Infinity as those are not supported by SQL Server
        SqlDataTypeTest.create()
                .addRoundTrip("real", "NULL", REAL, "CAST(NULL AS real)")
                .addRoundTrip("real", "3.14", REAL, "REAL '3.14'")
                .addRoundTrip("real", "3.1415927", REAL, "REAL '3.1415927'")
                .execute(getQueryRunner(), sqlServerCreateAndInsert("test_real"));

        SqlDataTypeTest.create()
                .addRoundTrip("real", "NULL", REAL, "CAST(NULL AS real)")
                .addRoundTrip("real", "3.14", REAL, "REAL '3.14'")
                .addRoundTrip("real", "3.1415927", REAL, "REAL '3.1415927'")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_real"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_real"));
    }

    @Test
    public void testFloat()
    {
        // SQL Server treats n as one of two possible values. If 1<=n<=24, n is treated as 24. If 25<=n<=53, n is treated as 53
        SqlDataTypeTest.create()
                .addRoundTrip("float", "1E100", DOUBLE, "double '1E100'")
                .addRoundTrip("float", "1.0", DOUBLE, "double '1.0'")
                .addRoundTrip("float", "123456.123456", DOUBLE, "double '123456.123456'")
                .addRoundTrip("float", "NULL", DOUBLE, "CAST(NULL AS double)")
                .addRoundTrip("float(1)", "100000.0", REAL, "REAL '100000.0'")
                .addRoundTrip("float(24)", "123000.0", REAL, "REAL '123000.0'")
                .addRoundTrip("float(24)", "NULL", REAL, "CAST(NULL AS real)")
                .addRoundTrip("float(25)", "1E100", DOUBLE, "double '1E100'")
                .addRoundTrip("float(53)", "1.0", DOUBLE, "double '1.0'")
                .addRoundTrip("float(53)", "1234567890123456789.0123456789", DOUBLE, "double '1234567890123456789.0123456789'")
                .addRoundTrip("float(53)", "NULL", DOUBLE, "CAST(NULL AS double)")
                .execute(getQueryRunner(), sqlServerCreateAndInsert("test_float"));
    }

    @Test
    public void testDouble()
    {
        // we are not testing Nan/-Infinity/+Infinity as those are not supported by SQL Server
        SqlDataTypeTest.create()
                .addRoundTrip("double precision", "NULL", DOUBLE, "CAST(NULL AS double)")
                .addRoundTrip("double precision", "1.0E100", DOUBLE, "1.0E100")
                .addRoundTrip("double precision", "123.456E10", DOUBLE, "123.456E10")
                .execute(getQueryRunner(), sqlServerCreateAndInsert("test_double"));

        SqlDataTypeTest.create()
                .addRoundTrip("double", "NULL", DOUBLE, "CAST(NULL AS double)")
                .addRoundTrip("double", "1.0E100", DOUBLE, "1.0E100")
                .addRoundTrip("double", "123.456E10", DOUBLE, "123.456E10")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_double"));
    }

    @Test
    public void testDecimal()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("decimal(3, 0)", "193", createDecimalType(3, 0), "CAST('193' AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 0)", "19", createDecimalType(3, 0), "CAST('19' AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 0)", "-193", createDecimalType(3, 0), "CAST('-193' AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 1)", "10.0", createDecimalType(3, 1), "CAST('10.0' AS decimal(3, 1))")
                .addRoundTrip("decimal(3, 1)", "10.1", createDecimalType(3, 1), "CAST('10.1' AS decimal(3, 1))")
                .addRoundTrip("decimal(3, 1)", "-10.1", createDecimalType(3, 1), "CAST('-10.1' AS decimal(3, 1))")
                .addRoundTrip("decimal(4, 2)", "2", createDecimalType(4, 2), "CAST('2' AS decimal(4, 2))")
                .addRoundTrip("decimal(4, 2)", "2.3", createDecimalType(4, 2), "CAST('2.3' AS decimal(4, 2))")
                .addRoundTrip("decimal(24, 2)", "2", createDecimalType(24, 2), "CAST('2' AS decimal(24, 2))")
                .addRoundTrip("decimal(24, 2)", "2.3", createDecimalType(24, 2), "CAST('2.3' AS decimal(24, 2))")
                .addRoundTrip("decimal(24, 2)", "123456789.3", createDecimalType(24, 2), "CAST('123456789.3' AS decimal(24, 2))")
                .addRoundTrip("decimal(24, 4)", "12345678901234567890.31", createDecimalType(24, 4), "CAST('12345678901234567890.31' AS decimal(24, 4))")
                .addRoundTrip("decimal(30, 5)", "3141592653589793238462643.38327", createDecimalType(30, 5), "CAST('3141592653589793238462643.38327' AS decimal(30, 5))")
                .addRoundTrip("decimal(30, 5)", "-3141592653589793238462643.38327", createDecimalType(30, 5), "CAST('-3141592653589793238462643.38327' AS decimal(30, 5))")
                .execute(getQueryRunner(), sqlServerCreateAndInsert("test_decimal"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_decimal"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_decimal"));
    }

    @Test
    public void testChar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("char(1)", "NULL", createCharType(1), "CAST(NULL AS char(1))")
                .addRoundTrip("char(10)", "'text_a'", createCharType(10), "CAST('text_a' AS char(10))")
                .addRoundTrip("char(255)", "'text_b'", createCharType(255), "CAST('text_b' AS char(255))")
                .addRoundTrip("char(4001)", "'text_c'", createCharType(4001), "CAST('text_c' AS char(4001))")
                .execute(getQueryRunner(), sqlServerCreateAndInsert("test_char"));

        SqlDataTypeTest.create()
                .addRoundTrip("char(1)", "NULL", createCharType(1), "CAST(NULL AS char(1))")
                .addRoundTrip("char(10)", "'text_a'", createCharType(10), "CAST('text_a' AS char(10))")
                .addRoundTrip("char(255)", "'text_b'", createCharType(255), "CAST('text_b' AS char(255))")
                .addRoundTrip("char(5)", "CAST('攻殻機動隊' AS char(5))", createCharType(5), "CAST('攻殻機動隊' AS char(5))")
                .addRoundTrip("char(32)", "CAST('攻殻機動隊' AS char(32))", createCharType(32), "CAST('攻殻機動隊' AS char(32))")
                .addRoundTrip("char(20)", "CAST('😂' AS char(20))", createCharType(20), "CAST('😂' AS char(20))")
                .addRoundTrip("char(77)", "CAST('Ну, погоди!' AS char(77))", createCharType(77), "CAST('Ну, погоди!' AS char(77))")
                .execute(getQueryRunner(), trinoCreateAndInsert("test_char"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_char"));
    }

    @Test
    public void testSqlServerNchar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("nchar(1)", "NULL", createCharType(1), "CAST(NULL AS char(1))")
                .addRoundTrip("nchar(10)", "'text_a'", createCharType(10), "CAST('text_a' AS char(10))")
                .addRoundTrip("nchar(255)", "'text_b'", createCharType(255), "CAST('text_b' AS char(255))")
                .execute(getQueryRunner(), sqlServerCreateAndInsert("test_sqlserver_char"));
    }

    @Test
    public void testTrinoLongChar()
    {
        // testing mapping char > 4000 -> varchar(max)
        SqlDataTypeTest.create()
                .addRoundTrip("char(4001)", "'text_c'", createUnboundedVarcharType(), "VARCHAR 'text_c'")
                .execute(getQueryRunner(), trinoCreateAndInsert("test_long_char"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_long_char"));
    }

    @Test
    public void testVarchar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("varchar(10)", "'text_a'", createVarcharType(10), "CAST('text_a' AS varchar(10))")
                .addRoundTrip("varchar(255)", "'text_b'", createVarcharType(255), "CAST('text_b' AS varchar(255))")
                .addRoundTrip("varchar(4001)", "'text_c'", createVarcharType(4001), "CAST('text_c' AS varchar(4001))")
                .execute(getQueryRunner(), sqlServerCreateAndInsert("test_varchar"));

        SqlDataTypeTest.create()
                .addRoundTrip("varchar(10)", "'text_a'", createVarcharType(10), "CAST('text_a' AS varchar(10))")
                .addRoundTrip("varchar(255)", "'text_b'", createVarcharType(255), "CAST('text_b' AS varchar(255))")
                .addRoundTrip("varchar(5)", "CAST('攻殻機動隊' AS varchar(5))", createVarcharType(5), "CAST('攻殻機動隊' AS varchar(5))")
                .addRoundTrip("varchar(32)", "CAST('攻殻機動隊' AS varchar(32))", createVarcharType(32), "CAST('攻殻機動隊' AS varchar(32))")
                .addRoundTrip("varchar(20)", "CAST('😂' AS varchar(20))", createVarcharType(20), "CAST('😂' AS varchar(20))")
                .addRoundTrip("varchar(77)", "CAST('Ну, погоди!' AS varchar(77))", createVarcharType(77), "CAST('Ну, погоди!' AS varchar(77))")
                .execute(getQueryRunner(), trinoCreateAndInsert("test_varchar"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_varchar"));
    }

    @Test
    public void testSqlServerNvarchar()
    {
        // Unicode literals must be prefixed with N'string'
        // https://docs.microsoft.com/en-us/sql/analytics-platform-system/load-with-insert?view=aps-pdw-2016-au7#char-varchar-nchar-and-nvarchar-data-types
        SqlDataTypeTest.create()
                .addRoundTrip("nvarchar(5)", "N'攻殻機動隊'", createVarcharType(5), "CAST('攻殻機動隊' AS varchar(5))")
                .addRoundTrip("nvarchar(32)", "N'攻殻機動隊'", createVarcharType(32), "CAST('攻殻機動隊' AS varchar(32))")
                .addRoundTrip("nvarchar(20)", "N'😂'", createVarcharType(20), "CAST('😂' AS varchar(20))")
                .addRoundTrip("nvarchar(77)", "N'Ну, погоди!'", createVarcharType(77), "CAST('Ну, погоди!' AS varchar(77))")
                .execute(getQueryRunner(), sqlServerCreateAndInsert("test_sqlserver_nvarchar"));
    }

    @Test
    public void testTrinoLongVarchar()
    {
        // testing mapping varchar > 4000 -> varchar(max)
        SqlDataTypeTest.create()
                .addRoundTrip("varchar(4001)", "'text_c'", createUnboundedVarcharType(), "VARCHAR 'text_c'")
                .execute(getQueryRunner(), trinoCreateAndInsert("test_long_varchar"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_long_varchar"));
    }

    @Test
    public void testSqlServerLongVarchar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("text", "'text_a'", createUnboundedVarcharType(), "VARCHAR 'text_a'")
                .addRoundTrip("ntext", "'text_a'", createVarcharType(1073741823), "CAST('text_a' as VARCHAR(1073741823))")
                .execute(getQueryRunner(), sqlServerCreateAndInsert("test_long_n_varchar"));
    }

    @Test
    public void testTrinoUnboundedVarchar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("varchar", "'text_a'", createUnboundedVarcharType(), "VARCHAR 'text_a'")
                .addRoundTrip("varchar", "'text_b'", createUnboundedVarcharType(), "VARCHAR 'text_b'")
                .addRoundTrip("varchar", "'text_d'", createUnboundedVarcharType(), "VARCHAR 'text_d'")
                .addRoundTrip("varchar", "VARCHAR '攻殻機動隊'", createUnboundedVarcharType(), "VARCHAR '攻殻機動隊'")
                .addRoundTrip("varchar", "VARCHAR '攻殻機動隊'", createUnboundedVarcharType(), "VARCHAR '攻殻機動隊'")
                .addRoundTrip("varchar", "VARCHAR '攻殻機動隊'", createUnboundedVarcharType(), "VARCHAR '攻殻機動隊'")
                .addRoundTrip("varchar", "VARCHAR '😂'", createUnboundedVarcharType(), "VARCHAR '😂'")
                .addRoundTrip("varchar", "VARCHAR 'Ну, погоди!'", createUnboundedVarcharType(), "VARCHAR 'Ну, погоди!'")
                .addRoundTrip("varchar", "'text_f'", createUnboundedVarcharType(), "VARCHAR 'text_f'")
                .execute(getQueryRunner(), trinoCreateAndInsert("test_unbounded_varchar"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_unbounded_varchar"));
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
                .execute(getQueryRunner(), trinoCreateAndInsert("test_varbinary"));

        // Binary literals must be prefixed with 0x
        // https://docs.microsoft.com/en-us/sql/analytics-platform-system/load-with-insert?view=aps-pdw-2016-au7#InsertingLiteralsBinary
        SqlDataTypeTest.create()
                .addRoundTrip("varbinary(10)", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("varbinary(20)", "0x", VARBINARY, "CAST('' AS varbinary)")
                .addRoundTrip("varbinary(30)", "0x68656C6C6F", VARBINARY, "X'68656C6C6F'")
                .addRoundTrip("varbinary(1000)", "0x5069C4996B6E6120C582C4856B61207720E69DB1E4BAACE983BD", VARBINARY, "X'5069C4996B6E6120C582C4856B61207720E69DB1E4BAACE983BD'")
                .addRoundTrip("varbinary(2000)", "0x4261672066756C6C206F6620F09F92B0", VARBINARY, "X'4261672066756C6C206F6620F09F92B0'")
                .addRoundTrip("varbinary(4000)", "0x0001020304050607080DF9367AA7000000", VARBINARY, "X'0001020304050607080DF9367AA7000000'") // non-text
                .addRoundTrip("varbinary(4000)", "0x000000000000", VARBINARY, "X'000000000000'")
                .execute(getQueryRunner(), sqlServerCreateAndInsert("test_varbinary"));
    }

    @Test
    public void testDate()
    {
        ZoneId jvmZone = ZoneId.systemDefault();
        checkState(jvmZone.getId().equals("America/Bahia_Banderas"), "This test assumes certain JVM time zone");
        LocalDate dateOfLocalTimeChangeForwardAtMidnightInJvmZone = LocalDate.of(1970, 1, 1);
        checkIsGap(jvmZone, dateOfLocalTimeChangeForwardAtMidnightInJvmZone.atStartOfDay());

        ZoneId someZone = ZoneId.of("Europe/Vilnius");
        LocalDate dateOfLocalTimeChangeForwardAtMidnightInSomeZone = LocalDate.of(1983, 4, 1);
        checkIsGap(someZone, dateOfLocalTimeChangeForwardAtMidnightInSomeZone.atStartOfDay());
        LocalDate dateOfLocalTimeChangeBackwardAtMidnightInSomeZone = LocalDate.of(1983, 10, 1);
        checkIsDoubled(someZone, dateOfLocalTimeChangeBackwardAtMidnightInSomeZone.atStartOfDay().minusMinutes(1));

        // BC dates not supported by SQL Server
        SqlDataTypeTest testsSqlServer = SqlDataTypeTest.create()
                .addRoundTrip("date", "NULL", DATE, "CAST(NULL AS DATE)")
                // first day of AD
                .addRoundTrip("date", "'0001-01-01'", DATE, "DATE '0001-01-01'")
                .addRoundTrip("date", "'0012-12-12'", DATE, "DATE '0012-12-12'")
                // before julian->gregorian switch
                .addRoundTrip("date", "'1500-01-01'", DATE, "DATE '1500-01-01'")
                // before epoch
                .addRoundTrip("date", "'1952-04-03'", DATE, "DATE '1952-04-03'")
                .addRoundTrip("date", "'1970-01-01'", DATE, "DATE '1970-01-01'")
                .addRoundTrip("date", "'1970-02-03'", DATE, "DATE '1970-02-03'")
                // summer on northern hemisphere (possible DST)
                .addRoundTrip("date", "'2017-07-01'", DATE, "DATE '2017-07-01'")
                // winter on northern hemisphere (possible DST on southern hemisphere)
                .addRoundTrip("date", "'2017-01-01'", DATE, "DATE '2017-01-01'")
                .addRoundTrip("date", "'1970-01-01'", DATE, "DATE '1970-01-01'")
                .addRoundTrip("date", "'1983-04-01'", DATE, "DATE '1983-04-01'")
                .addRoundTrip("date", "'1983-10-01'", DATE, "DATE '1983-10-01'");

        SqlDataTypeTest testsTrino = SqlDataTypeTest.create()
                .addRoundTrip("date", "NULL", DATE, "CAST(NULL AS DATE)")
                // first day of AD
                .addRoundTrip("date", "DATE '0001-01-01'", DATE, "DATE '0001-01-01'")
                .addRoundTrip("date", "DATE '0012-12-12'", DATE, "DATE '0012-12-12'")
                // before julian->gregorian switch
                .addRoundTrip("date", "DATE '1500-01-01'", DATE, "DATE '1500-01-01'")
                // before epoch
                .addRoundTrip("date", "DATE '1952-04-03'", DATE, "DATE '1952-04-03'")
                .addRoundTrip("date", "DATE '1970-01-01'", DATE, "DATE '1970-01-01'")
                .addRoundTrip("date", "DATE '1970-02-03'", DATE, "DATE '1970-02-03'")
                // summer on northern hemisphere (possible DST)
                .addRoundTrip("date", "DATE '2017-07-01'", DATE, "DATE '2017-07-01'")
                // winter on northern hemisphere (possible DST on southern hemisphere)
                .addRoundTrip("date", "DATE '2017-01-01'", DATE, "DATE '2017-01-01'")
                .addRoundTrip("date", "DATE '1970-01-01'", DATE, "DATE '1970-01-01'")
                .addRoundTrip("date", "DATE '1983-04-01'", DATE, "DATE '1983-04-01'")
                .addRoundTrip("date", "DATE '1983-10-01'", DATE, "DATE '1983-10-01'");

        for (String timeZoneId : ImmutableList.of(UTC_KEY.getId(), jvmZone.getId(), someZone.getId())) {
            Session session = Session.builder(getSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(timeZoneId))
                    .build();
            testsSqlServer.execute(getQueryRunner(), session, sqlServerCreateAndInsert("test_date"));
            testsTrino.execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_date"));
            testsTrino.execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_date"));
        }
    }

    @Test
    public void testDateJulianGregorianCalendarSwitch()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_old_date", "(dt DATE)", ImmutableList.of("DATE '1582-10-05'", "DATE '1582-10-14'"))) {
            // SQL Server returns +10 days when the date is in the range of 1582-10-05 and 1582-10-14, but we need to pass the original value in predicates
            assertQuery("SELECT * FROM " + table.getName(), "VALUES DATE '1582-10-15', DATE '1582-10-24'");
            assertQuery("SELECT * FROM " + table.getName() + " WHERE dt = DATE '1582-10-05'", "VALUES DATE '1582-10-15'");
            assertQueryReturnsEmptyResult("SELECT * FROM " + table.getName() + " WHERE dt = DATE '1582-10-15'");
        }
    }

    @Test
    public void testSqlServerDateUnsupported()
    {
        // SQL Server does not support > 4 digit years, this test will fail once > 4 digit years support will be added
        String unsupportedDate = "\'11111-01-01\'";
        String tableName = "test_date_unsupported" + randomTableSuffix();
        assertUpdate(format("CREATE TABLE %s (test_date date)", tableName));
        try {
            assertQueryFails(format("INSERT INTO %s VALUES (date %s)", tableName, unsupportedDate),
                    "Failed to insert data: Conversion failed when converting date and/or time from character string.");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    public void testTime()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("time(0)", "'00:00:00'", createTimeType(0), "TIME '00:00:00'")
                .addRoundTrip("time(6)", "'00:00:00.000000'", createTimeType(6), "TIME '00:00:00.000000'")
                .addRoundTrip("time(6)", "'00:00:00.123456'", createTimeType(6), "TIME '00:00:00.123456'")
                .addRoundTrip("time(0)", "'12:34:56'", createTimeType(0), "TIME '12:34:56'")
                .addRoundTrip("time(6)", "'12:34:56.123456'", createTimeType(6), "TIME '12:34:56.123456'")

                // maximal value for a precision
                .addRoundTrip("time(0)", "'23:59:59'", createTimeType(0), "TIME '23:59:59'")
                .addRoundTrip("time(1)", "'23:59:59.9'", createTimeType(1), "TIME '23:59:59.9'")
                .addRoundTrip("time(2)", "'23:59:59.99'", createTimeType(2), "TIME '23:59:59.99'")
                .addRoundTrip("time(3)", "'23:59:59.999'", createTimeType(3), "TIME '23:59:59.999'")
                .addRoundTrip("time(4)", "'23:59:59.9999'", createTimeType(4), "TIME '23:59:59.9999'")
                .addRoundTrip("time(5)", "'23:59:59.99999'", createTimeType(5), "TIME '23:59:59.99999'")
                .addRoundTrip("time(6)", "'23:59:59.999999'", createTimeType(6), "TIME '23:59:59.999999'")
                .addRoundTrip("time(7)", "'23:59:59.9999999'", createTimeType(7), "TIME '23:59:59.9999999'")

                .execute(getQueryRunner(), sqlServerCreateAndInsert("test_time"));

        SqlDataTypeTest.create()
                .addRoundTrip("TIME '00:00:00'", "TIME '00:00:00'")
                .addRoundTrip("TIME '00:00:00.000000'", "TIME '00:00:00.000000'")
                .addRoundTrip("TIME '00:00:00.123456'", "TIME '00:00:00.123456'")
                .addRoundTrip("TIME '12:34:56'", "TIME '12:34:56'")
                .addRoundTrip("TIME '12:34:56.123456'", "TIME '12:34:56.123456'")

                // maximal value for a precision
                .addRoundTrip("TIME '23:59:59'", "TIME '23:59:59'")
                .addRoundTrip("TIME '23:59:59.9'", "TIME '23:59:59.9'")
                .addRoundTrip("TIME '23:59:59.99'", "TIME '23:59:59.99'")
                .addRoundTrip("TIME '23:59:59.999'", "TIME '23:59:59.999'")
                .addRoundTrip("TIME '23:59:59.9999'", "TIME '23:59:59.9999'")
                .addRoundTrip("TIME '23:59:59.99999'", "TIME '23:59:59.99999'")
                .addRoundTrip("TIME '23:59:59.999999'", "TIME '23:59:59.999999'")
                .addRoundTrip("TIME '23:59:59.9999999'", "TIME '23:59:59.9999999'")

                .execute(getQueryRunner(), trinoCreateAsSelect("test_time"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_time"));

        SqlDataTypeTest.create()
                // round down
                .addRoundTrip("TIME '00:00:00.00000001'", "TIME '00:00:00.0000000'")
                .addRoundTrip("TIME '00:00:00.000000000001'", "TIME '00:00:00.0000000'")

                // round down, maximal value
                .addRoundTrip("TIME '00:00:00.00000004'", "TIME '00:00:00.0000000'")
                .addRoundTrip("TIME '00:00:00.000000049'", "TIME '00:00:00.0000000'")
                .addRoundTrip("TIME '00:00:00.0000000449'", "TIME '00:00:00.0000000'")
                .addRoundTrip("TIME '00:00:00.00000004449'", "TIME '00:00:00.0000000'")
                .addRoundTrip("TIME '00:00:00.000000044449'", "TIME '00:00:00.0000000'")

                // round up to next day, minimal value
                .addRoundTrip("TIME '23:59:59.99999995'", "TIME '00:00:00.0000000'")
                .addRoundTrip("TIME '23:59:59.999999950'", "TIME '00:00:00.0000000'")
                .addRoundTrip("TIME '23:59:59.9999999500'", "TIME '00:00:00.0000000'")
                .addRoundTrip("TIME '23:59:59.99999995000'", "TIME '00:00:00.0000000'")
                .addRoundTrip("TIME '23:59:59.999999950000'", "TIME '00:00:00.0000000'")

                // round up to next day, maximal value
                .addRoundTrip("TIME '23:59:59.99999999'", "TIME '00:00:00.0000000'")
                .addRoundTrip("TIME '23:59:59.999999999'", "TIME '00:00:00.0000000'")
                .addRoundTrip("TIME '23:59:59.9999999999'", "TIME '00:00:00.0000000'")
                .addRoundTrip("TIME '23:59:59.99999999999'", "TIME '00:00:00.0000000'")
                .addRoundTrip("TIME '23:59:59.999999999999'", "TIME '00:00:00.0000000'")

                // round down
                .addRoundTrip("TIME '23:59:59.999999949999'", "TIME '23:59:59.9999999'")

                .execute(getQueryRunner(), trinoCreateAndInsert("test_time"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_time"));
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    public void testTimestamp(ZoneId sessionZone)
    {
        SqlDataTypeTest tests = SqlDataTypeTest.create()

                // before epoch
                .addRoundTrip("TIMESTAMP '1958-01-01 13:18:03.123'", "TIMESTAMP '1958-01-01 13:18:03.123'")
                // after epoch
                .addRoundTrip("TIMESTAMP '2019-03-18 10:01:17.987'", "TIMESTAMP '2019-03-18 10:01:17.987'")
                // time doubled in JVM zone
                .addRoundTrip("TIMESTAMP '2018-10-28 01:33:17.456'", "TIMESTAMP '2018-10-28 01:33:17.456'")
                // time double in Vilnius
                .addRoundTrip("TIMESTAMP '2018-10-28 03:33:33.333'", "TIMESTAMP '2018-10-28 03:33:33.333'")
                // epoch
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.000'", "TIMESTAMP '1970-01-01 00:00:00.000'")
                // time gap in JVM zone
                .addRoundTrip("TIMESTAMP '1970-01-01 00:13:42.000'", "TIMESTAMP '1970-01-01 00:13:42.000'")
                .addRoundTrip("TIMESTAMP '2018-04-01 02:13:55.123'", "TIMESTAMP '2018-04-01 02:13:55.123'")
                // time gap in Vilnius
                .addRoundTrip("TIMESTAMP '2018-03-25 03:17:17.000'", "TIMESTAMP '2018-03-25 03:17:17.000'")
                // time gap in Kathmandu
                .addRoundTrip("TIMESTAMP '1986-01-01 00:13:07.000'", "TIMESTAMP '1986-01-01 00:13:07.000'")

                // same as above but with higher precision
                .addRoundTrip("TIMESTAMP '1958-01-01 13:18:03.1230000'", "TIMESTAMP '1958-01-01 13:18:03.1230000'")
                .addRoundTrip("TIMESTAMP '2019-03-18 10:01:17.9870000'", "TIMESTAMP '2019-03-18 10:01:17.9870000'")
                .addRoundTrip("TIMESTAMP '2018-10-28 01:33:17.4560000'", "TIMESTAMP '2018-10-28 01:33:17.4560000'")
                .addRoundTrip("TIMESTAMP '2018-10-28 03:33:33.3330000'", "TIMESTAMP '2018-10-28 03:33:33.3330000'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.0000000'", "TIMESTAMP '1970-01-01 00:00:00.0000000'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:13:42.0000000'", "TIMESTAMP '1970-01-01 00:13:42.0000000'")
                .addRoundTrip("TIMESTAMP '2018-04-01 02:13:55.1230000'", "TIMESTAMP '2018-04-01 02:13:55.1230000'")
                .addRoundTrip("TIMESTAMP '2018-03-25 03:17:17.0000000'", "TIMESTAMP '2018-03-25 03:17:17.0000000'")
                .addRoundTrip("TIMESTAMP '1986-01-01 00:13:07.0000000'", "TIMESTAMP '1986-01-01 00:13:07.0000000'")

                // test arbitrary time for all supported precisions
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00'", "TIMESTAMP '1970-01-01 00:00:00'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.1'", "TIMESTAMP '1970-01-01 00:00:00.1'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.12'", "TIMESTAMP '1970-01-01 00:00:00.12'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.123'", "TIMESTAMP '1970-01-01 00:00:00.123'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.1234'", "TIMESTAMP '1970-01-01 00:00:00.1234'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.12345'", "TIMESTAMP '1970-01-01 00:00:00.12345'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.123456'", "TIMESTAMP '1970-01-01 00:00:00.123456'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.1234567'", "TIMESTAMP '1970-01-01 00:00:00.1234567'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.12345670'", "TIMESTAMP '1970-01-01 00:00:00.1234567'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.123456749999'", "TIMESTAMP '1970-01-01 00:00:00.1234567'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.12345675'", "TIMESTAMP '1970-01-01 00:00:00.1234568'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.12345679'", "TIMESTAMP '1970-01-01 00:00:00.1234568'")

                // before epoch with second fraction
                .addRoundTrip("TIMESTAMP '1969-12-31 23:59:59.1230000'", "TIMESTAMP '1969-12-31 23:59:59.1230000'")
                .addRoundTrip("TIMESTAMP '1969-12-31 23:59:59.1234567'", "TIMESTAMP '1969-12-31 23:59:59.1234567'")

                // precision 0 ends up as precision 0
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00'", "TIMESTAMP '1970-01-01 00:00:00'")

                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.1'", "TIMESTAMP '1970-01-01 00:00:00.1'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.9'", "TIMESTAMP '1970-01-01 00:00:00.9'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.123'", "TIMESTAMP '1970-01-01 00:00:00.123'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.123000'", "TIMESTAMP '1970-01-01 00:00:00.123000'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.999'", "TIMESTAMP '1970-01-01 00:00:00.999'")
                // max supported precision
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.1234567'", "TIMESTAMP '1970-01-01 00:00:00.1234567'")

                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.1'", "TIMESTAMP '2020-09-27 12:34:56.1'")
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.9'", "TIMESTAMP '2020-09-27 12:34:56.9'")
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.123'", "TIMESTAMP '2020-09-27 12:34:56.123'")
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.123000'", "TIMESTAMP '2020-09-27 12:34:56.123000'")
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.999'", "TIMESTAMP '2020-09-27 12:34:56.999'")
                // max supported precision
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.1234567'", "TIMESTAMP '2020-09-27 12:34:56.1234567'")

                // round down
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.12345671'", "TIMESTAMP '1970-01-01 00:00:00.1234567'")

                // nanos round up, end result rounds down
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.1234567499'", "TIMESTAMP '1970-01-01 00:00:00.1234567'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.123456749999'", "TIMESTAMP '1970-01-01 00:00:00.1234567'")

                // round up
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.12345675'", "TIMESTAMP '1970-01-01 00:00:00.1234568'")

                // max precision
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.111222333444'", "TIMESTAMP '1970-01-01 00:00:00.1112223'")

                // round up to next second
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.99999995'", "TIMESTAMP '1970-01-01 00:00:01.0000000'")

                // round up to next day
                .addRoundTrip("TIMESTAMP '1970-01-01 23:59:59.99999995'", "TIMESTAMP '1970-01-02 00:00:00.0000000'")

                // negative epoch
                .addRoundTrip("TIMESTAMP '1969-12-31 23:59:59.99999995'", "TIMESTAMP '1970-01-01 00:00:00.0000000'")
                .addRoundTrip("TIMESTAMP '1969-12-31 23:59:59.999999949999'", "TIMESTAMP '1969-12-31 23:59:59.9999999'")
                .addRoundTrip("TIMESTAMP '1969-12-31 23:59:59.99999994'", "TIMESTAMP '1969-12-31 23:59:59.9999999'");

        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        tests.execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_timestamp"));
        tests.execute(getQueryRunner(), session, trinoCreateAsSelect("test_timestamp"));
        tests.execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_timestamp"));
        tests.execute(getQueryRunner(), session, trinoCreateAndInsert("test_timestamp"));
    }

    @Test
    public void testSqlServerDatetime2()
    {
        SqlDataTypeTest.create()
                // literal values with higher precision are NOT rounded and cause an error
                .addRoundTrip("DATETIME2(0)", "'1970-01-01 00:00:00'", createTimestampType(0), "TIMESTAMP '1970-01-01 00:00:00'")
                .addRoundTrip("DATETIME2(1)", "'1970-01-01 00:00:00.1'", createTimestampType(1), "TIMESTAMP '1970-01-01 00:00:00.1'")
                .addRoundTrip("DATETIME2(1)", "'1970-01-01 00:00:00.9'", createTimestampType(1), "TIMESTAMP '1970-01-01 00:00:00.9'")
                .addRoundTrip("DATETIME2(3)", "'1970-01-01 00:00:00.123'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.123'")
                .addRoundTrip("DATETIME2(6)", "'1970-01-01 00:00:00.123000'", createTimestampType(6), "TIMESTAMP '1970-01-01 00:00:00.123000'")
                .addRoundTrip("DATETIME2(3)", "'1970-01-01 00:00:00.999'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.999'")
                .addRoundTrip("DATETIME2(7)", "'1970-01-01 00:00:00.1234567'", createTimestampType(7), "TIMESTAMP '1970-01-01 00:00:00.1234567'")
                .addRoundTrip("DATETIME2(1)", "'2020-09-27 12:34:56.1'", createTimestampType(1), "TIMESTAMP '2020-09-27 12:34:56.1'")
                .addRoundTrip("DATETIME2(1)", "'2020-09-27 12:34:56.9'", createTimestampType(1), "TIMESTAMP '2020-09-27 12:34:56.9'")
                .addRoundTrip("DATETIME2(3)", "'2020-09-27 12:34:56.123'", createTimestampType(3), "TIMESTAMP '2020-09-27 12:34:56.123'")
                .addRoundTrip("DATETIME2(6)", "'2020-09-27 12:34:56.123000'", createTimestampType(6), "TIMESTAMP '2020-09-27 12:34:56.123000'")
                .addRoundTrip("DATETIME2(3)", "'2020-09-27 12:34:56.999'", createTimestampType(3), "TIMESTAMP '2020-09-27 12:34:56.999'")
                .addRoundTrip("DATETIME2(7)", "'2020-09-27 12:34:56.1234567'", createTimestampType(7), "TIMESTAMP '2020-09-27 12:34:56.1234567'")

                .addRoundTrip("DATETIME2(7)", "'1970-01-01 00:00:00'", createTimestampType(7), "TIMESTAMP '1970-01-01 00:00:00.0000000'")
                .addRoundTrip("DATETIME2(7)", "'1970-01-01 00:00:00.1'", createTimestampType(7), "TIMESTAMP '1970-01-01 00:00:00.1000000'")
                .addRoundTrip("DATETIME2(7)", "'1970-01-01 00:00:00.9'", createTimestampType(7), "TIMESTAMP '1970-01-01 00:00:00.9000000'")
                .addRoundTrip("DATETIME2(7)", "'1970-01-01 00:00:00.123'", createTimestampType(7), "TIMESTAMP '1970-01-01 00:00:00.1230000'")
                .addRoundTrip("DATETIME2(7)", "'1970-01-01 00:00:00.123000'", createTimestampType(7), "TIMESTAMP '1970-01-01 00:00:00.1230000'")
                .addRoundTrip("DATETIME2(7)", "'1970-01-01 00:00:00.999'", createTimestampType(7), "TIMESTAMP '1970-01-01 00:00:00.9990000'")
                .addRoundTrip("DATETIME2(7)", "'1970-01-01 00:00:00.1234567'", createTimestampType(7), "TIMESTAMP '1970-01-01 00:00:00.1234567'")
                .addRoundTrip("DATETIME2(7)", "'2020-09-27 12:34:56.1'", createTimestampType(7), "TIMESTAMP '2020-09-27 12:34:56.1000000'")
                .addRoundTrip("DATETIME2(7)", "'2020-09-27 12:34:56.9'", createTimestampType(7), "TIMESTAMP '2020-09-27 12:34:56.9000000'")
                .addRoundTrip("DATETIME2(7)", "'2020-09-27 12:34:56.123'", createTimestampType(7), "TIMESTAMP '2020-09-27 12:34:56.1230000'")
                .addRoundTrip("DATETIME2(7)", "'2020-09-27 12:34:56.123000'", createTimestampType(7), "TIMESTAMP '2020-09-27 12:34:56.1230000'")
                .addRoundTrip("DATETIME2(7)", "'2020-09-27 12:34:56.999'", createTimestampType(7), "TIMESTAMP '2020-09-27 12:34:56.9990000'")
                .addRoundTrip("DATETIME2(7)", "'2020-09-27 12:34:56.1234567'", createTimestampType(7), "TIMESTAMP '2020-09-27 12:34:56.1234567'")

                .execute(getQueryRunner(), sqlServerCreateAndInsert("test_sqlserver_timestamp"));
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    public void testSqlServerDatetimeOffset(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        SqlDataTypeTest.create()
                // With +00:00 time zone
                .addRoundTrip("DATETIMEOFFSET", "'1970-01-01 00:00:00'", createTimestampWithTimeZoneType(7), "TIMESTAMP '1970-01-01 00:00:00.0000000+00:00'")
                .addRoundTrip("DATETIMEOFFSET(1)", "'1970-01-01 00:00:00.1'", createTimestampWithTimeZoneType(1), "TIMESTAMP '1970-01-01 00:00:00.1+00:00'")
                .addRoundTrip("DATETIMEOFFSET(1)", "'1970-01-01 00:00:00.9'", createTimestampWithTimeZoneType(1), "TIMESTAMP '1970-01-01 00:00:00.9+00:00'")
                .addRoundTrip("DATETIMEOFFSET(3)", "'1970-01-01 00:00:00.123'", createTimestampWithTimeZoneType(3), "TIMESTAMP '1970-01-01 00:00:00.123+00:00'")
                .addRoundTrip("DATETIMEOFFSET(6)", "'1970-01-01 00:00:00.123000'", createTimestampWithTimeZoneType(6), "TIMESTAMP '1970-01-01 00:00:00.123000+00:00'")
                .addRoundTrip("DATETIMEOFFSET(7)", "'1970-01-01 00:00:00.1234567'", createTimestampWithTimeZoneType(7), "TIMESTAMP '1970-01-01 00:00:00.1234567+00:00'")
                .addRoundTrip("DATETIMEOFFSET(3)", "'1970-01-01 00:00:00.999'", createTimestampWithTimeZoneType(3), "TIMESTAMP '1970-01-01 00:00:00.999+00:00'")
                .addRoundTrip("DATETIMEOFFSET", "'1970-01-01 00:00:00.1234567'", createTimestampWithTimeZoneType(7), "TIMESTAMP '1970-01-01 00:00:00.1234567+00:00'")
                .addRoundTrip("DATETIMEOFFSET(1)", "'2020-09-27 12:34:56.1'", createTimestampWithTimeZoneType(1), "TIMESTAMP '2020-09-27 12:34:56.1+00:00'")
                .addRoundTrip("DATETIMEOFFSET(1)", "'2020-09-27 12:34:56.9'", createTimestampWithTimeZoneType(1), "TIMESTAMP '2020-09-27 12:34:56.9+00:00'")
                .addRoundTrip("DATETIMEOFFSET(3)", "'2020-09-27 12:34:56.123'", createTimestampWithTimeZoneType(3), "TIMESTAMP '2020-09-27 12:34:56.123+00:00'")
                .addRoundTrip("DATETIMEOFFSET(6)", "'2020-09-27 12:34:56.123000'", createTimestampWithTimeZoneType(6), "TIMESTAMP '2020-09-27 12:34:56.123000+00:00'")
                .addRoundTrip("DATETIMEOFFSET(7)", "'2020-09-27 12:34:56.9999999'", createTimestampWithTimeZoneType(7), "TIMESTAMP '2020-09-27 12:34:56.9999999+00:00'")
                .addRoundTrip("DATETIMEOFFSET(3)", "'2020-09-27 12:34:56.999'", createTimestampWithTimeZoneType(3), "TIMESTAMP '2020-09-27 12:34:56.999+00:00'")
                .addRoundTrip("DATETIMEOFFSET", "'2020-09-27 12:34:56.1234567'", createTimestampWithTimeZoneType(7), "TIMESTAMP '2020-09-27 12:34:56.1234567+00:00'")

                .addRoundTrip("DATETIMEOFFSET", "'1970-01-01 00:00:00'", createTimestampWithTimeZoneType(7), "TIMESTAMP '1970-01-01 00:00:00.0000000+00:00'")
                .addRoundTrip("DATETIMEOFFSET", "'1970-01-01 00:00:00.1'", createTimestampWithTimeZoneType(7), "TIMESTAMP '1970-01-01 00:00:00.1000000+00:00'")
                .addRoundTrip("DATETIMEOFFSET", "'1970-01-01 00:00:00.9'", createTimestampWithTimeZoneType(7), "TIMESTAMP '1970-01-01 00:00:00.9000000+00:00'")
                .addRoundTrip("DATETIMEOFFSET", "'1970-01-01 00:00:00.123'", createTimestampWithTimeZoneType(7), "TIMESTAMP '1970-01-01 00:00:00.1230000+00:00'")
                .addRoundTrip("DATETIMEOFFSET", "'1970-01-01 00:00:00.123000'", createTimestampWithTimeZoneType(7), "TIMESTAMP '1970-01-01 00:00:00.1230000+00:00'")
                .addRoundTrip("DATETIMEOFFSET", "'1970-01-01 00:00:00.999'", createTimestampWithTimeZoneType(7), "TIMESTAMP '1970-01-01 00:00:00.9990000+00:00'")
                .addRoundTrip("DATETIMEOFFSET", "'1970-01-01 00:00:00.1234567'", createTimestampWithTimeZoneType(7), "TIMESTAMP '1970-01-01 00:00:00.1234567+00:00'")
                .addRoundTrip("DATETIMEOFFSET", "'2020-09-27 12:34:56.1'", createTimestampWithTimeZoneType(7), "TIMESTAMP '2020-09-27 12:34:56.1000000+00:00'")
                .addRoundTrip("DATETIMEOFFSET", "'2020-09-27 12:34:56.9'", createTimestampWithTimeZoneType(7), "TIMESTAMP '2020-09-27 12:34:56.9000000+00:00'")
                .addRoundTrip("DATETIMEOFFSET", "'2020-09-27 12:34:56.123'", createTimestampWithTimeZoneType(7), "TIMESTAMP '2020-09-27 12:34:56.1230000+00:00'")
                .addRoundTrip("DATETIMEOFFSET", "'2020-09-27 12:34:56.123000'", createTimestampWithTimeZoneType(7), "TIMESTAMP '2020-09-27 12:34:56.1230000+00:00'")
                .addRoundTrip("DATETIMEOFFSET", "'2020-09-27 12:34:56.999'", createTimestampWithTimeZoneType(7), "TIMESTAMP '2020-09-27 12:34:56.9990000+00:00'")
                .addRoundTrip("DATETIMEOFFSET", "'2020-09-27 12:34:56.1234567'", createTimestampWithTimeZoneType(7), "TIMESTAMP '2020-09-27 12:34:56.1234567+00:00'")

                // With various time zone
                .addRoundTrip("DATETIMEOFFSET(1)", "'1970-01-01 00:00:00.1+01:00'", createTimestampWithTimeZoneType(1), "TIMESTAMP '1970-01-01 00:00:00.1+01:00'")
                .addRoundTrip("DATETIMEOFFSET(2)", "'1970-01-01 00:00:00.12+03:00'", createTimestampWithTimeZoneType(2), "TIMESTAMP '1970-01-01 00:00:00.12+03:00'")
                .addRoundTrip("DATETIMEOFFSET(3)", "'1970-01-01 00:00:00.123+03:00'", createTimestampWithTimeZoneType(3), "TIMESTAMP '1970-01-01 00:00:00.123+03:00'")
                .addRoundTrip("DATETIMEOFFSET(4)", "'1970-01-01 00:00:00.1234+04:00'", createTimestampWithTimeZoneType(4), "TIMESTAMP '1970-01-01 00:00:00.1234+04:00'")
                .addRoundTrip("DATETIMEOFFSET(5)", "'1970-01-01 00:00:00.12345+04:00'", createTimestampWithTimeZoneType(5), "TIMESTAMP '1970-01-01 00:00:00.12345+04:00'")
                .addRoundTrip("DATETIMEOFFSET(6)", "'1970-01-01 00:00:00.123456+04:00'", createTimestampWithTimeZoneType(6), "TIMESTAMP '1970-01-01 00:00:00.123456+04:00'")
                .addRoundTrip("DATETIMEOFFSET(7)", "'1970-01-01 00:00:00.1234567+07:00'", createTimestampWithTimeZoneType(7), "TIMESTAMP '1970-01-01 00:00:00.1234567+07:00'")
                .addRoundTrip("DATETIMEOFFSET", "'1970-01-01 00:00:00.1234567+07:00'", createTimestampWithTimeZoneType(7), "TIMESTAMP '1970-01-01 00:00:00.1234567+07:00'")

                .addRoundTrip("DATETIMEOFFSET(1)", "'2020-09-27 00:00:00.1+01:00'", createTimestampWithTimeZoneType(1), "TIMESTAMP '2020-09-27 00:00:00.1+01:00'")
                .addRoundTrip("DATETIMEOFFSET(2)", "'2020-09-27 00:00:00.12+03:00'", createTimestampWithTimeZoneType(2), "TIMESTAMP '2020-09-27 00:00:00.12+03:00'")
                .addRoundTrip("DATETIMEOFFSET(3)", "'2020-09-27 00:00:00.123+03:00'", createTimestampWithTimeZoneType(3), "TIMESTAMP '2020-09-27 00:00:00.123+03:00'")
                .addRoundTrip("DATETIMEOFFSET(4)", "'2020-09-27 00:00:00.1234+04:00'", createTimestampWithTimeZoneType(4), "TIMESTAMP '2020-09-27 00:00:00.1234+04:00'")
                .addRoundTrip("DATETIMEOFFSET(5)", "'2020-09-27 00:00:00.12345+04:00'", createTimestampWithTimeZoneType(5), "TIMESTAMP '2020-09-27 00:00:00.12345+04:00'")
                .addRoundTrip("DATETIMEOFFSET(6)", "'2020-09-27 00:00:00.123456+04:00'", createTimestampWithTimeZoneType(6), "TIMESTAMP '2020-09-27 00:00:00.123456+04:00'")
                .addRoundTrip("DATETIMEOFFSET(7)", "'2020-09-27 00:00:00.1234567+07:00'", createTimestampWithTimeZoneType(7), "TIMESTAMP '2020-09-27 00:00:00.1234567+07:00'")
                .addRoundTrip("DATETIMEOFFSET", "'2020-09-27 00:00:00.1234567+07:00'", createTimestampWithTimeZoneType(7), "TIMESTAMP '2020-09-27 00:00:00.1234567+07:00'")

                // before epoch with second fraction
                .addRoundTrip("DATETIMEOFFSET", "'1969-12-31 23:59:59.1230000'", createTimestampWithTimeZoneType(7), "TIMESTAMP '1969-12-31 23:59:59.1230000+00:00'")
                .addRoundTrip("DATETIMEOFFSET", "'1969-12-31 23:59:59.1234567'", createTimestampWithTimeZoneType(7), "TIMESTAMP '1969-12-31 23:59:59.1234567+00:00'")

                // round down
                .addRoundTrip("DATETIMEOFFSET(6)", "'1970-01-01 00:00:00.1234561'", createTimestampWithTimeZoneType(6), "TIMESTAMP '1970-01-01 00:00:00.123456+00:00'")

                // nanos round up, end result rounds down
                .addRoundTrip("DATETIMEOFFSET(4)", "'1970-01-01 00:00:00.1234499'", createTimestampWithTimeZoneType(4), "TIMESTAMP '1970-01-01 00:00:00.1234+00:00'")
                .addRoundTrip("DATETIMEOFFSET(5)", "'1970-01-01 00:00:00.12345499'", createTimestampWithTimeZoneType(5), "TIMESTAMP '1970-01-01 00:00:00.12345+00:00'")

                // round up
                .addRoundTrip("DATETIMEOFFSET(6)", "'1970-01-01 00:00:00.1234565'", createTimestampWithTimeZoneType(6), "TIMESTAMP '1970-01-01 00:00:00.123457+00:00'")

                // round up to next second
                .addRoundTrip("DATETIMEOFFSET(6)", "'1970-01-01 00:00:00.9999999'", createTimestampWithTimeZoneType(6), "TIMESTAMP '1970-01-01 00:00:01.000000+00:00'")

                // round up to next day
                .addRoundTrip("DATETIMEOFFSET(6)", "'1970-01-01 23:59:59.9999995'", createTimestampWithTimeZoneType(6), "TIMESTAMP '1970-01-02 00:00:00.000000+00:00'")

                // negative epoch
                .addRoundTrip("DATETIMEOFFSET(6)", "'1969-12-31 23:59:59.9999995'", createTimestampWithTimeZoneType(6), "TIMESTAMP '1970-01-01 00:00:00.000000+00:00'")
                .addRoundTrip("DATETIMEOFFSET(6)", "'1969-12-31 23:59:59.9999994'", createTimestampWithTimeZoneType(6), "TIMESTAMP '1969-12-31 23:59:59.999999+00:00'")

                .execute(getQueryRunner(), session, sqlServerCreateAndInsert("test_sqlserver_datetimeoffset"));
    }

    @DataProvider
    public Object[][] sessionZonesDataProvider()
    {
        return new Object[][] {
                {UTC},
                {ZoneId.systemDefault()},
                // using two non-JVM zones so that we don't need to worry what SQL Server system zone is
                // no DST in 1970, but has DST in later years (e.g. 2018)
                {ZoneId.of("Europe/Vilnius")},
                // minutes offset change since 1970-01-01, no DST
                {ZoneId.of("Asia/Kathmandu")},
                {ZoneId.of(TestingSession.DEFAULT_TIME_ZONE_KEY.getId())},
        };
    }

    protected DataSetup trinoCreateAsSelect(String tableNamePrefix)
    {
        return trinoCreateAsSelect(getSession(), tableNamePrefix);
    }

    protected DataSetup trinoCreateAsSelect(Session session, String tableNamePrefix)
    {
        return new CreateAsSelectDataSetup(new TrinoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    protected DataSetup trinoCreateAndInsert(String tableNamePrefix)
    {
        return trinoCreateAndInsert(getSession(), tableNamePrefix);
    }

    protected DataSetup trinoCreateAndInsert(Session session, String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(new TrinoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    protected DataSetup sqlServerCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(onRemoteDatabase(), tableNamePrefix);
    }

    protected DataSetup sqlServerCreateAndTrinoInsert(String tableNamePrefix)
    {
        return sqlServerCreateAndTrinoInsert(getSession(), tableNamePrefix);
    }

    protected DataSetup sqlServerCreateAndTrinoInsert(Session session, String tableNamePrefix)
    {
        return new CreateAndTrinoInsertDataSetup(onRemoteDatabase(), new TrinoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    private static void checkIsDoubled(ZoneId zone, LocalDateTime dateTime)
    {
        verify(zone.getRules().getValidOffsets(dateTime).size() == 2, "Expected %s to be doubled in %s", dateTime, zone);
    }

    private static boolean isGap(ZoneId zone, LocalDateTime dateTime)
    {
        return zone.getRules().getValidOffsets(dateTime).isEmpty();
    }

    private static void checkIsGap(ZoneId zone, LocalDateTime dateTime)
    {
        verify(isGap(zone, dateTime), "Expected %s to be a gap in %s", dateTime, zone);
    }

    private void assertSqlServerQueryFails(@Language("SQL") String sql, String expectedMessage)
    {
        assertThatThrownBy(() -> onRemoteDatabase().execute(sql))
                .getCause()
                .hasMessageContaining(expectedMessage);
    }

    protected SqlExecutor onRemoteDatabase()
    {
        return sqlServer::execute;
    }
}
