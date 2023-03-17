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
package io.trino.plugin.ignite;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.spi.type.TimeZoneKey;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingSession;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.datatype.SqlDataTypeTest;
import io.trino.testing.sql.JdbcSqlExecutor;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TrinoSqlExecutor;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.ZoneId;

import static io.trino.plugin.ignite.IgniteQueryRunner.createIgniteQueryRunner;
import static io.trino.plugin.ignite.IgniteQueryRunner.createSession;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestIgniteTypeMapping
        extends AbstractTestQueryFramework
{
    private TestingIgniteServer igniteServer;
    private final ZoneId jvmZone = ZoneId.systemDefault();
    // no DST in 1970, but has DST in later years (e.g. 2018)
    private final ZoneId vilnius = ZoneId.of("Europe/Vilnius");
    // minutes offset change since 1970-01-01, no DST
    private final ZoneId kathmandu = ZoneId.of("Asia/Kathmandu");

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.igniteServer = closeAfterClass(TestingIgniteServer.getInstance()).get();
        return createIgniteQueryRunner(
                igniteServer,
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableList.of());
    }

    @Test
    public void testBoolean()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("boolean", "true", BOOLEAN)
                .addRoundTrip("boolean", "false", BOOLEAN)
                .addRoundTrip("boolean", "NULL", BOOLEAN, "CAST(NULL AS BOOLEAN)")
                .execute(getQueryRunner(), igniteCreateAndInsert("test_boolean"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_boolean"));
    }

    @Test
    public void testTinyint()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("tinyint", "-128", TINYINT, "TINYINT '-128'") // min value in Ignite and Trino
                .addRoundTrip("tinyint", "5", TINYINT, "TINYINT '5'")
                .addRoundTrip("tinyint", "127", TINYINT, "TINYINT '127'") // max value in Ignite and Trino
                .addRoundTrip("tinyint", "NULL", TINYINT, "CAST(NULL AS TINYINT)")
                .execute(getQueryRunner(), trinoCreateAndInsert("test_tinyint"))
                .execute(getQueryRunner(), igniteCreateAndInsert("test_tinyint"));
    }

    @Test
    public void testSmallint()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("smallint", "-32768", SMALLINT, "SMALLINT '-32768'") // min value in Ignite and Trino
                .addRoundTrip("smallint", "32456", SMALLINT, "SMALLINT '32456'")
                .addRoundTrip("smallint", "32767", SMALLINT, "SMALLINT '32767'") // max value in Ignite and Trino
                .addRoundTrip("smallint", "NULL", SMALLINT, "CAST(NULL AS SMALLINT)")
                .execute(getQueryRunner(), igniteCreateAndInsert("test_smallint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_smallint"));
    }

    @Test
    public void testInt()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("int", "-2147483648", INTEGER, "-2147483648") // min value in Ignite and Trino
                .addRoundTrip("int", "1234567890", INTEGER, "1234567890")
                .addRoundTrip("int", "2147483647", INTEGER, "2147483647") // max value in Ignite and Trino
                .addRoundTrip("int", "NULL", INTEGER, "CAST(NULL AS INTEGER)")
                .execute(getQueryRunner(), igniteCreateAndInsert("test_int"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_int"));
    }

    @Test
    public void testBigint()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("bigint", "-9223372036854775808", BIGINT, "-9223372036854775808") // min value in Ignite and Trino
                .addRoundTrip("bigint", "123456789012", BIGINT, "123456789012")
                .addRoundTrip("bigint", "9223372036854775807", BIGINT, "9223372036854775807") // max value in Ignite and Trino
                .addRoundTrip("bigint", "NULL", BIGINT, "CAST(NULL AS BIGINT)")
                .execute(getQueryRunner(), igniteCreateAndInsert("test_bigint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_bigint"));
    }

    @Test
    public void testUnsupportedTinyint()
    {
        try (TestTable table = new TestTable(igniteServer::execute, "test_unsupported_tinyint", "(data tinyint primary key, ignore int)")) {
            assertIgniteQueryFailsWithColumnValueOutRange("INSERT INTO " + table.getName() + " (data) VALUES (-129)"); // min - 1
            assertIgniteQueryFailsWithColumnValueOutRange("INSERT INTO " + table.getName() + " (data) VALUES (128)"); // max + 1
        }
    }

    @Test
    public void testUnsupportedSmallint()
    {
        try (TestTable table = new TestTable(igniteServer::execute, "test_unsupported_smallint", "(data smallint primary key, ignore int)")) {
            assertIgniteQueryFailsWithColumnValueOutRange("INSERT INTO " + table.getName() + " (data) VALUES (-32769)"); // min - 1
            assertIgniteQueryFailsWithColumnValueOutRange("INSERT INTO " + table.getName() + " (data) VALUES (32768)"); // max + 1
        }
    }

    @Test
    public void testUnsupportedInt()
    {
        try (TestTable table = new TestTable(igniteServer::execute, "test_unsupported_int", "(data int primary key, ignore int)")) {
            assertIgniteQueryFailsWithColumnValueOutRange("INSERT INTO " + table.getName() + " (data) VALUES (-2147483649)"); // min - 1
            assertIgniteQueryFailsWithColumnValueOutRange("INSERT INTO " + table.getName() + " (data) VALUES (2147483648)"); // max + 1
        }
    }

    @Test
    public void testUnsupportedBigint()
    {
        try (TestTable table = new TestTable(igniteServer::execute, "test_unsupported_bigint", "(data bigint primary key, ignore int)")) {
            assertIgniteQueryFailsWithColumnValueOutRange("INSERT INTO " + table.getName() + " (data) VALUES (-9223372036854775809)"); // min - 1
            assertIgniteQueryFailsWithColumnValueOutRange("INSERT INTO " + table.getName() + " (data) VALUES (9223372036854775808)"); // max + 1
        }
    }

    private void assertIgniteQueryFailsWithColumnValueOutRange(@Language("SQL") String sql)
    {
        assertThatThrownBy(() -> igniteServer.execute(sql))
                .cause()
                .hasMessageContaining("Value conversion failed");
    }

    @Test
    public void testReal()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("real", "3.14", REAL, "REAL '3.14'")
                .addRoundTrip("real", "NULL", REAL, "CAST(NULL AS real)")
                .addRoundTrip("real", "3.1415927", REAL, "REAL '3.1415927'")
                .addRoundTrip("real", "'NaN'::real", REAL, "CAST(nan() AS real)")
                .addRoundTrip("real", "'-Infinity'::real", REAL, "CAST(-infinity() AS real)")
                .addRoundTrip("real", "'+Infinity'::real", REAL, "CAST(+infinity() AS real)")
                .execute(getQueryRunner(), igniteCreateAndInsert("ignite_test_real"));

        SqlDataTypeTest.create()
                .addRoundTrip("real", "3.14", REAL, "REAL '3.14'")
                .addRoundTrip("real", "NULL", REAL, "CAST(NULL AS real)")
                .addRoundTrip("real", "3.1415927", REAL, "REAL '3.1415927'")
                .addRoundTrip("real", "nan()", REAL, "CAST(nan() AS real)")
                .addRoundTrip("real", "-infinity()", REAL, "CAST(-infinity() AS real)")
                .addRoundTrip("real", "+infinity()", REAL, "CAST(+infinity() AS real)")
                .execute(getQueryRunner(), trinoCreateAndInsert("trino_test_real"));
    }

    @Test
    public void testDouble()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("double precision", "1.0E100", DOUBLE, "1.0E100")
                .addRoundTrip("double precision", "NULL", DOUBLE, "CAST(NULL AS double)")
                .addRoundTrip("double precision", "123.456E10", DOUBLE, "123.456E10")
                .addRoundTrip("double precision", "'NaN'::double precision", DOUBLE, "nan()")
                .addRoundTrip("double precision", "'+Infinity'::double precision", DOUBLE, "+infinity()")
                .addRoundTrip("double precision", "'-Infinity'::double precision", DOUBLE, "-infinity()")
                .execute(getQueryRunner(), igniteCreateAndInsert("ignite_test_double"));

        SqlDataTypeTest.create()
                .addRoundTrip("double", "1.0E100", DOUBLE, "1.0E100")
                .addRoundTrip("double", "NULL", DOUBLE, "CAST(NULL AS double)")
                .addRoundTrip("double", "123.456E10", DOUBLE, "123.456E10")
                .addRoundTrip("double", "nan()", DOUBLE, "nan()")
                .addRoundTrip("double", "+infinity()", DOUBLE, "+infinity()")
                .addRoundTrip("double", "-infinity()", DOUBLE, "-infinity()")
                .execute(getQueryRunner(), trinoCreateAndInsert(createSession(), "trino_test_double"));
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
                .addRoundTrip("decimal(38, 0)", "CAST(NULL AS decimal(38, 0))", createDecimalType(38, 0), "CAST(NULL AS decimal(38, 0))")
                .execute(getQueryRunner(), igniteCreateAndInsert("test_decimal"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_decimal"));
    }

    @Test
    public void testBinary()
    {
        binaryTest("binary")
                .execute(getQueryRunner(), igniteCreateAndInsert("test_varbinary"));

        binaryTest("varbinary")
                .execute(getQueryRunner(), trinoCreateAndInsert("test_varbinary"))
                .execute(getQueryRunner(), igniteCreateAndInsert("test_varbinary"));
    }

    private static SqlDataTypeTest binaryTest(String inputType)
    {
        return SqlDataTypeTest.create()
                .addRoundTrip(inputType, "X'68656C6C6F'", VARBINARY, "to_utf8('hello')")
                .addRoundTrip(inputType, "X''", VARBINARY, "X''")
                .addRoundTrip(inputType, "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip(inputType, "X'5069C4996B6E6120C582C4856B61207720E69DB1E4BAACE983BD'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip(inputType, "X'4261672066756C6C206F6620F09F92B0'", VARBINARY, "to_utf8('Bag full of 💰')")
                .addRoundTrip(inputType, "X'0001020304050607080DF9367AA7000000'", VARBINARY, "X'0001020304050607080DF9367AA7000000'") // non-text
                .addRoundTrip(inputType, "X'000000000000'", VARBINARY, "X'000000000000'");
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    public void testDate(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();
        SqlDataTypeTest.create()
                .addRoundTrip("int", "1", INTEGER, "1") // Avoid using date as primary key in Ignite, https://issues.apache.org/jira/browse/IGNITE-8552
                .addRoundTrip("date", "DATE '1970-01-01'", DATE, "DATE '1970-01-01'") // change forward at midnight in JVM
                .addRoundTrip("date", "DATE '1970-02-03'", DATE, "DATE '1970-02-03'")
                .addRoundTrip("date", "DATE '2017-07-01'", DATE, "DATE '2017-07-01'") // summer on northern hemisphere (possible DST)
                .addRoundTrip("date", "DATE '2017-01-01'", DATE, "DATE '2017-01-01'") // winter on northern hemisphere (possible DST on southern hemisphere)
                .addRoundTrip("date", "DATE '1983-04-01'", DATE, "DATE '1983-04-01'") // change forward at midnight in Vilnius
                .addRoundTrip("date", "DATE '1983-10-01'", DATE, "DATE '1983-10-01'")
                .addRoundTrip("date", "DATE '2983-10-01'", DATE, "DATE '2983-10-01'")
                .addRoundTrip("date", "DATE '3983-10-01'", DATE, "DATE '3983-10-01'")
                .addRoundTrip("date", "DATE '4983-10-01'", DATE, "DATE '4983-10-01'")
                .addRoundTrip("date", "DATE '9983-10-01'", DATE, "DATE '9983-10-01'")
                .addRoundTrip("date", "DATE '9999-01-01'", DATE, "DATE '9999-01-01'")
                .addRoundTrip("date", "DATE '9999-12-31'", DATE, "DATE '9999-12-31'")
                .execute(getQueryRunner(), session, trinoCreateAndInsert("test_date"))
                .execute(getQueryRunner(), session, igniteCreateAndInsert("test_date"));
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    public void testUnsupportedDateRange(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        assertQueryFails("CREATE TABLE test_unsupported_date_range_ctas (data) AS SELECT DATE '1582-10-05'", "Date must be between 1970-01-01 and 9999-12-31 in Ignite.*");
        assertUpdate("DROP TABLE IF EXISTS test_unsupported_date_range_ctas");

        ImmutableList<String> unsupportedDateValues = ImmutableList.of("-0001-01-01", "0001-01-01", "1000-01-01", "1969-12-31", "10000-01-01");
        try (TestTable table = new TestTable(igniteServer::execute, "test_unsupported_date_range", "(id int primary key, data date)")) {
            for (int i = 0; i < unsupportedDateValues.size(); i++) {
                doTestUnsupportedDateRange(session, table.getName(), i, unsupportedDateValues.get(i));
            }
        }
    }

    private void doTestUnsupportedDateRange(Session session, String tableName, int id, String date)
    {
        assertQueryFails(session, format("INSERT INTO %s VALUES(%d, DATE '%s')", tableName, id, date), ".*Date must be between 1970-01-01 and 9999-12-31 in Ignite.*");

        // test read unsupported date
        igniteServer.execute(format("INSERT INTO %s VALUES (%d, DATE '%s')", tableName, id, date));
        assertQueryFails(session, format("SELECT data FROM %s WHERE id = %d", tableName, id), "Date must be between 1970-01-01 and 9999-12-31 in Ignite.*");
    }

    // Ignite char is mapped to varchar
    @Test
    public void testChar()
    {
        charVarcharTest("char")
                .execute(getQueryRunner(), igniteCreateAndInsert("test_char"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_char"));
    }

    @Test
    public void testVarchar()
    {
        charVarcharTest("varchar")
                .addRoundTrip("varchar(20000)", "'攻殻機動隊'", createVarcharType(20000), "CAST('攻殻機動隊' AS varchar(20000))")
                .addRoundTrip("varchar(16777215)", "'text_f'", createVarcharType(16777215), "CAST('text_f' AS varchar(16777215))")
                .execute(getQueryRunner(), igniteCreateAndInsert("test_varchar"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_varchar"));
    }

    private static SqlDataTypeTest charVarcharTest(String inputType)
    {
        return SqlDataTypeTest.create()
                .addRoundTrip(inputType + "(10)", "'NULL'", createVarcharType(10), "CAST('NULL' AS varchar(10))")
                .addRoundTrip(inputType + "(10)", "NULL", createVarcharType(10), "CAST(NULL AS varchar(10))")
                .addRoundTrip(inputType + "(10)", "'简体'", createVarcharType(10), "CAST('简体' AS varchar(10))")
                .addRoundTrip(inputType + "(10)", "'text_a'", createVarcharType(10), "CAST('text_a' AS varchar(10))")
                .addRoundTrip(inputType + "(255)", "'text_b'", createVarcharType(255), "CAST('text_b' AS varchar(255))")
                .addRoundTrip(inputType + "(256)", "'text_c'", createVarcharType(256), "CAST('text_c' AS varchar(256))")
                .addRoundTrip(inputType + "(65535)", "'text_d'", createVarcharType(65535), "CAST('text_d' AS varchar(65535))")
                .addRoundTrip(inputType + "(65536)", "'text_e'", createVarcharType(65536), "CAST('text_e' AS varchar(65536))")
                .addRoundTrip(inputType + "(5)", "'攻殻機動隊'", createVarcharType(5), "CAST('攻殻機動隊' AS varchar(5))")
                .addRoundTrip(inputType + "(32)", "'攻殻機動隊'", createVarcharType(32), "CAST('攻殻機動隊' AS varchar(32))")
                .addRoundTrip(inputType + "(10)", "'😂'", createVarcharType(10), "CAST('😂' AS varchar(10))")
                .addRoundTrip(inputType + "(77)", "'Ну, погоди!'", createVarcharType(77), "CAST('Ну, погоди!' AS varchar(77))")
                .addRoundTrip(inputType + "(77)", "'+-*/!234=1'", createVarcharType(77), "CAST('+-*/!234=1' AS varchar(77))");
    }

    @Test
    public void testUnboundedVarchar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("varchar", "'NULL'", createUnboundedVarcharType(), "CAST('NULL' AS varchar)")
                .addRoundTrip("varchar", "NULL", createUnboundedVarcharType(), "CAST(NULL AS varchar)")
                .addRoundTrip("varchar", "'简体'", createUnboundedVarcharType(), "CAST('简体' AS varchar)")
                .addRoundTrip("varchar", "'text_a'", createUnboundedVarcharType(), "CAST('text_a' AS varchar)")
                .addRoundTrip("varchar", "'text_b'", createUnboundedVarcharType(), "CAST('text_b' AS varchar)")
                .addRoundTrip("varchar", "'text_c'", createUnboundedVarcharType(), "CAST('text_c' AS varchar)")
                .addRoundTrip("varchar", "'text_d'", createUnboundedVarcharType(), "CAST('text_d' AS varchar)")
                .addRoundTrip("varchar", "'text_e'", createUnboundedVarcharType(), "CAST('text_e' AS varchar)")
                .addRoundTrip("varchar", "'text_f'", createUnboundedVarcharType(), "CAST('text_f' AS varchar)")
                .addRoundTrip("varchar", "'攻殻機動隊'", createUnboundedVarcharType(), "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("varchar", "'攻殻機動隊'", createUnboundedVarcharType(), "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("varchar", "'攻殻機動隊'", createUnboundedVarcharType(), "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("varchar", "'😂'", createUnboundedVarcharType(), "CAST('😂' AS varchar)")
                .addRoundTrip("varchar", "'Ну, погоди!'", createUnboundedVarcharType(), "CAST('Ну, погоди!' AS varchar)")
                .addRoundTrip("varchar", "'+-*/!234=1'", createUnboundedVarcharType(), "CAST('+-*/!234=1' AS varchar)")
                .execute(getQueryRunner(), igniteCreateAndInsert("test_unbounded_varchar"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_unbounded_varchar"));
    }

    @DataProvider
    public Object[][] sessionZonesDataProvider()
    {
        return new Object[][] {
                {UTC},
                {jvmZone},
                // using two non-JVM zones so that we don't need to worry what Ignite system zone is
                {vilnius},
                {kathmandu},
                {TestingSession.DEFAULT_TIME_ZONE_KEY.getZoneId()},
        };
    }

    private DataSetup trinoCreateAndInsert(String tableNamePrefix)
    {
        return trinoCreateAndInsert(createSession(), tableNamePrefix);
    }

    private DataSetup trinoCreateAndInsert(Session session, String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(new TrinoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    private DataSetup igniteCreateAndInsert(String tableNamePrefix)
    {
        return new IgniteCreateAndInsertDataSetup(new JdbcSqlExecutor(igniteServer.getJdbcUrl()), tableNamePrefix);
    }
}
