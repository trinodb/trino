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
package io.trino.plugin.clickhouse;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.UuidType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingSession;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.datatype.CreateAndTrinoInsertDataSetup;
import io.trino.testing.datatype.CreateAsSelectDataSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.datatype.SqlDataTypeTest;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TrinoSqlExecutor;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.clickhouse.ClickHouseQueryRunner.TPCH_SCHEMA;
import static io.trino.plugin.clickhouse.ClickHouseQueryRunner.createClickHouseQueryRunner;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.type.IpAddressType.IPADDRESS;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;

public class TestClickHouseTypeMapping
        extends AbstractTestQueryFramework
{
    private final ZoneId jvmZone = ZoneId.systemDefault();

    // no DST in 1970, but has DST in later years (e.g. 2018)
    private final ZoneId vilnius = ZoneId.of("Europe/Vilnius");

    // minutes offset change since 1970-01-01, no DST
    private final ZoneId kathmandu = ZoneId.of("Asia/Kathmandu");

    private TestingClickHouseServer clickhouseServer;

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

        LocalDate timeGapInKathmandu = LocalDate.of(1986, 1, 1);
        checkIsGap(kathmandu, timeGapInKathmandu.atStartOfDay());
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

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        clickhouseServer = new TestingClickHouseServer();
        return createClickHouseQueryRunner(clickhouseServer, ImmutableMap.of(),
                ImmutableMap.<String, String>builder()
                        .put("metadata.cache-ttl", "10m")
                        .put("metadata.cache-missing", "true")
                        .buildOrThrow(),
                ImmutableList.of());
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        clickhouseServer.close();
    }

    @Test
    public void testBasicTypes()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("bigint", "123456789012", BIGINT, "123456789012")
                .addRoundTrip("integer", "1234567890", INTEGER, "1234567890")
                .addRoundTrip("smallint", "32456", SMALLINT, "SMALLINT '32456'")
                .addRoundTrip("tinyint", "5", TINYINT, "TINYINT '5'")
                .addRoundTrip("double", "123.45", DOUBLE, "DOUBLE '123.45'")
                .addRoundTrip("real", "123.45", REAL, "REAL '123.45'")

                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_basic_types"))

                .addRoundTrip("bigint", "NULL", BIGINT, "CAST(NULL AS BIGINT)")
                .addRoundTrip("integer", "NULL", INTEGER, "CAST(NULL AS INTEGER)")
                .addRoundTrip("smallint", "NULL", SMALLINT, "CAST(NULL AS SMALLINT)")
                .addRoundTrip("tinyint", "NULL", TINYINT, "CAST(NULL AS TINYINT)")
                .addRoundTrip("double", "NULL", DOUBLE, "CAST(NULL AS DOUBLE)")
                .addRoundTrip("real", "NULL", REAL, "CAST(NULL AS REAL)")

                .execute(getQueryRunner(), trinoCreateAsSelect("test_basic_types"));

        SqlDataTypeTest.create()
                .addRoundTrip("Nullable(bigint)", "NULL", BIGINT, "CAST(NULL AS BIGINT)")
                .addRoundTrip("Nullable(integer)", "NULL", INTEGER, "CAST(NULL AS INTEGER)")
                .addRoundTrip("Nullable(smallint)", "NULL", SMALLINT, "CAST(NULL AS SMALLINT)")
                .addRoundTrip("Nullable(tinyint)", "NULL", TINYINT, "CAST(NULL AS TINYINT)")
                .addRoundTrip("Nullable(double)", "NULL", DOUBLE, "CAST(NULL AS DOUBLE)")
                .addRoundTrip("Nullable(real)", "NULL", REAL, "CAST(NULL AS REAL)")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_nullable_types"));
    }

    @Test
    public void testReal()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("real", "12.5", REAL, "REAL '12.5'")
                .addRoundTrip("real", "nan()", REAL, "CAST(nan() AS REAL)")
                .addRoundTrip("real", "-infinity()", REAL, "CAST(-infinity() AS REAL)")
                .addRoundTrip("real", "+infinity()", REAL, "CAST(+infinity() AS REAL)")
                .addRoundTrip("real", "NULL", REAL, "CAST(NULL AS REAL)")
                .execute(getQueryRunner(), trinoCreateAsSelect("trino_test_real"));

        SqlDataTypeTest.create()
                .addRoundTrip("real", "12.5", REAL, "REAL '12.5'")
                .addRoundTrip("real", "nan", REAL, "CAST(nan() AS REAL)")
                .addRoundTrip("real", "-inf", REAL, "CAST(-infinity() AS REAL)")
                .addRoundTrip("real", "+inf", REAL, "CAST(+infinity() AS REAL)")
                .addRoundTrip("Nullable(real)", "NULL", REAL, "CAST(NULL AS REAL)")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_real"));
    }

    @Test
    public void testDouble()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("double", "3.1415926835", DOUBLE, "DOUBLE '3.1415926835'")
                .addRoundTrip("double", "1.79769E308", DOUBLE, "DOUBLE '1.79769E308'")
                .addRoundTrip("double", "2.225E-307", DOUBLE, "DOUBLE '2.225E-307'")

                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_double"))

                .addRoundTrip("double", "NULL", DOUBLE, "CAST(NULL AS DOUBLE)")

                .execute(getQueryRunner(), trinoCreateAsSelect("trino_test_double"));

        SqlDataTypeTest.create()
                .addRoundTrip("Nullable(double)", "NULL", DOUBLE, "CAST(NULL AS DOUBLE)")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.trino_test_nullable_double"));
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

                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_decimal"))

                .addRoundTrip("decimal(3, 1)", "NULL", createDecimalType(3, 1), "CAST(NULL AS decimal(3,1))")
                .addRoundTrip("decimal(30, 5)", "NULL", createDecimalType(30, 5), "CAST(NULL AS decimal(30,5))")

                .execute(getQueryRunner(), trinoCreateAsSelect("test_decimal"));

        SqlDataTypeTest.create()
                .addRoundTrip("Nullable(decimal(3, 1))", "NULL", createDecimalType(3, 1), "CAST(NULL AS decimal(3,1))")
                .addRoundTrip("Nullable(decimal(30, 5))", "NULL", createDecimalType(30, 5), "CAST(NULL AS decimal(30,5))")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_nullable_decimal"));
    }

    @Test
    public void testClickHouseChar()
    {
        // ClickHouse char is FixedString, which is arbitrary bytes
        SqlDataTypeTest.create()
                // plain
                .addRoundTrip("char(10)", "'text_a'", VARBINARY, "to_utf8('text_a')")
                .addRoundTrip("char(255)", "'text_b'", VARBINARY, "to_utf8('text_b')")
                .addRoundTrip("char(5)", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("char(32)", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("char(1)", "'😂'", VARBINARY, "to_utf8('😂')")
                .addRoundTrip("char(77)", "'Ну, погоди!'", VARBINARY, "to_utf8('Ну, погоди!')")
                // nullable
                .addRoundTrip("Nullable(char(10))", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("Nullable(char(10))", "'text_a'", VARBINARY, "to_utf8('text_a')")
                .addRoundTrip("Nullable(char(1))", "'😂'", VARBINARY, "to_utf8('😂')")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_char"));

        // Set map_string_as_varchar session property as true
        SqlDataTypeTest.create()
                // plain
                .addRoundTrip("char(10)", "'text_a'", VARCHAR, "CAST('text_a' AS varchar)")
                .addRoundTrip("char(255)", "'text_b'", VARCHAR, "CAST('text_b' AS varchar)")
                .addRoundTrip("char(5)", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("char(32)", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("char(1)", "'😂'", VARCHAR, "CAST('😂' AS varchar)")
                .addRoundTrip("char(77)", "'Ну, погоди!'", VARCHAR, "CAST('Ну, погоди!' AS varchar)")
                // nullable
                .addRoundTrip("Nullable(char(10))", "NULL", VARCHAR, "CAST(NULL AS varchar)")
                .addRoundTrip("Nullable(char(10))", "'text_a'", VARCHAR, "CAST('text_a' AS varchar)")
                .addRoundTrip("Nullable(char(1))", "'😂'", VARCHAR, "CAST('😂' AS varchar)")
                .execute(getQueryRunner(), mapStringAsVarcharSession(), clickhouseCreateAndInsert("tpch.test_char"));
    }

    @Test
    public void testClickHouseFixedString()
    {
        SqlDataTypeTest.create()
                // plain
                .addRoundTrip("FixedString(10)", "'c12345678b'", VARBINARY, "to_utf8('c12345678b')")
                .addRoundTrip("FixedString(10)", "'c123'", VARBINARY, "to_utf8('c123\0\0\0\0\0\0')")
                // nullable
                .addRoundTrip("Nullable(FixedString(10))", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("Nullable(FixedString(10))", "'c12345678b'", VARBINARY, "to_utf8('c12345678b')")
                .addRoundTrip("Nullable(FixedString(10))", "'c123'", VARBINARY, "to_utf8('c123\0\0\0\0\0\0')")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_fixed_string"));

        // Set map_string_as_varchar session property as true
        SqlDataTypeTest.create()
                // plain
                .addRoundTrip("FixedString(10)", "'c12345678b'", VARCHAR, "CAST('c12345678b' AS varchar)")
                .addRoundTrip("FixedString(10)", "'c123'", VARCHAR, "CAST('c123\0\0\0\0\0\0' AS varchar)")
                // nullable
                .addRoundTrip("Nullable(FixedString(10))", "NULL", VARCHAR, "CAST(NULL AS varchar)")
                .addRoundTrip("Nullable(FixedString(10))", "'c12345678b'", VARCHAR, "CAST('c12345678b' AS varchar)")
                .addRoundTrip("Nullable(FixedString(10))", "'c123'", VARCHAR, "CAST('c123\0\0\0\0\0\0' AS varchar)")
                .execute(getQueryRunner(), mapStringAsVarcharSession(), clickhouseCreateAndInsert("tpch.test_fixed_string"));
    }

    @Test
    public void testTrinoChar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("char(10)", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("char(10)", "'text_a'", VARBINARY, "to_utf8('text_a')")
                .addRoundTrip("char(255)", "'text_b'", VARBINARY, "to_utf8('text_b')")
                .addRoundTrip("char(5)", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("char(32)", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("char(1)", "'😂'", VARBINARY, "to_utf8('😂')")
                .addRoundTrip("char(77)", "'Ну, погоди!'", VARBINARY, "to_utf8('Ну, погоди!')")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_char"))
                .execute(getQueryRunner(), trinoCreateAsSelect(mapStringAsVarcharSession(), "test_char"));

        // Set map_string_as_varchar session property as true
        SqlDataTypeTest.create()
                .addRoundTrip("char(10)", "NULL", VARCHAR, "CAST(NULL AS varchar)")
                .addRoundTrip("char(10)", "'text_a'", VARCHAR, "CAST('text_a' AS varchar)")
                .addRoundTrip("char(255)", "'text_b'", VARCHAR, "CAST('text_b' AS varchar)")
                .addRoundTrip("char(5)", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("char(32)", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("char(1)", "'😂'", VARCHAR, "CAST('😂' AS varchar)")
                .addRoundTrip("char(77)", "'Ну, погоди!'", VARCHAR, "CAST('Ну, погоди!' AS varchar)")
                .execute(getQueryRunner(), mapStringAsVarcharSession(), trinoCreateAsSelect("test_char"))
                .execute(getQueryRunner(), mapStringAsVarcharSession(), trinoCreateAsSelect(mapStringAsVarcharSession(), "test_char"));
    }

    @Test
    public void testClickHouseVarchar()
    {
        // TODO add more test cases
        // ClickHouse varchar is String, which is arbitrary bytes
        SqlDataTypeTest.create()
                // plain
                .addRoundTrip("varchar(30)", "'Piękna łąka w 東京都'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                // nullable
                .addRoundTrip("Nullable(varchar(30))", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("Nullable(varchar(30))", "'Piękna łąka w 東京都'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_varchar"));

        // Set map_string_as_varchar session property as true
        SqlDataTypeTest.create()
                // plain
                .addRoundTrip("varchar(30)", "'Piękna łąka w 東京都'", VARCHAR, "CAST('Piękna łąka w 東京都' AS varchar)")
                // nullable
                .addRoundTrip("Nullable(varchar(30))", "NULL", VARCHAR, "CAST(NULL AS varchar)")
                .addRoundTrip("Nullable(varchar(30))", "'Piękna łąka w 東京都'", VARCHAR, "CAST('Piękna łąka w 東京都' AS varchar)")
                .execute(getQueryRunner(), mapStringAsVarcharSession(), clickhouseCreateAndInsert("tpch.test_varchar"));
    }

    @Test
    public void testClickHouseString()
    {
        // TODO add more test cases
        SqlDataTypeTest.create()
                // plain
                .addRoundTrip("String", "'Piękna łąka w 東京都'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                // nullable
                .addRoundTrip("Nullable(String)", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("Nullable(String)", "'Piękna łąka w 東京都'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_varchar"));

        // Set map_string_as_varchar session property as true
        SqlDataTypeTest.create()
                // plain
                .addRoundTrip("String", "'Piękna łąka w 東京都'", VARCHAR, "CAST('Piękna łąka w 東京都' AS varchar)")
                // nullable
                .addRoundTrip("Nullable(String)", "NULL", VARCHAR, "CAST(NULL AS varchar)")
                .addRoundTrip("Nullable(String)", "'Piękna łąka w 東京都'", VARCHAR, "CAST('Piękna łąka w 東京都' AS varchar)")
                .execute(getQueryRunner(), mapStringAsVarcharSession(), clickhouseCreateAndInsert("tpch.test_varchar"));
    }

    @Test
    public void testTrinoVarchar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("varchar(30)", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("varchar(30)", "'Piękna łąka w 東京都'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_varchar"))
                .execute(getQueryRunner(), trinoCreateAsSelect(mapStringAsVarcharSession(), "test_varchar"));

        // Set map_string_as_varchar session property as true
        SqlDataTypeTest.create()
                .addRoundTrip("varchar(30)", "NULL", VARCHAR, "CAST(NULL AS varchar)")
                .addRoundTrip("varchar(30)", "'Piękna łąka w 東京都'", VARCHAR, "CAST('Piękna łąka w 東京都' AS varchar)")
                .execute(getQueryRunner(), mapStringAsVarcharSession(), trinoCreateAsSelect("test_varchar"))
                .execute(getQueryRunner(), mapStringAsVarcharSession(), trinoCreateAsSelect(mapStringAsVarcharSession(), "test_varchar"));
    }

    @Test
    public void testTrinoVarbinary()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("varbinary", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("varbinary", "X''", VARBINARY, "X''")
                .addRoundTrip("varbinary", "X'68656C6C6F'", VARBINARY, "to_utf8('hello')")
                .addRoundTrip("varbinary", "X'5069C4996B6E6120C582C4856B61207720E69DB1E4BAACE983BD'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip("varbinary", "X'4261672066756C6C206F6620F09F92B0'", VARBINARY, "to_utf8('Bag full of 💰')")
                .addRoundTrip("varbinary", "X'0001020304050607080DF9367AA7000000'", VARBINARY, "X'0001020304050607080DF9367AA7000000'") // non-text
                .addRoundTrip("varbinary", "X'000000000000'", VARBINARY, "X'000000000000'")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_varbinary"));
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    public void testDate(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();
        SqlDataTypeTest.create()
                .addRoundTrip("date", "DATE '1970-01-01'", DATE, "DATE '1970-01-01'") // min value in ClickHouse
                .addRoundTrip("date", "DATE '1970-02-03'", DATE, "DATE '1970-02-03'")
                .addRoundTrip("date", "DATE '2017-07-01'", DATE, "DATE '2017-07-01'") // summer on northern hemisphere (possible DST)
                .addRoundTrip("date", "DATE '2017-01-01'", DATE, "DATE '2017-01-01'") // winter on northern hemisphere (possible DST on southern hemisphere)
                .addRoundTrip("date", "DATE '1970-01-01'", DATE, "DATE '1970-01-01'")
                .addRoundTrip("date", "DATE '1983-04-01'", DATE, "DATE '1983-04-01'")
                .addRoundTrip("date", "DATE '1983-10-01'", DATE, "DATE '1983-10-01'")
                .addRoundTrip("date", "DATE '2106-02-07'", DATE, "DATE '2106-02-07'") // max value in ClickHouse
                .execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_date"))
                .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_date"));

        // Null
        SqlDataTypeTest.create()
                .addRoundTrip("date", "NULL", DATE, "CAST(NULL AS DATE)")
                .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_date"));
        SqlDataTypeTest.create()
                .addRoundTrip("Nullable(date)", "NULL", DATE, "CAST(NULL AS DATE)")
                .execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_date"));
    }

    @Test
    public void testUnsupportedDate()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_unsupported_date", "(dt date)")) {
            assertQueryFails(
                    format("INSERT INTO %s VALUES (DATE '1969-12-31')", table.getName()),
                    "Date must be between 1970-01-01 and 2106-02-07 in ClickHouse: 1969-12-31");
            assertQueryFails(
                    format("INSERT INTO %s VALUES (DATE '2106-02-08')", table.getName()),
                    "Date must be between 1970-01-01 and 2106-02-07 in ClickHouse: 2106-02-08");
        }
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    public void testTimestamp(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        // TODO (https://github.com/trinodb/trino/issues/10538) Support writing timestamp and datetime types in ClickHouse connector
        addTimestampRoundTrips("timestamp")
                .execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_timestamp"));
        addTimestampRoundTrips("datetime")
                .execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_datetime"));
    }

    private SqlDataTypeTest addTimestampRoundTrips(String inputType)
    {
        return SqlDataTypeTest.create()
                .addRoundTrip(inputType, "'1969-12-31 23:59:59'", createTimestampType(0), "TIMESTAMP '1970-01-01 23:59:59'") // unsupported timestamp become 1970-01-01 23:59:59
                .addRoundTrip(inputType, "'1970-01-01 00:00:00'", createTimestampType(0), "TIMESTAMP '1970-01-01 00:00:00'") // min value in ClickHouse
                .addRoundTrip(inputType, "'1986-01-01 00:13:07'", createTimestampType(0), "TIMESTAMP '1986-01-01 00:13:07'") // time gap in Kathmandu
                .addRoundTrip(inputType, "'2018-03-25 03:17:17'", createTimestampType(0), "TIMESTAMP '2018-03-25 03:17:17'") // time gap in Vilnius
                .addRoundTrip(inputType, "'2018-10-28 01:33:17'", createTimestampType(0), "TIMESTAMP '2018-10-28 01:33:17'") // time doubled in JVM zone
                .addRoundTrip(inputType, "'2018-10-28 03:33:33'", createTimestampType(0), "TIMESTAMP '2018-10-28 03:33:33'") // time double in Vilnius
                .addRoundTrip(inputType, "'2105-12-31 23:59:59'", createTimestampType(0), "TIMESTAMP '2105-12-31 23:59:59'") // max value in ClickHouse
                .addRoundTrip(inputType, "'2106-01-01 00:00:00'", createTimestampType(0), "TIMESTAMP '2106-01-01 00:00:00'") // unsupported timestamp become 1970-01-01 23:59:59
                .addRoundTrip(format("Nullable(%s)", inputType), "NULL", createTimestampType(0), "CAST(NULL AS TIMESTAMP(0))");
    }

    @DataProvider
    public Object[][] sessionZonesDataProvider()
    {
        return new Object[][] {
                {UTC},
                {jvmZone},
                // using two non-JVM zones so that we don't need to worry what ClickHouse system zone is
                {vilnius},
                {kathmandu},
                {ZoneId.of(TestingSession.DEFAULT_TIME_ZONE_KEY.getId())},
        };
    }

    @Test
    public void testEnum()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("Enum('hello' = 1, 'world' = 2)", "'hello'", createUnboundedVarcharType(), "VARCHAR 'hello'")
                .addRoundTrip("Enum('hello' = 1, 'world' = 2)", "'world'", createUnboundedVarcharType(), "VARCHAR 'world'")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_enum"));
    }

    @Test
    public void testUuid()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("Nullable(UUID)", "NULL", UuidType.UUID, "CAST(NULL AS UUID)")
                .addRoundTrip("Nullable(UUID)", "'114514ea-0601-1981-1142-e9b55b0abd6d'", UuidType.UUID, "CAST('114514ea-0601-1981-1142-e9b55b0abd6d' AS UUID)")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("default.ck_test_uuid"));

        SqlDataTypeTest.create()
                .addRoundTrip("CAST(NULL AS UUID)", "cast(NULL as UUID)")
                .addRoundTrip("UUID '114514ea-0601-1981-1142-e9b55b0abd6d'", "CAST('114514ea-0601-1981-1142-e9b55b0abd6d' AS UUID)")
                .execute(getQueryRunner(), trinoCreateAsSelect("default.ck_test_uuid"))
                .execute(getQueryRunner(), trinoCreateAndInsert("default.ck_test_uuid"));
    }

    @Test
    public void testIp()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("IPv4", "'0.0.0.0'", IPADDRESS, "IPADDRESS '0.0.0.0'")
                .addRoundTrip("IPv4", "'116.253.40.133'", IPADDRESS, "IPADDRESS '116.253.40.133'")
                .addRoundTrip("IPv4", "'255.255.255.255'", IPADDRESS, "IPADDRESS '255.255.255.255'")
                .addRoundTrip("IPv6", "'::'", IPADDRESS, "IPADDRESS '::'")
                .addRoundTrip("IPv6", "'2001:44c8:129:2632:33:0:252:2'", IPADDRESS, "IPADDRESS '2001:44c8:129:2632:33:0:252:2'")
                .addRoundTrip("IPv6", "'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'", IPADDRESS, "IPADDRESS 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'")
                .addRoundTrip("Nullable(IPv4)", "NULL", IPADDRESS, "CAST(NULL AS IPADDRESS)")
                .addRoundTrip("Nullable(IPv6)", "NULL", IPADDRESS, "CAST(NULL AS IPADDRESS)")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_ip"));

        SqlDataTypeTest.create()
                .addRoundTrip("IPv4", "IPADDRESS '0.0.0.0'", IPADDRESS, "IPADDRESS '0.0.0.0'")
                .addRoundTrip("IPv4", "IPADDRESS '116.253.40.133'", IPADDRESS, "IPADDRESS '116.253.40.133'")
                .addRoundTrip("IPv4", "IPADDRESS '255.255.255.255'", IPADDRESS, "IPADDRESS '255.255.255.255'")
                .addRoundTrip("IPv6", "IPADDRESS '::'", IPADDRESS, "IPADDRESS '::'")
                .addRoundTrip("IPv6", "IPADDRESS '2001:44c8:129:2632:33:0:252:2'", IPADDRESS, "IPADDRESS '2001:44c8:129:2632:33:0:252:2'")
                .addRoundTrip("IPv6", "IPADDRESS 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'", IPADDRESS, "IPADDRESS 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'")
                .addRoundTrip("Nullable(IPv4)", "NULL", IPADDRESS, "CAST(NULL AS IPADDRESS)")
                .addRoundTrip("Nullable(IPv6)", "NULL", IPADDRESS, "CAST(NULL AS IPADDRESS)")
                .execute(getQueryRunner(), clickhouseCreateTrinoInsert("tpch.test_ip"));
    }

    private static Session mapStringAsVarcharSession()
    {
        return testSessionBuilder()
                .setCatalog("clickhouse")
                .setSchema(TPCH_SCHEMA)
                .setCatalogSessionProperty("clickhouse", "map_string_as_varchar", "true")
                .build();
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

    private DataSetup clickhouseCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(new ClickHouseSqlExecutor(clickhouseServer::execute), tableNamePrefix);
    }

    private DataSetup clickhouseCreateTrinoInsert(String tableNamePrefix)
    {
        return new CreateAndTrinoInsertDataSetup(new ClickHouseSqlExecutor(clickhouseServer::execute), new TrinoSqlExecutor(getQueryRunner()), tableNamePrefix);
    }
}
