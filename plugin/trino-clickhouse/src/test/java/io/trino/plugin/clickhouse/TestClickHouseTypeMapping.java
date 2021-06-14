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
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.datatype.CreateAsSelectDataSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.datatype.DataTypeTest;
import io.trino.testing.datatype.SqlDataTypeTest;
import io.trino.testing.sql.TrinoSqlExecutor;
import io.trino.type.UuidType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.clickhouse.ClickHouseQueryRunner.createClickHouseQueryRunner;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.testing.datatype.DataType.bigintDataType;
import static io.trino.testing.datatype.DataType.dateDataType;
import static io.trino.testing.datatype.DataType.doubleDataType;
import static io.trino.testing.datatype.DataType.integerDataType;
import static io.trino.testing.datatype.DataType.realDataType;
import static io.trino.testing.datatype.DataType.smallintDataType;
import static io.trino.testing.datatype.DataType.tinyintDataType;

public class TestClickHouseTypeMapping
        extends AbstractTestQueryFramework
{
    private TestingClickHouseServer clickhouseServer;

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
                        .put("allow-drop-table", "true")
                        .build(),
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
        // TODO SqlDataTypeTest
        DataTypeTest.create()
                .addRoundTrip(bigintDataType(), 123_456_789_012L)
                .addRoundTrip(integerDataType(), 1_234_567_890)
                .addRoundTrip(smallintDataType(), (short) 32_456)
                .addRoundTrip(tinyintDataType(), (byte) 5)
                .addRoundTrip(doubleDataType(), 123.45d)
                .addRoundTrip(realDataType(), 123.45f)

                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_basic_types"))

                // TODO test ClickHouse Nullable(...)
                .addRoundTrip(bigintDataType(), null)
                .addRoundTrip(integerDataType(), null)
                .addRoundTrip(smallintDataType(), null)
                .addRoundTrip(tinyintDataType(), null)
                .addRoundTrip(doubleDataType(), null)
                .addRoundTrip(realDataType(), null)

                .execute(getQueryRunner(), trinoCreateAsSelect("test_basic_types"));
    }

    @Test
    public void testReal()
    {
        // TODO SqlDataTypeTest
        DataTypeTest.create()
                .addRoundTrip(realDataType(), 12.5f)
                .addRoundTrip(realDataType(), Float.NaN)
                .addRoundTrip(realDataType(), Float.NEGATIVE_INFINITY)
                .addRoundTrip(realDataType(), Float.POSITIVE_INFINITY)

                // TODO .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_real"))

                // TODO test ClickHouse Nullable(...)
                .addRoundTrip(realDataType(), null)

                .execute(getQueryRunner(), trinoCreateAsSelect("trino__test_real"));
    }

    @Test
    public void testDouble()
    {
        // TODO SqlDataTypeTest
        DataTypeTest.create()
                .addRoundTrip(doubleDataType(), 3.1415926835)
                .addRoundTrip(doubleDataType(), 1.79769E+308)
                .addRoundTrip(doubleDataType(), 2.225E-307)

                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_decimal"))

                // TODO test ClickHouse Nullable(...)
                .addRoundTrip(doubleDataType(), null)

                .execute(getQueryRunner(), trinoCreateAsSelect("trino_test_double"));
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

                // TODO test ClickHouse Nullable(decimal(....))
                .addRoundTrip("decimal(3, 1)", "NULL", createDecimalType(3, 1), "CAST(NULL AS decimal(3,1))")
                .addRoundTrip("decimal(30, 5)", "NULL", createDecimalType(30, 5), "CAST(NULL AS decimal(30,5))")

                .execute(getQueryRunner(), trinoCreateAsSelect("test_decimal"));
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
                .execute(getQueryRunner(), trinoCreateAsSelect("test_char"));
    }

    @Test
    public void testClickHouseVarchar()
    {
        // ClickHouse varchar is String, which is arbitrary bytes
        SqlDataTypeTest.create()
                // TODO add more test cases
                // plain
                .addRoundTrip("varchar(30)", "'Piękna łąka w 東京都'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                // nullable
                .addRoundTrip("Nullable(varchar(30))", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("Nullable(varchar(30))", "'Piękna łąka w 東京都'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_varchar"));
    }

    @Test
    public void testClickHouseString()
    {
        SqlDataTypeTest.create()
                // TODO add more test cases
                // plain
                .addRoundTrip("String", "'Piękna łąka w 東京都'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                // nullable
                .addRoundTrip("Nullable(String)", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("Nullable(String)", "'Piękna łąka w 東京都'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_varchar"));
    }

    @Test
    public void testTrinoVarchar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("varchar(30)", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("varchar(30)", "'Piękna łąka w 東京都'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_varchar"));
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

        // TODO SqlDataTypeTest
        DataTypeTest testCases = DataTypeTest.create(true)
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
            testCases.execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_date"));
            testCases.execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_date"));
        }
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
                // TODO map to Trino IPADDRESS
                .addRoundTrip("IPv4", "'116.253.40.133'", createUnboundedVarcharType(), "VARCHAR '116.253.40.133'")
                // TODO map to Trino IPADDRESS
                .addRoundTrip("IPv6", "'2001:44c8:129:2632:33:0:252:2'", createUnboundedVarcharType(), "VARCHAR '2001:44c8:129:2632:33:0:252:2'")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_ip"));

        // TODO add test with IPADDRESS written from Trino
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
}
