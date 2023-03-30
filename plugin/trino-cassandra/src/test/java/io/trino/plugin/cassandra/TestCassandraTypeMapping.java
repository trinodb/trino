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
package io.trino.plugin.cassandra;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.cassandra.TestCassandraTable.ColumnDefinition;
import io.trino.spi.type.RowType.Field;
import io.trino.spi.type.TimeZoneKey;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingSession;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.datatype.CreateAsSelectDataSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.datatype.SqlDataTypeTest;
import io.trino.testing.sql.TrinoSqlExecutor;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.cassandra.CassandraQueryRunner.createCassandraQueryRunner;
import static io.trino.plugin.cassandra.TestCassandraTable.generalColumn;
import static io.trino.plugin.cassandra.TestCassandraTable.partitionColumn;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.RowType.anonymousRow;
import static io.trino.spi.type.RowType.rowType;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.IpAddressType.IPADDRESS;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestCassandraTypeMapping
        extends AbstractTestQueryFramework
{
    private final LocalDateTime beforeJulianGregorianSwitch = LocalDateTime.of(1952, 10, 4, 0, 0, 0);
    private final LocalDateTime beginJulianGregorianSwitch = LocalDateTime.of(1952, 10, 5, 0, 0, 0);
    private final LocalDateTime endJulianGregorianSwitch = LocalDateTime.of(1952, 10, 14, 0, 0, 0);
    private final LocalDateTime beforeEpoch = LocalDateTime.of(1958, 1, 1, 13, 18, 3, 123_000_000);
    private final LocalDateTime epoch = LocalDateTime.of(1970, 1, 1, 0, 0, 0);
    private final LocalDateTime afterEpoch = LocalDateTime.of(2019, 3, 18, 10, 1, 17, 987_000_000);

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

    private final ZoneOffset fixedOffsetEast = ZoneOffset.ofHoursMinutes(2, 17);
    private final ZoneOffset fixedOffsetWest = ZoneOffset.ofHoursMinutes(-7, -31);

    private CassandraServer server;
    private CassandraSession session;

    @BeforeClass
    public void setUp()
    {
        checkState(jvmZone.getId().equals("America/Bahia_Banderas"), "This test assumes certain JVM time zone");
        LocalDate dateOfLocalTimeChangeForwardAtMidnightInJvmZone = LocalDate.of(1970, 1, 1);
        checkIsGap(jvmZone, dateOfLocalTimeChangeForwardAtMidnightInJvmZone.atStartOfDay());
        checkIsGap(jvmZone, timeGapInJvmZone1);
        checkIsGap(jvmZone, timeGapInJvmZone2);
        checkIsDoubled(jvmZone, timeDoubledInJvmZone);

        LocalDate dateOfLocalTimeChangeForwardAtMidnightInSomeZone = LocalDate.of(1983, 4, 1);
        checkIsGap(vilnius, dateOfLocalTimeChangeForwardAtMidnightInSomeZone.atStartOfDay());
        LocalDate dateOfLocalTimeChangeBackwardAtMidnightInSomeZone = LocalDate.of(1983, 10, 1);
        checkIsDoubled(vilnius, dateOfLocalTimeChangeBackwardAtMidnightInSomeZone.atStartOfDay().minusMinutes(1));
        checkIsGap(vilnius, timeGapInVilnius);
        checkIsDoubled(vilnius, timeDoubledInVilnius);

        checkIsGap(kathmandu, timeGapInKathmandu);
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
        server = closeAfterClass(new CassandraServer());
        session = server.getSession();
        return createCassandraQueryRunner(
                server,
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableList.of());
    }

    @AfterClass(alwaysRun = true)
    public void cleanUp()
    {
        session.close();
        session = null;
    }

    @Test
    public void testBoolean()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("boolean", "NULL", BOOLEAN, "CAST(NULL AS BOOLEAN)")
                .addRoundTrip("boolean", "true", BOOLEAN)
                .addRoundTrip("boolean", "false", BOOLEAN)
                .execute(getQueryRunner(), cassandraCreateAndInsert("tpch.test_boolean"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_boolean"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_boolean"));
    }

    @Test
    public void testTinyint()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("tinyint", "NULL", TINYINT, "CAST(NULL AS TINYINT)")
                .addRoundTrip("tinyint", "-128", TINYINT, "TINYINT '-128'") // min value in Cassandra
                .addRoundTrip("tinyint", "5", TINYINT, "TINYINT '5'")
                .addRoundTrip("tinyint", "127", TINYINT, "TINYINT '127'") // max value in Cassandra
                .execute(getQueryRunner(), cassandraCreateAndInsert("tpch.test_tinyint"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_tinyint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_tinyint"));
    }

    @Test
    public void testUnsupportedTinyint()
    {
        try (TestCassandraTable table = testTable(
                "test_unsupported_tinyint",
                ImmutableList.of(partitionColumn("id", "tinyint"), generalColumn("data", "tinyint")),
                ImmutableList.of())) {
            assertCassandraQueryFails(
                    "INSERT INTO " + table.getTableName() + " (id, data) VALUES (1, -129)", // min - 1
                    "Unable to make byte from '-129'");
            assertCassandraQueryFails(
                    "INSERT INTO " + table.getTableName() + " (id, data) VALUES (2, 128)", // max + 1
                    "Unable to make byte from '128'");
        }
    }

    @Test
    public void testSmallint()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("smallint", "NULL", SMALLINT, "CAST(NULL AS SMALLINT)")
                .addRoundTrip("smallint", "-32768", SMALLINT, "SMALLINT '-32768'") // min value in Cassandra
                .addRoundTrip("smallint", "32456", SMALLINT, "SMALLINT '32456'")
                .addRoundTrip("smallint", "32767", SMALLINT, "SMALLINT '32767'") // max value in Cassandra
                .execute(getQueryRunner(), cassandraCreateAndInsert("tpch.test_smallint"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_smallint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_smallint"));
    }

    @Test
    public void testUnsupportedSmallint()
    {
        try (TestCassandraTable table = testTable(
                "test_unsupported_smallint",
                ImmutableList.of(partitionColumn("id", "smallint"), generalColumn("data", "smallint")),
                ImmutableList.of())) {
            assertCassandraQueryFails(
                    "INSERT INTO " + table.getTableName() + " (id, data) VALUES (1, -32769)", // min - 1
                    "Unable to make short from '-32769'");
            assertCassandraQueryFails(
                    "INSERT INTO " + table.getTableName() + " (id, data) VALUES (2, 32768)", // max + 1
                    "Unable to make short from '32768'");
        }
    }

    @Test
    public void testInt()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("int", "NULL", INTEGER, "CAST(NULL AS INTEGER)")
                .addRoundTrip("int", "-2147483648", INTEGER, "-2147483648") // min value in Cassandra
                .addRoundTrip("int", "1234567890", INTEGER, "1234567890")
                .addRoundTrip("int", "2147483647", INTEGER, "2147483647") // max value in Cassandra
                .execute(getQueryRunner(), cassandraCreateAndInsert("tpch.test_int"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_int"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_int"));
    }

    @Test
    public void testUnsupportedInt()
    {
        try (TestCassandraTable table = testTable(
                "test_unsupported_int",
                ImmutableList.of(partitionColumn("id", "int"), generalColumn("data", "int")),
                ImmutableList.of())) {
            assertCassandraQueryFails(
                    "INSERT INTO " + table.getTableName() + " (id, data) VALUES (1, -2147483649)", // min - 1
                    "Unable to make int from '-2147483649'");
            assertCassandraQueryFails(
                    "INSERT INTO " + table.getTableName() + " (id, data) VALUES (2, 2147483648)", // max + 1
                    "Unable to make int from '2147483648'");
        }
    }

    @Test
    public void testBigint()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("bigint", "NULL", BIGINT, "CAST(NULL AS BIGINT)")
                .addRoundTrip("bigint", "-9223372036854775808", BIGINT, "-9223372036854775808") // min value in Cassandra
                .addRoundTrip("bigint", "123456789012", BIGINT, "123456789012")
                .addRoundTrip("bigint", "9223372036854775807", BIGINT, "9223372036854775807") // max value in Cassandra
                .execute(getQueryRunner(), cassandraCreateAndInsert("tpch.test_bigint"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_bigint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_bigint"));
    }

    @Test
    public void testUnsupportedBigint()
    {
        try (TestCassandraTable table = testTable(
                "test_unsupported_bigint",
                ImmutableList.of(partitionColumn("id", "bigint"), generalColumn("data", "bigint")),
                ImmutableList.of())) {
            assertCassandraQueryFails(
                    "INSERT INTO " + table.getTableName() + " (id, data) VALUES (1, -9223372036854775809)", // min - 1
                    "Unable to make long from '-9223372036854775809'");
            assertCassandraQueryFails(
                    "INSERT INTO " + table.getTableName() + " (id, data) VALUES (2, 9223372036854775808)", // max + 1
                    "Unable to make long from '9223372036854775808'");
        }
    }

    @Test
    public void testReal()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("float", "NULL", REAL, "CAST(NULL AS REAL)")
                .addRoundTrip("float", "12.5", REAL, "REAL '12.5'")
                .addRoundTrip("float", "NaN", REAL, "CAST(nan() AS REAL)")
                .addRoundTrip("float", "-Infinity", REAL, "CAST(-infinity() AS REAL)")
                .addRoundTrip("float", "Infinity", REAL, "CAST(+infinity() AS REAL)")
                .execute(getQueryRunner(), cassandraCreateAndInsert("tpch.test_real"));

        SqlDataTypeTest.create()
                .addRoundTrip("real", "NULL", REAL, "CAST(NULL AS REAL)")
                .addRoundTrip("real", "12.5", REAL, "REAL '12.5'")
                .addRoundTrip("real", "nan()", REAL, "CAST(nan() AS REAL)")
                .addRoundTrip("real", "-infinity()", REAL, "CAST(-infinity() AS REAL)")
                .addRoundTrip("real", "+infinity()", REAL, "CAST(+infinity() AS REAL)")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_real"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_real"));
    }

    @Test
    public void testDouble()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("double", "NULL", DOUBLE, "CAST(NULL AS DOUBLE)")
                .addRoundTrip("double", "3.1415926835", DOUBLE, "DOUBLE '3.1415926835'")
                .addRoundTrip("double", "1.79769E308", DOUBLE, "DOUBLE '1.79769E308'")
                .addRoundTrip("double", "2.225E-307", DOUBLE, "DOUBLE '2.225E-307'")
                .execute(getQueryRunner(), cassandraCreateAndInsert("tpch.test_double"))
                .execute(getQueryRunner(), trinoCreateAsSelect("trino_test_double"))
                .execute(getQueryRunner(), trinoCreateAndInsert("trino_test_double"));
    }

    @Test
    public void testDecimal()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("decimal", "NULL", DOUBLE, "CAST(NULL AS DOUBLE)")
                .addRoundTrip("decimal", "3.1415926835", DOUBLE, "DOUBLE '3.1415926835'")
                .addRoundTrip("decimal", "1.79769E308", DOUBLE, "DOUBLE '1.79769E308'")
                .addRoundTrip("decimal", "2.225E-307", DOUBLE, "DOUBLE '2.225E-307'")
                .execute(getQueryRunner(), cassandraCreateAndInsert("tpch.test_decimal"));
    }

    @Test
    public void testCassandraAscii()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("ascii", "NULL", VARCHAR, "CAST(NULL AS varchar)")
                .addRoundTrip("ascii", "'text_a'", VARCHAR, "CAST('text_a' AS varchar)")
                .addRoundTrip("ascii", "'text_b'", VARCHAR, "CAST('text_b' AS varchar)")
                .addRoundTrip("ascii", "'text_c'", VARCHAR, "CAST('text_c' AS varchar)")
                .execute(getQueryRunner(), cassandraCreateAndInsert("tpch.test_ascii"));
    }

    @Test
    public void testCassandraText()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("text", "NULL", VARCHAR, "CAST(NULL AS varchar)")
                .addRoundTrip("text", "'text_a'", VARCHAR, "CAST('text_a' AS varchar)")
                .addRoundTrip("text", "'text_b'", VARCHAR, "CAST('text_b' AS varchar)")
                .addRoundTrip("text", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("text", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("text", "'😂'", VARCHAR, "CAST('😂' AS varchar)")
                .addRoundTrip("text", "'Ну, погоди!'", VARCHAR, "CAST('Ну, погоди!' AS varchar)")
                .execute(getQueryRunner(), cassandraCreateAndInsert("tpch.test_text"));
    }

    @Test
    public void testVarchar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("varchar", "NULL", VARCHAR, "CAST(NULL AS varchar)")
                .addRoundTrip("varchar", "'text_a'", VARCHAR, "CAST('text_a' AS varchar)")
                .addRoundTrip("varchar", "'text_b'", VARCHAR, "CAST('text_b' AS varchar)")
                .addRoundTrip("varchar", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("varchar", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("varchar", "'😂'", VARCHAR, "CAST('😂' AS varchar)")
                .addRoundTrip("varchar", "'Ну, погоди!'", VARCHAR, "CAST('Ну, погоди!' AS varchar)")
                .execute(getQueryRunner(), cassandraCreateAndInsert("tpch.test_varchar"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_varchar"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_varchar"));
    }

    @Test
    public void testCassandraList()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("list<int>", "NULL", VARCHAR, "CAST(NULL as varchar)")
                .addRoundTrip("list<int>", "[]", VARCHAR, "CAST(NULL as varchar)")
                .addRoundTrip("list<int>", "[17,4,2]", VARCHAR, "CAST('[17,4,2]' as varchar)")
                .execute(getQueryRunner(), cassandraCreateAndInsert("tpch.test_list"));
    }

    @Test
    public void testCassandraSet()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("set<text>", "NULL", VARCHAR, "CAST(NULL as varchar)")
                .addRoundTrip("set<text>", "{}", VARCHAR, "CAST(NULL as varchar)")
                .addRoundTrip("set<text>", "{'Trino'}", VARCHAR, "CAST('[\"Trino\"]' as varchar)")
                .execute(getQueryRunner(), cassandraCreateAndInsert("tpch.test_set"));
    }

    @Test
    public void testCassandraMap()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("map<text, text>", "NULL", VARCHAR, "CAST(NULL as varchar)")
                .addRoundTrip("map<text, text>", "{}", VARCHAR, "CAST(NULL as varchar)")
                .addRoundTrip("map<text, text>", "{'connector':'cassandra'}", VARCHAR, "CAST('{\"connector\":\"cassandra\"}' as varchar)")
                .execute(getQueryRunner(), cassandraCreateAndInsert("tpch.test_map"));
    }

    @Test
    public void testCassandraInet()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("inet", "NULL", IPADDRESS, "CAST(NULL AS ipaddress)")
                .addRoundTrip("inet", "'0.0.0.0'", IPADDRESS, "CAST('0.0.0.0' AS ipaddress)")
                .addRoundTrip("inet", "'116.253.40.133'", IPADDRESS, "CAST('116.253.40.133' AS ipaddress)")
                .addRoundTrip("inet", "'255.255.255.255'", IPADDRESS, "CAST('255.255.255.255' AS ipaddress)")
                .addRoundTrip("inet", "'::'", IPADDRESS, "CAST('::' AS ipaddress)")
                .addRoundTrip("inet", "'2001:44c8:129:2632:33:0:252:2'", IPADDRESS, "CAST('2001:44c8:129:2632:33:0:252:2' AS ipaddress)")
                .addRoundTrip("inet", "'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'", IPADDRESS, "CAST('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' AS ipaddress)")
                .addRoundTrip("inet", "'ffff:ffff:ffff:ffff:ffff:ffff:255.255.255.255'", IPADDRESS, "CAST('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' AS ipaddress)")
                .execute(getQueryRunner(), cassandraCreateAndInsert("tpch.test_inet"));
    }

    @Test
    public void testIpAddress()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("ipaddress", "NULL", IPADDRESS, "CAST(NULL AS ipaddress)")
                .addRoundTrip("ipaddress", "ipaddress '0.0.0.0'", IPADDRESS, "CAST('0.0.0.0' AS ipaddress)")
                .addRoundTrip("ipaddress", "ipaddress '116.253.40.133'", IPADDRESS, "CAST('116.253.40.133' AS ipaddress)")
                .addRoundTrip("ipaddress", "ipaddress '255.255.255.255'", IPADDRESS, "CAST('255.255.255.255' AS ipaddress)")
                .addRoundTrip("ipaddress", "ipaddress '::'", IPADDRESS, "CAST('::' AS ipaddress)")
                .addRoundTrip("ipaddress", "ipaddress '2001:44c8:129:2632:33:0:252:2'", IPADDRESS, "CAST('2001:44c8:129:2632:33:0:252:2' AS ipaddress)")
                .addRoundTrip("ipaddress", "ipaddress 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'", IPADDRESS, "CAST('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' AS ipaddress)")
                .addRoundTrip("ipaddress", "ipaddress 'ffff:ffff:ffff:ffff:ffff:ffff:255.255.255.255'", IPADDRESS, "CAST('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' AS ipaddress)")
                .execute(getQueryRunner(), trinoCreateAndInsert("test_ipaddress"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_ipaddress"));
    }

    @Test
    public void testCassandraVarint()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("varint", "NULL", VARCHAR, "CAST(NULL AS varchar)")
                .addRoundTrip("varint", "12345678910", VARCHAR, "CAST('12345678910' AS varchar)")
                .addRoundTrip("varint", "2147483648", VARCHAR, "CAST('2147483648' AS varchar)") // Integer.MAX_VALUE + 1
                .addRoundTrip("varint", "-2147483649", VARCHAR, "CAST('-2147483649' AS varchar)") // Integer.MIN_VALUE - 1
                .addRoundTrip("varint", "9223372036854775808", VARCHAR, "CAST('9223372036854775808' AS varchar)") // Long.MAX_VALUE + 1
                .addRoundTrip("varint", "-9223372036854775809", VARCHAR, "CAST('-9223372036854775809' AS varchar)") // Long.MIN_VALUE - 1
                .execute(getQueryRunner(), cassandraCreateAndInsert("tpch.test_varint"));
    }

    @Test
    public void testCassandraBlob()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("blob", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("blob", "varcharAsBlob('')", VARBINARY, "X''")
                .addRoundTrip("blob", "varcharAsBlob('hello')", VARBINARY, "to_utf8('hello')")
                .addRoundTrip("blob", "varcharAsBlob('Piękna łąka w 東京都')", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip("blob", "varcharAsBlob('Bag full of 💰')", VARBINARY, "to_utf8('Bag full of 💰')")
                // Binary literals must be prefixed with 0[xX]
                // https://docs.datastax.com/en/cql-oss/3.x/cql/cql_reference/blob_r.html
                .addRoundTrip("blob", "0x0001020304050607080DF9367AA7000000", VARBINARY, "X'0001020304050607080DF9367AA7000000'") // non-text
                .addRoundTrip("blob", "0x000000000000", VARBINARY, "X'000000000000'")
                .execute(getQueryRunner(), cassandraCreateAndInsert("tpch.test_blob"));
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
                .execute(getQueryRunner(), trinoCreateAsSelect("test_varbinary"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_varbinary"));
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    public void testDate(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        dateTest(Function.identity())
                .execute(getQueryRunner(), session, cassandraCreateAndInsert("tpch.test_date"));

        dateTest(inputLiteral -> format("DATE %s", inputLiteral))
                .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_date"))
                .execute(getQueryRunner(), session, trinoCreateAsSelect("test_date"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_date"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert("test_date"));
    }

    private SqlDataTypeTest dateTest(Function<String, String> inputLiteralFactory)
    {
        return SqlDataTypeTest.create()
                .addRoundTrip("date", "NULL", DATE, "CAST(NULL AS DATE)")
                .addRoundTrip("date", inputLiteralFactory.apply("'-5877641-06-23'"), DATE, "DATE '-5877641-06-23'") // min value in Cassandra and Trino
                .addRoundTrip("date", inputLiteralFactory.apply("'0001-01-01'"), DATE, "DATE '0001-01-01'")
                .addRoundTrip("date", inputLiteralFactory.apply("'1582-10-04'"), DATE, "DATE '1582-10-04'") // before julian->gregorian switch
                .addRoundTrip("date", inputLiteralFactory.apply("'1582-10-05'"), DATE, "DATE '1582-10-05'") // begin julian->gregorian switch
                .addRoundTrip("date", inputLiteralFactory.apply("'1582-10-14'"), DATE, "DATE '1582-10-14'") // end julian->gregorian switch
                .addRoundTrip("date", inputLiteralFactory.apply("'1952-04-03'"), DATE, "DATE '1952-04-03'") // before epoch
                .addRoundTrip("date", inputLiteralFactory.apply("'1970-01-01'"), DATE, "DATE '1970-01-01'")
                .addRoundTrip("date", inputLiteralFactory.apply("'1970-02-03'"), DATE, "DATE '1970-02-03'")
                .addRoundTrip("date", inputLiteralFactory.apply("'1983-04-01'"), DATE, "DATE '1983-04-01'")
                .addRoundTrip("date", inputLiteralFactory.apply("'1983-10-01'"), DATE, "DATE '1983-10-01'")
                .addRoundTrip("date", inputLiteralFactory.apply("'2017-07-01'"), DATE, "DATE '2017-07-01'") // summer on northern hemisphere (possible DST)
                .addRoundTrip("date", inputLiteralFactory.apply("'2017-01-01'"), DATE, "DATE '2017-01-01'") // winter on northern hemisphere (possible DST on southern hemisphere)
                .addRoundTrip("date", inputLiteralFactory.apply("'5881580-07-11'"), DATE, "DATE '5881580-07-11'"); // max value in Cassandra and Trino
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    public void testTime(ZoneId sessionZone)
    {
        LocalTime timeGapInJvmZone = LocalTime.of(0, 12, 34, 567_000_000);
        checkIsGap(jvmZone, timeGapInJvmZone.atDate(LocalDate.ofEpochDay(0)));

        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        timeTypeTest("time(9)", trinoTimeInputLiteralFactory())
                .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_time"))
                .execute(getQueryRunner(), session, trinoCreateAsSelect("test_time"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_time"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert("test_time"));

        timeTypeTest("time", cassandraTimeInputLiteralFactory())
                .execute(getQueryRunner(), session, cassandraCreateAndInsert("tpch.test_time"));
    }

    private static SqlDataTypeTest timeTypeTest(String inputType, Function<String, String> inputLiteralFactory)
    {
        return SqlDataTypeTest.create()
                .addRoundTrip(inputType, inputLiteralFactory.apply("'09:12:34'"), createTimeType(9), "TIME '09:12:34.000000000'")
                .addRoundTrip(inputType, inputLiteralFactory.apply("'10:12:34.000000000'"), createTimeType(9), "TIME '10:12:34.000000000'")
                .addRoundTrip(inputType, inputLiteralFactory.apply("'15:12:34.567000000'"), createTimeType(9), "TIME '15:12:34.567000000'")
                .addRoundTrip(inputType, inputLiteralFactory.apply("'23:59:59.000000000'"), createTimeType(9), "TIME '23:59:59.000000000'")
                .addRoundTrip(inputType, inputLiteralFactory.apply("'23:59:59.999000000'"), createTimeType(9), "TIME '23:59:59.999000000'")
                .addRoundTrip(inputType, inputLiteralFactory.apply("'23:59:59.999900000'"), createTimeType(9), "TIME '23:59:59.999900000'")
                .addRoundTrip(inputType, inputLiteralFactory.apply("'23:59:59.999990000'"), createTimeType(9), "TIME '23:59:59.999990000'")
                .addRoundTrip(inputType, inputLiteralFactory.apply("'23:59:59.999999999'"), createTimeType(9), "TIME '23:59:59.999999999'")
                .addRoundTrip(inputType, inputLiteralFactory.apply("NULL"), createTimeType(9), "CAST(NULL AS TIME(9))");
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    public void testCassandraTimestamp(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        timestampTest("timestamp", cassandraTimestampInputLiteralFactory(), timestampExpectedLiteralFactory())
                .addRoundTrip("timestamp", "-1", TIMESTAMP_TZ_MILLIS, "AT_TIMEZONE(TIMESTAMP '1969-12-31 23:59:59.999 UTC', 'UTC')") // negative timestamp
                .execute(getQueryRunner(), session, cassandraCreateAndInsert("tpch.test_cassandra_timestamp"));
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    public void testTrinoTimestampWithTimeZone(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        timestampTest("timestamp with time zone", trinoTimestampInputLiteralFactory(), timestampExpectedLiteralFactory())
                .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_trino_timestamp_with_time_zone"))
                .execute(getQueryRunner(), session, trinoCreateAsSelect("test_trino_timestamp_with_time_zone"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_trino_timestamp_with_time_zone"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert("test_trino_timestamp_with_time_zone"));
    }

    private SqlDataTypeTest timestampTest(String inputType, BiFunction<LocalDateTime, ZoneId, String> inputLiteralFactory, BiFunction<LocalDateTime, ZoneId, String> expectedLiteralFactory)
    {
        SqlDataTypeTest tests = SqlDataTypeTest.create()
                .addRoundTrip(inputType, "NULL", TIMESTAMP_TZ_MILLIS, "CAST(NULL AS TIMESTAMP WITH TIME ZONE)");

        for (ZoneId zoneId : ImmutableList.of(UTC, kathmandu, fixedOffsetEast, fixedOffsetWest)) {
            tests.addRoundTrip(inputType, inputLiteralFactory.apply(beforeJulianGregorianSwitch, zoneId), TIMESTAMP_TZ_MILLIS, expectedLiteralFactory.apply(beforeJulianGregorianSwitch, zoneId))
                    .addRoundTrip(inputType, inputLiteralFactory.apply(beginJulianGregorianSwitch, zoneId), TIMESTAMP_TZ_MILLIS, expectedLiteralFactory.apply(beginJulianGregorianSwitch, zoneId))
                    .addRoundTrip(inputType, inputLiteralFactory.apply(endJulianGregorianSwitch, zoneId), TIMESTAMP_TZ_MILLIS, expectedLiteralFactory.apply(endJulianGregorianSwitch, zoneId))
                    .addRoundTrip(inputType, inputLiteralFactory.apply(beforeEpoch, zoneId), TIMESTAMP_TZ_MILLIS, expectedLiteralFactory.apply(beforeEpoch, zoneId))
                    .addRoundTrip(inputType, inputLiteralFactory.apply(epoch, zoneId), TIMESTAMP_TZ_MILLIS, expectedLiteralFactory.apply(epoch, zoneId))
                    .addRoundTrip(inputType, inputLiteralFactory.apply(afterEpoch, zoneId), TIMESTAMP_TZ_MILLIS, expectedLiteralFactory.apply(afterEpoch, zoneId))
                    .addRoundTrip(inputType, inputLiteralFactory.apply(timeDoubledInJvmZone, zoneId), TIMESTAMP_TZ_MILLIS, expectedLiteralFactory.apply(timeDoubledInJvmZone, zoneId))
                    .addRoundTrip(inputType, inputLiteralFactory.apply(timeDoubledInVilnius, zoneId), TIMESTAMP_TZ_MILLIS, expectedLiteralFactory.apply(timeDoubledInVilnius, zoneId))
                    .addRoundTrip(inputType, inputLiteralFactory.apply(timeGapInJvmZone1, zoneId), TIMESTAMP_TZ_MILLIS, expectedLiteralFactory.apply(timeGapInJvmZone1, zoneId))
                    .addRoundTrip(inputType, inputLiteralFactory.apply(timeGapInJvmZone2, zoneId), TIMESTAMP_TZ_MILLIS, expectedLiteralFactory.apply(timeGapInJvmZone2, zoneId))
                    .addRoundTrip(inputType, inputLiteralFactory.apply(timeGapInVilnius, zoneId), TIMESTAMP_TZ_MILLIS, expectedLiteralFactory.apply(timeGapInVilnius, zoneId))
                    .addRoundTrip(inputType, inputLiteralFactory.apply(timeGapInKathmandu, zoneId), TIMESTAMP_TZ_MILLIS, expectedLiteralFactory.apply(timeGapInKathmandu, zoneId));
        }

        return tests;
    }

    @DataProvider
    public Object[][] sessionZonesDataProvider()
    {
        return new Object[][] {
                {UTC},
                {jvmZone},
                // using two non-JVM zones so that we don't need to worry what Cassandra system zone is
                {vilnius},
                {kathmandu},
                {TestingSession.DEFAULT_TIME_ZONE_KEY.getZoneId()},
        };
    }

    @Test
    public void testCassandraTimeUuid()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("timeuuid", "NULL", UUID, "CAST(NULL AS UUID)")
                .addRoundTrip("timeuuid", "50554d6e-29bb-11e5-b345-feff819cdc9f", UUID, "CAST('50554d6e-29bb-11e5-b345-feff819cdc9f' AS UUID)")
                .execute(getQueryRunner(), cassandraCreateAndInsert("tpch.test_timeuuid"));
    }

    @Test
    public void testUuid()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("uuid", "NULL", UUID, "CAST(NULL AS UUID)")
                .addRoundTrip("uuid", "114514ea-0601-1981-1142-e9b55b0abd6d", UUID, "CAST('114514ea-0601-1981-1142-e9b55b0abd6d' AS UUID)")
                .execute(getQueryRunner(), cassandraCreateAndInsert("tpch.test_uuid"));

        SqlDataTypeTest.create()
                .addRoundTrip("uuid", "NULL", UUID, "CAST(NULL AS UUID)")
                .addRoundTrip("uuid", "UUID '114514ea-0601-1981-1142-e9b55b0abd6d'", UUID, "CAST('114514ea-0601-1981-1142-e9b55b0abd6d' AS UUID)")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_uuid"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_uuid"));
    }

    @Test
    public void testCassandraTuple()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("tuple<int, text>", "NULL", anonymousRow(INTEGER, VARCHAR), "CAST(NULL AS ROW(INTEGER, VARCHAR))")
                .addRoundTrip("tuple<int, text>", "(3, 'hours')", anonymousRow(INTEGER, VARCHAR), "CAST(ROW(3, 'hours') AS ROW(INTEGER, VARCHAR))")
                .addRoundTrip("tuple<list<text>>", "(['Cassandra'])", anonymousRow(VARCHAR), "CAST(ROW('[\"Cassandra\"]') AS ROW(VARCHAR))")
                .addRoundTrip("tuple<map<text, text>>", "({'connector':'Cassandra'})", anonymousRow(VARCHAR), "CAST(ROW('{\"connector\":\"Cassandra\"}') AS ROW(VARCHAR))")
                .addRoundTrip("tuple<set<text>>", "({'Cassandra'})", anonymousRow(VARCHAR), "CAST(ROW('[\"Cassandra\"]') AS ROW(VARCHAR))")
                .addRoundTrip("tuple<tuple<int, text>>", "((3, 'hours'))", anonymousRow(anonymousRow(INTEGER, VARCHAR)), "CAST(ROW(ROW(3, 'hours')) AS ROW(ROW(INTEGER, VARCHAR)))")
                .execute(getQueryRunner(), cassandraCreateAndInsert("tpch.test_tuple"));
    }

    @Test
    public void testCassandraUdt()
    {
        session.execute("DROP TYPE IF EXISTS tpch.phone");
        session.execute("CREATE TYPE tpch.phone (country_code int, number text)");

        SqlDataTypeTest.create()
                .addRoundTrip(
                        "frozen<phone>",
                        "NULL",
                        rowType(new Field(Optional.of("country_code"), INTEGER), new Field(Optional.of("number"), VARCHAR)),
                        "CAST(NULL AS ROW(country_code INTEGER, number VARCHAR))")
                .addRoundTrip(
                        "frozen<phone>",
                        "{country_code: 1, number: '202 456-1111'}",
                        rowType(new Field(Optional.of("country_code"), INTEGER), new Field(Optional.of("number"), VARCHAR)),
                        "CAST(ROW(1, '202 456-1111') AS ROW(country_code INTEGER, number VARCHAR))")
                .execute(getQueryRunner(), cassandraCreateAndInsert("tpch.test_udt"));

        session.execute("DROP TYPE IF EXISTS tpch.phone");
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

    private DataSetup cassandraCreateAndInsert(String tableNamePrefix)
    {
        return new CassandraCreateAndInsertDataSetup(session::execute, tableNamePrefix, server);
    }

    private TestCassandraTable testTable(String namePrefix, List<ColumnDefinition> columnDefinitions, List<String> rowsToInsert)
    {
        return new TestCassandraTable(session::execute, server, "tpch", namePrefix, columnDefinitions, rowsToInsert);
    }

    private void assertCassandraQueryFails(@Language("SQL") String sql, String expectedMessage)
    {
        assertThatThrownBy(() -> session.execute(sql)).hasMessageContaining(expectedMessage);
    }

    private static Function<String, String> trinoTimeInputLiteralFactory()
    {
        return "CAST(%s AS TIME(9))"::formatted;
    }

    private static Function<String, String> cassandraTimeInputLiteralFactory()
    {
        return literal -> literal;
    }

    private static BiFunction<LocalDateTime, ZoneId, String> cassandraTimestampInputLiteralFactory()
    {
        return timestampInputLiteralFactory(Optional.empty());
    }

    private static BiFunction<LocalDateTime, ZoneId, String> trinoTimestampInputLiteralFactory()
    {
        return timestampInputLiteralFactory(Optional.of("TIMESTAMP "));
    }

    private static BiFunction<LocalDateTime, ZoneId, String> timestampInputLiteralFactory(Optional<String> inputLiteralPrefix)
    {
        return (inputLiteral, zone) -> format("%s'%s'", inputLiteralPrefix.orElse(""), DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSZ").format(inputLiteral.atZone(zone)));
    }

    private static BiFunction<LocalDateTime, ZoneId, String> timestampExpectedLiteralFactory()
    {
        return (expectedLiteral, zone) -> format("AT_TIMEZONE(TIMESTAMP '%s', 'UTC')", DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSS VV").format(expectedLiteral.atZone(zone)));
    }
}
