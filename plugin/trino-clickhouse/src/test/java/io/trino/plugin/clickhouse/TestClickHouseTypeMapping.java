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
import io.trino.testing.sql.JdbcSqlExecutor;
import io.trino.testing.sql.TrinoSqlExecutor;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.clickhouse.ClickHouseQueryRunner.createClickHouseQueryRunner;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.testing.datatype.DataType.bigintDataType;
import static io.trino.testing.datatype.DataType.dateDataType;
import static io.trino.testing.datatype.DataType.decimalDataType;
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
        DataTypeTest.create()
                .addRoundTrip(bigintDataType(), 123_456_789_012L)
                .addRoundTrip(integerDataType(), 1_234_567_890)
                .addRoundTrip(smallintDataType(), (short) 32_456)
                .addRoundTrip(tinyintDataType(), (byte) 5)
                .addRoundTrip(doubleDataType(), 123.45d)
                .addRoundTrip(realDataType(), 123.45f)
                .execute(getQueryRunner(), trinoCreateAsSelect("test_basic_types"));
    }

    @Test
    public void testFloat()
    {
        DataTypeTest.create()
                .addRoundTrip(realDataType(), Float.NaN)
                .addRoundTrip(realDataType(), Float.NEGATIVE_INFINITY)
                .addRoundTrip(realDataType(), Float.POSITIVE_INFINITY)
                .execute(getQueryRunner(), trinoCreateAsSelect("trino__test_real"));
    }

    @Test
    public void testDouble()
    {
        DataTypeTest.create()
                .addRoundTrip(doubleDataType(), 3.1415926835)
                .addRoundTrip(doubleDataType(), 1.79769E+308)
                .addRoundTrip(doubleDataType(), 2.225E-307)
                .execute(getQueryRunner(), trinoCreateAsSelect("trino_test_double"));
    }

    @Test
    public void testClickHouseCreatedDecimal()
    {
        decimalTests()
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_decimal"));
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
    public void testCharAndVarchar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("char(10)", "'text_a'", createUnboundedVarcharType(), "CAST('text_a' AS varchar)")
                .addRoundTrip("char(255)", "'text_b'", createUnboundedVarcharType(), "CAST('text_b' AS varchar)")
                .addRoundTrip("char(5)", "'攻殻機動隊'", createUnboundedVarcharType(), "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("char(32)", "'攻殻機動隊'", createUnboundedVarcharType(), "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("varchar(30)", "'Piękna łąka w 東京都'", createUnboundedVarcharType(), "cast('Piękna łąka w 東京都' as varchar)")
                .addRoundTrip("char(1)", "'😂'", createUnboundedVarcharType(), "CAST('😂' AS varchar)")
                .addRoundTrip("char(77)", "'Ну, погоди!'", createUnboundedVarcharType(), "CAST('Ну, погоди!' AS varchar)")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_char"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_char"));
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
        JdbcSqlExecutor jdbcSqlExecutor = new JdbcSqlExecutor(clickhouseServer.getJdbcUrl(), clickhouseServer.getProperties());
        jdbcSqlExecutor.execute("CREATE TABLE tpch.t_enum (id Int32, x Enum('hello' = 1, 'world' = 2))ENGINE = Log");
        jdbcSqlExecutor.execute("INSERT INTO tpch.t_enum VALUES (1, 'hello'), (2, 'world'), (3, 'hello')");
        try {
            assertQuery(
                    "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_name = 't_enum'",
                    "VALUES ('id','integer'),('x','varchar')");
            assertQuery("SELECT * FROM tpch.t_enum", "VALUES (1,'hello'),(2,'world'),(3,'hello')");
            assertQuery("SELECT * FROM tpch.t_enum WHERE x='hello'", "VALUES (1,'hello'),(3,'hello')");
        }
        finally {
            jdbcSqlExecutor.execute("DROP TABLE tpch.t_enum");
        }
    }

    @Test
    public void testClickHouseSpecialDataType()
    {
        JdbcSqlExecutor jdbcSqlExecutor = new JdbcSqlExecutor(clickhouseServer.getJdbcUrl(), clickhouseServer.getProperties());
        jdbcSqlExecutor.execute("CREATE TABLE tpch.tbl_ck_type (c_fs FixedString(10), c_uuid UUID, c_ipv4 IPv4, c_ipv6 IPv6) ENGINE = Log");
        jdbcSqlExecutor.execute("INSERT INTO tpch.tbl_ck_type VALUES ('c12345678b', '417ddc5d-e556-4d27-95dd-a34d84e46a50', '116.253.40.133', '2001:44c8:129:2632:33:0:252:2')");
        try {
            assertQuery("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_name = 'tbl_ck_type'",
                    "VALUES ('c_fs', 'varchar'), ('c_uuid', 'varchar'), ('c_ipv4', 'varchar'), ('c_ipv6', 'varchar')");
            assertQuery("SELECT * FROM tpch.tbl_ck_type", "VALUES ('c12345678b', '417ddc5d-e556-4d27-95dd-a34d84e46a50', '116.253.40.133', '2001:44c8:129:2632:33:0:252:2')");
        }
        finally {
            jdbcSqlExecutor.execute("DROP TABLE tpch.tbl_ck_type");
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

    private DataSetup clickhouseCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(new ClickHouseSqlExecutor(clickhouseServer.getJdbcUrl()), tableNamePrefix);
    }
}
