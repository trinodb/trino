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
package io.trino.jdbc;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logging;
import io.trino.client.ClientSelectedRole;
import io.trino.execution.QueryState;
import io.trino.plugin.blackhole.BlackHolePlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.type.TimeZoneKey;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.testing.Assertions.assertContains;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static io.trino.execution.QueryState.FAILED;
import static io.trino.execution.QueryState.RUNNING;
import static java.lang.Float.POSITIVE_INFINITY;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestTrinoDriver
{
    private static final DateTimeZone ASIA_ORAL_ZONE = DateTimeZone.forID("Asia/Oral");
    private static final GregorianCalendar ASIA_ORAL_CALENDAR = new GregorianCalendar(TimeZone.getTimeZone(ZoneId.of(ASIA_ORAL_ZONE.getID())));
    private static final String TEST_CATALOG = "test_catalog";

    private TestingTrinoServer server;
    private ExecutorService executorService;

    @BeforeClass
    public void setup()
            throws Exception
    {
        Logging.initialize();
        server = TestingTrinoServer.create();
        server.installPlugin(new TpchPlugin());
        server.createCatalog(TEST_CATALOG, "tpch");
        server.installPlugin(new BlackHolePlugin());
        server.createCatalog("blackhole", "blackhole");
        server.waitForNodeRefresh(java.time.Duration.ofSeconds(10));
        setupTestTables();
        executorService = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
    }

    private void setupTestTables()
            throws SQLException
    {
        try (Connection connection = createConnection("blackhole", "blackhole");
                Statement statement = connection.createStatement()) {
            assertEquals(statement.executeUpdate("CREATE SCHEMA blackhole.blackhole"), 0);
            assertEquals(statement.executeUpdate("CREATE TABLE test_table (x bigint)"), 0);

            assertEquals(statement.executeUpdate("CREATE TABLE slow_test_table (x bigint) " +
                    "WITH (" +
                    "   split_count = 1, " +
                    "   pages_per_split = 1, " +
                    "   rows_per_page = 1, " +
                    "   page_processing_delay = '1m'" +
                    ")"), 0);
        }
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
            throws Exception
    {
        executorService.shutdownNow();
        executorService = null;
        server.close();
        server = null;
    }

    @Test
    public void testDriverManager()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet rs = statement.executeQuery("" +
                        "SELECT " +
                        "  123 _integer" +
                        ",  12300000000 _bigint" +
                        ", 'foo' _varchar" +
                        ", 0.1E0 _double" +
                        ", true _boolean" +
                        ", cast('hello' as varbinary) _varbinary" +
                        ", DECIMAL '1234567890.1234567' _decimal_short" +
                        ", DECIMAL '.12345678901234567890123456789012345678' _decimal_long" +
                        ", approx_set(42) _hll" +
                        ", cast('foo' as char(5)) _char")) {
                    ResultSetMetaData metadata = rs.getMetaData();

                    assertEquals(metadata.getColumnCount(), 10);

                    assertEquals(metadata.getColumnLabel(1), "_integer");
                    assertEquals(metadata.getColumnType(1), Types.INTEGER);

                    assertEquals(metadata.getColumnLabel(2), "_bigint");
                    assertEquals(metadata.getColumnType(2), Types.BIGINT);

                    assertEquals(metadata.getColumnLabel(3), "_varchar");
                    assertEquals(metadata.getColumnType(3), Types.VARCHAR);

                    assertEquals(metadata.getColumnLabel(4), "_double");
                    assertEquals(metadata.getColumnType(4), Types.DOUBLE);

                    assertEquals(metadata.getColumnLabel(5), "_boolean");
                    assertEquals(metadata.getColumnType(5), Types.BOOLEAN);

                    assertEquals(metadata.getColumnLabel(6), "_varbinary");
                    assertEquals(metadata.getColumnType(6), Types.VARBINARY);

                    assertEquals(metadata.getColumnLabel(7), "_decimal_short");
                    assertEquals(metadata.getColumnType(7), Types.DECIMAL);

                    assertEquals(metadata.getColumnLabel(8), "_decimal_long");
                    assertEquals(metadata.getColumnType(8), Types.DECIMAL);

                    assertEquals(metadata.getColumnLabel(9), "_hll");
                    assertEquals(metadata.getColumnType(9), Types.JAVA_OBJECT);

                    assertEquals(metadata.getColumnLabel(10), "_char");
                    assertEquals(metadata.getColumnType(10), Types.CHAR);

                    assertTrue(rs.next());

                    assertEquals(rs.getObject(1), 123);
                    assertEquals(rs.getObject("_integer"), 123);
                    assertEquals(rs.getInt(1), 123);
                    assertEquals(rs.getInt("_integer"), 123);
                    assertEquals(rs.getLong(1), 123L);
                    assertEquals(rs.getLong("_integer"), 123L);

                    assertEquals(rs.getObject(2), 12300000000L);
                    assertEquals(rs.getObject("_bigint"), 12300000000L);
                    assertEquals(rs.getLong(2), 12300000000L);
                    assertEquals(rs.getLong("_bigint"), 12300000000L);

                    assertEquals(rs.getObject(3), "foo");
                    assertEquals(rs.getObject("_varchar"), "foo");
                    assertEquals(rs.getString(3), "foo");
                    assertEquals(rs.getString("_varchar"), "foo");

                    assertEquals(rs.getObject(4), 0.1);
                    assertEquals(rs.getObject("_double"), 0.1);
                    assertEquals(rs.getDouble(4), 0.1);
                    assertEquals(rs.getDouble("_double"), 0.1);

                    assertEquals(rs.getObject(5), true);
                    assertEquals(rs.getObject("_boolean"), true);
                    assertEquals(rs.getBoolean(5), true);
                    assertEquals(rs.getBoolean("_boolean"), true);
                    assertEquals(rs.getByte("_boolean"), 1);
                    assertEquals(rs.getShort("_boolean"), 1);
                    assertEquals(rs.getInt("_boolean"), 1);
                    assertEquals(rs.getLong("_boolean"), 1L);
                    assertEquals(rs.getFloat("_boolean"), 1.0f);
                    assertEquals(rs.getDouble("_boolean"), 1.0);

                    assertEquals(rs.getObject(6), "hello".getBytes(UTF_8));
                    assertEquals(rs.getObject("_varbinary"), "hello".getBytes(UTF_8));
                    assertEquals(rs.getBytes(6), "hello".getBytes(UTF_8));
                    assertEquals(rs.getBytes("_varbinary"), "hello".getBytes(UTF_8));

                    assertEquals(rs.getObject(7), new BigDecimal("1234567890.1234567"));
                    assertEquals(rs.getObject("_decimal_short"), new BigDecimal("1234567890.1234567"));
                    assertEquals(rs.getBigDecimal(7), new BigDecimal("1234567890.1234567"));
                    assertEquals(rs.getBigDecimal("_decimal_short"), new BigDecimal("1234567890.1234567"));
                    assertEquals(rs.getBigDecimal(7, 1), new BigDecimal("1234567890.1"));
                    assertEquals(rs.getBigDecimal("_decimal_short", 1), new BigDecimal("1234567890.1"));

                    assertEquals(rs.getObject(8), new BigDecimal(".12345678901234567890123456789012345678"));
                    assertEquals(rs.getObject("_decimal_long"), new BigDecimal(".12345678901234567890123456789012345678"));
                    assertEquals(rs.getBigDecimal(8), new BigDecimal(".12345678901234567890123456789012345678"));
                    assertEquals(rs.getBigDecimal("_decimal_long"), new BigDecimal(".12345678901234567890123456789012345678"));
                    assertEquals(rs.getBigDecimal(8, 6), new BigDecimal(".123457"));
                    assertEquals(rs.getBigDecimal("_decimal_long", 6), new BigDecimal(".123457"));

                    assertInstanceOf(rs.getObject(9), byte[].class);
                    assertInstanceOf(rs.getObject("_hll"), byte[].class);
                    assertInstanceOf(rs.getBytes(9), byte[].class);
                    assertInstanceOf(rs.getBytes("_hll"), byte[].class);

                    assertEquals(rs.getObject(10), "foo  ");
                    assertEquals(rs.getObject("_char"), "foo  ");
                    assertEquals(rs.getString(10), "foo  ");
                    assertEquals(rs.getString("_char"), "foo  ");

                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test
    public void testTypes()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet rs = statement.executeQuery("SELECT " +
                        "  TIME '3:04:05' as a" +
                        ", TIME '6:07:08 +06:17' as b" +
                        ", TIME '9:10:11 +02:00' as c" +
                        ", TIMESTAMP '2001-02-03 3:04:05' as d" +
                        ", TIMESTAMP '2004-05-06 6:07:08 +06:17' as e" +
                        ", TIMESTAMP '2007-08-09 9:10:11 Europe/Berlin' as f" +
                        ", DATE '2013-03-22' as g" +
                        ", INTERVAL '123-11' YEAR TO MONTH as h" +
                        ", INTERVAL '11 22:33:44.555' DAY TO SECOND as i" +
                        ", REAL '123.45' as j" +
                        ", REAL 'Infinity' as k" +
                        "")) {
                    assertTrue(rs.next());

                    assertEquals(rs.getTime(1), new Time(new DateTime(1970, 1, 1, 3, 4, 5).getMillis()));
                    assertEquals(rs.getTime(1, ASIA_ORAL_CALENDAR), new Time(new DateTime(1970, 1, 1, 3, 4, 5, ASIA_ORAL_ZONE).getMillis()));
                    assertEquals(rs.getObject(1), new Time(new DateTime(1970, 1, 1, 3, 4, 5).getMillis()));
                    assertEquals(rs.getTime("a"), new Time(new DateTime(1970, 1, 1, 3, 4, 5).getMillis()));
                    assertEquals(rs.getTime("a", ASIA_ORAL_CALENDAR), new Time(new DateTime(1970, 1, 1, 3, 4, 5, ASIA_ORAL_ZONE).getMillis()));
                    assertEquals(rs.getObject("a"), new Time(new DateTime(1970, 1, 1, 3, 4, 5).getMillis()));

                    assertEquals(rs.getTime(2), new Time(new DateTime(1970, 1, 1, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));
                    assertEquals(rs.getTime(2, ASIA_ORAL_CALENDAR), new Time(new DateTime(1970, 1, 1, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));
                    assertEquals(rs.getObject(2), new Time(new DateTime(1970, 1, 1, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));
                    assertEquals(rs.getTime("b"), new Time(new DateTime(1970, 1, 1, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));
                    assertEquals(rs.getTime("b", ASIA_ORAL_CALENDAR), new Time(new DateTime(1970, 1, 1, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));
                    assertEquals(rs.getObject("b"), new Time(new DateTime(1970, 1, 1, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));

                    assertEquals(rs.getTime(3), new Time(new DateTime(1970, 1, 1, 9, 10, 11, DateTimeZone.forOffsetHoursMinutes(2, 0)).getMillis()));
                    assertEquals(rs.getTime(3, ASIA_ORAL_CALENDAR), new Time(new DateTime(1970, 1, 1, 9, 10, 11, DateTimeZone.forOffsetHoursMinutes(2, 0)).getMillis()));
                    assertEquals(rs.getObject(3), new Time(new DateTime(1970, 1, 1, 9, 10, 11, DateTimeZone.forOffsetHoursMinutes(2, 0)).getMillis()));
                    assertEquals(rs.getTime("c"), new Time(new DateTime(1970, 1, 1, 9, 10, 11, DateTimeZone.forOffsetHoursMinutes(2, 0)).getMillis()));
                    assertEquals(rs.getTime("c", ASIA_ORAL_CALENDAR), new Time(new DateTime(1970, 1, 1, 9, 10, 11, DateTimeZone.forOffsetHoursMinutes(2, 0)).getMillis()));
                    assertEquals(rs.getObject("c"), new Time(new DateTime(1970, 1, 1, 9, 10, 11, DateTimeZone.forOffsetHoursMinutes(2, 0)).getMillis()));

                    assertEquals(rs.getTimestamp(4), new Timestamp(new DateTime(2001, 2, 3, 3, 4, 5).getMillis()));
                    assertEquals(rs.getTimestamp(4, ASIA_ORAL_CALENDAR), new Timestamp(new DateTime(2001, 2, 3, 3, 4, 5, ASIA_ORAL_ZONE).getMillis()));
                    assertEquals(rs.getObject(4), new Timestamp(new DateTime(2001, 2, 3, 3, 4, 5).getMillis()));
                    assertEquals(rs.getTimestamp("d"), new Timestamp(new DateTime(2001, 2, 3, 3, 4, 5).getMillis()));
                    assertEquals(rs.getTimestamp("d", ASIA_ORAL_CALENDAR), new Timestamp(new DateTime(2001, 2, 3, 3, 4, 5, ASIA_ORAL_ZONE).getMillis()));
                    assertEquals(rs.getObject("d"), new Timestamp(new DateTime(2001, 2, 3, 3, 4, 5).getMillis()));

                    assertEquals(rs.getTimestamp(5), new Timestamp(new DateTime(2004, 5, 6, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));
                    assertEquals(rs.getTimestamp(5, ASIA_ORAL_CALENDAR), new Timestamp(new DateTime(2004, 5, 6, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));
                    assertEquals(rs.getObject(5), new Timestamp(new DateTime(2004, 5, 6, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));
                    assertEquals(rs.getObject(5, ZonedDateTime.class), ZonedDateTime.of(2004, 5, 6, 6, 7, 8, 0, ZoneOffset.ofHoursMinutes(6, 17)));
                    assertEquals(rs.getTimestamp("e"), new Timestamp(new DateTime(2004, 5, 6, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));
                    assertEquals(rs.getTimestamp("e", ASIA_ORAL_CALENDAR), new Timestamp(new DateTime(2004, 5, 6, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));
                    assertEquals(rs.getObject("e"), new Timestamp(new DateTime(2004, 5, 6, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));
                    assertEquals(rs.getObject("e", ZonedDateTime.class), ZonedDateTime.of(2004, 5, 6, 6, 7, 8, 0, ZoneOffset.ofHoursMinutes(6, 17)));

                    assertEquals(rs.getTimestamp(6), new Timestamp(new DateTime(2007, 8, 9, 9, 10, 11, DateTimeZone.forID("Europe/Berlin")).getMillis()));
                    assertEquals(rs.getTimestamp(6, ASIA_ORAL_CALENDAR), new Timestamp(new DateTime(2007, 8, 9, 9, 10, 11, DateTimeZone.forID("Europe/Berlin")).getMillis()));
                    assertEquals(rs.getObject(6), new Timestamp(new DateTime(2007, 8, 9, 9, 10, 11, DateTimeZone.forID("Europe/Berlin")).getMillis()));
                    assertEquals(rs.getObject(6, ZonedDateTime.class), ZonedDateTime.of(2007, 8, 9, 9, 10, 11, 0, ZoneId.of("Europe/Berlin")));
                    assertEquals(rs.getTimestamp("f"), new Timestamp(new DateTime(2007, 8, 9, 9, 10, 11, DateTimeZone.forID("Europe/Berlin")).getMillis()));
                    assertEquals(rs.getTimestamp("f", ASIA_ORAL_CALENDAR), new Timestamp(new DateTime(2007, 8, 9, 9, 10, 11, DateTimeZone.forID("Europe/Berlin")).getMillis()));
                    assertEquals(rs.getObject("f"), new Timestamp(new DateTime(2007, 8, 9, 9, 10, 11, DateTimeZone.forID("Europe/Berlin")).getMillis()));
                    assertEquals(rs.getObject("f", ZonedDateTime.class), ZonedDateTime.of(2007, 8, 9, 9, 10, 11, 0, ZoneId.of("Europe/Berlin")));

                    assertEquals(rs.getDate(7), new Date(new DateTime(2013, 3, 22, 0, 0).getMillis()));
                    assertEquals(rs.getDate(7, ASIA_ORAL_CALENDAR), new Date(new DateTime(2013, 3, 22, 0, 0, ASIA_ORAL_ZONE).getMillis()));
                    assertEquals(rs.getObject(7), new Date(new DateTime(2013, 3, 22, 0, 0).getMillis()));
                    assertEquals(rs.getDate("g"), new Date(new DateTime(2013, 3, 22, 0, 0).getMillis()));
                    assertEquals(rs.getDate("g", ASIA_ORAL_CALENDAR), new Date(new DateTime(2013, 3, 22, 0, 0, ASIA_ORAL_ZONE).getMillis()));
                    assertEquals(rs.getObject("g"), new Date(new DateTime(2013, 3, 22, 0, 0).getMillis()));

                    assertEquals(rs.getObject(8), new TrinoIntervalYearMonth(123, 11));
                    assertEquals(rs.getObject("h"), new TrinoIntervalYearMonth(123, 11));
                    assertEquals(rs.getObject(9), new TrinoIntervalDayTime(11, 22, 33, 44, 555));
                    assertEquals(rs.getObject("i"), new TrinoIntervalDayTime(11, 22, 33, 44, 555));

                    assertEquals(rs.getFloat(10), 123.45f);
                    assertEquals(rs.getObject(10), 123.45f);
                    assertEquals(rs.getFloat("j"), 123.45f);
                    assertEquals(rs.getObject("j"), 123.45f);

                    assertEquals(rs.getFloat(11), POSITIVE_INFINITY);
                    assertEquals(rs.getObject(11), POSITIVE_INFINITY);
                    assertEquals(rs.getFloat("k"), POSITIVE_INFINITY);
                    assertEquals(rs.getObject("k"), POSITIVE_INFINITY);
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test
    public void testGetDriverVersion()
            throws Exception
    {
        Driver driver = DriverManager.getDriver("jdbc:trino:");
        assertThat(driver.getMajorVersion()).isGreaterThan(350);
        assertThat(driver.getMinorVersion()).isEqualTo(0);

        try (Connection connection = createConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            assertEquals(metaData.getDriverName(), "Trino JDBC Driver");
            assertThat(metaData.getDriverVersion()).startsWith(String.valueOf(driver.getMajorVersion()));
            assertEquals(metaData.getDriverMajorVersion(), driver.getMajorVersion());
            assertEquals(metaData.getDriverMinorVersion(), driver.getMinorVersion());
        }
    }

    @Test
    public void testNullUrl()
            throws Exception
    {
        Driver driver = DriverManager.getDriver("jdbc:trino:");

        assertThatThrownBy(() -> driver.connect(null, null))
                .isInstanceOf(SQLException.class)
                .hasMessage("URL is null");

        assertThatThrownBy(() -> driver.acceptsURL(null))
                .isInstanceOf(SQLException.class)
                .hasMessage("URL is null");
    }

    @Test
    public void testDriverPropertyInfoEmpty()
            throws Exception
    {
        Driver driver = DriverManager.getDriver("jdbc:trino:");

        Properties properties = new Properties();
        DriverPropertyInfo[] infos = driver.getPropertyInfo(jdbcUrl(), properties);

        assertThat(infos)
                .extracting(TestTrinoDriver::driverPropertyInfoToString)
                .contains("{name=user, required=false}")
                .contains("{name=password, required=false}")
                .contains("{name=accessToken, required=false}")
                .contains("{name=SSL, required=false, choices=[true, false]}");

        assertThat(infos).extracting(x -> x.name)
                .doesNotContain("SSLVerification", "SSLTrustStorePath");
    }

    @Test
    public void testDriverPropertyInfoSslEnabled()
            throws Exception
    {
        Driver driver = DriverManager.getDriver("jdbc:trino:");

        Properties properties = new Properties();
        properties.setProperty("user", "test");
        properties.setProperty("SSL", "true");
        DriverPropertyInfo[] infos = driver.getPropertyInfo(jdbcUrl(), properties);

        assertThat(infos)
                .extracting(TestTrinoDriver::driverPropertyInfoToString)
                .contains("{name=user, value=test, required=false}")
                .contains("{name=SSL, value=true, required=false, choices=[true, false]}")
                .contains("{name=SSLVerification, required=false, choices=[FULL, CA, NONE]}")
                .contains("{name=SSLTrustStorePath, required=false}");
    }

    private static String driverPropertyInfoToString(DriverPropertyInfo info)
    {
        return toStringHelper("")
                .add("name", info.name)
                .add("value", info.value)
                .add("description", info.description)
                .add("required", info.required)
                .add("choices", info.choices)
                .omitNullValues()
                .toString();
    }

    @Test
    public void testExecuteWithQuery()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (Statement statement = connection.createStatement()) {
                assertTrue(statement.execute("SELECT 123 x, 'foo' y, CAST(NULL AS bigint) z"));
                ResultSet rs = statement.getResultSet();

                assertEquals(statement.getUpdateCount(), -1);
                assertEquals(statement.getLargeUpdateCount(), -1);
                assertTrue(rs.next());

                assertEquals(rs.getLong(1), 123);
                assertFalse(rs.wasNull());
                assertEquals(rs.getLong("x"), 123);
                assertFalse(rs.wasNull());

                assertEquals(rs.getLong(3), 0);
                assertTrue(rs.wasNull());
                assertEquals(rs.getLong("z"), 0);
                assertTrue(rs.wasNull());
                assertNull(rs.getObject("z"));
                assertTrue(rs.wasNull());

                assertEquals(rs.getString(2), "foo");
                assertFalse(rs.wasNull());
                assertEquals(rs.getString("y"), "foo");
                assertFalse(rs.wasNull());

                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testExecuteUpdateWithInsert()
            throws Exception
    {
        try (Connection connection = createConnection("blackhole", "blackhole")) {
            try (Statement statement = connection.createStatement()) {
                assertEquals(statement.executeUpdate("INSERT INTO test_table VALUES (1), (2)"), 2);
                assertNull(statement.getResultSet());
                assertEquals(statement.getUpdateCount(), 2);
                assertEquals(statement.getLargeUpdateCount(), 2);
            }
        }
    }

    @Test
    public void testExecuteUpdateWithCreateTable()
            throws Exception
    {
        try (Connection connection = createConnection("blackhole", "blackhole")) {
            try (Statement statement = connection.createStatement()) {
                assertEquals(statement.executeUpdate("CREATE TABLE test_execute_create (x bigint)"), 0);
                assertNull(statement.getResultSet());
                assertEquals(statement.getUpdateCount(), 0);
                assertEquals(statement.getLargeUpdateCount(), 0);
            }
        }
    }

    @Test
    public void testExecuteUpdateWithQuery()
            throws Exception
    {
        try (Connection connection = createConnection("blackhole", "blackhole")) {
            try (Statement statement = connection.createStatement()) {
                String sql = "SELECT 123 x, 'foo' y, CAST(NULL AS bigint) z";
                assertThatThrownBy(() -> statement.executeUpdate(sql))
                        .isInstanceOf(SQLException.class)
                        .hasMessage("SQL is not an update statement: %s", sql);
            }
        }
    }

    @Test
    public void testExecuteQueryWithInsert()
            throws Exception
    {
        try (Connection connection = createConnection("blackhole", "blackhole")) {
            try (Statement statement = connection.createStatement()) {
                String sql = "INSERT INTO test_table VALUES (1)";
                assertThatThrownBy(() -> statement.executeQuery(sql))
                        .isInstanceOf(SQLException.class)
                        .hasMessage("SQL statement is not a query: %s", sql);
            }
        }
    }

    @Test
    public void testStatementReuse()
            throws Exception
    {
        try (Connection connection = createConnection("blackhole", "blackhole")) {
            try (Statement statement = connection.createStatement()) {
                // update statement
                assertFalse(statement.execute("INSERT INTO test_table VALUES (1), (2)"));
                assertNull(statement.getResultSet());
                assertEquals(statement.getUpdateCount(), 2);
                assertEquals(statement.getLargeUpdateCount(), 2);

                // query statement
                assertTrue(statement.execute("SELECT 123 x, 'foo' y, CAST(NULL AS bigint) z"));
                ResultSet resultSet = statement.getResultSet();
                assertNotNull(resultSet);
                assertEquals(statement.getUpdateCount(), -1);
                assertEquals(statement.getLargeUpdateCount(), -1);
                resultSet.close();

                // update statement
                assertFalse(statement.execute("INSERT INTO test_table VALUES (1), (2), (3)"));
                assertNull(statement.getResultSet());
                assertEquals(statement.getUpdateCount(), 3);
                assertEquals(statement.getLargeUpdateCount(), 3);
            }
        }
    }

    @Test
    public void testGetUpdateCount()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (Statement statement = connection.createStatement()) {
                assertTrue(statement.execute("SELECT 123 x, 'foo' y"));
                assertEquals(statement.getUpdateCount(), -1);
                assertEquals(statement.getLargeUpdateCount(), -1);
            }
        }
    }

    @Test
    public void testResultSetClose()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (Statement statement = connection.createStatement()) {
                assertTrue(statement.execute("SELECT 123 x, 'foo' y"));
                ResultSet result = statement.getResultSet();
                assertFalse(result.isClosed());
                result.close();
                assertTrue(result.isClosed());
            }
        }
    }

    @Test
    public void testGetResultSet()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (Statement statement = connection.createStatement()) {
                assertTrue(statement.execute("SELECT 123 x, 'foo' y"));
                ResultSet result = statement.getResultSet();
                assertNotNull(result);
                assertFalse(result.isClosed());
                statement.getMoreResults();
                assertTrue(result.isClosed());

                assertTrue(statement.execute("SELECT 123 x, 'foo' y"));
                result = statement.getResultSet();
                assertNotNull(result);
                assertFalse(result.isClosed());

                assertTrue(statement.execute("SELECT 123 x, 'foo' y"));
                assertFalse(statement.getMoreResults(Statement.CLOSE_CURRENT_RESULT));
            }
        }
    }

    @Test(expectedExceptions = SQLFeatureNotSupportedException.class, expectedExceptionsMessageRegExp = "Multiple open results not supported")
    public void testGetMoreResultsException()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (Statement statement = connection.createStatement()) {
                assertTrue(statement.execute("SELECT 123 x, 'foo' y"));
                statement.getMoreResults(Statement.KEEP_CURRENT_RESULT);
            }
        }
    }

    @Test
    public void testGetMoreResultsClearsUpdateCount()
            throws Exception
    {
        try (Connection connection = createConnection("blackhole", "default")) {
            try (TrinoStatement statement = connection.createStatement().unwrap(TrinoStatement.class)) {
                assertFalse(statement.execute("CREATE TABLE test_more_results_clears_update_count (id bigint)"));
                assertEquals(statement.getUpdateCount(), 0);
                assertEquals(statement.getUpdateType(), "CREATE TABLE");
                assertFalse(statement.getMoreResults());
                assertEquals(statement.getUpdateCount(), -1);
                assertNull(statement.getUpdateType());
            }
            finally {
                try (Statement statement = connection.createStatement()) {
                    statement.execute("DROP TABLE test_more_results_clears_update_count");
                }
            }
        }
    }

    @Test
    public void testSetTimeZoneId()
            throws Exception
    {
        TimeZoneKey defaultZoneKey = TimeZoneKey.getTimeZoneKey(TimeZone.getDefault().getID());
        DateTimeZone defaultZone = DateTimeZone.forTimeZone(TimeZone.getDefault());
        String sql = "SELECT current_timezone() zone, TIMESTAMP '2001-02-03 3:04:05' ts";

        try (Connection connection = createConnection()) {
            try (Statement statement = connection.createStatement();
                    ResultSet rs = statement.executeQuery(sql)) {
                assertTrue(rs.next());
                assertEquals(rs.getString("zone"), defaultZoneKey.getId());
                assertEquals(rs.getTimestamp("ts"), new Timestamp(new DateTime(2001, 2, 3, 3, 4, 5, defaultZone).getMillis()));
            }

            connection.unwrap(TrinoConnection.class).setTimeZoneId("UTC");
            try (Statement statement = connection.createStatement();
                    ResultSet rs = statement.executeQuery(sql)) {
                assertTrue(rs.next());
                assertEquals(rs.getString("zone"), "UTC");
                // setting the session timezone has no effect on the interpretation of timestamps in the JDBC driver
                assertEquals(rs.getTimestamp("ts"), new Timestamp(new DateTime(2001, 2, 3, 3, 4, 5, defaultZone).getMillis()));
            }
        }
    }

    @Test
    public void testConnectionStringWithCatalogAndSchema()
            throws Exception
    {
        String prefix = format("jdbc:trino://%s", server.getAddress());

        Connection connection;
        connection = DriverManager.getConnection(prefix + "/a/b/", "test", null);
        assertEquals(connection.getCatalog(), "a");
        assertEquals(connection.getSchema(), "b");

        connection = DriverManager.getConnection(prefix + "/a/b", "test", null);
        assertEquals(connection.getCatalog(), "a");
        assertEquals(connection.getSchema(), "b");

        connection = DriverManager.getConnection(prefix + "/a/", "test", null);
        assertEquals(connection.getCatalog(), "a");
        assertNull(connection.getSchema());

        connection = DriverManager.getConnection(prefix + "/a", "test", null);
        assertEquals(connection.getCatalog(), "a");
        assertNull(connection.getSchema());

        connection = DriverManager.getConnection(prefix + "/", "test", null);
        assertNull(connection.getCatalog());
        assertNull(connection.getSchema());

        connection = DriverManager.getConnection(prefix, "test", null);
        assertNull(connection.getCatalog());
        assertNull(connection.getSchema());
    }

    @Test
    public void testConnectionWithCatalogAndSchema()
            throws Exception
    {
        try (Connection connection = createConnection(TEST_CATALOG, "information_schema")) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet rs = statement.executeQuery("" +
                        "SELECT table_catalog, table_schema " +
                        "FROM tables " +
                        "WHERE table_schema = 'information_schema' " +
                        "  AND table_name = 'tables'")) {
                    ResultSetMetaData metadata = rs.getMetaData();
                    assertEquals(metadata.getColumnCount(), 2);
                    assertEquals(metadata.getColumnLabel(1), "table_catalog");
                    assertEquals(metadata.getColumnLabel(2), "table_schema");
                    assertTrue(rs.next());
                    assertEquals(rs.getString("table_catalog"), TEST_CATALOG);
                }
            }
        }
    }

    @Test
    public void testConnectionWithCatalog()
            throws Exception
    {
        try (Connection connection = createConnection(TEST_CATALOG)) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet rs = statement.executeQuery("" +
                        "SELECT table_catalog, table_schema " +
                        "FROM information_schema.tables " +
                        "WHERE table_schema = 'information_schema' " +
                        "  AND table_name = 'tables'")) {
                    ResultSetMetaData metadata = rs.getMetaData();
                    assertEquals(metadata.getColumnCount(), 2);
                    assertEquals(metadata.getColumnLabel(1), "table_catalog");
                    assertEquals(metadata.getColumnLabel(2), "table_schema");
                    assertTrue(rs.next());
                    assertEquals(rs.getString("table_catalog"), TEST_CATALOG);
                }
            }
        }
    }

    @Test
    public void testConnectionResourceHandling()
            throws Exception
    {
        List<Connection> connections = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            Connection connection = createConnection();
            connections.add(connection);

            try (Statement statement = connection.createStatement();
                    ResultSet rs = statement.executeQuery("SELECT 123")) {
                assertTrue(rs.next());
            }
        }

        for (Connection connection : connections) {
            connection.close();
        }
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = ".* does not exist")
    public void testBadQuery()
            throws Exception
    {
        try (Connection connection = createConnection(TEST_CATALOG, "tiny")) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet ignored = statement.executeQuery("SELECT * FROM bad_table")) {
                    fail("expected exception");
                }
            }
        }
    }

    @Test
    public void testNullConnectProperties()
            throws Exception
    {
        DriverManager.getDriver("jdbc:trino:").connect(jdbcUrl(), null);
    }

    @Test
    public void testPropertyAllowed()
            throws Exception
    {
        assertThatThrownBy(() -> DriverManager.getConnection(jdbcUrl(),
                toProperties(ImmutableMap.<String, String>builder()
                        .put("user", "test")
                        .put("KerberosPrincipal", "test")
                        .buildOrThrow())))
                .isInstanceOf(SQLException.class)
                .hasMessage("Connection property 'KerberosPrincipal' is not allowed");

        assertThat(DriverManager.getConnection(jdbcUrl(),
                toProperties(ImmutableMap.<String, String>builder()
                        .put("user", "test")
                        .put("KerberosRemoteServiceName", "example.com")
                        .put("KerberosPrincipal", "test")
                        .put("SSL", "true")
                        .buildOrThrow())))
                .isNotNull();

        assertThatThrownBy(() -> DriverManager.getConnection(jdbcUrl(),
                toProperties(ImmutableMap.<String, String>builder()
                        .put("user", "test")
                        .put("SSLVerification", "NONE")
                        .buildOrThrow())))
                .isInstanceOf(SQLException.class)
                .hasMessage("Connection property 'SSLVerification' is not allowed");

        assertThat(DriverManager.getConnection(jdbcUrl(),
                toProperties(ImmutableMap.<String, String>builder()
                        .put("user", "test")
                        .put("SSL", "true")
                        .put("SSLVerification", "NONE")
                        .buildOrThrow())))
                .isNotNull();

        assertThat(DriverManager.getConnection(jdbcUrl(),
                toProperties(ImmutableMap.<String, String>builder()
                        .put("user", "test")
                        .put("SSL", "true")
                        .put("SSLVerification", "NONE")
                        .put("assumeLiteralNamesInMetadataCallsForNonConformingClients", "true")
                        .buildOrThrow())))
                .isNotNull();

        assertThat(DriverManager.getConnection(jdbcUrl(),
                toProperties(ImmutableMap.<String, String>builder()
                        .put("user", "test")
                        .put("SSL", "true")
                        .put("SSLVerification", "NONE")
                        .put("assumeLiteralUnderscoreInMetadataCallsForNonConformingClients", "true")
                        .buildOrThrow())))
                .isNotNull();
    }

    @Test
    public void testSetRole()
            throws Exception
    {
        try (TrinoConnection connection = createConnection(TEST_CATALOG, "tiny").unwrap(TrinoConnection.class)) {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("SET ROLE ALL");
            }
            assertEquals(connection.getRoles(), ImmutableMap.of("system", new ClientSelectedRole(ClientSelectedRole.Type.ALL, Optional.empty())));
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("SET ROLE NONE");
            }
            assertEquals(connection.getRoles(), ImmutableMap.of("system", new ClientSelectedRole(ClientSelectedRole.Type.NONE, Optional.empty())));

            try (Statement statement = connection.createStatement()) {
                // There is no way to create system roles right now
                assertThatThrownBy(() -> statement.executeUpdate("SET ROLE bar"))
                        .hasMessageMatching(".* Role 'bar' does not exist");

                // Only hive connector supports roles
                assertThatThrownBy(() -> statement.executeUpdate("SET ROLE ALL IN " + TEST_CATALOG))
                        .hasMessageMatching(".* Catalog '.*' does not support role management");
                assertThatThrownBy(() -> statement.executeUpdate("SET ROLE NONE IN " + TEST_CATALOG))
                        .hasMessageMatching(".* Catalog '.*' does not support role management");
                assertThatThrownBy(() -> statement.executeUpdate("SET ROLE bar IN " + TEST_CATALOG))
                        .hasMessageMatching(".* Catalog '.*' does not support role management");
            }
        }
    }

    @Test(timeOut = 10000)
    public void testQueryCancelByInterrupt()
            throws Exception
    {
        CountDownLatch queryStarted = new CountDownLatch(1);
        CountDownLatch queryFinished = new CountDownLatch(1);
        AtomicReference<String> queryId = new AtomicReference<>();
        AtomicReference<Throwable> queryFailure = new AtomicReference<>();

        Future<?> queryFuture = executorService.submit(() -> {
            try (Connection connection = createConnection("blackhole", "blackhole");
                    Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery("SELECT * FROM slow_test_table")) {
                queryId.set(resultSet.unwrap(TrinoResultSet.class).getQueryId());
                queryStarted.countDown();
                try {
                    resultSet.next();
                }
                catch (SQLException t) {
                    queryFailure.set(t);
                }
                finally {
                    queryFinished.countDown();
                }
            }
            return null;
        });

        // start query and make sure it is not finished
        assertTrue(queryStarted.await(10, SECONDS));
        assertNotNull(queryId.get());
        assertFalse(getQueryState(queryId.get()).isDone());

        // interrupt JDBC thread that is waiting for query results
        queryFuture.cancel(true);

        // make sure the query was aborted
        assertTrue(queryFinished.await(10, SECONDS));
        assertThat(queryFailure.get())
                .isInstanceOf(SQLException.class)
                .hasMessage("Interrupted");
        assertEquals(getQueryState(queryId.get()), FAILED);
    }

    @Test(timeOut = 10000)
    public void testQueryCancelExplicit()
            throws Exception
    {
        CountDownLatch queryStarted = new CountDownLatch(1);
        CountDownLatch queryFinished = new CountDownLatch(1);
        AtomicReference<String> queryId = new AtomicReference<>();
        AtomicReference<Throwable> queryFailure = new AtomicReference<>();

        try (Connection connection = createConnection("blackhole", "blackhole");
                Statement statement = connection.createStatement()) {
            // execute the slow query on another thread
            executorService.execute(() -> {
                try (ResultSet resultSet = statement.executeQuery("SELECT * FROM slow_test_table")) {
                    queryId.set(resultSet.unwrap(TrinoResultSet.class).getQueryId());
                    queryStarted.countDown();
                    resultSet.next();
                }
                catch (SQLException t) {
                    queryFailure.set(t);
                }
                finally {
                    queryFinished.countDown();
                }
            });

            // start query and make sure it is not finished
            queryStarted.await(10, SECONDS);
            assertNotNull(queryId.get());
            assertFalse(getQueryState(queryId.get()).isDone());

            // cancel the query from this test thread
            statement.cancel();

            // make sure the query was aborted
            queryFinished.await(10, SECONDS);
            assertNotNull(queryFailure.get());
            assertEquals(getQueryState(queryId.get()), FAILED);
        }
    }

    @Test(timeOut = 10000)
    public void testUpdateCancelExplicit()
            throws Exception
    {
        CountDownLatch queryFinished = new CountDownLatch(1);
        AtomicReference<Throwable> queryFailure = new AtomicReference<>();
        String queryUuid = "/* " + UUID.randomUUID().toString() + " */";

        try (Connection connection = createConnection("blackhole", "blackhole");
                Statement statement = connection.createStatement()) {
            // execute the slow update on another thread
            executorService.execute(() -> {
                try {
                    statement.executeUpdate("CREATE TABLE test_cancel_create AS SELECT * FROM slow_test_table " + queryUuid);
                }
                catch (SQLException t) {
                    queryFailure.set(t);
                }
                finally {
                    queryFinished.countDown();
                }
            });

            // start query and make sure it is not finished
            while (true) {
                Optional<QueryState> state = findQueryState(queryUuid);
                if (state.isPresent()) {
                    assertFalse(state.get().isDone());
                    break;
                }
                MILLISECONDS.sleep(50);
            }

            // cancel the query from this test thread
            statement.cancel();

            // make sure the query was aborted
            queryFinished.await(10, SECONDS);
            assertNotNull(queryFailure.get());
            assertEquals(findQueryState(queryUuid), Optional.of(FAILED));
        }
    }

    @Test(timeOut = 10000)
    public void testQueryTimeout()
            throws Exception
    {
        try (Connection connection = createConnection("blackhole", "blackhole");
                Statement statement = connection.createStatement()) {
            statement.executeUpdate("CREATE TABLE test_query_timeout (key BIGINT) " +
                    "WITH (" +
                    "   split_count = 1, " +
                    "   pages_per_split = 1, " +
                    "   rows_per_page = 1, " +
                    "   page_processing_delay = '1m'" +
                    ")");
        }

        CountDownLatch queryFinished = new CountDownLatch(1);
        AtomicReference<Throwable> queryFailure = new AtomicReference<>();

        executorService.submit(() -> {
            try (Connection connection = createConnection("blackhole", "blackhole");
                    Statement statement = connection.createStatement()) {
                statement.setQueryTimeout(1);
                try (ResultSet resultSet = statement.executeQuery("SELECT * FROM test_query_timeout")) {
                    try {
                        resultSet.next();
                    }
                    catch (SQLException t) {
                        queryFailure.set(t);
                    }
                    finally {
                        queryFinished.countDown();
                    }
                }
            }
            return null;
        });

        // make sure the query timed out
        queryFinished.await();
        assertNotNull(queryFailure.get());
        assertContains(queryFailure.get().getMessage(), "Query exceeded maximum time limit of 1.00s");

        try (Connection connection = createConnection("blackhole", "blackhole");
                Statement statement = connection.createStatement()) {
            statement.executeUpdate("DROP TABLE test_query_timeout");
        }
    }

    @Test(timeOut = 10000)
    public void testQueryPartialCancel()
            throws Exception
    {
        try (Connection connection = createConnection("blackhole", "blackhole");
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SELECT count(*) FROM slow_test_table")) {
            statement.unwrap(TrinoStatement.class).partialCancel();
            assertTrue(resultSet.next());
            assertEquals(resultSet.getLong(1), 0);
        }
    }

    @Test(timeOut = 10000)
    public void testUpdatePartialCancel()
            throws Exception
    {
        CountDownLatch queryRunning = new CountDownLatch(1);

        try (Connection connection = createConnection("blackhole", "blackhole");
                Statement statement = connection.createStatement()) {
            // execute the slow update on another thread
            Future<Integer> future = executorService.submit(() ->
                    statement.executeUpdate("INSERT INTO test_table SELECT count(*) x FROM slow_test_table"));

            // wait for query to start running
            statement.unwrap(TrinoStatement.class).setProgressMonitor(stats -> {
                if (stats.getState().equals(RUNNING.toString())) {
                    queryRunning.countDown();
                }
            });
            queryRunning.await(10, SECONDS);

            // perform partial cancel from this test thread
            statement.unwrap(TrinoStatement.class).partialCancel();

            // make sure query completes
            assertEquals(future.get(10, SECONDS), (Integer) 1);
        }
    }

    private QueryState getQueryState(String queryId)
            throws SQLException
    {
        String sql = format("SELECT state FROM system.runtime.queries WHERE query_id = '%s'", queryId);
        try (Connection connection = createConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            assertTrue(resultSet.next(), "Query was not found");
            return QueryState.valueOf(requireNonNull(resultSet.getString(1)));
        }
    }

    private Optional<QueryState> findQueryState(String text)
            throws SQLException
    {
        String sql = format("SELECT state FROM system.runtime.queries WHERE regexp_like(query, '%s$') /* */", Pattern.quote(text));
        try (Connection connection = createConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            if (!resultSet.next()) {
                return Optional.empty();
            }
            QueryState state = QueryState.valueOf(requireNonNull(resultSet.getString(1)));
            assertFalse(resultSet.next(), "Found multiple queries");
            return Optional.of(state);
        }
    }

    private String jdbcUrl()
    {
        return format("jdbc:trino://%s", server.getAddress());
    }

    private Connection createConnection()
            throws SQLException
    {
        String url = format("jdbc:trino://%s", server.getAddress());
        return DriverManager.getConnection(url, "test", null);
    }

    private Connection createConnection(String catalog)
            throws SQLException
    {
        String url = format("jdbc:trino://%s/%s", server.getAddress(), catalog);
        return DriverManager.getConnection(url, "test", null);
    }

    private Connection createConnection(String catalog, String schema)
            throws SQLException
    {
        String url = format("jdbc:trino://%s/%s/%s", server.getAddress(), catalog, schema);
        return DriverManager.getConnection(url, "test", null);
    }

    private static Properties toProperties(Map<String, String> map)
    {
        Properties properties = new Properties();
        map.forEach(properties::setProperty);
        return properties;
    }
}
