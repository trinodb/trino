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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logging;
import io.trino.client.ClientSelectedRole;
import io.trino.client.DnsResolver;
import io.trino.execution.QueryState;
import io.trino.plugin.blackhole.BlackHolePlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.type.TimeZoneKey;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.UnknownHostException;
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
import java.time.zone.ZoneRulesException;
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
import static org.assertj.core.api.Fail.fail;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestTrinoDriver
{
    private static final DateTimeZone ASIA_ORAL_ZONE = DateTimeZone.forID("Asia/Oral");
    private static final GregorianCalendar ASIA_ORAL_CALENDAR = new GregorianCalendar(TimeZone.getTimeZone(ZoneId.of(ASIA_ORAL_ZONE.getID())));
    private static final String TEST_CATALOG = "test_catalog";

    private TestingTrinoServer server;
    private ExecutorService executorService;

    @BeforeAll
    public void setup()
            throws Exception
    {
        Logging.initialize();
        server = TestingTrinoServer.create();
        server.installPlugin(new TpchPlugin());
        server.createCatalog(TEST_CATALOG, "tpch");
        server.installPlugin(new BlackHolePlugin());
        server.createCatalog("blackhole", "blackhole");
        setupTestTables();
        executorService = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
    }

    private void setupTestTables()
            throws SQLException
    {
        try (Connection connection = createConnection("blackhole", "blackhole");
                Statement statement = connection.createStatement()) {
            assertThat(statement.executeUpdate("CREATE SCHEMA blackhole.blackhole")).isEqualTo(0);
            assertThat(statement.executeUpdate("CREATE TABLE test_table (x bigint)")).isEqualTo(0);

            assertThat(statement.executeUpdate("CREATE TABLE slow_test_table (x bigint) " +
                    "WITH (" +
                    "   split_count = 1, " +
                    "   pages_per_split = 1, " +
                    "   rows_per_page = 1, " +
                    "   page_processing_delay = '1m'" +
                    ")")).isEqualTo(0);
        }
    }

    @AfterAll
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

                    assertThat(metadata.getColumnCount()).isEqualTo(10);

                    assertThat(metadata.getColumnLabel(1)).isEqualTo("_integer");
                    assertThat(metadata.getColumnType(1)).isEqualTo(Types.INTEGER);

                    assertThat(metadata.getColumnLabel(2)).isEqualTo("_bigint");
                    assertThat(metadata.getColumnType(2)).isEqualTo(Types.BIGINT);

                    assertThat(metadata.getColumnLabel(3)).isEqualTo("_varchar");
                    assertThat(metadata.getColumnType(3)).isEqualTo(Types.VARCHAR);

                    assertThat(metadata.getColumnLabel(4)).isEqualTo("_double");
                    assertThat(metadata.getColumnType(4)).isEqualTo(Types.DOUBLE);

                    assertThat(metadata.getColumnLabel(5)).isEqualTo("_boolean");
                    assertThat(metadata.getColumnType(5)).isEqualTo(Types.BOOLEAN);

                    assertThat(metadata.getColumnLabel(6)).isEqualTo("_varbinary");
                    assertThat(metadata.getColumnType(6)).isEqualTo(Types.VARBINARY);

                    assertThat(metadata.getColumnLabel(7)).isEqualTo("_decimal_short");
                    assertThat(metadata.getColumnType(7)).isEqualTo(Types.DECIMAL);

                    assertThat(metadata.getColumnLabel(8)).isEqualTo("_decimal_long");
                    assertThat(metadata.getColumnType(8)).isEqualTo(Types.DECIMAL);

                    assertThat(metadata.getColumnLabel(9)).isEqualTo("_hll");
                    assertThat(metadata.getColumnType(9)).isEqualTo(Types.JAVA_OBJECT);

                    assertThat(metadata.getColumnLabel(10)).isEqualTo("_char");
                    assertThat(metadata.getColumnType(10)).isEqualTo(Types.CHAR);

                    assertThat(rs.next()).isTrue();

                    assertThat(rs.getObject(1)).isEqualTo(123);
                    assertThat(rs.getObject("_integer")).isEqualTo(123);
                    assertThat(rs.getInt(1)).isEqualTo(123);
                    assertThat(rs.getInt("_integer")).isEqualTo(123);
                    assertThat(rs.getLong(1)).isEqualTo(123L);
                    assertThat(rs.getLong("_integer")).isEqualTo(123L);

                    assertThat(rs.getObject(2)).isEqualTo(12300000000L);
                    assertThat(rs.getObject("_bigint")).isEqualTo(12300000000L);
                    assertThat(rs.getLong(2)).isEqualTo(12300000000L);
                    assertThat(rs.getLong("_bigint")).isEqualTo(12300000000L);

                    assertThat(rs.getObject(3)).isEqualTo("foo");
                    assertThat(rs.getObject("_varchar")).isEqualTo("foo");
                    assertThat(rs.getString(3)).isEqualTo("foo");
                    assertThat(rs.getString("_varchar")).isEqualTo("foo");

                    assertThat(rs.getObject(4)).isEqualTo(0.1);
                    assertThat(rs.getObject("_double")).isEqualTo(0.1);
                    assertThat(rs.getDouble(4)).isEqualTo(0.1);
                    assertThat(rs.getDouble("_double")).isEqualTo(0.1);

                    assertThat(rs.getObject(5)).isEqualTo(true);
                    assertThat(rs.getObject("_boolean")).isEqualTo(true);
                    assertThat(rs.getBoolean(5)).isEqualTo(true);
                    assertThat(rs.getBoolean("_boolean")).isEqualTo(true);
                    assertThat(rs.getByte("_boolean")).isEqualTo((byte) 1);
                    assertThat(rs.getShort("_boolean")).isEqualTo((short) 1);
                    assertThat(rs.getInt("_boolean")).isEqualTo(1);
                    assertThat(rs.getLong("_boolean")).isEqualTo(1L);
                    assertThat(rs.getFloat("_boolean")).isEqualTo(1.0f);
                    assertThat(rs.getDouble("_boolean")).isEqualTo(1.0);

                    assertThat(rs.getObject(6)).isEqualTo("hello".getBytes(UTF_8));
                    assertThat(rs.getObject("_varbinary")).isEqualTo("hello".getBytes(UTF_8));
                    assertThat(rs.getBytes(6)).isEqualTo("hello".getBytes(UTF_8));
                    assertThat(rs.getBytes("_varbinary")).isEqualTo("hello".getBytes(UTF_8));

                    assertThat(rs.getObject(7)).isEqualTo(new BigDecimal("1234567890.1234567"));
                    assertThat(rs.getObject("_decimal_short")).isEqualTo(new BigDecimal("1234567890.1234567"));
                    assertThat(rs.getBigDecimal(7)).isEqualTo(new BigDecimal("1234567890.1234567"));
                    assertThat(rs.getBigDecimal("_decimal_short")).isEqualTo(new BigDecimal("1234567890.1234567"));
                    assertThat(rs.getBigDecimal(7, 1)).isEqualTo(new BigDecimal("1234567890.1"));
                    assertThat(rs.getBigDecimal("_decimal_short", 1)).isEqualTo(new BigDecimal("1234567890.1"));

                    assertThat(rs.getObject(8)).isEqualTo(new BigDecimal(".12345678901234567890123456789012345678"));
                    assertThat(rs.getObject("_decimal_long")).isEqualTo(new BigDecimal(".12345678901234567890123456789012345678"));
                    assertThat(rs.getBigDecimal(8)).isEqualTo(new BigDecimal(".12345678901234567890123456789012345678"));
                    assertThat(rs.getBigDecimal("_decimal_long")).isEqualTo(new BigDecimal(".12345678901234567890123456789012345678"));
                    assertThat(rs.getBigDecimal(8, 6)).isEqualTo(new BigDecimal(".123457"));
                    assertThat(rs.getBigDecimal("_decimal_long", 6)).isEqualTo(new BigDecimal(".123457"));

                    assertInstanceOf(rs.getObject(9), byte[].class);
                    assertInstanceOf(rs.getObject("_hll"), byte[].class);
                    assertInstanceOf(rs.getBytes(9), byte[].class);
                    assertInstanceOf(rs.getBytes("_hll"), byte[].class);

                    assertThat(rs.getObject(10)).isEqualTo("foo  ");
                    assertThat(rs.getObject("_char")).isEqualTo("foo  ");
                    assertThat(rs.getString(10)).isEqualTo("foo  ");
                    assertThat(rs.getString("_char")).isEqualTo("foo  ");

                    assertThat(rs.next()).isFalse();
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
                    assertThat(rs.next()).isTrue();

                    assertThat(rs.getTime(1)).isEqualTo(new Time(new DateTime(1970, 1, 1, 3, 4, 5).getMillis()));
                    assertThat(rs.getTime(1, ASIA_ORAL_CALENDAR)).isEqualTo(new Time(new DateTime(1970, 1, 1, 3, 4, 5, ASIA_ORAL_ZONE).getMillis()));
                    assertThat(rs.getObject(1)).isEqualTo(new Time(new DateTime(1970, 1, 1, 3, 4, 5).getMillis()));
                    assertThat(rs.getTime("a")).isEqualTo(new Time(new DateTime(1970, 1, 1, 3, 4, 5).getMillis()));
                    assertThat(rs.getTime("a", ASIA_ORAL_CALENDAR)).isEqualTo(new Time(new DateTime(1970, 1, 1, 3, 4, 5, ASIA_ORAL_ZONE).getMillis()));
                    assertThat(rs.getObject("a")).isEqualTo(new Time(new DateTime(1970, 1, 1, 3, 4, 5).getMillis()));

                    assertThat(rs.getTime(2)).isEqualTo(new Time(new DateTime(1970, 1, 1, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));
                    assertThat(rs.getTime(2, ASIA_ORAL_CALENDAR)).isEqualTo(new Time(new DateTime(1970, 1, 1, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));
                    assertThat(rs.getObject(2)).isEqualTo(new Time(new DateTime(1970, 1, 1, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));
                    assertThat(rs.getTime("b")).isEqualTo(new Time(new DateTime(1970, 1, 1, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));
                    assertThat(rs.getTime("b", ASIA_ORAL_CALENDAR)).isEqualTo(new Time(new DateTime(1970, 1, 1, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));
                    assertThat(rs.getObject("b")).isEqualTo(new Time(new DateTime(1970, 1, 1, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));

                    assertThat(rs.getTime(3)).isEqualTo(new Time(new DateTime(1970, 1, 1, 9, 10, 11, DateTimeZone.forOffsetHoursMinutes(2, 0)).getMillis()));
                    assertThat(rs.getTime(3, ASIA_ORAL_CALENDAR)).isEqualTo(new Time(new DateTime(1970, 1, 1, 9, 10, 11, DateTimeZone.forOffsetHoursMinutes(2, 0)).getMillis()));
                    assertThat(rs.getObject(3)).isEqualTo(new Time(new DateTime(1970, 1, 1, 9, 10, 11, DateTimeZone.forOffsetHoursMinutes(2, 0)).getMillis()));
                    assertThat(rs.getTime("c")).isEqualTo(new Time(new DateTime(1970, 1, 1, 9, 10, 11, DateTimeZone.forOffsetHoursMinutes(2, 0)).getMillis()));
                    assertThat(rs.getTime("c", ASIA_ORAL_CALENDAR)).isEqualTo(new Time(new DateTime(1970, 1, 1, 9, 10, 11, DateTimeZone.forOffsetHoursMinutes(2, 0)).getMillis()));
                    assertThat(rs.getObject("c")).isEqualTo(new Time(new DateTime(1970, 1, 1, 9, 10, 11, DateTimeZone.forOffsetHoursMinutes(2, 0)).getMillis()));

                    assertThat(rs.getTimestamp(4)).isEqualTo(new Timestamp(new DateTime(2001, 2, 3, 3, 4, 5).getMillis()));
                    assertThat(rs.getTimestamp(4, ASIA_ORAL_CALENDAR)).isEqualTo(new Timestamp(new DateTime(2001, 2, 3, 3, 4, 5, ASIA_ORAL_ZONE).getMillis()));
                    assertThat(rs.getObject(4)).isEqualTo(new Timestamp(new DateTime(2001, 2, 3, 3, 4, 5).getMillis()));
                    assertThat(rs.getTimestamp("d")).isEqualTo(new Timestamp(new DateTime(2001, 2, 3, 3, 4, 5).getMillis()));
                    assertThat(rs.getTimestamp("d", ASIA_ORAL_CALENDAR)).isEqualTo(new Timestamp(new DateTime(2001, 2, 3, 3, 4, 5, ASIA_ORAL_ZONE).getMillis()));
                    assertThat(rs.getObject("d")).isEqualTo(new Timestamp(new DateTime(2001, 2, 3, 3, 4, 5).getMillis()));

                    assertThat(rs.getTimestamp(5)).isEqualTo(new Timestamp(new DateTime(2004, 5, 6, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));
                    assertThat(rs.getTimestamp(5, ASIA_ORAL_CALENDAR)).isEqualTo(new Timestamp(new DateTime(2004, 5, 6, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));
                    assertThat(rs.getObject(5)).isEqualTo(new Timestamp(new DateTime(2004, 5, 6, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));
                    assertThat(rs.getObject(5, ZonedDateTime.class)).isEqualTo(ZonedDateTime.of(2004, 5, 6, 6, 7, 8, 0, ZoneOffset.ofHoursMinutes(6, 17)));
                    assertThat(rs.getTimestamp("e")).isEqualTo(new Timestamp(new DateTime(2004, 5, 6, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));
                    assertThat(rs.getTimestamp("e", ASIA_ORAL_CALENDAR)).isEqualTo(new Timestamp(new DateTime(2004, 5, 6, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));
                    assertThat(rs.getObject("e")).isEqualTo(new Timestamp(new DateTime(2004, 5, 6, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));
                    assertThat(rs.getObject("e", ZonedDateTime.class)).isEqualTo(ZonedDateTime.of(2004, 5, 6, 6, 7, 8, 0, ZoneOffset.ofHoursMinutes(6, 17)));

                    assertThat(rs.getTimestamp(6)).isEqualTo(new Timestamp(new DateTime(2007, 8, 9, 9, 10, 11, DateTimeZone.forID("Europe/Berlin")).getMillis()));
                    assertThat(rs.getTimestamp(6, ASIA_ORAL_CALENDAR)).isEqualTo(new Timestamp(new DateTime(2007, 8, 9, 9, 10, 11, DateTimeZone.forID("Europe/Berlin")).getMillis()));
                    assertThat(rs.getObject(6)).isEqualTo(new Timestamp(new DateTime(2007, 8, 9, 9, 10, 11, DateTimeZone.forID("Europe/Berlin")).getMillis()));
                    assertThat(rs.getObject(6, ZonedDateTime.class)).isEqualTo(ZonedDateTime.of(2007, 8, 9, 9, 10, 11, 0, ZoneId.of("Europe/Berlin")));
                    assertThat(rs.getTimestamp("f")).isEqualTo(new Timestamp(new DateTime(2007, 8, 9, 9, 10, 11, DateTimeZone.forID("Europe/Berlin")).getMillis()));
                    assertThat(rs.getTimestamp("f", ASIA_ORAL_CALENDAR)).isEqualTo(new Timestamp(new DateTime(2007, 8, 9, 9, 10, 11, DateTimeZone.forID("Europe/Berlin")).getMillis()));
                    assertThat(rs.getObject("f")).isEqualTo(new Timestamp(new DateTime(2007, 8, 9, 9, 10, 11, DateTimeZone.forID("Europe/Berlin")).getMillis()));
                    assertThat(rs.getObject("f", ZonedDateTime.class)).isEqualTo(ZonedDateTime.of(2007, 8, 9, 9, 10, 11, 0, ZoneId.of("Europe/Berlin")));

                    assertThat(rs.getDate(7)).isEqualTo(new Date(new DateTime(2013, 3, 22, 0, 0).getMillis()));
                    assertThat(rs.getDate(7, ASIA_ORAL_CALENDAR)).isEqualTo(new Date(new DateTime(2013, 3, 22, 0, 0, ASIA_ORAL_ZONE).getMillis()));
                    assertThat(rs.getObject(7)).isEqualTo(new Date(new DateTime(2013, 3, 22, 0, 0).getMillis()));
                    assertThat(rs.getDate("g")).isEqualTo(new Date(new DateTime(2013, 3, 22, 0, 0).getMillis()));
                    assertThat(rs.getDate("g", ASIA_ORAL_CALENDAR)).isEqualTo(new Date(new DateTime(2013, 3, 22, 0, 0, ASIA_ORAL_ZONE).getMillis()));
                    assertThat(rs.getObject("g")).isEqualTo(new Date(new DateTime(2013, 3, 22, 0, 0).getMillis()));

                    assertThat(rs.getObject(8)).isEqualTo(new TrinoIntervalYearMonth(123, 11));
                    assertThat(rs.getObject("h")).isEqualTo(new TrinoIntervalYearMonth(123, 11));
                    assertThat(rs.getObject(9)).isEqualTo(new TrinoIntervalDayTime(11, 22, 33, 44, 555));
                    assertThat(rs.getObject("i")).isEqualTo(new TrinoIntervalDayTime(11, 22, 33, 44, 555));

                    assertThat(rs.getFloat(10)).isEqualTo(123.45f);
                    assertThat(rs.getObject(10)).isEqualTo(123.45f);
                    assertThat(rs.getFloat("j")).isEqualTo(123.45f);
                    assertThat(rs.getObject("j")).isEqualTo(123.45f);

                    assertThat(rs.getFloat(11)).isEqualTo(POSITIVE_INFINITY);
                    assertThat(rs.getObject(11)).isEqualTo(POSITIVE_INFINITY);
                    assertThat(rs.getFloat("k")).isEqualTo(POSITIVE_INFINITY);
                    assertThat(rs.getObject("k")).isEqualTo(POSITIVE_INFINITY);
                    assertThat(rs.next()).isFalse();
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
            assertThat(metaData.getDriverName()).isEqualTo("Trino JDBC Driver");
            assertThat(metaData.getDriverVersion()).startsWith(String.valueOf(driver.getMajorVersion()));
            assertThat(metaData.getDriverMajorVersion()).isEqualTo(driver.getMajorVersion());
            assertThat(metaData.getDriverMinorVersion()).isEqualTo(driver.getMinorVersion());
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
                assertThat(statement.execute("SELECT 123 x, 'foo' y, CAST(NULL AS bigint) z")).isTrue();
                ResultSet rs = statement.getResultSet();

                assertThat(statement.getUpdateCount()).isEqualTo(-1);
                assertThat(statement.getLargeUpdateCount()).isEqualTo(-1);
                assertThat(rs.next()).isTrue();

                assertThat(rs.getLong(1)).isEqualTo(123);
                assertThat(rs.wasNull()).isFalse();
                assertThat(rs.getLong("x")).isEqualTo(123);
                assertThat(rs.wasNull()).isFalse();

                assertThat(rs.getLong(3)).isEqualTo(0);
                assertThat(rs.wasNull()).isTrue();
                assertThat(rs.getLong("z")).isEqualTo(0);
                assertThat(rs.wasNull()).isTrue();
                assertThat(rs.getObject("z")).isNull();
                assertThat(rs.wasNull()).isTrue();

                assertThat(rs.getString(2)).isEqualTo("foo");
                assertThat(rs.wasNull()).isFalse();
                assertThat(rs.getString("y")).isEqualTo("foo");
                assertThat(rs.wasNull()).isFalse();

                assertThat(rs.next()).isFalse();
            }
        }
    }

    @Test
    public void testExecuteUpdateWithInsert()
            throws Exception
    {
        try (Connection connection = createConnection("blackhole", "blackhole")) {
            try (Statement statement = connection.createStatement()) {
                assertThat(statement.executeUpdate("INSERT INTO test_table VALUES (1), (2)")).isEqualTo(2);
                assertThat(statement.getResultSet()).isNull();
                assertThat(statement.getUpdateCount()).isEqualTo(2);
                assertThat(statement.getLargeUpdateCount()).isEqualTo(2);
            }
        }
    }

    @Test
    public void testExecuteUpdateWithCreateTable()
            throws Exception
    {
        try (Connection connection = createConnection("blackhole", "blackhole")) {
            try (Statement statement = connection.createStatement()) {
                assertThat(statement.executeUpdate("CREATE TABLE test_execute_create (x bigint)")).isEqualTo(0);
                assertThat(statement.getResultSet()).isNull();
                assertThat(statement.getUpdateCount()).isEqualTo(0);
                assertThat(statement.getLargeUpdateCount()).isEqualTo(0);
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
                assertThat(statement.execute("INSERT INTO test_table VALUES (1), (2)")).isFalse();
                assertThat(statement.getResultSet()).isNull();
                assertThat(statement.getUpdateCount()).isEqualTo(2);
                assertThat(statement.getLargeUpdateCount()).isEqualTo(2);

                // query statement
                assertThat(statement.execute("SELECT 123 x, 'foo' y, CAST(NULL AS bigint) z")).isTrue();
                ResultSet resultSet = statement.getResultSet();
                assertThat(resultSet).isNotNull();
                assertThat(statement.getUpdateCount()).isEqualTo(-1);
                assertThat(statement.getLargeUpdateCount()).isEqualTo(-1);
                resultSet.close();

                // update statement
                assertThat(statement.execute("INSERT INTO test_table VALUES (1), (2), (3)")).isFalse();
                assertThat(statement.getResultSet()).isNull();
                assertThat(statement.getUpdateCount()).isEqualTo(3);
                assertThat(statement.getLargeUpdateCount()).isEqualTo(3);
            }
        }
    }

    @Test
    public void testGetUpdateCount()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (Statement statement = connection.createStatement()) {
                assertThat(statement.execute("SELECT 123 x, 'foo' y")).isTrue();
                assertThat(statement.getUpdateCount()).isEqualTo(-1);
                assertThat(statement.getLargeUpdateCount()).isEqualTo(-1);
            }
        }
    }

    @Test
    public void testResultSetClose()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (Statement statement = connection.createStatement()) {
                assertThat(statement.execute("SELECT 123 x, 'foo' y")).isTrue();
                ResultSet result = statement.getResultSet();
                assertThat(result.isClosed()).isFalse();
                result.close();
                assertThat(result.isClosed()).isTrue();
            }
        }
    }

    @Test
    public void testGetResultSet()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (Statement statement = connection.createStatement()) {
                assertThat(statement.execute("SELECT 123 x, 'foo' y")).isTrue();
                ResultSet result = statement.getResultSet();
                assertThat(result).isNotNull();
                assertThat(result.isClosed()).isFalse();
                statement.getMoreResults();
                assertThat(result.isClosed()).isTrue();

                assertThat(statement.execute("SELECT 123 x, 'foo' y")).isTrue();
                result = statement.getResultSet();
                assertThat(result).isNotNull();
                assertThat(result.isClosed()).isFalse();

                assertThat(statement.execute("SELECT 123 x, 'foo' y")).isTrue();
                assertThat(statement.getMoreResults(Statement.CLOSE_CURRENT_RESULT)).isFalse();
            }
        }
    }

    @Test
    public void testGetMoreResultsException()
            throws Exception
    {
        assertThatThrownBy(() -> {
            try (Connection connection = createConnection()) {
                try (Statement statement = connection.createStatement()) {
                    assertThat(statement.execute("SELECT 123 x, 'foo' y")).isTrue();
                    statement.getMoreResults(Statement.KEEP_CURRENT_RESULT);
                }
            }
        })
                .isInstanceOf(SQLFeatureNotSupportedException.class)
                .hasMessage("Multiple open results not supported");
    }

    @Test
    public void testGetMoreResultsClearsUpdateCount()
            throws Exception
    {
        try (Connection connection = createConnection("blackhole", "default")) {
            try (TrinoStatement statement = connection.createStatement().unwrap(TrinoStatement.class)) {
                assertThat(statement.execute("CREATE TABLE test_more_results_clears_update_count (id bigint)")).isFalse();
                assertThat(statement.getUpdateCount()).isEqualTo(0);
                assertThat(statement.getUpdateType()).isEqualTo("CREATE TABLE");
                assertThat(statement.getMoreResults()).isFalse();
                assertThat(statement.getUpdateCount()).isEqualTo(-1);
                assertThat(statement.getUpdateType()).isNull();
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
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("zone")).isEqualTo(defaultZoneKey.getId());
                assertThat(rs.getTimestamp("ts")).isEqualTo(new Timestamp(new DateTime(2001, 2, 3, 3, 4, 5, defaultZone).getMillis()));
            }

            connection.unwrap(TrinoConnection.class).setTimeZoneId("UTC");
            try (Statement statement = connection.createStatement();
                    ResultSet rs = statement.executeQuery(sql)) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("zone")).isEqualTo("UTC");
                // setting the session timezone has no effect on the interpretation of timestamps in the JDBC driver
                assertThat(rs.getTimestamp("ts")).isEqualTo(new Timestamp(new DateTime(2001, 2, 3, 3, 4, 5, defaultZone).getMillis()));
            }
        }

        try (Connection connection = createConnectionWithParameter("timezone=Asia/Kolkata")) {
            try (Statement statement = connection.createStatement();
                    ResultSet rs = statement.executeQuery(sql)) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("zone")).isEqualTo("Asia/Kolkata");
                // setting the session timezone has no effect on the interpretation of timestamps in the JDBC driver
                assertThat(rs.getTimestamp("ts")).isEqualTo(new Timestamp(new DateTime(2001, 2, 3, 3, 4, 5, defaultZone).getMillis()));
            }
        }

        try (Connection connection = createConnectionWithParameter("timezone=UTC+05:30")) {
            try (Statement statement = connection.createStatement();
                    ResultSet rs = statement.executeQuery(sql)) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("zone")).isEqualTo("+05:30");
                // setting the session timezone has no effect on the interpretation of timestamps in the JDBC driver
                assertThat(rs.getTimestamp("ts")).isEqualTo(new Timestamp(new DateTime(2001, 2, 3, 3, 4, 5, defaultZone).getMillis()));
            }
        }

        assertThatThrownBy(() -> createConnectionWithParameter("timezone=Asia/NOT_FOUND"))
                .isInstanceOf(SQLException.class)
                .hasMessage("Connection property timezone value is invalid: Asia/NOT_FOUND")
                .hasRootCauseInstanceOf(ZoneRulesException.class)
                .hasRootCauseMessage("Unknown time-zone ID: Asia/NOT_FOUND");
    }

    @Test
    public void testConnectionStringWithCatalogAndSchema()
            throws Exception
    {
        String prefix = format("jdbc:trino://%s", server.getAddress());

        Connection connection;
        connection = DriverManager.getConnection(prefix + "/a/b/", "test", null);
        assertThat(connection.getCatalog()).isEqualTo("a");
        assertThat(connection.getSchema()).isEqualTo("b");

        connection = DriverManager.getConnection(prefix + "/a/b", "test", null);
        assertThat(connection.getCatalog()).isEqualTo("a");
        assertThat(connection.getSchema()).isEqualTo("b");

        connection = DriverManager.getConnection(prefix + "/a/", "test", null);
        assertThat(connection.getCatalog()).isEqualTo("a");
        assertThat(connection.getSchema()).isNull();

        connection = DriverManager.getConnection(prefix + "/a", "test", null);
        assertThat(connection.getCatalog()).isEqualTo("a");
        assertThat(connection.getSchema()).isNull();

        connection = DriverManager.getConnection(prefix + "/", "test", null);
        assertThat(connection.getCatalog()).isNull();
        assertThat(connection.getSchema()).isNull();

        connection = DriverManager.getConnection(prefix, "test", null);
        assertThat(connection.getCatalog()).isNull();
        assertThat(connection.getSchema()).isNull();
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
                    assertThat(metadata.getColumnCount()).isEqualTo(2);
                    assertThat(metadata.getColumnLabel(1)).isEqualTo("table_catalog");
                    assertThat(metadata.getColumnLabel(2)).isEqualTo("table_schema");
                    assertThat(rs.next()).isTrue();
                    assertThat(rs.getString("table_catalog")).isEqualTo(TEST_CATALOG);
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
                    assertThat(metadata.getColumnCount()).isEqualTo(2);
                    assertThat(metadata.getColumnLabel(1)).isEqualTo("table_catalog");
                    assertThat(metadata.getColumnLabel(2)).isEqualTo("table_schema");
                    assertThat(rs.next()).isTrue();
                    assertThat(rs.getString("table_catalog")).isEqualTo(TEST_CATALOG);
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
                assertThat(rs.next()).isTrue();
            }
        }

        for (Connection connection : connections) {
            connection.close();
        }
    }

    @Test
    public void testBadQuery()
            throws Exception
    {
        assertThatThrownBy(() -> {
            try (Connection connection = createConnection(TEST_CATALOG, "tiny")) {
                try (Statement statement = connection.createStatement()) {
                    try (ResultSet ignored = statement.executeQuery("SELECT * FROM bad_table")) {
                        fail("expected exception");
                    }
                }
            }
        })
                .isInstanceOf(SQLException.class)
                .hasMessageMatching(".* does not exist");
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
                .hasMessage("Connection property KerberosPrincipal requires KerberosRemoteServiceName to be set");

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
                .hasMessage("Connection property SSLVerification requires TLS/SSL to be enabled");

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
            assertThat(connection.getRoles()).isEqualTo(ImmutableMap.of("system", new ClientSelectedRole(ClientSelectedRole.Type.ALL, Optional.empty())));
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("SET ROLE NONE");
            }
            assertThat(connection.getRoles()).isEqualTo(ImmutableMap.of("system", new ClientSelectedRole(ClientSelectedRole.Type.NONE, Optional.empty())));

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

    @Test
    @Timeout(10)
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
        assertThat(queryStarted.await(10, SECONDS)).isTrue();
        assertThat(queryId.get()).isNotNull();
        assertThat(getQueryState(queryId.get()).isDone()).isFalse();

        // interrupt JDBC thread that is waiting for query results
        queryFuture.cancel(true);

        // make sure the query was aborted
        assertThat(queryFinished.await(10, SECONDS)).isTrue();
        assertThat(queryFailure.get())
                .isInstanceOf(SQLException.class)
                .hasMessage("Interrupted");
        assertThat(getQueryState(queryId.get())).isEqualTo(FAILED);
    }

    @Test
    @Timeout(10)
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
            assertThat(queryId.get()).isNotNull();
            assertThat(getQueryState(queryId.get()).isDone()).isFalse();

            // cancel the query from this test thread
            statement.cancel();

            // make sure the query was aborted
            queryFinished.await(10, SECONDS);
            assertThat(queryFailure.get()).isNotNull();
            assertThat(getQueryState(queryId.get())).isEqualTo(FAILED);
        }
    }

    @Test
    @Timeout(10)
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
                    assertThat(state.get().isDone()).isFalse();
                    break;
                }
                MILLISECONDS.sleep(50);
            }

            // cancel the query from this test thread
            statement.cancel();

            // make sure the query was aborted
            queryFinished.await(10, SECONDS);
            assertThat(queryFailure.get()).isNotNull();
            assertThat(findQueryState(queryUuid)).isEqualTo(Optional.of(FAILED));
        }
    }

    @Test
    @Timeout(10)
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
        assertThat(queryFailure.get()).isNotNull();
        assertContains(queryFailure.get().getMessage(), "Query exceeded maximum time limit of 1.00s");

        try (Connection connection = createConnection("blackhole", "blackhole");
                Statement statement = connection.createStatement()) {
            statement.executeUpdate("DROP TABLE test_query_timeout");
        }
    }

    @Test
    @Timeout(10)
    public void testQueryPartialCancel()
            throws Exception
    {
        try (Connection connection = createConnection("blackhole", "blackhole");
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SELECT count(*) FROM slow_test_table")) {
            statement.unwrap(TrinoStatement.class).partialCancel();
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getLong(1)).isEqualTo(0);
        }
    }

    @Test
    @Timeout(10)
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
            assertThat(future.get(10, SECONDS)).isEqualTo((Integer) 1);
        }
    }

    @Test
    public void testCustomDnsResolver()
            throws Exception
    {
        Properties properties = new Properties();
        properties.setProperty("dnsResolver", TestingDnsResolver.class.getName());
        properties.setProperty("dnsResolverContext", server.getAddress().getHost());
        properties.setProperty("user", "test");

        String url = "jdbc:trino://mycustomaddress:" + server.getAddress().getPort();
        try (Connection connection = DriverManager.getConnection(url, properties)) {
            try (Statement statement = connection.createStatement()) {
                assertThat(statement.execute("SELECT 1")).isTrue();
            }
        }
    }

    @Test
    @Timeout(10)
    public void testResetSessionAuthorization()
            throws Exception
    {
        try (TrinoConnection connection = createConnection("blackhole", "blackhole").unwrap(TrinoConnection.class);
                Statement statement = connection.createStatement()) {
            assertThat(connection.getAuthorizationUser()).isEqualTo(null);
            assertThat(getCurrentUser(connection)).isEqualTo("test");
            statement.execute("SET SESSION AUTHORIZATION john");
            assertThat(connection.getAuthorizationUser()).isEqualTo("john");
            assertThat(getCurrentUser(connection)).isEqualTo("john");
            statement.execute("SET SESSION AUTHORIZATION bob");
            assertThat(connection.getAuthorizationUser()).isEqualTo("bob");
            assertThat(getCurrentUser(connection)).isEqualTo("bob");
            statement.execute("RESET SESSION AUTHORIZATION");
            assertThat(connection.getAuthorizationUser()).isEqualTo(null);
            assertThat(getCurrentUser(connection)).isEqualTo("test");
        }
    }

    @Test
    @Timeout(10)
    public void testSetRoleAfterSetSessionAuthorization()
            throws Exception
    {
        try (TrinoConnection connection = createConnection("blackhole", "blackhole").unwrap(TrinoConnection.class);
                Statement statement = connection.createStatement()) {
            statement.execute("SET SESSION AUTHORIZATION john");
            assertThat(connection.getAuthorizationUser()).isEqualTo("john");
            statement.execute("SET ROLE ALL");
            assertThat(connection.getRoles()).isEqualTo(ImmutableMap.of("system", new ClientSelectedRole(ClientSelectedRole.Type.ALL, Optional.empty())));
            statement.execute("SET SESSION AUTHORIZATION bob");
            assertThat(connection.getAuthorizationUser()).isEqualTo("bob");
            assertThat(connection.getRoles()).isEqualTo(ImmutableMap.of());
            statement.execute("SET ROLE NONE");
            assertThat(connection.getRoles()).isEqualTo(ImmutableMap.of("system", new ClientSelectedRole(ClientSelectedRole.Type.NONE, Optional.empty())));
            statement.execute("RESET SESSION AUTHORIZATION");
            assertThat(connection.getAuthorizationUser()).isEqualTo(null);
            assertThat(connection.getRoles()).isEqualTo(ImmutableMap.of());
        }
    }

    private QueryState getQueryState(String queryId)
            throws SQLException
    {
        String sql = format("SELECT state FROM system.runtime.queries WHERE query_id = '%s'", queryId);
        try (Connection connection = createConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            assertThat(resultSet.next())
                    .describedAs("Query was not found")
                    .isTrue();
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
            assertThat(resultSet.next())
                    .describedAs("Found multiple queries")
                    .isFalse();
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

    private Connection createConnectionWithParameter(String parameter)
            throws SQLException
    {
        String url = format("jdbc:trino://%s?%s", server.getAddress(), parameter);
        return DriverManager.getConnection(url, "test", null);
    }

    private static Properties toProperties(Map<String, String> map)
    {
        Properties properties = new Properties();
        map.forEach(properties::setProperty);
        return properties;
    }

    private static String getCurrentUser(Connection connection)
            throws SQLException
    {
        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SELECT current_user")) {
            while (rs.next()) {
                return rs.getString(1);
            }
        }

        throw new RuntimeException("Failed to get CURRENT_USER");
    }

    public static class TestingDnsResolver
            implements DnsResolver
    {
        private final String context;

        public TestingDnsResolver(String context)
        {
            this.context = requireNonNull(context, "context is null");
        }

        @Override
        public List<InetAddress> lookup(String hostname)
                throws UnknownHostException
        {
            if ("mycustomaddress".equals(hostname)) {
                return ImmutableList.of(InetAddress.getByName(context));
            }
            throw new UnknownHostException("Cannot resolve host: " + hostname);
        }
    }
}
