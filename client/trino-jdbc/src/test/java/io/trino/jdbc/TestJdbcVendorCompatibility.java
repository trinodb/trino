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
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.util.AutoCloseableCloser;
import oracle.jdbc.OracleType;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.TimeZone;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.sql.JDBCType.DATE;
import static java.sql.JDBCType.TIME;
import static java.sql.JDBCType.TIMESTAMP;
import static java.sql.JDBCType.TIMESTAMP_WITH_TIMEZONE;
import static java.sql.JDBCType.VARBINARY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestJdbcVendorCompatibility
{
    private static final String OTHER_TIMEZONE = "Asia/Kathmandu";

    private Logger log;
    private TestingTrinoServer server;
    private List<ReferenceDriver> referenceDrivers;

    private Connection connection;
    private Statement statement;

    @BeforeClass
    public void setupServer()
    {
        assertNotEquals(OTHER_TIMEZONE, TimeZone.getDefault().getID(), "We need a timezone different from the default JVM one");
        Logging.initialize();
        log = Logger.get(TestJdbcVendorCompatibility.class);
        server = TestingTrinoServer.create();

        // Capture resources as soon as they are allocated. Ensure all allocate resources are cleaned up even if e.g. the last one fails to start.
        referenceDrivers = new ArrayList<>();
        referenceDrivers.add(new PostgresqlReferenceDriver());
        referenceDrivers.add(new OracleReferenceDriver());
    }

    @AfterClass(alwaysRun = true)
    public void tearDownServer()
            throws Exception
    {
        try (AutoCloseableCloser closer = AutoCloseableCloser.create()) {
            if (referenceDrivers != null) {
                referenceDrivers.forEach(closer::register);
                referenceDrivers.clear();
            }
            if (server != null) {
                closer.register(server);
                server = null;
            }
            if (connection != null) {
                closer.register(connection);
                connection = null;
            }
            if (statement != null) {
                closer.register(statement);
                statement = null;
            }
        }
    }

    @SuppressWarnings("JDBCResourceOpenedButNotSafelyClosed")
    @BeforeMethod
    public void setUp()
            throws Exception
    {
        // recreate connection since tests modify connection state
        connection = DriverManager.getConnection("jdbc:trino://" + server.getAddress(), "test", null);
        statement = connection.createStatement();
        referenceDrivers.forEach(ReferenceDriver::setUp);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        if (statement != null) {
            statement.close();
            statement = null;
        }
        if (connection != null) {
            connection.close();
            connection = null;
        }
        for (ReferenceDriver driver : referenceDrivers) {
            try {
                driver.tearDown();
            }
            catch (Exception e) {
                log.warn(e, "Failed to close reference JDBC driver %s; continuing", driver);
            }
        }
    }

    // test behavior with UTC and time zones to the east and west
    @DataProvider
    public Object[][] timeZoneIds()
    {
        return new Object[][] {
                {Optional.empty()},
                {Optional.of("UTC")},
                {Optional.of("Europe/Warsaw")},
                {Optional.of("America/Denver")},
                {Optional.of(ZoneId.systemDefault().getId())}
        };
    }

    @Test
    public void testVarbinary()
            throws Exception
    {
        checkRepresentation(
                "X'12345678'",
                ImmutableList.of(
                        "bytea E'\\\\x12345678'", // PostgreSQL
                        "hextoraw('12345678')"), // Oracle
                VARBINARY,
                Optional.empty(),
                (rs, reference, column) -> {
                    assertThat(rs.getBytes(column)).isEqualTo(new byte[] {0x12, 0x34, 0x56, 0x78});
                    assertThat(rs.getBytes(column)).isEqualTo(reference.getBytes(column));
                    assertThat(rs.getObject(column)).isEqualTo(reference.getObject(column));

                    // Trino returns "0x<hex>"
                    // PostgreSQL returns "\x<hex>"
                    // Oracle returns "<hex>"
                    assertThat(rs.getString(column).replaceFirst("^0x", ""))
                            .isEqualTo(reference.getString(column).replaceFirst("^\\\\x", ""));
                });
    }

    @Test(dataProvider = "timeZoneIds")
    public void testDate(Optional<String> sessionTimezoneId)
            throws Exception
    {
        checkRepresentation("DATE '2018-02-13'", DATE, sessionTimezoneId, (rs, reference, column) -> {
            assertEquals(rs.getDate(column), reference.getDate(column));
            assertEquals(rs.getDate(column), Date.valueOf(LocalDate.of(2018, 2, 13)));

            // with calendar
            assertEquals(rs.getDate(column, getCalendar()), reference.getDate(column, getCalendar()));
            assertEquals(rs.getDate(column, getCalendar()), new Date(LocalDate.of(2018, 2, 13).atStartOfDay(getZoneId()).toInstant().toEpochMilli()));
        });
    }

    @Test(dataProvider = "timeZoneIds")
    public void testTimestamp(Optional<String> sessionTimezoneId)
            throws Exception
    {
        checkRepresentation("TIMESTAMP '2018-02-13 13:14:15.123'", TIMESTAMP, sessionTimezoneId, (rs, reference, column) -> {
            assertEquals(rs.getTimestamp(column), reference.getTimestamp(column));
            assertEquals(
                    rs.getTimestamp(column),
                    Timestamp.valueOf(LocalDateTime.of(2018, 2, 13, 13, 14, 15, 123_000_000)));

            // with calendar
            assertEquals(rs.getTimestamp(column, getCalendar()), reference.getTimestamp(column, getCalendar()));
            assertEquals(
                    rs.getTimestamp(column, getCalendar()),
                    new Timestamp(LocalDateTime.of(2018, 2, 13, 13, 14, 15, 123_000_000).atZone(getZoneId()).toInstant().toEpochMilli()));
        });
    }

    @Test(dataProvider = "timeZoneIds")
    public void testTimestampWithTimeZone(Optional<String> sessionTimezoneId)
            throws Exception
    {
        checkRepresentation(
                "TIMESTAMP '1970-01-01 00:00:00.000 +00:00'", // Trino
                ImmutableList.of(
                        "TIMESTAMP WITH TIME ZONE '1970-01-01 00:00:00.000 +00:00'", // PostgreSQL
                        "from_tz(TIMESTAMP '1970-01-01 00:00:00.000', '+00:00')"), // Oracle
                TIMESTAMP_WITH_TIMEZONE,
                sessionTimezoneId,
                (rs, reference, column) -> {
                    Timestamp timestampForPointInTime = Timestamp.from(Instant.EPOCH);

                    assertEquals(rs.getTimestamp(column).getTime(), reference.getTimestamp(column).getTime()); // point in time
                    assertEquals(rs.getTimestamp(column), reference.getTimestamp(column));
                    assertEquals(rs.getTimestamp(column), timestampForPointInTime);

                    // with calendar
                    assertEquals(rs.getTimestamp(column, getCalendar()).getTime(), reference.getTimestamp(column, getCalendar()).getTime()); // point in time
                    assertEquals(rs.getTimestamp(column, getCalendar()), reference.getTimestamp(column, getCalendar()));
                    assertEquals(rs.getTimestamp(column, getCalendar()), timestampForPointInTime);
                });

        checkRepresentation(
                "TIMESTAMP '2018-02-13 13:14:15.123 +03:15'", // Trino
                ImmutableList.of(
                        "TIMESTAMP WITH TIME ZONE '2018-02-13 13:14:15.123 +03:15'", // PostgreSQL
                        "from_tz(TIMESTAMP '2018-02-13 13:14:15.123', '+03:15')"), // Oracle
                TIMESTAMP_WITH_TIMEZONE,
                sessionTimezoneId,
                (rs, reference, column) -> {
                    Timestamp timestampForPointInTime = Timestamp.from(
                            ZonedDateTime.of(2018, 2, 13, 13, 14, 15, 123_000_000, ZoneOffset.ofHoursMinutes(3, 15))
                                    .toInstant());

                    assertEquals(rs.getTimestamp(column).getTime(), reference.getTimestamp(column).getTime()); // point in time
                    assertEquals(rs.getTimestamp(column), reference.getTimestamp(column));
                    assertEquals(rs.getTimestamp(column), timestampForPointInTime);

                    // with calendar
                    assertEquals(rs.getTimestamp(column, getCalendar()).getTime(), reference.getTimestamp(column, getCalendar()).getTime()); // point in time
                    assertEquals(rs.getTimestamp(column, getCalendar()), reference.getTimestamp(column, getCalendar()));
                    assertEquals(rs.getTimestamp(column, getCalendar()), timestampForPointInTime);
                });

        checkRepresentation(
                "TIMESTAMP '2018-02-13 13:14:15.123 Europe/Warsaw'", // Trino
                ImmutableList.of(
                        "TIMESTAMP WITH TIME ZONE '2018-02-13 13:14:15.123 Europe/Warsaw'", // PostgreSQL
                        "from_tz(TIMESTAMP '2018-02-13 13:14:15.123', 'Europe/Warsaw')"), // Oracle
                TIMESTAMP_WITH_TIMEZONE,
                sessionTimezoneId,
                (rs, reference, column) -> {
                    Timestamp timestampForPointInTime = Timestamp.from(
                            ZonedDateTime.of(2018, 2, 13, 13, 14, 15, 123_000_000, ZoneId.of("Europe/Warsaw"))
                                    .toInstant());

                    assertEquals(rs.getTimestamp(column).getTime(), reference.getTimestamp(column).getTime()); // point in time
                    assertEquals(rs.getTimestamp(column), reference.getTimestamp(column));
                    assertEquals(rs.getTimestamp(column), timestampForPointInTime);

                    // with calendar
                    assertEquals(rs.getTimestamp(column, getCalendar()).getTime(), reference.getTimestamp(column, getCalendar()).getTime()); // point in time
                    assertEquals(rs.getTimestamp(column, getCalendar()), reference.getTimestamp(column, getCalendar()));
                    assertEquals(rs.getTimestamp(column, getCalendar()), timestampForPointInTime);
                });
    }

    @Test(dataProvider = "timeZoneIds")
    public void testTime(Optional<String> sessionTimezoneId)
            throws Exception
    {
        checkRepresentation("TIME '09:39:05'", TIME, sessionTimezoneId, (rs, reference, column) -> {
            assertEquals(rs.getTime(column), reference.getTime(column));
            assertEquals(rs.getTime(column), Time.valueOf(LocalTime.of(9, 39, 5)));

            // with calendar
            assertEquals(rs.getTime(column, getCalendar()), reference.getTime(column, getCalendar()));
            assertEquals(rs.getTime(column, getCalendar()), new Time(LocalDate.of(1970, 1, 1).atTime(LocalTime.of(9, 39, 5)).atZone(getZoneId()).toInstant().toEpochMilli()));
        });
    }

    @Test(dataProvider = "timeZoneIds")
    public void testDateRoundTrip(Optional<String> sessionTimezoneId)
            throws SQLException
    {
        LocalDate date = LocalDate.of(2001, 5, 6);
        Date sqlDate = Date.valueOf(date);
        java.util.Date javaDate = new java.util.Date(sqlDate.getTime());
        LocalDateTime dateTime = LocalDateTime.of(date, LocalTime.of(12, 34, 56));
        Timestamp sqlTimestamp = Timestamp.valueOf(dateTime);

        assertParameter(sqlDate, sessionTimezoneId, (ps, i) -> ps.setDate(i, sqlDate));
        assertParameter(sqlDate, sessionTimezoneId, (ps, i) -> ps.setObject(i, sqlDate));
        assertParameter(sqlDate, sessionTimezoneId, (ps, i) -> ps.setObject(i, sqlDate, Types.DATE));
        assertParameter(sqlDate, sessionTimezoneId, (ps, i) -> ps.setObject(i, sqlTimestamp, Types.DATE));
        assertParameter(sqlDate, sessionTimezoneId, (ps, i) -> ps.setObject(i, javaDate, Types.DATE));
        assertParameter(sqlDate, sessionTimezoneId, (ps, i) -> ps.setObject(i, date, Types.DATE));
        assertParameter(sqlDate, sessionTimezoneId, (ps, i) -> ps.setObject(i, dateTime, Types.DATE));
        assertParameter(sqlDate, sessionTimezoneId, (ps, i) -> ps.setObject(i, "2001-05-06", Types.DATE));
    }

    @Test(dataProvider = "timeZoneIds")
    public void testTimestampRoundTrip(Optional<String> sessionTimezoneId)
            throws SQLException
    {
        LocalDateTime dateTime = LocalDateTime.of(2001, 5, 6, 12, 34, 56);
        Date sqlDate = Date.valueOf(dateTime.toLocalDate());
        Time sqlTime = Time.valueOf(dateTime.toLocalTime());
        Timestamp sqlTimestamp = Timestamp.valueOf(dateTime);
        Timestamp sameInstantInWarsawZone = Timestamp.valueOf(dateTime.atZone(ZoneId.systemDefault()).withZoneSameInstant(ZoneId.of("Europe/Warsaw")).toLocalDateTime());
        java.util.Date javaDate = java.util.Date.from(dateTime.atZone(ZoneId.systemDefault()).toInstant());

        assertParameter(sqlTimestamp, sessionTimezoneId, (ps, i) -> ps.setTimestamp(i, sqlTimestamp));
        assertParameter(sqlTimestamp, sessionTimezoneId, (ps, i) -> ps.setTimestamp(i, sqlTimestamp, null));
        assertParameter(sqlTimestamp, sessionTimezoneId, (ps, i) -> ps.setTimestamp(i, sqlTimestamp, Calendar.getInstance()));
        assertParameter(sameInstantInWarsawZone, sessionTimezoneId, (ps, i) -> ps.setTimestamp(i, sqlTimestamp, Calendar.getInstance(TimeZone.getTimeZone(ZoneId.of("Europe/Warsaw")))));
        assertParameter(sqlTimestamp, sessionTimezoneId, (ps, i) -> ps.setObject(i, sqlTimestamp));
        assertParameter(new Timestamp(sqlDate.getTime()), sessionTimezoneId, (ps, i) -> ps.setObject(i, sqlDate, Types.TIMESTAMP));
        assertParameter(new Timestamp(sqlTime.getTime()), sessionTimezoneId, (ps, i) -> ps.setObject(i, sqlTime, Types.TIMESTAMP));
        assertParameter(sqlTimestamp, sessionTimezoneId, (ps, i) -> ps.setObject(i, sqlTimestamp, Types.TIMESTAMP));
        assertParameter(sqlTimestamp, sessionTimezoneId, (ps, i) -> ps.setObject(i, javaDate, Types.TIMESTAMP));
        assertParameter(sqlTimestamp, sessionTimezoneId, (ps, i) -> ps.setObject(i, dateTime, Types.TIMESTAMP));
        assertParameter(sqlTimestamp, sessionTimezoneId, (ps, i) -> ps.setObject(i, "2001-05-06 12:34:56", Types.TIMESTAMP));
    }

    private void assertParameter(Object expectedValue, Optional<String> sessionTimezoneId, Binder binder)
            throws SQLException
    {
        // connection is recreated before each test invocation
        sessionTimezoneId.ifPresent(connection.unwrap(TrinoConnection.class)::setTimeZoneId);
        try (PreparedStatement statement = connection.prepareStatement("SELECT ?")) {
            binder.bind(statement, 1);

            try (ResultSet rs = statement.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(expectedValue, rs.getObject(1));
                assertFalse(rs.next());
            }
        }
    }

    private void checkRepresentation(String expression, JDBCType type, Optional<String> sessionTimezoneId, ResultAssertion assertion)
            throws Exception
    {
        List<String> referenceDriversExpressions = referenceDrivers.stream()
                .map(driver -> driver.supports(type) ? expression : "")
                .collect(toImmutableList());
        checkRepresentation(expression, referenceDriversExpressions, type, sessionTimezoneId, assertion);
    }

    private void checkRepresentation(String trinoExpression, List<String> referenceDriversExpressions, JDBCType type, Optional<String> sessionTimezoneId, ResultAssertion assertion)
            throws Exception
    {
        verify(referenceDriversExpressions.size() == referenceDrivers.size(), "Wrong referenceDriversExpressions list size");
        int tests = 0;
        List<AssertionError> failures = new ArrayList<>();

        for (int i = 0; i < referenceDrivers.size(); i++) {
            ReferenceDriver driver = referenceDrivers.get(i);
            String referenceExpression = referenceDriversExpressions.get(i);
            if (!driver.supports(type)) {
                verify(referenceExpression.isEmpty(), "referenceExpression must be empty for %s so that the test code clearly indicates which cases are actually tested", driver);
            }
            else {
                tests++;
                log.info("Checking behavior against %s using expression: %s", driver, referenceExpression);
                try {
                    verify(!referenceExpression.isEmpty(), "referenceExpression is empty");
                    checkRepresentation(trinoExpression, referenceExpression, type, sessionTimezoneId, driver, assertion);
                }
                catch (RuntimeException | AssertionError e) {
                    String message = format("Failure when checking behavior against %s", driver);
                    // log immediately since further tests may take more time; "log and rethrown" is not harmful in tests
                    log.error(e, "%s", message);
                    failures.add(new AssertionError(message, e));
                }
            }
        }

        verify(tests > 0, "No reference driver found supporting %s", type);

        if (!failures.isEmpty()) {
            if (failures.size() == 1 && tests == 1) {
                // The only applicable driver failed
                throw getOnlyElement(failures);
            }

            AssertionError error = new AssertionError(format("Test failed for %s reference drivers out of %s applicable", failures.size(), tests));
            failures.forEach(error::addSuppressed);
            throw error;
        }
    }

    private void checkRepresentation(String trinoExpression, String referenceExpression, JDBCType type, Optional<String> sessionTimezoneId, ReferenceDriver reference, ResultAssertion assertion)
            throws Exception
    {
        try (ResultSet trinoResultSet = trinoQuery(trinoExpression, sessionTimezoneId);
                ResultSet referenceResultSet = reference.query(referenceExpression, sessionTimezoneId)) {
            assertTrue(trinoResultSet.next());
            assertTrue(referenceResultSet.next());
            assertion.accept(trinoResultSet, referenceResultSet, 1);

            assertThat(trinoResultSet.getMetaData().getColumnType(1)).as("Trino declared SQL type")
                    .isEqualTo(type.getVendorTypeNumber());

            assertThat(referenceResultSet.getMetaData().getColumnType(1)).as("Reference driver's declared SQL type for " + type)
                    .isEqualTo(reference.expectedDeclaredJdbcType(type));

            assertFalse(trinoResultSet.next());
            assertFalse(referenceResultSet.next());
        }
    }

    private ResultSet trinoQuery(String expression, Optional<String> sessionTimezoneId)
            throws Exception
    {
        // connection is recreated before each test invocation
        sessionTimezoneId.ifPresent(connection.unwrap(TrinoConnection.class)::setTimeZoneId);
        return statement.executeQuery("SELECT " + expression);
    }

    private Calendar getCalendar()
    {
        return Calendar.getInstance(TimeZone.getTimeZone(ZoneId.of(OTHER_TIMEZONE)));
    }

    private ZoneId getZoneId()
    {
        return ZoneId.of(getCalendar().getTimeZone().getID());
    }

    private interface ReferenceDriver
            extends Closeable
    {
        ResultSet query(String expression, Optional<String> timezoneId)
                throws Exception;

        boolean supports(JDBCType type);

        int expectedDeclaredJdbcType(JDBCType type);

        void setUp();

        void tearDown()
                throws Exception;

        @Override
        void close();
    }

    private static class OracleReferenceDriver
            implements ReferenceDriver
    {
        private final OracleContainer oracleServer;
        private Connection connection;
        private Statement statement;
        private Optional<Optional<String>> timezoneSet = Optional.empty();

        OracleReferenceDriver()
        {
            oracleServer = new OracleContainer("gvenzl/oracle-xe:11.2.0.2-full")
                    .usingSid();
            oracleServer.start();
        }

        @Override
        public ResultSet query(String expression, Optional<String> timezoneId)
                throws Exception
        {
            verify(!timezoneSet.isPresent() || Objects.equals(timezoneSet.get(), timezoneId), "Cannot set time zone %s while %s set previously", timezoneId, timezoneSet);
            timezoneSet = Optional.of(timezoneId);
            if (timezoneId.isPresent()) {
                statement.execute(format("ALTER SESSION SET TIME_ZONE='%s'", timezoneId.get()));
            }
            return statement.executeQuery(format("SELECT %s FROM dual", expression));
        }

        @Override
        public boolean supports(JDBCType type)
        {
            if (type == TIME) {
                return false;
            }
            return true;
        }

        @Override
        public int expectedDeclaredJdbcType(JDBCType type)
        {
            switch (type) {
                case DATE:
                    // Oracle's DATE is actually a TIMESTAMP
                    return Types.TIMESTAMP;
                case TIMESTAMP_WITH_TIMEZONE:
                    // Oracle declares TIMESTAMP WITH TIME ZONE using vendor-specific type number
                    return OracleType.TIMESTAMP_WITH_TIME_ZONE.getVendorTypeNumber();
                default:
                    return type.getVendorTypeNumber();
            }
        }

        @Override
        public void setUp()
        {
            try {
                // recreate connection since tests modify connection state
                connection = DriverManager.getConnection(oracleServer.getJdbcUrl(), oracleServer.getUsername(), oracleServer.getPassword());
                statement = connection.createStatement();
                timezoneSet = Optional.empty();
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void tearDown()
                throws Exception
        {
            statement.close();
            connection.close();
        }

        @Override
        public void close()
        {
            oracleServer.stop();
        }

        @Override
        public String toString()
        {
            return "[oracle]";
        }
    }

    private static class PostgresqlReferenceDriver
            implements ReferenceDriver
    {
        private final PostgreSQLContainer<?> postgresqlContainer;
        private Connection connection;
        private Statement statement;
        private Optional<Optional<String>> timezoneSet = Optional.empty();

        PostgresqlReferenceDriver()
        {
            // Use the current latest PostgreSQL version as the reference
            postgresqlContainer = new PostgreSQLContainer<>("postgres:12.4");
            postgresqlContainer.start();
        }

        @Override
        public ResultSet query(String expression, Optional<String> timezoneId)
                throws Exception
        {
            verify(!timezoneSet.isPresent() || Objects.equals(timezoneSet.get(), timezoneId), "Cannot set time zone %s while %s set previously", timezoneId, timezoneSet);
            timezoneSet = Optional.of(timezoneId);
            if (timezoneId.isPresent()) {
                statement.execute(format("SET SESSION TIME ZONE '%s'", timezoneId.get()));
            }
            return statement.executeQuery(format("SELECT %s", expression));
        }

        @Override
        public boolean supports(JDBCType type)
        {
            return true;
        }

        @Override
        public int expectedDeclaredJdbcType(JDBCType type)
        {
            switch (type) {
                case TIMESTAMP_WITH_TIMEZONE:
                    // PostgreSQL returns TIMESTAMP WITH TIME ZONE declaring it as TIMESTAMP on JDBC level
                    return Types.TIMESTAMP;
                case VARBINARY:
                    return Types.BINARY;
                default:
                    return type.getVendorTypeNumber();
            }
        }

        @Override
        public void setUp()
        {
            try {
                // recreate connection since tests modify connection state
                connection = postgresqlContainer.createConnection("");
                statement = connection.createStatement();
                timezoneSet = Optional.empty();
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void tearDown()
                throws Exception
        {
            statement.close();
            connection.close();
        }

        @Override
        public void close()
        {
            postgresqlContainer.stop();
        }

        @Override
        public String toString()
        {
            return "[postgresql]";
        }
    }

    @FunctionalInterface
    private interface ResultAssertion
    {
        void accept(ResultSet rs, ResultSet reference, int column)
                throws Exception;
    }

    private interface Binder
    {
        void bind(PreparedStatement ps, int i)
                throws SQLException;
    }
}
