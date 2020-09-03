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
package io.prestosql.jdbc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closer;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.prestosql.server.testing.TestingPrestoServer;
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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

import static java.lang.String.format;
import static java.sql.JDBCType.DATE;
import static java.sql.JDBCType.TIME;
import static java.sql.JDBCType.TIMESTAMP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

/**
 * Checks that JDBC dates and timestamps are interpreted in the client JVM timezone and should not
 * be affected by the session timezone.  We also compare the behavior to reference JDBC drivers.
 */
@Test(singleThreaded = true)
public class TestJdbcResultSetTimezone
{
    private static final String OTHER_TIMEZONE = "Asia/Kathmandu";

    private Logger log;
    private TestingPrestoServer server;
    private List<ReferenceDriver> referenceDrivers;

    private Connection connection;
    private Statement statement;

    @BeforeClass
    public void setupServer()
    {
        assertNotEquals(OTHER_TIMEZONE, TimeZone.getDefault().getID(), "We need a timezone different from the default JVM one");
        Logging.initialize();
        log = Logger.get(TestJdbcResultSetTimezone.class);
        server = TestingPrestoServer.create();
        referenceDrivers = ImmutableList.of(new PostgresqlReferenceDriver(), new OracleReferenceDriver());
    }

    @AfterClass(alwaysRun = true)
    public void tearDownServer()
            throws Exception
    {
        try (Closer closer = Closer.create()) {
            referenceDrivers.forEach(closer::register);
            closer.register(server);
        }
    }

    @SuppressWarnings("JDBCResourceOpenedButNotSafelyClosed")
    @BeforeMethod
    public void setUp()
            throws Exception
    {
        connection = DriverManager.getConnection("jdbc:presto://" + server.getAddress(), "test", null);
        statement = connection.createStatement();
        referenceDrivers.forEach(ReferenceDriver::setUp);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        statement.close();
        connection.close();
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
                {"UTC"},
                {"Europe/Warsaw"},
                {"America/Denver"},
                {ZoneId.systemDefault().getId()}
        };
    }

    @Test(dataProvider = "timeZoneIds")
    public void testDate(String sessionTimezoneId)
            throws Exception
    {
        checkRepresentation("DATE '2018-02-13'", DATE, sessionTimezoneId, (rs, reference, column) -> {
            assertEquals(rs.getDate(column), reference.getDate(column));
            assertEquals(rs.getDate(column), Date.valueOf(LocalDate.of(2018, 2, 13)));
        });

        checkRepresentation("DATE '2018-02-13'", DATE, sessionTimezoneId, (rs, reference, column) -> {
            assertEquals(rs.getDate(column, getCalendar()), reference.getDate(column, getCalendar()));
            assertEquals(rs.getDate(column, getCalendar()), new Date(LocalDate.of(2018, 2, 13).atStartOfDay(getZoneId()).toInstant().toEpochMilli()));
        });
    }

    @Test(dataProvider = "timeZoneIds")
    public void testTimestamp(String sessionTimezoneId)
            throws Exception
    {
        checkRepresentation("TIMESTAMP '2018-02-13 13:14:15.123'", TIMESTAMP, sessionTimezoneId, (rs, reference, column) -> {
            assertEquals(rs.getTimestamp(column), reference.getTimestamp(column));
            assertEquals(
                    rs.getTimestamp(column),
                    Timestamp.valueOf(LocalDateTime.of(2018, 2, 13, 13, 14, 15, 123_000_000)));
        });

        checkRepresentation("TIMESTAMP '2018-02-13 13:14:15.123'", TIMESTAMP, sessionTimezoneId, (rs, reference, column) -> {
            assertEquals(rs.getTimestamp(column, getCalendar()), reference.getTimestamp(column, getCalendar()));
            assertEquals(
                    rs.getTimestamp(column, getCalendar()),
                    new Timestamp(LocalDateTime.of(2018, 2, 13, 13, 14, 15, 123_000_000).atZone(getZoneId()).toInstant().toEpochMilli()));
        });
    }

    @Test(dataProvider = "timeZoneIds")
    public void testTime(String sessionTimezoneId)
            throws Exception
    {
        checkRepresentation("TIME '09:39:05'", TIME, sessionTimezoneId, (rs, reference, column) -> {
            assertEquals(rs.getTime(column), reference.getTime(column));
            assertEquals(rs.getTime(column), Time.valueOf(LocalTime.of(9, 39, 5)));
        });

        checkRepresentation("TIME '09:39:05'", TIME, sessionTimezoneId, (rs, reference, column) -> {
            assertEquals(rs.getTime(column, getCalendar()), reference.getTime(column, getCalendar()));
            assertEquals(rs.getTime(column, getCalendar()), new Time(LocalDate.of(1970, 1, 1).atTime(LocalTime.of(9, 39, 5)).atZone(getZoneId()).toInstant().toEpochMilli()));
        });
    }

    @Test(dataProvider = "timeZoneIds")
    public void testDateRoundTrip(String sessionTimezoneId)
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
    public void testTimestampRoundTrip(String sessionTimezoneId)
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
        assertParameter(sameInstantInWarsawZone, sessionTimezoneId, (ps, i) -> ps.setTimestamp(i, sqlTimestamp, Calendar.getInstance(TimeZone.getTimeZone("Europe/Warsaw"))));
        assertParameter(sqlTimestamp, sessionTimezoneId, (ps, i) -> ps.setObject(i, sqlTimestamp));
        assertParameter(new Timestamp(sqlDate.getTime()), sessionTimezoneId, (ps, i) -> ps.setObject(i, sqlDate, Types.TIMESTAMP));
        assertParameter(new Timestamp(sqlTime.getTime()), sessionTimezoneId, (ps, i) -> ps.setObject(i, sqlTime, Types.TIMESTAMP));
        assertParameter(sqlTimestamp, sessionTimezoneId, (ps, i) -> ps.setObject(i, sqlTimestamp, Types.TIMESTAMP));
        assertParameter(sqlTimestamp, sessionTimezoneId, (ps, i) -> ps.setObject(i, javaDate, Types.TIMESTAMP));
        assertParameter(sqlTimestamp, sessionTimezoneId, (ps, i) -> ps.setObject(i, dateTime, Types.TIMESTAMP));
        assertParameter(sqlTimestamp, sessionTimezoneId, (ps, i) -> ps.setObject(i, "2001-05-06 12:34:56", Types.TIMESTAMP));
    }

    private void assertParameter(Object expectedValue, String sessionTimezoneId, Binder binder)
            throws SQLException
    {
        connection.unwrap(PrestoConnection.class).setTimeZoneId(sessionTimezoneId);
        try (PreparedStatement statement = connection.prepareStatement("SELECT ?")) {
            binder.bind(statement, 1);

            try (ResultSet rs = statement.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(expectedValue, rs.getObject(1));
                assertFalse(rs.next());
            }
        }
    }

    private void checkRepresentation(String expression, JDBCType type, String sessionTimezoneId, ResultAssertion assertion)
            throws Exception
    {
        for (ReferenceDriver driver : referenceDrivers) {
            if (driver.supports(type)) {
                log.info("Checking behavior against %s", driver);
                checkRepresentation(expression, sessionTimezoneId, driver, assertion);
            }
        }
    }

    private void checkRepresentation(String expression, String sessionTimezoneId, ReferenceDriver reference, ResultAssertion assertion)
            throws Exception
    {
        try (ResultSet rs = prestoQuery(expression, sessionTimezoneId); ResultSet referenceResultSet = reference.query(expression, sessionTimezoneId)) {
            assertTrue(rs.next());
            assertTrue(referenceResultSet.next());
            assertion.accept(rs, referenceResultSet, 1);
            assertFalse(rs.next());
            assertFalse(referenceResultSet.next());
        }
    }

    private ResultSet prestoQuery(String expression, String timezoneId)
            throws Exception
    {
        connection.unwrap(PrestoConnection.class).setTimeZoneId(timezoneId);
        return statement.executeQuery("SELECT " + expression);
    }

    private Calendar getCalendar()
    {
        return Calendar.getInstance(TimeZone.getTimeZone(OTHER_TIMEZONE));
    }

    private ZoneId getZoneId()
    {
        return ZoneId.of(getCalendar().getTimeZone().getID());
    }

    private interface ReferenceDriver
            extends Closeable
    {
        ResultSet query(String expression, String timezoneId) throws Exception;

        boolean supports(JDBCType type);

        void setUp();

        void tearDown() throws Exception;

        @Override
        void close();
    }

    private static class OracleReferenceDriver
            implements ReferenceDriver
    {
        private static final Set<JDBCType> SUPPORTED_TYPES = ImmutableSet.of(DATE, TIMESTAMP);
        private OracleContainer oracleServer;
        private Connection connection;
        private Statement statement;

        OracleReferenceDriver()
        {
            oracleServer = new OracleContainer("wnameless/oracle-xe-11g-r2");
            oracleServer.start();
        }

        @Override
        public ResultSet query(String expression, String timezoneId)
                throws Exception
        {
            statement.execute(format("ALTER SESSION SET TIME_ZONE='%s'", timezoneId));
            return statement.executeQuery(format("SELECT %s FROM dual", expression));
        }

        @Override
        public boolean supports(JDBCType type)
        {
            return SUPPORTED_TYPES.contains(type);
        }

        @Override
        public void setUp()
        {
            try {
                connection = DriverManager.getConnection(oracleServer.getJdbcUrl(), oracleServer.getUsername(), oracleServer.getPassword());
                statement = connection.createStatement();
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
        private static final Set<JDBCType> SUPPORTED_TYPES = ImmutableSet.of(DATE, TIME, TIMESTAMP);
        private PostgreSQLContainer<?> postgresqlContainer;
        private Connection connection;
        private Statement statement;

        PostgresqlReferenceDriver()
        {
            // Use the current latest PostgreSQL version as the reference
            postgresqlContainer = new PostgreSQLContainer<>("postgres:12.4");
            postgresqlContainer.start();
        }

        @Override
        public ResultSet query(String expression, String timezoneId)
                throws Exception
        {
            statement.execute(format("SET SESSION TIME ZONE '%s'", timezoneId));
            return statement.executeQuery(format("SELECT %s", expression));
        }

        @Override
        public boolean supports(JDBCType type)
        {
            return SUPPORTED_TYPES.contains(type);
        }

        @Override
        public void setUp()
        {
            try {
                connection = postgresqlContainer.createConnection("");
                statement = connection.createStatement();
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
