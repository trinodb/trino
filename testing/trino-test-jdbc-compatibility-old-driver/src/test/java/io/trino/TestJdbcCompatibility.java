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
package io.trino;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logging;
import io.trino.plugin.mongodb.MongoPlugin;
import io.trino.server.testing.TestingTrinoServer;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Consumer;

import static io.trino.JdbcDriverCapabilities.correctlyReportsTimestampWithTimeZone;
import static io.trino.JdbcDriverCapabilities.driverVersion;
import static io.trino.JdbcDriverCapabilities.hasBrokenParametricTimestampWithTimeZoneSupport;
import static io.trino.JdbcDriverCapabilities.jdbcDriver;
import static io.trino.JdbcDriverCapabilities.supportsParametricTimestamp;
import static io.trino.JdbcDriverCapabilities.supportsParametricTimestampWithTimeZone;
import static io.trino.JdbcDriverCapabilities.supportsSessionPropertiesViaConnectionUri;
import static io.trino.JdbcDriverCapabilities.supportsTimestampObjectRepresentationInCollections;
import static io.trino.JdbcDriverCapabilities.testedVersion;
import static java.lang.String.format;
import static java.sql.Types.ARRAY;
import static java.sql.Types.JAVA_OBJECT;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TIMESTAMP_WITH_TIMEZONE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The main purpose of this class is to test cases when current server implementation breaks older JDBC clients
 * to ensure that current implementation is backward-compatible.
 * <p>
 * {@link TestJdbcResultSetCompatibilityOldDriver} in that regard is responsible for testing forward compatibility
 * as it's using old test code and old JDBC client against current server implementation.
 * <p>
 * This test in turn is run using an old JDBC client against current server implementation.
 */
public class TestJdbcCompatibility
{
    private static final Optional<Integer> VERSION_UNDER_TEST = testedVersion();
    private static final int TIMESTAMP_DEFAULT_PRECISION = 3;

    private TestingTrinoServer server;
    private String serverUrl;

    @Test
    public void ensureProperDriverVersionLoaded()
    {
        if (VERSION_UNDER_TEST.isEmpty()) {
            throw new SkipException("Information about JDBC version under test is missing");
        }

        assertThat(driverVersion())
                .isEqualTo(VERSION_UNDER_TEST.get());

        assertThat(jdbcDriver().getClass().getPackage().getImplementationVersion())
                .isEqualTo(VERSION_UNDER_TEST.get().toString());
    }

    @BeforeClass
    public void setup()
    {
        Logging.initialize();

        server = TestingTrinoServer.builder()
                .build();

        server.installPlugin(new MongoPlugin());

        serverUrl = format("jdbc:trino://%s", server.getAddress());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        server.close();
        server = null;
    }

    @Test
    public void testLongPreparedStatement()
            throws Exception
    {
        String sql = format("SELECT '%s' = '%s'", "x".repeat(100_000), "y".repeat(100_000));

        try (Connection connection = getConnection();
                PreparedStatement statement = connection.prepareStatement(sql);
                ResultSet rs = statement.executeQuery()) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getBoolean(1)).isFalse();
            assertThat(rs.next()).isFalse();
        }
    }

    @Test
    public void testSelectTimestamp()
    {
        String query = "SELECT TIMESTAMP '2012-10-31 01:00'";
        checkRepresentation(query, Timestamp.valueOf("2012-10-31 01:00:00.000"), TIMESTAMP, ResultSet::getTimestamp);
    }

    @Test
    public void testSelectTimestampWithTimeZone()
    {
        if (hasBrokenParametricTimestampWithTimeZoneSupport()) {
            throw new SkipException("This version reports PARAMETRIC_DATETIME client capability but TIMESTAMP WITH TIME ZONE is not supported");
        }

        String query = "SELECT timestamp '2012-10-31 01:00 Australia/Eucla'";
        checkRepresentation(query, Timestamp.valueOf("2012-10-30 10:15:00.000"), correctlyReportsTimestampWithTimeZone() ? TIMESTAMP_WITH_TIMEZONE : TIMESTAMP, ResultSet::getTimestamp);
    }

    @Test
    public void testSelectParametricTimestamp()
    {
        if (!supportsParametricTimestamp()) {
            testSelectParametricTimestamp("TIMESTAMP '2004-08-24 23:55:23'", Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23")), "2004-08-24 23:55:23.000");
            testSelectParametricTimestamp("TIMESTAMP '2004-08-24 23:55:23.1'", Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.1")), "2004-08-24 23:55:23.100");
            testSelectParametricTimestamp("TIMESTAMP '2004-08-24 23:55:23.12'", Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.12")), "2004-08-24 23:55:23.120");
            testSelectParametricTimestamp("TIMESTAMP '2004-08-24 23:55:23.123'", Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.123")), "2004-08-24 23:55:23.123");
            testSelectParametricTimestamp("TIMESTAMP '2004-08-24 23:55:23.1234'", Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.123")), "2004-08-24 23:55:23.123");
            testSelectParametricTimestamp("TIMESTAMP '2004-08-24 23:55:23.12345'", Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.123")), "2004-08-24 23:55:23.123");
            testSelectParametricTimestamp("TIMESTAMP '2004-08-24 23:55:23.123456'", Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.123")), "2004-08-24 23:55:23.123");
            testSelectParametricTimestamp("TIMESTAMP '2004-08-24 23:55:23.1234567'", Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.123")), "2004-08-24 23:55:23.123");
            testSelectParametricTimestamp("TIMESTAMP '2004-08-24 23:55:23.12345678'", Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.123")), "2004-08-24 23:55:23.123");
            testSelectParametricTimestamp("TIMESTAMP '2004-08-24 23:55:23.123456789'", Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.123")), "2004-08-24 23:55:23.123");
            testSelectParametricTimestamp("TIMESTAMP '2004-08-24 23:55:23.1234567890'", Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.123")), "2004-08-24 23:55:23.123");
            testSelectParametricTimestamp("TIMESTAMP '2004-08-24 23:55:23.12345678901'", Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.123")), "2004-08-24 23:55:23.123");
            testSelectParametricTimestamp("TIMESTAMP '2004-08-24 23:55:23.123456789012'", Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.123")), "2004-08-24 23:55:23.123");
            return;
        }

        testSelectParametricTimestamp("TIMESTAMP '2004-08-24 23:55:23'", Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23")), "2004-08-24 23:55:23");
        testSelectParametricTimestamp("TIMESTAMP '2004-08-24 23:55:23.1'", Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.1")), "2004-08-24 23:55:23.1");
        testSelectParametricTimestamp("TIMESTAMP '2004-08-24 23:55:23.12'", Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.12")), "2004-08-24 23:55:23.12");
        testSelectParametricTimestamp("TIMESTAMP '2004-08-24 23:55:23.123'", Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.123")), "2004-08-24 23:55:23.123");
        testSelectParametricTimestamp("TIMESTAMP '2004-08-24 23:55:23.1234'", Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.1234")), "2004-08-24 23:55:23.1234");
        testSelectParametricTimestamp("TIMESTAMP '2004-08-24 23:55:23.12345'", Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.12345")), "2004-08-24 23:55:23.12345");
        testSelectParametricTimestamp("TIMESTAMP '2004-08-24 23:55:23.123456'", Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.123456")), "2004-08-24 23:55:23.123456");
        testSelectParametricTimestamp("TIMESTAMP '2004-08-24 23:55:23.1234567'", Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.1234567")), "2004-08-24 23:55:23.1234567");
        testSelectParametricTimestamp("TIMESTAMP '2004-08-24 23:55:23.12345678'", Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.12345678")), "2004-08-24 23:55:23.12345678");
        testSelectParametricTimestamp("TIMESTAMP '2004-08-24 23:55:23.123456789'", Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.123456789")), "2004-08-24 23:55:23.123456789");
        testSelectParametricTimestamp("TIMESTAMP '2004-08-24 23:55:23.1234567890'", Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.123456789")), "2004-08-24 23:55:23.1234567890");
        testSelectParametricTimestamp("TIMESTAMP '2004-08-24 23:55:23.12345678901'", Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.123456789")), "2004-08-24 23:55:23.12345678901");
        testSelectParametricTimestamp("TIMESTAMP '2004-08-24 23:55:23.123456789012'", Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.123456789")), "2004-08-24 23:55:23.123456789012");
    }

    private void testSelectParametricTimestamp(String expression, Object expectedValue, String expectedString)
    {
        String query = "SELECT " + expression;
        checkRepresentation(query, TIMESTAMP, (resultSet, columnIndex) -> {
            assertThat(resultSet.getTimestamp(columnIndex)).isEqualTo(expectedValue);
            assertThat(resultSet.getObject(columnIndex)).isEqualTo(expectedValue);
            assertThat(resultSet.getString(columnIndex)).isEqualTo(expectedString);
        });
    }

    @Test
    public void testSelectParametricTimestampWithTimeZone()
    {
        if (hasBrokenParametricTimestampWithTimeZoneSupport()) {
            throw new SkipException("This version reports PARAMETRIC_DATETIME client capability but TIMESTAMP WITH TIME ZONE is not supported");
        }

        if (!supportsParametricTimestampWithTimeZone()) {
            testSelectParametricTimestampWithTimeZone("TIMESTAMP '2004-08-24 23:55:23 Australia/Eucla'", 0, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T09:10:23")), "2004-08-24 23:55:23.000 Australia/Eucla");
            testSelectParametricTimestampWithTimeZone("TIMESTAMP '2004-08-24 23:55:23.1 Australia/Eucla'", 1, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T09:10:23.1")), "2004-08-24 23:55:23.100 Australia/Eucla");
            testSelectParametricTimestampWithTimeZone("TIMESTAMP '2004-08-24 23:55:23.12 Australia/Eucla'", 2, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T09:10:23.12")), "2004-08-24 23:55:23.120 Australia/Eucla");
            testSelectParametricTimestampWithTimeZone("TIMESTAMP '2004-08-24 23:55:23.123 Australia/Eucla'", 3, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T09:10:23.123")), "2004-08-24 23:55:23.123 Australia/Eucla");
            testSelectParametricTimestampWithTimeZone("TIMESTAMP '2004-08-24 23:55:23.1234 Australia/Eucla'", 4, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T09:10:23.123")), "2004-08-24 23:55:23.123 Australia/Eucla");
            testSelectParametricTimestampWithTimeZone("TIMESTAMP '2004-08-24 23:55:23.12345 Australia/Eucla'", 5, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T09:10:23.123")), "2004-08-24 23:55:23.123 Australia/Eucla");
            testSelectParametricTimestampWithTimeZone("TIMESTAMP '2004-08-24 23:55:23.123456 Australia/Eucla'", 6, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T09:10:23.123")), "2004-08-24 23:55:23.123 Australia/Eucla");
            testSelectParametricTimestampWithTimeZone("TIMESTAMP '2004-08-24 23:55:23.1234567 Australia/Eucla'", 7, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T09:10:23.123")), "2004-08-24 23:55:23.123 Australia/Eucla");
            testSelectParametricTimestampWithTimeZone("TIMESTAMP '2004-08-24 23:55:23.12345678 Australia/Eucla'", 8, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T09:10:23.123")), "2004-08-24 23:55:23.123 Australia/Eucla");
            testSelectParametricTimestampWithTimeZone("TIMESTAMP '2004-08-24 23:55:23.123456789 Australia/Eucla'", 9, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T09:10:23.123")), "2004-08-24 23:55:23.123 Australia/Eucla");
            testSelectParametricTimestampWithTimeZone("TIMESTAMP '2004-08-24 23:55:23.1234567890 Australia/Eucla'", 10, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T09:10:23.123")), "2004-08-24 23:55:23.123 Australia/Eucla");
            testSelectParametricTimestampWithTimeZone("TIMESTAMP '2004-08-24 23:55:23.12345678901 Australia/Eucla'", 11, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T09:10:23.123")), "2004-08-24 23:55:23.123 Australia/Eucla");
            testSelectParametricTimestampWithTimeZone("TIMESTAMP '2004-08-24 23:55:23.123456789012 Australia/Eucla'", 12, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T09:10:23.123")), "2004-08-24 23:55:23.123 Australia/Eucla");
            return;
        }

        testSelectParametricTimestampWithTimeZone("TIMESTAMP '2004-08-24 23:55:23 Australia/Eucla'", 0, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T09:10:23")), "2004-08-24 23:55:23 Australia/Eucla");
        testSelectParametricTimestampWithTimeZone("TIMESTAMP '2004-08-24 23:55:23.1 Australia/Eucla'", 1, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T09:10:23.1")), "2004-08-24 23:55:23.1 Australia/Eucla");
        testSelectParametricTimestampWithTimeZone("TIMESTAMP '2004-08-24 23:55:23.12 Australia/Eucla'", 2, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T09:10:23.12")), "2004-08-24 23:55:23.12 Australia/Eucla");
        testSelectParametricTimestampWithTimeZone("TIMESTAMP '2004-08-24 23:55:23.123 Australia/Eucla'", 3, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T09:10:23.123")), "2004-08-24 23:55:23.123 Australia/Eucla");
        testSelectParametricTimestampWithTimeZone("TIMESTAMP '2004-08-24 23:55:23.1234 Australia/Eucla'", 4, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T09:10:23.1234")), "2004-08-24 23:55:23.1234 Australia/Eucla");
        testSelectParametricTimestampWithTimeZone("TIMESTAMP '2004-08-24 23:55:23.12345 Australia/Eucla'", 5, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T09:10:23.12345")), "2004-08-24 23:55:23.12345 Australia/Eucla");
        testSelectParametricTimestampWithTimeZone("TIMESTAMP '2004-08-24 23:55:23.123456 Australia/Eucla'", 6, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T09:10:23.123456")), "2004-08-24 23:55:23.123456 Australia/Eucla");
        testSelectParametricTimestampWithTimeZone("TIMESTAMP '2004-08-24 23:55:23.1234567 Australia/Eucla'", 7, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T09:10:23.1234567")), "2004-08-24 23:55:23.1234567 Australia/Eucla");
        testSelectParametricTimestampWithTimeZone("TIMESTAMP '2004-08-24 23:55:23.12345678 Australia/Eucla'", 8, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T09:10:23.12345678")), "2004-08-24 23:55:23.12345678 Australia/Eucla");
        testSelectParametricTimestampWithTimeZone("TIMESTAMP '2004-08-24 23:55:23.123456789 Australia/Eucla'", 9, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T09:10:23.123456789")), "2004-08-24 23:55:23.123456789 Australia/Eucla");
        testSelectParametricTimestampWithTimeZone("TIMESTAMP '2004-08-24 23:55:23.1234567890 Australia/Eucla'", 10, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T09:10:23.123456789")), "2004-08-24 23:55:23.1234567890 Australia/Eucla");
        testSelectParametricTimestampWithTimeZone("TIMESTAMP '2004-08-24 23:55:23.12345678901 Australia/Eucla'", 11, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T09:10:23.123456789")), "2004-08-24 23:55:23.12345678901 Australia/Eucla");
        testSelectParametricTimestampWithTimeZone("TIMESTAMP '2004-08-24 23:55:23.123456789012 Australia/Eucla'", 12, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T09:10:23.123456789")), "2004-08-24 23:55:23.123456789012 Australia/Eucla");
    }

    private void testSelectParametricTimestampWithTimeZone(String expression, int expectedPrecision, Object expectedValue, String expectedString)
    {
        String query = "SELECT " + expression;
        checkRepresentation(query, correctlyReportsTimestampWithTimeZone() ? TIMESTAMP_WITH_TIMEZONE : TIMESTAMP, (resultSet, columnIndex) -> {
            assertThat(resultSet.getTimestamp(columnIndex)).isEqualTo(expectedValue);
            assertThat(resultSet.getObject(columnIndex)).isEqualTo(expectedValue);
            assertThat(resultSet.getString(columnIndex)).isEqualTo(expectedString);
        });
        checkDescribeTimestampType(query, "%s", expectedPrecision, true);
    }

    @Test
    public void testSelectParametricTimestampInMap()
    {
        if (!supportsParametricTimestamp()) {
            testSelectParametricTimestampInMap("TIMESTAMP '2004-08-24 23:55:23'", 0, "2004-08-24 23:55:23.000");
            testSelectParametricTimestampInMap("TIMESTAMP '2004-08-24 23:55:23.1'", 1, "2004-08-24 23:55:23.100");
            testSelectParametricTimestampInMap("TIMESTAMP '2004-08-24 23:55:23.12'", 2, "2004-08-24 23:55:23.120");
            testSelectParametricTimestampInMap("TIMESTAMP '2004-08-24 23:55:23.123'", 3, "2004-08-24 23:55:23.123");
            testSelectParametricTimestampInMap("TIMESTAMP '2004-08-24 23:55:23.1234'", 4, "2004-08-24 23:55:23.123");
            testSelectParametricTimestampInMap("TIMESTAMP '2004-08-24 23:55:23.12345'", 5, "2004-08-24 23:55:23.123");
            testSelectParametricTimestampInMap("TIMESTAMP '2004-08-24 23:55:23.123456'", 6, "2004-08-24 23:55:23.123");
            testSelectParametricTimestampInMap("TIMESTAMP '2004-08-24 23:55:23.1234567'", 7, "2004-08-24 23:55:23.123");
            testSelectParametricTimestampInMap("TIMESTAMP '2004-08-24 23:55:23.12345678'", 8, "2004-08-24 23:55:23.123");
            testSelectParametricTimestampInMap("TIMESTAMP '2004-08-24 23:55:23.123456789'", 9, "2004-08-24 23:55:23.123");
            testSelectParametricTimestampInMap("TIMESTAMP '2004-08-24 23:55:23.1234567890'", 10, "2004-08-24 23:55:23.123");
            testSelectParametricTimestampInMap("TIMESTAMP '2004-08-24 23:55:23.12345678901'", 11, "2004-08-24 23:55:23.123");
            testSelectParametricTimestampInMap("TIMESTAMP '2004-08-24 23:55:23.123456789012'", 12, "2004-08-24 23:55:23.123");
            return;
        }

        if (!supportsTimestampObjectRepresentationInCollections()) {
            testSelectParametricTimestampInMap("TIMESTAMP '2004-08-24 23:55:23'", 0, "2004-08-24 23:55:23");
            testSelectParametricTimestampInMap("TIMESTAMP '2004-08-24 23:55:23.1'", 1, "2004-08-24 23:55:23.1");
            testSelectParametricTimestampInMap("TIMESTAMP '2004-08-24 23:55:23.12'", 2, "2004-08-24 23:55:23.12");
            testSelectParametricTimestampInMap("TIMESTAMP '2004-08-24 23:55:23.123'", 3, "2004-08-24 23:55:23.123");
            testSelectParametricTimestampInMap("TIMESTAMP '2004-08-24 23:55:23.1234'", 4, "2004-08-24 23:55:23.1234");
            testSelectParametricTimestampInMap("TIMESTAMP '2004-08-24 23:55:23.12345'", 5, "2004-08-24 23:55:23.12345");
            testSelectParametricTimestampInMap("TIMESTAMP '2004-08-24 23:55:23.123456'", 6, "2004-08-24 23:55:23.123456");
            testSelectParametricTimestampInMap("TIMESTAMP '2004-08-24 23:55:23.1234567'", 7, "2004-08-24 23:55:23.1234567");
            testSelectParametricTimestampInMap("TIMESTAMP '2004-08-24 23:55:23.12345678'", 8, "2004-08-24 23:55:23.12345678");
            testSelectParametricTimestampInMap("TIMESTAMP '2004-08-24 23:55:23.123456789'", 9, "2004-08-24 23:55:23.123456789");
            testSelectParametricTimestampInMap("TIMESTAMP '2004-08-24 23:55:23.1234567890'", 10, "2004-08-24 23:55:23.1234567890");
            testSelectParametricTimestampInMap("TIMESTAMP '2004-08-24 23:55:23.12345678901'", 11, "2004-08-24 23:55:23.12345678901");
            testSelectParametricTimestampInMap("TIMESTAMP '2004-08-24 23:55:23.123456789012'", 12, "2004-08-24 23:55:23.123456789012");
            return;
        }

        testSelectParametricTimestampInMap("TIMESTAMP '2004-08-24 23:55:23'", 0, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23")));
        testSelectParametricTimestampInMap("TIMESTAMP '2004-08-24 23:55:23.1'", 1, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.1")));
        testSelectParametricTimestampInMap("TIMESTAMP '2004-08-24 23:55:23.12'", 2, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.12")));
        testSelectParametricTimestampInMap("TIMESTAMP '2004-08-24 23:55:23.123'", 3, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.123")));
        testSelectParametricTimestampInMap("TIMESTAMP '2004-08-24 23:55:23.1234'", 4, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.1234")));
        testSelectParametricTimestampInMap("TIMESTAMP '2004-08-24 23:55:23.12345'", 5, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.12345")));
        testSelectParametricTimestampInMap("TIMESTAMP '2004-08-24 23:55:23.123456'", 6, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.123456")));
        testSelectParametricTimestampInMap("TIMESTAMP '2004-08-24 23:55:23.1234567'", 7, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.1234567")));
        testSelectParametricTimestampInMap("TIMESTAMP '2004-08-24 23:55:23.12345678'", 8, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.12345678")));
        testSelectParametricTimestampInMap("TIMESTAMP '2004-08-24 23:55:23.123456789'", 9, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.123456789")));
        testSelectParametricTimestampInMap("TIMESTAMP '2004-08-24 23:55:23.1234567890'", 10, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.123456789")));
        testSelectParametricTimestampInMap("TIMESTAMP '2004-08-24 23:55:23.12345678901'", 11, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.123456789")));
        testSelectParametricTimestampInMap("TIMESTAMP '2004-08-24 23:55:23.123456789012'", 12, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.123456789")));
    }

    private void testSelectParametricTimestampInMap(String elementExpression, int expectedPrecision, Object expectedValue)
    {
        String query = format("SELECT map_from_entries(ARRAY[('timestamp', %s)])", elementExpression);
        checkRepresentation(query, expectedValue, JAVA_OBJECT, TestJdbcCompatibility::getSingleElementFromMap);
        checkDescribeTimestampType(query, "map(varchar(9), %s)", expectedPrecision, false);
    }

    @Test
    public void testSelectParametricTimestampWithTimeZoneInMap()
    {
        if (hasBrokenParametricTimestampWithTimeZoneSupport()) {
            throw new SkipException("This version reports PARAMETRIC_DATETIME client capability but TIMESTAMP WITH TIME ZONE is not supported");
        }

        if (!supportsParametricTimestampWithTimeZone()) {
            testSelectParametricTimestampWithTimeZoneInMap("TIMESTAMP '2004-08-24 23:55:23 Australia/Eucla'", 0, "2004-08-24 23:55:23.000 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInMap("TIMESTAMP '2004-08-24 23:55:23.1 Australia/Eucla'", 1, "2004-08-24 23:55:23.100 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInMap("TIMESTAMP '2004-08-24 23:55:23.12 Australia/Eucla'", 2, "2004-08-24 23:55:23.120 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInMap("TIMESTAMP '2004-08-24 23:55:23.123 Australia/Eucla'", 3, "2004-08-24 23:55:23.123 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInMap("TIMESTAMP '2004-08-24 23:55:23.1234 Australia/Eucla'", 4, "2004-08-24 23:55:23.123 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInMap("TIMESTAMP '2004-08-24 23:55:23.12345 Australia/Eucla'", 5, "2004-08-24 23:55:23.123 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInMap("TIMESTAMP '2004-08-24 23:55:23.123456 Australia/Eucla'", 6, "2004-08-24 23:55:23.123 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInMap("TIMESTAMP '2004-08-24 23:55:23.1234567 Australia/Eucla'", 7, "2004-08-24 23:55:23.123 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInMap("TIMESTAMP '2004-08-24 23:55:23.12345678 Australia/Eucla'", 8, "2004-08-24 23:55:23.123 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInMap("TIMESTAMP '2004-08-24 23:55:23.123456789 Australia/Eucla'", 9, "2004-08-24 23:55:23.123 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInMap("TIMESTAMP '2004-08-24 23:55:23.1234567890 Australia/Eucla'", 10, "2004-08-24 23:55:23.123 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInMap("TIMESTAMP '2004-08-24 23:55:23.12345678901 Australia/Eucla'", 11, "2004-08-24 23:55:23.123 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInMap("TIMESTAMP '2004-08-24 23:55:23.123456789012 Australia/Eucla'", 12, "2004-08-24 23:55:23.123 Australia/Eucla");
            return;
        }

        if (!supportsTimestampObjectRepresentationInCollections()) {
            testSelectParametricTimestampWithTimeZoneInMap("TIMESTAMP '2004-08-24 23:55:23 Australia/Eucla'", 0, "2004-08-24 23:55:23 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInMap("TIMESTAMP '2004-08-24 23:55:23.1 Australia/Eucla'", 1, "2004-08-24 23:55:23.1 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInMap("TIMESTAMP '2004-08-24 23:55:23.12 Australia/Eucla'", 2, "2004-08-24 23:55:23.12 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInMap("TIMESTAMP '2004-08-24 23:55:23.123 Australia/Eucla'", 3, "2004-08-24 23:55:23.123 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInMap("TIMESTAMP '2004-08-24 23:55:23.1234 Australia/Eucla'", 4, "2004-08-24 23:55:23.1234 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInMap("TIMESTAMP '2004-08-24 23:55:23.12345 Australia/Eucla'", 5, "2004-08-24 23:55:23.12345 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInMap("TIMESTAMP '2004-08-24 23:55:23.123456 Australia/Eucla'", 6, "2004-08-24 23:55:23.123456 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInMap("TIMESTAMP '2004-08-24 23:55:23.1234567 Australia/Eucla'", 7, "2004-08-24 23:55:23.1234567 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInMap("TIMESTAMP '2004-08-24 23:55:23.12345678 Australia/Eucla'", 8, "2004-08-24 23:55:23.12345678 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInMap("TIMESTAMP '2004-08-24 23:55:23.123456789 Australia/Eucla'", 9, "2004-08-24 23:55:23.123456789 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInMap("TIMESTAMP '2004-08-24 23:55:23.1234567890 Australia/Eucla'", 10, "2004-08-24 23:55:23.1234567890 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInMap("TIMESTAMP '2004-08-24 23:55:23.12345678901 Australia/Eucla'", 11, "2004-08-24 23:55:23.12345678901 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInMap("TIMESTAMP '2004-08-24 23:55:23.123456789012 Australia/Eucla'", 12, "2004-08-24 23:55:23.123456789012 Australia/Eucla");
            return;
        }

        testSelectParametricTimestampWithTimeZoneInMap("TIMESTAMP '2004-08-24 23:55:23 Australia/Eucla'", 0, Timestamp.from(ZonedDateTime.parse("2004-08-24T23:55:23+08:45[Australia/Eucla]").toInstant()));
        testSelectParametricTimestampWithTimeZoneInMap("TIMESTAMP '2004-08-24 23:55:23.1 Australia/Eucla'", 1, Timestamp.from(ZonedDateTime.parse("2004-08-24T23:55:23.1+08:45[Australia/Eucla]").toInstant()));
        testSelectParametricTimestampWithTimeZoneInMap("TIMESTAMP '2004-08-24 23:55:23.12 Australia/Eucla'", 2, Timestamp.from(ZonedDateTime.parse("2004-08-24T23:55:23.12+08:45[Australia/Eucla]").toInstant()));
        testSelectParametricTimestampWithTimeZoneInMap("TIMESTAMP '2004-08-24 23:55:23.123 Australia/Eucla'", 3, Timestamp.from(ZonedDateTime.parse("2004-08-24T23:55:23.123+08:45[Australia/Eucla]").toInstant()));
        testSelectParametricTimestampWithTimeZoneInMap("TIMESTAMP '2004-08-24 23:55:23.1234 Australia/Eucla'", 4, Timestamp.from(ZonedDateTime.parse("2004-08-24T23:55:23.1234+08:45[Australia/Eucla]").toInstant()));
        testSelectParametricTimestampWithTimeZoneInMap("TIMESTAMP '2004-08-24 23:55:23.12345 Australia/Eucla'", 5, Timestamp.from(ZonedDateTime.parse("2004-08-24T23:55:23.12345+08:45[Australia/Eucla]").toInstant()));
        testSelectParametricTimestampWithTimeZoneInMap("TIMESTAMP '2004-08-24 23:55:23.123456 Australia/Eucla'", 6, Timestamp.from(ZonedDateTime.parse("2004-08-24T23:55:23.123456+08:45[Australia/Eucla]").toInstant()));
        testSelectParametricTimestampWithTimeZoneInMap("TIMESTAMP '2004-08-24 23:55:23.1234567 Australia/Eucla'", 7, Timestamp.from(ZonedDateTime.parse("2004-08-24T23:55:23.1234567+08:45[Australia/Eucla]").toInstant()));
        testSelectParametricTimestampWithTimeZoneInMap("TIMESTAMP '2004-08-24 23:55:23.12345678 Australia/Eucla'", 8, Timestamp.from(ZonedDateTime.parse("2004-08-24T23:55:23.12345678+08:45[Australia/Eucla]").toInstant()));
        testSelectParametricTimestampWithTimeZoneInMap("TIMESTAMP '2004-08-24 23:55:23.123456789 Australia/Eucla'", 9, Timestamp.from(ZonedDateTime.parse("2004-08-24T23:55:23.123456789+08:45[Australia/Eucla]").toInstant()));
        testSelectParametricTimestampWithTimeZoneInMap("TIMESTAMP '2004-08-24 23:55:23.1234567890 Australia/Eucla'", 10, Timestamp.from(ZonedDateTime.parse("2004-08-24T23:55:23.123456789+08:45[Australia/Eucla]").toInstant()));
        testSelectParametricTimestampWithTimeZoneInMap("TIMESTAMP '2004-08-24 23:55:23.12345678901 Australia/Eucla'", 11, Timestamp.from(ZonedDateTime.parse("2004-08-24T23:55:23.123456789+08:45[Australia/Eucla]").toInstant()));
        testSelectParametricTimestampWithTimeZoneInMap("TIMESTAMP '2004-08-24 23:55:23.123456789012 Australia/Eucla'", 12, Timestamp.from(ZonedDateTime.parse("2004-08-24T23:55:23.123456789+08:45[Australia/Eucla]").toInstant()));
    }

    private void testSelectParametricTimestampWithTimeZoneInMap(String elementExpression, int expectedPrecision, Object expectedValue)
    {
        String query = format("SELECT map_from_entries(ARRAY[('timestamp', %s)])", elementExpression);

        checkRepresentation(query, expectedValue, JAVA_OBJECT, TestJdbcCompatibility::getSingleElementFromMap);
        checkDescribeTimestampType(query, "map(varchar(9), %s)", expectedPrecision, true);
    }

    @Test
    public void testSelectParametricTimestampInArray()
    {
        if (!supportsParametricTimestamp()) {
            testSelectParametricTimestampInArray("TIMESTAMP '2004-08-24 23:55:23'", 0, "2004-08-24 23:55:23.000");
            testSelectParametricTimestampInArray("TIMESTAMP '2004-08-24 23:55:23.1'", 1, "2004-08-24 23:55:23.100");
            testSelectParametricTimestampInArray("TIMESTAMP '2004-08-24 23:55:23.12'", 2, "2004-08-24 23:55:23.120");
            testSelectParametricTimestampInArray("TIMESTAMP '2004-08-24 23:55:23.123'", 3, "2004-08-24 23:55:23.123");
            testSelectParametricTimestampInArray("TIMESTAMP '2004-08-24 23:55:23.1234'", 4, "2004-08-24 23:55:23.123");
            testSelectParametricTimestampInArray("TIMESTAMP '2004-08-24 23:55:23.12345'", 5, "2004-08-24 23:55:23.123");
            testSelectParametricTimestampInArray("TIMESTAMP '2004-08-24 23:55:23.123456'", 6, "2004-08-24 23:55:23.123");
            testSelectParametricTimestampInArray("TIMESTAMP '2004-08-24 23:55:23.1234567'", 7, "2004-08-24 23:55:23.123");
            testSelectParametricTimestampInArray("TIMESTAMP '2004-08-24 23:55:23.12345678'", 8, "2004-08-24 23:55:23.123");
            testSelectParametricTimestampInArray("TIMESTAMP '2004-08-24 23:55:23.123456789'", 9, "2004-08-24 23:55:23.123");
            testSelectParametricTimestampInArray("TIMESTAMP '2004-08-24 23:55:23.1234567890'", 10, "2004-08-24 23:55:23.123");
            testSelectParametricTimestampInArray("TIMESTAMP '2004-08-24 23:55:23.12345678901'", 11, "2004-08-24 23:55:23.123");
            testSelectParametricTimestampInArray("TIMESTAMP '2004-08-24 23:55:23.123456789012'", 12, "2004-08-24 23:55:23.123");
            return;
        }

        if (!supportsTimestampObjectRepresentationInCollections()) {
            testSelectParametricTimestampInArray("TIMESTAMP '2004-08-24 23:55:23'", 0, "2004-08-24 23:55:23");
            testSelectParametricTimestampInArray("TIMESTAMP '2004-08-24 23:55:23.1'", 1, "2004-08-24 23:55:23.1");
            testSelectParametricTimestampInArray("TIMESTAMP '2004-08-24 23:55:23.12'", 2, "2004-08-24 23:55:23.12");
            testSelectParametricTimestampInArray("TIMESTAMP '2004-08-24 23:55:23.123'", 3, "2004-08-24 23:55:23.123");
            testSelectParametricTimestampInArray("TIMESTAMP '2004-08-24 23:55:23.1234'", 4, "2004-08-24 23:55:23.1234");
            testSelectParametricTimestampInArray("TIMESTAMP '2004-08-24 23:55:23.12345'", 5, "2004-08-24 23:55:23.12345");
            testSelectParametricTimestampInArray("TIMESTAMP '2004-08-24 23:55:23.123456'", 6, "2004-08-24 23:55:23.123456");
            testSelectParametricTimestampInArray("TIMESTAMP '2004-08-24 23:55:23.1234567'", 7, "2004-08-24 23:55:23.1234567");
            testSelectParametricTimestampInArray("TIMESTAMP '2004-08-24 23:55:23.12345678'", 8, "2004-08-24 23:55:23.12345678");
            testSelectParametricTimestampInArray("TIMESTAMP '2004-08-24 23:55:23.123456789'", 9, "2004-08-24 23:55:23.123456789");
            testSelectParametricTimestampInArray("TIMESTAMP '2004-08-24 23:55:23.1234567890'", 10, "2004-08-24 23:55:23.1234567890");
            testSelectParametricTimestampInArray("TIMESTAMP '2004-08-24 23:55:23.12345678901'", 11, "2004-08-24 23:55:23.12345678901");
            testSelectParametricTimestampInArray("TIMESTAMP '2004-08-24 23:55:23.123456789012'", 12, "2004-08-24 23:55:23.123456789012");
            return;
        }

        testSelectParametricTimestampInArray("TIMESTAMP '2004-08-24 23:55:23'", 0, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23")));
        testSelectParametricTimestampInArray("TIMESTAMP '2004-08-24 23:55:23.1'", 1, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.1")));
        testSelectParametricTimestampInArray("TIMESTAMP '2004-08-24 23:55:23.12'", 2, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.12")));
        testSelectParametricTimestampInArray("TIMESTAMP '2004-08-24 23:55:23.123'", 3, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.123")));
        testSelectParametricTimestampInArray("TIMESTAMP '2004-08-24 23:55:23.1234'", 4, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.1234")));
        testSelectParametricTimestampInArray("TIMESTAMP '2004-08-24 23:55:23.12345'", 5, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.12345")));
        testSelectParametricTimestampInArray("TIMESTAMP '2004-08-24 23:55:23.123456'", 6, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.123456")));
        testSelectParametricTimestampInArray("TIMESTAMP '2004-08-24 23:55:23.1234567'", 7, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.1234567")));
        testSelectParametricTimestampInArray("TIMESTAMP '2004-08-24 23:55:23.12345678'", 8, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.12345678")));
        testSelectParametricTimestampInArray("TIMESTAMP '2004-08-24 23:55:23.123456789'", 9, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.123456789")));
        testSelectParametricTimestampInArray("TIMESTAMP '2004-08-24 23:55:23.1234567890'", 10, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.123456789")));
        testSelectParametricTimestampInArray("TIMESTAMP '2004-08-24 23:55:23.12345678901'", 11, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.123456789")));
        testSelectParametricTimestampInArray("TIMESTAMP '2004-08-24 23:55:23.123456789012'", 12, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.123456789")));
    }

    private void testSelectParametricTimestampInArray(String elementExpression, int expectedPrecision, Object expectedValue)
    {
        String query = format("SELECT ARRAY[%s]", elementExpression);
        checkRepresentation(query, expectedValue, ARRAY, TestJdbcCompatibility::getSingleElementFromArray);
        checkDescribeTimestampType(query, "array(%s)", expectedPrecision, false);
    }

    @Test
    public void testSelectParametricTimestampWithTimeZoneInArray()
    {
        if (hasBrokenParametricTimestampWithTimeZoneSupport()) {
            throw new SkipException("This version reports PARAMETRIC_DATETIME client capability but TIMESTAMP WITH TIME ZONE is not supported");
        }

        if (!supportsParametricTimestampWithTimeZone()) {
            testSelectParametricTimestampWithTimeZoneInArray("TIMESTAMP '2004-08-24 23:55:23 Australia/Eucla'", 0, "2004-08-24 23:55:23.000 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInArray("TIMESTAMP '2004-08-24 23:55:23.1 Australia/Eucla'", 1, "2004-08-24 23:55:23.100 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInArray("TIMESTAMP '2004-08-24 23:55:23.12 Australia/Eucla'", 2, "2004-08-24 23:55:23.120 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInArray("TIMESTAMP '2004-08-24 23:55:23.123 Australia/Eucla'", 3, "2004-08-24 23:55:23.123 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInArray("TIMESTAMP '2004-08-24 23:55:23.1234 Australia/Eucla'", 4, "2004-08-24 23:55:23.123 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInArray("TIMESTAMP '2004-08-24 23:55:23.12345 Australia/Eucla'", 5, "2004-08-24 23:55:23.123 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInArray("TIMESTAMP '2004-08-24 23:55:23.123456 Australia/Eucla'", 6, "2004-08-24 23:55:23.123 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInArray("TIMESTAMP '2004-08-24 23:55:23.1234567 Australia/Eucla'", 7, "2004-08-24 23:55:23.123 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInArray("TIMESTAMP '2004-08-24 23:55:23.12345678 Australia/Eucla'", 8, "2004-08-24 23:55:23.123 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInArray("TIMESTAMP '2004-08-24 23:55:23.123456789 Australia/Eucla'", 9, "2004-08-24 23:55:23.123 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInArray("TIMESTAMP '2004-08-24 23:55:23.1234567890 Australia/Eucla'", 10, "2004-08-24 23:55:23.123 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInArray("TIMESTAMP '2004-08-24 23:55:23.12345678901 Australia/Eucla'", 11, "2004-08-24 23:55:23.123 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInArray("TIMESTAMP '2004-08-24 23:55:23.123456789012 Australia/Eucla'", 12, "2004-08-24 23:55:23.123 Australia/Eucla");
            return;
        }

        if (!supportsTimestampObjectRepresentationInCollections()) {
            testSelectParametricTimestampWithTimeZoneInArray("TIMESTAMP '2004-08-24 23:55:23 Australia/Eucla'", 0, "2004-08-24 23:55:23 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInArray("TIMESTAMP '2004-08-24 23:55:23.1 Australia/Eucla'", 1, "2004-08-24 23:55:23.1 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInArray("TIMESTAMP '2004-08-24 23:55:23.12 Australia/Eucla'", 2, "2004-08-24 23:55:23.12 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInArray("TIMESTAMP '2004-08-24 23:55:23.123 Australia/Eucla'", 3, "2004-08-24 23:55:23.123 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInArray("TIMESTAMP '2004-08-24 23:55:23.1234 Australia/Eucla'", 4, "2004-08-24 23:55:23.1234 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInArray("TIMESTAMP '2004-08-24 23:55:23.12345 Australia/Eucla'", 5, "2004-08-24 23:55:23.12345 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInArray("TIMESTAMP '2004-08-24 23:55:23.123456 Australia/Eucla'", 6, "2004-08-24 23:55:23.123456 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInArray("TIMESTAMP '2004-08-24 23:55:23.1234567 Australia/Eucla'", 7, "2004-08-24 23:55:23.1234567 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInArray("TIMESTAMP '2004-08-24 23:55:23.12345678 Australia/Eucla'", 8, "2004-08-24 23:55:23.12345678 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInArray("TIMESTAMP '2004-08-24 23:55:23.123456789 Australia/Eucla'", 9, "2004-08-24 23:55:23.123456789 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInArray("TIMESTAMP '2004-08-24 23:55:23.1234567890 Australia/Eucla'", 10, "2004-08-24 23:55:23.1234567890 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInArray("TIMESTAMP '2004-08-24 23:55:23.12345678901 Australia/Eucla'", 11, "2004-08-24 23:55:23.12345678901 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInArray("TIMESTAMP '2004-08-24 23:55:23.123456789012 Australia/Eucla'", 12, "2004-08-24 23:55:23.123456789012 Australia/Eucla");
            return;
        }

        testSelectParametricTimestampWithTimeZoneInArray("TIMESTAMP '2004-08-24 23:55:23 Australia/Eucla'", 0, Timestamp.from(ZonedDateTime.parse("2004-08-24T23:55:23+08:45[Australia/Eucla]").toInstant()));
        testSelectParametricTimestampWithTimeZoneInArray("TIMESTAMP '2004-08-24 23:55:23.1 Australia/Eucla'", 1, Timestamp.from(ZonedDateTime.parse("2004-08-24T23:55:23.1+08:45[Australia/Eucla]").toInstant()));
        testSelectParametricTimestampWithTimeZoneInArray("TIMESTAMP '2004-08-24 23:55:23.12 Australia/Eucla'", 2, Timestamp.from(ZonedDateTime.parse("2004-08-24T23:55:23.12+08:45[Australia/Eucla]").toInstant()));
        testSelectParametricTimestampWithTimeZoneInArray("TIMESTAMP '2004-08-24 23:55:23.123 Australia/Eucla'", 3, Timestamp.from(ZonedDateTime.parse("2004-08-24T23:55:23.123+08:45[Australia/Eucla]").toInstant()));
        testSelectParametricTimestampWithTimeZoneInArray("TIMESTAMP '2004-08-24 23:55:23.1234 Australia/Eucla'", 4, Timestamp.from(ZonedDateTime.parse("2004-08-24T23:55:23.1234+08:45[Australia/Eucla]").toInstant()));
        testSelectParametricTimestampWithTimeZoneInArray("TIMESTAMP '2004-08-24 23:55:23.12345 Australia/Eucla'", 5, Timestamp.from(ZonedDateTime.parse("2004-08-24T23:55:23.12345+08:45[Australia/Eucla]").toInstant()));
        testSelectParametricTimestampWithTimeZoneInArray("TIMESTAMP '2004-08-24 23:55:23.123456 Australia/Eucla'", 6, Timestamp.from(ZonedDateTime.parse("2004-08-24T23:55:23.123456+08:45[Australia/Eucla]").toInstant()));
        testSelectParametricTimestampWithTimeZoneInArray("TIMESTAMP '2004-08-24 23:55:23.1234567 Australia/Eucla'", 7, Timestamp.from(ZonedDateTime.parse("2004-08-24T23:55:23.1234567+08:45[Australia/Eucla]").toInstant()));
        testSelectParametricTimestampWithTimeZoneInArray("TIMESTAMP '2004-08-24 23:55:23.12345678 Australia/Eucla'", 8, Timestamp.from(ZonedDateTime.parse("2004-08-24T23:55:23.12345678+08:45[Australia/Eucla]").toInstant()));
        testSelectParametricTimestampWithTimeZoneInArray("TIMESTAMP '2004-08-24 23:55:23.123456789 Australia/Eucla'", 9, Timestamp.from(ZonedDateTime.parse("2004-08-24T23:55:23.123456789+08:45[Australia/Eucla]").toInstant()));
        testSelectParametricTimestampWithTimeZoneInArray("TIMESTAMP '2004-08-24 23:55:23.1234567890 Australia/Eucla'", 10, Timestamp.from(ZonedDateTime.parse("2004-08-24T23:55:23.123456789+08:45[Australia/Eucla]").toInstant()));
        testSelectParametricTimestampWithTimeZoneInArray("TIMESTAMP '2004-08-24 23:55:23.12345678901 Australia/Eucla'", 11, Timestamp.from(ZonedDateTime.parse("2004-08-24T23:55:23.123456789+08:45[Australia/Eucla]").toInstant()));
        testSelectParametricTimestampWithTimeZoneInArray("TIMESTAMP '2004-08-24 23:55:23.123456789012 Australia/Eucla'", 12, Timestamp.from(ZonedDateTime.parse("2004-08-24T23:55:23.123456789+08:45[Australia/Eucla]").toInstant()));
    }

    private void testSelectParametricTimestampWithTimeZoneInArray(String elementExpression, int expectedPrecision, Object expectedValue)
    {
        String query = format("SELECT ARRAY[%s]", elementExpression);
        checkRepresentation(query, expectedValue, ARRAY, TestJdbcCompatibility::getSingleElementFromArray);
        checkDescribeTimestampType(query, "array(%s)", expectedPrecision, true);
    }

    @Test
    public void testSelectParametricTimestampInRow()
    {
        if (!supportsParametricTimestamp()) {
            testSelectParametricTimestampInRow("TIMESTAMP '2004-08-24 23:55:23'", 0, "2004-08-24 23:55:23.000");
            testSelectParametricTimestampInRow("TIMESTAMP '2004-08-24 23:55:23.1'", 1, "2004-08-24 23:55:23.100");
            testSelectParametricTimestampInRow("TIMESTAMP '2004-08-24 23:55:23.12'", 2, "2004-08-24 23:55:23.120");
            testSelectParametricTimestampInRow("TIMESTAMP '2004-08-24 23:55:23.123'", 3, "2004-08-24 23:55:23.123");
            testSelectParametricTimestampInRow("TIMESTAMP '2004-08-24 23:55:23.1234'", 4, "2004-08-24 23:55:23.123");
            testSelectParametricTimestampInRow("TIMESTAMP '2004-08-24 23:55:23.12345'", 5, "2004-08-24 23:55:23.123");
            testSelectParametricTimestampInRow("TIMESTAMP '2004-08-24 23:55:23.123456'", 6, "2004-08-24 23:55:23.123");
            testSelectParametricTimestampInRow("TIMESTAMP '2004-08-24 23:55:23.1234567'", 7, "2004-08-24 23:55:23.123");
            testSelectParametricTimestampInRow("TIMESTAMP '2004-08-24 23:55:23.12345678'", 8, "2004-08-24 23:55:23.123");
            testSelectParametricTimestampInRow("TIMESTAMP '2004-08-24 23:55:23.123456789'", 9, "2004-08-24 23:55:23.123");
            testSelectParametricTimestampInRow("TIMESTAMP '2004-08-24 23:55:23.1234567890'", 10, "2004-08-24 23:55:23.123");
            testSelectParametricTimestampInRow("TIMESTAMP '2004-08-24 23:55:23.12345678901'", 11, "2004-08-24 23:55:23.123");
            testSelectParametricTimestampInRow("TIMESTAMP '2004-08-24 23:55:23.123456789012'", 12, "2004-08-24 23:55:23.123");
            return;
        }

        if (!supportsTimestampObjectRepresentationInCollections()) {
            testSelectParametricTimestampInRow("TIMESTAMP '2004-08-24 23:55:23'", 0, "2004-08-24 23:55:23");
            testSelectParametricTimestampInRow("TIMESTAMP '2004-08-24 23:55:23.1'", 1, "2004-08-24 23:55:23.1");
            testSelectParametricTimestampInRow("TIMESTAMP '2004-08-24 23:55:23.12'", 2, "2004-08-24 23:55:23.12");
            testSelectParametricTimestampInRow("TIMESTAMP '2004-08-24 23:55:23.123'", 3, "2004-08-24 23:55:23.123");
            testSelectParametricTimestampInRow("TIMESTAMP '2004-08-24 23:55:23.1234'", 4, "2004-08-24 23:55:23.1234");
            testSelectParametricTimestampInRow("TIMESTAMP '2004-08-24 23:55:23.12345'", 5, "2004-08-24 23:55:23.12345");
            testSelectParametricTimestampInRow("TIMESTAMP '2004-08-24 23:55:23.123456'", 6, "2004-08-24 23:55:23.123456");
            testSelectParametricTimestampInRow("TIMESTAMP '2004-08-24 23:55:23.1234567'", 7, "2004-08-24 23:55:23.1234567");
            testSelectParametricTimestampInRow("TIMESTAMP '2004-08-24 23:55:23.12345678'", 8, "2004-08-24 23:55:23.12345678");
            testSelectParametricTimestampInRow("TIMESTAMP '2004-08-24 23:55:23.123456789'", 9, "2004-08-24 23:55:23.123456789");
            testSelectParametricTimestampInRow("TIMESTAMP '2004-08-24 23:55:23.1234567890'", 10, "2004-08-24 23:55:23.1234567890");
            testSelectParametricTimestampInRow("TIMESTAMP '2004-08-24 23:55:23.12345678901'", 11, "2004-08-24 23:55:23.12345678901");
            testSelectParametricTimestampInRow("TIMESTAMP '2004-08-24 23:55:23.123456789012'", 12, "2004-08-24 23:55:23.123456789012");
            return;
        }

        testSelectParametricTimestampInRow("TIMESTAMP '2004-08-24 23:55:23'", 0, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23")));
        testSelectParametricTimestampInRow("TIMESTAMP '2004-08-24 23:55:23.1'", 1, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.1")));
        testSelectParametricTimestampInRow("TIMESTAMP '2004-08-24 23:55:23.12'", 2, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.12")));
        testSelectParametricTimestampInRow("TIMESTAMP '2004-08-24 23:55:23.123'", 3, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.123")));
        testSelectParametricTimestampInRow("TIMESTAMP '2004-08-24 23:55:23.1234'", 4, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.1234")));
        testSelectParametricTimestampInRow("TIMESTAMP '2004-08-24 23:55:23.12345'", 5, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.12345")));
        testSelectParametricTimestampInRow("TIMESTAMP '2004-08-24 23:55:23.123456'", 6, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.123456")));
        testSelectParametricTimestampInRow("TIMESTAMP '2004-08-24 23:55:23.1234567'", 7, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.1234567")));
        testSelectParametricTimestampInRow("TIMESTAMP '2004-08-24 23:55:23.12345678'", 8, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.12345678")));
        testSelectParametricTimestampInRow("TIMESTAMP '2004-08-24 23:55:23.123456789'", 9, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.123456789")));
        testSelectParametricTimestampInRow("TIMESTAMP '2004-08-24 23:55:23.1234567890'", 10, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.123456789")));
        testSelectParametricTimestampInRow("TIMESTAMP '2004-08-24 23:55:23.12345678901'", 11, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.123456789")));
        testSelectParametricTimestampInRow("TIMESTAMP '2004-08-24 23:55:23.123456789012'", 12, Timestamp.valueOf(LocalDateTime.parse("2004-08-24T23:55:23.123456789")));
    }

    private void testSelectParametricTimestampInRow(String elementExpression, int precision, Object expectedValue)
    {
        String query = format("SELECT CAST(ROW(%s) AS row(timestamp timestamp(%d)))", elementExpression, precision);
        checkRepresentation(query, expectedValue, JAVA_OBJECT, TestJdbcCompatibility::getSingleElementFromRow);
        checkDescribeTimestampType(query, "row(timestamp %s)", precision, false);
    }

    @Test
    public void testSelectParametricTimestampWithTimeZoneInRow()
    {
        if (hasBrokenParametricTimestampWithTimeZoneSupport()) {
            throw new SkipException("This version reports PARAMETRIC_DATETIME client capability but TIMESTAMP WITH TIME ZONE is not supported");
        }

        if (!supportsParametricTimestampWithTimeZone()) {
            testSelectParametricTimestampWithTimeZoneInRow("TIMESTAMP '2004-08-24 23:55:23 Australia/Eucla'", 0, "2004-08-24 23:55:23.000 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInRow("TIMESTAMP '2004-08-24 23:55:23.1 Australia/Eucla'", 1, "2004-08-24 23:55:23.100 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInRow("TIMESTAMP '2004-08-24 23:55:23.12 Australia/Eucla'", 2, "2004-08-24 23:55:23.120 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInRow("TIMESTAMP '2004-08-24 23:55:23.123 Australia/Eucla'", 3, "2004-08-24 23:55:23.123 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInRow("TIMESTAMP '2004-08-24 23:55:23.1234 Australia/Eucla'", 4, "2004-08-24 23:55:23.123 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInRow("TIMESTAMP '2004-08-24 23:55:23.12345 Australia/Eucla'", 5, "2004-08-24 23:55:23.123 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInRow("TIMESTAMP '2004-08-24 23:55:23.123456 Australia/Eucla'", 6, "2004-08-24 23:55:23.123 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInRow("TIMESTAMP '2004-08-24 23:55:23.1234567 Australia/Eucla'", 7, "2004-08-24 23:55:23.123 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInRow("TIMESTAMP '2004-08-24 23:55:23.12345678 Australia/Eucla'", 8, "2004-08-24 23:55:23.123 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInRow("TIMESTAMP '2004-08-24 23:55:23.123456789 Australia/Eucla'", 9, "2004-08-24 23:55:23.123 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInRow("TIMESTAMP '2004-08-24 23:55:23.1234567890 Australia/Eucla'", 10, "2004-08-24 23:55:23.123 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInRow("TIMESTAMP '2004-08-24 23:55:23.12345678901 Australia/Eucla'", 11, "2004-08-24 23:55:23.123 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInRow("TIMESTAMP '2004-08-24 23:55:23.123456789012 Australia/Eucla'", 12, "2004-08-24 23:55:23.123 Australia/Eucla");
            return;
        }

        if (!supportsTimestampObjectRepresentationInCollections()) {
            testSelectParametricTimestampWithTimeZoneInRow("TIMESTAMP '2004-08-24 23:55:23 Australia/Eucla'", 0, "2004-08-24 23:55:23 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInRow("TIMESTAMP '2004-08-24 23:55:23.1 Australia/Eucla'", 1, "2004-08-24 23:55:23.1 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInRow("TIMESTAMP '2004-08-24 23:55:23.12 Australia/Eucla'", 2, "2004-08-24 23:55:23.12 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInRow("TIMESTAMP '2004-08-24 23:55:23.123 Australia/Eucla'", 3, "2004-08-24 23:55:23.123 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInRow("TIMESTAMP '2004-08-24 23:55:23.1234 Australia/Eucla'", 4, "2004-08-24 23:55:23.1234 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInRow("TIMESTAMP '2004-08-24 23:55:23.12345 Australia/Eucla'", 5, "2004-08-24 23:55:23.12345 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInRow("TIMESTAMP '2004-08-24 23:55:23.123456 Australia/Eucla'", 6, "2004-08-24 23:55:23.123456 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInRow("TIMESTAMP '2004-08-24 23:55:23.1234567 Australia/Eucla'", 7, "2004-08-24 23:55:23.1234567 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInRow("TIMESTAMP '2004-08-24 23:55:23.12345678 Australia/Eucla'", 8, "2004-08-24 23:55:23.12345678 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInRow("TIMESTAMP '2004-08-24 23:55:23.123456789 Australia/Eucla'", 9, "2004-08-24 23:55:23.123456789 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInRow("TIMESTAMP '2004-08-24 23:55:23.1234567890 Australia/Eucla'", 10, "2004-08-24 23:55:23.1234567890 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInRow("TIMESTAMP '2004-08-24 23:55:23.12345678901 Australia/Eucla'", 11, "2004-08-24 23:55:23.12345678901 Australia/Eucla");
            testSelectParametricTimestampWithTimeZoneInRow("TIMESTAMP '2004-08-24 23:55:23.123456789012 Australia/Eucla'", 12, "2004-08-24 23:55:23.123456789012 Australia/Eucla");
            return;
        }

        testSelectParametricTimestampWithTimeZoneInRow("TIMESTAMP '2004-08-24 23:55:23 Australia/Eucla'", 0, Timestamp.from(ZonedDateTime.parse("2004-08-24T23:55:23+08:45[Australia/Eucla]").toInstant()));
        testSelectParametricTimestampWithTimeZoneInRow("TIMESTAMP '2004-08-24 23:55:23.1 Australia/Eucla'", 1, Timestamp.from(ZonedDateTime.parse("2004-08-24T23:55:23.1+08:45[Australia/Eucla]").toInstant()));
        testSelectParametricTimestampWithTimeZoneInRow("TIMESTAMP '2004-08-24 23:55:23.12 Australia/Eucla'", 2, Timestamp.from(ZonedDateTime.parse("2004-08-24T23:55:23.12+08:45[Australia/Eucla]").toInstant()));
        testSelectParametricTimestampWithTimeZoneInRow("TIMESTAMP '2004-08-24 23:55:23.123 Australia/Eucla'", 3, Timestamp.from(ZonedDateTime.parse("2004-08-24T23:55:23.123+08:45[Australia/Eucla]").toInstant()));
        testSelectParametricTimestampWithTimeZoneInRow("TIMESTAMP '2004-08-24 23:55:23.1234 Australia/Eucla'", 4, Timestamp.from(ZonedDateTime.parse("2004-08-24T23:55:23.1234+08:45[Australia/Eucla]").toInstant()));
        testSelectParametricTimestampWithTimeZoneInRow("TIMESTAMP '2004-08-24 23:55:23.12345 Australia/Eucla'", 5, Timestamp.from(ZonedDateTime.parse("2004-08-24T23:55:23.12345+08:45[Australia/Eucla]").toInstant()));
        testSelectParametricTimestampWithTimeZoneInRow("TIMESTAMP '2004-08-24 23:55:23.123456 Australia/Eucla'", 6, Timestamp.from(ZonedDateTime.parse("2004-08-24T23:55:23.123456+08:45[Australia/Eucla]").toInstant()));
        testSelectParametricTimestampWithTimeZoneInRow("TIMESTAMP '2004-08-24 23:55:23.1234567 Australia/Eucla'", 7, Timestamp.from(ZonedDateTime.parse("2004-08-24T23:55:23.1234567+08:45[Australia/Eucla]").toInstant()));
        testSelectParametricTimestampWithTimeZoneInRow("TIMESTAMP '2004-08-24 23:55:23.12345678 Australia/Eucla'", 8, Timestamp.from(ZonedDateTime.parse("2004-08-24T23:55:23.12345678+08:45[Australia/Eucla]").toInstant()));
        testSelectParametricTimestampWithTimeZoneInRow("TIMESTAMP '2004-08-24 23:55:23.123456789 Australia/Eucla'", 9, Timestamp.from(ZonedDateTime.parse("2004-08-24T23:55:23.123456789+08:45[Australia/Eucla]").toInstant()));
        testSelectParametricTimestampWithTimeZoneInRow("TIMESTAMP '2004-08-24 23:55:23.1234567890 Australia/Eucla'", 10, Timestamp.from(ZonedDateTime.parse("2004-08-24T23:55:23.123456789+08:45[Australia/Eucla]").toInstant()));
        testSelectParametricTimestampWithTimeZoneInRow("TIMESTAMP '2004-08-24 23:55:23.12345678901 Australia/Eucla'", 11, Timestamp.from(ZonedDateTime.parse("2004-08-24T23:55:23.123456789+08:45[Australia/Eucla]").toInstant()));
        testSelectParametricTimestampWithTimeZoneInRow("TIMESTAMP '2004-08-24 23:55:23.123456789012 Australia/Eucla'", 12, Timestamp.from(ZonedDateTime.parse("2004-08-24T23:55:23.123456789+08:45[Australia/Eucla]").toInstant()));
    }

    private void testSelectParametricTimestampWithTimeZoneInRow(String elementExpression, int precision, Object expectedValue)
    {
        String query = format("SELECT CAST(ROW(%s) AS row(timestamp timestamp(%d) with time zone))", elementExpression, precision);
        checkRepresentation(query, expectedValue, JAVA_OBJECT, TestJdbcCompatibility::getSingleElementFromRow);
        checkDescribeTimestampType(query, "row(timestamp %s)", precision, true);
    }

    @Test
    public void testSelectMongoObjectId()
    {
        String query = "SELECT ObjectId('55b151633864d6438c61a9ce') AS objectId";
        checkRepresentation(query, JAVA_OBJECT, (resultSet, columnIndex) -> {
            assertThat(resultSet.getObject(columnIndex)).isEqualTo(new byte[] {85, -79, 81, 99, 56, 100, -42, 67, -116, 97, -87, -50});
        });
    }

    @Test
    public void testSelectRealCastToDecimal()
    {
        String query = "SELECT CAST(col as DECIMAL(30, 2)) FROM (VALUES (real '99.01')) AS t (col)";
        checkRepresentation(query, new BigDecimal("99.01"), Types.DECIMAL, ResultSet::getBigDecimal);
    }

    @Test
    public void testSelectArray()
    {
        String query = "SELECT ARRAY['Trino', 'is', 'awesome']";
        checkRepresentation(query, ARRAY, (rs, column) -> assertThat(rs.getArray(column).getArray()).isEqualTo(new Object[] {"Trino", "is", "awesome"}));
    }

    @Test
    public void testSelectMultiMap()
    {
        String query = "SELECT multimap_from_entries(ARRAY[(1, 'x'), (2, 'y'), (1, 'z')])";
        checkRepresentation(query, JAVA_OBJECT, (rs, column) -> {
            Map<Integer, Object> values = (Map<Integer, Object>) rs.getObject(column);
            assertThat(values).containsEntry(1, ImmutableList.of("x", "z"));
            assertThat(values).containsEntry(2, ImmutableList.of("y"));
        });
    }

    @Test
    public void testSelectRow()
    {
        String query = "SELECT CAST(ROW(1, 2e0) AS ROW(x BIGINT, y DOUBLE))";
        checkRepresentation(query, JAVA_OBJECT, (rs, column) -> {
            Map<String, Object> values = getRowAsMap(rs, column);

            assertThat(values).containsEntry("x", 1L);
            assertThat(values).containsEntry("y", 2e0);
        });
    }

    @Test
    public void testSelectJson()
    {
        String query = "SELECT json_parse('[{\"1\":\"value\"}, 2, 3]')";
        checkRepresentation(query, JAVA_OBJECT, (rs, column) -> {
            assertThat(rs.getObject(column)).isEqualTo("[{\"1\":\"value\"},2,3]");
        });
    }

    private <T> void checkRepresentation(String query, T expectedValue, int expectedType, ResultSetMapper<T> extractValue)
    {
        try (Connection connection = getConnection();
                PreparedStatement statement = connection.prepareStatement(query);
                ResultSet rs = statement.executeQuery()) {
            assertThat(rs.next()).isTrue();
            assertThat(extractValue.read(rs, 1)).isEqualTo(expectedValue);
            assertThat(rs.getMetaData().getColumnType(1)).isEqualTo(expectedType);
            assertThat(rs.next()).isFalse();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void checkRepresentation(String query, int expectedType, ResultSetAssertion extractValue)
    {
        try (Connection connection = getConnection();
                PreparedStatement statement = connection.prepareStatement(query);
                ResultSet rs = statement.executeQuery()) {
            assertThat(rs.next()).isTrue();
            extractValue.check(rs, 1);
            assertThat(rs.getMetaData().getColumnType(1)).isEqualTo(expectedType);
            assertThat(rs.next()).isFalse();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void checkDescribeTimestampType(String query, String expectedTypePattern, int precision, boolean withTimeZone)
    {
        assertDescribeType(query, format(expectedTypePattern, describeTimestampType(precision, true, withTimeZone)), true);
        assertDescribeType(query, format(expectedTypePattern, describeTimestampType(precision, false, withTimeZone)), false);
        assertDescribeOutputType(query, format(expectedTypePattern, describeTimestampType(precision, true, withTimeZone)), true);
        assertDescribeOutputType(query, format(expectedTypePattern, describeTimestampType(precision, false, withTimeZone)), false);
    }

    private void assertDescribeType(String query, String expectedType, boolean omitPrecission)
    {
        if (omitPrecission && !supportsSessionPropertiesViaConnectionUri()) {
            // Driver does not support setting session properties
            return;
        }

        useConnection(omitDateTimeTypePrecision(omitPrecission), connection -> {
            try {
                connection.prepareStatement(format("SELECT 1 FROM (%s AS timestamp) WHERE timestamp = ?", query));

                try (ResultSet resultSet = connection.prepareStatement("DESCRIBE INPUT statement1").executeQuery()) {
                    assertThat(resultSet.next()).isTrue();
                    assertThat(resultSet.getString(2)).isEqualTo(expectedType);
                }
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void assertDescribeOutputType(String query, String expectedType, boolean omitPrecision)
    {
        if (omitPrecision && !supportsSessionPropertiesViaConnectionUri()) {
            // Driver does not support setting session properties
            return;
        }

        useConnection(omitDateTimeTypePrecision(omitPrecision), connection -> {
            try {
                connection.prepareStatement(query);

                try (ResultSet resultSet = connection.prepareStatement("DESCRIBE OUTPUT statement1").executeQuery()) {
                    assertThat(resultSet.next()).isTrue();
                    assertThat(resultSet.getString(5)).isEqualTo(expectedType);
                }
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static String describeTimestampType(int precision, boolean ommitPrecision, boolean withTimezone)
    {
        if (ommitPrecision && precision == TIMESTAMP_DEFAULT_PRECISION) {
            if (withTimezone) {
                return "timestamp with time zone";
            }

            return "timestamp";
        }

        if (withTimezone) {
            return format("timestamp(%d) with time zone", precision);
        }

        return format("timestamp(%d)", precision);
    }

    private static Object getSingleElementFromArray(ResultSet resultSet, int columnIndex)
            throws SQLException
    {
        Array array = resultSet.getArray(columnIndex);
        return ((Object[]) array.getArray(1, 1))[0];
    }

    private static Object getSingleElementFromRow(ResultSet resultSet, int columnIndex)
            throws SQLException
    {
        return getRowAsMap(resultSet, columnIndex).get("timestamp");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> getRowAsMap(ResultSet resultSet, int columnIndex)
            throws SQLException
    {
        Object value = resultSet.getObject(columnIndex);
        if (value != null && !(value instanceof Map)) {
            value = resultSet.getObject(columnIndex, Map.class);
        }
        return (Map<String, Object>) value;
    }

    private static Object getSingleElementFromMap(ResultSet resultSet, int columnIndex)
            throws SQLException
    {
        Map<?, ?> map = (Map<?, ?>) resultSet.getObject(columnIndex);
        return map.get("timestamp");
    }

    private Connection getConnection()
            throws SQLException
    {
        return getConnection(ImmutableMap.of());
    }

    private Connection getConnection(Map<String, String> params)
            throws SQLException
    {
        Properties properties = new Properties();
        properties.putAll(params);
        properties.put("user", "test");
        properties.put("password", "");

        return DriverManager.getConnection(serverUrl, properties);
    }

    private void useConnection(Map<String, String> connectionParameters, Consumer<Connection> consumer)
    {
        try (Connection conn = getConnection(connectionParameters)) {
            consumer.accept(conn);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Map<String, String> omitDateTimeTypePrecision(boolean omit)
    {
        if (!omit) {
            return ImmutableMap.of();
        }

        return ImmutableMap.of("sessionProperties", "omit_datetime_type_precision:true");
    }

    @FunctionalInterface
    interface ResultSetMapper<T>
    {
        T read(ResultSet resultSet, int columnIndex)
                throws SQLException;
    }

    @FunctionalInterface
    interface ResultSetAssertion
    {
        void check(ResultSet resultSet, int columnIndex)
                throws SQLException;
    }
}
