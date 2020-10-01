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
package io.prestosql;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logging;
import io.prestosql.plugin.mongodb.MongoPlugin;
import io.prestosql.server.testing.TestingPrestoServer;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Consumer;

import static com.google.common.base.Strings.repeat;
import static com.google.common.base.Verify.verify;
import static io.prestosql.JdbcDriverCapabilities.driverVersion;
import static io.prestosql.JdbcDriverCapabilities.hasBrokenParametricTimestampWithTimeZoneSupport;
import static io.prestosql.JdbcDriverCapabilities.supportsParametricTimestamp;
import static io.prestosql.JdbcDriverCapabilities.supportsParametricTimestampWithTimeZone;
import static io.prestosql.JdbcDriverCapabilities.supportsSessionPropertiesViaConnectionUri;
import static io.prestosql.JdbcDriverCapabilities.testedVersion;
import static java.lang.Integer.min;
import static java.lang.String.format;
import static java.sql.Types.ARRAY;
import static java.sql.Types.JAVA_OBJECT;
import static java.sql.Types.TIMESTAMP;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The main purpose of this class is to test cases when current server implementation breaks older JDBC clients
 * to ensure that current implementation is backward-compatible.
 *
 * {@link TestJdbcResultSetCompatibilityOldDriver} in that regard is responsible for testing forward compatibility
 * as it's using old test code and old JDBC client against current server implementation.
 *
 * This test in turn is run using an old JDBC client against current server implementation.
 */
public class TestJdbcCompatibility
{
    private static final Optional<Integer> VERSION_UNDER_TEST = testedVersion();
    private static final int TIMESTAMP_DEFAULT_PRECISION = 3;
    private static final int TIMESTAMP_JDBC_MAX_PRECISION = 9; // nanoseconds
    private static final int TIMESTAMP_MAX_PRECISION = 12; // picoseconds
    private static final String TESTED_TZ = "Australia/Eucla"; // GMT+8:45

    private TestingPrestoServer server;
    private String serverUrl;

    @Test
    public void ensureProperDriverVersionLoaded()
    {
        if (VERSION_UNDER_TEST.isEmpty()) {
            throw new SkipException("Information about JDBC version under test is missing");
        }

        assertThat(driverVersion())
                .isEqualTo(VERSION_UNDER_TEST.get());
    }

    @BeforeClass
    public void setup()
    {
        Logging.initialize();

        server = TestingPrestoServer.builder()
                .build();

        server.installPlugin(new MongoPlugin());

        serverUrl = format("jdbc:presto://%s", server.getAddress());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        server.close();
    }

    @Test
    public void ensureUsingProperClientVersion()
            throws ClassNotFoundException
    {
        if (VERSION_UNDER_TEST.isEmpty()) {
            throw new SkipException("Information about JDBC version under test is missing");
        }

        assertThat(Class.forName("io.prestosql.jdbc.$internal.client.StatementClientFactory").getPackage().getImplementationVersion())
                .isEqualTo(VERSION_UNDER_TEST.get().toString());
    }

    @Test
    public void testLongPreparedStatement()
            throws Exception
    {
        String sql = format("SELECT '%s' = '%s'", repeat("x", 100_000), repeat("y", 100_000));

        try (ResultSet rs = runQuery(sql)) {
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

        String query = format("SELECT timestamp '2012-10-31 01:00 %s'", TESTED_TZ);
        checkRepresentation(query, Timestamp.valueOf("2012-10-30 10:15:00.000"), TIMESTAMP, ResultSet::getTimestamp);
    }

    @Test
    public void testSelectParametricTimestamp()
    {
        String timestamp = "2421-01-19 15:55:23.393456298901";

        for (int precision = 0; precision <= TIMESTAMP_MAX_PRECISION; precision++) {
            String query = format("SELECT cast(TIMESTAMP '%s' as TIMESTAMP(%d))", timestamp, precision);
            Timestamp expectedTimestamp = roundedSqlTimestamp(timestamp, precision, TIMESTAMP_JDBC_MAX_PRECISION, false);

            int currentPrecision = precision;

            checkRepresentation(query, TIMESTAMP, (resultSet, columnIndex) -> {
                assertThat(resultSet.getTimestamp(columnIndex)).isEqualTo(expectedTimestamp);
                assertThat(resultSet.getObject(columnIndex)).isEqualTo(expectedTimestamp);
                assertThat(resultSet.getString(columnIndex)).isEqualTo(roundedTimestamp(timestamp, currentPrecision, TIMESTAMP_MAX_PRECISION, false));
            });
        }
    }

    @Test
    public void testSelectParametricTimestampWithTimeZone()
    {
        if (hasBrokenParametricTimestampWithTimeZoneSupport()) {
            throw new SkipException("This version reports PARAMETRIC_DATETIME client capability but TIMESTAMP WITH TIME ZONE is not supported");
        }

        String timestamp = "2020-07-16 15:55:23.383345789012";
        String localTimestamp = "2020-07-16 02:10:23.383345789012";

        for (int precision = 0; precision <= TIMESTAMP_MAX_PRECISION; precision++) {
            String query = format("SELECT cast(TIMESTAMP '%s %s' as TIMESTAMP(%d) WITH TIME ZONE)", timestamp, TESTED_TZ, precision);
            String expectedTimestamp = roundedTimestamp(timestamp, precision, TIMESTAMP_MAX_PRECISION, true);

            int currentPrecision = precision;

            checkRepresentation(query, TIMESTAMP, (resultSet, columnIndex) -> {
                assertThat(resultSet.getString(columnIndex)).isEqualTo(expectedTimestamp + " " + TESTED_TZ);
                assertThat(resultSet.getTimestamp(columnIndex)).isEqualTo(roundedSqlTimestamp(localTimestamp, currentPrecision, TIMESTAMP_JDBC_MAX_PRECISION, true));
                assertThat(resultSet.getObject(columnIndex)).isEqualTo(roundedSqlTimestamp(localTimestamp, currentPrecision, TIMESTAMP_JDBC_MAX_PRECISION, true));
            });

            checkDescribeTimestampType(query, "%s", precision, true);
        }
    }

    @Test
    public void testSelectParametricTimestampInMap()
    {
        String timestamp = "2004-08-24 23:55:23.383345789012";

        for (int precision = 0; precision <= TIMESTAMP_MAX_PRECISION; precision++) {
            String query = format("SELECT map_from_entries(ARRAY[('timestamp', cast(TIMESTAMP '%s' as TIMESTAMP(%d)))])", timestamp, precision);
            String expectedTimestamp = roundedTimestamp(timestamp, precision, TIMESTAMP_MAX_PRECISION, false);

            checkRepresentation(query, expectedTimestamp, JAVA_OBJECT, TestJdbcCompatibility::getSingleElementFromMap);
            checkDescribeTimestampType(query, "map(varchar(9), %s)", precision, false);
        }
    }

    @Test
    public void testSelectParametricTimestampWithTimezoneInMap()
    {
        if (hasBrokenParametricTimestampWithTimeZoneSupport()) {
            throw new SkipException("This version reports PARAMETRIC_DATETIME client capability but TIMESTAMP WITH TIME ZONE is not supported");
        }

        String timestamp = "2004-08-24 23:55:23.383345789012";

        for (int precision = 0; precision <= TIMESTAMP_MAX_PRECISION; precision++) {
            String query = format("SELECT map_from_entries(ARRAY[('timestamp', cast(TIMESTAMP '%s %s' as TIMESTAMP(%d) WITH TIME ZONE))])", timestamp, TESTED_TZ, precision);
            String expectedTimestamp = roundedTimestamp(timestamp, precision, TIMESTAMP_MAX_PRECISION, true);

            checkRepresentation(query, expectedTimestamp + " " + TESTED_TZ, JAVA_OBJECT, TestJdbcCompatibility::getSingleElementFromMap);
            checkDescribeTimestampType(query, "map(varchar(9), %s)", precision, true);
        }
    }

    @Test
    public void testSelectParametricTimestampInArray()
    {
        String timestamp = "2004-08-24 23:55:23.383345789012";

        for (int precision = 0; precision <= TIMESTAMP_MAX_PRECISION; precision++) {
            String query = format("SELECT ARRAY[cast(TIMESTAMP '%s' as TIMESTAMP(%d))]", timestamp, precision);
            String expectedTimestamp = roundedTimestamp(timestamp, precision, TIMESTAMP_MAX_PRECISION, false);

            checkRepresentation(query, expectedTimestamp, ARRAY, TestJdbcCompatibility::getSingleElementFromArray);
            checkDescribeTimestampType(query, "array(%s)", precision, false);
        }
    }

    @Test
    public void testSelectParametricTimestampWithTimeZoneInArray()
    {
        if (hasBrokenParametricTimestampWithTimeZoneSupport()) {
            throw new SkipException("This version reports PARAMETRIC_DATETIME client capability but TIMESTAMP WITH TIME ZONE is not supported");
        }

        String timestamp = "1984-08-24 23:55:23.383345789012";

        for (int precision = 0; precision <= TIMESTAMP_MAX_PRECISION; precision++) {
            String query = format("SELECT ARRAY[cast(TIMESTAMP '%s %s' as TIMESTAMP(%d) WITH TIME ZONE)]", timestamp, TESTED_TZ, precision);
            String expectedTimestamp = roundedTimestamp(timestamp, precision, TIMESTAMP_MAX_PRECISION, true);

            checkRepresentation(query, expectedTimestamp + " " + TESTED_TZ, ARRAY, TestJdbcCompatibility::getSingleElementFromArray);
            checkDescribeTimestampType(query, "array(%s)", precision, true);
        }
    }

    @Test
    public void testSelectParametricTimestampInRow()
    {
        String timestamp = "2014-08-24 23:55:23.383345789012";

        for (int precision = 0; precision <= TIMESTAMP_MAX_PRECISION; precision++) {
            String query = format("SELECT CAST(ROW(TIMESTAMP '%s') AS ROW(timestamp TIMESTAMP(%d)))", timestamp, precision);
            String expectedTimestamp = roundedTimestamp(timestamp, precision, TIMESTAMP_MAX_PRECISION, false);

            checkRepresentation(query, expectedTimestamp, JAVA_OBJECT, TestJdbcCompatibility::getSingleElementFromRow);
            checkDescribeTimestampType(query, "row(timestamp %s)", precision, false);
        }
    }

    @Test
    public void testSelectParametricTimestampWithTimeZoneInRow()
    {
        if (hasBrokenParametricTimestampWithTimeZoneSupport()) {
            throw new SkipException("This version reports PARAMETRIC_DATETIME client capability but TIMESTAMP WITH TIME ZONE is not supported");
        }

        String timestamp = "2004-08-24 23:55:23.383345789012";

        for (int precision = 0; precision <= TIMESTAMP_MAX_PRECISION; precision++) {
            String query = format("SELECT CAST(ROW(TIMESTAMP '%s %s') AS ROW(timestamp TIMESTAMP(%d) WITH TIME ZONE))", timestamp, TESTED_TZ, precision);
            String expectedTimestamp = roundedTimestamp(timestamp, precision, TIMESTAMP_MAX_PRECISION, true);

            checkRepresentation(query, expectedTimestamp + " " + TESTED_TZ, JAVA_OBJECT, TestJdbcCompatibility::getSingleElementFromRow);
            checkDescribeTimestampType(query, "row(timestamp %s)", precision, true);
        }
    }

    @Test
    public void testSelectMongoObjectId()
    {
        String query = "SELECT ObjectId('55b151633864d6438c61a9ce') AS objectId";
        checkRepresentation(query, JAVA_OBJECT, (resultSet, columnIndex) -> {
            assertThat(resultSet.getObject(columnIndex)).isEqualTo(new byte[]{85, -79, 81, 99, 56, 100, -42, 67, -116, 97, -87, -50});
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
        String query = "SELECT ARRAY['presto', 'is', 'awesome']";
        checkRepresentation(query, ARRAY, (rs, column) -> assertThat(rs.getArray(column).getArray()).isEqualTo(new Object[]{"presto", "is", "awesome"}));
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
            Map<String, Object> values = (Map<String, Object>) rs.getObject(column);

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
        try (ResultSet rs = runQuery(query)) {
            assertThat(rs.next()).isTrue();
            assertThat(extractValue.read(rs, 1)).isEqualTo(expectedValue);
            assertThat(rs.getMetaData().getColumnType(1)).isEqualTo(expectedType);
            assertThat(rs.next()).isFalse();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void checkRepresentation(String query, int expectedType, ResultSetAssertion extractValue)
    {
        try (ResultSet rs = runQuery(query)) {
            assertThat(rs.next()).isTrue();
            extractValue.check(rs, 1);
            assertThat(rs.getMetaData().getColumnType(1)).isEqualTo(expectedType);
            assertThat(rs.next()).isFalse();
        }
        catch (Exception e) {
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

    private static Timestamp roundedSqlTimestamp(String timestamp, int precision, int maxPrecision, boolean withTz)
    {
        return Timestamp.valueOf(roundedTimestamp(timestamp, precision, maxPrecision, withTz));
    }

    private static Object getSingleElementFromArray(ResultSet resultSet, int columnIndex)
            throws SQLException
    {
        Array array = resultSet.getArray(columnIndex);
        return ((Object[]) array.getArray(1, 1))[0];
    }

    @SuppressWarnings("UncheckedCast")
    private static Object getSingleElementFromRow(ResultSet resultSet, int columnIndex)
            throws SQLException
    {
        return ((Map<String, Object>) resultSet.getObject(columnIndex)).get("timestamp");
    }

    private static Object getSingleElementFromMap(ResultSet resultSet, int columnIndex)
            throws SQLException
    {
        Map map = (Map<String, Object>) resultSet.getObject(columnIndex);
        return map.get("timestamp");
    }

    private static String roundedTimestamp(String timestamp, int precision, int maxPrecision, boolean withTz)
    {
        List<String> parts = Splitter.on('.').splitToList(timestamp);
        verify(parts.size() == 2);

        BigDecimal fractional = new BigDecimal(format("0.%s", parts.get(1)));

        int supportedPrecision = determineSupportedPrecision(precision, maxPrecision, withTz);

        if (supportedPrecision == 0) {
            return parts.get(0);
        }

        int timestampPrecision = min(precision, maxPrecision);

        BigDecimal scaledFractional = fractional
                .setScale(timestampPrecision, RoundingMode.HALF_EVEN)
                .setScale(supportedPrecision, RoundingMode.HALF_EVEN);

        return format("%s.%s", parts.get(0), scaledFractional.toString().substring(2));
    }

    private static int determineSupportedPrecision(int precision, int maxPrecision, boolean withTz)
    {
        if (!withTz && supportsParametricTimestamp()) {
            return min(precision, maxPrecision);
        }

        if (withTz && supportsParametricTimestampWithTimeZone()) {
            return min(precision, maxPrecision);
        }

        // Before parametric timestamps were introduced, timestamp had precision = 3
        return TIMESTAMP_DEFAULT_PRECISION;
    }

    private ResultSet runQuery(String query)
    {
        return runQuery(query, ImmutableMap.of());
    }

    private ResultSet runQuery(String query, Map<String, String> params)
    {
        Properties properties = new Properties();
        properties.putAll(params);
        properties.put("user", "test");
        properties.put("password", "");

        try (Connection connection = DriverManager.getConnection(serverUrl, properties); PreparedStatement stmt = connection.prepareStatement(query)) {
            return stmt.executeQuery();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void useConnection(Map<String, String> connectionParameters, Consumer<Connection> consumer)
    {
        Properties properties = new Properties();
        properties.putAll(connectionParameters);
        properties.put("user", "test");
        properties.put("password", "");

        try (Connection conn = DriverManager.getConnection(serverUrl, properties)) {
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
