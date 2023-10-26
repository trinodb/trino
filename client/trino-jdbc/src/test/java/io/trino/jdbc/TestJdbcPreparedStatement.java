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
import com.google.common.collect.Iterables;
import io.airlift.log.Logging;
import io.trino.client.ClientTypeSignature;
import io.trino.client.ClientTypeSignatureParameter;
import io.trino.plugin.blackhole.BlackHolePlugin;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.server.testing.TestingTrinoServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.stream.LongStream;

import static com.google.common.base.Strings.repeat;
import static com.google.common.base.Verify.verify;
import static com.google.common.primitives.Ints.asList;
import static io.trino.client.ClientTypeSignature.VARCHAR_UNBOUNDED_LENGTH;
import static io.trino.jdbc.BaseTestJdbcResultSet.toSqlTime;
import static io.trino.jdbc.TestingJdbcUtils.list;
import static io.trino.jdbc.TestingJdbcUtils.readRows;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.sql.ParameterMetaData.parameterModeUnknown;
import static java.sql.ParameterMetaData.parameterNullableUnknown;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@TestInstance(PER_CLASS)
public class TestJdbcPreparedStatement
{
    private static final int HEADER_SIZE_LIMIT = 16 * 1024;

    private TestingTrinoServer server;

    @BeforeAll
    public void setup()
            throws Exception
    {
        Logging.initialize();
        server = TestingTrinoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .put("http-server.max-request-header-size", format("%sB", HEADER_SIZE_LIMIT))
                        .put("http-server.max-response-header-size", format("%sB", HEADER_SIZE_LIMIT))
                        .buildOrThrow())
                .build();
        server.installPlugin(new BlackHolePlugin());
        server.installPlugin(new MemoryPlugin());
        server.createCatalog("blackhole", "blackhole");
        server.createCatalog("memory", "memory");
        server.waitForNodeRefresh(Duration.ofSeconds(10));

        try (Connection connection = createConnection(false);
                Statement statement = connection.createStatement()) {
            statement.executeUpdate("CREATE SCHEMA blackhole.blackhole");
        }
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        server.close();
        server = null;
    }

    @Test
    public void testExecuteQuery()
            throws Exception
    {
        testExecuteQuery(false);
        testExecuteQuery(true);
    }

    private void testExecuteQuery(boolean useLegacyPreparedStatements)
            throws Exception
    {
        try (Connection connection = createConnection(useLegacyPreparedStatements);
                PreparedStatement statement = connection.prepareStatement("SELECT ?, ?")) {
            statement.setInt(1, 123);
            statement.setString(2, "hello");

            try (ResultSet rs = statement.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 123);
                assertEquals(rs.getString(2), "hello");
                assertFalse(rs.next());
            }

            assertTrue(statement.execute());
            try (ResultSet rs = statement.getResultSet()) {
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 123);
                assertEquals(rs.getString(2), "hello");
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testGetMetadata()
            throws Exception
    {
        testGetMetadata(true);
        testGetMetadata(false);
    }

    private void testGetMetadata(boolean useLegacyPreparedStatements)
            throws Exception
    {
        try (Connection connection = createConnection("blackhole", "blackhole", useLegacyPreparedStatements)) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE test_get_metadata (" +
                        "c_boolean boolean, " +
                        "c_decimal decimal, " +
                        "c_decimal_2 decimal(10,3)," +
                        "c_varchar varchar, " +
                        "c_varchar_2 varchar(10), " +
                        "c_row row(x integer, y array(integer)), " +
                        "c_array array(integer), " +
                        "c_map map(integer, integer))");
            }

            try (PreparedStatement statement = connection.prepareStatement(
                    "SELECT * FROM test_get_metadata")) {
                ResultSetMetaData metadata = statement.getMetaData();
                assertEquals(metadata.getColumnCount(), 8);
                for (int i = 1; i <= metadata.getColumnCount(); i++) {
                    assertEquals(metadata.getCatalogName(i), "blackhole");
                    assertEquals(metadata.getSchemaName(i), "blackhole");
                    assertEquals(metadata.getTableName(i), "test_get_metadata");
                }

                assertEquals(metadata.getColumnName(1), "c_boolean");
                assertEquals(metadata.getColumnTypeName(1), "boolean");

                assertEquals(metadata.getColumnName(2), "c_decimal");
                assertEquals(metadata.getColumnTypeName(2), "decimal(38,0)");

                assertEquals(metadata.getColumnName(3), "c_decimal_2");
                assertEquals(metadata.getColumnTypeName(3), "decimal(10,3)");

                assertEquals(metadata.getColumnName(4), "c_varchar");
                assertEquals(metadata.getColumnTypeName(4), "varchar");

                assertEquals(metadata.getColumnName(5), "c_varchar_2");
                assertEquals(metadata.getColumnTypeName(5), "varchar(10)");

                assertEquals(metadata.getColumnName(6), "c_row");
                assertEquals(metadata.getColumnTypeName(6), "row");

                assertEquals(metadata.getColumnName(7), "c_array");
                assertEquals(metadata.getColumnTypeName(7), "array");

                assertEquals(metadata.getColumnName(8), "c_map");
                assertEquals(metadata.getColumnTypeName(8), "map");
            }

            try (Statement statement = connection.createStatement()) {
                statement.execute("DROP TABLE test_get_metadata");
            }
        }
    }

    @Test
    public void testGetParameterMetaData()
            throws Exception
    {
        testGetParameterMetaData(true);
        testGetParameterMetaData(false);
    }

    private void testGetParameterMetaData(boolean useLegacyPreparedStatements)
            throws Exception
    {
        try (Connection connection = createConnection("blackhole", "blackhole", useLegacyPreparedStatements)) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE test_get_parameterMetaData (" +
                        "c_boolean boolean, " +
                        "c_decimal decimal, " +
                        "c_decimal_2 decimal(10,3)," +
                        "c_varchar varchar, " +
                        "c_varchar_2 varchar(5), " +
                        "c_row row(x integer, y array(integer)), " +
                        "c_array array(integer), " +
                        "c_map map(integer, integer), " +
                        "c_tinyint tinyint, " +
                        "c_integer integer, " +
                        "c_bigint bigint, " +
                        "c_smallint smallint, " +
                        "c_real real, " +
                        "c_double double)");
            }

            try (PreparedStatement statement = connection.prepareStatement(
                    "SELECT ? FROM test_get_parameterMetaData WHERE c_boolean = ? AND c_decimal = ? " +
                            "AND c_decimal_2 = ? AND c_varchar = ? AND c_varchar_2 = ? AND c_row = ? " +
                            "AND c_array = ? AND c_map = ? AND c_tinyint = ? AND c_integer = ? AND c_bigint = ? " +
                            "AND c_smallint = ? AND c_real = ? AND c_double = ?")) {
                ParameterMetaData parameterMetaData = statement.getParameterMetaData();
                assertEquals(parameterMetaData.getParameterCount(), 15);

                assertEquals(parameterMetaData.getParameterClassName(1), "unknown");
                assertEquals(parameterMetaData.getParameterType(1), Types.NULL);
                assertEquals(parameterMetaData.getParameterTypeName(1), "unknown");
                assertEquals(parameterMetaData.isNullable(1), parameterNullableUnknown);
                assertFalse(parameterMetaData.isSigned(1));
                assertEquals(parameterMetaData.getParameterMode(1), parameterModeUnknown);

                assertEquals(parameterMetaData.getParameterClassName(2), Boolean.class.getName());
                assertEquals(parameterMetaData.getParameterType(2), Types.BOOLEAN);
                assertEquals(parameterMetaData.getParameterTypeName(2), "boolean");
                assertEquals(parameterMetaData.isNullable(2), parameterNullableUnknown);
                assertFalse(parameterMetaData.isSigned(2));
                assertEquals(parameterMetaData.getParameterMode(2), parameterModeUnknown);

                assertEquals(parameterMetaData.getParameterClassName(3), BigDecimal.class.getName());
                assertEquals(parameterMetaData.getParameterType(3), Types.DECIMAL);
                assertEquals(parameterMetaData.getParameterTypeName(3), "decimal");
                assertEquals(parameterMetaData.isNullable(3), parameterNullableUnknown);
                assertTrue(parameterMetaData.isSigned(3));
                assertEquals(parameterMetaData.getParameterMode(3), parameterModeUnknown);

                assertEquals(parameterMetaData.getParameterClassName(4), BigDecimal.class.getName());
                assertEquals(parameterMetaData.getParameterType(4), Types.DECIMAL);
                assertEquals(parameterMetaData.getParameterTypeName(4), "decimal");
                assertEquals(parameterMetaData.getPrecision(4), 10);
                assertEquals(parameterMetaData.getScale(4), 3);
                assertEquals(parameterMetaData.isNullable(4), parameterNullableUnknown);
                assertTrue(parameterMetaData.isSigned(4));
                assertEquals(parameterMetaData.getParameterMode(4), parameterModeUnknown);

                assertEquals(parameterMetaData.getParameterClassName(5), String.class.getName());
                assertEquals(parameterMetaData.getParameterType(5), Types.VARCHAR);
                assertEquals(parameterMetaData.getParameterTypeName(5), "varchar");
                assertEquals(parameterMetaData.isNullable(5), parameterNullableUnknown);
                assertFalse(parameterMetaData.isSigned(5));
                assertEquals(parameterMetaData.getParameterMode(5), parameterModeUnknown);

                assertEquals(parameterMetaData.getParameterClassName(6), String.class.getName());
                assertEquals(parameterMetaData.getParameterType(6), Types.VARCHAR);
                assertEquals(parameterMetaData.getParameterTypeName(6), "varchar");
                assertEquals(parameterMetaData.getPrecision(6), 5);
                assertEquals(parameterMetaData.isNullable(6), parameterNullableUnknown);
                assertFalse(parameterMetaData.isSigned(6));
                assertEquals(parameterMetaData.getParameterMode(6), parameterModeUnknown);

                assertEquals(parameterMetaData.getParameterClassName(7), String.class.getName());
                assertEquals(parameterMetaData.getParameterType(7), Types.JAVA_OBJECT);
                assertEquals(parameterMetaData.getParameterTypeName(7), "row");
                assertEquals(parameterMetaData.isNullable(7), parameterNullableUnknown);
                assertFalse(parameterMetaData.isSigned(7));
                assertEquals(parameterMetaData.getParameterMode(7), parameterModeUnknown);

                assertEquals(parameterMetaData.getParameterClassName(8), Array.class.getName());
                assertEquals(parameterMetaData.getParameterType(8), Types.ARRAY);
                assertEquals(parameterMetaData.getParameterTypeName(8), "array");
                assertEquals(parameterMetaData.isNullable(8), parameterNullableUnknown);
                assertFalse(parameterMetaData.isSigned(8));
                assertEquals(parameterMetaData.getParameterMode(8), parameterModeUnknown);

                assertEquals(parameterMetaData.getParameterClassName(9), String.class.getName());
                assertEquals(parameterMetaData.getParameterType(9), Types.JAVA_OBJECT);
                assertEquals(parameterMetaData.getParameterTypeName(9), "map");
                assertEquals(parameterMetaData.isNullable(9), parameterNullableUnknown);
                assertFalse(parameterMetaData.isSigned(9));
                assertEquals(parameterMetaData.getParameterMode(9), parameterModeUnknown);

                assertEquals(parameterMetaData.getParameterClassName(10), Byte.class.getName());
                assertEquals(parameterMetaData.getParameterType(10), Types.TINYINT);
                assertEquals(parameterMetaData.getParameterTypeName(10), "tinyint");
                assertEquals(parameterMetaData.isNullable(10), parameterNullableUnknown);
                assertTrue(parameterMetaData.isSigned(10));
                assertEquals(parameterMetaData.getParameterMode(10), parameterModeUnknown);

                assertEquals(parameterMetaData.getParameterClassName(11), Integer.class.getName());
                assertEquals(parameterMetaData.getParameterType(11), Types.INTEGER);
                assertEquals(parameterMetaData.getParameterTypeName(11), "integer");
                assertEquals(parameterMetaData.isNullable(11), parameterNullableUnknown);
                assertTrue(parameterMetaData.isSigned(11));
                assertEquals(parameterMetaData.getParameterMode(11), parameterModeUnknown);

                assertEquals(parameterMetaData.getParameterClassName(12), Long.class.getName());
                assertEquals(parameterMetaData.getParameterType(12), Types.BIGINT);
                assertEquals(parameterMetaData.getParameterTypeName(12), "bigint");
                assertEquals(parameterMetaData.isNullable(12), parameterNullableUnknown);
                assertTrue(parameterMetaData.isSigned(12));
                assertEquals(parameterMetaData.getParameterMode(12), parameterModeUnknown);

                assertEquals(parameterMetaData.getParameterClassName(13), Short.class.getName());
                assertEquals(parameterMetaData.getParameterType(13), Types.SMALLINT);
                assertEquals(parameterMetaData.getParameterTypeName(13), "smallint");
                assertEquals(parameterMetaData.isNullable(13), parameterNullableUnknown);
                assertTrue(parameterMetaData.isSigned(13));
                assertEquals(parameterMetaData.getParameterMode(13), parameterModeUnknown);

                assertEquals(parameterMetaData.getParameterClassName(14), Float.class.getName());
                assertEquals(parameterMetaData.getParameterType(14), Types.REAL);
                assertEquals(parameterMetaData.getParameterTypeName(14), "real");
                assertEquals(parameterMetaData.isNullable(14), parameterNullableUnknown);
                assertTrue(parameterMetaData.isSigned(14));
                assertEquals(parameterMetaData.getParameterMode(14), parameterModeUnknown);

                assertEquals(parameterMetaData.getParameterClassName(15), Double.class.getName());
                assertEquals(parameterMetaData.getParameterType(15), Types.DOUBLE);
                assertEquals(parameterMetaData.getParameterTypeName(15), "double");
                assertEquals(parameterMetaData.isNullable(15), parameterNullableUnknown);
                assertTrue(parameterMetaData.isSigned(15));
                assertEquals(parameterMetaData.getParameterMode(15), parameterModeUnknown);
            }

            try (Statement statement = connection.createStatement()) {
                statement.execute("DROP TABLE test_get_parameterMetaData");
            }
        }
    }

    @Test
    public void testGetClientTypeSignatureFromTypeString()
    {
        ClientTypeSignature actualClientTypeSignature = TrinoPreparedStatement.getClientTypeSignatureFromTypeString("boolean");
        ClientTypeSignature expectedClientTypeSignature = new ClientTypeSignature("boolean", ImmutableList.of());
        assertEquals(actualClientTypeSignature, expectedClientTypeSignature);

        actualClientTypeSignature = TrinoPreparedStatement.getClientTypeSignatureFromTypeString("decimal(10,3)");
        expectedClientTypeSignature = new ClientTypeSignature("decimal", ImmutableList.of(
                ClientTypeSignatureParameter.ofLong(10),
                ClientTypeSignatureParameter.ofLong(3)));
        assertEquals(actualClientTypeSignature, expectedClientTypeSignature);

        actualClientTypeSignature = TrinoPreparedStatement.getClientTypeSignatureFromTypeString("varchar");
        expectedClientTypeSignature = new ClientTypeSignature("varchar", ImmutableList.of(ClientTypeSignatureParameter.ofLong(VARCHAR_UNBOUNDED_LENGTH)));
        assertEquals(actualClientTypeSignature, expectedClientTypeSignature);

        actualClientTypeSignature = TrinoPreparedStatement.getClientTypeSignatureFromTypeString("varchar(10)");
        expectedClientTypeSignature = new ClientTypeSignature("varchar", ImmutableList.of(ClientTypeSignatureParameter.ofLong(10)));
        assertEquals(actualClientTypeSignature, expectedClientTypeSignature);

        actualClientTypeSignature = TrinoPreparedStatement.getClientTypeSignatureFromTypeString("row(x integer, y array(integer))");
        expectedClientTypeSignature = new ClientTypeSignature("row", ImmutableList.of());
        assertEquals(actualClientTypeSignature, expectedClientTypeSignature);

        actualClientTypeSignature = TrinoPreparedStatement.getClientTypeSignatureFromTypeString("array(integer)");
        expectedClientTypeSignature = new ClientTypeSignature("array", ImmutableList.of());
        assertEquals(actualClientTypeSignature, expectedClientTypeSignature);

        actualClientTypeSignature = TrinoPreparedStatement.getClientTypeSignatureFromTypeString("map(integer, integer)");
        expectedClientTypeSignature = new ClientTypeSignature("map", ImmutableList.of());
        assertEquals(actualClientTypeSignature, expectedClientTypeSignature);

        actualClientTypeSignature = TrinoPreparedStatement.getClientTypeSignatureFromTypeString("timestamp(12) with time zone");
        expectedClientTypeSignature = new ClientTypeSignature("timestamp with time zone", ImmutableList.of(ClientTypeSignatureParameter.ofLong(12)));
        assertEquals(actualClientTypeSignature, expectedClientTypeSignature);

        actualClientTypeSignature = TrinoPreparedStatement.getClientTypeSignatureFromTypeString("time(13) with time zone");
        expectedClientTypeSignature = new ClientTypeSignature("time with time zone", ImmutableList.of(ClientTypeSignatureParameter.ofLong(13)));
        assertEquals(actualClientTypeSignature, expectedClientTypeSignature);
    }

    @Test
    public void testDeallocate()
            throws Exception
    {
        testDeallocate(true);
        testDeallocate(false);
    }

    private void testDeallocate(boolean useLegacyPreparedStatements)
            throws Exception
    {
        try (Connection connection = createConnection(useLegacyPreparedStatements)) {
            for (int i = 0; i < 200; i++) {
                try {
                    connection.prepareStatement("SELECT '" + repeat("a", 300) + "'").close();
                }
                catch (Exception e) {
                    throw new RuntimeException("Failed at " + i, e);
                }
            }
        }
    }

    @Test
    public void testCloseIdempotency()
            throws Exception
    {
        testCloseIdempotency(true);
        testCloseIdempotency(false);
    }

    private void testCloseIdempotency(boolean useLegacyPreparedStatements)
            throws Exception
    {
        try (Connection connection = createConnection(useLegacyPreparedStatements)) {
            PreparedStatement statement = connection.prepareStatement("SELECT 123");
            statement.close();
            statement.close();
        }
    }

    @Test
    public void testLargePreparedStatement()
            throws Exception
    {
        testLargePreparedStatement(true);
        testLargePreparedStatement(false);
    }

    private void testLargePreparedStatement(boolean useLegacyPreparedStatements)
            throws Exception
    {
        int elements = HEADER_SIZE_LIMIT + 1;
        try (Connection connection = createConnection(useLegacyPreparedStatements);
                PreparedStatement statement = connection.prepareStatement("VALUES ?" + repeat(", ?", elements - 1))) {
            for (int i = 0; i < elements; i++) {
                statement.setLong(i + 1, i);
            }
            try (ResultSet resultSet = statement.executeQuery()) {
                assertThat(readRows(resultSet).stream().map(Iterables::getOnlyElement))
                        .containsExactlyInAnyOrder(LongStream.range(0, elements).boxed().toArray());
            }
        }
    }

    @Test
    public void testExecuteUpdate()
            throws Exception
    {
        testExecuteUpdate(true);
        testExecuteUpdate(false);
    }

    public void testExecuteUpdate(boolean useLegacyPreparedStatements)
            throws Exception
    {
        try (Connection connection = createConnection("blackhole", "blackhole", useLegacyPreparedStatements)) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE test_execute_update (" +
                        "c_boolean boolean, " +
                        "c_bigint bigint, " +
                        "c_double double, " +
                        "c_decimal decimal, " +
                        "c_varchar varchar, " +
                        "c_varbinary varbinary, " +
                        "c_null bigint)");
            }

            try (PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO test_execute_update VALUES (?, ?, ?, ?, ?, ?, ?)")) {
                statement.setBoolean(1, true);
                statement.setLong(2, 5L);
                statement.setDouble(3, 7.0d);
                statement.setBigDecimal(4, BigDecimal.valueOf(8L));
                statement.setString(5, "abc'xyz");
                statement.setBytes(6, "xyz".getBytes(UTF_8));
                statement.setNull(7, Types.BIGINT);

                assertEquals(statement.executeUpdate(), 1);

                assertFalse(statement.execute());
                assertEquals(statement.getUpdateCount(), 1);
                assertEquals(statement.getLargeUpdateCount(), 1);
            }

            try (Statement statement = connection.createStatement()) {
                statement.execute("DROP TABLE test_execute_update");
            }
        }
    }

    @Test
    public void testExecuteBatch()
            throws Exception
    {
        testExecuteBatch(true);
        testExecuteBatch(false);
    }

    private void testExecuteBatch(boolean useLegacyPreparedStatements)
            throws Exception
    {
        try (Connection connection = createConnection("memory", "default", useLegacyPreparedStatements)) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE test_execute_batch(c_int integer)");
            }

            try (PreparedStatement preparedStatement = connection.prepareStatement(
                    "INSERT INTO test_execute_batch VALUES (?)")) {
                // Run executeBatch before addBatch
                assertEquals(preparedStatement.executeBatch(), new int[] {});

                for (int i = 0; i < 3; i++) {
                    preparedStatement.setInt(1, i);
                    preparedStatement.addBatch();
                }
                assertEquals(preparedStatement.executeBatch(), new int[] {1, 1, 1});

                try (Statement statement = connection.createStatement()) {
                    ResultSet resultSet = statement.executeQuery("SELECT c_int FROM test_execute_batch");
                    assertThat(readRows(resultSet))
                            .containsExactlyInAnyOrder(
                                    list(0),
                                    list(1),
                                    list(2));
                }

                // Make sure the above executeBatch cleared existing batch
                assertEquals(preparedStatement.executeBatch(), new int[] {});

                // clearBatch removes added batch and cancel batch mode
                preparedStatement.setBoolean(1, true);
                preparedStatement.clearBatch();
                assertEquals(preparedStatement.executeBatch(), new int[] {});

                preparedStatement.setInt(1, 1);
                assertEquals(preparedStatement.executeUpdate(), 1);
            }

            try (Statement statement = connection.createStatement()) {
                statement.execute("DROP TABLE test_execute_batch");
            }
        }
    }

    @Test
    public void testInvalidExecuteBatch()
            throws Exception
    {
        testInvalidExecuteBatch(true);
        testInvalidExecuteBatch(false);
    }

    private void testInvalidExecuteBatch(boolean useLegacyPreparedStatements)
            throws Exception
    {
        try (Connection connection = createConnection("blackhole", "blackhole", useLegacyPreparedStatements)) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE test_invalid_execute_batch(c_int integer)");
            }

            try (PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO test_invalid_execute_batch VALUES (?)")) {
                statement.setInt(1, 1);
                statement.addBatch();

                String message = "Batch prepared statement must be executed using executeBatch method";
                assertThatThrownBy(statement::executeQuery)
                        .isInstanceOf(SQLException.class)
                        .hasMessage(message);
                assertThatThrownBy(statement::executeUpdate)
                        .isInstanceOf(SQLException.class)
                        .hasMessage(message);
                assertThatThrownBy(statement::executeLargeUpdate)
                        .isInstanceOf(SQLException.class)
                        .hasMessage(message);
                assertThatThrownBy(statement::execute)
                        .isInstanceOf(SQLException.class)
                        .hasMessage(message);
            }

            try (Statement statement = connection.createStatement()) {
                statement.execute("DROP TABLE test_invalid_execute_batch");
            }
        }
    }

    @Test
    public void testPrepareMultiple()
            throws Exception
    {
        testPrepareMultiple(true);
        testPrepareMultiple(false);
    }

    private void testPrepareMultiple(boolean useLegacyPreparedStatements)
            throws Exception
    {
        try (Connection connection = createConnection(useLegacyPreparedStatements);
                PreparedStatement statement1 = connection.prepareStatement("SELECT 123");
                PreparedStatement statement2 = connection.prepareStatement("SELECT 456")) {
            try (ResultSet rs = statement1.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 123);
                assertFalse(rs.next());
            }

            try (ResultSet rs = statement2.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 456);
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testPrepareLarge()
            throws Exception
    {
        testPrepareLarge(true);
        testPrepareLarge(false);
    }

    private void testPrepareLarge(boolean useLegacyPreparedStatements)
            throws Exception
    {
        String sql = format("SELECT '%s' = '%s'", repeat("x", 100_000), repeat("y", 100_000));
        try (Connection connection = createConnection(useLegacyPreparedStatements);
                PreparedStatement statement = connection.prepareStatement(sql);
                ResultSet rs = statement.executeQuery()) {
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testSetNull()
            throws Exception
    {
        testSetNull(true);
        testSetNull(false);
    }

    private void testSetNull(boolean useLegacyPreparedStatements)
            throws Exception
    {
        assertSetNull(Types.BOOLEAN, useLegacyPreparedStatements);
        assertSetNull(Types.BIT, Types.BOOLEAN, useLegacyPreparedStatements);
        assertSetNull(Types.TINYINT, useLegacyPreparedStatements);
        assertSetNull(Types.SMALLINT, useLegacyPreparedStatements);
        assertSetNull(Types.INTEGER, useLegacyPreparedStatements);
        assertSetNull(Types.BIGINT, useLegacyPreparedStatements);
        assertSetNull(Types.REAL, useLegacyPreparedStatements);
        assertSetNull(Types.FLOAT, Types.REAL, useLegacyPreparedStatements);
        assertSetNull(Types.DECIMAL, useLegacyPreparedStatements);
        assertSetNull(Types.NUMERIC, Types.DECIMAL, useLegacyPreparedStatements);
        assertSetNull(Types.CHAR, useLegacyPreparedStatements);
        assertSetNull(Types.NCHAR, Types.CHAR, useLegacyPreparedStatements);
        assertSetNull(Types.VARCHAR, Types.VARCHAR, useLegacyPreparedStatements);
        assertSetNull(Types.NVARCHAR, Types.VARCHAR, useLegacyPreparedStatements);
        assertSetNull(Types.LONGVARCHAR, Types.VARCHAR, useLegacyPreparedStatements);
        assertSetNull(Types.VARCHAR, Types.VARCHAR, useLegacyPreparedStatements);
        assertSetNull(Types.CLOB, Types.VARCHAR, useLegacyPreparedStatements);
        assertSetNull(Types.NCLOB, Types.VARCHAR, useLegacyPreparedStatements);
        assertSetNull(Types.VARBINARY, Types.VARBINARY, useLegacyPreparedStatements);
        assertSetNull(Types.VARBINARY, useLegacyPreparedStatements);
        assertSetNull(Types.BLOB, Types.VARBINARY, useLegacyPreparedStatements);
        assertSetNull(Types.DATE, useLegacyPreparedStatements);
        assertSetNull(Types.TIME, useLegacyPreparedStatements);
        assertSetNull(Types.TIMESTAMP, useLegacyPreparedStatements);
        assertSetNull(Types.NULL, useLegacyPreparedStatements);
    }

    private void assertSetNull(int sqlType, boolean useLegacyPreparedStatements)
            throws SQLException
    {
        assertSetNull(sqlType, sqlType, useLegacyPreparedStatements);
    }

    private void assertSetNull(int sqlType, int expectedSqlType, boolean useLegacyPreparedStatements)
            throws SQLException
    {
        try (Connection connection = createConnection(useLegacyPreparedStatements);
                PreparedStatement statement = connection.prepareStatement("SELECT ?")) {
            statement.setNull(1, sqlType);

            try (ResultSet rs = statement.executeQuery()) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
                assertFalse(rs.next());

                assertEquals(rs.getMetaData().getColumnType(1), expectedSqlType);
            }
        }
    }

    @Test
    public void testConvertBoolean()
            throws SQLException
    {
        testConvertBoolean(true);
        testConvertBoolean(false);
    }

    private void testConvertBoolean(boolean useLegacyPreparedStatements)
            throws SQLException
    {
        assertBind((ps, i) -> ps.setBoolean(i, true), useLegacyPreparedStatements).roundTripsAs(Types.BOOLEAN, true);
        assertBind((ps, i) -> ps.setBoolean(i, false), useLegacyPreparedStatements).roundTripsAs(Types.BOOLEAN, false);
        assertBind((ps, i) -> ps.setObject(i, true), useLegacyPreparedStatements).roundTripsAs(Types.BOOLEAN, true);
        assertBind((ps, i) -> ps.setObject(i, false), useLegacyPreparedStatements).roundTripsAs(Types.BOOLEAN, false);

        for (int type : asList(Types.BOOLEAN, Types.BIT)) {
            assertBind((ps, i) -> ps.setObject(i, true, type), useLegacyPreparedStatements).roundTripsAs(Types.BOOLEAN, true);
            assertBind((ps, i) -> ps.setObject(i, false, type), useLegacyPreparedStatements).roundTripsAs(Types.BOOLEAN, false);
            assertBind((ps, i) -> ps.setObject(i, 13, type), useLegacyPreparedStatements).roundTripsAs(Types.BOOLEAN, true);
            assertBind((ps, i) -> ps.setObject(i, 0, type), useLegacyPreparedStatements).roundTripsAs(Types.BOOLEAN, false);
            assertBind((ps, i) -> ps.setObject(i, "1", type), useLegacyPreparedStatements).roundTripsAs(Types.BOOLEAN, true);
            assertBind((ps, i) -> ps.setObject(i, "true", type), useLegacyPreparedStatements).roundTripsAs(Types.BOOLEAN, true);
            assertBind((ps, i) -> ps.setObject(i, "0", type), useLegacyPreparedStatements).roundTripsAs(Types.BOOLEAN, false);
            assertBind((ps, i) -> ps.setObject(i, "false", type), useLegacyPreparedStatements).roundTripsAs(Types.BOOLEAN, false);
        }
    }

    @Test
    public void testConvertTinyint()
            throws SQLException
    {
        testConvertTinyint(true);
        testConvertTinyint(false);
    }

    private void testConvertTinyint(boolean useLegacyPreparedStatements)
            throws SQLException
    {
        assertBind((ps, i) -> ps.setByte(i, (byte) 123), useLegacyPreparedStatements).roundTripsAs(Types.TINYINT, (byte) 123);
        assertBind((ps, i) -> ps.setObject(i, (byte) 123), useLegacyPreparedStatements).roundTripsAs(Types.TINYINT, (byte) 123);
        assertBind((ps, i) -> ps.setObject(i, (byte) 123, Types.TINYINT), useLegacyPreparedStatements).roundTripsAs(Types.TINYINT, (byte) 123);
        assertBind((ps, i) -> ps.setObject(i, (short) 123, Types.TINYINT), useLegacyPreparedStatements).roundTripsAs(Types.TINYINT, (byte) 123);
        assertBind((ps, i) -> ps.setObject(i, 123, Types.TINYINT), useLegacyPreparedStatements).roundTripsAs(Types.TINYINT, (byte) 123);
        assertBind((ps, i) -> ps.setObject(i, 123L, Types.TINYINT), useLegacyPreparedStatements).roundTripsAs(Types.TINYINT, (byte) 123);
        assertBind((ps, i) -> ps.setObject(i, 123.9f, Types.TINYINT), useLegacyPreparedStatements).roundTripsAs(Types.TINYINT, (byte) 123);
        assertBind((ps, i) -> ps.setObject(i, 123.9d, Types.TINYINT), useLegacyPreparedStatements).roundTripsAs(Types.TINYINT, (byte) 123);
        assertBind((ps, i) -> ps.setObject(i, BigInteger.valueOf(123), Types.TINYINT), useLegacyPreparedStatements).roundTripsAs(Types.TINYINT, (byte) 123);
        assertBind((ps, i) -> ps.setObject(i, BigDecimal.valueOf(123), Types.TINYINT), useLegacyPreparedStatements).roundTripsAs(Types.TINYINT, (byte) 123);
        assertBind((ps, i) -> ps.setObject(i, BigDecimal.valueOf(123.9), Types.TINYINT), useLegacyPreparedStatements).roundTripsAs(Types.TINYINT, (byte) 123);
        assertBind((ps, i) -> ps.setObject(i, "123", Types.TINYINT), useLegacyPreparedStatements).roundTripsAs(Types.TINYINT, (byte) 123);
        assertBind((ps, i) -> ps.setObject(i, true, Types.TINYINT), useLegacyPreparedStatements).roundTripsAs(Types.TINYINT, (byte) 1);
        assertBind((ps, i) -> ps.setObject(i, false, Types.TINYINT), useLegacyPreparedStatements).roundTripsAs(Types.TINYINT, (byte) 0);
    }

    @Test
    public void testConvertSmallint()
            throws SQLException
    {
        testConvertSmallint(true);
        testConvertSmallint(false);
    }

    private void testConvertSmallint(boolean useLegacyPreparedStatements)
            throws SQLException
    {
        assertBind((ps, i) -> ps.setShort(i, (short) 123), useLegacyPreparedStatements).roundTripsAs(Types.SMALLINT, (short) 123);
        assertBind((ps, i) -> ps.setObject(i, (short) 123), useLegacyPreparedStatements).roundTripsAs(Types.SMALLINT, (short) 123);
        assertBind((ps, i) -> ps.setObject(i, (byte) 123, Types.SMALLINT), useLegacyPreparedStatements).roundTripsAs(Types.SMALLINT, (short) 123);
        assertBind((ps, i) -> ps.setObject(i, (short) 123, Types.SMALLINT), useLegacyPreparedStatements).roundTripsAs(Types.SMALLINT, (short) 123);
        assertBind((ps, i) -> ps.setObject(i, 123, Types.SMALLINT), useLegacyPreparedStatements).roundTripsAs(Types.SMALLINT, (short) 123);
        assertBind((ps, i) -> ps.setObject(i, 123L, Types.SMALLINT), useLegacyPreparedStatements).roundTripsAs(Types.SMALLINT, (short) 123);
        assertBind((ps, i) -> ps.setObject(i, 123.9f, Types.SMALLINT), useLegacyPreparedStatements).roundTripsAs(Types.SMALLINT, (short) 123);
        assertBind((ps, i) -> ps.setObject(i, 123.9d, Types.SMALLINT), useLegacyPreparedStatements).roundTripsAs(Types.SMALLINT, (short) 123);
        assertBind((ps, i) -> ps.setObject(i, BigInteger.valueOf(123), Types.SMALLINT), useLegacyPreparedStatements).roundTripsAs(Types.SMALLINT, (short) 123);
        assertBind((ps, i) -> ps.setObject(i, BigDecimal.valueOf(123), Types.SMALLINT), useLegacyPreparedStatements).roundTripsAs(Types.SMALLINT, (short) 123);
        assertBind((ps, i) -> ps.setObject(i, BigDecimal.valueOf(123.9), Types.SMALLINT), useLegacyPreparedStatements).roundTripsAs(Types.SMALLINT, (short) 123);
        assertBind((ps, i) -> ps.setObject(i, "123", Types.SMALLINT), useLegacyPreparedStatements).roundTripsAs(Types.SMALLINT, (short) 123);
        assertBind((ps, i) -> ps.setObject(i, true, Types.SMALLINT), useLegacyPreparedStatements).roundTripsAs(Types.SMALLINT, (short) 1);
        assertBind((ps, i) -> ps.setObject(i, false, Types.SMALLINT), useLegacyPreparedStatements).roundTripsAs(Types.SMALLINT, (short) 0);
    }

    @Test
    public void testConvertInteger()
            throws SQLException
    {
        testConvertInteger(true);
        testConvertInteger(false);
    }

    private void testConvertInteger(boolean useLegacyPreparedStatements)
            throws SQLException
    {
        assertBind((ps, i) -> ps.setInt(i, 123), useLegacyPreparedStatements).roundTripsAs(Types.INTEGER, 123);
        assertBind((ps, i) -> ps.setObject(i, 123), useLegacyPreparedStatements).roundTripsAs(Types.INTEGER, 123);
        assertBind((ps, i) -> ps.setObject(i, (byte) 123, Types.INTEGER), useLegacyPreparedStatements).roundTripsAs(Types.INTEGER, 123);
        assertBind((ps, i) -> ps.setObject(i, (byte) 123, Types.INTEGER), useLegacyPreparedStatements).roundTripsAs(Types.INTEGER, 123);
        assertBind((ps, i) -> ps.setObject(i, (short) 123, Types.INTEGER), useLegacyPreparedStatements).roundTripsAs(Types.INTEGER, 123);
        assertBind((ps, i) -> ps.setObject(i, 123, Types.INTEGER), useLegacyPreparedStatements).roundTripsAs(Types.INTEGER, 123);
        assertBind((ps, i) -> ps.setObject(i, 123L, Types.INTEGER), useLegacyPreparedStatements).roundTripsAs(Types.INTEGER, 123);
        assertBind((ps, i) -> ps.setObject(i, 123.9f, Types.INTEGER), useLegacyPreparedStatements).roundTripsAs(Types.INTEGER, 123);
        assertBind((ps, i) -> ps.setObject(i, 123.9d, Types.INTEGER), useLegacyPreparedStatements).roundTripsAs(Types.INTEGER, 123);
        assertBind((ps, i) -> ps.setObject(i, BigInteger.valueOf(123), Types.INTEGER), useLegacyPreparedStatements).roundTripsAs(Types.INTEGER, 123);
        assertBind((ps, i) -> ps.setObject(i, BigDecimal.valueOf(123), Types.INTEGER), useLegacyPreparedStatements).roundTripsAs(Types.INTEGER, 123);
        assertBind((ps, i) -> ps.setObject(i, BigDecimal.valueOf(123.9), Types.INTEGER), useLegacyPreparedStatements).roundTripsAs(Types.INTEGER, 123);
        assertBind((ps, i) -> ps.setObject(i, "123", Types.INTEGER), useLegacyPreparedStatements).roundTripsAs(Types.INTEGER, 123);
        assertBind((ps, i) -> ps.setObject(i, true, Types.INTEGER), useLegacyPreparedStatements).roundTripsAs(Types.INTEGER, 1);
        assertBind((ps, i) -> ps.setObject(i, false, Types.INTEGER), useLegacyPreparedStatements).roundTripsAs(Types.INTEGER, 0);
    }

    @Test
    public void testConvertBigint()
            throws SQLException
    {
        testConvertBigint(true);
        testConvertBigint(false);
    }

    private void testConvertBigint(boolean useLegacyPreparedStatements)
            throws SQLException
    {
        assertBind((ps, i) -> ps.setLong(i, 123L), useLegacyPreparedStatements).roundTripsAs(Types.BIGINT, 123L);
        assertBind((ps, i) -> ps.setObject(i, 123L), useLegacyPreparedStatements).roundTripsAs(Types.BIGINT, 123L);
        assertBind((ps, i) -> ps.setObject(i, (byte) 123, Types.BIGINT), useLegacyPreparedStatements).roundTripsAs(Types.BIGINT, 123L);
        assertBind((ps, i) -> ps.setObject(i, (short) 123, Types.BIGINT), useLegacyPreparedStatements).roundTripsAs(Types.BIGINT, 123L);
        assertBind((ps, i) -> ps.setObject(i, 123, Types.BIGINT), useLegacyPreparedStatements).roundTripsAs(Types.BIGINT, 123L);
        assertBind((ps, i) -> ps.setObject(i, 123L, Types.BIGINT), useLegacyPreparedStatements).roundTripsAs(Types.BIGINT, 123L);
        assertBind((ps, i) -> ps.setObject(i, 123.9f, Types.BIGINT), useLegacyPreparedStatements).roundTripsAs(Types.BIGINT, 123L);
        assertBind((ps, i) -> ps.setObject(i, 123.9d, Types.BIGINT), useLegacyPreparedStatements).roundTripsAs(Types.BIGINT, 123L);
        assertBind((ps, i) -> ps.setObject(i, BigInteger.valueOf(123), Types.BIGINT), useLegacyPreparedStatements).roundTripsAs(Types.BIGINT, 123L);
        assertBind((ps, i) -> ps.setObject(i, BigDecimal.valueOf(123), Types.BIGINT), useLegacyPreparedStatements).roundTripsAs(Types.BIGINT, 123L);
        assertBind((ps, i) -> ps.setObject(i, BigDecimal.valueOf(123.9), Types.BIGINT), useLegacyPreparedStatements).roundTripsAs(Types.BIGINT, 123L);
        assertBind((ps, i) -> ps.setObject(i, "123", Types.BIGINT), useLegacyPreparedStatements).roundTripsAs(Types.BIGINT, 123L);
        assertBind((ps, i) -> ps.setObject(i, true, Types.BIGINT), useLegacyPreparedStatements).roundTripsAs(Types.BIGINT, 1L);
        assertBind((ps, i) -> ps.setObject(i, false, Types.BIGINT), useLegacyPreparedStatements).roundTripsAs(Types.BIGINT, 0L);
    }

    @Test
    public void testConvertReal()
            throws SQLException
    {
        testConvertReal(true);
        testConvertReal(false);
    }

    private void testConvertReal(boolean useLegacyPreparedStatements)
            throws SQLException
    {
        assertBind((ps, i) -> ps.setFloat(i, 4.2f), useLegacyPreparedStatements).roundTripsAs(Types.REAL, 4.2f);
        assertBind((ps, i) -> ps.setObject(i, 4.2f), useLegacyPreparedStatements).roundTripsAs(Types.REAL, 4.2f);

        for (int type : asList(Types.REAL, Types.FLOAT)) {
            assertBind((ps, i) -> ps.setObject(i, (byte) 123, type), useLegacyPreparedStatements).roundTripsAs(Types.REAL, 123.0f);
            assertBind((ps, i) -> ps.setObject(i, (short) 123, type), useLegacyPreparedStatements).roundTripsAs(Types.REAL, 123.0f);
            assertBind((ps, i) -> ps.setObject(i, 123, type), useLegacyPreparedStatements).roundTripsAs(Types.REAL, 123.0f);
            assertBind((ps, i) -> ps.setObject(i, 123L, type), useLegacyPreparedStatements).roundTripsAs(Types.REAL, 123.0f);
            assertBind((ps, i) -> ps.setObject(i, 123.9f, type), useLegacyPreparedStatements).roundTripsAs(Types.REAL, 123.9f);
            assertBind((ps, i) -> ps.setObject(i, 123.9d, type), useLegacyPreparedStatements).roundTripsAs(Types.REAL, 123.9f);
            assertBind((ps, i) -> ps.setObject(i, BigInteger.valueOf(123), type), useLegacyPreparedStatements).roundTripsAs(Types.REAL, 123.0f);
            assertBind((ps, i) -> ps.setObject(i, BigDecimal.valueOf(123), type), useLegacyPreparedStatements).roundTripsAs(Types.REAL, 123.0f);
            assertBind((ps, i) -> ps.setObject(i, BigDecimal.valueOf(123.9), type), useLegacyPreparedStatements).roundTripsAs(Types.REAL, 123.9f);
            assertBind((ps, i) -> ps.setObject(i, "4.2", type), useLegacyPreparedStatements).roundTripsAs(Types.REAL, 4.2f);
            assertBind((ps, i) -> ps.setObject(i, true, type), useLegacyPreparedStatements).roundTripsAs(Types.REAL, 1.0f);
            assertBind((ps, i) -> ps.setObject(i, false, type), useLegacyPreparedStatements).roundTripsAs(Types.REAL, 0.0f);
        }
    }

    @Test
    public void testConvertDouble()
            throws SQLException
    {
        testConvertDouble(true);
        testConvertDouble(false);
    }

    private void testConvertDouble(boolean useLegacyPreparedStatements)
            throws SQLException
    {
        assertBind((ps, i) -> ps.setDouble(i, 4.2d), useLegacyPreparedStatements).roundTripsAs(Types.DOUBLE, 4.2d);
        assertBind((ps, i) -> ps.setObject(i, 4.2d), useLegacyPreparedStatements).roundTripsAs(Types.DOUBLE, 4.2d);
        assertBind((ps, i) -> ps.setObject(i, (byte) 123, Types.DOUBLE), useLegacyPreparedStatements).roundTripsAs(Types.DOUBLE, 123.0d);
        assertBind((ps, i) -> ps.setObject(i, (short) 123, Types.DOUBLE), useLegacyPreparedStatements).roundTripsAs(Types.DOUBLE, 123.0d);
        assertBind((ps, i) -> ps.setObject(i, 123, Types.DOUBLE), useLegacyPreparedStatements).roundTripsAs(Types.DOUBLE, 123.0d);
        assertBind((ps, i) -> ps.setObject(i, 123L, Types.DOUBLE), useLegacyPreparedStatements).roundTripsAs(Types.DOUBLE, 123.0d);
        assertBind((ps, i) -> ps.setObject(i, 123.9f, Types.DOUBLE), useLegacyPreparedStatements).roundTripsAs(Types.DOUBLE, (double) 123.9f);
        assertBind((ps, i) -> ps.setObject(i, 123.9d, Types.DOUBLE), useLegacyPreparedStatements).roundTripsAs(Types.DOUBLE, 123.9d);
        assertBind((ps, i) -> ps.setObject(i, BigInteger.valueOf(123), Types.DOUBLE), useLegacyPreparedStatements).roundTripsAs(Types.DOUBLE, 123.0d);
        assertBind((ps, i) -> ps.setObject(i, BigDecimal.valueOf(123), Types.DOUBLE), useLegacyPreparedStatements).roundTripsAs(Types.DOUBLE, 123.0d);
        assertBind((ps, i) -> ps.setObject(i, BigDecimal.valueOf(123.9), Types.DOUBLE), useLegacyPreparedStatements).roundTripsAs(Types.DOUBLE, 123.9d);
        assertBind((ps, i) -> ps.setObject(i, "4.2", Types.DOUBLE), useLegacyPreparedStatements).roundTripsAs(Types.DOUBLE, 4.2d);
        assertBind((ps, i) -> ps.setObject(i, true, Types.DOUBLE), useLegacyPreparedStatements).roundTripsAs(Types.DOUBLE, 1.0d);
        assertBind((ps, i) -> ps.setObject(i, false, Types.DOUBLE), useLegacyPreparedStatements).roundTripsAs(Types.DOUBLE, 0.0d);
    }

    @Test
    public void testConvertDecimal()
            throws SQLException
    {
        testConvertDecimal(true);
        testConvertDecimal(false);
    }

    private void testConvertDecimal(boolean useLegacyPreparedStatements)
            throws SQLException
    {
        assertBind((ps, i) -> ps.setBigDecimal(i, BigDecimal.valueOf(123)), useLegacyPreparedStatements).roundTripsAs(Types.DECIMAL, BigDecimal.valueOf(123));
        assertBind((ps, i) -> ps.setObject(i, BigDecimal.valueOf(123)), useLegacyPreparedStatements).roundTripsAs(Types.DECIMAL, BigDecimal.valueOf(123));

        for (int type : asList(Types.DECIMAL, Types.NUMERIC)) {
            assertBind((ps, i) -> ps.setObject(i, (byte) 123, type), useLegacyPreparedStatements).roundTripsAs(Types.DECIMAL, BigDecimal.valueOf(123));
            assertBind((ps, i) -> ps.setObject(i, (short) 123, type), useLegacyPreparedStatements).roundTripsAs(Types.DECIMAL, BigDecimal.valueOf(123));
            assertBind((ps, i) -> ps.setObject(i, 123, type), useLegacyPreparedStatements).roundTripsAs(Types.DECIMAL, BigDecimal.valueOf(123));
            assertBind((ps, i) -> ps.setObject(i, 123L, type), useLegacyPreparedStatements).roundTripsAs(Types.DECIMAL, BigDecimal.valueOf(123));
            assertBind((ps, i) -> ps.setObject(i, 123.9f, type), useLegacyPreparedStatements).roundTripsAs(Types.DECIMAL, BigDecimal.valueOf(123.9f));
            assertBind((ps, i) -> ps.setObject(i, 123.9d, type), useLegacyPreparedStatements).roundTripsAs(Types.DECIMAL, BigDecimal.valueOf(123.9d));
            assertBind((ps, i) -> ps.setObject(i, BigInteger.valueOf(123), type), useLegacyPreparedStatements).roundTripsAs(Types.DECIMAL, BigDecimal.valueOf(123));
            assertBind((ps, i) -> ps.setObject(i, BigDecimal.valueOf(123), type), useLegacyPreparedStatements).roundTripsAs(Types.DECIMAL, BigDecimal.valueOf(123));
            assertBind((ps, i) -> ps.setObject(i, BigDecimal.valueOf(123.9d), type), useLegacyPreparedStatements).roundTripsAs(Types.DECIMAL, BigDecimal.valueOf(123.9d));
            assertBind((ps, i) -> ps.setObject(i, "123", type), useLegacyPreparedStatements).roundTripsAs(Types.DECIMAL, BigDecimal.valueOf(123));
            assertBind((ps, i) -> ps.setObject(i, true, type), useLegacyPreparedStatements).roundTripsAs(Types.DECIMAL, BigDecimal.valueOf(1));
            assertBind((ps, i) -> ps.setObject(i, false, type), useLegacyPreparedStatements).roundTripsAs(Types.DECIMAL, BigDecimal.valueOf(0));
        }
    }

    @Test
    public void testConvertVarchar()
            throws SQLException
    {
        testConvertVarchar(true);
        testConvertVarchar(false);
    }

    private void testConvertVarchar(boolean useLegacyPreparedStatements)
            throws SQLException
    {
        assertBind((ps, i) -> ps.setString(i, "hello"), useLegacyPreparedStatements).roundTripsAs(Types.VARCHAR, "hello");
        assertBind((ps, i) -> ps.setObject(i, "hello"), useLegacyPreparedStatements).roundTripsAs(Types.VARCHAR, "hello");

        String unicodeAndNull = "abc'xyz\0\u2603\uD835\uDCABtest";
        assertBind((ps, i) -> ps.setString(i, unicodeAndNull), useLegacyPreparedStatements).roundTripsAs(Types.VARCHAR, unicodeAndNull);

        for (int type : asList(Types.CHAR, Types.NCHAR, Types.VARCHAR, Types.NVARCHAR, Types.LONGVARCHAR, Types.LONGNVARCHAR)) {
            assertBind((ps, i) -> ps.setObject(i, (byte) 123, type), useLegacyPreparedStatements).roundTripsAs(Types.VARCHAR, "123");
            assertBind((ps, i) -> ps.setObject(i, (byte) 123, type), useLegacyPreparedStatements).roundTripsAs(Types.VARCHAR, "123");
            assertBind((ps, i) -> ps.setObject(i, (short) 123, type), useLegacyPreparedStatements).roundTripsAs(Types.VARCHAR, "123");
            assertBind((ps, i) -> ps.setObject(i, 123, type), useLegacyPreparedStatements).roundTripsAs(Types.VARCHAR, "123");
            assertBind((ps, i) -> ps.setObject(i, 123L, type), useLegacyPreparedStatements).roundTripsAs(Types.VARCHAR, "123");
            assertBind((ps, i) -> ps.setObject(i, 123.9f, type), useLegacyPreparedStatements).roundTripsAs(Types.VARCHAR, "123.9");
            assertBind((ps, i) -> ps.setObject(i, 123.9d, type), useLegacyPreparedStatements).roundTripsAs(Types.VARCHAR, "123.9");
            assertBind((ps, i) -> ps.setObject(i, BigInteger.valueOf(123), type), useLegacyPreparedStatements).roundTripsAs(Types.VARCHAR, "123");
            assertBind((ps, i) -> ps.setObject(i, BigDecimal.valueOf(123), type), useLegacyPreparedStatements).roundTripsAs(Types.VARCHAR, "123");
            assertBind((ps, i) -> ps.setObject(i, BigDecimal.valueOf(123.9), type), useLegacyPreparedStatements).roundTripsAs(Types.VARCHAR, "123.9");
            assertBind((ps, i) -> ps.setObject(i, "hello", type), useLegacyPreparedStatements).roundTripsAs(Types.VARCHAR, "hello");
            assertBind((ps, i) -> ps.setObject(i, true, type), useLegacyPreparedStatements).roundTripsAs(Types.VARCHAR, "true");
            assertBind((ps, i) -> ps.setObject(i, false, type), useLegacyPreparedStatements).roundTripsAs(Types.VARCHAR, "false");
        }
    }

    @Test
    public void testConvertVarbinary()
            throws SQLException
    {
        testConvertVarbinary(true);
        testConvertVarbinary(false);
    }

    private void testConvertVarbinary(boolean useLegacyPreparedStatements)
            throws SQLException
    {
        String value = "abc\0xyz";
        byte[] bytes = value.getBytes(UTF_8);

        assertBind((ps, i) -> ps.setBytes(i, bytes), useLegacyPreparedStatements).roundTripsAs(Types.VARBINARY, bytes);
        assertBind((ps, i) -> ps.setObject(i, bytes), useLegacyPreparedStatements).roundTripsAs(Types.VARBINARY, bytes);

        for (int type : asList(Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY)) {
            assertBind((ps, i) -> ps.setObject(i, bytes, type), useLegacyPreparedStatements).roundTripsAs(Types.VARBINARY, bytes);
            assertBind((ps, i) -> ps.setObject(i, value, type), useLegacyPreparedStatements).roundTripsAs(Types.VARBINARY, bytes);
        }
    }

    @Test
    public void testConvertDate()
            throws SQLException
    {
        testConvertDate(true);
        testConvertDate(false);
    }

    private void testConvertDate(boolean useLegacyPreparedStatements)
            throws SQLException
    {
        LocalDate date = LocalDate.of(2001, 5, 6);
        Date sqlDate = Date.valueOf(date);
        java.util.Date javaDate = new java.util.Date(sqlDate.getTime());
        LocalDateTime dateTime = LocalDateTime.of(date, LocalTime.of(12, 34, 56));
        Timestamp sqlTimestamp = Timestamp.valueOf(dateTime);

        assertBind((ps, i) -> ps.setDate(i, sqlDate), useLegacyPreparedStatements)
                .resultsIn("date", "DATE '2001-05-06'")
                .roundTripsAs(Types.DATE, sqlDate);

        assertBind((ps, i) -> ps.setObject(i, sqlDate), useLegacyPreparedStatements)
                .resultsIn("date", "DATE '2001-05-06'")
                .roundTripsAs(Types.DATE, sqlDate);

        assertBind((ps, i) -> ps.setObject(i, sqlDate, Types.DATE), useLegacyPreparedStatements)
                .resultsIn("date", "DATE '2001-05-06'")
                .roundTripsAs(Types.DATE, sqlDate);

        assertBind((ps, i) -> ps.setObject(i, sqlTimestamp, Types.DATE), useLegacyPreparedStatements)
                .resultsIn("date", "DATE '2001-05-06'")
                .roundTripsAs(Types.DATE, sqlDate);

        assertBind((ps, i) -> ps.setObject(i, javaDate, Types.DATE), useLegacyPreparedStatements)
                .resultsIn("date", "DATE '2001-05-06'")
                .roundTripsAs(Types.DATE, sqlDate);

        assertBind((ps, i) -> ps.setObject(i, date, Types.DATE), useLegacyPreparedStatements)
                .resultsIn("date", "DATE '2001-05-06'")
                .roundTripsAs(Types.DATE, sqlDate);

        assertBind((ps, i) -> ps.setObject(i, dateTime, Types.DATE), useLegacyPreparedStatements)
                .resultsIn("date", "DATE '2001-05-06'")
                .roundTripsAs(Types.DATE, sqlDate);

        assertBind((ps, i) -> ps.setObject(i, "2001-05-06", Types.DATE), useLegacyPreparedStatements)
                .resultsIn("date", "DATE '2001-05-06'")
                .roundTripsAs(Types.DATE, sqlDate);
    }

    @Test
    public void testConvertLocalDate()
            throws SQLException
    {
        testConvertLocalDate(true);
        testConvertLocalDate(false);
    }

    private void testConvertLocalDate(boolean useLegacyPreparedStatements)
            throws SQLException
    {
        LocalDate date = LocalDate.of(2001, 5, 6);

        assertBind((ps, i) -> ps.setObject(i, date), useLegacyPreparedStatements)
                .resultsIn("date", "DATE '2001-05-06'")
                .roundTripsAs(Types.DATE, Date.valueOf(date));

        assertBind((ps, i) -> ps.setObject(i, date, Types.DATE), useLegacyPreparedStatements)
                .resultsIn("date", "DATE '2001-05-06'")
                .roundTripsAs(Types.DATE, Date.valueOf(date));

        assertBind((ps, i) -> ps.setObject(i, date, Types.TIME), useLegacyPreparedStatements)
                .isInvalid("Cannot convert instance of java.time.LocalDate to time");

        assertBind((ps, i) -> ps.setObject(i, date, Types.TIME_WITH_TIMEZONE), useLegacyPreparedStatements)
                .isInvalid("Cannot convert instance of java.time.LocalDate to time with time zone");

        assertBind((ps, i) -> ps.setObject(i, date, Types.TIMESTAMP), useLegacyPreparedStatements)
                .isInvalid("Cannot convert instance of java.time.LocalDate to timestamp");

        assertBind((ps, i) -> ps.setObject(i, date, Types.TIMESTAMP_WITH_TIMEZONE), useLegacyPreparedStatements)
                .isInvalid("Cannot convert instance of java.time.LocalDate to timestamp with time zone");

        LocalDate jvmGapDate = LocalDate.of(1970, 1, 1);
        checkIsGap(ZoneId.systemDefault(), jvmGapDate.atTime(LocalTime.MIDNIGHT));

        assertBind((ps, i) -> ps.setObject(i, jvmGapDate), useLegacyPreparedStatements)
                .resultsIn("date", "DATE '1970-01-01'")
                .roundTripsAs(Types.DATE, Date.valueOf(jvmGapDate));

        assertBind((ps, i) -> ps.setObject(i, jvmGapDate, Types.DATE), useLegacyPreparedStatements)
                .roundTripsAs(Types.DATE, Date.valueOf(jvmGapDate));
    }

    @Test
    public void testConvertTime()
            throws SQLException
    {
        testConvertTime(true);
        testConvertTime(false);
    }

    private void testConvertTime(boolean useLegacyPreparedStatements)
            throws SQLException
    {
        LocalTime time = LocalTime.of(12, 34, 56);
        Time sqlTime = Time.valueOf(time);
        java.util.Date javaDate = new java.util.Date(sqlTime.getTime());
        LocalDateTime dateTime = LocalDateTime.of(LocalDate.of(2001, 5, 6), time);
        Timestamp sqlTimestamp = Timestamp.valueOf(dateTime);

        assertBind((ps, i) -> ps.setTime(i, sqlTime), useLegacyPreparedStatements)
                .resultsIn("time(3)", "TIME '12:34:56.000'")
                .roundTripsAs(Types.TIME, sqlTime);

        assertBind((ps, i) -> ps.setObject(i, sqlTime), useLegacyPreparedStatements)
                .resultsIn("time(3)", "TIME '12:34:56.000'")
                .roundTripsAs(Types.TIME, sqlTime);

        assertBind((ps, i) -> ps.setObject(i, sqlTime, Types.TIME), useLegacyPreparedStatements)
                .resultsIn("time(3)", "TIME '12:34:56.000'")
                .roundTripsAs(Types.TIME, sqlTime);

        assertBind((ps, i) -> ps.setObject(i, sqlTimestamp, Types.TIME), useLegacyPreparedStatements)
                .resultsIn("time(3)", "TIME '12:34:56.000'")
                .roundTripsAs(Types.TIME, sqlTime);

        assertBind((ps, i) -> ps.setObject(i, javaDate, Types.TIME), useLegacyPreparedStatements)
                .resultsIn("time(3)", "TIME '12:34:56.000'")
                .roundTripsAs(Types.TIME, sqlTime);

        assertBind((ps, i) -> ps.setObject(i, dateTime, Types.TIME), useLegacyPreparedStatements)
                .resultsIn("time(0)", "TIME '12:34:56'")
                .roundTripsAs(Types.TIME, sqlTime);

        assertBind((ps, i) -> ps.setObject(i, "12:34:56", Types.TIME), useLegacyPreparedStatements)
                .resultsIn("time(0)", "TIME '12:34:56'")
                .roundTripsAs(Types.TIME, sqlTime);

        assertBind((ps, i) -> ps.setObject(i, "12:34:56.123", Types.TIME), useLegacyPreparedStatements).resultsIn("time(3)", "TIME '12:34:56.123'");
        assertBind((ps, i) -> ps.setObject(i, "12:34:56.123456", Types.TIME), useLegacyPreparedStatements).resultsIn("time(6)", "TIME '12:34:56.123456'");

        assertBind((ps, i) -> ps.setObject(i, "12:34:56.123456789", Types.TIME), useLegacyPreparedStatements)
                .resultsIn("time(9)", "TIME '12:34:56.123456789'");

        assertBind((ps, i) -> ps.setObject(i, "12:34:56.123456789012", Types.TIME), useLegacyPreparedStatements)
                .resultsIn("time(12)", "TIME '12:34:56.123456789012'");

        Time timeWithDecisecond = new Time(sqlTime.getTime() + 100);
        assertBind((ps, i) -> ps.setObject(i, timeWithDecisecond), useLegacyPreparedStatements)
                .resultsIn("time(3)", "TIME '12:34:56.100'")
                .roundTripsAs(Types.TIME, timeWithDecisecond);

        assertBind((ps, i) -> ps.setObject(i, timeWithDecisecond, Types.TIME), useLegacyPreparedStatements)
                .resultsIn("time(3)", "TIME '12:34:56.100'")
                .roundTripsAs(Types.TIME, timeWithDecisecond);

        Time timeWithMillisecond = new Time(sqlTime.getTime() + 123);
        assertBind((ps, i) -> ps.setObject(i, timeWithMillisecond), useLegacyPreparedStatements)
                .resultsIn("time(3)", "TIME '12:34:56.123'")
                .roundTripsAs(Types.TIME, timeWithMillisecond);

        assertBind((ps, i) -> ps.setObject(i, timeWithMillisecond, Types.TIME), useLegacyPreparedStatements)
                .resultsIn("time(3)", "TIME '12:34:56.123'")
                .roundTripsAs(Types.TIME, timeWithMillisecond);
    }

    @Test
    public void testConvertTimeWithTimeZone()
            throws SQLException
    {
        testConvertTimeWithTimeZone(true);
        testConvertTimeWithTimeZone(false);
    }

    private void testConvertTimeWithTimeZone(boolean useLegacyPreparedStatements)
            throws SQLException
    {
        // zero fraction
        assertBind((ps, i) -> ps.setObject(i, OffsetTime.of(12, 34, 56, 0, UTC), Types.TIME_WITH_TIMEZONE), useLegacyPreparedStatements)
                .resultsIn("time(0) with time zone", "TIME '12:34:56+00:00'")
                .roundTripsAs(Types.TIME_WITH_TIMEZONE, toSqlTime(LocalTime.of(5, 34, 56)));

        // setObject with implicit type
        assertBind((ps, i) -> ps.setObject(i, OffsetTime.of(12, 34, 56, 0, UTC)), useLegacyPreparedStatements)
                .resultsIn("time(0) with time zone", "TIME '12:34:56+00:00'");

        // setObject with JDBCType
        assertBind((ps, i) -> ps.setObject(i, OffsetTime.of(12, 34, 56, 0, UTC), JDBCType.TIME_WITH_TIMEZONE), useLegacyPreparedStatements)
                .resultsIn("time(0) with time zone", "TIME '12:34:56+00:00'");

        // millisecond precision
        assertBind((ps, i) -> ps.setObject(i, OffsetTime.of(12, 34, 56, 555_000_000, UTC), Types.TIME_WITH_TIMEZONE), useLegacyPreparedStatements)
                .resultsIn("time(3) with time zone", "TIME '12:34:56.555+00:00'")
                .roundTripsAs(Types.TIME_WITH_TIMEZONE, toSqlTime(LocalTime.of(5, 34, 56, 555_000_000)));

        // microsecond precision
        assertBind((ps, i) -> ps.setObject(i, OffsetTime.of(12, 34, 56, 555_555_000, UTC), Types.TIME_WITH_TIMEZONE), useLegacyPreparedStatements)
                .resultsIn("time(6) with time zone", "TIME '12:34:56.555555+00:00'")
                .roundTripsAs(Types.TIME_WITH_TIMEZONE, toSqlTime(LocalTime.of(5, 34, 56, 556_000_000)));

        // nanosecond precision
        assertBind((ps, i) -> ps.setObject(i, OffsetTime.of(12, 34, 56, 555_555_555, UTC), Types.TIME_WITH_TIMEZONE), useLegacyPreparedStatements)
                .resultsIn("time(9) with time zone", "TIME '12:34:56.555555555+00:00'")
                .roundTripsAs(Types.TIME_WITH_TIMEZONE, toSqlTime(LocalTime.of(5, 34, 56, 556_000_000)));

        // positive offset
        assertBind((ps, i) -> ps.setObject(i, OffsetTime.of(12, 34, 56, 123_456_789, ZoneOffset.ofHoursMinutes(7, 35)), Types.TIME_WITH_TIMEZONE), useLegacyPreparedStatements)
                .resultsIn("time(9) with time zone", "TIME '12:34:56.123456789+07:35'");
        // TODO (https://github.com/trinodb/trino/issues/6351) the result is not as expected here:
        //      .roundTripsAs(Types.TIME_WITH_TIMEZONE, toSqlTime(LocalTime.of(20, 59, 56, 123_000_000)));

        // negative offset
        assertBind((ps, i) -> ps.setObject(i, OffsetTime.of(12, 34, 56, 123_456_789, ZoneOffset.ofHoursMinutes(-7, -35)), Types.TIME_WITH_TIMEZONE), useLegacyPreparedStatements)
                .resultsIn("time(9) with time zone", "TIME '12:34:56.123456789-07:35'")
                .roundTripsAs(Types.TIME_WITH_TIMEZONE, toSqlTime(LocalTime.of(13, 9, 56, 123_000_000)));

        // String as TIME WITH TIME ZONE
        assertBind((ps, i) -> ps.setObject(i, "12:34:56.123 +05:45", Types.TIME_WITH_TIMEZONE), useLegacyPreparedStatements)
                .resultsIn("time(3) with time zone", "TIME '12:34:56.123 +05:45'");

        assertBind((ps, i) -> ps.setObject(i, "12:34:56.123456 +05:45", Types.TIME_WITH_TIMEZONE), useLegacyPreparedStatements)
                .resultsIn("time(6) with time zone", "TIME '12:34:56.123456 +05:45'");

        assertBind((ps, i) -> ps.setObject(i, "12:34:56.123456789 +05:45", Types.TIME_WITH_TIMEZONE), useLegacyPreparedStatements)
                .resultsIn("time(9) with time zone", "TIME '12:34:56.123456789 +05:45'");

        assertBind((ps, i) -> ps.setObject(i, "12:34:56.123456789012 +05:45", Types.TIME_WITH_TIMEZONE), useLegacyPreparedStatements)
                .resultsIn("time(12) with time zone", "TIME '12:34:56.123456789012 +05:45'");
    }

    @Test
    public void testConvertTimestamp()
            throws SQLException
    {
        testConvertTimestamp(true);
        testConvertTimestamp(false);
    }

    private void testConvertTimestamp(boolean useLegacyPreparedStatements)
            throws SQLException
    {
        LocalDateTime dateTime = LocalDateTime.of(2001, 5, 6, 12, 34, 56);
        Date sqlDate = Date.valueOf(dateTime.toLocalDate());
        Time sqlTime = Time.valueOf(dateTime.toLocalTime());
        Timestamp sqlTimestamp = Timestamp.valueOf(dateTime);
        Timestamp sameInstantInWarsawZone = Timestamp.valueOf(dateTime.atZone(ZoneId.systemDefault()).withZoneSameInstant(ZoneId.of("Europe/Warsaw")).toLocalDateTime());
        java.util.Date javaDate = java.util.Date.from(dateTime.atZone(ZoneId.systemDefault()).toInstant());

        assertBind((ps, i) -> ps.setTimestamp(i, sqlTimestamp), useLegacyPreparedStatements)
                .resultsIn("timestamp(3)", "TIMESTAMP '2001-05-06 12:34:56.000'")
                .roundTripsAs(Types.TIMESTAMP, sqlTimestamp);

        assertBind((ps, i) -> ps.setTimestamp(i, sqlTimestamp, null), useLegacyPreparedStatements)
                .resultsIn("timestamp(3)", "TIMESTAMP '2001-05-06 12:34:56.000'")
                .roundTripsAs(Types.TIMESTAMP, sqlTimestamp);

        assertBind((ps, i) -> ps.setTimestamp(i, sqlTimestamp, Calendar.getInstance()), useLegacyPreparedStatements)
                .resultsIn("timestamp(3)", "TIMESTAMP '2001-05-06 12:34:56.000'")
                .roundTripsAs(Types.TIMESTAMP, sqlTimestamp);

        assertBind((ps, i) -> ps.setTimestamp(i, sqlTimestamp, Calendar.getInstance(TimeZone.getTimeZone(ZoneId.of("Europe/Warsaw")))), useLegacyPreparedStatements)
                .resultsIn("timestamp(3)", "TIMESTAMP '2001-05-06 20:34:56.000'")
                .roundTripsAs(Types.TIMESTAMP, sameInstantInWarsawZone);

        assertBind((ps, i) -> ps.setObject(i, sqlTimestamp), useLegacyPreparedStatements)
                .resultsIn("timestamp(3)", "TIMESTAMP '2001-05-06 12:34:56.000'")
                .roundTripsAs(Types.TIMESTAMP, sqlTimestamp);

        assertBind((ps, i) -> ps.setObject(i, sqlDate, Types.TIMESTAMP), useLegacyPreparedStatements)
                .resultsIn("timestamp(3)", "TIMESTAMP '2001-05-06 00:00:00.000'")
                .roundTripsAs(Types.TIMESTAMP, new Timestamp(sqlDate.getTime()));

        assertBind((ps, i) -> ps.setObject(i, sqlTime, Types.TIMESTAMP), useLegacyPreparedStatements)
                .resultsIn("timestamp(3)", "TIMESTAMP '1970-01-01 12:34:56.000'")
                .roundTripsAs(Types.TIMESTAMP, new Timestamp(sqlTime.getTime()));

        assertBind((ps, i) -> ps.setObject(i, sqlTimestamp, Types.TIMESTAMP), useLegacyPreparedStatements)
                .resultsIn("timestamp(3)", "TIMESTAMP '2001-05-06 12:34:56.000'")
                .roundTripsAs(Types.TIMESTAMP, sqlTimestamp);

        assertBind((ps, i) -> ps.setObject(i, javaDate, Types.TIMESTAMP), useLegacyPreparedStatements)
                .resultsIn("timestamp(3)", "TIMESTAMP '2001-05-06 12:34:56.000'")
                .roundTripsAs(Types.TIMESTAMP, sqlTimestamp);

        assertBind((ps, i) -> ps.setObject(i, dateTime, Types.TIMESTAMP), useLegacyPreparedStatements)
                .resultsIn("timestamp(0)", "TIMESTAMP '2001-05-06 12:34:56'")
                .roundTripsAs(Types.TIMESTAMP, sqlTimestamp);

        assertBind((ps, i) -> ps.setObject(i, "2001-05-06 12:34:56", Types.TIMESTAMP), useLegacyPreparedStatements)
                .resultsIn("timestamp(0)", "TIMESTAMP '2001-05-06 12:34:56'")
                .roundTripsAs(Types.TIMESTAMP, sqlTimestamp);

        assertBind((ps, i) -> ps.setObject(i, "2001-05-06 12:34:56.123", Types.TIMESTAMP), useLegacyPreparedStatements)
                .resultsIn("timestamp(3)", "TIMESTAMP '2001-05-06 12:34:56.123'");

        assertBind((ps, i) -> ps.setObject(i, "2001-05-06 12:34:56.123456", Types.TIMESTAMP), useLegacyPreparedStatements)
                .resultsIn("timestamp(6)", "TIMESTAMP '2001-05-06 12:34:56.123456'");

        assertBind((ps, i) -> ps.setObject(i, "2001-05-06 12:34:56.123456789", Types.TIMESTAMP), useLegacyPreparedStatements)
                .resultsIn("timestamp(9)", "TIMESTAMP '2001-05-06 12:34:56.123456789'");

        assertBind((ps, i) -> ps.setObject(i, "2001-05-06 12:34:56.123456789012", Types.TIMESTAMP), useLegacyPreparedStatements)
                .resultsIn("timestamp(12)", "TIMESTAMP '2001-05-06 12:34:56.123456789012'");

        Timestamp timestampWithWithDecisecond = new Timestamp(sqlTimestamp.getTime() + 100);
        assertBind((ps, i) -> ps.setTimestamp(i, timestampWithWithDecisecond), useLegacyPreparedStatements)
                .resultsIn("timestamp(3)", "TIMESTAMP '2001-05-06 12:34:56.100'")
                .roundTripsAs(Types.TIMESTAMP, timestampWithWithDecisecond);

        assertBind((ps, i) -> ps.setObject(i, timestampWithWithDecisecond), useLegacyPreparedStatements)
                .resultsIn("timestamp(3)", "TIMESTAMP '2001-05-06 12:34:56.100'")
                .roundTripsAs(Types.TIMESTAMP, timestampWithWithDecisecond);

        assertBind((ps, i) -> ps.setObject(i, timestampWithWithDecisecond, Types.TIMESTAMP), useLegacyPreparedStatements)
                .resultsIn("timestamp(3)", "TIMESTAMP '2001-05-06 12:34:56.100'")
                .roundTripsAs(Types.TIMESTAMP, timestampWithWithDecisecond);

        Timestamp timestampWithMillisecond = new Timestamp(sqlTimestamp.getTime() + 123);
        assertBind((ps, i) -> ps.setTimestamp(i, timestampWithMillisecond), useLegacyPreparedStatements)
                .resultsIn("timestamp(3)", "TIMESTAMP '2001-05-06 12:34:56.123'")
                .roundTripsAs(Types.TIMESTAMP, timestampWithMillisecond);

        assertBind((ps, i) -> ps.setObject(i, timestampWithMillisecond), useLegacyPreparedStatements)
                .resultsIn("timestamp(3)", "TIMESTAMP '2001-05-06 12:34:56.123'")
                .roundTripsAs(Types.TIMESTAMP, timestampWithMillisecond);

        assertBind((ps, i) -> ps.setObject(i, timestampWithMillisecond, Types.TIMESTAMP), useLegacyPreparedStatements)
                .resultsIn("timestamp(3)", "TIMESTAMP '2001-05-06 12:34:56.123'")
                .roundTripsAs(Types.TIMESTAMP, timestampWithMillisecond);
    }

    @Test
    public void testConvertTimestampWithTimeZone()
            throws SQLException
    {
        testConvertTimestampWithTimeZone(true);
        testConvertTimestampWithTimeZone(false);
    }

    private void testConvertTimestampWithTimeZone(boolean useLegacyPreparedStatements)
            throws SQLException
    {
        // TODO (https://github.com/trinodb/trino/issues/6299) support ZonedDateTime

        // String as TIMESTAMP WITH TIME ZONE
        assertBind((ps, i) -> ps.setObject(i, "1970-01-01 12:34:56.123 +05:45", Types.TIMESTAMP_WITH_TIMEZONE), useLegacyPreparedStatements)
                .resultsIn("timestamp(3) with time zone", "TIMESTAMP '1970-01-01 12:34:56.123 +05:45'");

        assertBind((ps, i) -> ps.setObject(i, "1970-01-01 12:34:56.123456 +05:45", Types.TIMESTAMP_WITH_TIMEZONE), useLegacyPreparedStatements)
                .resultsIn("timestamp(6) with time zone", "TIMESTAMP '1970-01-01 12:34:56.123456 +05:45'");

        assertBind((ps, i) -> ps.setObject(i, "1970-01-01 12:34:56.123456789 +05:45", Types.TIMESTAMP_WITH_TIMEZONE), useLegacyPreparedStatements)
                .resultsIn("timestamp(9) with time zone", "TIMESTAMP '1970-01-01 12:34:56.123456789 +05:45'");

        assertBind((ps, i) -> ps.setObject(i, "1970-01-01 12:34:56.123456789012 +05:45", Types.TIMESTAMP_WITH_TIMEZONE), useLegacyPreparedStatements)
                .resultsIn("timestamp(12) with time zone", "TIMESTAMP '1970-01-01 12:34:56.123456789012 +05:45'");
    }

    @Test
    public void testInvalidConversions()
            throws SQLException
    {
        testInvalidConversions(true);
        testInvalidConversions(false);
    }

    private void testInvalidConversions(boolean useLegacyPreparedStatements)
            throws SQLException
    {
        assertBind((ps, i) -> ps.setObject(i, String.class), useLegacyPreparedStatements).isInvalid("Unsupported object type: java.lang.Class");
        assertBind((ps, i) -> ps.setObject(i, String.class, Types.BIGINT), useLegacyPreparedStatements)
                .isInvalid("Cannot convert instance of java.lang.Class to SQL type " + Types.BIGINT);
        assertBind((ps, i) -> ps.setObject(i, "abc", Types.SMALLINT), useLegacyPreparedStatements)
                .isInvalid("Cannot convert instance of java.lang.String to SQL type " + Types.SMALLINT);
    }

    @Test
    public void testLegacyPreparedStatementsTrue()
            throws Exception
    {
        testLegacyPreparedStatementsSetting(true,
                "EXECUTE %statement% USING %values%");
    }

    @Test
    public void testLegacyPreparedStatementsFalse()
            throws Exception
    {
        testLegacyPreparedStatementsSetting(false,
                "EXECUTE IMMEDIATE '%query%' USING %values%");
    }

    private BindAssertion assertBind(Binder binder, boolean useLegacyPreparedStatements)
    {
        return new BindAssertion(() -> this.createConnection(useLegacyPreparedStatements), binder);
    }

    private Connection createConnection(boolean useLegacyPreparedStatements)
            throws SQLException
    {
        String url = format("jdbc:trino://%s?legacyPreparedStatements=" + useLegacyPreparedStatements, server.getAddress());
        return DriverManager.getConnection(url, "test", null);
    }

    private Connection createConnection(String catalog, String schema, boolean useLegacyPreparedStatements)
            throws SQLException
    {
        String url = format("jdbc:trino://%s/%s/%s?legacyPreparedStatements=" + useLegacyPreparedStatements, server.getAddress(), catalog, schema);
        return DriverManager.getConnection(url, "test", null);
    }

    private void testLegacyPreparedStatementsSetting(boolean legacyPreparedStatements, String expectedSql)
            throws Exception
    {
        String selectSql = "SELECT * FROM blackhole.blackhole.test_table WHERE x = ? AND y = ? AND y <> 'Test'";
        String insertSql = "INSERT INTO blackhole.blackhole.test_table (x, y) VALUES (?, ?)";

        try (Connection connection = createConnection(legacyPreparedStatements)) {
            try (Statement statement = connection.createStatement()) {
                assertEquals(statement.executeUpdate("CREATE TABLE blackhole.blackhole.test_table (x bigint, y varchar)"), 0);
            }

            try (PreparedStatement ps = connection.prepareStatement(selectSql)) {
                ps.setInt(1, 42);
                ps.setString(2, "value1's");

                ps.executeQuery();
                checkSQLExecuted(connection, expectedSql
                        .replace("%statement%", "statement1")
                        .replace("%query%", selectSql.replace("'", "''"))
                        .replace("%values%", "INTEGER '42', 'value1''s'"));
            }

            try (PreparedStatement ps = connection.prepareStatement(selectSql)) {
                ps.setInt(1, 42);
                ps.setString(2, "value1's");

                ps.execute();
                checkSQLExecuted(connection, expectedSql
                        .replace("%statement%", "statement2")
                        .replace("%query%", selectSql.replace("'", "''"))
                        .replace("%values%", "INTEGER '42', 'value1''s'"));
            }

            try (PreparedStatement ps = connection.prepareStatement(insertSql)) {
                ps.setInt(1, 42);
                ps.setString(2, "value1's");

                ps.executeLargeUpdate();
                checkSQLExecuted(connection, expectedSql
                        .replace("%statement%", "statement3")
                        .replace("%query%", insertSql.replace("'", "''"))
                        .replace("%values%", "INTEGER '42', 'value1''s'"));
            }

            try (PreparedStatement ps = connection.prepareStatement(insertSql)) {
                ps.setInt(1, 42);
                ps.setString(2, "value1's");
                ps.addBatch();

                ps.setInt(1, 43);
                ps.setString(2, "value2's");
                ps.addBatch();

                ps.executeBatch();
                String statement4 = expectedSql
                        .replace("%statement%", "statement4")
                        .replace("%query%", insertSql.replace("'", "''"));
                checkSQLExecuted(connection, statement4
                        .replace("%values%", "INTEGER '42', 'value1''s'"));
                checkSQLExecuted(connection, statement4
                        .replace("%values%", "INTEGER '43', 'value2''s'"));
            }

            try (Statement statement = connection.createStatement()) {
                assertEquals(statement.executeUpdate("DROP TABLE blackhole.blackhole.test_table"), 0);
            }
        }
    }

    private void checkSQLExecuted(Connection connection, String expectedSql)
    {
        String sql = format("SELECT state FROM system.runtime.queries WHERE query = '%s'", expectedSql.replace("'", "''"));

        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            assertTrue(resultSet.next(), "Cannot find SQL query " + expectedSql);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static class BindAssertion
    {
        private final ConnectionFactory connectionFactory;
        private final Binder binder;

        public BindAssertion(ConnectionFactory connectionFactory, Binder binder)
        {
            this.connectionFactory = requireNonNull(connectionFactory, "connectionFactory is null");
            this.binder = requireNonNull(binder, "binder is null");
        }

        public BindAssertion isInvalid(String expectedMessage)
                throws SQLException
        {
            try (Connection connection = connectionFactory.createConnection();
                    PreparedStatement statement = connection.prepareStatement("SELECT ?")) {
                assertThatThrownBy(() -> binder.bind(statement, 1))
                        .isInstanceOf(SQLException.class)
                        .hasMessage(expectedMessage);
            }

            return this;
        }

        public BindAssertion roundTripsAs(int expectedSqlType, Object expectedValue)
                throws SQLException
        {
            try (Connection connection = connectionFactory.createConnection();
                    PreparedStatement statement = connection.prepareStatement("SELECT ?")) {
                binder.bind(statement, 1);

                try (ResultSet rs = statement.executeQuery()) {
                    verify(rs.next(), "no row returned");
                    assertEquals(rs.getObject(1), expectedValue);
                    verify(!rs.next(), "unexpected second row");

                    assertEquals(rs.getMetaData().getColumnType(1), expectedSqlType);
                }
            }

            return this;
        }

        public BindAssertion resultsIn(String type, String expectedValueLiteral)
                throws SQLException
        {
            String sql = "" +
                    "SELECT " +
                    "  typeof(bound) type_of_bind, " +
                    "  bound, " +
                    "  CAST(bound AS varchar) bound_as_varchar, " +
                    "  typeof(literal) type_of_literal, " +
                    "  literal, " +
                    "  CAST(literal AS varchar) literal_as_varchar, " +
                    "  bound = literal are_equal " +
                    "FROM (VALUES (?, " + expectedValueLiteral + ")) t(bound, literal)";

            try (Connection connection = connectionFactory.createConnection();
                    PreparedStatement statement = connection.prepareStatement(sql)) {
                binder.bind(statement, 1);

                try (ResultSet rs = statement.executeQuery()) {
                    verify(rs.next(), "no row returned");
                    assertThat(rs.getString("type_of_bind")).as("type_of_bind")
                            .isEqualTo(type);
                    assertThat(rs.getString("type_of_literal")).as("type_of_literal (sanity check)")
                            .isEqualTo(type);
                    assertThat(rs.getString("bound_as_varchar")).as("bound should cast to VARCHAR the same way as literal " + expectedValueLiteral)
                            .isEqualTo(rs.getString("literal_as_varchar"));
                    // TODO (https://github.com/trinodb/trino/issues/6242) ResultSet.getObject sometimes fails
                    //  assertThat(rs.getObject("bound")).as("bound value should round trip the same way as literal " + expectedValueLiteral)
                    //        .isEqualTo(rs.getObject("literal"));
                    assertThat(rs.getObject("are_equal")).as("Expected bound value to be equal to " + expectedValueLiteral)
                            .isEqualTo(true);
                    verify(!rs.next(), "unexpected second row");
                }
            }

            return this;
        }
    }

    private interface Binder
    {
        void bind(PreparedStatement ps, int i)
                throws SQLException;
    }

    private interface ConnectionFactory
    {
        Connection createConnection()
                throws SQLException;
    }

    private static void checkIsGap(ZoneId zone, LocalDateTime dateTime)
    {
        verify(isGap(zone, dateTime), "Expected %s to be a gap in %s", dateTime, zone);
    }

    private static boolean isGap(ZoneId zone, LocalDateTime dateTime)
    {
        return zone.getRules().getValidOffsets(dateTime).isEmpty();
    }
}
