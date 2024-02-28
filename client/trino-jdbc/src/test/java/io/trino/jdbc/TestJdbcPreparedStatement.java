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
import org.junit.jupiter.api.parallel.Execution;

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
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.sql.ParameterMetaData.parameterModeUnknown;
import static java.sql.ParameterMetaData.parameterNullableUnknown;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
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

    private void testExecuteQuery(boolean explicitPrepare)
            throws Exception
    {
        try (Connection connection = createConnection(explicitPrepare);
                PreparedStatement statement = connection.prepareStatement("SELECT ?, ?")) {
            statement.setInt(1, 123);
            statement.setString(2, "hello");

            try (ResultSet rs = statement.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt(1)).isEqualTo(123);
                assertThat(rs.getString(2)).isEqualTo("hello");
                assertThat(rs.next()).isFalse();
            }

            assertThat(statement.execute()).isTrue();
            try (ResultSet rs = statement.getResultSet()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt(1)).isEqualTo(123);
                assertThat(rs.getString(2)).isEqualTo("hello");
                assertThat(rs.next()).isFalse();
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

    private void testGetMetadata(boolean explicitPrepare)
            throws Exception
    {
        String tableName = "test_get_metadata_" + randomNameSuffix();
        try (Connection connection = createConnection("blackhole", "blackhole", explicitPrepare)) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE " + tableName + " (" +
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
                    "SELECT * FROM " + tableName)) {
                ResultSetMetaData metadata = statement.getMetaData();
                assertThat(metadata.getColumnCount()).isEqualTo(8);
                for (int i = 1; i <= metadata.getColumnCount(); i++) {
                    assertThat(metadata.getCatalogName(i)).isEqualTo("blackhole");
                    assertThat(metadata.getSchemaName(i)).isEqualTo("blackhole");
                    assertThat(metadata.getTableName(i)).isEqualTo(tableName);
                }

                assertThat(metadata.getColumnName(1)).isEqualTo("c_boolean");
                assertThat(metadata.getColumnTypeName(1)).isEqualTo("boolean");

                assertThat(metadata.getColumnName(2)).isEqualTo("c_decimal");
                assertThat(metadata.getColumnTypeName(2)).isEqualTo("decimal(38,0)");

                assertThat(metadata.getColumnName(3)).isEqualTo("c_decimal_2");
                assertThat(metadata.getColumnTypeName(3)).isEqualTo("decimal(10,3)");

                assertThat(metadata.getColumnName(4)).isEqualTo("c_varchar");
                assertThat(metadata.getColumnTypeName(4)).isEqualTo("varchar");

                assertThat(metadata.getColumnName(5)).isEqualTo("c_varchar_2");
                assertThat(metadata.getColumnTypeName(5)).isEqualTo("varchar(10)");

                assertThat(metadata.getColumnName(6)).isEqualTo("c_row");
                assertThat(metadata.getColumnTypeName(6)).isEqualTo("row");

                assertThat(metadata.getColumnName(7)).isEqualTo("c_array");
                assertThat(metadata.getColumnTypeName(7)).isEqualTo("array");

                assertThat(metadata.getColumnName(8)).isEqualTo("c_map");
                assertThat(metadata.getColumnTypeName(8)).isEqualTo("map");
            }

            try (Statement statement = connection.createStatement()) {
                statement.execute("DROP TABLE " + tableName);
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

    private void testGetParameterMetaData(boolean explicitPrepare)
            throws Exception
    {
        String tableName = "test_get_parameterMetaData_" + randomNameSuffix();
        try (Connection connection = createConnection("blackhole", "blackhole", explicitPrepare)) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE " + tableName + " (" +
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
                    "SELECT ? FROM " + tableName + " WHERE c_boolean = ? AND c_decimal = ? " +
                            "AND c_decimal_2 = ? AND c_varchar = ? AND c_varchar_2 = ? AND c_row = ? " +
                            "AND c_array = ? AND c_map = ? AND c_tinyint = ? AND c_integer = ? AND c_bigint = ? " +
                            "AND c_smallint = ? AND c_real = ? AND c_double = ?")) {
                ParameterMetaData parameterMetaData = statement.getParameterMetaData();
                assertThat(parameterMetaData.getParameterCount()).isEqualTo(15);

                assertThat(parameterMetaData.getParameterClassName(1)).isEqualTo("unknown");
                assertThat(parameterMetaData.getParameterType(1)).isEqualTo(Types.NULL);
                assertThat(parameterMetaData.getParameterTypeName(1)).isEqualTo("unknown");
                assertThat(parameterMetaData.isNullable(1)).isEqualTo(parameterNullableUnknown);
                assertThat(parameterMetaData.isSigned(1)).isFalse();
                assertThat(parameterMetaData.getParameterMode(1)).isEqualTo(parameterModeUnknown);

                assertThat(parameterMetaData.getParameterClassName(2)).isEqualTo(Boolean.class.getName());
                assertThat(parameterMetaData.getParameterType(2)).isEqualTo(Types.BOOLEAN);
                assertThat(parameterMetaData.getParameterTypeName(2)).isEqualTo("boolean");
                assertThat(parameterMetaData.isNullable(2)).isEqualTo(parameterNullableUnknown);
                assertThat(parameterMetaData.isSigned(2)).isFalse();
                assertThat(parameterMetaData.getParameterMode(2)).isEqualTo(parameterModeUnknown);

                assertThat(parameterMetaData.getParameterClassName(3)).isEqualTo(BigDecimal.class.getName());
                assertThat(parameterMetaData.getParameterType(3)).isEqualTo(Types.DECIMAL);
                assertThat(parameterMetaData.getParameterTypeName(3)).isEqualTo("decimal");
                assertThat(parameterMetaData.isNullable(3)).isEqualTo(parameterNullableUnknown);
                assertThat(parameterMetaData.isSigned(3)).isTrue();
                assertThat(parameterMetaData.getParameterMode(3)).isEqualTo(parameterModeUnknown);

                assertThat(parameterMetaData.getParameterClassName(4)).isEqualTo(BigDecimal.class.getName());
                assertThat(parameterMetaData.getParameterType(4)).isEqualTo(Types.DECIMAL);
                assertThat(parameterMetaData.getParameterTypeName(4)).isEqualTo("decimal");
                assertThat(parameterMetaData.getPrecision(4)).isEqualTo(10);
                assertThat(parameterMetaData.getScale(4)).isEqualTo(3);
                assertThat(parameterMetaData.isNullable(4)).isEqualTo(parameterNullableUnknown);
                assertThat(parameterMetaData.isSigned(4)).isTrue();
                assertThat(parameterMetaData.getParameterMode(4)).isEqualTo(parameterModeUnknown);

                assertThat(parameterMetaData.getParameterClassName(5)).isEqualTo(String.class.getName());
                assertThat(parameterMetaData.getParameterType(5)).isEqualTo(Types.VARCHAR);
                assertThat(parameterMetaData.getParameterTypeName(5)).isEqualTo("varchar");
                assertThat(parameterMetaData.isNullable(5)).isEqualTo(parameterNullableUnknown);
                assertThat(parameterMetaData.isSigned(5)).isFalse();
                assertThat(parameterMetaData.getParameterMode(5)).isEqualTo(parameterModeUnknown);

                assertThat(parameterMetaData.getParameterClassName(6)).isEqualTo(String.class.getName());
                assertThat(parameterMetaData.getParameterType(6)).isEqualTo(Types.VARCHAR);
                assertThat(parameterMetaData.getParameterTypeName(6)).isEqualTo("varchar");
                assertThat(parameterMetaData.getPrecision(6)).isEqualTo(5);
                assertThat(parameterMetaData.isNullable(6)).isEqualTo(parameterNullableUnknown);
                assertThat(parameterMetaData.isSigned(6)).isFalse();
                assertThat(parameterMetaData.getParameterMode(6)).isEqualTo(parameterModeUnknown);

                assertThat(parameterMetaData.getParameterClassName(7)).isEqualTo(String.class.getName());
                assertThat(parameterMetaData.getParameterType(7)).isEqualTo(Types.JAVA_OBJECT);
                assertThat(parameterMetaData.getParameterTypeName(7)).isEqualTo("row");
                assertThat(parameterMetaData.isNullable(7)).isEqualTo(parameterNullableUnknown);
                assertThat(parameterMetaData.isSigned(7)).isFalse();
                assertThat(parameterMetaData.getParameterMode(7)).isEqualTo(parameterModeUnknown);

                assertThat(parameterMetaData.getParameterClassName(8)).isEqualTo(Array.class.getName());
                assertThat(parameterMetaData.getParameterType(8)).isEqualTo(Types.ARRAY);
                assertThat(parameterMetaData.getParameterTypeName(8)).isEqualTo("array");
                assertThat(parameterMetaData.isNullable(8)).isEqualTo(parameterNullableUnknown);
                assertThat(parameterMetaData.isSigned(8)).isFalse();
                assertThat(parameterMetaData.getParameterMode(8)).isEqualTo(parameterModeUnknown);

                assertThat(parameterMetaData.getParameterClassName(9)).isEqualTo(String.class.getName());
                assertThat(parameterMetaData.getParameterType(9)).isEqualTo(Types.JAVA_OBJECT);
                assertThat(parameterMetaData.getParameterTypeName(9)).isEqualTo("map");
                assertThat(parameterMetaData.isNullable(9)).isEqualTo(parameterNullableUnknown);
                assertThat(parameterMetaData.isSigned(9)).isFalse();
                assertThat(parameterMetaData.getParameterMode(9)).isEqualTo(parameterModeUnknown);

                assertThat(parameterMetaData.getParameterClassName(10)).isEqualTo(Byte.class.getName());
                assertThat(parameterMetaData.getParameterType(10)).isEqualTo(Types.TINYINT);
                assertThat(parameterMetaData.getParameterTypeName(10)).isEqualTo("tinyint");
                assertThat(parameterMetaData.isNullable(10)).isEqualTo(parameterNullableUnknown);
                assertThat(parameterMetaData.isSigned(10)).isTrue();
                assertThat(parameterMetaData.getParameterMode(10)).isEqualTo(parameterModeUnknown);

                assertThat(parameterMetaData.getParameterClassName(11)).isEqualTo(Integer.class.getName());
                assertThat(parameterMetaData.getParameterType(11)).isEqualTo(Types.INTEGER);
                assertThat(parameterMetaData.getParameterTypeName(11)).isEqualTo("integer");
                assertThat(parameterMetaData.isNullable(11)).isEqualTo(parameterNullableUnknown);
                assertThat(parameterMetaData.isSigned(11)).isTrue();
                assertThat(parameterMetaData.getParameterMode(11)).isEqualTo(parameterModeUnknown);

                assertThat(parameterMetaData.getParameterClassName(12)).isEqualTo(Long.class.getName());
                assertThat(parameterMetaData.getParameterType(12)).isEqualTo(Types.BIGINT);
                assertThat(parameterMetaData.getParameterTypeName(12)).isEqualTo("bigint");
                assertThat(parameterMetaData.isNullable(12)).isEqualTo(parameterNullableUnknown);
                assertThat(parameterMetaData.isSigned(12)).isTrue();
                assertThat(parameterMetaData.getParameterMode(12)).isEqualTo(parameterModeUnknown);

                assertThat(parameterMetaData.getParameterClassName(13)).isEqualTo(Short.class.getName());
                assertThat(parameterMetaData.getParameterType(13)).isEqualTo(Types.SMALLINT);
                assertThat(parameterMetaData.getParameterTypeName(13)).isEqualTo("smallint");
                assertThat(parameterMetaData.isNullable(13)).isEqualTo(parameterNullableUnknown);
                assertThat(parameterMetaData.isSigned(13)).isTrue();
                assertThat(parameterMetaData.getParameterMode(13)).isEqualTo(parameterModeUnknown);

                assertThat(parameterMetaData.getParameterClassName(14)).isEqualTo(Float.class.getName());
                assertThat(parameterMetaData.getParameterType(14)).isEqualTo(Types.REAL);
                assertThat(parameterMetaData.getParameterTypeName(14)).isEqualTo("real");
                assertThat(parameterMetaData.isNullable(14)).isEqualTo(parameterNullableUnknown);
                assertThat(parameterMetaData.isSigned(14)).isTrue();
                assertThat(parameterMetaData.getParameterMode(14)).isEqualTo(parameterModeUnknown);

                assertThat(parameterMetaData.getParameterClassName(15)).isEqualTo(Double.class.getName());
                assertThat(parameterMetaData.getParameterType(15)).isEqualTo(Types.DOUBLE);
                assertThat(parameterMetaData.getParameterTypeName(15)).isEqualTo("double");
                assertThat(parameterMetaData.isNullable(15)).isEqualTo(parameterNullableUnknown);
                assertThat(parameterMetaData.isSigned(15)).isTrue();
                assertThat(parameterMetaData.getParameterMode(15)).isEqualTo(parameterModeUnknown);
            }

            try (Statement statement = connection.createStatement()) {
                statement.execute("DROP TABLE " + tableName);
            }
        }
    }

    @Test
    public void testGetClientTypeSignatureFromTypeString()
    {
        ClientTypeSignature actualClientTypeSignature = TrinoPreparedStatement.getClientTypeSignatureFromTypeString("boolean");
        ClientTypeSignature expectedClientTypeSignature = new ClientTypeSignature("boolean", ImmutableList.of());
        assertThat(actualClientTypeSignature).isEqualTo(expectedClientTypeSignature);

        actualClientTypeSignature = TrinoPreparedStatement.getClientTypeSignatureFromTypeString("decimal(10,3)");
        expectedClientTypeSignature = new ClientTypeSignature("decimal", ImmutableList.of(
                ClientTypeSignatureParameter.ofLong(10),
                ClientTypeSignatureParameter.ofLong(3)));
        assertThat(actualClientTypeSignature).isEqualTo(expectedClientTypeSignature);

        actualClientTypeSignature = TrinoPreparedStatement.getClientTypeSignatureFromTypeString("varchar");
        expectedClientTypeSignature = new ClientTypeSignature("varchar", ImmutableList.of(ClientTypeSignatureParameter.ofLong(VARCHAR_UNBOUNDED_LENGTH)));
        assertThat(actualClientTypeSignature).isEqualTo(expectedClientTypeSignature);

        actualClientTypeSignature = TrinoPreparedStatement.getClientTypeSignatureFromTypeString("varchar(10)");
        expectedClientTypeSignature = new ClientTypeSignature("varchar", ImmutableList.of(ClientTypeSignatureParameter.ofLong(10)));
        assertThat(actualClientTypeSignature).isEqualTo(expectedClientTypeSignature);

        actualClientTypeSignature = TrinoPreparedStatement.getClientTypeSignatureFromTypeString("row(x integer, y array(integer))");
        expectedClientTypeSignature = new ClientTypeSignature("row", ImmutableList.of());
        assertThat(actualClientTypeSignature).isEqualTo(expectedClientTypeSignature);

        actualClientTypeSignature = TrinoPreparedStatement.getClientTypeSignatureFromTypeString("array(integer)");
        expectedClientTypeSignature = new ClientTypeSignature("array", ImmutableList.of());
        assertThat(actualClientTypeSignature).isEqualTo(expectedClientTypeSignature);

        actualClientTypeSignature = TrinoPreparedStatement.getClientTypeSignatureFromTypeString("map(integer, integer)");
        expectedClientTypeSignature = new ClientTypeSignature("map", ImmutableList.of());
        assertThat(actualClientTypeSignature).isEqualTo(expectedClientTypeSignature);

        actualClientTypeSignature = TrinoPreparedStatement.getClientTypeSignatureFromTypeString("timestamp(12) with time zone");
        expectedClientTypeSignature = new ClientTypeSignature("timestamp with time zone", ImmutableList.of(ClientTypeSignatureParameter.ofLong(12)));
        assertThat(actualClientTypeSignature).isEqualTo(expectedClientTypeSignature);

        actualClientTypeSignature = TrinoPreparedStatement.getClientTypeSignatureFromTypeString("time(13) with time zone");
        expectedClientTypeSignature = new ClientTypeSignature("time with time zone", ImmutableList.of(ClientTypeSignatureParameter.ofLong(13)));
        assertThat(actualClientTypeSignature).isEqualTo(expectedClientTypeSignature);
    }

    @Test
    public void testDeallocate()
            throws Exception
    {
        testDeallocate(true);
        testDeallocate(false);
    }

    private void testDeallocate(boolean explicitPrepare)
            throws Exception
    {
        try (Connection connection = createConnection(explicitPrepare)) {
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

    private void testCloseIdempotency(boolean explicitPrepare)
            throws Exception
    {
        try (Connection connection = createConnection(explicitPrepare)) {
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

    private void testLargePreparedStatement(boolean explicitPrepare)
            throws Exception
    {
        int elements = HEADER_SIZE_LIMIT + 1;
        try (Connection connection = createConnection(explicitPrepare);
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

    public void testExecuteUpdate(boolean explicitPrepare)
            throws Exception
    {
        String tableName = "test_execute_update_" + randomNameSuffix();
        try (Connection connection = createConnection("blackhole", "blackhole", explicitPrepare)) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE " + tableName + " (" +
                        "c_boolean boolean, " +
                        "c_bigint bigint, " +
                        "c_double double, " +
                        "c_decimal decimal, " +
                        "c_varchar varchar, " +
                        "c_varbinary varbinary, " +
                        "c_null bigint)");
            }

            try (PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO " + tableName + " VALUES (?, ?, ?, ?, ?, ?, ?)")) {
                statement.setBoolean(1, true);
                statement.setLong(2, 5L);
                statement.setDouble(3, 7.0d);
                statement.setBigDecimal(4, BigDecimal.valueOf(8L));
                statement.setString(5, "abc'xyz");
                statement.setBytes(6, "xyz".getBytes(UTF_8));
                statement.setNull(7, Types.BIGINT);

                assertThat(statement.executeUpdate()).isEqualTo(1);

                assertThat(statement.execute()).isFalse();
                assertThat(statement.getUpdateCount()).isEqualTo(1);
                assertThat(statement.getLargeUpdateCount()).isEqualTo(1);
            }

            try (Statement statement = connection.createStatement()) {
                statement.execute("DROP TABLE " + tableName);
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

    private void testExecuteBatch(boolean explicitPrepare)
            throws Exception
    {
        String tableName = "test_execute_batch_" + randomNameSuffix();
        try (Connection connection = createConnection("memory", "default", explicitPrepare)) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE " + tableName + "(c_int integer)");
            }

            try (PreparedStatement preparedStatement = connection.prepareStatement(
                    "INSERT INTO " + tableName + " VALUES (?)")) {
                // Run executeBatch before addBatch
                assertThat(preparedStatement.executeBatch()).isEqualTo(new int[] {});

                for (int i = 0; i < 3; i++) {
                    preparedStatement.setInt(1, i);
                    preparedStatement.addBatch();
                }
                assertThat(preparedStatement.executeBatch()).isEqualTo(new int[] {1, 1, 1});

                try (Statement statement = connection.createStatement()) {
                    ResultSet resultSet = statement.executeQuery("SELECT c_int FROM " + tableName);
                    assertThat(readRows(resultSet))
                            .containsExactlyInAnyOrder(
                                    list(0),
                                    list(1),
                                    list(2));
                }

                // Make sure the above executeBatch cleared existing batch
                assertThat(preparedStatement.executeBatch()).isEqualTo(new int[] {});

                // clearBatch removes added batch and cancel batch mode
                preparedStatement.setBoolean(1, true);
                preparedStatement.clearBatch();
                assertThat(preparedStatement.executeBatch()).isEqualTo(new int[] {});

                preparedStatement.setInt(1, 1);
                assertThat(preparedStatement.executeUpdate()).isEqualTo(1);
            }

            try (Statement statement = connection.createStatement()) {
                statement.execute("DROP TABLE " + tableName);
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

    private void testInvalidExecuteBatch(boolean explicitPrepare)
            throws Exception
    {
        String tableName = "test_execute_invalid_batch_" + randomNameSuffix();
        try (Connection connection = createConnection("blackhole", "blackhole", explicitPrepare)) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE " + tableName + "(c_int integer)");
            }

            try (PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO " + tableName + " VALUES (?)")) {
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
                statement.execute("DROP TABLE " + tableName);
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

    private void testPrepareMultiple(boolean explicitPrepare)
            throws Exception
    {
        try (Connection connection = createConnection(explicitPrepare);
                PreparedStatement statement1 = connection.prepareStatement("SELECT 123");
                PreparedStatement statement2 = connection.prepareStatement("SELECT 456")) {
            try (ResultSet rs = statement1.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong(1)).isEqualTo(123);
                assertThat(rs.next()).isFalse();
            }

            try (ResultSet rs = statement2.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong(1)).isEqualTo(456);
                assertThat(rs.next()).isFalse();
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

    private void testPrepareLarge(boolean explicitPrepare)
            throws Exception
    {
        String sql = format("SELECT '%s' = '%s'", repeat("x", 100_000), repeat("y", 100_000));
        try (Connection connection = createConnection(explicitPrepare);
                PreparedStatement statement = connection.prepareStatement(sql);
                ResultSet rs = statement.executeQuery()) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getBoolean(1)).isFalse();
            assertThat(rs.next()).isFalse();
        }
    }

    @Test
    public void testSetNull()
            throws Exception
    {
        testSetNull(true);
        testSetNull(false);
    }

    private void testSetNull(boolean explicitPrepare)
            throws Exception
    {
        assertSetNull(Types.BOOLEAN, explicitPrepare);
        assertSetNull(Types.BIT, Types.BOOLEAN, explicitPrepare);
        assertSetNull(Types.TINYINT, explicitPrepare);
        assertSetNull(Types.SMALLINT, explicitPrepare);
        assertSetNull(Types.INTEGER, explicitPrepare);
        assertSetNull(Types.BIGINT, explicitPrepare);
        assertSetNull(Types.REAL, explicitPrepare);
        assertSetNull(Types.FLOAT, Types.REAL, explicitPrepare);
        assertSetNull(Types.DECIMAL, explicitPrepare);
        assertSetNull(Types.NUMERIC, Types.DECIMAL, explicitPrepare);
        assertSetNull(Types.CHAR, explicitPrepare);
        assertSetNull(Types.NCHAR, Types.CHAR, explicitPrepare);
        assertSetNull(Types.VARCHAR, Types.VARCHAR, explicitPrepare);
        assertSetNull(Types.NVARCHAR, Types.VARCHAR, explicitPrepare);
        assertSetNull(Types.LONGVARCHAR, Types.VARCHAR, explicitPrepare);
        assertSetNull(Types.VARCHAR, Types.VARCHAR, explicitPrepare);
        assertSetNull(Types.CLOB, Types.VARCHAR, explicitPrepare);
        assertSetNull(Types.NCLOB, Types.VARCHAR, explicitPrepare);
        assertSetNull(Types.VARBINARY, Types.VARBINARY, explicitPrepare);
        assertSetNull(Types.VARBINARY, explicitPrepare);
        assertSetNull(Types.BLOB, Types.VARBINARY, explicitPrepare);
        assertSetNull(Types.DATE, explicitPrepare);
        assertSetNull(Types.TIME, explicitPrepare);
        assertSetNull(Types.TIMESTAMP, explicitPrepare);
        assertSetNull(Types.NULL, explicitPrepare);
    }

    private void assertSetNull(int sqlType, boolean explicitPrepare)
            throws SQLException
    {
        assertSetNull(sqlType, sqlType, explicitPrepare);
    }

    private void assertSetNull(int sqlType, int expectedSqlType, boolean explicitPrepare)
            throws SQLException
    {
        try (Connection connection = createConnection(explicitPrepare);
                PreparedStatement statement = connection.prepareStatement("SELECT ?")) {
            statement.setNull(1, sqlType);

            try (ResultSet rs = statement.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getObject(1)).isNull();
                assertThat(rs.wasNull()).isTrue();
                assertThat(rs.next()).isFalse();

                assertThat(rs.getMetaData().getColumnType(1)).isEqualTo(expectedSqlType);
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

    private void testConvertBoolean(boolean explicitPrepare)
            throws SQLException
    {
        assertBind((ps, i) -> ps.setBoolean(i, true), explicitPrepare).roundTripsAs(Types.BOOLEAN, true);
        assertBind((ps, i) -> ps.setBoolean(i, false), explicitPrepare).roundTripsAs(Types.BOOLEAN, false);
        assertBind((ps, i) -> ps.setObject(i, true), explicitPrepare).roundTripsAs(Types.BOOLEAN, true);
        assertBind((ps, i) -> ps.setObject(i, false), explicitPrepare).roundTripsAs(Types.BOOLEAN, false);

        for (int type : asList(Types.BOOLEAN, Types.BIT)) {
            assertBind((ps, i) -> ps.setObject(i, true, type), explicitPrepare).roundTripsAs(Types.BOOLEAN, true);
            assertBind((ps, i) -> ps.setObject(i, false, type), explicitPrepare).roundTripsAs(Types.BOOLEAN, false);
            assertBind((ps, i) -> ps.setObject(i, 13, type), explicitPrepare).roundTripsAs(Types.BOOLEAN, true);
            assertBind((ps, i) -> ps.setObject(i, 0, type), explicitPrepare).roundTripsAs(Types.BOOLEAN, false);
            assertBind((ps, i) -> ps.setObject(i, "1", type), explicitPrepare).roundTripsAs(Types.BOOLEAN, true);
            assertBind((ps, i) -> ps.setObject(i, "true", type), explicitPrepare).roundTripsAs(Types.BOOLEAN, true);
            assertBind((ps, i) -> ps.setObject(i, "0", type), explicitPrepare).roundTripsAs(Types.BOOLEAN, false);
            assertBind((ps, i) -> ps.setObject(i, "false", type), explicitPrepare).roundTripsAs(Types.BOOLEAN, false);
        }
    }

    @Test
    public void testConvertTinyint()
            throws SQLException
    {
        testConvertTinyint(true);
        testConvertTinyint(false);
    }

    private void testConvertTinyint(boolean explicitPrepare)
            throws SQLException
    {
        assertBind((ps, i) -> ps.setByte(i, (byte) 123), explicitPrepare).roundTripsAs(Types.TINYINT, (byte) 123);
        assertBind((ps, i) -> ps.setObject(i, (byte) 123), explicitPrepare).roundTripsAs(Types.TINYINT, (byte) 123);
        assertBind((ps, i) -> ps.setObject(i, (byte) 123, Types.TINYINT), explicitPrepare).roundTripsAs(Types.TINYINT, (byte) 123);
        assertBind((ps, i) -> ps.setObject(i, (short) 123, Types.TINYINT), explicitPrepare).roundTripsAs(Types.TINYINT, (byte) 123);
        assertBind((ps, i) -> ps.setObject(i, 123, Types.TINYINT), explicitPrepare).roundTripsAs(Types.TINYINT, (byte) 123);
        assertBind((ps, i) -> ps.setObject(i, 123L, Types.TINYINT), explicitPrepare).roundTripsAs(Types.TINYINT, (byte) 123);
        assertBind((ps, i) -> ps.setObject(i, 123.9f, Types.TINYINT), explicitPrepare).roundTripsAs(Types.TINYINT, (byte) 123);
        assertBind((ps, i) -> ps.setObject(i, 123.9d, Types.TINYINT), explicitPrepare).roundTripsAs(Types.TINYINT, (byte) 123);
        assertBind((ps, i) -> ps.setObject(i, BigInteger.valueOf(123), Types.TINYINT), explicitPrepare).roundTripsAs(Types.TINYINT, (byte) 123);
        assertBind((ps, i) -> ps.setObject(i, BigDecimal.valueOf(123), Types.TINYINT), explicitPrepare).roundTripsAs(Types.TINYINT, (byte) 123);
        assertBind((ps, i) -> ps.setObject(i, BigDecimal.valueOf(123.9), Types.TINYINT), explicitPrepare).roundTripsAs(Types.TINYINT, (byte) 123);
        assertBind((ps, i) -> ps.setObject(i, "123", Types.TINYINT), explicitPrepare).roundTripsAs(Types.TINYINT, (byte) 123);
        assertBind((ps, i) -> ps.setObject(i, true, Types.TINYINT), explicitPrepare).roundTripsAs(Types.TINYINT, (byte) 1);
        assertBind((ps, i) -> ps.setObject(i, false, Types.TINYINT), explicitPrepare).roundTripsAs(Types.TINYINT, (byte) 0);
    }

    @Test
    public void testConvertSmallint()
            throws SQLException
    {
        testConvertSmallint(true);
        testConvertSmallint(false);
    }

    private void testConvertSmallint(boolean explicitPrepare)
            throws SQLException
    {
        assertBind((ps, i) -> ps.setShort(i, (short) 123), explicitPrepare).roundTripsAs(Types.SMALLINT, (short) 123);
        assertBind((ps, i) -> ps.setObject(i, (short) 123), explicitPrepare).roundTripsAs(Types.SMALLINT, (short) 123);
        assertBind((ps, i) -> ps.setObject(i, (byte) 123, Types.SMALLINT), explicitPrepare).roundTripsAs(Types.SMALLINT, (short) 123);
        assertBind((ps, i) -> ps.setObject(i, (short) 123, Types.SMALLINT), explicitPrepare).roundTripsAs(Types.SMALLINT, (short) 123);
        assertBind((ps, i) -> ps.setObject(i, 123, Types.SMALLINT), explicitPrepare).roundTripsAs(Types.SMALLINT, (short) 123);
        assertBind((ps, i) -> ps.setObject(i, 123L, Types.SMALLINT), explicitPrepare).roundTripsAs(Types.SMALLINT, (short) 123);
        assertBind((ps, i) -> ps.setObject(i, 123.9f, Types.SMALLINT), explicitPrepare).roundTripsAs(Types.SMALLINT, (short) 123);
        assertBind((ps, i) -> ps.setObject(i, 123.9d, Types.SMALLINT), explicitPrepare).roundTripsAs(Types.SMALLINT, (short) 123);
        assertBind((ps, i) -> ps.setObject(i, BigInteger.valueOf(123), Types.SMALLINT), explicitPrepare).roundTripsAs(Types.SMALLINT, (short) 123);
        assertBind((ps, i) -> ps.setObject(i, BigDecimal.valueOf(123), Types.SMALLINT), explicitPrepare).roundTripsAs(Types.SMALLINT, (short) 123);
        assertBind((ps, i) -> ps.setObject(i, BigDecimal.valueOf(123.9), Types.SMALLINT), explicitPrepare).roundTripsAs(Types.SMALLINT, (short) 123);
        assertBind((ps, i) -> ps.setObject(i, "123", Types.SMALLINT), explicitPrepare).roundTripsAs(Types.SMALLINT, (short) 123);
        assertBind((ps, i) -> ps.setObject(i, true, Types.SMALLINT), explicitPrepare).roundTripsAs(Types.SMALLINT, (short) 1);
        assertBind((ps, i) -> ps.setObject(i, false, Types.SMALLINT), explicitPrepare).roundTripsAs(Types.SMALLINT, (short) 0);
    }

    @Test
    public void testConvertInteger()
            throws SQLException
    {
        testConvertInteger(true);
        testConvertInteger(false);
    }

    private void testConvertInteger(boolean explicitPrepare)
            throws SQLException
    {
        assertBind((ps, i) -> ps.setInt(i, 123), explicitPrepare).roundTripsAs(Types.INTEGER, 123);
        assertBind((ps, i) -> ps.setObject(i, 123), explicitPrepare).roundTripsAs(Types.INTEGER, 123);
        assertBind((ps, i) -> ps.setObject(i, (byte) 123, Types.INTEGER), explicitPrepare).roundTripsAs(Types.INTEGER, 123);
        assertBind((ps, i) -> ps.setObject(i, (byte) 123, Types.INTEGER), explicitPrepare).roundTripsAs(Types.INTEGER, 123);
        assertBind((ps, i) -> ps.setObject(i, (short) 123, Types.INTEGER), explicitPrepare).roundTripsAs(Types.INTEGER, 123);
        assertBind((ps, i) -> ps.setObject(i, 123, Types.INTEGER), explicitPrepare).roundTripsAs(Types.INTEGER, 123);
        assertBind((ps, i) -> ps.setObject(i, 123L, Types.INTEGER), explicitPrepare).roundTripsAs(Types.INTEGER, 123);
        assertBind((ps, i) -> ps.setObject(i, 123.9f, Types.INTEGER), explicitPrepare).roundTripsAs(Types.INTEGER, 123);
        assertBind((ps, i) -> ps.setObject(i, 123.9d, Types.INTEGER), explicitPrepare).roundTripsAs(Types.INTEGER, 123);
        assertBind((ps, i) -> ps.setObject(i, BigInteger.valueOf(123), Types.INTEGER), explicitPrepare).roundTripsAs(Types.INTEGER, 123);
        assertBind((ps, i) -> ps.setObject(i, BigDecimal.valueOf(123), Types.INTEGER), explicitPrepare).roundTripsAs(Types.INTEGER, 123);
        assertBind((ps, i) -> ps.setObject(i, BigDecimal.valueOf(123.9), Types.INTEGER), explicitPrepare).roundTripsAs(Types.INTEGER, 123);
        assertBind((ps, i) -> ps.setObject(i, "123", Types.INTEGER), explicitPrepare).roundTripsAs(Types.INTEGER, 123);
        assertBind((ps, i) -> ps.setObject(i, true, Types.INTEGER), explicitPrepare).roundTripsAs(Types.INTEGER, 1);
        assertBind((ps, i) -> ps.setObject(i, false, Types.INTEGER), explicitPrepare).roundTripsAs(Types.INTEGER, 0);
    }

    @Test
    public void testConvertBigint()
            throws SQLException
    {
        testConvertBigint(true);
        testConvertBigint(false);
    }

    private void testConvertBigint(boolean explicitPrepare)
            throws SQLException
    {
        assertBind((ps, i) -> ps.setLong(i, 123L), explicitPrepare).roundTripsAs(Types.BIGINT, 123L);
        assertBind((ps, i) -> ps.setObject(i, 123L), explicitPrepare).roundTripsAs(Types.BIGINT, 123L);
        assertBind((ps, i) -> ps.setObject(i, (byte) 123, Types.BIGINT), explicitPrepare).roundTripsAs(Types.BIGINT, 123L);
        assertBind((ps, i) -> ps.setObject(i, (short) 123, Types.BIGINT), explicitPrepare).roundTripsAs(Types.BIGINT, 123L);
        assertBind((ps, i) -> ps.setObject(i, 123, Types.BIGINT), explicitPrepare).roundTripsAs(Types.BIGINT, 123L);
        assertBind((ps, i) -> ps.setObject(i, 123L, Types.BIGINT), explicitPrepare).roundTripsAs(Types.BIGINT, 123L);
        assertBind((ps, i) -> ps.setObject(i, 123.9f, Types.BIGINT), explicitPrepare).roundTripsAs(Types.BIGINT, 123L);
        assertBind((ps, i) -> ps.setObject(i, 123.9d, Types.BIGINT), explicitPrepare).roundTripsAs(Types.BIGINT, 123L);
        assertBind((ps, i) -> ps.setObject(i, BigInteger.valueOf(123), Types.BIGINT), explicitPrepare).roundTripsAs(Types.BIGINT, 123L);
        assertBind((ps, i) -> ps.setObject(i, BigDecimal.valueOf(123), Types.BIGINT), explicitPrepare).roundTripsAs(Types.BIGINT, 123L);
        assertBind((ps, i) -> ps.setObject(i, BigDecimal.valueOf(123.9), Types.BIGINT), explicitPrepare).roundTripsAs(Types.BIGINT, 123L);
        assertBind((ps, i) -> ps.setObject(i, "123", Types.BIGINT), explicitPrepare).roundTripsAs(Types.BIGINT, 123L);
        assertBind((ps, i) -> ps.setObject(i, true, Types.BIGINT), explicitPrepare).roundTripsAs(Types.BIGINT, 1L);
        assertBind((ps, i) -> ps.setObject(i, false, Types.BIGINT), explicitPrepare).roundTripsAs(Types.BIGINT, 0L);
    }

    @Test
    public void testConvertReal()
            throws SQLException
    {
        testConvertReal(true);
        testConvertReal(false);
    }

    private void testConvertReal(boolean explicitPrepare)
            throws SQLException
    {
        assertBind((ps, i) -> ps.setFloat(i, 4.2f), explicitPrepare).roundTripsAs(Types.REAL, 4.2f);
        assertBind((ps, i) -> ps.setObject(i, 4.2f), explicitPrepare).roundTripsAs(Types.REAL, 4.2f);

        for (int type : asList(Types.REAL, Types.FLOAT)) {
            assertBind((ps, i) -> ps.setObject(i, (byte) 123, type), explicitPrepare).roundTripsAs(Types.REAL, 123.0f);
            assertBind((ps, i) -> ps.setObject(i, (short) 123, type), explicitPrepare).roundTripsAs(Types.REAL, 123.0f);
            assertBind((ps, i) -> ps.setObject(i, 123, type), explicitPrepare).roundTripsAs(Types.REAL, 123.0f);
            assertBind((ps, i) -> ps.setObject(i, 123L, type), explicitPrepare).roundTripsAs(Types.REAL, 123.0f);
            assertBind((ps, i) -> ps.setObject(i, 123.9f, type), explicitPrepare).roundTripsAs(Types.REAL, 123.9f);
            assertBind((ps, i) -> ps.setObject(i, 123.9d, type), explicitPrepare).roundTripsAs(Types.REAL, 123.9f);
            assertBind((ps, i) -> ps.setObject(i, BigInteger.valueOf(123), type), explicitPrepare).roundTripsAs(Types.REAL, 123.0f);
            assertBind((ps, i) -> ps.setObject(i, BigDecimal.valueOf(123), type), explicitPrepare).roundTripsAs(Types.REAL, 123.0f);
            assertBind((ps, i) -> ps.setObject(i, BigDecimal.valueOf(123.9), type), explicitPrepare).roundTripsAs(Types.REAL, 123.9f);
            assertBind((ps, i) -> ps.setObject(i, "4.2", type), explicitPrepare).roundTripsAs(Types.REAL, 4.2f);
            assertBind((ps, i) -> ps.setObject(i, true, type), explicitPrepare).roundTripsAs(Types.REAL, 1.0f);
            assertBind((ps, i) -> ps.setObject(i, false, type), explicitPrepare).roundTripsAs(Types.REAL, 0.0f);
        }
    }

    @Test
    public void testConvertDouble()
            throws SQLException
    {
        testConvertDouble(true);
        testConvertDouble(false);
    }

    private void testConvertDouble(boolean explicitPrepare)
            throws SQLException
    {
        assertBind((ps, i) -> ps.setDouble(i, 4.2d), explicitPrepare).roundTripsAs(Types.DOUBLE, 4.2d);
        assertBind((ps, i) -> ps.setObject(i, 4.2d), explicitPrepare).roundTripsAs(Types.DOUBLE, 4.2d);
        assertBind((ps, i) -> ps.setObject(i, (byte) 123, Types.DOUBLE), explicitPrepare).roundTripsAs(Types.DOUBLE, 123.0d);
        assertBind((ps, i) -> ps.setObject(i, (short) 123, Types.DOUBLE), explicitPrepare).roundTripsAs(Types.DOUBLE, 123.0d);
        assertBind((ps, i) -> ps.setObject(i, 123, Types.DOUBLE), explicitPrepare).roundTripsAs(Types.DOUBLE, 123.0d);
        assertBind((ps, i) -> ps.setObject(i, 123L, Types.DOUBLE), explicitPrepare).roundTripsAs(Types.DOUBLE, 123.0d);
        assertBind((ps, i) -> ps.setObject(i, 123.9f, Types.DOUBLE), explicitPrepare).roundTripsAs(Types.DOUBLE, (double) 123.9f);
        assertBind((ps, i) -> ps.setObject(i, 123.9d, Types.DOUBLE), explicitPrepare).roundTripsAs(Types.DOUBLE, 123.9d);
        assertBind((ps, i) -> ps.setObject(i, BigInteger.valueOf(123), Types.DOUBLE), explicitPrepare).roundTripsAs(Types.DOUBLE, 123.0d);
        assertBind((ps, i) -> ps.setObject(i, BigDecimal.valueOf(123), Types.DOUBLE), explicitPrepare).roundTripsAs(Types.DOUBLE, 123.0d);
        assertBind((ps, i) -> ps.setObject(i, BigDecimal.valueOf(123.9), Types.DOUBLE), explicitPrepare).roundTripsAs(Types.DOUBLE, 123.9d);
        assertBind((ps, i) -> ps.setObject(i, "4.2", Types.DOUBLE), explicitPrepare).roundTripsAs(Types.DOUBLE, 4.2d);
        assertBind((ps, i) -> ps.setObject(i, true, Types.DOUBLE), explicitPrepare).roundTripsAs(Types.DOUBLE, 1.0d);
        assertBind((ps, i) -> ps.setObject(i, false, Types.DOUBLE), explicitPrepare).roundTripsAs(Types.DOUBLE, 0.0d);
    }

    @Test
    public void testConvertDecimal()
            throws SQLException
    {
        testConvertDecimal(true);
        testConvertDecimal(false);
    }

    private void testConvertDecimal(boolean explicitPrepare)
            throws SQLException
    {
        assertBind((ps, i) -> ps.setBigDecimal(i, BigDecimal.valueOf(123)), explicitPrepare).roundTripsAs(Types.DECIMAL, BigDecimal.valueOf(123));
        assertBind((ps, i) -> ps.setObject(i, BigDecimal.valueOf(123)), explicitPrepare).roundTripsAs(Types.DECIMAL, BigDecimal.valueOf(123));

        for (int type : asList(Types.DECIMAL, Types.NUMERIC)) {
            assertBind((ps, i) -> ps.setObject(i, (byte) 123, type), explicitPrepare).roundTripsAs(Types.DECIMAL, BigDecimal.valueOf(123));
            assertBind((ps, i) -> ps.setObject(i, (short) 123, type), explicitPrepare).roundTripsAs(Types.DECIMAL, BigDecimal.valueOf(123));
            assertBind((ps, i) -> ps.setObject(i, 123, type), explicitPrepare).roundTripsAs(Types.DECIMAL, BigDecimal.valueOf(123));
            assertBind((ps, i) -> ps.setObject(i, 123L, type), explicitPrepare).roundTripsAs(Types.DECIMAL, BigDecimal.valueOf(123));
            assertBind((ps, i) -> ps.setObject(i, 123.9f, type), explicitPrepare).roundTripsAs(Types.DECIMAL, BigDecimal.valueOf(123.9f));
            assertBind((ps, i) -> ps.setObject(i, 123.9d, type), explicitPrepare).roundTripsAs(Types.DECIMAL, BigDecimal.valueOf(123.9d));
            assertBind((ps, i) -> ps.setObject(i, BigInteger.valueOf(123), type), explicitPrepare).roundTripsAs(Types.DECIMAL, BigDecimal.valueOf(123));
            assertBind((ps, i) -> ps.setObject(i, BigDecimal.valueOf(123), type), explicitPrepare).roundTripsAs(Types.DECIMAL, BigDecimal.valueOf(123));
            assertBind((ps, i) -> ps.setObject(i, BigDecimal.valueOf(123.9d), type), explicitPrepare).roundTripsAs(Types.DECIMAL, BigDecimal.valueOf(123.9d));
            assertBind((ps, i) -> ps.setObject(i, "123", type), explicitPrepare).roundTripsAs(Types.DECIMAL, BigDecimal.valueOf(123));
            assertBind((ps, i) -> ps.setObject(i, true, type), explicitPrepare).roundTripsAs(Types.DECIMAL, BigDecimal.valueOf(1));
            assertBind((ps, i) -> ps.setObject(i, false, type), explicitPrepare).roundTripsAs(Types.DECIMAL, BigDecimal.valueOf(0));
        }
    }

    @Test
    public void testConvertVarchar()
            throws SQLException
    {
        testConvertVarchar(true);
        testConvertVarchar(false);
    }

    private void testConvertVarchar(boolean explicitPrepare)
            throws SQLException
    {
        assertBind((ps, i) -> ps.setString(i, "hello"), explicitPrepare).roundTripsAs(Types.VARCHAR, "hello");
        assertBind((ps, i) -> ps.setObject(i, "hello"), explicitPrepare).roundTripsAs(Types.VARCHAR, "hello");

        String unicodeAndNull = "abc'xyz\0\u2603\uD835\uDCABtest";
        assertBind((ps, i) -> ps.setString(i, unicodeAndNull), explicitPrepare).roundTripsAs(Types.VARCHAR, unicodeAndNull);

        for (int type : asList(Types.CHAR, Types.NCHAR, Types.VARCHAR, Types.NVARCHAR, Types.LONGVARCHAR, Types.LONGNVARCHAR)) {
            assertBind((ps, i) -> ps.setObject(i, (byte) 123, type), explicitPrepare).roundTripsAs(Types.VARCHAR, "123");
            assertBind((ps, i) -> ps.setObject(i, (byte) 123, type), explicitPrepare).roundTripsAs(Types.VARCHAR, "123");
            assertBind((ps, i) -> ps.setObject(i, (short) 123, type), explicitPrepare).roundTripsAs(Types.VARCHAR, "123");
            assertBind((ps, i) -> ps.setObject(i, 123, type), explicitPrepare).roundTripsAs(Types.VARCHAR, "123");
            assertBind((ps, i) -> ps.setObject(i, 123L, type), explicitPrepare).roundTripsAs(Types.VARCHAR, "123");
            assertBind((ps, i) -> ps.setObject(i, 123.9f, type), explicitPrepare).roundTripsAs(Types.VARCHAR, "123.9");
            assertBind((ps, i) -> ps.setObject(i, 123.9d, type), explicitPrepare).roundTripsAs(Types.VARCHAR, "123.9");
            assertBind((ps, i) -> ps.setObject(i, BigInteger.valueOf(123), type), explicitPrepare).roundTripsAs(Types.VARCHAR, "123");
            assertBind((ps, i) -> ps.setObject(i, BigDecimal.valueOf(123), type), explicitPrepare).roundTripsAs(Types.VARCHAR, "123");
            assertBind((ps, i) -> ps.setObject(i, BigDecimal.valueOf(123.9), type), explicitPrepare).roundTripsAs(Types.VARCHAR, "123.9");
            assertBind((ps, i) -> ps.setObject(i, "hello", type), explicitPrepare).roundTripsAs(Types.VARCHAR, "hello");
            assertBind((ps, i) -> ps.setObject(i, true, type), explicitPrepare).roundTripsAs(Types.VARCHAR, "true");
            assertBind((ps, i) -> ps.setObject(i, false, type), explicitPrepare).roundTripsAs(Types.VARCHAR, "false");
        }
    }

    @Test
    public void testConvertVarbinary()
            throws SQLException
    {
        testConvertVarbinary(true);
        testConvertVarbinary(false);
    }

    private void testConvertVarbinary(boolean explicitPrepare)
            throws SQLException
    {
        String value = "abc\0xyz";
        byte[] bytes = value.getBytes(UTF_8);

        assertBind((ps, i) -> ps.setBytes(i, bytes), explicitPrepare).roundTripsAs(Types.VARBINARY, bytes);
        assertBind((ps, i) -> ps.setObject(i, bytes), explicitPrepare).roundTripsAs(Types.VARBINARY, bytes);

        for (int type : asList(Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY)) {
            assertBind((ps, i) -> ps.setObject(i, bytes, type), explicitPrepare).roundTripsAs(Types.VARBINARY, bytes);
            assertBind((ps, i) -> ps.setObject(i, value, type), explicitPrepare).roundTripsAs(Types.VARBINARY, bytes);
        }
    }

    @Test
    public void testConvertDate()
            throws SQLException
    {
        testConvertDate(true);
        testConvertDate(false);
    }

    private void testConvertDate(boolean explicitPrepare)
            throws SQLException
    {
        LocalDate date = LocalDate.of(2001, 5, 6);
        Date sqlDate = Date.valueOf(date);
        java.util.Date javaDate = new java.util.Date(sqlDate.getTime());
        LocalDateTime dateTime = LocalDateTime.of(date, LocalTime.of(12, 34, 56));
        Timestamp sqlTimestamp = Timestamp.valueOf(dateTime);

        assertBind((ps, i) -> ps.setDate(i, sqlDate), explicitPrepare)
                .resultsIn("date", "DATE '2001-05-06'")
                .roundTripsAs(Types.DATE, sqlDate);

        assertBind((ps, i) -> ps.setObject(i, sqlDate), explicitPrepare)
                .resultsIn("date", "DATE '2001-05-06'")
                .roundTripsAs(Types.DATE, sqlDate);

        assertBind((ps, i) -> ps.setObject(i, sqlDate, Types.DATE), explicitPrepare)
                .resultsIn("date", "DATE '2001-05-06'")
                .roundTripsAs(Types.DATE, sqlDate);

        assertBind((ps, i) -> ps.setObject(i, sqlTimestamp, Types.DATE), explicitPrepare)
                .resultsIn("date", "DATE '2001-05-06'")
                .roundTripsAs(Types.DATE, sqlDate);

        assertBind((ps, i) -> ps.setObject(i, javaDate, Types.DATE), explicitPrepare)
                .resultsIn("date", "DATE '2001-05-06'")
                .roundTripsAs(Types.DATE, sqlDate);

        assertBind((ps, i) -> ps.setObject(i, date, Types.DATE), explicitPrepare)
                .resultsIn("date", "DATE '2001-05-06'")
                .roundTripsAs(Types.DATE, sqlDate);

        assertBind((ps, i) -> ps.setObject(i, dateTime, Types.DATE), explicitPrepare)
                .resultsIn("date", "DATE '2001-05-06'")
                .roundTripsAs(Types.DATE, sqlDate);

        assertBind((ps, i) -> ps.setObject(i, "2001-05-06", Types.DATE), explicitPrepare)
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

    private void testConvertLocalDate(boolean explicitPrepare)
            throws SQLException
    {
        LocalDate date = LocalDate.of(2001, 5, 6);

        assertBind((ps, i) -> ps.setObject(i, date), explicitPrepare)
                .resultsIn("date", "DATE '2001-05-06'")
                .roundTripsAs(Types.DATE, Date.valueOf(date));

        assertBind((ps, i) -> ps.setObject(i, date, Types.DATE), explicitPrepare)
                .resultsIn("date", "DATE '2001-05-06'")
                .roundTripsAs(Types.DATE, Date.valueOf(date));

        assertBind((ps, i) -> ps.setObject(i, date, Types.TIME), explicitPrepare)
                .isInvalid("Cannot convert instance of java.time.LocalDate to time");

        assertBind((ps, i) -> ps.setObject(i, date, Types.TIME_WITH_TIMEZONE), explicitPrepare)
                .isInvalid("Cannot convert instance of java.time.LocalDate to time with time zone");

        assertBind((ps, i) -> ps.setObject(i, date, Types.TIMESTAMP), explicitPrepare)
                .isInvalid("Cannot convert instance of java.time.LocalDate to timestamp");

        assertBind((ps, i) -> ps.setObject(i, date, Types.TIMESTAMP_WITH_TIMEZONE), explicitPrepare)
                .isInvalid("Cannot convert instance of java.time.LocalDate to timestamp with time zone");

        LocalDate jvmGapDate = LocalDate.of(1970, 1, 1);
        checkIsGap(ZoneId.systemDefault(), jvmGapDate.atTime(LocalTime.MIDNIGHT));

        assertBind((ps, i) -> ps.setObject(i, jvmGapDate), explicitPrepare)
                .resultsIn("date", "DATE '1970-01-01'")
                .roundTripsAs(Types.DATE, Date.valueOf(jvmGapDate));

        assertBind((ps, i) -> ps.setObject(i, jvmGapDate, Types.DATE), explicitPrepare)
                .roundTripsAs(Types.DATE, Date.valueOf(jvmGapDate));
    }

    @Test
    public void testConvertTime()
            throws SQLException
    {
        testConvertTime(true);
        testConvertTime(false);
    }

    private void testConvertTime(boolean explicitPrepare)
            throws SQLException
    {
        LocalTime time = LocalTime.of(12, 34, 56);
        Time sqlTime = Time.valueOf(time);
        java.util.Date javaDate = new java.util.Date(sqlTime.getTime());
        LocalDateTime dateTime = LocalDateTime.of(LocalDate.of(2001, 5, 6), time);
        Timestamp sqlTimestamp = Timestamp.valueOf(dateTime);

        assertBind((ps, i) -> ps.setTime(i, sqlTime), explicitPrepare)
                .resultsIn("time(3)", "TIME '12:34:56.000'")
                .roundTripsAs(Types.TIME, sqlTime);

        assertBind((ps, i) -> ps.setObject(i, sqlTime), explicitPrepare)
                .resultsIn("time(3)", "TIME '12:34:56.000'")
                .roundTripsAs(Types.TIME, sqlTime);

        assertBind((ps, i) -> ps.setObject(i, sqlTime, Types.TIME), explicitPrepare)
                .resultsIn("time(3)", "TIME '12:34:56.000'")
                .roundTripsAs(Types.TIME, sqlTime);

        assertBind((ps, i) -> ps.setObject(i, sqlTimestamp, Types.TIME), explicitPrepare)
                .resultsIn("time(3)", "TIME '12:34:56.000'")
                .roundTripsAs(Types.TIME, sqlTime);

        assertBind((ps, i) -> ps.setObject(i, javaDate, Types.TIME), explicitPrepare)
                .resultsIn("time(3)", "TIME '12:34:56.000'")
                .roundTripsAs(Types.TIME, sqlTime);

        assertBind((ps, i) -> ps.setObject(i, dateTime, Types.TIME), explicitPrepare)
                .resultsIn("time(0)", "TIME '12:34:56'")
                .roundTripsAs(Types.TIME, sqlTime);

        assertBind((ps, i) -> ps.setObject(i, "12:34:56", Types.TIME), explicitPrepare)
                .resultsIn("time(0)", "TIME '12:34:56'")
                .roundTripsAs(Types.TIME, sqlTime);

        assertBind((ps, i) -> ps.setObject(i, "12:34:56.123", Types.TIME), explicitPrepare).resultsIn("time(3)", "TIME '12:34:56.123'");
        assertBind((ps, i) -> ps.setObject(i, "12:34:56.123456", Types.TIME), explicitPrepare).resultsIn("time(6)", "TIME '12:34:56.123456'");

        assertBind((ps, i) -> ps.setObject(i, "12:34:56.123456789", Types.TIME), explicitPrepare)
                .resultsIn("time(9)", "TIME '12:34:56.123456789'");

        assertBind((ps, i) -> ps.setObject(i, "12:34:56.123456789012", Types.TIME), explicitPrepare)
                .resultsIn("time(12)", "TIME '12:34:56.123456789012'");

        Time timeWithDecisecond = new Time(sqlTime.getTime() + 100);
        assertBind((ps, i) -> ps.setObject(i, timeWithDecisecond), explicitPrepare)
                .resultsIn("time(3)", "TIME '12:34:56.100'")
                .roundTripsAs(Types.TIME, timeWithDecisecond);

        assertBind((ps, i) -> ps.setObject(i, timeWithDecisecond, Types.TIME), explicitPrepare)
                .resultsIn("time(3)", "TIME '12:34:56.100'")
                .roundTripsAs(Types.TIME, timeWithDecisecond);

        Time timeWithMillisecond = new Time(sqlTime.getTime() + 123);
        assertBind((ps, i) -> ps.setObject(i, timeWithMillisecond), explicitPrepare)
                .resultsIn("time(3)", "TIME '12:34:56.123'")
                .roundTripsAs(Types.TIME, timeWithMillisecond);

        assertBind((ps, i) -> ps.setObject(i, timeWithMillisecond, Types.TIME), explicitPrepare)
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

    private void testConvertTimeWithTimeZone(boolean explicitPrepare)
            throws SQLException
    {
        // zero fraction
        assertBind((ps, i) -> ps.setObject(i, OffsetTime.of(12, 34, 56, 0, UTC), Types.TIME_WITH_TIMEZONE), explicitPrepare)
                .resultsIn("time(0) with time zone", "TIME '12:34:56+00:00'")
                .roundTripsAs(Types.TIME_WITH_TIMEZONE, toSqlTime(LocalTime.of(5, 34, 56)));

        // setObject with implicit type
        assertBind((ps, i) -> ps.setObject(i, OffsetTime.of(12, 34, 56, 0, UTC)), explicitPrepare)
                .resultsIn("time(0) with time zone", "TIME '12:34:56+00:00'");

        // setObject with JDBCType
        assertBind((ps, i) -> ps.setObject(i, OffsetTime.of(12, 34, 56, 0, UTC), JDBCType.TIME_WITH_TIMEZONE), explicitPrepare)
                .resultsIn("time(0) with time zone", "TIME '12:34:56+00:00'");

        // millisecond precision
        assertBind((ps, i) -> ps.setObject(i, OffsetTime.of(12, 34, 56, 555_000_000, UTC), Types.TIME_WITH_TIMEZONE), explicitPrepare)
                .resultsIn("time(3) with time zone", "TIME '12:34:56.555+00:00'")
                .roundTripsAs(Types.TIME_WITH_TIMEZONE, toSqlTime(LocalTime.of(5, 34, 56, 555_000_000)));

        // microsecond precision
        assertBind((ps, i) -> ps.setObject(i, OffsetTime.of(12, 34, 56, 555_555_000, UTC), Types.TIME_WITH_TIMEZONE), explicitPrepare)
                .resultsIn("time(6) with time zone", "TIME '12:34:56.555555+00:00'")
                .roundTripsAs(Types.TIME_WITH_TIMEZONE, toSqlTime(LocalTime.of(5, 34, 56, 556_000_000)));

        // nanosecond precision
        assertBind((ps, i) -> ps.setObject(i, OffsetTime.of(12, 34, 56, 555_555_555, UTC), Types.TIME_WITH_TIMEZONE), explicitPrepare)
                .resultsIn("time(9) with time zone", "TIME '12:34:56.555555555+00:00'")
                .roundTripsAs(Types.TIME_WITH_TIMEZONE, toSqlTime(LocalTime.of(5, 34, 56, 556_000_000)));

        // positive offset
        assertBind((ps, i) -> ps.setObject(i, OffsetTime.of(12, 34, 56, 123_456_789, ZoneOffset.ofHoursMinutes(7, 35)), Types.TIME_WITH_TIMEZONE), explicitPrepare)
                .resultsIn("time(9) with time zone", "TIME '12:34:56.123456789+07:35'");
        // TODO (https://github.com/trinodb/trino/issues/6351) the result is not as expected here:
        //      .roundTripsAs(Types.TIME_WITH_TIMEZONE, toSqlTime(LocalTime.of(20, 59, 56, 123_000_000)));

        // negative offset
        assertBind((ps, i) -> ps.setObject(i, OffsetTime.of(12, 34, 56, 123_456_789, ZoneOffset.ofHoursMinutes(-7, -35)), Types.TIME_WITH_TIMEZONE), explicitPrepare)
                .resultsIn("time(9) with time zone", "TIME '12:34:56.123456789-07:35'")
                .roundTripsAs(Types.TIME_WITH_TIMEZONE, toSqlTime(LocalTime.of(13, 9, 56, 123_000_000)));

        // String as TIME WITH TIME ZONE
        assertBind((ps, i) -> ps.setObject(i, "12:34:56.123 +05:45", Types.TIME_WITH_TIMEZONE), explicitPrepare)
                .resultsIn("time(3) with time zone", "TIME '12:34:56.123 +05:45'");

        assertBind((ps, i) -> ps.setObject(i, "12:34:56.123456 +05:45", Types.TIME_WITH_TIMEZONE), explicitPrepare)
                .resultsIn("time(6) with time zone", "TIME '12:34:56.123456 +05:45'");

        assertBind((ps, i) -> ps.setObject(i, "12:34:56.123456789 +05:45", Types.TIME_WITH_TIMEZONE), explicitPrepare)
                .resultsIn("time(9) with time zone", "TIME '12:34:56.123456789 +05:45'");

        assertBind((ps, i) -> ps.setObject(i, "12:34:56.123456789012 +05:45", Types.TIME_WITH_TIMEZONE), explicitPrepare)
                .resultsIn("time(12) with time zone", "TIME '12:34:56.123456789012 +05:45'");
    }

    @Test
    public void testConvertTimestamp()
            throws SQLException
    {
        testConvertTimestamp(true);
        testConvertTimestamp(false);
    }

    private void testConvertTimestamp(boolean explicitPrepare)
            throws SQLException
    {
        LocalDateTime dateTime = LocalDateTime.of(2001, 5, 6, 12, 34, 56);
        Date sqlDate = Date.valueOf(dateTime.toLocalDate());
        Time sqlTime = Time.valueOf(dateTime.toLocalTime());
        Timestamp sqlTimestamp = Timestamp.valueOf(dateTime);
        Timestamp sameInstantInWarsawZone = Timestamp.valueOf(dateTime.atZone(ZoneId.systemDefault()).withZoneSameInstant(ZoneId.of("Europe/Warsaw")).toLocalDateTime());
        java.util.Date javaDate = java.util.Date.from(dateTime.atZone(ZoneId.systemDefault()).toInstant());

        assertBind((ps, i) -> ps.setTimestamp(i, sqlTimestamp), explicitPrepare)
                .resultsIn("timestamp(3)", "TIMESTAMP '2001-05-06 12:34:56.000'")
                .roundTripsAs(Types.TIMESTAMP, sqlTimestamp);

        assertBind((ps, i) -> ps.setTimestamp(i, sqlTimestamp, null), explicitPrepare)
                .resultsIn("timestamp(3)", "TIMESTAMP '2001-05-06 12:34:56.000'")
                .roundTripsAs(Types.TIMESTAMP, sqlTimestamp);

        assertBind((ps, i) -> ps.setTimestamp(i, sqlTimestamp, Calendar.getInstance()), explicitPrepare)
                .resultsIn("timestamp(3)", "TIMESTAMP '2001-05-06 12:34:56.000'")
                .roundTripsAs(Types.TIMESTAMP, sqlTimestamp);

        assertBind((ps, i) -> ps.setTimestamp(i, sqlTimestamp, Calendar.getInstance(TimeZone.getTimeZone(ZoneId.of("Europe/Warsaw")))), explicitPrepare)
                .resultsIn("timestamp(3)", "TIMESTAMP '2001-05-06 20:34:56.000'")
                .roundTripsAs(Types.TIMESTAMP, sameInstantInWarsawZone);

        assertBind((ps, i) -> ps.setObject(i, sqlTimestamp), explicitPrepare)
                .resultsIn("timestamp(3)", "TIMESTAMP '2001-05-06 12:34:56.000'")
                .roundTripsAs(Types.TIMESTAMP, sqlTimestamp);

        assertBind((ps, i) -> ps.setObject(i, sqlDate, Types.TIMESTAMP), explicitPrepare)
                .resultsIn("timestamp(3)", "TIMESTAMP '2001-05-06 00:00:00.000'")
                .roundTripsAs(Types.TIMESTAMP, new Timestamp(sqlDate.getTime()));

        assertBind((ps, i) -> ps.setObject(i, sqlTime, Types.TIMESTAMP), explicitPrepare)
                .resultsIn("timestamp(3)", "TIMESTAMP '1970-01-01 12:34:56.000'")
                .roundTripsAs(Types.TIMESTAMP, new Timestamp(sqlTime.getTime()));

        assertBind((ps, i) -> ps.setObject(i, sqlTimestamp, Types.TIMESTAMP), explicitPrepare)
                .resultsIn("timestamp(3)", "TIMESTAMP '2001-05-06 12:34:56.000'")
                .roundTripsAs(Types.TIMESTAMP, sqlTimestamp);

        assertBind((ps, i) -> ps.setObject(i, javaDate, Types.TIMESTAMP), explicitPrepare)
                .resultsIn("timestamp(3)", "TIMESTAMP '2001-05-06 12:34:56.000'")
                .roundTripsAs(Types.TIMESTAMP, sqlTimestamp);

        assertBind((ps, i) -> ps.setObject(i, dateTime, Types.TIMESTAMP), explicitPrepare)
                .resultsIn("timestamp(0)", "TIMESTAMP '2001-05-06 12:34:56'")
                .roundTripsAs(Types.TIMESTAMP, sqlTimestamp);

        assertBind((ps, i) -> ps.setObject(i, "2001-05-06 12:34:56", Types.TIMESTAMP), explicitPrepare)
                .resultsIn("timestamp(0)", "TIMESTAMP '2001-05-06 12:34:56'")
                .roundTripsAs(Types.TIMESTAMP, sqlTimestamp);

        assertBind((ps, i) -> ps.setObject(i, "2001-05-06 12:34:56.123", Types.TIMESTAMP), explicitPrepare)
                .resultsIn("timestamp(3)", "TIMESTAMP '2001-05-06 12:34:56.123'");

        assertBind((ps, i) -> ps.setObject(i, "2001-05-06 12:34:56.123456", Types.TIMESTAMP), explicitPrepare)
                .resultsIn("timestamp(6)", "TIMESTAMP '2001-05-06 12:34:56.123456'");

        assertBind((ps, i) -> ps.setObject(i, "2001-05-06 12:34:56.123456789", Types.TIMESTAMP), explicitPrepare)
                .resultsIn("timestamp(9)", "TIMESTAMP '2001-05-06 12:34:56.123456789'");

        assertBind((ps, i) -> ps.setObject(i, "2001-05-06 12:34:56.123456789012", Types.TIMESTAMP), explicitPrepare)
                .resultsIn("timestamp(12)", "TIMESTAMP '2001-05-06 12:34:56.123456789012'");

        Timestamp timestampWithWithDecisecond = new Timestamp(sqlTimestamp.getTime() + 100);
        assertBind((ps, i) -> ps.setTimestamp(i, timestampWithWithDecisecond), explicitPrepare)
                .resultsIn("timestamp(3)", "TIMESTAMP '2001-05-06 12:34:56.100'")
                .roundTripsAs(Types.TIMESTAMP, timestampWithWithDecisecond);

        assertBind((ps, i) -> ps.setObject(i, timestampWithWithDecisecond), explicitPrepare)
                .resultsIn("timestamp(3)", "TIMESTAMP '2001-05-06 12:34:56.100'")
                .roundTripsAs(Types.TIMESTAMP, timestampWithWithDecisecond);

        assertBind((ps, i) -> ps.setObject(i, timestampWithWithDecisecond, Types.TIMESTAMP), explicitPrepare)
                .resultsIn("timestamp(3)", "TIMESTAMP '2001-05-06 12:34:56.100'")
                .roundTripsAs(Types.TIMESTAMP, timestampWithWithDecisecond);

        Timestamp timestampWithMillisecond = new Timestamp(sqlTimestamp.getTime() + 123);
        assertBind((ps, i) -> ps.setTimestamp(i, timestampWithMillisecond), explicitPrepare)
                .resultsIn("timestamp(3)", "TIMESTAMP '2001-05-06 12:34:56.123'")
                .roundTripsAs(Types.TIMESTAMP, timestampWithMillisecond);

        assertBind((ps, i) -> ps.setObject(i, timestampWithMillisecond), explicitPrepare)
                .resultsIn("timestamp(3)", "TIMESTAMP '2001-05-06 12:34:56.123'")
                .roundTripsAs(Types.TIMESTAMP, timestampWithMillisecond);

        assertBind((ps, i) -> ps.setObject(i, timestampWithMillisecond, Types.TIMESTAMP), explicitPrepare)
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

    private void testConvertTimestampWithTimeZone(boolean explicitPrepare)
            throws SQLException
    {
        // TODO (https://github.com/trinodb/trino/issues/6299) support ZonedDateTime

        // String as TIMESTAMP WITH TIME ZONE
        assertBind((ps, i) -> ps.setObject(i, "1970-01-01 12:34:56.123 +05:45", Types.TIMESTAMP_WITH_TIMEZONE), explicitPrepare)
                .resultsIn("timestamp(3) with time zone", "TIMESTAMP '1970-01-01 12:34:56.123 +05:45'");

        assertBind((ps, i) -> ps.setObject(i, "1970-01-01 12:34:56.123456 +05:45", Types.TIMESTAMP_WITH_TIMEZONE), explicitPrepare)
                .resultsIn("timestamp(6) with time zone", "TIMESTAMP '1970-01-01 12:34:56.123456 +05:45'");

        assertBind((ps, i) -> ps.setObject(i, "1970-01-01 12:34:56.123456789 +05:45", Types.TIMESTAMP_WITH_TIMEZONE), explicitPrepare)
                .resultsIn("timestamp(9) with time zone", "TIMESTAMP '1970-01-01 12:34:56.123456789 +05:45'");

        assertBind((ps, i) -> ps.setObject(i, "1970-01-01 12:34:56.123456789012 +05:45", Types.TIMESTAMP_WITH_TIMEZONE), explicitPrepare)
                .resultsIn("timestamp(12) with time zone", "TIMESTAMP '1970-01-01 12:34:56.123456789012 +05:45'");
    }

    @Test
    public void testInvalidConversions()
            throws SQLException
    {
        testInvalidConversions(true);
        testInvalidConversions(false);
    }

    private void testInvalidConversions(boolean explicitPrepare)
            throws SQLException
    {
        assertBind((ps, i) -> ps.setObject(i, String.class), explicitPrepare).isInvalid("Unsupported object type: java.lang.Class");
        assertBind((ps, i) -> ps.setObject(i, String.class, Types.BIGINT), explicitPrepare)
                .isInvalid("Cannot convert instance of java.lang.Class to SQL type " + Types.BIGINT);
        assertBind((ps, i) -> ps.setObject(i, "abc", Types.SMALLINT), explicitPrepare)
                .isInvalid("Cannot convert instance of java.lang.String to SQL type " + Types.SMALLINT);
    }

    @Test
    public void testExplicitPrepare()
            throws Exception
    {
        testExplicitPrepareSetting(true,
                "EXECUTE %statement% USING %values%");
    }

    @Test
    public void testExecuteImmediate()
            throws Exception
    {
        testExplicitPrepareSetting(false,
                "EXECUTE IMMEDIATE '%query%' USING %values%");
    }

    private BindAssertion assertBind(Binder binder, boolean explicitPrepare)
    {
        return new BindAssertion(() -> this.createConnection(explicitPrepare), binder);
    }

    private Connection createConnection(boolean explicitPrepare)
            throws SQLException
    {
        String url = format("jdbc:trino://%s?explicitPrepare=" + explicitPrepare, server.getAddress());
        return DriverManager.getConnection(url, "test", null);
    }

    private Connection createConnection(String catalog, String schema, boolean explicitPrepare)
            throws SQLException
    {
        String url = format("jdbc:trino://%s/%s/%s?explicitPrepare=" + explicitPrepare, server.getAddress(), catalog, schema);
        return DriverManager.getConnection(url, "test", null);
    }

    private void testExplicitPrepareSetting(boolean explicitPrepare, String expectedSql)
            throws Exception
    {
        String tableName = "test_table_" + randomNameSuffix();
        String selectSql = "SELECT * FROM blackhole.blackhole." + tableName + " WHERE x = ? AND y = ? AND y <> 'Test'";
        String insertSql = "INSERT INTO blackhole.blackhole." + tableName + " (x, y) VALUES (?, ?)";

        try (Connection connection = createConnection(explicitPrepare)) {
            try (Statement statement = connection.createStatement()) {
                assertThat(statement.executeUpdate("CREATE TABLE blackhole.blackhole." + tableName + " (x bigint, y varchar)")).isEqualTo(0);
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
                assertThat(statement.executeUpdate("DROP TABLE blackhole.blackhole." + tableName)).isEqualTo(0);
            }
        }
    }

    private void checkSQLExecuted(Connection connection, String expectedSql)
    {
        String sql = format("SELECT state FROM system.runtime.queries WHERE query = '%s'", expectedSql.replace("'", "''"));

        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            assertThat(resultSet.next())
                    .describedAs("Cannot find SQL query " + expectedSql)
                    .isTrue();
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
                    assertThat(rs.getObject(1)).isEqualTo(expectedValue);
                    verify(!rs.next(), "unexpected second row");

                    assertThat(rs.getMetaData().getColumnType(1)).isEqualTo(expectedSqlType);
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
