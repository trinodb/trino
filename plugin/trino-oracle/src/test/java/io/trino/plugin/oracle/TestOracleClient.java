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
package io.trino.plugin.oracle;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.base.mapping.DefaultIdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.WriteFunction;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.NumberType;
import io.trino.spi.type.Type;
import io.trino.testing.TestingConnectorSession;
import oracle.jdbc.OracleTypes;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.reflect.Reflection.newProxy;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOracleClient
{
    private static final JdbcClient CLIENT = new OracleClient(
            new BaseJdbcConfig(),
            new OracleConfig(),
            new JdbcStatisticsConfig(),
            _ -> {
                throw new UnsupportedOperationException();
            },
            new OracleQueryBuilder(RemoteQueryModifier.NONE),
            new DefaultIdentifierMapping(),
            RemoteQueryModifier.NONE);

    private static final ConnectorSession SESSION = TestingConnectorSession.builder()
            .setPropertyMetadata(new OracleSessionProperties(new OracleConfig()).getSessionProperties())
            .build();

    @Test
    public void testTypedNullWriteMapping()
            throws SQLException
    {
        testTypedNullWriteMapping(BOOLEAN, "?", Types.TINYINT);
        testTypedNullWriteMapping(TINYINT, "?", Types.TINYINT);
        testTypedNullWriteMapping(SMALLINT, "?", Types.SMALLINT);
        testTypedNullWriteMapping(INTEGER, "?", Types.INTEGER);
        testTypedNullWriteMapping(BIGINT, "?", Types.BIGINT);
        testTypedNullWriteMapping(REAL, "?", Types.REAL);
        testTypedNullWriteMapping(DOUBLE, "?", Types.DOUBLE);
        testTypedNullWriteMapping(VARBINARY, "?", Types.VARBINARY);
        testTypedNullWriteMapping(createCharType(25), "?", Types.NCHAR);
        testTypedNullWriteMapping(createDecimalType(16, 6), "?", Types.DECIMAL);
        testTypedNullWriteMapping(createDecimalType(36, 12), "?", Types.DECIMAL);
        testTypedNullWriteMapping(createUnboundedVarcharType(), "?", Types.VARCHAR);
        testTypedNullWriteMapping(createVarcharType(123), "?", Types.VARCHAR);
        testTypedNullWriteMapping(TIMESTAMP_SECONDS, "TO_DATE(?, 'SYYYY-MM-DD HH24:MI:SS')", Types.VARCHAR);
        testTypedNullWriteMapping(TIMESTAMP_MILLIS, "TO_TIMESTAMP(?, 'SYYYY-MM-DD HH24:MI:SS.FF3')", Types.VARCHAR);
        testTypedNullWriteMapping(TIMESTAMP_MICROS, "TO_TIMESTAMP(?, 'SYYYY-MM-DD HH24:MI:SS.FF6')", Types.VARCHAR);
        testTypedNullWriteMapping(TIMESTAMP_NANOS, "TO_TIMESTAMP(?, 'SYYYY-MM-DD HH24:MI:SS.FF9')", Types.VARCHAR);
        testTypedNullWriteMapping(TIMESTAMP_TZ_MILLIS, "?", OracleTypes.TIMESTAMPTZ);
        testTypedNullWriteMapping(DATE, "TO_DATE(?, 'SYYYY-MM-DD')", Types.VARCHAR);
    }

    @Test
    public void testNumberColumnWithZeroPrecisionMapsToNumberType()
    {
        // Oracle JDBC can return columnSize=0, decimalDigits=0 for NUMBER columns in stored views
        // and materialized views. This must not throw "DECIMAL precision must be in range [1, 38]: 0".
        JdbcTypeHandle typeHandle = new JdbcTypeHandle(
                OracleTypes.NUMBER,
                Optional.of("NUMBER"),
                Optional.of(0),
                Optional.of(0),
                Optional.empty(),
                Optional.empty());
        assertThat(CLIENT.toColumnMapping(SESSION, null, typeHandle))
                .isPresent()
                .get()
                .satisfies(mapping -> assertThat(mapping.getType()).isEqualTo(NumberType.NUMBER));
    }

    private void testTypedNullWriteMapping(Type type, String bindExpression, int nullJdbcType)
            throws SQLException
    {
        WriteMapping writeMapping = CLIENT.toWriteMapping(SESSION, type);
        assertThat(writeMapping.getWriteFunction()).isNotNull();
        WriteFunction writeFunction = writeMapping.getWriteFunction();

        assertThat(writeFunction.getBindExpression()).isEqualTo(bindExpression);

        PreparedStatement statementProxy = newProxy(PreparedStatement.class, (_, method, args) -> {
            // Calling setNull with a proper JDBC type is important for Oracle as it
            // allows prepared statement to be cached and reused by the database cursor
            // while writing. If the default implementation of the WriteFunction is used,
            // NULL is sent with the JDBC type 0, which invalidates the cache for prepared statement.
            // After invalidation, statement needs to be parsed and analyzed again,
            // which has severe performance cost when writing large datasets containing NULLs.
            assertThat(method.getName()).isEqualTo("setNull");
            assertThat(args.length).isEqualTo(2);
            assertThat(args[0]).isEqualTo(1325);
            assertThat(args[1])
                    .describedAs("expected jdbc type for NULL value")
                    .isEqualTo(nullJdbcType);

            return null;
        });

        writeFunction.setNull(statementProxy, 1325);
    }

    @Test
    public void testGetTableStatistics()
    {
        JdbcTableHandle tableHandle = new JdbcTableHandle(
                new SchemaTableName("tpch", "orders"),
                new RemoteTableName(Optional.empty(), Optional.of("TPCH"), "ORDERS"),
                Optional.empty());

        JdbcColumnHandle idColumn = JdbcColumnHandle.builder()
                .setColumnName("ID")
                .setJdbcTypeHandle(new JdbcTypeHandle(OracleTypes.NUMBER, Optional.of("NUMBER"), Optional.of(19), Optional.of(0), Optional.empty(), Optional.empty()))
                .setColumnType(createDecimalType(19, 0))
                .build();
        JdbcColumnHandle nameColumn = JdbcColumnHandle.builder()
                .setColumnName("NAME")
                .setJdbcTypeHandle(new JdbcTypeHandle(OracleTypes.VARCHAR, Optional.of("VARCHAR2"), Optional.of(50), Optional.empty(), Optional.empty(), Optional.empty()))
                .setColumnType(createVarcharType(50))
                .build();
        JdbcColumnHandle allNullColumn = JdbcColumnHandle.builder()
                .setColumnName("NOTES")
                .setJdbcTypeHandle(new JdbcTypeHandle(OracleTypes.VARCHAR, Optional.of("VARCHAR2"), Optional.of(100), Optional.empty(), Optional.empty(), Optional.empty()))
                .setColumnType(createVarcharType(100))
                .build();

        ConnectionFactory connectionFactory = createMockStatisticsConnectionFactory(
                Optional.of(1000L),
                Optional.empty(),
                List.of(
                        Map.of("COLUMN_NAME", "ID", "NUM_DISTINCT", 1000L, "NUM_NULLS", 0L, "AVG_COL_LEN", 6L),
                        Map.of("COLUMN_NAME", "NAME", "NUM_DISTINCT", 50L, "NUM_NULLS", 200L, "AVG_COL_LEN", 12L),
                        Map.of("COLUMN_NAME", "NOTES", "NUM_DISTINCT", 0L, "NUM_NULLS", 1000L, "AVG_COL_LEN", 0L)));

        JdbcClient client = createClientWithColumns(new OracleConfig(), new JdbcStatisticsConfig(), connectionFactory, List.of(idColumn, nameColumn, allNullColumn));
        TableStatistics statistics = client.getTableStatistics(SESSION, tableHandle);

        assertThat(statistics.getRowCount()).isEqualTo(Estimate.of(1000));
        assertThat(statistics.getColumnStatistics().get(idColumn)).isEqualTo(ColumnStatistics.builder()
                .setNullsFraction(Estimate.of(0.0))
                .setDistinctValuesCount(Estimate.of(1000))
                .setDataSize(Estimate.of(6000.0))
                .build());
        assertThat(statistics.getColumnStatistics().get(nameColumn)).isEqualTo(ColumnStatistics.builder()
                .setNullsFraction(Estimate.of(0.2))
                .setDistinctValuesCount(Estimate.of(50))
                .setDataSize(Estimate.of(12000.0))
                .build());
        assertThat(statistics.getColumnStatistics().get(allNullColumn)).isEqualTo(ColumnStatistics.builder()
                .setNullsFraction(Estimate.of(1.0))
                .setDistinctValuesCount(Estimate.of(0))
                .setDataSize(Estimate.zero())
                .build());
    }

    @Test
    public void testGetTableStatisticsNotAnalyzed()
    {
        JdbcTableHandle tableHandle = new JdbcTableHandle(
                new SchemaTableName("tpch", "orders"),
                new RemoteTableName(Optional.empty(), Optional.of("TPCH"), "ORDERS"),
                Optional.empty());

        ConnectionFactory connectionFactory = createMockStatisticsConnectionFactory(Optional.empty(), Optional.empty(), List.of());
        JdbcClient client = createClientWithColumns(new OracleConfig(), new JdbcStatisticsConfig(), connectionFactory, List.of());

        assertThat(client.getTableStatistics(SESSION, tableHandle)).isEqualTo(TableStatistics.empty());
    }

    @Test
    public void testGetTableStatisticsDisabled()
    {
        JdbcTableHandle tableHandle = new JdbcTableHandle(
                new SchemaTableName("tpch", "orders"),
                new RemoteTableName(Optional.empty(), Optional.of("TPCH"), "ORDERS"),
                Optional.empty());

        JdbcClient client = createClientWithColumns(
                new OracleConfig(),
                new JdbcStatisticsConfig().setEnabled(false),
                _ -> {
                    throw new AssertionError("Connection should not be opened when statistics are disabled");
                },
                List.of());

        assertThat(client.getTableStatistics(SESSION, tableHandle)).isEqualTo(TableStatistics.empty());
    }

    @Test
    public void testGetTableStatisticsForSynonym()
    {
        JdbcTableHandle synonymHandle = new JdbcTableHandle(
                new SchemaTableName("tpch", "orders_syn"),
                new RemoteTableName(Optional.empty(), Optional.of("TPCH"), "ORDERS_SYN"),
                Optional.empty());

        JdbcColumnHandle idColumn = JdbcColumnHandle.builder()
                .setColumnName("ID")
                .setJdbcTypeHandle(new JdbcTypeHandle(OracleTypes.NUMBER, Optional.of("NUMBER"), Optional.of(19), Optional.of(0), Optional.empty(), Optional.empty()))
                .setColumnType(createDecimalType(19, 0))
                .build();

        ConnectionFactory connectionFactory = createMockStatisticsConnectionFactory(
                Optional.of(500L),
                Optional.of(new RemoteTableName(Optional.of("TARGET_OWNER"), Optional.of("TARGET_OWNER"), "TARGET_ORDERS")),
                List.of(Map.of("COLUMN_NAME", "ID", "NUM_DISTINCT", 500L, "NUM_NULLS", 0L, "AVG_COL_LEN", 5L)));

        JdbcClient client = createClientWithColumns(
                new OracleConfig().setSynonymsEnabled(true),
                new JdbcStatisticsConfig(),
                connectionFactory,
                List.of(idColumn));

        TableStatistics statistics = client.getTableStatistics(SESSION, synonymHandle);
        assertThat(statistics.getRowCount()).isEqualTo(Estimate.of(500));
        assertThat(statistics.getColumnStatistics().get(idColumn)).isEqualTo(ColumnStatistics.builder()
                .setNullsFraction(Estimate.of(0.0))
                .setDistinctValuesCount(Estimate.of(500))
                .setDataSize(Estimate.of(2500.0))
                .build());
    }

    private static JdbcClient createClientWithColumns(
            OracleConfig oracleConfig,
            JdbcStatisticsConfig statisticsConfig,
            ConnectionFactory connectionFactory,
            List<JdbcColumnHandle> columns)
    {
        return new OracleClient(
                new BaseJdbcConfig(),
                oracleConfig,
                statisticsConfig,
                connectionFactory,
                new OracleQueryBuilder(RemoteQueryModifier.NONE),
                new DefaultIdentifierMapping(),
                RemoteQueryModifier.NONE)
        {
            @Override
            public List<JdbcColumnHandle> getColumns(ConnectorSession session, SchemaTableName schemaTableName, RemoteTableName remoteTableName)
            {
                return ImmutableList.copyOf(columns);
            }
        };
    }

    private static ConnectionFactory createMockStatisticsConnectionFactory(
            Optional<Long> rowCount,
            Optional<RemoteTableName> synonymTarget,
            List<Map<String, Object>> columnStatsRows)
    {
        return _ -> newProxy(Connection.class, (_, method, args) -> {
            if (method.getName().equals("close")) {
                return null;
            }
            if (method.getName().equals("getMetaData")) {
                return newProxy(DatabaseMetaData.class, (_, metaMethod, _) -> {
                    if (metaMethod.getName().equals("storesUpperCaseIdentifiers")) {
                        return true;
                    }
                    return null;
                });
            }
            if (method.getName().equals("prepareStatement")) {
                String sql = (String) args[0];
                String[] boundParams = new String[4];
                return newProxy(PreparedStatement.class, (_, stmtMethod, stmtArgs) -> {
                    if (stmtMethod.getName().equals("close")) {
                        return null;
                    }
                    if (stmtMethod.getName().equals("setString")) {
                        boundParams[(int) stmtArgs[0]] = (String) stmtArgs[1];
                        return null;
                    }
                    if (stmtMethod.getName().equals("executeQuery")) {
                        if (sql.contains("FROM ALL_TABLES")) {
                            boolean matchesTarget = synonymTarget.isEmpty() ||
                                    (synonymTarget.get().getSchemaName().orElseThrow().equals(boundParams[1]) &&
                                            synonymTarget.get().getTableName().equals(boundParams[2]));
                            List<Map<String, Object>> rows = (matchesTarget && rowCount.isPresent())
                                    ? List.of(Map.of("NUM_ROWS", rowCount.get()))
                                    : List.of();
                            return createMockResultSet(rows);
                        }
                        if (sql.contains("FROM ALL_SYNONYMS")) {
                            List<Map<String, Object>> rows = synonymTarget
                                    .map(target -> List.of(Map.<String, Object>of(
                                            "TABLE_OWNER", target.getSchemaName().orElseThrow(),
                                            "TABLE_NAME", target.getTableName())))
                                    .orElseGet(List::of);
                            return createMockResultSet(rows);
                        }
                        if (sql.contains("FROM ALL_TAB_COL_STATISTICS")) {
                            return createMockResultSet(columnStatsRows);
                        }
                    }
                    throw new UnsupportedOperationException("Unexpected PreparedStatement method: " + stmtMethod);
                });
            }
            throw new UnsupportedOperationException("Unexpected Connection method: " + method);
        });
    }

    private static ResultSet createMockResultSet(List<Map<String, Object>> rows)
    {
        Iterator<Map<String, Object>> iterator = rows.iterator();
        return newProxy(ResultSet.class, new InvocationHandler()
        {
            private Map<String, Object> currentRow;
            private boolean lastWasNull;

            @Override
            public Object invoke(Object proxy, Method method, Object[] args)
            {
                return switch (method.getName()) {
                    case "close" -> null;
                    case "next" -> {
                        if (iterator.hasNext()) {
                            currentRow = iterator.next();
                            yield true;
                        }
                        yield false;
                    }
                    case "getString" -> {
                        Object value = currentRow.get((String) args[0]);
                        lastWasNull = (value == null);
                        yield (String) value;
                    }
                    case "getLong" -> {
                        Object value = currentRow.get((String) args[0]);
                        lastWasNull = (value == null);
                        yield value == null ? 0L : ((Number) value).longValue();
                    }
                    case "wasNull" -> lastWasNull;
                    default -> throw new UnsupportedOperationException("Unexpected ResultSet method: " + method);
                };
            }
        });
    }
}
