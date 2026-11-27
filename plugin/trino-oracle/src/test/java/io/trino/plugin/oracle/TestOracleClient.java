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
import io.trino.plugin.jdbc.JdbcQueryRelationHandle;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.WriteFunction;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;
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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;

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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    private static final JdbcColumnHandle ID_COLUMN = JdbcColumnHandle.builder()
            .setColumnName("ID")
            .setJdbcTypeHandle(new JdbcTypeHandle(OracleTypes.NUMBER, Optional.of("NUMBER"), Optional.of(19), Optional.of(0), Optional.empty(), Optional.empty()))
            .setColumnType(createDecimalType(19, 0))
            .build();

    private static final JdbcColumnHandle NAME_COLUMN = JdbcColumnHandle.builder()
            .setColumnName("NAME")
            .setJdbcTypeHandle(new JdbcTypeHandle(OracleTypes.VARCHAR, Optional.of("VARCHAR2"), Optional.of(50), Optional.empty(), Optional.empty(), Optional.empty()))
            .setColumnType(createVarcharType(50))
            .build();

    private static final JdbcColumnHandle NOTES_COLUMN = JdbcColumnHandle.builder()
            .setColumnName("NOTES")
            .setJdbcTypeHandle(new JdbcTypeHandle(OracleTypes.VARCHAR, Optional.of("VARCHAR2"), Optional.of(100), Optional.empty(), Optional.empty(), Optional.empty()))
            .setColumnType(createVarcharType(100))
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

        MockStatisticsContext mock = new MockStatisticsContext(
                Map.of(new TableKey("TPCH", "ORDERS"), Optional.of(1000L)),
                Map.of(),
                Map.of(
                        new TableKey("TPCH", "ORDERS"), List.of(
                                columnStatRow("ID", 1000L, 0L, 6L),
                                columnStatRow("NAME", 50L, 200L, 12L),
                                columnStatRow("NOTES", 0L, 1000L, 0L))));

        JdbcClient client = createClientWithColumns(new OracleConfig(), new JdbcStatisticsConfig(), mock, List.of(ID_COLUMN, NAME_COLUMN, NOTES_COLUMN));
        TableStatistics statistics = client.getTableStatistics(SESSION, tableHandle);

        assertThat(mock.getColumnsCalls.get()).isEqualTo(1);
        assertThat(mock.activeConnections.get()).isZero();
        assertThat(statistics.getRowCount()).isEqualTo(Estimate.of(1000));
        assertThat(statistics.getColumnStatistics().get(ID_COLUMN)).isEqualTo(ColumnStatistics.builder()
                .setNullsFraction(Estimate.of(0.0))
                .setDistinctValuesCount(Estimate.of(1000))
                .setDataSize(Estimate.of(6000.0))
                .build());
        assertThat(statistics.getColumnStatistics().get(NAME_COLUMN)).isEqualTo(ColumnStatistics.builder()
                .setNullsFraction(Estimate.of(0.2))
                .setDistinctValuesCount(Estimate.of(50))
                .setDataSize(Estimate.of(12000.0))
                .build());
        assertThat(statistics.getColumnStatistics().get(NOTES_COLUMN)).isEqualTo(ColumnStatistics.builder()
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

        MockStatisticsContext mock = new MockStatisticsContext(
                Map.of(new TableKey("TPCH", "ORDERS"), Optional.empty()),
                Map.of(),
                Map.of());
        JdbcClient client = createClientWithColumns(new OracleConfig(), new JdbcStatisticsConfig(), mock, List.of(ID_COLUMN));

        assertThat(client.getTableStatistics(SESSION, tableHandle)).isEqualTo(TableStatistics.empty());
        assertThat(mock.synonymQueries.get()).isZero();
        assertThat(mock.columnStatsQueries.get()).isZero();
        assertThat(mock.getColumnsCalls.get()).isZero();
        assertThat(mock.activeConnections.get()).isZero();
    }

    @Test
    public void testGetTableStatisticsEmptyTable()
    {
        JdbcTableHandle tableHandle = new JdbcTableHandle(
                new SchemaTableName("tpch", "orders"),
                new RemoteTableName(Optional.empty(), Optional.of("TPCH"), "ORDERS"),
                Optional.empty());

        MockStatisticsContext mock = new MockStatisticsContext(
                Map.of(new TableKey("TPCH", "ORDERS"), Optional.of(0L)),
                Map.of(),
                Map.of());
        JdbcClient client = createClientWithColumns(new OracleConfig(), new JdbcStatisticsConfig(), mock, List.of(ID_COLUMN));

        TableStatistics statistics = client.getTableStatistics(SESSION, tableHandle);
        assertThat(statistics).isEqualTo(TableStatistics.builder().setRowCount(Estimate.of(0)).build());
        assertThat(mock.columnStatsQueries.get()).isZero();
        assertThat(mock.getColumnsCalls.get()).isZero();
        assertThat(mock.activeConnections.get()).isZero();
    }

    @Test
    public void testGetTableStatisticsDisabled()
    {
        JdbcTableHandle tableHandle = new JdbcTableHandle(
                new SchemaTableName("tpch", "orders"),
                new RemoteTableName(Optional.empty(), Optional.of("TPCH"), "ORDERS"),
                Optional.empty());

        JdbcClient client = new OracleClient(
                new BaseJdbcConfig(),
                new OracleConfig(),
                new JdbcStatisticsConfig().setEnabled(false),
                _ -> {
                    throw new AssertionError("Connection should not be opened when statistics are disabled");
                },
                new OracleQueryBuilder(RemoteQueryModifier.NONE),
                new DefaultIdentifierMapping(),
                RemoteQueryModifier.NONE);

        assertThat(client.getTableStatistics(SESSION, tableHandle)).isEqualTo(TableStatistics.empty());
    }

    @Test
    public void testGetTableStatisticsNonNamedRelation()
    {
        JdbcTableHandle queryHandle = new JdbcTableHandle(
                new JdbcQueryRelationHandle(new PreparedQuery("SELECT 1 FROM DUAL", ImmutableList.of())),
                TupleDomain.all(),
                ImmutableList.of(),
                Optional.empty(),
                OptionalLong.empty(),
                Optional.empty(),
                Optional.empty(),
                0,
                Optional.empty(),
                ImmutableList.of());

        JdbcClient client = new OracleClient(
                new BaseJdbcConfig(),
                new OracleConfig(),
                new JdbcStatisticsConfig(),
                _ -> {
                    throw new AssertionError("Connection should not be opened for query relations");
                },
                new OracleQueryBuilder(RemoteQueryModifier.NONE),
                new DefaultIdentifierMapping(),
                RemoteQueryModifier.NONE);

        assertThat(client.getTableStatistics(SESSION, queryHandle)).isEqualTo(TableStatistics.empty());
    }

    @Test
    public void testGetTableStatisticsForSynonym()
    {
        JdbcTableHandle synonymHandle = new JdbcTableHandle(
                new SchemaTableName("tpch", "orders_syn"),
                new RemoteTableName(Optional.empty(), Optional.of("TPCH"), "ORDERS_SYN"),
                Optional.empty());

        // Column statistics exist ONLY under TARGET_OWNER.TARGET_ORDERS (never under TPCH.ORDERS_SYN)
        MockStatisticsContext mock = new MockStatisticsContext(
                Map.of(new TableKey("TARGET_OWNER", "TARGET_ORDERS"), Optional.of(500L)),
                Map.of(new TableKey("TPCH", "ORDERS_SYN"), new TableKey("TARGET_OWNER", "TARGET_ORDERS")),
                Map.of(
                        new TableKey("TARGET_OWNER", "TARGET_ORDERS"), List.of(columnStatRow("ID", 500L, 0L, 5L))));

        JdbcClient client = createClientWithColumns(
                new OracleConfig().setSynonymsEnabled(true),
                new JdbcStatisticsConfig(),
                mock,
                List.of(ID_COLUMN));

        TableStatistics statistics = client.getTableStatistics(SESSION, synonymHandle);
        assertThat(mock.synonymQueries.get()).isEqualTo(1);
        assertThat(mock.columnStatsQueries.get()).isEqualTo(1);
        assertThat(mock.activeConnections.get()).isZero();
        assertThat(statistics.getRowCount()).isEqualTo(Estimate.of(500));
        assertThat(statistics.getColumnStatistics().get(ID_COLUMN)).isEqualTo(ColumnStatistics.builder()
                .setNullsFraction(Estimate.of(0.0))
                .setDistinctValuesCount(Estimate.of(500))
                .setDataSize(Estimate.of(2500.0))
                .build());
    }

    @Test
    public void testGetTableStatisticsPublicSynonym()
    {
        JdbcTableHandle publicSynonymHandle = new JdbcTableHandle(
                new SchemaTableName("public", "orders_pub"),
                new RemoteTableName(Optional.empty(), Optional.of("PUBLIC"), "ORDERS_PUB"),
                Optional.empty());

        MockStatisticsContext mock = new MockStatisticsContext(
                Map.of(new TableKey("TARGET_OWNER", "TARGET_ORDERS"), Optional.of(250L)),
                Map.of(new TableKey("PUBLIC", "ORDERS_PUB"), new TableKey("TARGET_OWNER", "TARGET_ORDERS")),
                Map.of(
                        new TableKey("TARGET_OWNER", "TARGET_ORDERS"), List.of(columnStatRow("ID", 250L, 0L, 4L))));

        JdbcClient client = createClientWithColumns(
                new OracleConfig().setSynonymsEnabled(true),
                new JdbcStatisticsConfig(),
                mock,
                List.of(ID_COLUMN));

        TableStatistics statistics = client.getTableStatistics(SESSION, publicSynonymHandle);
        assertThat(statistics.getRowCount()).isEqualTo(Estimate.of(250));
        assertThat(statistics.getColumnStatistics().get(ID_COLUMN)).isEqualTo(ColumnStatistics.builder()
                .setNullsFraction(Estimate.of(0.0))
                .setDistinctValuesCount(Estimate.of(250))
                .setDataSize(Estimate.of(1000.0))
                .build());
    }

    @Test
    public void testGetTableStatisticsForChainedSynonyms()
    {
        JdbcTableHandle chainedSynonymHandle = new JdbcTableHandle(
                new SchemaTableName("tpch", "orders_syn1"),
                new RemoteTableName(Optional.empty(), Optional.of("TPCH"), "ORDERS_SYN1"),
                Optional.empty());

        MockStatisticsContext mock = new MockStatisticsContext(
                Map.of(new TableKey("TARGET_OWNER", "TARGET_ORDERS"), Optional.of(300L)),
                Map.of(
                        new TableKey("TPCH", "ORDERS_SYN1"), new TableKey("TPCH", "ORDERS_SYN2"),
                        new TableKey("TPCH", "ORDERS_SYN2"), new TableKey("TARGET_OWNER", "TARGET_ORDERS")),
                Map.of(
                        new TableKey("TARGET_OWNER", "TARGET_ORDERS"), List.of(columnStatRow("ID", 300L, 0L, 5L))));

        JdbcClient client = createClientWithColumns(
                new OracleConfig().setSynonymsEnabled(true),
                new JdbcStatisticsConfig(),
                mock,
                List.of(ID_COLUMN));

        TableStatistics statistics = client.getTableStatistics(SESSION, chainedSynonymHandle);
        assertThat(mock.synonymQueries.get()).isEqualTo(2);
        assertThat(statistics.getRowCount()).isEqualTo(Estimate.of(300));
        assertThat(statistics.getColumnStatistics().get(ID_COLUMN)).isEqualTo(ColumnStatistics.builder()
                .setNullsFraction(Estimate.of(0.0))
                .setDistinctValuesCount(Estimate.of(300))
                .setDataSize(Estimate.of(1500.0))
                .build());
    }

    @Test
    public void testGetTableStatisticsForCircularSynonyms()
    {
        JdbcTableHandle circularHandle = new JdbcTableHandle(
                new SchemaTableName("tpch", "syn1"),
                new RemoteTableName(Optional.empty(), Optional.of("TPCH"), "SYN1"),
                Optional.empty());

        MockStatisticsContext mock = new MockStatisticsContext(
                Map.of(),
                Map.of(
                        new TableKey("TPCH", "SYN1"), new TableKey("TPCH", "SYN2"),
                        new TableKey("TPCH", "SYN2"), new TableKey("TPCH", "SYN1")),
                Map.of());

        JdbcClient client = createClientWithColumns(
                new OracleConfig().setSynonymsEnabled(true),
                new JdbcStatisticsConfig(),
                mock,
                List.of(ID_COLUMN));

        assertThat(client.getTableStatistics(SESSION, circularHandle)).isEqualTo(TableStatistics.empty());
        assertThat(mock.synonymQueries.get()).isEqualTo(2);
        assertThat(mock.activeConnections.get()).isZero();
    }

    @Test
    public void testGetTableStatisticsSynonymDisabled()
    {
        JdbcTableHandle synonymHandle = new JdbcTableHandle(
                new SchemaTableName("tpch", "orders_syn"),
                new RemoteTableName(Optional.empty(), Optional.of("TPCH"), "ORDERS_SYN"),
                Optional.empty());

        MockStatisticsContext mock = new MockStatisticsContext(
                Map.of(new TableKey("TARGET_OWNER", "TARGET_ORDERS"), Optional.of(500L)),
                Map.of(new TableKey("TPCH", "ORDERS_SYN"), new TableKey("TARGET_OWNER", "TARGET_ORDERS")),
                Map.of());

        JdbcClient client = createClientWithColumns(
                new OracleConfig().setSynonymsEnabled(false),
                new JdbcStatisticsConfig(),
                mock,
                List.of(ID_COLUMN));

        assertThat(client.getTableStatistics(SESSION, synonymHandle)).isEqualTo(TableStatistics.empty());
        assertThat(mock.synonymQueries.get()).isZero();
    }

    @Test
    public void testGetTableStatisticsUnanalyzedTableDoesNotFallbackToPublicSynonym()
    {
        JdbcTableHandle unanalyzedTableHandle = new JdbcTableHandle(
                new SchemaTableName("tpch", "orders"),
                new RemoteTableName(Optional.empty(), Optional.of("TPCH"), "ORDERS"),
                Optional.empty());

        MockStatisticsContext mock = new MockStatisticsContext(
                Map.of(
                        new TableKey("TPCH", "ORDERS"), Optional.empty(),
                        new TableKey("OTHER", "ORDERS"), Optional.of(500L)),
                Map.of(new TableKey("PUBLIC", "ORDERS"), new TableKey("OTHER", "ORDERS")),
                Map.of(
                        new TableKey("OTHER", "ORDERS"), List.of(columnStatRow("ID", 500L, 0L, 5L))));

        JdbcClient client = createClientWithColumns(
                new OracleConfig().setSynonymsEnabled(true),
                new JdbcStatisticsConfig(),
                mock,
                List.of(ID_COLUMN));

        assertThat(client.getTableStatistics(SESSION, unanalyzedTableHandle)).isEqualTo(TableStatistics.empty());
        assertThat(mock.synonymQueries.get()).isZero();
    }

    @Test
    public void testGetTableStatisticsViewDoesNotFallbackToPublicSynonym()
    {
        JdbcTableHandle viewHandle = new JdbcTableHandle(
                new SchemaTableName("tpch", "my_view"),
                new RemoteTableName(Optional.empty(), Optional.of("TPCH"), "MY_VIEW"),
                Optional.empty());

        // MY_VIEW is absent from ALL_TABLES and absent from TPCH synonyms,
        // even though a PUBLIC synonym PUBLIC.MY_VIEW -> OTHER.ORDERS exists.
        MockStatisticsContext mock = new MockStatisticsContext(
                Map.of(new TableKey("OTHER", "ORDERS"), Optional.of(500L)),
                Map.of(new TableKey("PUBLIC", "MY_VIEW"), new TableKey("OTHER", "ORDERS")),
                Map.of(
                        new TableKey("OTHER", "ORDERS"), List.of(columnStatRow("ID", 500L, 0L, 5L))));

        JdbcClient client = createClientWithColumns(
                new OracleConfig().setSynonymsEnabled(true),
                new JdbcStatisticsConfig(),
                mock,
                List.of(ID_COLUMN));

        assertThat(client.getTableStatistics(SESSION, viewHandle)).isEqualTo(TableStatistics.empty());
    }

    @Test
    public void testGetTableStatisticsPartialNullClampedAndNegativeValues()
    {
        JdbcTableHandle tableHandle = new JdbcTableHandle(
                new SchemaTableName("tpch", "orders"),
                new RemoteTableName(Optional.empty(), Optional.of("TPCH"), "ORDERS"),
                Optional.empty());

        // Partial nulls and out-of-range values
        MockStatisticsContext mock = new MockStatisticsContext(
                Map.of(new TableKey("TPCH", "ORDERS"), Optional.of(100L)),
                Map.of(),
                Map.of(
                        new TableKey("TPCH", "ORDERS"), List.of(
                                columnStatRow("ID", 500L, null, null),
                                columnStatRow("NAME", 80L, 40L, 10L),
                                columnStatRow("NOTES", 10L, 150L, -5L))));

        JdbcClient client = createClientWithColumns(
                new OracleConfig(),
                new JdbcStatisticsConfig(),
                mock,
                List.of(ID_COLUMN, NAME_COLUMN, NOTES_COLUMN));

        TableStatistics statistics = client.getTableStatistics(SESSION, tableHandle);
        assertThat(statistics.getRowCount()).isEqualTo(Estimate.of(100));
        // NUM_NULLS null, AVG_COL_LEN null => unknown nullsFraction and unknown dataSize; numDistinct clamped to rowCount=100
        assertThat(statistics.getColumnStatistics().get(ID_COLUMN)).isEqualTo(ColumnStatistics.builder()
                .setDistinctValuesCount(Estimate.of(100))
                .build());
        // numDistinct=80 clamped to rowCount - numNulls = 60
        assertThat(statistics.getColumnStatistics().get(NAME_COLUMN)).isEqualTo(ColumnStatistics.builder()
                .setNullsFraction(Estimate.of(0.4))
                .setDistinctValuesCount(Estimate.of(60))
                .setDataSize(Estimate.of(1000.0))
                .build());
        // numNulls=150 >= rowCount=100 => nullsFraction clamped to 1.0, maxDistinct=0, dataSize=0 (negative avgColLen ignored)
        assertThat(statistics.getColumnStatistics().get(NOTES_COLUMN)).isEqualTo(ColumnStatistics.builder()
                .setNullsFraction(Estimate.of(1.0))
                .setDistinctValuesCount(Estimate.of(0))
                .setDataSize(Estimate.zero())
                .build());

        // Negative NUM_ROWS in dictionary => empty statistics
        MockStatisticsContext negativeRowsMock = new MockStatisticsContext(
                Map.of(new TableKey("TPCH", "ORDERS"), Optional.of(-1L)),
                Map.of(),
                Map.of());
        JdbcClient negativeRowsClient = createClientWithColumns(new OracleConfig(), new JdbcStatisticsConfig(), negativeRowsMock, List.of(ID_COLUMN));
        assertThat(negativeRowsClient.getTableStatistics(SESSION, tableHandle)).isEqualTo(TableStatistics.empty());

        // Negative NUM_NULLS / NUM_DISTINCT / AVG_COL_LEN ignored defensively
        JdbcColumnHandle negCol = ID_COLUMN;
        MockStatisticsContext negColMock = new MockStatisticsContext(
                Map.of(new TableKey("TPCH", "ORDERS"), Optional.of(100L)),
                Map.of(),
                Map.of(new TableKey("TPCH", "ORDERS"), List.of(columnStatRow("ID", -5L, -1L, -10L))));
        JdbcClient negColClient = createClientWithColumns(new OracleConfig(), new JdbcStatisticsConfig(), negColMock, List.of(negCol));
        assertThat(negColClient.getTableStatistics(SESSION, tableHandle).getColumnStatistics().get(negCol))
                .isEqualTo(ColumnStatistics.empty());
    }

    @Test
    public void testGetTableStatisticsPropagatesTrinoException()
    {
        SchemaTableName schemaTableName = new SchemaTableName("tpch", "dropped_table");
        JdbcTableHandle tableHandle = new JdbcTableHandle(
                schemaTableName,
                new RemoteTableName(Optional.empty(), Optional.of("TPCH"), "DROPPED_TABLE"),
                Optional.empty());

        MockStatisticsContext mock = new MockStatisticsContext(
                Map.of(new TableKey("TPCH", "DROPPED_TABLE"), Optional.of(100L)),
                Map.of(),
                Map.of(
                        new TableKey("TPCH", "DROPPED_TABLE"), List.of(columnStatRow("ID", 100L, 0L, 5L))));

        JdbcClient client = new OracleClient(
                new BaseJdbcConfig(),
                new OracleConfig(),
                new JdbcStatisticsConfig(),
                mock,
                new OracleQueryBuilder(RemoteQueryModifier.NONE),
                new DefaultIdentifierMapping(),
                RemoteQueryModifier.NONE)
        {
            @Override
            public List<JdbcColumnHandle> getColumns(ConnectorSession session, SchemaTableName requestedTable, RemoteTableName remoteTableName)
            {
                assertThat(mock.activeConnections.get()).isZero();
                throw new TableNotFoundException(requestedTable);
            }
        };

        assertThatThrownBy(() -> client.getTableStatistics(SESSION, tableHandle))
                .isInstanceOf(TableNotFoundException.class);
        assertThat(mock.activeConnections.get()).isZero();
    }

    @Test
    public void testGetTableStatisticsLobZeroDistinctWithNonNullRows()
    {
        JdbcTableHandle tableHandle = new JdbcTableHandle(
                new SchemaTableName("tpch", "lob_orders"),
                new RemoteTableName(Optional.empty(), Optional.of("TPCH"), "LOB_ORDERS"),
                Optional.empty());

        MockStatisticsContext mock = new MockStatisticsContext(
                Map.of(new TableKey("TPCH", "LOB_ORDERS"), Optional.of(100L)),
                Map.of(),
                Map.of(
                        new TableKey("TPCH", "LOB_ORDERS"), List.of(
                                columnStatRow("ID", 0L, 0L, 51L),
                                columnStatRow("NAME", 0L, 50L, 26L),
                                columnStatRow("NOTES", 0L, 100L, 0L))));

        JdbcClient client = createClientWithColumns(
                new OracleConfig(),
                new JdbcStatisticsConfig(),
                mock,
                List.of(ID_COLUMN, NAME_COLUMN, NOTES_COLUMN));

        TableStatistics statistics = client.getTableStatistics(SESSION, tableHandle);
        assertThat(statistics.getRowCount()).isEqualTo(Estimate.of(100));
        // Non-null LOB: NUM_DISTINCT=0 means NDV was not gathered, so distinctValuesCount stays unknown
        assertThat(statistics.getColumnStatistics().get(ID_COLUMN)).isEqualTo(ColumnStatistics.builder()
                .setNullsFraction(Estimate.of(0.0))
                .setDistinctValuesCount(Estimate.unknown())
                .setDataSize(Estimate.of(5100.0))
                .build());
        // Partially null LOB: keeps nullsFraction=0.5 and dataSize=2600.0 with unknown NDV
        assertThat(statistics.getColumnStatistics().get(NAME_COLUMN)).isEqualTo(ColumnStatistics.builder()
                .setNullsFraction(Estimate.of(0.5))
                .setDistinctValuesCount(Estimate.unknown())
                .setDataSize(Estimate.of(2600.0))
                .build());
        // All-null LOB: NUM_NULLS >= rowCount => NDV=0, nullsFraction=1.0, dataSize=0
        assertThat(statistics.getColumnStatistics().get(NOTES_COLUMN)).isEqualTo(ColumnStatistics.builder()
                .setNullsFraction(Estimate.of(1.0))
                .setDistinctValuesCount(Estimate.zero())
                .setDataSize(Estimate.zero())
                .build());
    }

    private static Map<String, Object> columnStatRow(String columnName, Long numDistinct, Long numNulls, Long avgColLen)
    {
        Map<String, Object> row = new HashMap<>();
        row.put("COLUMN_NAME", columnName);
        row.put("NUM_DISTINCT", numDistinct);
        row.put("NUM_NULLS", numNulls);
        row.put("AVG_COL_LEN", avgColLen);
        return row;
    }

    private static JdbcClient createClientWithColumns(
            OracleConfig oracleConfig,
            JdbcStatisticsConfig statisticsConfig,
            MockStatisticsContext mock,
            List<JdbcColumnHandle> columns)
    {
        return new OracleClient(
                new BaseJdbcConfig(),
                oracleConfig,
                statisticsConfig,
                mock,
                new OracleQueryBuilder(RemoteQueryModifier.NONE),
                new DefaultIdentifierMapping(),
                RemoteQueryModifier.NONE)
        {
            @Override
            public List<JdbcColumnHandle> getColumns(ConnectorSession session, SchemaTableName schemaTableName, RemoteTableName remoteTableName)
            {
                mock.getColumnsCalls.incrementAndGet();
                // Verify the statistics query connection was closed before getColumns runs (avoids connection pool deadlock)
                assertThat(mock.activeConnections.get()).isZero();
                return ImmutableList.copyOf(columns);
            }
        };
    }

    private record TableKey(String owner, String table) {}

    private static final class MockStatisticsContext
            implements ConnectionFactory
    {
        private final Map<TableKey, Optional<Long>> tables;
        private final Map<TableKey, TableKey> synonyms;
        private final Map<TableKey, List<Map<String, Object>>> columnStats;
        private final AtomicInteger activeConnections = new AtomicInteger();
        private final AtomicInteger synonymQueries = new AtomicInteger();
        private final AtomicInteger columnStatsQueries = new AtomicInteger();
        private final AtomicInteger getColumnsCalls = new AtomicInteger();

        private MockStatisticsContext(
                Map<TableKey, Optional<Long>> tables,
                Map<TableKey, TableKey> synonyms,
                Map<TableKey, List<Map<String, Object>>> columnStats)
        {
            this.tables = Map.copyOf(tables);
            this.synonyms = Map.copyOf(synonyms);
            this.columnStats = Map.copyOf(columnStats);
        }

        @Override
        public Connection openConnection(ConnectorSession session)
        {
            activeConnections.incrementAndGet();
            return newProxy(Connection.class, (_, method, args) -> {
                if (method.getName().equals("close")) {
                    activeConnections.decrementAndGet();
                    return null;
                }
                if (method.getName().equals("prepareStatement")) {
                    String sql = (String) args[0];
                    Map<Integer, String> boundParams = new HashMap<>();
                    return newProxy(PreparedStatement.class, (_, stmtMethod, stmtArgs) -> {
                        if (stmtMethod.getName().equals("close")) {
                            return null;
                        }
                        if (stmtMethod.getName().equals("setString")) {
                            int paramIndex = (int) stmtArgs[0];
                            assertThat(paramIndex).isIn(1, 2);
                            boundParams.put(paramIndex, (String) stmtArgs[1]);
                            return null;
                        }
                        if (stmtMethod.getName().equals("executeQuery")) {
                            assertThat(boundParams).containsOnlyKeys(1, 2);
                            TableKey key = new TableKey(boundParams.get(1), boundParams.get(2));
                            if (sql.contains("FROM ALL_TABLES")) {
                                if (!tables.containsKey(key)) {
                                    return createMockResultSet(List.of());
                                }
                                Optional<Long> rowCount = tables.get(key);
                                Map<String, Object> row = new HashMap<>();
                                row.put("NUM_ROWS", rowCount.orElse(null));
                                return createMockResultSet(List.of(row));
                            }
                            if (sql.contains("FROM ALL_SYNONYMS")) {
                                synonymQueries.incrementAndGet();
                                TableKey target = synonyms.get(key);
                                if (target == null) {
                                    return createMockResultSet(List.of());
                                }
                                return createMockResultSet(List.of(Map.of(
                                        "TABLE_OWNER", target.owner(),
                                        "TABLE_NAME", target.table())));
                            }
                            if (sql.contains("FROM ALL_TAB_COL_STATISTICS")) {
                                columnStatsQueries.incrementAndGet();
                                return createMockResultSet(columnStats.getOrDefault(key, List.of()));
                            }
                        }
                        throw new UnsupportedOperationException("Unexpected PreparedStatement method: " + stmtMethod);
                    });
                }
                throw new UnsupportedOperationException("Unexpected Connection method: " + method);
            });
        }
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
