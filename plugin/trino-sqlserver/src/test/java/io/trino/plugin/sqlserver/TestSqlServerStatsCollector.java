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
package io.trino.plugin.sqlserver;

import io.airlift.log.Logger;
import io.trino.plugin.base.mapping.DefaultIdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DefaultQueryBuilder;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.VarcharType;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

import java.sql.SQLException;
import java.sql.Types;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestSqlServerStatsCollector
{
    private static final Logger log = Logger.get(TestSqlServerStatsCollector.class);
    private static final String SCHEMA = "test";
    private static final String TABLE = "foo";

    private TestingSqlServer sqlServer;
    private ConnectionFactory connectionFactory;
    private SqlServerStatsCollector statsCollector;
    private SqlServerClient sqlServerClient;
    private ConnectorSession session;
    private JdbcTableHandle table;

    @BeforeAll
    public void init()
            throws Exception
    {
        this.sqlServer = new TestingSqlServer("2022-latest");
        this.connectionFactory = ignoredSession -> sqlServer.createConnection();
        this.statsCollector = new SqlServerStatsCollector(connectionFactory);
        this.sqlServerClient = new SqlServerClient(
                new BaseJdbcConfig(),
                new JdbcStatisticsConfig(),
                connectionFactory,
                new DefaultQueryBuilder(RemoteQueryModifier.NONE),
                new DefaultIdentifierMapping(),
                statsCollector,
                RemoteQueryModifier.NONE);
        this.session = TestingConnectorSession.builder().build();
        table = createTable();
    }

    private JdbcTableHandle createTable()
    {
        sqlServer.execute("CREATE SCHEMA %s".formatted(SCHEMA));

        SchemaTableName schemaTableName = new SchemaTableName(SCHEMA, TABLE);
        ImmutableList.Builder<ColumnMetadata> columnsBuilder = ImmutableList.builder();
        StringBuilder insertValues = new StringBuilder();
        for (int i = 0; i < 10; ++i) {
            columnsBuilder.add(new ColumnMetadata("c_int_" + i, IntegerType.INTEGER));
            columnsBuilder.add(new ColumnMetadata("c_varchar_" + i, VarcharType.createVarcharType(10)));
            if (i > 0) {
                insertValues.append(',');
            }
            insertValues.append(i).append(',').append("'a").append(i).append("'");
        }
        ImmutableList<ColumnMetadata> columns = columnsBuilder.build();
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(schemaTableName, columns);

        sqlServerClient.createTable(session, tableMetadata);

        sqlServer.execute("INSERT INTO %s.%s (%s) VALUES (%s)".formatted(
                SCHEMA,
                TABLE,
                columns.stream().map(ColumnMetadata::getName).collect(Collectors.joining(",")),
                insertValues.toString()));

        for (ColumnMetadata column : columns) {
            sqlServer.execute("CREATE STATISTICS %1$s ON %2$s.%3$s (%1$s)".formatted(column.getName(), SCHEMA, TABLE));
        }
        sqlServer.execute("UPDATE STATISTICS %s.%s".formatted(SCHEMA, TABLE));

        return sqlServerClient.getTableHandle(session, schemaTableName).orElseThrow();
    }

    @Test
    public void testIsNewStatisticsSupported()
            throws SQLException
    {
        // This ensures that we can use the new statistics for testing
        assertThat(statsCollector.isNewStatisticsSupported(session)).isTrue();
    }

    @Test
    public void testLegacyStatsCollection()
            throws SQLException
    {
        testStatistics(false);
    }

    @Test
    public void testNewStatsCollection()
            throws SQLException
    {
        testStatistics(true);
    }

    private void testStatistics(boolean newStats)
            throws SQLException
    {
        TestingSqlServerStatsCollector testingStatsCollector = new TestingSqlServerStatsCollector(newStats, connectionFactory);
        TableStatistics statistics = testingStatsCollector.readTableStatistics(session, table, sqlServerClient);
        assertThat(statistics.getRowCount().getValue()).isEqualTo(1.0);
        sqlServerClient.getColumns(session, table).forEach(columnHandle -> {
            ColumnStatistics columnStatistics = statistics.getColumnStatistics().get(columnHandle);
//            log.info("newStats: %s, stats: %s", newStats, statistics.getColumnStatistics());
            assertThat(columnStatistics.getNullsFraction().getValue()).isEqualTo(0.0);
            Estimate dataSize = columnStatistics.getDataSize();
            switch (columnHandle.getJdbcTypeHandle().jdbcType()) {
                case Types.INTEGER:
                    assertThat(dataSize.getValue()).isEqualTo(4.0);
                    break;
                case Types.NVARCHAR:
                case Types.VARCHAR:
                    if (newStats) {
                        assertThat(dataSize.isUnknown()).isTrue();
                    }
                    else {
                        assertThat(dataSize.getValue()).isEqualTo(4.0);
                    }
                    break;
                default:
                    throw new RuntimeException("Added columns of different types to the test but didn't update the switch: " + columnHandle.getJdbcTypeHandle().jdbcType());
            }
        });
    }

    private static class TestingSqlServerStatsCollector
            extends SqlServerStatsCollector
    {
        private final boolean supportNewStats;

        public TestingSqlServerStatsCollector(boolean supportNewStats,
                                              ConnectionFactory connectionFactory)
        {
            super(connectionFactory);
            this.supportNewStats = supportNewStats;
        }

        @Override
        boolean isNewStatisticsSupported(ConnectorSession session)
                throws SQLException
        {
            return supportNewStats;
        }
    }
}
