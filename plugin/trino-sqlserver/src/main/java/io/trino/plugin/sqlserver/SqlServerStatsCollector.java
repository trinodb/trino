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

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.MoreCollectors.toOptional;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SqlServerStatsCollector
{
    private static final Logger log = Logger.get(SqlServerStatsCollector.class);

    private final ConnectionFactory connectionFactory;

    @Inject
    public SqlServerStatsCollector(ConnectionFactory connectionFactory)
    {
        this.connectionFactory = requireNonNull(connectionFactory, "connectionFactory is null");
    }

    TableStatistics readTableStatistics(ConnectorSession session, JdbcTableHandle table, SqlServerClient sqlServerClient)
            throws SQLException
    {
        checkArgument(table.isNamedRelation(), "Relation is not a table: %s", table);

        try (Connection connection = connectionFactory.openConnection(session);
                Handle handle = Jdbi.open(connection)) {
            RemoteTableName remoteTableName = table.getRequiredNamedRelation().getRemoteTableName();
            String catalog = remoteTableName.getCatalogName().orElse(null);
            String schema = remoteTableName.getSchemaName().orElse(null);
            String tableName = remoteTableName.getTableName();

            StatisticsDao statisticsDao = new StatisticsDao(handle);
            Long tableObjectId = statisticsDao.getTableObjectId(catalog, schema, tableName);
            if (tableObjectId == null) {
                // Table not found
                return TableStatistics.empty();
            }

            Long rowCount = statisticsDao.getRowCount(tableObjectId);
            if (rowCount == null) {
                // Table disappeared
                return TableStatistics.empty();
            }

            if (rowCount == 0) {
                return TableStatistics.empty();
            }

            TableStatistics.Builder tableStatistics = TableStatistics.builder();
            tableStatistics.setRowCount(Estimate.of(rowCount));

            Map<String, String> columnNameToStatisticsName = getColumnNameToStatisticsName(table, statisticsDao, tableObjectId);

            for (JdbcColumnHandle column : sqlServerClient.getColumns(session, table)) {
                String statisticName = columnNameToStatisticsName.get(column.getColumnName());
                if (statisticName == null) {
                    // No statistic for column
                    continue;
                }

                double averageColumnLength;
                long notNullValues = 0;
                long nullValues = 0;
                long distinctValues = 0;

                try (CallableStatement showStatistics = handle.getConnection().prepareCall("DBCC SHOW_STATISTICS (?, ?)")) {
                    showStatistics.setString(1, format("%s.%s.%s", catalog, schema, tableName));
                    showStatistics.setString(2, statisticName);

                    boolean isResultSet = showStatistics.execute();
                    checkState(isResultSet, "Expected SHOW_STATISTICS to return a result set");
                    try (ResultSet resultSet = showStatistics.getResultSet()) {
                        checkState(resultSet.next(), "No rows in result set");

                        averageColumnLength = resultSet.getDouble("Average Key Length"); // NULL values are accounted for with length 0

                        checkState(!resultSet.next(), "More than one row in result set");
                    }

                    isResultSet = showStatistics.getMoreResults();
                    checkState(isResultSet, "Expected SHOW_STATISTICS to return second result set");
                    showStatistics.getResultSet().close();

                    isResultSet = showStatistics.getMoreResults();
                    checkState(isResultSet, "Expected SHOW_STATISTICS to return third result set");
                    try (ResultSet resultSet = showStatistics.getResultSet()) {
                        while (resultSet.next()) {
                            resultSet.getObject("RANGE_HI_KEY");
                            if (resultSet.wasNull()) {
                                // Null fraction
                                checkState(resultSet.getLong("RANGE_ROWS") == 0, "Unexpected RANGE_ROWS for null fraction");
                                checkState(resultSet.getLong("DISTINCT_RANGE_ROWS") == 0, "Unexpected DISTINCT_RANGE_ROWS for null fraction");
                                checkState(nullValues == 0, "Multiple null fraction entries");
                                nullValues += resultSet.getLong("EQ_ROWS");
                            }
                            else {
                                // TODO discover min/max from resultSet.getXxx("RANGE_HI_KEY")
                                notNullValues += resultSet.getLong("RANGE_ROWS") // rows strictly within a bucket
                                        + resultSet.getLong("EQ_ROWS"); // rows equal to RANGE_HI_KEY
                                distinctValues += resultSet.getLong("DISTINCT_RANGE_ROWS") // NDV strictly within a bucket
                                        + (resultSet.getLong("EQ_ROWS") > 0 ? 1 : 0);
                            }
                        }
                    }
                }

                ColumnStatistics statistics = ColumnStatistics.builder()
                        .setNullsFraction(Estimate.of(
                                (notNullValues + nullValues == 0)
                                        ? 1
                                        : (1.0 * nullValues / (notNullValues + nullValues))))
                        .setDistinctValuesCount(Estimate.of(distinctValues))
                        .setDataSize(Estimate.of(rowCount * averageColumnLength))
                        .build();

                tableStatistics.setColumnStatistics(column, statistics);
            }

            return tableStatistics.build();
        }
    }

    private static class StatisticsDao
    {
        private final Handle handle;

        public StatisticsDao(Handle handle)
        {
            this.handle = requireNonNull(handle, "handle is null");
        }

        Long getTableObjectId(String catalog, String schema, String tableName)
        {
            return handle.createQuery("SELECT object_id(:table)")
                    .bind("table", format("%s.%s.%s", catalog, schema, tableName))
                    .mapTo(Long.class)
                    .one();
        }

        Long getRowCount(long tableObjectId)
        {
            return handle.createQuery("" +
                            "SELECT sum(rows) row_count " +
                            "FROM sys.partitions " +
                            "WHERE object_id = :object_id " +
                            "AND index_id IN (0, 1)") // 0 = heap, 1 = clustered index, 2 or greater = non-clustered index
                    .bind("object_id", tableObjectId)
                    .mapTo(Long.class)
                    .one();
        }

        List<String> getSingleColumnStatistics(long tableObjectId)
        {
            return handle.createQuery("" +
                            "SELECT s.name " +
                            "FROM sys.stats AS s " +
                            "JOIN sys.stats_columns AS sc ON s.object_id = sc.object_id AND s.stats_id = sc.stats_id " +
                            "WHERE s.object_id = :object_id " +
                            "GROUP BY s.name " +
                            "HAVING count(*) = 1 " +
                            "ORDER BY s.name")
                    .bind("object_id", tableObjectId)
                    .mapTo(String.class)
                    .list();
        }

        String getSingleColumnStatisticsColumnName(long tableObjectId, String statisticsName)
        {
            return handle.createQuery("" +
                            "SELECT c.name " +
                            "FROM sys.stats AS s " +
                            "JOIN sys.stats_columns AS sc ON s.object_id = sc.object_id AND s.stats_id = sc.stats_id " +
                            "JOIN sys.columns AS c ON sc.object_id = c.object_id AND c.column_id = sc.column_id " +
                            "WHERE s.object_id = :object_id " +
                            "AND s.name = :statistics_name")
                    .bind("object_id", tableObjectId)
                    .bind("statistics_name", statisticsName)
                    .mapTo(String.class)
                    .collect(toOptional()) // verify there is no more than 1 column name returned
                    .orElse(null);
        }
    }

    private static Map<String, String> getColumnNameToStatisticsName(JdbcTableHandle table, StatisticsDao statisticsDao, Long tableObjectId)
    {
        List<String> singleColumnStatistics = statisticsDao.getSingleColumnStatistics(tableObjectId);

        Map<String, String> columnNameToStatisticsName = new HashMap<>();
        for (String statisticName : singleColumnStatistics) {
            String columnName = statisticsDao.getSingleColumnStatisticsColumnName(tableObjectId, statisticName);
            if (columnName == null) {
                // Table or statistics disappeared
                continue;
            }

            if (columnNameToStatisticsName.putIfAbsent(columnName, statisticName) != null) {
                log.debug("Multiple statistics for %s in %s: %s and %s", columnName, table, columnNameToStatisticsName.get(columnName), statisticName);
            }
        }
        return columnNameToStatisticsName;
    }
}
