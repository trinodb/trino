/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.sqlserver;

import com.starburstdata.presto.plugin.jdbc.redirection.RedirectionsProvider;
import com.starburstdata.presto.plugin.jdbc.redirection.TableScanRedirection;
import com.starburstdata.presto.plugin.jdbc.stats.JdbcStatisticsConfig;
import com.starburstdata.presto.plugin.jdbc.stats.TableStatisticsClient;
import io.airlift.log.Logger;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.plugin.sqlserver.SqlServerClient;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.TableScanRedirectApplicationResult;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.statistics.ColumnStatistics;
import io.prestosql.spi.statistics.Estimate;
import io.prestosql.spi.statistics.TableStatistics;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import javax.inject.Inject;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.MoreCollectors.toOptional;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class StarburstSqlServerClient
        extends SqlServerClient
{
    private static final Logger log = Logger.get(StarburstSqlServerClient.class);

    private final TableStatisticsClient tableStatisticsClient;
    private final TableScanRedirection tableScanRedirection;

    @Inject
    public StarburstSqlServerClient(
            BaseJdbcConfig config,
            JdbcStatisticsConfig statisticsConfig,
            RedirectionsProvider redirectionsProvider,
            ConnectionFactory connectionFactory)
    {
        super(config, connectionFactory);
        tableStatisticsClient = new TableStatisticsClient(this::readTableStatistics, statisticsConfig);
        tableScanRedirection = new TableScanRedirection(redirectionsProvider);
    }

    @Override
    public Optional<TableScanRedirectApplicationResult> getTableScanRedirection(ConnectorSession session, JdbcTableHandle handle)
    {
        return tableScanRedirection.getTableScanRedirection(session, handle, this);
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle, TupleDomain<ColumnHandle> tupleDomain)
    {
        return tableStatisticsClient.getTableStatistics(session, handle, tupleDomain);
    }

    private Optional<TableStatistics> readTableStatistics(ConnectorSession session, JdbcTableHandle table)
            throws SQLException
    {
        if (table.getGroupingSets().isPresent()) {
            // TODO retrieve statistics for base table and derive statistics for the aggregation
            return Optional.empty();
        }

        try (Connection connection = connectionFactory.openConnection(session);
                Handle handle = Jdbi.open(connection)) {
            String catalog = table.getCatalogName();
            String schema = table.getSchemaName();
            String tableName = table.getTableName();

            StatisticsDao statisticsDao = new StatisticsDao(handle);
            Long tableObjectId = statisticsDao.getTableObjectId(catalog, schema, tableName);
            if (tableObjectId == null) {
                // Table not found
                return Optional.empty();
            }

            Long rowCount = statisticsDao.getRowCount(tableObjectId);
            if (rowCount == null) {
                // Table disappeared
                return Optional.empty();
            }

            if (rowCount == 0) {
                return Optional.of(TableStatistics.empty());
            }

            TableStatistics.Builder tableStatistics = TableStatistics.builder();
            tableStatistics.setRowCount(Estimate.of(rowCount));

            Map<String, String> columnNameToStatisticsName = getColumnNameToStatisticsName(table, statisticsDao, tableObjectId);

            for (JdbcColumnHandle column : this.getColumns(session, table)) {
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

            return Optional.of(tableStatistics.build());
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
                    .findOnly();
        }

        Long getRowCount(long tableObjectId)
        {
            return handle.createQuery("" +
                    "SELECT sum(rows) row_count " +
                    "FROM sys.partitions " +
                    "WHERE object_id = :object_id")
                    .bind("object_id", tableObjectId)
                    .mapTo(Long.class)
                    .findOnly();
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
}
