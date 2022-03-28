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

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.starburstdata.presto.plugin.jdbc.redirection.TableScanRedirection;
import com.starburstdata.presto.plugin.jdbc.stats.JdbcStatisticsConfig;
import io.airlift.log.Logger;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcJoinCondition;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.mapping.IdentifierMapping;
import io.trino.plugin.sqlserver.SqlServerClient;
import io.trino.plugin.sqlserver.SqlServerConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.TableScanRedirectApplicationResult;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.collect.MoreCollectors.toOptional;
import static com.starburstdata.presto.plugin.sqlserver.StarburstCommonSqlServerSessionProperties.isBulkCopyForWrite;
import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerSessionProperties.isBulkCopyForWriteLockDestinationTable;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.JdbcJoinPushdownUtil.implementJoinCostAware;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class StarburstSqlServerClient
        extends SqlServerClient
{
    private static final Logger log = Logger.get(StarburstSqlServerClient.class);

    private final boolean statisticsEnabled;
    private final TableScanRedirection tableScanRedirection;

    @Inject
    public StarburstSqlServerClient(
            BaseJdbcConfig config,
            SqlServerConfig sqlServerConfig,
            JdbcStatisticsConfig statisticsConfig,
            TableScanRedirection tableScanRedirection,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping)
    {
        super(config, sqlServerConfig, connectionFactory, queryBuilder, identifierMapping);
        this.statisticsEnabled = requireNonNull(statisticsConfig, "statisticsConfig is null").isEnabled();
        this.tableScanRedirection = requireNonNull(tableScanRedirection, "tableScanRedirection is null");
    }

    @Override
    public Optional<PreparedQuery> implementJoin(
            ConnectorSession session,
            JoinType joinType,
            PreparedQuery leftSource,
            PreparedQuery rightSource,
            List<JdbcJoinCondition> joinConditions,
            Map<JdbcColumnHandle, String> rightAssignments,
            Map<JdbcColumnHandle, String> leftAssignments,
            JoinStatistics statistics)
    {
        return implementJoinCostAware(
                session,
                joinType,
                leftSource,
                rightSource,
                statistics,
                () -> super.implementJoin(session, joinType, leftSource, rightSource, joinConditions, rightAssignments, leftAssignments, statistics));
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        JdbcOutputTableHandle table = super.beginCreateTable(session, tableMetadata);
        enableTableLockOnBulkLoadTableOption(session, table);
        return table;
    }

    @Override
    public JdbcOutputTableHandle beginInsertTable(ConnectorSession session, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columns)
    {
        JdbcOutputTableHandle table = super.beginInsertTable(session, tableHandle, columns);
        enableTableLockOnBulkLoadTableOption(session, table);
        return table;
    }

    private void enableTableLockOnBulkLoadTableOption(ConnectorSession session, JdbcOutputTableHandle table)
    {
        if (!isTableLockNeeded(session)) {
            return;
        }
        try (Connection connection = connectionFactory.openConnection(session)) {
            // 'table lock on bulk load' table option causes the bulk load processes on user-defined tables to obtain a bulk update lock
            // note: this is not a request to lock a table immediately
            String sql = format("EXEC sp_tableoption '%s', 'table lock on bulk load', '1'",
                    quoted(table.getCatalogName(), table.getSchemaName(), table.getTemporaryTableName()));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected String buildInsertSql(ConnectorSession session, RemoteTableName targetTable, RemoteTableName sourceTable, List<String> columnNames)
    {
        String columns = columnNames.stream()
                .map(this::quoted)
                .collect(joining(", "));
        return format("INSERT INTO %s %s (%s) SELECT %s FROM %s",
                targetTable,
                isTableLockNeeded(session) ? "WITH (TABLOCK)" : "", // TABLOCK is a prerequisite for minimal logging in SQL Server
                columns,
                columns,
                sourceTable);
    }

    /**
     * Table lock is a prerequisite for `minimal logging` in SQL Server
     *
     * @see <a href="https://docs.microsoft.com/en-us/sql/relational-databases/import-export/prerequisites-for-minimal-logging-in-bulk-import">minimal logging</a>
     */
    protected boolean isTableLockNeeded(ConnectorSession session)
    {
        return isBulkCopyForWrite(session) && isBulkCopyForWriteLockDestinationTable(session);
    }

    @Override
    public Optional<TableScanRedirectApplicationResult> getTableScanRedirection(ConnectorSession session, JdbcTableHandle handle)
    {
        return tableScanRedirection.getTableScanRedirection(session, handle, this);
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle, TupleDomain<ColumnHandle> tupleDomain)
    {
        if (!statisticsEnabled) {
            return TableStatistics.empty();
        }
        if (!handle.isNamedRelation()) {
            return TableStatistics.empty();
        }
        try {
            return readTableStatistics(session, handle);
        }
        catch (SQLException | RuntimeException e) {
            throwIfInstanceOf(e, TrinoException.class);
            throw new TrinoException(JDBC_ERROR, "Failed fetching statistics for table: " + handle, e);
        }
    }

    @Override
    public Connection getConnection(ConnectorSession session, JdbcOutputTableHandle handle)
            throws SQLException
    {
        Connection connection = super.getConnection(session, handle);
        try {
            connection.unwrap(SQLServerConnection.class)
                    .setUseBulkCopyForBatchInsert(isBulkCopyForWrite(session));
        }
        catch (SQLException e) {
            connection.close();
            throw e;
        }
        return connection;
    }

    private TableStatistics readTableStatistics(ConnectorSession session, JdbcTableHandle table)
            throws SQLException
    {
        checkArgument(table.isNamedRelation(), "Relation is not a table: %s", table);

        try (Connection connection = connectionFactory.openConnection(session);
                Handle handle = Jdbi.open(connection)) {
            String catalog = table.getCatalogName();
            String schema = table.getSchemaName();
            String tableName = table.getTableName();

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

            return tableStatistics.build();
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
                    "WHERE object_id = :object_id " +
                    "AND index_id IN (0, 1)") // 0 = heap, 1 = clustered index, 2 or greater = non-clustered index
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
