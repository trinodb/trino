/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.oracle;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.starburstdata.trino.plugin.license.LicenseManager;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DefaultQueryBuilder;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcMetadataConfig;
import io.trino.plugin.jdbc.JdbcNamedRelationHandle;
import io.trino.plugin.jdbc.JdbcSortItem;
import io.trino.plugin.jdbc.JdbcSplit;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.plugin.oracle.OracleClient;
import io.trino.plugin.oracle.OracleConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import oracle.jdbc.OracleConnection;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.JdbcJoinPushdownUtil.implementJoinCostAware;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;

public class StarburstOracleClient
        extends OracleClient
{
    private static final int DEFAULT_ROW_FETCH_SIZE = 1000;

    private final boolean synonymsEnabled;
    private final boolean statisticsEnabled;

    @Inject
    public StarburstOracleClient(
            LicenseManager licenseManager,
            BaseJdbcConfig config,
            JdbcMetadataConfig jdbcMetadataConfig,
            JdbcStatisticsConfig statisticsConfig,
            OracleConfig oracleConfig,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier)
    {
        super(config, oracleConfig, connectionFactory, queryBuilder, identifierMapping, queryModifier);
        synonymsEnabled = oracleConfig.isSynonymsEnabled();
        this.statisticsEnabled = requireNonNull(statisticsConfig, "statisticsConfig is null").isEnabled();
        if (jdbcMetadataConfig.isAggregationPushdownEnabled()) {
            licenseManager.checkLicense();
        }
    }

    @Override
    public Optional<PreparedQuery> implementJoin(
            ConnectorSession session,
            JoinType joinType,
            PreparedQuery leftSource,
            Map<JdbcColumnHandle, String> leftProjections,
            PreparedQuery rightSource,
            Map<JdbcColumnHandle, String> rightProjections,
            List<ParameterizedExpression> joinConditions,
            JoinStatistics statistics)
    {
        return implementJoinCostAware(
                session,
                joinType,
                leftSource,
                rightSource,
                statistics,
                () -> super.implementJoin(session, joinType, leftSource, leftProjections, rightSource, rightProjections, joinConditions, statistics));
    }

    /**
     * @deprecated This should never be called; use {@link OracleSplitManager} to get splits.
     */
    @Deprecated
    @Override
    public ConnectorSplitSource getSplits(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        throw new UnsupportedOperationException("Wrong entry point to get Oracle splits");
    }

    @Override
    public PreparedQuery prepareQuery(ConnectorSession session, JdbcTableHandle table, Optional<List<List<JdbcColumnHandle>>> groupingSets, List<JdbcColumnHandle> columns, Map<String, ParameterizedExpression> columnExpressions)
    {
        try (Connection connection = connectionFactory.openConnection(session)) {
            PreparedQuery preparedQuery = queryBuilder.prepareSelectQuery(
                    this,
                    session,
                    connection,
                    table.getRelationHandle(),
                    groupingSets,
                    columns,
                    columnExpressions,
                    table.getConstraint(),
                    Optional.empty());
            preparedQuery = applyQueryTransformations(table, preparedQuery);
            return preparedQuery;
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public PreparedStatement buildSql(ConnectorSession session, Connection connection, JdbcSplit split, JdbcTableHandle table, List<JdbcColumnHandle> columns)
            throws SQLException
    {
        OracleQueryBuilder queryBuilder = new OracleQueryBuilder(queryModifier, ((OracleSplit) split).getPartitionNames());
        PreparedQuery preparedQuery = queryBuilder.prepareSelectQuery(
                this,
                session,
                connection,
                table.getRelationHandle(),
                Optional.empty(),
                columns,
                ImmutableMap.of(),
                table.getConstraint(),
                getAdditionalPredicate(table.getConstraintExpressions(), split.getAdditionalPredicate()));
        preparedQuery = applyQueryTransformations(table, preparedQuery);
        return queryBuilder.prepareStatement(this, session, connection, preparedQuery, Optional.empty());
    }

    @Override
    public Connection getConnection(ConnectorSession session, JdbcSplit split, JdbcTableHandle tableHandle)
            throws SQLException
    {
        Connection connection = super.getConnection(session, split, tableHandle);
        try {
            // We cannot return unwrapped connection as it won't return to connection pool upon close
            connection.unwrap(OracleConnection.class)
                    .setDefaultRowPrefetch(DEFAULT_ROW_FETCH_SIZE);
        }
        catch (SQLException e) {
            connection.close();
            throw e;
        }
        return connection;
    }

    @Override
    // TODO: migrate to OSS?
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        if (tableHandle.getColumns().isPresent()) {
            return tableHandle.getColumns().get();
        }
        if (!synonymsEnabled) {
            return super.getColumns(session, tableHandle);
        }
        // MAJOR HACK ALERT!!!
        // We had to introduce the hack because of bug in Oracle JDBC client where
        // BaseJdbcClient#getColumns is not working when openProxySession is used and setIncludeSynonym(true) are used.
        // Below we are forcing to use oracle.jdbc.driver.OracleDatabaseMetaData.getColumnsWithWildcardsPlsql,
        // this method was used when setIncludeSynonym(false) is set, then openProxySession is also working as expected
        // Forcing is done by using wildcard '%' at the end of table name. And so we have to filter rows with columns from other tables.
        // Whenever you change this method make sure TestOracleIntegrationSmokeTest.testGetColumns covers your changes.
        checkArgument(tableHandle.isNamedRelation(), "Cannot get columns for %s", tableHandle);
        JdbcNamedRelationHandle namedRelation = tableHandle.getRequiredNamedRelation();
        try (Connection connection = connectionFactory.openConnection(session)) {
            try (ResultSet resultSet = getColumns(tableHandle, connection.getMetaData(), "%")) {
                List<JdbcColumnHandle> columns = new ArrayList<>();
                while (resultSet.next()) {
                    if (!resultSet.getString("TABLE_NAME").equals(namedRelation.getRemoteTableName().getTableName())) {
                        continue;
                    }
                    JdbcTypeHandle typeHandle = new JdbcTypeHandle(resultSet.getInt("DATA_TYPE"), Optional.ofNullable(resultSet.getString("TYPE_NAME")), Optional.of(resultSet.getInt("COLUMN_SIZE")), Optional.of(resultSet.getInt("DECIMAL_DIGITS")), Optional.empty(), Optional.empty());
                    Optional<ColumnMapping> columnMapping = toColumnMapping(session, connection, typeHandle);
                    // skip unsupported column types
                    if (columnMapping.isPresent()) {
                        String columnName = resultSet.getString("COLUMN_NAME");
                        columns.add(new JdbcColumnHandle(columnName, typeHandle, columnMapping.get().getType()));
                    }
                }
                if (columns.isEmpty()) {
                    // Table has no supported columns, but such table is not supported in Presto
                    throw new TableNotFoundException(namedRelation.getSchemaTableName());
                }
                return ImmutableList.copyOf(columns);
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    private ResultSet getColumns(JdbcTableHandle tableHandle, DatabaseMetaData metadata, String tableNameSuffix)
            throws SQLException
    {
        String escape = metadata.getSearchStringEscape();
        RemoteTableName remoteTableName = tableHandle.getRequiredNamedRelation().getRemoteTableName();
        return metadata.getColumns(
                remoteTableName.getCatalogName().orElse(null),
                escapeObjectNameForMetadataQuery(remoteTableName.getSchemaName(), escape).orElse(null),
                escapeObjectNameForMetadataQuery(Optional.ofNullable(remoteTableName.getTableName()), escape).orElse("") + tableNameSuffix,
                null);
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        verify(super.isLimitGuaranteed(session), "Super implementation changed");
        return StarburstOracleSessionProperties.getParallelismType(session) == OracleParallelismType.NO_PARALLELISM;
    }

    @Override
    public boolean supportsTopN(ConnectorSession session, JdbcTableHandle handle, List<JdbcSortItem> sortOrder)
    {
        return true;
    }

    @Override
    protected Optional<TopNFunction> topNFunction()
    {
        verify(super.topNFunction().isEmpty(), "Super implementation changed");
        // NOTE: The syntax used here is supported since Oracle 12c (older releases are not supported by Oracle)
        return Optional.of(TopNFunction.sqlStandard(this::quoted));
    }

    @Override
    public boolean isTopNGuaranteed(ConnectorSession session)
    {
        return StarburstOracleSessionProperties.getParallelismType(session) == OracleParallelismType.NO_PARALLELISM;
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

    private TableStatistics readTableStatistics(ConnectorSession session, JdbcTableHandle table)
            throws SQLException
    {
        checkArgument(table.isNamedRelation(), "Relation is not a table: %s", table);

        try (Connection connection = connectionFactory.openConnection(session);
                Handle handle = Jdbi.open(connection)) {
            StatisticsDao statisticsDao = new StatisticsDao(handle);

            RemoteTableName remoteTableName = table.getRequiredNamedRelation().getRemoteTableName();
            Long rowCount = statisticsDao.getRowCount(remoteTableName.getSchemaName().orElse(null), remoteTableName.getTableName());
            if (rowCount == null) {
                return TableStatistics.empty();
            }

            TableStatistics.Builder tableStatistics = TableStatistics.builder();
            tableStatistics.setRowCount(Estimate.of(rowCount));

            if (rowCount == 0) {
                return tableStatistics.build();
            }

            Map<String, ColumnStatisticsResult> columnStatistics = statisticsDao.getColumnStatistics(remoteTableName.getSchemaName().orElse(null), remoteTableName.getTableName())
                    .stream()
                    .collect(toImmutableMap(ColumnStatisticsResult::getColumnName, identity()));

            for (JdbcColumnHandle column : this.getColumns(session, table)) {
                ColumnStatisticsResult result = columnStatistics.get(column.getColumnName());
                if (result == null) {
                    continue;
                }

                ColumnStatistics statistics = ColumnStatistics.builder()
                        .setNullsFraction(result.getNullsCount()
                                .map(nullsCount -> Estimate.of(1.0 * nullsCount / rowCount))
                                .orElseGet(Estimate::unknown))
                        .setDistinctValuesCount(result.getDistinctValuesCount()
                                .map(Estimate::of)
                                .orElseGet(Estimate::unknown))
                        .setDataSize(result.getAverageColumnLength()
                                /*
                                 * ALL_TAB_COLUMNS.AVG_COL_LEN is hard to interpret precisely:
                                 * - it can be `0` for all-null column
                                 * - it can be `len+1` for varchar column filled with constant of length `len`, as if each row contained a is-null byte or length
                                 * - it can be `len/2+1` for varchar column half-filled with constant (or random) of length `len`, as if each row contained a is-null byte or length
                                 * - it can be `2` for varchar column with single non-null value of length 10, as if ... (?)
                                 * - it looks storage size does not directly depend on `IS NULL` column attribute
                                 *
                                 * Since the interpretation of the value is not obvious, we do not deduce is-null bytes. They will be accounted for second time in
                                 * `PlanNodeStatsEstimate.getOutputSizeForSymbol`, but this is the safer thing to do.
                                 */
                                .map(averageColumnLength -> Estimate.of(1.0 * averageColumnLength * rowCount))
                                .orElseGet(Estimate::unknown))
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

        Long getRowCount(String schema, String tableName)
        {
            return handle.createQuery("SELECT NUM_ROWS FROM ALL_TAB_STATISTICS WHERE OWNER = :schema AND TABLE_NAME = :table_name and PARTITION_NAME IS NULL")
                    .bind("schema", schema)
                    .bind("table_name", tableName)
                    .mapTo(Long.class)
                    .findFirst()
                    .orElse(null);
        }

        List<ColumnStatisticsResult> getColumnStatistics(String schema, String tableName)
        {
            // [SEP-3425]    we are not using ALL_TAB_COL_STATISTICS, here because we observed queries which took multiple minutes when obtaining statistics for partitioned tables.
            //               It adds slight risk, because the statistics-related columns in ALL_TAB_COLUMNS are marked as deprecated and present only for backward
            //               compatibility with Oracle 7 (see: https://docs.oracle.com/cd/B14117_01/server.101/b10755/statviews_1180.htm)
            return handle.createQuery("SELECT COLUMN_NAME, NUM_NULLS, NUM_DISTINCT, AVG_COL_LEN FROM ALL_TAB_COLUMNS WHERE OWNER = :schema AND TABLE_NAME = :table_name")
                    .bind("schema", schema)
                    .bind("table_name", tableName)
                    .map((rs, ctx) -> new ColumnStatisticsResult(
                            requireNonNull(rs.getString("COLUMN_NAME"), "COLUMN_NAME is null"),
                            Optional.ofNullable(rs.getObject("NUM_NULLS", Long.class)),
                            Optional.ofNullable(rs.getObject("NUM_DISTINCT", Long.class)),
                            Optional.ofNullable(rs.getObject("AVG_COL_LEN", Long.class))))
                    .list();
        }
    }

    private static class ColumnStatisticsResult
    {
        private final String columnName;
        private final Optional<Long> nullsCount;
        private final Optional<Long> distinctValuesCount;
        private final Optional<Long> averageColumnLength;

        ColumnStatisticsResult(String columnName, Optional<Long> nullsCount, Optional<Long> distinctValuesCount, Optional<Long> averageColumnLength)
        {
            this.columnName = columnName;
            this.nullsCount = nullsCount;
            this.distinctValuesCount = distinctValuesCount;
            this.averageColumnLength = averageColumnLength;
        }

        String getColumnName()
        {
            return columnName;
        }

        Optional<Long> getNullsCount()
        {
            return nullsCount;
        }

        Optional<Long> getDistinctValuesCount()
        {
            return distinctValuesCount;
        }

        Optional<Long> getAverageColumnLength()
        {
            return averageColumnLength;
        }
    }

    private static class OracleQueryBuilder
            extends DefaultQueryBuilder
    {
        private final Optional<List<String>> partitionNames;

        public OracleQueryBuilder(RemoteQueryModifier queryModifier, Optional<List<String>> partitionNames)
        {
            super(queryModifier);
            this.partitionNames = requireNonNull(partitionNames, "partitionNames is null");
        }

        @Override
        protected String getRelation(JdbcClient client, RemoteTableName remoteTableName)
        {
            String tableName = super.getRelation(client, remoteTableName);
            return partitionNames
                    .map(batch -> batch.stream()
                            .map(partitionName -> format("SELECT * FROM %s PARTITION (%s)", tableName, partitionName))
                            .collect(joining(" UNION ALL ", "(", ")"))) // wrap subquery in parentheses
                    .orElse(tableName);
        }
    }
}
