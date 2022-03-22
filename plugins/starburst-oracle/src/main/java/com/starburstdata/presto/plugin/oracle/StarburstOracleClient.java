/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.oracle;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.starburstdata.presto.license.LicenseManager;
import com.starburstdata.presto.plugin.jdbc.redirection.TableScanRedirection;
import com.starburstdata.presto.plugin.jdbc.stats.JdbcStatisticsConfig;
import io.trino.plugin.base.aggregation.AggregateFunctionRewriter;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DefaultQueryBuilder;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcJoinCondition;
import io.trino.plugin.jdbc.JdbcMetadataConfig;
import io.trino.plugin.jdbc.JdbcSortItem;
import io.trino.plugin.jdbc.JdbcSplit;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.aggregation.ImplementAvgDecimal;
import io.trino.plugin.jdbc.aggregation.ImplementAvgFloatingPoint;
import io.trino.plugin.jdbc.aggregation.ImplementCount;
import io.trino.plugin.jdbc.aggregation.ImplementCountAll;
import io.trino.plugin.jdbc.aggregation.ImplementCountDistinct;
import io.trino.plugin.jdbc.aggregation.ImplementCovariancePop;
import io.trino.plugin.jdbc.aggregation.ImplementCovarianceSamp;
import io.trino.plugin.jdbc.aggregation.ImplementMinMax;
import io.trino.plugin.jdbc.aggregation.ImplementStddevPop;
import io.trino.plugin.jdbc.aggregation.ImplementStddevSamp;
import io.trino.plugin.jdbc.aggregation.ImplementSum;
import io.trino.plugin.jdbc.aggregation.ImplementVariancePop;
import io.trino.plugin.jdbc.aggregation.ImplementVarianceSamp;
import io.trino.plugin.jdbc.expression.JdbcConnectorExpressionRewriterBuilder;
import io.trino.plugin.jdbc.mapping.IdentifierMapping;
import io.trino.plugin.oracle.OracleClient;
import io.trino.plugin.oracle.OracleConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.connector.TableScanRedirectApplicationResult;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.DecimalType;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleTypes;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.starburstdata.presto.license.StarburstFeature.ORACLE_EXTENSIONS;
import static com.starburstdata.presto.plugin.jdbc.JdbcJoinPushdownUtil.implementJoinCostAware;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_NON_TRANSIENT_ERROR;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;

public class StarburstOracleClient
        extends OracleClient
{
    private static final int DEFAULT_ROW_FETCH_SIZE = 1000;
    private static final int PRESTO_BIGINT_TYPE = 832_424_001;

    private final boolean synonymsEnabled;
    private final LicenseManager licenseManager;
    private final ConnectorExpressionRewriter<String> connectorExpressionRewriter;
    private final AggregateFunctionRewriter<JdbcExpression, String> aggregateFunctionRewriter;
    private final boolean statisticsEnabled;
    private final TableScanRedirection tableScanRedirection;

    @Inject
    public StarburstOracleClient(
            LicenseManager licenseManager,
            BaseJdbcConfig config,
            JdbcMetadataConfig jdbcMetadataConfig,
            JdbcStatisticsConfig statisticsConfig,
            TableScanRedirection tableScanRedirection,
            OracleConfig oracleConfig,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping)
    {
        super(config, oracleConfig, connectionFactory, queryBuilder, identifierMapping);
        synonymsEnabled = oracleConfig.isSynonymsEnabled();
        this.licenseManager = requireNonNull(licenseManager, "licenseManager is null");
        this.connectorExpressionRewriter = JdbcConnectorExpressionRewriterBuilder.newBuilder()
                .addStandardRules(this::quoted)
                .build();
        JdbcTypeHandle bigintTypeHandle = new JdbcTypeHandle(PRESTO_BIGINT_TYPE, Optional.of("NUMBER"), 0, 0, Optional.empty());
        this.aggregateFunctionRewriter = new AggregateFunctionRewriter<>(
                this.connectorExpressionRewriter,
                ImmutableSet.<AggregateFunctionRule<JdbcExpression, String>>builder()
                        .add(new ImplementCountAll(bigintTypeHandle))
                        .add(new ImplementCount(bigintTypeHandle))
                        .add(new ImplementCountDistinct(bigintTypeHandle, true))
                        .add(new ImplementMinMax(true))
                        .add(new ImplementSum(StarburstOracleClient::toTypeHandle))
                        .add(new ImplementAvgFloatingPoint())
                        .add(new ImplementAvgDecimal())
                        .add(new ImplementStddevSamp())
                        .add(new ImplementStddevPop())
                        .add(new ImplementVarianceSamp())
                        .add(new ImplementVariancePop())
                        .add(new ImplementCovarianceSamp())
                        .add(new ImplementCovariancePop())
                        .build());
        this.statisticsEnabled = requireNonNull(statisticsConfig, "statisticsConfig is null").isEnabled();
        this.tableScanRedirection = requireNonNull(tableScanRedirection, "tableScanRedirection is null");

        if (jdbcMetadataConfig.isAggregationPushdownEnabled()) {
            licenseManager.checkFeature(ORACLE_EXTENSIONS);
        }
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
    public PreparedQuery prepareQuery(ConnectorSession session, JdbcTableHandle table, Optional<List<List<JdbcColumnHandle>>> groupingSets, List<JdbcColumnHandle> columns, Map<String, String> columnExpressions)
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
        OracleQueryBuilder queryBuilder = new OracleQueryBuilder(((OracleSplit) split).getPartitionNames());
        PreparedQuery preparedQuery = queryBuilder.prepareSelectQuery(
                this,
                session,
                connection,
                table.getRelationHandle(),
                Optional.empty(),
                columns,
                ImmutableMap.of(),
                table.getConstraint(),
                split.getAdditionalPredicate());
        preparedQuery = applyQueryTransformations(table, preparedQuery);
        return queryBuilder.prepareStatement(this, session, connection, preparedQuery);
    }

    @Override
    protected String generateTemporaryTableName()
    {
        // Oracle before 12.2 doesn't allow identifiers over 30 characters
        String id = super.generateTemporaryTableName();
        return id.substring(0, min(30, id.length()));
    }

    @Override
    public String quoted(String name)
    {
        if (name.contains("\"")) {
            // ORA-03001: unimplemented feature
            throw new TrinoException(JDBC_NON_TRANSIENT_ERROR, "Oracle does not support escaping '\"' in identifiers");
        }
        return identifierQuote + name + identifierQuote;
    }

    @Override
    public Connection getConnection(ConnectorSession session, JdbcSplit split)
            throws SQLException
    {
        Connection connection = super.getConnection(session, split);
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
        try (Connection connection = connectionFactory.openConnection(session)) {
            try (ResultSet resultSet = getColumns(tableHandle, connection.getMetaData(), "%")) {
                List<JdbcColumnHandle> columns = new ArrayList<>();
                while (resultSet.next()) {
                    if (!resultSet.getString("TABLE_NAME").equals(tableHandle.getTableName())) {
                        continue;
                    }
                    JdbcTypeHandle typeHandle = new JdbcTypeHandle(
                            resultSet.getInt("DATA_TYPE"),
                            Optional.ofNullable(resultSet.getString("TYPE_NAME")),
                            resultSet.getInt("COLUMN_SIZE"),
                            resultSet.getInt("DECIMAL_DIGITS"),
                            Optional.empty());
                    Optional<ColumnMapping> columnMapping = toColumnMapping(session, connection, typeHandle);
                    // skip unsupported column types
                    if (columnMapping.isPresent()) {
                        String columnName = resultSet.getString("COLUMN_NAME");
                        columns.add(new JdbcColumnHandle(columnName, typeHandle, columnMapping.get().getType()));
                    }
                }
                if (columns.isEmpty()) {
                    // Table has no supported columns, but such table is not supported in Presto
                    throw new TableNotFoundException(tableHandle.getSchemaTableName());
                }
                return ImmutableList.copyOf(columns);
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    private static ResultSet getColumns(JdbcTableHandle tableHandle, DatabaseMetaData metadata, String tableNameSuffix)
            throws SQLException
    {
        String escape = metadata.getSearchStringEscape();
        return metadata.getColumns(
                tableHandle.getCatalogName(),
                escapeNamePattern(Optional.ofNullable(tableHandle.getSchemaName()), escape).orElse(null),
                escapeNamePattern(Optional.ofNullable(tableHandle.getTableName()), escape).orElse("") + tableNameSuffix,
                null);
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle type)
    {
        if (type.getJdbcType() == PRESTO_BIGINT_TYPE) {
            // Synthetic column
            return Optional.of(bigintColumnMapping());
        }

        Optional<ColumnMapping> mappingToVarchar = getForcedMappingToVarchar(type);
        if (mappingToVarchar.isPresent()) {
            return mappingToVarchar;
        }

        return super.toColumnMapping(session, connection, type);
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        licenseManager.checkFeature(ORACLE_EXTENSIONS);
        // TODO support complex ConnectorExpressions
        return aggregateFunctionRewriter.rewrite(session, aggregate, assignments);
    }

    private static Optional<JdbcTypeHandle> toTypeHandle(DecimalType decimalType)
    {
        return Optional.of(new JdbcTypeHandle(OracleTypes.NUMBER, Optional.of("NUMBER"), decimalType.getPrecision(), decimalType.getScale(), Optional.empty()));
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> format("SELECT * FROM (%s) WHERE ROWNUM <= %s", sql, limit));
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
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
        // NOTE: The syntax used here is supported since Oracle 12c (older releases are not supported by Oracle)
        return Optional.of(TopNFunction.sqlStandard(this::quoted));
    }

    @Override
    public boolean isTopNGuaranteed(ConnectorSession session)
    {
        return StarburstOracleSessionProperties.getParallelismType(session) == OracleParallelismType.NO_PARALLELISM;
    }

    @Override
    // TODO: migrate to OSS?
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping schemas");
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

    private TableStatistics readTableStatistics(ConnectorSession session, JdbcTableHandle table)
            throws SQLException
    {
        checkArgument(table.isNamedRelation(), "Relation is not a table: %s", table);

        try (Connection connection = connectionFactory.openConnection(session);
                Handle handle = Jdbi.open(connection)) {
            StatisticsDao statisticsDao = new StatisticsDao(handle);

            Long rowCount = statisticsDao.getRowCount(table.getSchemaName(), table.getTableName());
            if (rowCount == null) {
                return TableStatistics.empty();
            }

            TableStatistics.Builder tableStatistics = TableStatistics.builder();
            tableStatistics.setRowCount(Estimate.of(rowCount));

            if (rowCount == 0) {
                return tableStatistics.build();
            }

            Map<String, ColumnStatisticsResult> columnStatistics = statisticsDao.getColumnStatistics(table.getSchemaName(), table.getTableName()).stream()
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

        public OracleQueryBuilder(Optional<List<String>> partitionNames)
        {
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
