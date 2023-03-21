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
package io.trino.plugin.snowflake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.plugin.base.aggregation.AggregateFunctionRewriter;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcJoinCondition;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcSortItem;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.StandardColumnMappings;
import io.trino.plugin.jdbc.UnsupportedTypeHandling;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.aggregation.ImplementAvgDecimal;
import io.trino.plugin.jdbc.aggregation.ImplementAvgFloatingPoint;
import io.trino.plugin.jdbc.aggregation.ImplementCount;
import io.trino.plugin.jdbc.aggregation.ImplementCountAll;
import io.trino.plugin.jdbc.aggregation.ImplementMinMax;
import io.trino.plugin.jdbc.aggregation.ImplementStddevPop;
import io.trino.plugin.jdbc.aggregation.ImplementStddevSamp;
import io.trino.plugin.jdbc.aggregation.ImplementVariancePop;
import io.trino.plugin.jdbc.aggregation.ImplementVarianceSamp;
import io.trino.plugin.jdbc.expression.JdbcConnectorExpressionRewriterBuilder;
import io.trino.plugin.jdbc.expression.RewriteComparison;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.plugin.jdbc.mapping.IdentifierMapping;
import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.JoinCondition;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.JdbcJoinPushdownUtil.implementJoinCostAware;
import static io.trino.plugin.jdbc.PredicatePushdownController.FULL_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultVarcharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.IGNORE;
import static io.trino.spi.ErrorType.INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeWithTimeZoneParametricType.TIME_WITH_TIME_ZONE;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.sql.DatabaseMetaData.columnNoNulls;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;

public class SnowflakeClient
        extends BaseJdbcClient
{
    private static final Logger LOG = Logger.get(SnowflakeClient.class);
    private static final String DUPLICATE_TABLE_SQLSTATE = "42P07";

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private final SnowflakeConfig snowflakeConfig;
    private final boolean statisticsEnabled;
    private final ConnectorExpressionRewriter<String> connectorExpressionRewriter;
    private final AggregateFunctionRewriter<JdbcExpression, String> aggregateFunctionRewriter;

    @Inject
    public SnowflakeClient(
            BaseJdbcConfig config,
            SnowflakeConfig snowflakeConfig,
            JdbcStatisticsConfig statisticsConfig,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            TypeManager typeManager,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier)
    {
        super("\"", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, queryModifier, true);
        statisticsConfig.setEnabled(true);
        this.snowflakeConfig = snowflakeConfig;
        this.statisticsEnabled = statisticsConfig.isEnabled();

        this.connectorExpressionRewriter = JdbcConnectorExpressionRewriterBuilder.newBuilder()
                .addStandardRules(this::quoted)
                .add(new RewriteComparison(ImmutableSet.of(RewriteComparison.ComparisonOperator.EQUAL,
                        RewriteComparison.ComparisonOperator.NOT_EQUAL,
                        RewriteComparison.ComparisonOperator.GREATER_THAN,
                        RewriteComparison.ComparisonOperator.GREATER_THAN_OR_EQUAL,
                        RewriteComparison.ComparisonOperator.LESS_THAN,
                        RewriteComparison.ComparisonOperator.LESS_THAN_OR_EQUAL)))
                .build();
        JdbcTypeHandle bigintTypeHandle = new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        this.aggregateFunctionRewriter = new AggregateFunctionRewriter<>(
                this.connectorExpressionRewriter, ImmutableSet.<AggregateFunctionRule<JdbcExpression, String>>builder()
                .add(new ImplementCountAll(bigintTypeHandle))
                .add(new ImplementCount(bigintTypeHandle))
                .add(new ImplementMinMax(false))
                .add(new ImplementAvgFloatingPoint())
                .add(new ImplementAvgDecimal())
                .add(new ImplementStddevSamp())
                .add(new ImplementStddevPop())
                .add(new ImplementVarianceSamp())
                .add(new ImplementVariancePop())
                .build());
    }

    private static Optional<Long> readRowCountTableStat(StatisticsDao statisticsDao, JdbcTableHandle table)
    {
        RemoteTableName remoteTableName = table.getRequiredNamedRelation().getRemoteTableName();
        String schemaName = remoteTableName.getSchemaName().orElse(null);
        Optional<Long> rowCount = statisticsDao.getRowCount(schemaName, remoteTableName.getTableName());
        return rowCount;
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        int jdbcType = typeHandle.getJdbcType();
        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }
        //below case mapped as per https://docs.snowflake.com/en/sql-reference/data-types-numeric.html#int-integer-bigint-smallint-tinyint-byteint
        /**
         * Ideally all date and time for other connectors is parsed as long, however this fails on the snowflake connector hence converted as varchar.
         * Tried converting to Trino Slice type, but that is just a buffered wrap over varchar, hence varchar is a safer choice for now.
         */
        switch (jdbcType) {
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.TINYINT:
            case Types.BIGINT:
            case Types.NUMERIC:
                return Optional.of(StandardColumnMappings.bigintColumnMapping());
            case Types.REAL:
            case Types.FLOAT:
            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());
            case Types.DECIMAL:
                return Optional.of(decimalColumnMapping(createDecimalType(Decimals.MAX_PRECISION, typeHandle.getRequiredColumnSize())));
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                return Optional.of(defaultVarcharColumnMapping(typeHandle.getRequiredColumnSize(), true));
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return Optional.of(ColumnMapping.sliceMapping(VARBINARY, varbinaryReadFunction(), varbinaryWriteFunction(), FULL_PUSHDOWN));
            case Types.DATE:
                return Optional.of(ColumnMapping.longMapping(
                        DATE,
                        ResultSet::getLong,
                        (statement, index, value) -> statement.setString(index, DATE_FORMATTER.format(LocalDate.ofEpochDay(value))), FULL_PUSHDOWN));
            case Types.TIMESTAMP:
                return Optional.of(StandardColumnMappings.bigintColumnMapping());
            case Types.BOOLEAN:
                return Optional.of(StandardColumnMappings.booleanColumnMapping());
            default:
                throw new TrinoException(SnowflakeErrorCode.SNOWFLAKE_COLUMN_MAPPING_ERROR, "Snowflake type mapper cannot build type mapping for JDBC type " + typeHandle.getJdbcType());
        }
    }

    @Override
    protected Optional<TopNFunction> topNFunction()
    {
        return Optional.of((query, sortItems, limit) -> {
            String orderBy = sortItems.stream()
                    .map(sortItem -> {
                        String ordering = sortItem.getSortOrder().isAscending() ? "ASC" : "DESC";
                        String nullsHandling = sortItem.getSortOrder().isNullsFirst() ? "NULLS FIRST" : "NULLS LAST";
                        return format("%s %s %s", quoted(sortItem.getColumn().getColumnName()), ordering, nullsHandling);
                    })
                    .collect(joining(", "));

            return format("%s ORDER BY %s LIMIT %d", query, orderBy, limit);
        });
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type == BOOLEAN) {
            return WriteMapping.booleanMapping("BOOLEAN", booleanWriteFunction());
        }
        if (type == TINYINT || type == SMALLINT || type == INTEGER || type == BIGINT) {
            return WriteMapping.longMapping("NUMBER(38,0)", bigintWriteFunction());
        }
        if (type == REAL || type == DOUBLE) {
            return WriteMapping.doubleMapping("FLOAT", doubleWriteFunction());
        }

        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            String dataType = format("decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.objectMapping(dataType, StandardColumnMappings.longDecimalWriteFunction(decimalType));
        }

        if (type == DATE) {
            return WriteMapping.longMapping(
                    "Date",
                    (statement, index, value) -> statement.setString(index, DATE_FORMATTER.format(LocalDate.ofEpochDay(value))));
        }

        if (TIME_WITH_TIME_ZONE.equals(type)) {
            return WriteMapping.sliceMapping("TIMESTAMP_TZ", varcharWriteFunction());
        }
        if (TIMESTAMP_MILLIS.equals(type)) {
            // TODO use `timestampWriteFunction` (https://github.com/trinodb/trino/issues/6910)
            return WriteMapping.longMapping("NUMBER(38,0)", bigintWriteFunction());
        }
        if (VARBINARY.equals(type)) {
            return WriteMapping.sliceMapping("varbinary", varbinaryWriteFunction());
        }

        if (type instanceof CharType) {
            return WriteMapping.sliceMapping("char(" + ((CharType) type).getLength() + ")", charWriteFunction());
        }
        if (type instanceof TimestampType timestampType) {
            if (timestampType.getPrecision() == 0) {
                return WriteMapping.longMapping(
                        "NUMBER(38,0)",
                        PreparedStatement::setLong);
            }
            else {
                throw new RuntimeException("Cannot process this " + timestampType.getPrecision());
            }
        }

        if (type instanceof VarcharType) {
            VarcharType varcharType = (VarcharType) type;
            String dataType;
            if (varcharType.isUnbounded()) {
                dataType = "varchar";
            }
            else if (varcharType.getBoundedLength() <= 16777215) {
                dataType = String.format("varchar(%s)", varcharType.getBoundedLength());
            }
            else {
                dataType = "varchar";
            }
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }
        throw new TrinoException(SnowflakeErrorCode.SNOWFLAKE_COLUMN_MAPPING_ERROR, "Cannot create write time mapping for " + type);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        try {
            createTable(session, tableMetadata, tableMetadata.getTable().getTableName());
        }
        catch (SQLException e) {
            boolean exists = DUPLICATE_TABLE_SQLSTATE.equals(e.getSQLState());
            throw new TrinoException(exists ? ALREADY_EXISTS : JDBC_ERROR, e);
        }
    }

    @Override
    public void setTableProperties(ConnectorSession session, JdbcTableHandle handle, Map<String, Optional<Object>> properties)
    {
        super.setTableProperties(session, handle, properties);
    }

    @Override
    public JdbcOutputTableHandle beginInsertTable(ConnectorSession session, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columns)
    {
        return super.beginInsertTable(session, tableHandle, columns);
    }

    @Override
    public Optional<String> getTableComment(ResultSet resultSet)
    {
        // Don't return a comment until the connector supports creating tables with comment
        return Optional.empty();
    }

    @Override
    protected void renameTable(ConnectorSession session, Connection connection, String catalogName, String remoteSchemaName, String remoteTableName, String newRemoteSchemaName, String newRemoteTableName)
            throws SQLException
    {
        if (!remoteSchemaName.equals(newRemoteSchemaName)) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming tables across schemas");
        }

        execute(session, connection, format(
                "ALTER TABLE %s RENAME TO %s",
                quoted(catalogName, remoteSchemaName, remoteTableName),
                quoted(catalogName, newRemoteSchemaName, newRemoteTableName)));
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException
    {
        // fetch-size is ignored when connection is in auto-commit
        connection.setAutoCommit(false);
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setFetchSize(1000);
        return statement;
    }

    protected String createTableSql(RemoteTableName remoteTableName, List<String> columns, ConnectorTableMetadata tableMetadata)
    {
        return format("CREATE TABLE %s (%s)", quoted(remoteTableName), join(", ", columns));
    }

    @Override
    public boolean supportsRetries()
    {
        return false;
    }

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        if (tableHandle.getColumns().isPresent()) {
            return tableHandle.getColumns().get();
        }
        checkArgument(tableHandle.isNamedRelation(), "Cannot get columns for %s", tableHandle);
        SchemaTableName schemaTableName = tableHandle.getRequiredNamedRelation().getSchemaTableName();

        try (Connection connection = connectionFactory.openConnection(session)) {
            Map<String, Integer> arrayColumnDimensions = ImmutableMap.of();
            try (ResultSet resultSet = getColumns(tableHandle, connection.getMetaData())) {
                int allColumns = 0;
                List<JdbcColumnHandle> columns = new ArrayList<>();
                while (resultSet.next()) {
                    allColumns++;
                    String columnName = resultSet.getString("COLUMN_NAME");
                    JdbcTypeHandle typeHandle = new JdbcTypeHandle(
                            getInteger(resultSet, "DATA_TYPE").orElseThrow(() -> new IllegalStateException("DATA_TYPE is null")),
                            Optional.of(resultSet.getString("TYPE_NAME")),
                            getInteger(resultSet, "COLUMN_SIZE"),
                            getInteger(resultSet, "DECIMAL_DIGITS"),
                            Optional.ofNullable(arrayColumnDimensions.get(columnName)),
                            Optional.empty());
                    Optional<ColumnMapping> columnMapping = toColumnMapping(session, connection, typeHandle);
                    LOG.debug("Mapping data type of '%s' column '%s': %s mapped to %s", schemaTableName, columnName, typeHandle, columnMapping);
                    // skip unsupported column types
                    if (columnMapping.isPresent()) {
                        boolean nullable = (resultSet.getInt("NULLABLE") != columnNoNulls);
                        Optional<String> comment = Optional.ofNullable(resultSet.getString("REMARKS"));
                        columns.add(JdbcColumnHandle.builder()
                                .setColumnName(columnName)
                                .setJdbcTypeHandle(typeHandle)
                                .setColumnType(columnMapping.get().getType())
                                .setNullable(nullable)
                                .setComment(comment)
                                .build());
                    }
                    if (!columnMapping.isPresent()) {
                        UnsupportedTypeHandling unsupportedTypeHandling = getUnsupportedTypeHandling(session);
                        verify(
                                unsupportedTypeHandling == IGNORE,
                                "Unsupported type handling is set to %s, but toColumnMapping() returned empty for %s",
                                unsupportedTypeHandling,
                                typeHandle);
                    }
                }
                if (columns.isEmpty()) {
                    // A table may have no supported columns. In rare cases a table might have no columns at all.
                    throw new TableNotFoundException(
                            schemaTableName,
                            format("Table '%s' has no supported columns (all %s columns are not supported)", schemaTableName, allColumns));
                }
                return ImmutableList.copyOf(columns);
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public Collection<String> listSchemas(Connection connection)
    {
        try {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet schemas = metaData.getSchemas(snowflakeConfig.getCatalog(), null);
            Set<String> outputSchemas = new HashSet<>();
            while (schemas.next()) {
                String schema = schemas.getString("TABLE_SCHEM");
                outputSchemas.add(schema);
            }
            return outputSchemas;
        }
        catch (SQLException e) {
            throw new TrinoException(SnowflakeErrorCode.SNOWFLAKE_COLUMN_MAPPING_ERROR,
                    e);
        }
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        return aggregateFunctionRewriter.rewrite(session, aggregate, assignments);
    }

    @Override
    public Optional<String> convertPredicate(ConnectorSession session, ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        // return connectorExpressionRewriter.rewrite(session, expression, assignments);
        // return super.convertPredicate(session, expression, assignments);
        return connectorExpressionRewriter.rewrite(session, expression, assignments);
    }

    @Override
    public boolean supportsTopN(ConnectorSession session, JdbcTableHandle handle, List<JdbcSortItem> sortOrder)
    {
        return true;
    }

    @Override
    public boolean isTopNGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> sql + " LIMIT " + limit);
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    public OptionalLong delete(ConnectorSession session, JdbcTableHandle handle)
    {
        checkArgument(handle.isNamedRelation(), "Unable to delete from synthetic table: %s", handle);
        checkArgument(!handle.getLimit().isPresent(), "Unable to delete when limit is set: %s", handle);
        checkArgument(!handle.getSortOrder().isPresent(), "Unable to delete when sort order is set: %s", handle);
        PreparedQuery preparedQuery = null;
        try (Connection connection = connectionFactory.openConnection(session)) {
            verify(connection.getAutoCommit());
            preparedQuery = queryBuilder.prepareDeleteQuery(
                    this,
                    session,
                    connection,
                    handle.getRequiredNamedRelation(),
                    handle.getConstraint(),
                    getAdditionalPredicate(handle.getConstraintExpressions(), Optional.empty()));
            try (PreparedStatement preparedStatement = queryBuilder.prepareStatement(this, session, connection, preparedQuery)) {
                int affectedRowsCount = preparedStatement.executeUpdate();
                // In getPreparedStatement we set autocommit to false so here we need an explicit commit
                connection.commit();
                return OptionalLong.of(affectedRowsCount);
            }
        }
        catch (SQLException e) {
            if (preparedQuery != null) {
                LOG.error("Cannot execute delete query: {}", preparedQuery.getQuery(), e);
            }
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle)
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
            LOG.error("ERROR when fetching table stats ", e);
            return TableStatistics.empty();
        }
    }

    private TableStatistics readTableStatistics(ConnectorSession session, JdbcTableHandle table)
            throws SQLException
    {
        checkArgument(table.isNamedRelation(), "Relation is not a table: %s", table);

        try (Connection connection = connectionFactory.openConnection(session)) {
            StatisticsDao statisticsDao = new StatisticsDao(connection, snowflakeConfig);
            Optional<Long> optionalRowCount = readRowCountTableStat(statisticsDao, table);
            if (optionalRowCount.isEmpty()) {
                return TableStatistics.empty();
            }
            long rowCount = optionalRowCount.get();
            TableStatistics.Builder tableStatistics = TableStatistics.builder();
            tableStatistics.setRowCount(Estimate.of(rowCount));

            if (rowCount == 0) {
                return tableStatistics.build();
            }

            RemoteTableName remoteTableName = table.getRequiredNamedRelation().getRemoteTableName();
            Map<String, ColumnStatisticsResult> columnStatistics = statisticsDao.getColumnStatistics(remoteTableName.getSchemaName().orElse(null), remoteTableName.getTableName()).stream()
                    .collect(toImmutableMap(ColumnStatisticsResult::getColumnName, identity()));

            for (JdbcColumnHandle column : this.getColumns(session, table)) {
                ColumnStatisticsResult result = columnStatistics.get(column.getColumnName());
                if (result == null) {
                    continue;
                }
                ColumnStatistics statistics = ColumnStatistics.builder()
                        .setNullsFraction(result.getNullsFraction()
                                .map(Estimate::of)
                                .orElseGet(Estimate::unknown))
                        .setDistinctValuesCount(result.getDistinctValuesIndicator()
                                .map(distinctValuesIndicator -> {
                                    if (distinctValuesIndicator >= 0.0) {
                                        return distinctValuesIndicator;
                                    }
                                    return -distinctValuesIndicator * rowCount;
                                })
                                .map(Estimate::of)
                                .orElseGet(Estimate::unknown))
                        .setDataSize(result.getAverageColumnLength()
                                .flatMap(averageColumnLength ->
                                        result.getNullsFraction().map(nullsFraction ->
                                                Estimate.of(1.0 * averageColumnLength * rowCount * (1 - nullsFraction))))
                                .orElseGet(Estimate::unknown))
                        .build();

                tableStatistics.setColumnStatistics(column, statistics);
            }

            TableStatistics statistics = tableStatistics.build();
            return statistics;
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
        if (joinType == JoinType.FULL_OUTER) {
            // FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
            return Optional.empty();
        }
        return implementJoinCostAware(
                session,
                joinType,
                leftSource,
                rightSource,
                statistics,
                () -> super.implementJoin(session, joinType, leftSource, rightSource, joinConditions, rightAssignments, leftAssignments, statistics));
    }

    @Override
    protected boolean isSupportedJoinCondition(ConnectorSession session, JdbcJoinCondition joinCondition)
    {
        return joinCondition.getOperator() != JoinCondition.Operator.IS_DISTINCT_FROM;
    }

    @Override
    protected void verifySchemaName(DatabaseMetaData databaseMetadata, String schemaName)
            throws SQLException
    {
        // PostgreSQL truncates schema name to 63 chars silently
        if (schemaName.length() > databaseMetadata.getMaxSchemaNameLength()) {
            throw new TrinoException(NOT_SUPPORTED, format("Schema name must be shorter than or equal to '%s' characters but got '%s'", databaseMetadata.getMaxSchemaNameLength(), schemaName.length()));
        }
    }

    @Override
    protected void verifyTableName(DatabaseMetaData databaseMetadata, String tableName)
            throws SQLException
    {
        // PostgreSQL truncates table name to 63 chars silently
        if (tableName.length() > databaseMetadata.getMaxTableNameLength()) {
            throw new TrinoException(NOT_SUPPORTED, format("Table name must be shorter than or equal to '%s' characters but got '%s'", databaseMetadata.getMaxTableNameLength(), tableName.length()));
        }
    }

    @Override
    protected void verifyColumnName(DatabaseMetaData databaseMetadata, String columnName)
            throws SQLException
    {
        if (columnName.length() > databaseMetadata.getMaxColumnNameLength()) {
            throw new TrinoException(NOT_SUPPORTED, format("Column name must be shorter than or equal to '%s' characters but got '%s': '%s'", databaseMetadata.getMaxColumnNameLength(), columnName.length(), columnName));
        }
    }

    @Override
    public void setColumnComment(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Optional<String> comment)
    {
        // PostgreSQL doesn't support prepared statement for COMMENT statement
        String sql = format(
                "COMMENT ON COLUMN %s.%s IS %s",
                quoted(handle.asPlainTable().getRemoteTableName()),
                quoted(column.getColumnName()),
                comment.map(BaseJdbcClient::varcharLiteral).orElse("NULL"));
        execute(session, sql);
    }

    public enum SnowflakeErrorCode
            implements ErrorCodeSupplier
    {
        SNOWFLAKE_COLUMN_MAPPING_ERROR(1, INTERNAL_ERROR);
        private final ErrorCode errorCode;

        SnowflakeErrorCode(int code, ErrorType type)
        {
            errorCode = new ErrorCode(code + 0x0506_0000, name(), type);
        }

        @Override
        public ErrorCode toErrorCode()
        {
            return errorCode;
        }
    }

    private static class StatisticsDao
    {
        private final Connection connection;
        private final SnowflakeConfig config;

        public StatisticsDao(Connection connection, SnowflakeConfig config)
        {
            this.connection = requireNonNull(connection, "handle is null");
            this.config = requireNonNull(config, "handle is null");
        }

        Optional<Long> getRowCount(String schema, String tableName)
        {
            Optional<Long> recordCount = Optional.empty();
            try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT *\n" +
                    "FROM \"INFORMATION_SCHEMA\".\"TABLES\"\n" +
                    "WHERE TABLE_NAME = ? \n" +
                    "and TABLE_SCHEMA = ? and TABLE_CATALOG = ?\n" +
                    "and TABLE_TYPE = 'BASE TABLE'")) {
                preparedStatement.setString(1, tableName);
                preparedStatement.setString(2, schema);
                preparedStatement.setString(3, config.getCatalog());
                ResultSet resultSet = preparedStatement.executeQuery();
                int tables = 0;
                while (resultSet.next()) {
                    tables++;
                    recordCount = Optional.of(resultSet.getLong("ROW_COUNT"));
                }
                if (tables > 1) {
                    LOG.error(new RuntimeException("Table stats fetching more than one table " + tables));
                }
                return recordCount;
            }
            catch (Exception e) {
                LOG.error("Cannot fetch row count for Table: {} Schema: {}", tableName, schema);
                return recordCount;
            }
        }

        List<ColumnStatisticsResult> getColumnStatistics(String schema, String tableName)
                throws SQLException
        {
            List<ColumnStatisticsResult> results = new ArrayList<>();

            try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT *\n" +
                    "FROM \"INFORMATION_SCHEMA\".\"COLUMNS\"\n" +
                    "WHERE TABLE_NAME = ? \n" +
                    "and TABLE_SCHEMA = ? and TABLE_CATALOG = ?\n")) {
                preparedStatement.setString(1, tableName);
                preparedStatement.setString(2, schema);
                preparedStatement.setString(3, config.getCatalog());
                ResultSet resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    String columnName = resultSet.getString("COLUMN_NAME");
                    String isNullable = resultSet.getString("IS_NULLABLE");
                    String dataType = resultSet.getString("DATA_TYPE");
                    int columnLength = 0;
                    if (dataType.equalsIgnoreCase("TEXT")) {
                        columnLength = resultSet.getInt("CHARACTER_MAXIMUM_LENGTH");
                    }
                    else {
                        columnLength = resultSet.getInt("NUMERIC_PRECISION");
                    }
                    Optional<Float> distinctValue = Optional.empty();
                    Optional<Float> nullFaction = Optional.empty();
                    if (!isNullable.equalsIgnoreCase("YES")) {
                        nullFaction = Optional.of(0.0F);
                    }
                    Optional<Integer> maxLength = Optional.of(columnLength);
                    ColumnStatisticsResult columnStatisticsResult = new ColumnStatisticsResult(columnName, nullFaction, distinctValue, maxLength);
                    results.add(columnStatisticsResult);
                }
                return results;
            }
            catch (Exception e) {
                LOG.error("STATS GATHER FAILURE ", e);
                e.printStackTrace();
                return results;
            }
        }
    }

    private static class ColumnStatisticsResult
    {
        private final String columnName;
        private final Optional<Float> nullsFraction;
        private final Optional<Float> distinctValuesIndicator;
        private final Optional<Integer> averageColumnLength;

        public ColumnStatisticsResult(String columnName, Optional<Float> nullsFraction, Optional<Float> distinctValuesIndicator, Optional<Integer> averageColumnLength)
        {
            this.columnName = columnName;
            this.nullsFraction = nullsFraction;
            this.distinctValuesIndicator = distinctValuesIndicator;
            this.averageColumnLength = averageColumnLength;
        }

        public String getColumnName()
        {
            return columnName;
        }

        public Optional<Float> getNullsFraction()
        {
            return nullsFraction;
        }

        public Optional<Float> getDistinctValuesIndicator()
        {
            return distinctValuesIndicator;
        }

        public Optional<Integer> getAverageColumnLength()
        {
            return averageColumnLength;
        }

        @Override
        public String toString()
        {
            return "ColumnStatisticsResult{" +
                    "columnName='" + columnName + '\'' +
                    ", nullsFraction=" + nullsFraction +
                    ", distinctValuesIndicator=" + distinctValuesIndicator +
                    ", averageColumnLength=" + averageColumnLength +
                    '}';
        }
    }
}
