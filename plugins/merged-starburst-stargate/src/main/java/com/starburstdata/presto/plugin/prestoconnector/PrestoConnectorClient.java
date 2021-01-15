/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.prestoconnector;

import com.google.common.base.VerifyException;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableSet;
import com.starburstdata.presto.plugin.jdbc.redirection.TableScanRedirection;
import com.starburstdata.presto.plugin.jdbc.stats.JdbcStatisticsConfig;
import com.starburstdata.presto.plugin.jdbc.stats.TableStatisticsClient;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcExpression;
import io.prestosql.plugin.jdbc.JdbcOutputTableHandle;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.plugin.jdbc.QueryBuilder;
import io.prestosql.plugin.jdbc.SliceWriteFunction;
import io.prestosql.plugin.jdbc.WriteMapping;
import io.prestosql.plugin.jdbc.expression.AggregateFunctionRewriter;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.AggregateFunction;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.TableScanRedirectApplicationResult;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.statistics.ColumnStatistics;
import io.prestosql.spi.statistics.DoubleRange;
import io.prestosql.spi.statistics.Estimate;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.TimeType;
import io.prestosql.spi.type.TimeWithTimeZoneType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TimestampWithTimeZoneType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.VarcharType;

import javax.inject.Inject;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoColumnMappings.prestoDateColumnMapping;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoColumnMappings.prestoTimeColumnMapping;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoColumnMappings.prestoTimeWithTimeZoneColumnMapping;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoColumnMappings.prestoTimeWithTimeZoneWriteMapping;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoColumnMappings.prestoTimeWriteMapping;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoColumnMappings.prestoTimestampColumnMapping;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoColumnMappings.prestoTimestampWithTimeZoneColumnMapping;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoColumnMappings.prestoTimestampWithTimeZoneWriteMapping;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoColumnMappings.prestoTimestampWriteMapping;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.plugin.base.util.JsonTypeUtil.jsonParse;
import static io.prestosql.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.booleanColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.dateWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.defaultCharColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.defaultVarcharColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.realColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varbinaryColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.prestosql.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.prestosql.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.StandardTypes.JSON;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.function.Function.identity;

public class PrestoConnectorClient
        extends BaseJdbcClient
{
    private static final Logger log = Logger.get(PrestoConnectorClient.class);

    private final Type jsonType;

    private enum FunctionsCacheKey
    {
        SINGLETON
    }

    private final boolean enableWrites;
    private final Cache<FunctionsCacheKey, Set<String>> supportedAggregateFunctions;
    private final AggregateFunctionRewriter aggregateFunctionRewriter;
    private final TableStatisticsClient tableStatisticsClient;
    private final TableScanRedirection tableScanRedirection;

    @Inject
    public PrestoConnectorClient(
            BaseJdbcConfig config,
            JdbcStatisticsConfig statisticsConfig,
            TableScanRedirection tableScanRedirection,
            ConnectionFactory connectionFactory,
            TypeManager typeManager,
            @EnableWrites boolean enableWrites)
    {
        super(config, "\"", connectionFactory);
        this.enableWrites = enableWrites;
        this.jsonType = requireNonNull(typeManager, "typeManager is null").getType(new TypeSignature(JSON));

        this.supportedAggregateFunctions = CacheBuilder.newBuilder()
                .expireAfterWrite(30, MINUTES)
                .build();
        this.aggregateFunctionRewriter = new AggregateFunctionRewriter(this::quoted, Set.of(
                new PrestoAggregateFunctionRewriteRule(
                        this::getSupportedAggregateFunctions,
                        this::toTypeHandle)));
        this.tableStatisticsClient = new TableStatisticsClient(this::readTableStatistics, statisticsConfig);
        this.tableScanRedirection = requireNonNull(tableScanRedirection, "tableScanRedirection is null");
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column)
    {
        String sql = format(
                "ALTER TABLE %s ADD COLUMN %s",
                quoted(handle.getRemoteTableName()),
                getColumnDefinitionSql(session, column, column.getName()));
        execute(session, sql);
    }

    @Override
    public void setColumnComment(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Optional<String> comment)
    {
        String sql = format(
                "COMMENT ON COLUMN %s.%s IS %s",
                quoted(handle.getRemoteTableName()),
                quoted(column.getColumnName()),
                comment.isPresent() ? format("'%s'", comment.get()) : "NULL");
        execute(session, sql);
    }

    @Override
    public JdbcOutputTableHandle beginInsertTable(ConnectorSession session, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columns)
    {
        if (!enableWrites) {
            throw new PrestoException(NOT_SUPPORTED, "This connector does not support inserts");
        }
        return super.beginInsertTable(session, tableHandle, columns);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        if (!enableWrites) {
            throw new PrestoException(NOT_SUPPORTED, "This connector does not support creating tables");
        }
        super.createTable(session, tableMetadata);
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        if (!enableWrites) {
            throw new PrestoException(NOT_SUPPORTED, "This connector does not support creating tables with data");
        }
        return super.beginCreateTable(session, tableMetadata);
    }

    @Override
    public void dropTable(ConnectorSession session, JdbcTableHandle handle)
    {
        if (!enableWrites) {
            throw new PrestoException(NOT_SUPPORTED, "This connector does not support dropping tables");
        }
        super.dropTable(session, handle);
    }

    @Override
    public void renameTable(ConnectorSession session, JdbcTableHandle handle, SchemaTableName newTableName)
    {
        if (!enableWrites) {
            throw new PrestoException(NOT_SUPPORTED, "This connector does not support renaming tables");
        }
        super.renameTable(session, handle, newTableName);
    }

    @Override
    public void renameColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        if (!enableWrites) {
            throw new PrestoException(NOT_SUPPORTED, "This connector does not support renaming columns");
        }
        super.renameColumn(session, handle, jdbcColumn, newColumnName);
    }

    @Override
    public void dropColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        if (!enableWrites) {
            throw new PrestoException(NOT_SUPPORTED, "This connector does not support dropping columns");
        }
        super.dropColumn(session, handle, column);
    }

    @Override
    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        Optional<ColumnMapping> columnMapping = convertToPrestoType(session, typeHandle);
        columnMapping.ifPresent(mapping -> {
            // Ensure toTypeHandle stays up to date when we add new type mappings
            Type type = mapping.getType();
            JdbcTypeHandle syntheticTypeHandle = toTypeHandle(type)
                    .orElseThrow(() -> new VerifyException(format("Cannot convert type %s [%s] back to JdbcTypeHandle", type, typeHandle)));
            ColumnMapping mappingForSyntheticHandle = convertToPrestoType(session, syntheticTypeHandle)
                    .orElseThrow(() -> new VerifyException(format("JdbcTypeHandle %s constructed for %s [%s] cannot be converted to type", syntheticTypeHandle, type, typeHandle)));
            verify(
                    mappingForSyntheticHandle.getType().equals(type),
                    "Type mismatch, original type is %s [%s], converted type is %s [%s]",
                    type,
                    typeHandle,
                    mappingForSyntheticHandle.getType(),
                    syntheticTypeHandle);
        });
        return columnMapping;
    }

    private Optional<ColumnMapping> convertToPrestoType(ConnectorSession session, JdbcTypeHandle typeHandle)
    {
        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }

        String jdbcTypeName = typeHandle.getJdbcTypeName()
                // type name may be missing for synthetic type handles
                .orElse("");

        switch (jdbcTypeName) {
            case JSON:
                return Optional.of(jsonColumnMapping());
        }

        switch (typeHandle.getJdbcType()) {
            case Types.BOOLEAN:
                return Optional.of(booleanColumnMapping());

            case Types.TINYINT:
                return Optional.of(tinyintColumnMapping());

            case Types.SMALLINT:
                return Optional.of(smallintColumnMapping());

            case Types.INTEGER:
                return Optional.of(integerColumnMapping());

            case Types.BIGINT:
                return Optional.of(bigintColumnMapping());

            case Types.REAL:
                return Optional.of(realColumnMapping());

            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());

            case Types.DECIMAL:
                return Optional.of(decimalColumnMapping(createDecimalType(typeHandle.getRequiredColumnSize(), typeHandle.getRequiredDecimalDigits())));

            case Types.CHAR:
                return Optional.of(defaultCharColumnMapping(typeHandle.getRequiredColumnSize()));

            case Types.VARCHAR:
                // Presto JDBC reports column size of VarcharType.UNBOUNDED_LENGTH for an unbounded varchar, and so it will be mapped to unbounded varchar here too
                return Optional.of(defaultVarcharColumnMapping(typeHandle.getRequiredColumnSize()));

            case Types.VARBINARY:
                return Optional.of(varbinaryColumnMapping());

            case Types.DATE:
                return Optional.of(prestoDateColumnMapping());

            case Types.TIME:
                return Optional.of(prestoTimeColumnMapping(typeHandle.getRequiredDecimalDigits()));

            case Types.TIME_WITH_TIMEZONE:
                return Optional.of(prestoTimeWithTimeZoneColumnMapping(typeHandle.getRequiredDecimalDigits()));

            case Types.TIMESTAMP:
                return Optional.of(prestoTimestampColumnMapping(typeHandle.getRequiredDecimalDigits()));

            case Types.TIMESTAMP_WITH_TIMEZONE:
                return Optional.of(prestoTimestampWithTimeZoneColumnMapping(typeHandle.getRequiredDecimalDigits()));
        }

        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }

        log.debug("Unsupported type: %s", typeHandle);
        return Optional.empty();
    }

    private ColumnMapping jsonColumnMapping()
    {
        return ColumnMapping.sliceMapping(
                jsonType,
                (resultSet, columnIndex) -> jsonParse(utf8Slice(resultSet.getString(columnIndex))),
                new JsonWriteFunction(),
                // JSON is not orderable and EquatableValueSet currently breaks QueryBuilder
                DISABLE_PUSHDOWN);
    }

    private Optional<JdbcTypeHandle> toTypeHandle(Type type)
    {
        requireNonNull(type, "type is null");

        if (type == BOOLEAN) {
            return Optional.of(jdbcTypeHandle(Types.BOOLEAN));
        }

        if (type == TINYINT) {
            return Optional.of(jdbcTypeHandle(Types.TINYINT));
        }

        if (type == SMALLINT) {
            return Optional.of(jdbcTypeHandle(Types.SMALLINT));
        }

        if (type == INTEGER) {
            return Optional.of(jdbcTypeHandle(Types.INTEGER));
        }

        if (type == BIGINT) {
            return Optional.of(jdbcTypeHandle(Types.BIGINT));
        }

        if (type == REAL) {
            return Optional.of(jdbcTypeHandle(Types.REAL));
        }

        if (type == DOUBLE) {
            return Optional.of(jdbcTypeHandle(Types.DOUBLE));
        }

        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            return Optional.of(new JdbcTypeHandle(Types.DECIMAL, Optional.empty(), Optional.of(decimalType.getPrecision()), Optional.of(decimalType.getScale()), Optional.empty(), Optional.empty()));
        }

        if (type instanceof CharType) {
            return Optional.of(jdbcTypeHandleWithColumnSize(Types.CHAR, ((CharType) type).getLength()));
        }

        if (type instanceof VarcharType) {
            // See io.prestosql.connector.system.jdbc.ColumnJdbcTable#columnSize
            int columnSize = ((VarcharType) type).getLength().orElse(VarcharType.UNBOUNDED_LENGTH);
            return Optional.of(jdbcTypeHandleWithColumnSize(Types.VARCHAR, columnSize));
        }

        if (type == VARBINARY) {
            return Optional.of(jdbcTypeHandle(Types.VARBINARY));
        }

        if (type.equals(jsonType)) {
            return Optional.of(jdbcTypeHandleWithTypeName(Types.JAVA_OBJECT, JSON));
        }

        if (type == DATE) {
            return Optional.of(jdbcTypeHandle(Types.DATE));
        }

        if (type instanceof TimeType) {
            return Optional.of(jdbcTypeHandleWithDecimalDigits(Types.TIME, ((TimeType) type).getPrecision()));
        }

        if (type instanceof TimeWithTimeZoneType) {
            return Optional.of(jdbcTypeHandleWithDecimalDigits(Types.TIME_WITH_TIMEZONE, ((TimeWithTimeZoneType) type).getPrecision()));
        }

        if (type instanceof TimestampType) {
            return Optional.of(jdbcTypeHandleWithDecimalDigits(Types.TIMESTAMP, ((TimestampType) type).getPrecision()));
        }

        if (type instanceof TimestampWithTimeZoneType) {
            return Optional.of(jdbcTypeHandleWithDecimalDigits(Types.TIMESTAMP_WITH_TIMEZONE, ((TimestampWithTimeZoneType) type).getPrecision()));
        }

        log.debug("Type cannot be converted to JdbcTypeHandle: %s", type);
        return Optional.empty();
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type == BOOLEAN) {
            return WriteMapping.booleanMapping("boolean", booleanWriteFunction());
        }

        if (type == TINYINT) {
            return WriteMapping.longMapping("tinyint", tinyintWriteFunction());
        }
        if (type == SMALLINT) {
            return WriteMapping.longMapping("smallint", smallintWriteFunction());
        }
        if (type == INTEGER) {
            return WriteMapping.longMapping("integer", integerWriteFunction());
        }
        if (type == BIGINT) {
            return WriteMapping.longMapping("bigint", bigintWriteFunction());
        }

        if (type == REAL) {
            return WriteMapping.longMapping("real", realWriteFunction());
        }
        if (type == DOUBLE) {
            return WriteMapping.doubleMapping("double", doubleWriteFunction());
        }

        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            String dataType = format("decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.sliceMapping(dataType, longDecimalWriteFunction(decimalType));
        }

        if (type.equals(jsonType)) {
            return WriteMapping.sliceMapping(JSON, new JsonWriteFunction());
        }

        if (type instanceof CharType) {
            CharType charType = (CharType) type;
            String dataType = format("char(%s)", charType.getLength());
            return WriteMapping.sliceMapping(dataType, charWriteFunction());
        }

        if (type instanceof VarcharType) {
            VarcharType varcharType = (VarcharType) type;
            String dataType = varcharType.isUnbounded()
                    ? "varchar"
                    : format("varchar(%s)", varcharType.getBoundedLength());
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }

        if (type == VARBINARY) {
            return WriteMapping.sliceMapping("varbinary", varbinaryWriteFunction());
        }

        if (type == DATE) {
            return WriteMapping.longMapping("date", dateWriteFunction());
        }

        if (type instanceof TimeType) {
            return prestoTimeWriteMapping((TimeType) type);
        }

        if (type instanceof TimeWithTimeZoneType) {
            return prestoTimeWithTimeZoneWriteMapping((TimeWithTimeZoneType) type);
        }

        if (type instanceof TimestampType) {
            return prestoTimestampWriteMapping((TimestampType) type);
        }

        if (type instanceof TimestampWithTimeZoneType) {
            return prestoTimestampWithTimeZoneWriteMapping((TimestampWithTimeZoneType) type);
        }

        throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        // TODO support complex ConnectorExpressions
        return aggregateFunctionRewriter.rewrite(session, aggregate, assignments);
    }

    private Set<String> getSupportedAggregateFunctions(ConnectorSession session)
    {
        try {
            return supportedAggregateFunctions.get(FunctionsCacheKey.SINGLETON, () -> {
                try {
                    return listAggregateFunctions(session);
                }
                // Catch exceptions from the driver only. Any other exception is likely bug in the code.
                catch (SQLException e) {
                    log.warn(e, "Failed to list aggregate functions");
                    // If we reached aggregation pushdown, the remote cluster is likely up & running and so it may not
                    // be safe to retry the listing immediately. Cache the failure.
                    return Set.of();
                }
            });
        }
        catch (ExecutionException e) {
            // Impossible, as the loader does not throw checked exceptions
            throw new RuntimeException(e);
        }
    }

    private Set<String> listAggregateFunctions(ConnectorSession session)
            throws SQLException
    {
        try (Connection connection = connectionFactory.openConnection(session);
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SHOW FUNCTIONS")) {
            ImmutableSet.Builder<String> functions = ImmutableSet.builder();
            while (resultSet.next()) {
                if ("aggregate".equals(resultSet.getString("Function Type"))) {
                    functions.add(resultSet.getString("Function"));
                }
            }
            return functions.build();
        }
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

    private static JdbcTypeHandle jdbcTypeHandle(int jdbcType)
    {
        return new JdbcTypeHandle(jdbcType, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    private static JdbcTypeHandle jdbcTypeHandleWithTypeName(int jdbcType, String typeName)
    {
        return new JdbcTypeHandle(jdbcType, Optional.ofNullable(typeName), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    private static JdbcTypeHandle jdbcTypeHandleWithColumnSize(int jdbcType, int columnSize)
    {
        return new JdbcTypeHandle(jdbcType, Optional.empty(), Optional.of(columnSize), Optional.empty(), Optional.empty(), Optional.empty());
    }

    private static JdbcTypeHandle jdbcTypeHandleWithDecimalDigits(int jdbcType, int decimalDigits)
    {
        return new JdbcTypeHandle(jdbcType, Optional.empty(), Optional.empty(), Optional.of(decimalDigits), Optional.empty(), Optional.empty());
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle, TupleDomain<ColumnHandle> tupleDomain)
    {
        return tableStatisticsClient.getTableStatistics(session, handle, tupleDomain);
    }

    @Override
    public Optional<TableScanRedirectApplicationResult> getTableScanRedirection(ConnectorSession session, JdbcTableHandle handle)
    {
        return tableScanRedirection.getTableScanRedirection(session, handle, this);
    }

    private Optional<TableStatistics> readTableStatistics(ConnectorSession session, JdbcTableHandle table)
            throws SQLException
    {
        if (table.getGroupingSets().isPresent()) {
            // TODO(https://starburstdata.atlassian.net/browse/PRESTO-4856) retrieve statistics for base table and derive statistics for the aggregation
            return Optional.empty();
        }

        List<JdbcColumnHandle> jdbcColumnHandles = getColumns(session, table);
        Map<String, JdbcColumnHandle> columnHandles = jdbcColumnHandles.stream()
                .collect(toImmutableMap(JdbcColumnHandle::getColumnName, identity()));

        try (Connection connection = connectionFactory.openConnection(session);
                PreparedStatement statement = getShowStatsStatement(session, connection, table, jdbcColumnHandles);
                ResultSet resultSet = statement.executeQuery()) {
            TableStatistics.Builder tableStatisticsBuilder = TableStatistics.builder();

            while (resultSet.next()) {
                Optional<String> columnName = Optional.ofNullable(resultSet.getString("column_name"));
                if (columnName.isEmpty()) {
                    tableStatisticsBuilder.setRowCount(toEstimate(Optional.ofNullable(resultSet.getObject("row_count", Double.class))));
                }
                else {
                    JdbcColumnHandle columnHandle = columnHandles.get(columnName.get());
                    if (columnHandle == null) {
                        // Table schema could have been modified concurrently.
                        continue;
                    }

                    ColumnStatistics.Builder columnStatisticsBuilder = ColumnStatistics.builder();
                    columnStatisticsBuilder
                            .setDataSize(toEstimate(Optional.ofNullable(resultSet.getObject("data_size", Double.class))))
                            .setDistinctValuesCount(toEstimate(Optional.ofNullable(resultSet.getObject("distinct_values_count", Double.class))))
                            .setNullsFraction(toEstimate(Optional.ofNullable(resultSet.getObject("nulls_fraction", Double.class))));

                    Optional<String> lowValue = Optional.ofNullable(resultSet.getString("low_value"));
                    Optional<String> highValue = Optional.ofNullable(resultSet.getString("high_value"));
                    if (isNumericType(columnHandle.getColumnType())) {
                        columnStatisticsBuilder.setRange(createNumericRange(lowValue, highValue));
                    }
                    else if (columnHandle.getColumnType() == DATE) {
                        columnStatisticsBuilder.setRange(createDateRange(lowValue, highValue));
                    }

                    tableStatisticsBuilder.setColumnStatistics(columnHandle, columnStatisticsBuilder.build());
                }
            }

            return Optional.of(tableStatisticsBuilder.build());
        }
    }

    private PreparedStatement getShowStatsStatement(ConnectorSession session, Connection connection, JdbcTableHandle table, List<JdbcColumnHandle> jdbcColumnHandles)
            throws SQLException
    {
        checkArgument(table.getGroupingSets().isEmpty(), "table grouping sets should be empty: %s", table.getGroupingSets());
        return new QueryBuilder(this).buildSql(
                session,
                connection,
                table.getRemoteTableName(),
                table.getGroupingSets(),
                jdbcColumnHandles,
                table.getConstraint(),
                Optional.empty(),
                sql -> "SHOW STATS FOR (" + sql + ")");
    }

    private static Estimate toEstimate(Optional<Double> value)
    {
        return value.map(Estimate::of)
                .orElseGet(Estimate::unknown);
    }

    private static boolean isNumericType(Type type)
    {
        return type == TINYINT || type == SMALLINT || type == INTEGER || type == BIGINT || type == REAL || type == DOUBLE || type instanceof DecimalType;
    }

    private static Optional<DoubleRange> createNumericRange(Optional<String> minValue, Optional<String> maxValue)
    {
        if (minValue.isEmpty() || maxValue.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(new DoubleRange(
                minValue.map(BigDecimal::new).map(BigDecimal::doubleValue).orElse(Double.NEGATIVE_INFINITY),
                maxValue.map(BigDecimal::new).map(BigDecimal::doubleValue).orElse(Double.POSITIVE_INFINITY)));
    }

    private static Optional<DoubleRange> createDateRange(Optional<String> minValue, Optional<String> maxValue)
    {
        if (minValue.isEmpty() || maxValue.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(new DoubleRange(
                minValue
                        .map(LocalDate::parse)
                        .map(LocalDate::toEpochDay)
                        .map(Long::doubleValue)
                        .orElse(Double.NEGATIVE_INFINITY),
                maxValue
                        .map(LocalDate::parse)
                        .map(LocalDate::toEpochDay)
                        .map(Long::doubleValue)
                        .orElse(Double.POSITIVE_INFINITY)));
    }

    private static class JsonWriteFunction
            implements SliceWriteFunction
    {
        @Override
        public String getBindExpression()
        {
            return "json_parse(?)";
        }

        @Override
        public void set(PreparedStatement statement, int index, Slice value)
                throws SQLException
        {
            statement.setString(index, value.toStringUtf8());
        }
    }
}
