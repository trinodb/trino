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
package io.trino.plugin.trino;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.aggregation.AggregateFunctionRewriter;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcJoinCondition;
import io.trino.plugin.jdbc.JdbcJoinPushdownUtil;
import io.trino.plugin.jdbc.JdbcNamedRelationHandle;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcSortItem;
import io.trino.plugin.jdbc.JdbcSplit;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.aggregation.ImplementAvgDecimal;
import io.trino.plugin.jdbc.aggregation.ImplementAvgFloatingPoint;
import io.trino.plugin.jdbc.aggregation.ImplementCount;
import io.trino.plugin.jdbc.aggregation.ImplementCountAll;
import io.trino.plugin.jdbc.aggregation.ImplementCountDistinct;
import io.trino.plugin.jdbc.aggregation.ImplementMinMax;
import io.trino.plugin.jdbc.aggregation.ImplementSum;
import io.trino.plugin.jdbc.expression.ComparisonOperator;
import io.trino.plugin.jdbc.expression.JdbcConnectorExpressionRewriterBuilder;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.expression.RewriteComparison;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnPosition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.StandardFunctions;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.trino.matching.Pattern.typeOf;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.String.format;

/**
 * JDBC client for connecting one Trino instance to another.
 * <p>
 * Supports all standard Trino types including complex types (ARRAY, MAP, ROW)
 * by parsing JDBC type metadata and recursively mapping to Trino's type system.
 */
public class TrinoClient
        extends BaseJdbcClient
{
    private static final Logger log = Logger.get(TrinoClient.class);

    private static final Duration CAPABILITIES_LOAD_BACKOFF = Duration.ofSeconds(10);

    private static final JdbcTypeHandle BIGINT_TYPE_HANDLE = new JdbcTypeHandle(
            Types.BIGINT, Optional.of("bigint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    private final ConnectorExpressionRewriter<ParameterizedExpression> connectorExpressionRewriter;
    private final AggregateFunctionRewriter<JdbcExpression, ParameterizedExpression> aggregateFunctionRewriter;
    private final JsonTransportHelper jsonTransportHelper;
    private final TrinoReadMappingFactory readMappingFactory;
    private final PassthroughQueryMetadataHelper passthroughQueryMetadataHelper;
    private final TrinoDelegationAnalyzer delegationAnalyzer;
    private final boolean statisticsEnabled;
    private final AtomicReference<TrinoRemoteCapabilities> remoteCapabilities = new AtomicReference<>();
    private final FailureBackoff capabilitiesLoadBackoff = new FailureBackoff(CAPABILITIES_LOAD_BACKOFF);
    private final AtomicBoolean remoteVersionLogged = new AtomicBoolean();

    @Inject
    public TrinoClient(
            BaseJdbcConfig config,
            JdbcStatisticsConfig statisticsConfig,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            TypeManager typeManager,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier remoteQueryModifier)
    {
        super("\"", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, remoteQueryModifier, true);
        this.statisticsEnabled = statisticsConfig.isEnabled();
        this.jsonTransportHelper = new JsonTransportHelper(this::quoted);
        this.readMappingFactory = new TrinoReadMappingFactory(typeManager, typeHandle -> mapToUnboundedVarchar(typeHandle));
        this.passthroughQueryMetadataHelper = new PassthroughQueryMetadataHelper(typeManager, this::toColumnMapping);
        TrinoCompatibilityRegistry compatibilityRegistry = new TrinoCompatibilityRegistry();
        this.delegationAnalyzer = new TrinoDelegationAnalyzer(new TrinoRemoteSqlRenderer(this::quoted, compatibilityRegistry));

        // Two expression layers coexist by design: the renderer
        // (TrinoRemoteSqlRenderer, behind TrinoDelegationAnalyzer) is the
        // Trino-native extension path for delegating compatible expressions, while
        // the standard JDBC rewriter below is the baseline — it defines the
        // semantics when delegation is disabled (remote-delegation.enabled=false)
        // and backs the aggregate function rewriter, so it cannot be removed.
        this.connectorExpressionRewriter = createConnectorExpressionRewriter(this::quoted);

        // Register only aggregate implementations whose semantics match remote Trino.
        this.aggregateFunctionRewriter = new AggregateFunctionRewriter<>(
                connectorExpressionRewriter,
                ImmutableSet.<AggregateFunctionRule<JdbcExpression, ParameterizedExpression>>builder()
                        .add(new ImplementCountAll(BIGINT_TYPE_HANDLE))
                        .add(new ImplementCount(BIGINT_TYPE_HANDLE))
                        .add(new ImplementCountDistinct(BIGINT_TYPE_HANDLE, true))
                        .add(new ImplementMinMax(true)) // both sides share Trino ordering semantics
                        .add(new ImplementSum(TrinoClient::decimalTypeHandle))
                        .add(new ImplementAvgFloatingPoint())
                        .add(new ImplementAvgDecimal())
                        .build());
    }

    static ConnectorExpressionRewriter<ParameterizedExpression> createConnectorExpressionRewriter(Function<String, String> identifierQuote)
    {
        return JdbcConnectorExpressionRewriterBuilder.newBuilder()
                .addStandardRules(identifierQuote)
                .add(new RewriteComparison(EnumSet.allOf(ComparisonOperator.class)))
                .add(new RewriteBinaryArithmeticOperator(StandardFunctions.ADD_FUNCTION_NAME, "+"))
                .add(new RewriteBinaryArithmeticOperator(StandardFunctions.SUBTRACT_FUNCTION_NAME, "-"))
                .build();
    }

    private static Optional<JdbcTypeHandle> decimalTypeHandle(DecimalType decimalType)
    {
        return Optional.of(new JdbcTypeHandle(
                Types.DECIMAL,
                Optional.of(decimalType.getDisplayName()),
                Optional.of(decimalType.getPrecision()),
                Optional.of(decimalType.getScale()),
                Optional.empty(),
                Optional.empty()));
    }

    private static final class RewriteBinaryArithmeticOperator
            implements ConnectorExpressionRule<Call, ParameterizedExpression>
    {
        private final FunctionName functionName;
        private final String sqlOperator;

        private RewriteBinaryArithmeticOperator(FunctionName functionName, String sqlOperator)
        {
            this.functionName = functionName;
            this.sqlOperator = sqlOperator;
        }

        @Override
        public Pattern<Call> getPattern()
        {
            return typeOf(Call.class)
                    .matching(call -> call.getArguments().size() == 2)
                    .matching(call -> call.getFunctionName().equals(functionName));
        }

        @Override
        public Optional<ParameterizedExpression> rewrite(Call expression, Captures captures, RewriteContext<ParameterizedExpression> context)
        {
            Optional<ParameterizedExpression> left = context.defaultRewrite(expression.getArguments().get(0));
            Optional<ParameterizedExpression> right = context.defaultRewrite(expression.getArguments().get(1));
            if (left.isEmpty() || right.isEmpty()) {
                return Optional.empty();
            }

            ParameterizedExpression leftExpression = left.get();
            ParameterizedExpression rightExpression = right.get();
            List<QueryParameter> parameters = new ArrayList<>(
                    leftExpression.parameters().size() + rightExpression.parameters().size());
            parameters.addAll(leftExpression.parameters());
            parameters.addAll(rightExpression.parameters());

            return Optional.of(new ParameterizedExpression(
                    format("(%s %s %s)", leftExpression.expression(), sqlOperator, rightExpression.expression()),
                    List.copyOf(parameters)));
        }
    }

    // -------------------------------------------------------------------------
    // Pushdown support: LIMIT, TopN, Aggregation, Join
    //
    // Both sides are Trino with identical SQL syntax, so the supported pushdown
    // operations below can significantly reduce data transfer through JDBC.
    // -------------------------------------------------------------------------

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
    protected Optional<TopNFunction> topNFunction()
    {
        return Optional.of(TopNFunction.sqlStandard(this::quoted));
    }

    @Override
    public boolean supportsTopN(ConnectorSession session, JdbcTableHandle handle, List<JdbcSortItem> sortOrder)
    {
        return true;
    }

    @Override
    public boolean isTopNGuaranteed(ConnectorSession session)
    {
        // A transport projection can wrap the remote TopN in an outer query whose
        // ordering is not guaranteed by SQL, so retain the local TopN operation.
        return false;
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        return delegationAnalyzer.analyzeAggregation(session, aggregate, assignments, getRemoteCapabilities(session))
                .or(() -> aggregateFunctionRewriter.rewrite(session, aggregate, assignments));
    }

    @Override
    public Optional<ParameterizedExpression> convertPredicate(ConnectorSession session, ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        return delegationAnalyzer.analyzePredicate(session, expression, assignments, getRemoteCapabilities(session))
                .or(() -> connectorExpressionRewriter.rewrite(session, expression, assignments));
    }

    @Override
    public Optional<JdbcExpression> convertProjection(ConnectorSession session, JdbcTableHandle handle, ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        if (!handle.getUpdateAssignments().isEmpty() || assignments.values().stream().anyMatch(TrinoClient::isHiddenJdbcColumn)) {
            return Optional.empty();
        }
        return delegationAnalyzer.analyzeProjection(session, expression, assignments, getRemoteCapabilities(session));
    }

    @Override
    protected boolean isSupportedJoinCondition(ConnectorSession session, JdbcJoinCondition joinCondition)
    {
        // This only declares operator support for the base JDBC join implementation.
        // Planner-side coercions and rendered join conditions determine whether any
        // compatibility-sensitive casts are present.
        return switch (joinCondition.getOperator()) {
            case EQUAL,
                 NOT_EQUAL,
                 LESS_THAN,
                 LESS_THAN_OR_EQUAL,
                 GREATER_THAN,
                 GREATER_THAN_OR_EQUAL,
                 IDENTICAL -> true;
        };
    }

    @Override
    public boolean supportsMerge()
    {
        return false;
    }

    @Override
    @SuppressWarnings("deprecation") // Support the legacy JDBC join path while it remains part of the SPI
    public Optional<PreparedQuery> legacyImplementJoin(
            ConnectorSession session,
            JoinType joinType,
            PreparedQuery leftSource,
            PreparedQuery rightSource,
            List<JdbcJoinCondition> joinConditions,
            Map<JdbcColumnHandle, String> rightAssignments,
            Map<JdbcColumnHandle, String> leftAssignments,
            JoinStatistics statistics)
    {
        return JdbcJoinPushdownUtil.implementJoinCostAware(
                session,
                joinType,
                leftSource,
                rightSource,
                statistics,
                () -> super.legacyImplementJoin(
                        session,
                        joinType,
                        leftSource,
                        rightSource,
                        joinConditions,
                        rightAssignments,
                        leftAssignments,
                        statistics));
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
        return JdbcJoinPushdownUtil.implementJoinCostAware(
                session,
                joinType,
                leftSource,
                rightSource,
                statistics,
                () -> super.implementJoin(
                        session,
                        joinType,
                        leftSource,
                        leftProjections,
                        rightSource,
                        rightProjections,
                        joinConditions,
                        statistics));
    }

    @Override
    protected PreparedQuery prepareQuery(
            ConnectorSession session,
            Connection connection,
            JdbcTableHandle table,
            Optional<List<List<JdbcColumnHandle>>> groupingSets,
            List<JdbcColumnHandle> columns,
            Map<String, ParameterizedExpression> columnExpressions,
            Optional<JdbcSplit> split)
    {
        PreparedQuery preparedQuery = super.prepareQuery(session, connection, table, groupingSets, columns, columnExpressions, split);
        // The transport projection must be applied exactly once, at scan time (split present).
        // Planning-time invocations (join source synthesis) pass no split; wrapping there too
        // would re-wrap the already-cast columns when the joined query is scanned.
        return split.isPresent() ? applyTransportProjection(preparedQuery, columns) : preparedQuery;
    }

    @Override
    public JdbcTableHandle getTableHandle(ConnectorSession session, PreparedQuery preparedQuery)
    {
        PreparedQuery normalizedQuery = normalizePassthroughQuery(preparedQuery);
        PassthroughQueryValidator.validate(normalizedQuery.query());
        try (Connection connection = getConnection(session)) {
            List<PassthroughQueryMetadataHelper.DescribedOutputColumn> outputColumns = passthroughQueryMetadataHelper.describeOutputColumns(connection, normalizedQuery);
            PreparedQuery addressableQuery = passthroughQueryMetadataHelper.withOutputAliases(normalizedQuery, outputColumns);
            JdbcTableHandle tableHandle;
            try {
                tableHandle = super.getTableHandle(session, addressableQuery);
            }
            catch (TrinoException | UnsupportedOperationException metadataFailure) {
                // BaseJdbcClient.getTableHandle may throw TrinoException for passthrough
                // queries that use types or syntax not representable by standard JDBC metadata.
                // Some paths surface as UnsupportedOperationException from BaseJdbcClient.
                // Fall back to building the table handle from DESCRIBE OUTPUT metadata.
                return executeFallback(
                        metadataFailure,
                        () -> passthroughQueryMetadataHelper.buildPassthroughTableHandle(session, connection, addressableQuery, outputColumns));
            }
            return passthroughQueryMetadataHelper.rewriteColumns(session, connection, tableHandle, outputColumns);
        }
        catch (SQLException e) {
            // DESCRIBE OUTPUT-based metadata is unavailable for this query; retry the
            // plain JDBC metadata path, keeping this failure attached if the retry
            // also fails. Connector-internal failures (TrinoException) propagate.
            return executeFallback(e, () -> super.getTableHandle(session, normalizedQuery));
        }
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle)
    {
        if (!statisticsEnabled || !handle.isNamedRelation()) {
            return TableStatistics.builder().build();
        }

        JdbcNamedRelationHandle relation = handle.getRequiredNamedRelation();
        String statsQuery = "SHOW STATS FOR " + quoted(relation.getRemoteTableName());
        try {
            List<JdbcColumnHandle> columns = handle.getColumns()
                    .orElseGet(() -> getColumns(session, relation.getSchemaTableName(), relation.getRemoteTableName()));
            try (Connection connection = getConnection(session);
                    PreparedStatement statement = connection.prepareStatement(statsQuery);
                    ResultSet resultSet = statement.executeQuery()) {
                logRemoteVersionOnce(connection);
                TableStatistics.Builder tableStatistics = TableStatistics.builder();
                while (resultSet.next()) {
                    String columnName = resultSet.getString("column_name");
                    if (columnName == null) {
                        Double rowCount = getNullableDouble(resultSet, "row_count");
                        if (rowCount != null) {
                            tableStatistics.setRowCount(Estimate.of(rowCount));
                        }
                        continue;
                    }

                    Optional<JdbcColumnHandle> column = findColumn(columns, columnName);
                    if (column.isPresent()) {
                        tableStatistics.setColumnStatistics(column.orElseThrow(), ColumnStatistics.builder()
                                .setDataSize(toEstimate(getNullableDouble(resultSet, "data_size")))
                                .setDistinctValuesCount(toEstimate(getNullableDouble(resultSet, "distinct_values_count")))
                                .setNullsFraction(toEstimate(getNullableDouble(resultSet, "nulls_fraction")))
                                .build());
                    }
                }
                return tableStatistics.build();
            }
        }
        catch (SQLException | RuntimeException e) {
            log.warn(e, "Failed to fetch table statistics: %s", statsQuery);
            return TableStatistics.builder().build();
        }
    }

    private void logRemoteVersionOnce(Connection connection)
    {
        if (remoteVersionLogged.compareAndSet(false, true)) {
            try {
                String version = connection.getMetaData().getDatabaseProductVersion();
                log.info("Remote Trino version: %s", version);
            }
            catch (SQLException e) {
                log.warn(e, "Unable to determine remote Trino version");
            }
        }
    }

    // -------------------------------------------------------------------------
    // Read-only enforcement: block all write and DDL operations
    // -------------------------------------------------------------------------

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        // JDBC query builders use write mappings for SELECT predicate parameter binding.
        // Keep those mappings available while blocking data-changing operations separately.
        return TrinoParameterBindingFactory.createWriteMapping(type)
                .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "Unsupported parameter type for pushdown: " + type));
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables");
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Consumer<Runnable> rollbackActionCollector)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with data");
    }

    @Override
    public void renameTable(ConnectorSession session, JdbcTableHandle handle, SchemaTableName newTableName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming tables");
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column, ColumnPosition position)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support adding columns");
    }

    @Override
    public void renameColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, String newColumnName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming columns");
    }

    @Override
    public void dropColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping columns");
    }

    @Override
    public void setColumnType(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Type type)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support setting column types");
    }

    @Override
    public void dropNotNullConstraint(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping NOT NULL constraints");
    }

    @Override
    public void dropTable(ConnectorSession session, JdbcTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping tables");
    }

    @Override
    public void truncateTable(ConnectorSession session, JdbcTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support truncating tables");
    }

    @Override
    public JdbcOutputTableHandle beginInsertTable(ConnectorSession session, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columns)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support inserts");
    }

    @Override
    public OptionalLong delete(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support deletes");
    }

    @Override
    public OptionalLong update(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support updates");
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating schemas");
    }

    @Override
    public void renameSchema(ConnectorSession session, String schemaName, String newSchemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming schemas");
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping schemas");
    }

    // -------------------------------------------------------------------------
    // Read mapping: JDBC type -> Trino type
    // -------------------------------------------------------------------------

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }
        return readMappingFactory.createColumnMapping(session, typeHandle);
    }

    private PreparedQuery applyTransportProjection(PreparedQuery preparedQuery, List<JdbcColumnHandle> columns)
    {
        List<TrinoTypeClassifier.TransportKind> transportKinds = columns.stream()
                .map(JdbcColumnHandle::getColumnType)
                .map(TrinoTypeClassifier::transportKind)
                .toList();
        if (transportKinds.stream().allMatch(kind -> kind == TrinoTypeClassifier.TransportKind.NATIVE)) {
            return preparedQuery;
        }

        String relationAlias = quoted("_trino_transport");
        List<String> selectItems = new ArrayList<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            JdbcColumnHandle column = columns.get(i);
            Type logicalType = column.getColumnType();
            TrinoTypeClassifier.TransportKind transportKind = transportKinds.get(i);
            String reference = relationAlias + "." + quoted(column.getColumnName());
            String alias = quoted(column.getColumnName());
            selectItems.add(switch (transportKind) {
                case NATIVE -> reference + " AS " + alias;
                case VARCHAR_CAST -> (logicalType instanceof TimestampWithTimeZoneType
                        ? TimestampWithTimeZoneTransport.readExpression(reference)
                        : "CAST(" + reference + " AS VARCHAR)") + " AS " + alias;
                case VARBINARY_CAST -> "CAST(" + reference + " AS VARBINARY) AS " + alias;
                case JSON_CAST -> "json_format(CAST(" + jsonTransportHelper.buildJsonTransportExpression(reference, logicalType) + " AS JSON)) AS " + alias;
            });
        }

        return preparedQuery.transformQuery(sql ->
                "SELECT " + String.join(", ", selectItems) + " FROM (" + sql + ") " + relationAlias);
    }

    private TrinoRemoteCapabilities getRemoteCapabilities(ConnectorSession session)
    {
        TrinoRemoteCapabilities cached = remoteCapabilities.get();
        if (cached != null) {
            return cached;
        }
        if (!capabilitiesLoadBackoff.shouldAttempt()) {
            return TrinoRemoteCapabilities.unavailable();
        }

        TrinoRemoteCapabilities loaded;
        try (Connection connection = getConnection(session)) {
            loaded = TrinoRemoteCapabilities.load(connection);
            loaded.version().ifPresent(version -> {
                if (remoteVersionLogged.compareAndSet(false, true)) {
                    log.info("Remote Trino version: %s", version);
                }
            });
        }
        catch (SQLException | RuntimeException e) {
            // Transient failures must not be cached: only suppress reload attempts
            // briefly, so a remote outage does not disable delegation until restart
            capabilitiesLoadBackoff.recordFailure();
            log.warn(e, "Unable to determine remote Trino capabilities; Trino-native delegation will fall back to local execution where possible");
            return TrinoRemoteCapabilities.unavailable();
        }

        if (remoteCapabilities.compareAndSet(null, loaded)) {
            return loaded;
        }
        return remoteCapabilities.get();
    }

    private static Estimate toEstimate(Double value)
    {
        return value == null ? Estimate.unknown() : Estimate.of(value);
    }

    private static Double getNullableDouble(ResultSet resultSet, String columnName)
            throws SQLException
    {
        Object value = resultSet.getObject(columnName);
        if (value == null) {
            return null;
        }
        return ((Number) value).doubleValue();
    }

    private static Optional<JdbcColumnHandle> findColumn(List<JdbcColumnHandle> columns, String columnName)
    {
        return columns.stream()
                .filter(column -> column.getColumnName().equalsIgnoreCase(columnName))
                .findFirst();
    }

    private static boolean isHiddenJdbcColumn(ColumnHandle columnHandle)
    {
        return columnHandle instanceof JdbcColumnHandle jdbcColumnHandle && jdbcColumnHandle.getColumnName().startsWith("$");
    }

    static String stripTrailingSemicolon(String sql)
    {
        String trimmed = sql.trim();
        if (trimmed.endsWith(";")) {
            return trimmed.substring(0, trimmed.length() - 1);
        }
        return trimmed;
    }

    static PreparedQuery normalizePassthroughQuery(PreparedQuery preparedQuery)
    {
        return preparedQuery.transformQuery(TrinoClient::stripTrailingSemicolon);
    }

    static <T> T executeFallback(Throwable primaryFailure, Supplier<T> fallback)
    {
        try {
            return fallback.get();
        }
        catch (RuntimeException fallbackFailure) {
            if (fallbackFailure != primaryFailure) {
                fallbackFailure.addSuppressed(primaryFailure);
            }
            throw fallbackFailure;
        }
    }
}
