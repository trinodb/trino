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
package io.trino.plugin.jdbc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.trino.plugin.jdbc.JdbcProcedureHandle.ProcedureQuery;
import io.trino.plugin.jdbc.PredicatePushdownController.DomainPushdownResult;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.ptf.Procedure.ProcedureFunctionHandle;
import io.trino.plugin.jdbc.ptf.Query.QueryFunctionHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableSchema;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.JoinApplicationResult;
import io.trino.spi.connector.JoinCondition;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableFunctionApplicationResult;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.connector.TableScanRedirectApplicationResult;
import io.trino.spi.connector.TopNApplicationResult;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.Variable;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.Type;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.google.common.base.Functions.identity;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.base.expression.ConnectorExpressions.and;
import static io.trino.plugin.base.expression.ConnectorExpressions.extractConjuncts;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_NON_TRANSIENT_ERROR;
import static io.trino.plugin.jdbc.JdbcMetadataSessionProperties.isAggregationPushdownEnabled;
import static io.trino.plugin.jdbc.JdbcMetadataSessionProperties.isComplexExpressionPushdown;
import static io.trino.plugin.jdbc.JdbcMetadataSessionProperties.isJoinPushdownEnabled;
import static io.trino.plugin.jdbc.JdbcMetadataSessionProperties.isTopNPushdownEnabled;
import static io.trino.plugin.jdbc.JdbcWriteSessionProperties.isNonTransactionalInsert;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

public class DefaultJdbcMetadata
        implements JdbcMetadata
{
    private static final String SYNTHETIC_COLUMN_NAME_PREFIX = "_pfgnrtd_";
    private static final String DELETE_ROW_ID = "_trino_artificial_column_handle_for_delete_row_id_";
    private static final String MERGE_ROW_ID = "$merge_row_id";

    private final JdbcClient jdbcClient;
    private final boolean precalculateStatisticsForPushdown;
    private final Set<JdbcQueryEventListener> jdbcQueryEventListeners;

    private final AtomicReference<Runnable> rollbackAction = new AtomicReference<>();

    public DefaultJdbcMetadata(JdbcClient jdbcClient, boolean precalculateStatisticsForPushdown, Set<JdbcQueryEventListener> jdbcQueryEventListeners)
    {
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
        this.precalculateStatisticsForPushdown = precalculateStatisticsForPushdown;
        this.jdbcQueryEventListeners = ImmutableSet.copyOf(requireNonNull(jdbcQueryEventListeners, "queryEventListeners is null"));
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        return jdbcClient.schemaExists(session, schemaName);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.copyOf(jdbcClient.getSchemaNames(session));
    }

    @Override
    public JdbcTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        return jdbcClient.getTableHandle(session, tableName)
                .orElse(null);
    }

    @Override
    public JdbcTableHandle getTableHandle(ConnectorSession session, PreparedQuery preparedQuery)
    {
        return jdbcClient.getTableHandle(session, preparedQuery);
    }

    @Override
    public JdbcProcedureHandle getProcedureHandle(ConnectorSession session, ProcedureQuery procedureQuery)
    {
        return jdbcClient.getProcedureHandle(session, procedureQuery);
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        return jdbcClient.getSystemTable(session, tableName);
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle table, Constraint constraint)
    {
        if (isTableHandleForProcedure(table)) {
            return Optional.empty();
        }

        JdbcTableHandle handle = (JdbcTableHandle) table;
        if (handle.getSortOrder().isPresent() && handle.getLimit().isPresent()) {
            handle = flushAttributesAsQuery(session, handle);
        }

        TupleDomain<ColumnHandle> oldDomain = handle.getConstraint();
        TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(constraint.getSummary());
        List<ParameterizedExpression> newConstraintExpressions;
        TupleDomain<ColumnHandle> remainingFilter;
        Optional<ConnectorExpression> remainingExpression;
        if (newDomain.isNone()) {
            newConstraintExpressions = ImmutableList.of();
            remainingFilter = TupleDomain.all();
            remainingExpression = Optional.of(Constant.TRUE);
        }
        else {
            Map<ColumnHandle, Domain> domains = newDomain.getDomains().orElseThrow();
            List<JdbcColumnHandle> columnHandles = domains.keySet().stream()
                    .map(JdbcColumnHandle.class::cast)
                    .collect(toImmutableList());
            List<ColumnMapping> columnMappings = jdbcClient.toColumnMappings(
                    session,
                    columnHandles.stream()
                            .map(JdbcColumnHandle::getJdbcTypeHandle)
                            .collect(toImmutableList()));

            Map<ColumnHandle, Domain> supported = new HashMap<>();
            Map<ColumnHandle, Domain> unsupported = new HashMap<>();
            for (int i = 0; i < columnHandles.size(); i++) {
                JdbcColumnHandle column = columnHandles.get(i);
                ColumnMapping mapping = columnMappings.get(i);
                DomainPushdownResult pushdownResult = mapping.getPredicatePushdownController().apply(session, domains.get(column));
                supported.put(column, pushdownResult.getPushedDown());
                unsupported.put(column, pushdownResult.getRemainingFilter());
            }

            newDomain = TupleDomain.withColumnDomains(supported);
            remainingFilter = TupleDomain.withColumnDomains(unsupported);

            if (isComplexExpressionPushdown(session)) {
                List<ParameterizedExpression> newExpressions = new ArrayList<>();
                List<ConnectorExpression> remainingExpressions = new ArrayList<>();
                for (ConnectorExpression expression : extractConjuncts(constraint.getExpression())) {
                    Optional<ParameterizedExpression> converted = jdbcClient.convertPredicate(session, expression, constraint.getAssignments());
                    if (converted.isPresent()) {
                        newExpressions.add(converted.get());
                    }
                    else {
                        remainingExpressions.add(expression);
                    }
                }
                newConstraintExpressions = ImmutableSet.<ParameterizedExpression>builder()
                        .addAll(handle.getConstraintExpressions())
                        .addAll(newExpressions)
                        .build().asList();
                remainingExpression = Optional.of(and(remainingExpressions));
            }
            else {
                newConstraintExpressions = ImmutableList.of();
                remainingExpression = Optional.empty();
            }
        }

        if (oldDomain.equals(newDomain) &&
                handle.getConstraintExpressions().equals(newConstraintExpressions)) {
            return Optional.empty();
        }

        handle = new JdbcTableHandle(
                handle.getRelationHandle(),
                newDomain,
                newConstraintExpressions,
                handle.getSortOrder(),
                handle.getLimit(),
                handle.getColumns(),
                handle.getOtherReferencedTables(),
                handle.getNextSyntheticColumnId(),
                handle.getAuthorization());

        return Optional.of(
                remainingExpression.isPresent()
                        ? new ConstraintApplicationResult<>(handle, remainingFilter, remainingExpression.get(), precalculateStatisticsForPushdown)
                        : new ConstraintApplicationResult<>(handle, remainingFilter, precalculateStatisticsForPushdown));
    }

    private JdbcTableHandle flushAttributesAsQuery(ConnectorSession session, JdbcTableHandle handle)
    {
        List<JdbcColumnHandle> columns = jdbcClient.getColumns(session, handle);
        PreparedQuery preparedQuery = jdbcClient.prepareQuery(session, handle, Optional.empty(), columns, ImmutableMap.of());

        return new JdbcTableHandle(
                new JdbcQueryRelationHandle(preparedQuery),
                TupleDomain.all(),
                ImmutableList.of(),
                Optional.empty(),
                OptionalLong.empty(),
                Optional.of(columns),
                handle.getAllReferencedTables(),
                handle.getNextSyntheticColumnId(),
                handle.getAuthorization());
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session,
            ConnectorTableHandle table,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        if (isTableHandleForProcedure(table)) {
            return Optional.empty();
        }

        JdbcTableHandle handle = (JdbcTableHandle) table;

        List<JdbcColumnHandle> newColumns = assignments.values().stream()
                .map(JdbcColumnHandle.class::cast)
                .collect(toImmutableList());

        if (handle.getColumns().isPresent()) {
            Set<JdbcColumnHandle> newColumnSet = ImmutableSet.copyOf(newColumns);
            Set<JdbcColumnHandle> tableColumnSet = ImmutableSet.copyOf(handle.getColumns().get());
            if (newColumnSet.equals(tableColumnSet)) {
                return Optional.empty();
            }

            Set<JdbcColumnHandle> newPhysicalColumns = newColumns.stream()
                    // It may happen fresh table handle comes with a columns prepared already.
                    // In such case it may happen that applyProjection may want to add merge/delete row id, which is created later during the planning.
                    .filter(column -> !column.getColumnName().equals(MERGE_ROW_ID))
                    .filter(column -> !column.getColumnName().equals(DELETE_ROW_ID))
                    .collect(toImmutableSet());
            verify(tableColumnSet.containsAll(newPhysicalColumns), "applyProjection called with columns %s and some are not available in existing query: %s", newPhysicalColumns, tableColumnSet);
        }

        return Optional.of(new ProjectionApplicationResult<>(
                new JdbcTableHandle(
                        handle.getRelationHandle(),
                        handle.getConstraint(),
                        handle.getConstraintExpressions(),
                        handle.getSortOrder(),
                        handle.getLimit(),
                        Optional.of(newColumns),
                        handle.getOtherReferencedTables(),
                        handle.getNextSyntheticColumnId(),
                        handle.getAuthorization()),
                projections,
                assignments.entrySet().stream()
                        .map(assignment -> new Assignment(
                                assignment.getKey(),
                                assignment.getValue(),
                                ((JdbcColumnHandle) assignment.getValue()).getColumnType()))
                        .collect(toImmutableList()),
                precalculateStatisticsForPushdown));
    }

    @Override
    public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(
            ConnectorSession session,
            ConnectorTableHandle table,
            List<AggregateFunction> aggregates,
            Map<String, ColumnHandle> assignments,
            List<List<ColumnHandle>> groupingSets)
    {
        if (isTableHandleForProcedure(table)) {
            return Optional.empty();
        }

        if (!isAggregationPushdownEnabled(session)) {
            return Optional.empty();
        }

        JdbcTableHandle handle = (JdbcTableHandle) table;

        // Global aggregation is represented by [[]]
        verify(!groupingSets.isEmpty(), "No grouping sets provided");

        // Global aggregation (with no other grouping sets) is handled implicitly by the DefaultQueryBuilder, thanks to presence of aggregate functions.
        // When there are no aggregate functions, it would need to be handled explicitly. However, such pushdown isn't sensible, since engine knows that
        // there will be exactly one result row.
        verify(!aggregates.isEmpty() || !groupingSets.equals(List.of(List.of())), "Unexpected global aggregation with no aggregate functions");

        if (!jdbcClient.supportsAggregationPushdown(session, handle, aggregates, assignments, groupingSets)) {
            // JDBC client implementation prevents pushdown for the given table
            return Optional.empty();
        }

        if (handle.getLimit().isPresent()) {
            handle = flushAttributesAsQuery(session, handle);
        }

        int nextSyntheticColumnId = handle.getNextSyntheticColumnId();

        ImmutableList.Builder<JdbcColumnHandle> newColumns = ImmutableList.builder();
        ImmutableList.Builder<ConnectorExpression> projections = ImmutableList.builder();
        ImmutableList.Builder<Assignment> resultAssignments = ImmutableList.builder();
        ImmutableMap.Builder<String, ParameterizedExpression> expressions = ImmutableMap.builder();

        List<List<JdbcColumnHandle>> groupingSetsAsJdbcColumnHandles = groupingSets.stream()
                .map(groupingSet -> groupingSet.stream()
                        .map(JdbcColumnHandle.class::cast)
                        .collect(toImmutableList()))
                .collect(toImmutableList());
        Optional<List<JdbcColumnHandle>> tableColumns = handle.getColumns();
        groupingSetsAsJdbcColumnHandles.stream()
                .flatMap(List::stream)
                .distinct()
                .peek(handle.getColumns().<Consumer<JdbcColumnHandle>>map(
                                columns -> groupKey -> verify(columns.contains(groupKey),
                                        "applyAggregation called with a grouping column %s which was not included in the table columns: %s",
                                        groupKey,
                                        tableColumns))
                        .orElse(groupKey -> {}))
                .forEach(newColumns::add);

        for (AggregateFunction aggregate : aggregates) {
            Optional<JdbcExpression> expression = jdbcClient.implementAggregation(session, aggregate, assignments);
            if (expression.isEmpty()) {
                return Optional.empty();
            }

            String columnName = SYNTHETIC_COLUMN_NAME_PREFIX + nextSyntheticColumnId;
            nextSyntheticColumnId++;
            JdbcColumnHandle newColumn = JdbcColumnHandle.builder()
                    .setColumnName(columnName)
                    .setJdbcTypeHandle(expression.get().getJdbcTypeHandle())
                    .setColumnType(aggregate.getOutputType())
                    .setComment(Optional.of("synthetic"))
                    .build();

            newColumns.add(newColumn);
            projections.add(new Variable(newColumn.getColumnName(), aggregate.getOutputType()));
            resultAssignments.add(new Assignment(newColumn.getColumnName(), newColumn, aggregate.getOutputType()));
            expressions.put(columnName, new ParameterizedExpression(expression.get().getExpression(), expression.get().getParameters()));
        }

        List<JdbcColumnHandle> newColumnsList = newColumns.build();

        // We need to have matching column handles in JdbcTableHandle constructed below, as columns read via JDBC must match column handles list.
        // For more context see assertion in JdbcRecordSetProvider.getRecordSet
        PreparedQuery preparedQuery = jdbcClient.prepareQuery(
                session,
                handle,
                Optional.of(groupingSetsAsJdbcColumnHandles),
                newColumnsList,
                expressions.buildOrThrow());
        handle = new JdbcTableHandle(
                new JdbcQueryRelationHandle(preparedQuery),
                TupleDomain.all(),
                ImmutableList.of(),
                Optional.empty(),
                OptionalLong.empty(),
                Optional.of(newColumnsList),
                handle.getAllReferencedTables(),
                nextSyntheticColumnId,
                handle.getAuthorization());

        return Optional.of(new AggregationApplicationResult<>(handle, projections.build(), resultAssignments.build(), ImmutableMap.of(), precalculateStatisticsForPushdown));
    }

    @Override
    public Optional<JoinApplicationResult<ConnectorTableHandle>> applyJoin(
            ConnectorSession session,
            JoinType joinType,
            ConnectorTableHandle left,
            ConnectorTableHandle right,
            List<JoinCondition> joinConditions,
            Map<String, ColumnHandle> leftAssignments,
            Map<String, ColumnHandle> rightAssignments,
            JoinStatistics statistics)
    {
        if (isTableHandleForProcedure(left) || isTableHandleForProcedure(right)) {
            return Optional.empty();
        }

        if (!isJoinPushdownEnabled(session)) {
            return Optional.empty();
        }

        JdbcTableHandle leftHandle = flushAttributesAsQuery(session, (JdbcTableHandle) left);
        JdbcTableHandle rightHandle = flushAttributesAsQuery(session, (JdbcTableHandle) right);

        if (!leftHandle.getAuthorization().equals(rightHandle.getAuthorization())) {
            return Optional.empty();
        }
        int nextSyntheticColumnId = max(leftHandle.getNextSyntheticColumnId(), rightHandle.getNextSyntheticColumnId());

        ImmutableMap.Builder<JdbcColumnHandle, JdbcColumnHandle> newLeftColumnsBuilder = ImmutableMap.builder();
        for (JdbcColumnHandle column : jdbcClient.getColumns(session, leftHandle)) {
            newLeftColumnsBuilder.put(column, JdbcColumnHandle.builderFrom(column)
                    .setColumnName(column.getColumnName() + "_" + nextSyntheticColumnId)
                    .build());
            nextSyntheticColumnId++;
        }
        Map<JdbcColumnHandle, JdbcColumnHandle> newLeftColumns = newLeftColumnsBuilder.buildOrThrow();

        ImmutableMap.Builder<JdbcColumnHandle, JdbcColumnHandle> newRightColumnsBuilder = ImmutableMap.builder();
        for (JdbcColumnHandle column : jdbcClient.getColumns(session, rightHandle)) {
            newRightColumnsBuilder.put(column, JdbcColumnHandle.builderFrom(column)
                    .setColumnName(column.getColumnName() + "_" + nextSyntheticColumnId)
                    .build());
            nextSyntheticColumnId++;
        }
        Map<JdbcColumnHandle, JdbcColumnHandle> newRightColumns = newRightColumnsBuilder.buildOrThrow();

        ImmutableList.Builder<JdbcJoinCondition> jdbcJoinConditions = ImmutableList.builder();
        for (JoinCondition joinCondition : joinConditions) {
            Optional<JdbcColumnHandle> leftColumn = getVariableColumnHandle(leftAssignments, joinCondition.getLeftExpression());
            Optional<JdbcColumnHandle> rightColumn = getVariableColumnHandle(rightAssignments, joinCondition.getRightExpression());
            if (leftColumn.isEmpty() || rightColumn.isEmpty()) {
                return Optional.empty();
            }
            jdbcJoinConditions.add(new JdbcJoinCondition(leftColumn.get(), joinCondition.getOperator(), rightColumn.get()));
        }

        Optional<PreparedQuery> joinQuery = jdbcClient.implementJoin(
                session,
                joinType,
                asPreparedQuery(leftHandle),
                asPreparedQuery(rightHandle),
                jdbcJoinConditions.build(),
                newRightColumns.entrySet().stream()
                        .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().getColumnName())),
                newLeftColumns.entrySet().stream()
                        .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().getColumnName())),
                statistics);

        if (joinQuery.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(new JoinApplicationResult<>(
                new JdbcTableHandle(
                        new JdbcQueryRelationHandle(joinQuery.get()),
                        TupleDomain.all(),
                        ImmutableList.of(),
                        Optional.empty(),
                        OptionalLong.empty(),
                        Optional.of(
                                ImmutableList.<JdbcColumnHandle>builder()
                                        .addAll(newLeftColumns.values())
                                        .addAll(newRightColumns.values())
                                        .build()),
                        leftHandle.getAllReferencedTables().flatMap(leftReferencedTables ->
                                rightHandle.getAllReferencedTables().map(rightReferencedTables ->
                                        ImmutableSet.<SchemaTableName>builder()
                                                .addAll(leftReferencedTables)
                                                .addAll(rightReferencedTables)
                                                .build())),
                        nextSyntheticColumnId,
                        leftHandle.getAuthorization()),
                ImmutableMap.copyOf(newLeftColumns),
                ImmutableMap.copyOf(newRightColumns),
                precalculateStatisticsForPushdown));
    }

    private static Optional<JdbcColumnHandle> getVariableColumnHandle(Map<String, ColumnHandle> assignments, ConnectorExpression expression)
    {
        requireNonNull(assignments, "assignments is null");
        requireNonNull(expression, "expression is null");
        if (!(expression instanceof Variable)) {
            return Optional.empty();
        }

        String name = ((Variable) expression).getName();
        ColumnHandle columnHandle = assignments.get(name);
        verifyNotNull(columnHandle, "No assignment for %s", name);
        return Optional.of(((JdbcColumnHandle) columnHandle));
    }

    private static PreparedQuery asPreparedQuery(JdbcTableHandle tableHandle)
    {
        checkArgument(
                tableHandle.getConstraint().equals(TupleDomain.all()) &&
                        tableHandle.getLimit().isEmpty() &&
                        tableHandle.getRelationHandle() instanceof JdbcQueryRelationHandle,
                "Handle is not a plain query: %s",
                tableHandle);
        return ((JdbcQueryRelationHandle) tableHandle.getRelationHandle()).getPreparedQuery();
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle table, long limit)
    {
        if (isTableHandleForProcedure(table)) {
            return Optional.empty();
        }

        JdbcTableHandle handle = (JdbcTableHandle) table;

        if (limit > Integer.MAX_VALUE) {
            // Some databases, e.g. Phoenix, Redshift, do not support limit exceeding 2147483647.
            return Optional.empty();
        }

        if (!jdbcClient.supportsLimit()) {
            return Optional.empty();
        }

        if (handle.getLimit().isPresent() && handle.getLimit().getAsLong() <= limit) {
            return Optional.empty();
        }

        handle = new JdbcTableHandle(
                handle.getRelationHandle(),
                handle.getConstraint(),
                handle.getConstraintExpressions(),
                handle.getSortOrder(),
                OptionalLong.of(limit),
                handle.getColumns(),
                handle.getOtherReferencedTables(),
                handle.getNextSyntheticColumnId(),
                handle.getAuthorization());

        return Optional.of(new LimitApplicationResult<>(handle, jdbcClient.isLimitGuaranteed(session), precalculateStatisticsForPushdown));
    }

    @Override
    public Optional<TopNApplicationResult<ConnectorTableHandle>> applyTopN(
            ConnectorSession session,
            ConnectorTableHandle table,
            long topNCount,
            List<SortItem> sortItems,
            Map<String, ColumnHandle> assignments)
    {
        if (isTableHandleForProcedure(table)) {
            return Optional.empty();
        }

        if (!isTopNPushdownEnabled(session)) {
            return Optional.empty();
        }

        verify(!sortItems.isEmpty(), "sortItems are empty");
        JdbcTableHandle handle = (JdbcTableHandle) table;

        List<JdbcSortItem> resultSortOrder = sortItems.stream()
                .map(sortItem -> {
                    verify(assignments.containsKey(sortItem.getName()), "assignments does not contain sortItem: %s", sortItem.getName());
                    return new JdbcSortItem(((JdbcColumnHandle) assignments.get(sortItem.getName())), sortItem.getSortOrder());
                })
                .collect(toImmutableList());

        if (!jdbcClient.supportsTopN(session, handle, resultSortOrder)) {
            // JDBC client implementation prevents TopN pushdown for the given table and sort items
            // e.g. when order by on a given type does not match Trino semantics
            return Optional.empty();
        }

        if (handle.getSortOrder().isPresent() || handle.getLimit().isPresent()) {
            if (handle.getLimit().equals(OptionalLong.of(topNCount)) && handle.getSortOrder().equals(Optional.of(resultSortOrder))) {
                return Optional.empty();
            }

            handle = flushAttributesAsQuery(session, handle);
        }

        JdbcTableHandle sortedTableHandle = new JdbcTableHandle(
                handle.getRelationHandle(),
                handle.getConstraint(),
                handle.getConstraintExpressions(),
                Optional.of(resultSortOrder),
                OptionalLong.of(topNCount),
                handle.getColumns(),
                handle.getOtherReferencedTables(),
                handle.getNextSyntheticColumnId(),
                handle.getAuthorization());

        return Optional.of(new TopNApplicationResult<>(sortedTableHandle, jdbcClient.isTopNGuaranteed(session), precalculateStatisticsForPushdown));
    }

    @Override
    public Optional<TableFunctionApplicationResult<ConnectorTableHandle>> applyTableFunction(ConnectorSession session, ConnectorTableFunctionHandle handle)
    {
        if (handle instanceof QueryFunctionHandle queryFunctionHandle) {
            return Optional.of(getTableFunctionApplicationResult(session, queryFunctionHandle.getTableHandle()));
        }
        if (handle instanceof ProcedureFunctionHandle procedureFunctionHandle) {
            return Optional.of(getTableFunctionApplicationResult(session, procedureFunctionHandle.getTableHandle()));
        }
        return Optional.empty();
    }

    private TableFunctionApplicationResult<ConnectorTableHandle> getTableFunctionApplicationResult(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(getColumnHandles(session, tableHandle).values());
        return new TableFunctionApplicationResult<>(tableHandle, columnHandles);
    }

    @Override
    public Optional<TableScanRedirectApplicationResult> applyTableScanRedirect(ConnectorSession session, ConnectorTableHandle table)
    {
        if (isTableHandleForProcedure(table)) {
            return Optional.empty();
        }

        JdbcTableHandle tableHandle = (JdbcTableHandle) table;
        return jdbcClient.getTableScanRedirection(session, tableHandle);
    }

    @Override
    public SchemaTableName getTableName(ConnectorSession session, ConnectorTableHandle table)
    {
        if (table instanceof JdbcProcedureHandle) {
            // TODO (https://github.com/trinodb/trino/issues/6694) SchemaTableName should not be required for synthetic JdbcProcedureHandle
            return new SchemaTableName("_generated", "_generated_procedure");
        }
        JdbcTableHandle handle = (JdbcTableHandle) table;
        return handle.isNamedRelation()
                ? handle.getRequiredNamedRelation().getSchemaTableName()
                // TODO (https://github.com/trinodb/trino/issues/6694) SchemaTableName should not be required for synthetic ConnectorTableHandle
                : new SchemaTableName("_generated", "_generated_query");
    }

    @Override
    public ConnectorTableSchema getTableSchema(ConnectorSession session, ConnectorTableHandle table)
    {
        JdbcTableHandle handle = (JdbcTableHandle) table;
        return new ConnectorTableSchema(
                handle.getRequiredNamedRelation().getSchemaTableName(),
                jdbcClient.getColumns(session, handle).stream()
                        .map(JdbcColumnHandle::getColumnSchema)
                        .collect(toImmutableList()));
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        JdbcTableHandle handle = (JdbcTableHandle) table;
        return new ConnectorTableMetadata(
                handle.getRequiredNamedRelation().getSchemaTableName(),
                jdbcClient.getColumns(session, handle).stream()
                        .map(JdbcColumnHandle::getColumnMetadata)
                        .collect(toImmutableList()),
                jdbcClient.getTableProperties(session, handle),
                getTableComment(handle));
    }

    public static Optional<String> getTableComment(JdbcTableHandle handle)
    {
        return handle.isNamedRelation() ? handle.getRequiredNamedRelation().getComment() : Optional.empty();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return jdbcClient.getTableNames(session, schemaName);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        if (tableHandle instanceof JdbcProcedureHandle procedureHandle) {
            return procedureHandle.getColumns().orElseThrow().stream()
                    .collect(toImmutableMap(columnHandle -> columnHandle.getColumnMetadata().getName(), identity()));
        }

        return jdbcClient.getColumns(session, (JdbcTableHandle) tableHandle).stream()
                .collect(toImmutableMap(columnHandle -> columnHandle.getColumnMetadata().getName(), identity()));
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        List<SchemaTableName> tables = prefix.toOptionalSchemaTableName()
                .<List<SchemaTableName>>map(ImmutableList::of)
                .orElseGet(() -> listTables(session, prefix.getSchema()));
        for (SchemaTableName tableName : tables) {
            try {
                jdbcClient.getTableHandle(session, tableName)
                        .ifPresent(tableHandle -> columns.put(tableName, getTableMetadata(session, tableHandle).getColumns()));
            }
            catch (TableNotFoundException | AccessDeniedException e) {
                // table disappeared during listing operation or user is not allowed to access it
                // these exceptions are ignored because listTableColumns is used for metadata queries (SELECT FROM information_schema)
            }
        }
        return columns.buildOrThrow();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((JdbcColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        JdbcTableHandle handle = (JdbcTableHandle) tableHandle;
        verify(!handle.isSynthetic(), "Not a table reference: %s", handle);
        jdbcClient.dropTable(session, handle);
    }

    private void verifyRetryMode(ConnectorSession session, RetryMode retryMode)
    {
        if (retryMode != NO_RETRIES) {
            if (!jdbcClient.supportsRetries()) {
                throw new TrinoException(NOT_SUPPORTED, "This connector does not support query or task retries");
            }
            if (isNonTransactionalInsert(session)) {
                throw new TrinoException(NOT_SUPPORTED, "Query and task retries are incompatible with non-transactional inserts");
            }
        }
    }

    @Override
    public Optional<Type> getSupportedType(ConnectorSession session, Type type)
    {
        return jdbcClient.getSupportedType(session, type);
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode)
    {
        verifyRetryMode(session, retryMode);
        JdbcOutputTableHandle handle = jdbcClient.beginCreateTable(session, tableMetadata);
        setRollback(() -> jdbcClient.rollbackCreateTable(session, handle));
        return handle;
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        jdbcClient.createTable(session, tableMetadata);
    }

    private Set<Long> getSuccessfulPageSinkIds(Collection<Slice> fragments)
    {
        return fragments.stream()
                .map(slice -> slice.getLong(0))
                .collect(toImmutableSet());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        JdbcOutputTableHandle handle = (JdbcOutputTableHandle) tableHandle;
        jdbcClient.commitCreateTable(session, handle, getSuccessfulPageSinkIds(fragments));
        return Optional.empty();
    }

    @Override
    public void beginQuery(ConnectorSession session)
    {
        onQueryEvent(jdbcQueryEventListener -> jdbcQueryEventListener.beginQuery(session), "Query begin failed");
    }

    @Override
    public void cleanupQuery(ConnectorSession session)
    {
        onQueryEvent(jdbcQueryEventListener -> jdbcQueryEventListener.cleanupQuery(session), "Query cleanup failed");
    }

    private void onQueryEvent(Consumer<JdbcQueryEventListener> queryEventListenerConsumer, String errorMessage)
    {
        List<RuntimeException> exceptions = new ArrayList<>();
        for (JdbcQueryEventListener jdbcQueryEventListener : jdbcQueryEventListeners) {
            try {
                queryEventListenerConsumer.accept(jdbcQueryEventListener);
            }
            catch (RuntimeException exception) {
                exceptions.add(exception);
            }
        }
        if (!exceptions.isEmpty()) {
            TrinoException trinoException = new TrinoException(JDBC_NON_TRANSIENT_ERROR, errorMessage);
            exceptions.forEach(trinoException::addSuppressed);
            throw trinoException;
        }
    }

    private void setRollback(Runnable action)
    {
        checkState(rollbackAction.compareAndSet(null, action), "rollback action is already set");
    }

    @Override
    public void rollback()
    {
        Optional.ofNullable(rollbackAction.getAndSet(null)).ifPresent(Runnable::run);
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode)
    {
        verify(!((JdbcTableHandle) tableHandle).isSynthetic(), "Not a table reference: %s", tableHandle);
        verifyRetryMode(session, retryMode);
        List<JdbcColumnHandle> columnHandles = columns.stream()
                .map(JdbcColumnHandle.class::cast)
                .collect(toImmutableList());
        JdbcOutputTableHandle handle = jdbcClient.beginInsertTable(session, (JdbcTableHandle) tableHandle, columnHandles);
        setRollback(() -> jdbcClient.rollbackCreateTable(session, handle));
        return handle;
    }

    @Override
    public boolean supportsMissingColumnsOnInsert()
    {
        return true;
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        JdbcOutputTableHandle jdbcInsertHandle = (JdbcOutputTableHandle) tableHandle;
        jdbcClient.finishInsertTable(session, jdbcInsertHandle, getSuccessfulPageSinkIds(fragments));
        return Optional.empty();
    }

    @Override
    public ColumnHandle getMergeRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        verify(!isTableHandleForProcedure(tableHandle), "Not a table reference: %s", tableHandle);
        // The column is used for row-level merge, which is not supported, but it's required during analysis anyway.
        return new JdbcColumnHandle(
                MERGE_ROW_ID,
                new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()),
                BIGINT);
    }

    @Override
    public Optional<ConnectorTableHandle> applyDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        verify(!isTableHandleForProcedure(handle), "Not a table reference: %s", handle);
        return Optional.of(handle);
    }

    @Override
    public OptionalLong executeDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        return jdbcClient.delete(session, (JdbcTableHandle) handle);
    }

    @Override
    public void truncateTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        jdbcClient.truncateTable(session, (JdbcTableHandle) tableHandle);
    }

    @Override
    public void setTableComment(ConnectorSession session, ConnectorTableHandle table, Optional<String> comment)
    {
        JdbcTableHandle tableHandle = (JdbcTableHandle) table;
        verify(!tableHandle.isSynthetic(), "Not a table reference: %s", tableHandle);
        jdbcClient.setTableComment(session, tableHandle, comment);
    }

    @Override
    public void setColumnComment(ConnectorSession session, ConnectorTableHandle table, ColumnHandle column, Optional<String> comment)
    {
        JdbcTableHandle tableHandle = (JdbcTableHandle) table;
        JdbcColumnHandle columnHandle = (JdbcColumnHandle) column;
        verify(!tableHandle.isSynthetic(), "Not a table reference: %s", tableHandle);
        jdbcClient.setColumnComment(session, tableHandle, columnHandle, comment);
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle table, ColumnMetadata columnMetadata)
    {
        JdbcTableHandle tableHandle = (JdbcTableHandle) table;
        verify(!tableHandle.isSynthetic(), "Not a table reference: %s", tableHandle);
        jdbcClient.addColumn(session, tableHandle, columnMetadata);
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle table, ColumnHandle column)
    {
        JdbcTableHandle tableHandle = (JdbcTableHandle) table;
        JdbcColumnHandle columnHandle = (JdbcColumnHandle) column;
        verify(!tableHandle.isSynthetic(), "Not a table reference: %s", tableHandle);
        jdbcClient.dropColumn(session, tableHandle, columnHandle);
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle table, ColumnHandle column, String target)
    {
        JdbcTableHandle tableHandle = (JdbcTableHandle) table;
        JdbcColumnHandle columnHandle = (JdbcColumnHandle) column;
        verify(!tableHandle.isSynthetic(), "Not a table reference: %s", tableHandle);
        jdbcClient.renameColumn(session, tableHandle, columnHandle, target);
    }

    @Override
    public void setColumnType(ConnectorSession session, ConnectorTableHandle table, ColumnHandle column, Type type)
    {
        JdbcTableHandle tableHandle = (JdbcTableHandle) table;
        JdbcColumnHandle columnHandle = (JdbcColumnHandle) column;
        verify(!tableHandle.isSynthetic(), "Not a table reference: %s", tableHandle);
        jdbcClient.setColumnType(session, tableHandle, columnHandle, type);
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle table, SchemaTableName newTableName)
    {
        JdbcTableHandle tableHandle = (JdbcTableHandle) table;
        verify(!tableHandle.isSynthetic(), "Not a table reference: %s", tableHandle);
        jdbcClient.renameTable(session, tableHandle, newTableName);
    }

    @Override
    public void setTableProperties(ConnectorSession session, ConnectorTableHandle table, Map<String, Optional<Object>> properties)
    {
        JdbcTableHandle tableHandle = (JdbcTableHandle) table;
        verify(!tableHandle.isSynthetic(), "Not a table reference: %s", tableHandle);
        jdbcClient.setTableProperties(session, tableHandle, properties);
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        if (isTableHandleForProcedure(tableHandle)) {
            return TableStatistics.empty();
        }
        JdbcTableHandle handle = (JdbcTableHandle) tableHandle;
        return jdbcClient.getTableStatistics(session, handle);
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        jdbcClient.createSchema(session, schemaName);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        jdbcClient.dropSchema(session, schemaName, cascade);
    }

    @Override
    public void renameSchema(ConnectorSession session, String schemaName, String newSchemaName)
    {
        jdbcClient.renameSchema(session, schemaName, newSchemaName);
    }

    @Override
    public OptionalInt getMaxWriterTasks(ConnectorSession session)
    {
        return jdbcClient.getMaxWriteParallelism(session);
    }

    private static boolean isTableHandleForProcedure(ConnectorTableHandle tableHandle)
    {
        return tableHandle instanceof JdbcProcedureHandle;
    }
}
