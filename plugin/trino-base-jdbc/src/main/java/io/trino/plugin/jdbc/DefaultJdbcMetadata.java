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
import io.trino.plugin.jdbc.PredicatePushdownController.DomainPushdownResult;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorNewTableLayout;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayoutHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorTableSchema;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.JoinApplicationResult;
import io.trino.spi.connector.JoinCondition;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.connector.TableScanRedirectApplicationResult;
import io.trino.spi.connector.TopNApplicationResult;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.TableStatistics;

import java.sql.Types;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import static io.trino.plugin.jdbc.JdbcMetadataSessionProperties.isAggregationPushdownEnabled;
import static io.trino.plugin.jdbc.JdbcMetadataSessionProperties.isJoinPushdownEnabled;
import static io.trino.plugin.jdbc.JdbcMetadataSessionProperties.isTopNPushdownEnabled;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

public class DefaultJdbcMetadata
        implements JdbcMetadata
{
    private static final String SYNTHETIC_COLUMN_NAME_PREFIX = "_pfgnrtd_";

    private final JdbcClient jdbcClient;
    private final boolean allowDropTable;

    private final AtomicReference<Runnable> rollbackAction = new AtomicReference<>();

    public DefaultJdbcMetadata(JdbcClient jdbcClient, boolean allowDropTable)
    {
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
        this.allowDropTable = allowDropTable;
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
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        return jdbcClient.getSystemTable(session, tableName);
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle table, Constraint constraint)
    {
        JdbcTableHandle handle = (JdbcTableHandle) table;
        if (handle.getSortOrder().isPresent() && handle.getLimit().isPresent()) {
            handle = flushAttributesAsQuery(session, handle);
        }

        TupleDomain<ColumnHandle> oldDomain = handle.getConstraint();
        TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(constraint.getSummary());

        TupleDomain<ColumnHandle> remainingFilter;
        if (newDomain.isNone()) {
            remainingFilter = TupleDomain.all();
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
        }

        if (oldDomain.equals(newDomain)) {
            return Optional.empty();
        }

        handle = new JdbcTableHandle(
                handle.getRelationHandle(),
                newDomain,
                handle.getSortOrder(),
                handle.getLimit(),
                handle.getColumns(),
                handle.getOtherReferencedTables(),
                handle.getNextSyntheticColumnId());

        return Optional.of(new ConstraintApplicationResult<>(handle, remainingFilter, false));
    }

    private JdbcTableHandle flushAttributesAsQuery(ConnectorSession session, JdbcTableHandle handle)
    {
        List<JdbcColumnHandle> columns = jdbcClient.getColumns(session, handle);
        PreparedQuery preparedQuery = jdbcClient.prepareQuery(session, handle, Optional.empty(), columns, ImmutableMap.of());

        return new JdbcTableHandle(
                new JdbcQueryRelationHandle(preparedQuery),
                TupleDomain.all(),
                Optional.empty(),
                OptionalLong.empty(),
                Optional.of(columns),
                handle.getAllReferencedTables(),
                handle.getNextSyntheticColumnId());
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session,
            ConnectorTableHandle table,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
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

            verify(tableColumnSet.containsAll(newColumnSet), "applyProjection called with columns %s and some are not available in existing query: %s", newColumnSet, tableColumnSet);
        }

        return Optional.of(new ProjectionApplicationResult<>(
                new JdbcTableHandle(
                        handle.getRelationHandle(),
                        handle.getConstraint(),
                        handle.getSortOrder(),
                        handle.getLimit(),
                        Optional.of(newColumns),
                        handle.getOtherReferencedTables(),
                        handle.getNextSyntheticColumnId()),
                projections,
                assignments.entrySet().stream()
                        .map(assignment -> new Assignment(
                                assignment.getKey(),
                                assignment.getValue(),
                                ((JdbcColumnHandle) assignment.getValue()).getColumnType()))
                        .collect(toImmutableList()),
                false));
    }

    @Override
    public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(
            ConnectorSession session,
            ConnectorTableHandle table,
            List<AggregateFunction> aggregates,
            Map<String, ColumnHandle> assignments,
            List<List<ColumnHandle>> groupingSets,
            Set<String> requiredColumns)
    {
        if (!isAggregationPushdownEnabled(session)) {
            return Optional.empty();
        }

        JdbcTableHandle handle = (JdbcTableHandle) table;

        // Global aggregation is represented by [[]]
        verify(!groupingSets.isEmpty(), "No grouping sets provided");

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
        ImmutableMap.Builder<String, String> expressions = ImmutableMap.builder();

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
                .filter(column -> requiredColumns.isEmpty() || requiredColumns.contains(column.getColumnName()))
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
            expressions.put(columnName, expression.get().getExpression());
        }

        List<JdbcColumnHandle> newColumnsList = newColumns.build();

        // TODO(https://github.com/trinodb/trino/issues/9021) We are reading all grouping columns from remote database as at this point we are not able to tell if they are needed up in the query.
        // As a reason of that we need to also have matching column handles in JdbcTableHandle constructed below, as columns read via JDBC must match column handles list.
        // For more context see assertion in JdbcRecordSetProvider.getRecordSet
        PreparedQuery preparedQuery = jdbcClient.prepareQuery(
                session,
                handle,
                Optional.of(groupingSetsAsJdbcColumnHandles),
                newColumnsList,
                expressions.build());
        handle = new JdbcTableHandle(
                new JdbcQueryRelationHandle(preparedQuery),
                TupleDomain.all(),
                Optional.empty(),
                OptionalLong.empty(),
                Optional.of(newColumnsList),
                handle.getAllReferencedTables(),
                nextSyntheticColumnId);

        return Optional.of(new AggregationApplicationResult<>(handle, projections.build(), resultAssignments.build(), ImmutableMap.of(), false));
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
        if (!isJoinPushdownEnabled(session)) {
            return Optional.empty();
        }

        JdbcTableHandle leftHandle = flushAttributesAsQuery(session, (JdbcTableHandle) left);
        JdbcTableHandle rightHandle = flushAttributesAsQuery(session, (JdbcTableHandle) right);
        int nextSyntheticColumnId = max(leftHandle.getNextSyntheticColumnId(), rightHandle.getNextSyntheticColumnId());

        ImmutableMap.Builder<JdbcColumnHandle, JdbcColumnHandle> newLeftColumnsBuilder = ImmutableMap.builder();
        for (JdbcColumnHandle column : jdbcClient.getColumns(session, leftHandle)) {
            newLeftColumnsBuilder.put(column, JdbcColumnHandle.builderFrom(column)
                    .setColumnName(column.getColumnName() + "_" + nextSyntheticColumnId)
                    .build());
            nextSyntheticColumnId++;
        }
        Map<JdbcColumnHandle, JdbcColumnHandle> newLeftColumns = newLeftColumnsBuilder.build();

        ImmutableMap.Builder<JdbcColumnHandle, JdbcColumnHandle> newRightColumnsBuilder = ImmutableMap.builder();
        for (JdbcColumnHandle column : jdbcClient.getColumns(session, rightHandle)) {
            newRightColumnsBuilder.put(column, JdbcColumnHandle.builderFrom(column)
                    .setColumnName(column.getColumnName() + "_" + nextSyntheticColumnId)
                    .build());
            nextSyntheticColumnId++;
        }
        Map<JdbcColumnHandle, JdbcColumnHandle> newRightColumns = newRightColumnsBuilder.build();

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
                        Optional.empty(),
                        OptionalLong.empty(),
                        Optional.of(
                                ImmutableList.<JdbcColumnHandle>builder()
                                        .addAll(newLeftColumns.values())
                                        .addAll(newRightColumns.values())
                                        .build()),
                        ImmutableSet.<SchemaTableName>builder()
                                .addAll(leftHandle.getAllReferencedTables())
                                .addAll(rightHandle.getAllReferencedTables())
                                .build(),
                        nextSyntheticColumnId),
                ImmutableMap.copyOf(newLeftColumns),
                ImmutableMap.copyOf(newRightColumns),
                false));
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
                handle.getSortOrder(),
                OptionalLong.of(limit),
                handle.getColumns(),
                handle.getOtherReferencedTables(),
                handle.getNextSyntheticColumnId());

        return Optional.of(new LimitApplicationResult<>(handle, jdbcClient.isLimitGuaranteed(session), false));
    }

    @Override
    public Optional<TopNApplicationResult<ConnectorTableHandle>> applyTopN(
            ConnectorSession session,
            ConnectorTableHandle table,
            long topNCount,
            List<SortItem> sortItems,
            Map<String, ColumnHandle> assignments)
    {
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
                Optional.of(resultSortOrder),
                OptionalLong.of(topNCount),
                handle.getColumns(),
                handle.getOtherReferencedTables(),
                handle.getNextSyntheticColumnId());

        return Optional.of(new TopNApplicationResult<>(sortedTableHandle, jdbcClient.isTopNGuaranteed(session), false));
    }

    @Override
    public Optional<TableScanRedirectApplicationResult> applyTableScanRedirect(ConnectorSession session, ConnectorTableHandle table)
    {
        JdbcTableHandle tableHandle = (JdbcTableHandle) table;
        return jdbcClient.getTableScanRedirection(session, tableHandle);
    }

    @Override
    public boolean usesLegacyTableLayouts()
    {
        return false;
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        return new ConnectorTableProperties();
    }

    @Override
    public ConnectorTableSchema getTableSchema(ConnectorSession session, ConnectorTableHandle table)
    {
        JdbcTableHandle handle = (JdbcTableHandle) table;

        return new ConnectorTableSchema(
                getSchemaTableName(handle),
                jdbcClient.getColumns(session, handle).stream()
                        .map(JdbcColumnHandle::getColumnSchema)
                        .collect(toImmutableList()));
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        JdbcTableHandle handle = (JdbcTableHandle) table;

        return new ConnectorTableMetadata(
                getSchemaTableName(handle),
                jdbcClient.getColumns(session, handle).stream()
                        .map(JdbcColumnHandle::getColumnMetadata)
                        .collect(toImmutableList()),
                jdbcClient.getTableProperties(session, handle));
    }

    public static SchemaTableName getSchemaTableName(JdbcTableHandle handle)
    {
        return handle.isNamedRelation()
                ? handle.getRequiredNamedRelation().getSchemaTableName()
                // TODO (https://github.com/trinodb/trino/issues/6694) SchemaTableName should not be required for synthetic ConnectorTableHandle
                : new SchemaTableName("_generated", "_generated_query");
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return jdbcClient.getTableNames(session, schemaName);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
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
        return columns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((JdbcColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        if (!allowDropTable) {
            throw new TrinoException(PERMISSION_DENIED, "DROP TABLE is disabled in this catalog");
        }
        JdbcTableHandle handle = (JdbcTableHandle) tableHandle;
        verify(!handle.isSynthetic(), "Not a table reference: %s", handle);
        jdbcClient.dropTable(session, handle);
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        JdbcOutputTableHandle handle = jdbcClient.beginCreateTable(session, tableMetadata);
        setRollback(() -> jdbcClient.rollbackCreateTable(session, handle));
        return handle;
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        jdbcClient.createTable(session, tableMetadata);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        JdbcOutputTableHandle handle = (JdbcOutputTableHandle) tableHandle;
        jdbcClient.commitCreateTable(session, handle);
        return Optional.empty();
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
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns)
    {
        verify(!((JdbcTableHandle) tableHandle).isSynthetic(), "Not a table reference: %s", tableHandle);
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
        jdbcClient.finishInsertTable(session, jdbcInsertHandle);
        return Optional.empty();
    }

    @Override
    public ColumnHandle getDeleteRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        // The column is used for row-level delete, which is not supported, but it's required during analysis anyway.
        return new JdbcColumnHandle(
                "$update_row_id",
                new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()),
                BIGINT);
    }

    @Override
    public ConnectorTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new TrinoException(NOT_SUPPORTED, "Unsupported delete");
    }

    @Override
    public boolean supportsMetadataDelete(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorTableLayoutHandle tableLayoutHandle)
    {
        return true;
    }

    @Override
    public Optional<ConnectorTableHandle> applyDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
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
    public void renameTable(ConnectorSession session, ConnectorTableHandle table, SchemaTableName newTableName)
    {
        JdbcTableHandle tableHandle = (JdbcTableHandle) table;
        verify(!tableHandle.isSynthetic(), "Not a table reference: %s", tableHandle);
        jdbcClient.renameTable(session, tableHandle, newTableName);
    }

    @Override
    public void setTableProperties(ConnectorSession session, ConnectorTableHandle table, Map<String, Object> properties)
    {
        JdbcTableHandle tableHandle = (JdbcTableHandle) table;
        verify(!tableHandle.isSynthetic(), "Not a table reference: %s", tableHandle);
        jdbcClient.setTableProperties(session, tableHandle, properties);
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle, Constraint constraint)
    {
        JdbcTableHandle handle = (JdbcTableHandle) tableHandle;
        return jdbcClient.getTableStatistics(session, handle, constraint.getSummary());
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        jdbcClient.createSchema(session, schemaName);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        jdbcClient.dropSchema(session, schemaName);
    }
}
