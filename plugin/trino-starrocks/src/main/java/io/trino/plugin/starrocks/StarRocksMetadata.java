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
package io.trino.plugin.starrocks;

import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.connector.TopNApplicationResult;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.Objects.requireNonNull;

public class StarRocksMetadata
        implements ConnectorMetadata
{
    private final StarRocksMetadataClient metadataClient;
    private final StarRocksTypeMapper typeMapper;

    @Inject
    public StarRocksMetadata(StarRocksMetadataClient metadataClient, StarRocksTypeMapper typeMapper)
    {
        this.metadataClient = requireNonNull(metadataClient, "metadataClient is null");
        this.typeMapper = requireNonNull(typeMapper, "typeMapper is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return metadataClient.listSchemaNames(session);
    }

    @Override
    public ConnectorTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }

        return metadataClient.getTable(session, tableName)
                .map(remoteTable -> new StarRocksTableHandle(
                        tableName.getSchemaName(),
                        tableName.getTableName(),
                        remoteTable.remoteCatalogName(),
                        remoteTable.remoteSchemaName(),
                        remoteTable.remoteTableName(),
                        remoteTable.relationType()))
                .orElse(null);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> optionalSchemaName)
    {
        return metadataClient.listTables(session, optionalSchemaName);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        StarRocksTableHandle tableHandle = (StarRocksTableHandle) table;
        return getRemoteTable(session, tableHandle)
                .map(this::toTableMetadata)
                .orElse(null);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        StarRocksTableHandle starRocksTableHandle = (StarRocksTableHandle) tableHandle;
        StarRocksRemoteTable remoteTable = getRemoteTable(session, starRocksTableHandle)
                .orElseThrow(() -> new TableNotFoundException(starRocksTableHandle.toSchemaTableName()));

        Map<String, ColumnHandle> columnHandles = new LinkedHashMap<>();
        for (StarRocksColumnHandle columnHandle : toColumnHandles(remoteTable.columns())) {
            ColumnHandle previous = columnHandles.putIfAbsent(columnHandle.columnName(), columnHandle);
            if (previous != null) {
                throw new TrinoException(NOT_SUPPORTED, "Duplicate column after case folding: " + columnHandle.columnName());
            }
        }
        return Collections.unmodifiableMap(columnHandles);
    }

    @SuppressWarnings("deprecation")
    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        Map<SchemaTableName, List<ColumnMetadata>> tableColumns = new LinkedHashMap<>();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            getRemoteTable(session, tableName).ifPresent(remoteTable -> tableColumns.put(tableName, toColumnMetadata(remoteTable.columns())));
        }
        return Collections.unmodifiableMap(tableColumns);
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((StarRocksColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle table)
    {
        StarRocksTableHandle handle = (StarRocksTableHandle) table;
        if (handle.aggregation().isPresent() || !handle.constraint().isAll() || handle.limit().isPresent()) {
            return TableStatistics.empty();
        }

        OptionalLong rowCount = metadataClient.getTableRowCount(session, handle.toSchemaTableName());
        if (rowCount.isEmpty()) {
            return TableStatistics.empty();
        }

        return TableStatistics.builder()
                .setRowCount(Estimate.of((double) rowCount.getAsLong()))
                .build();
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle table, long limit)
    {
        StarRocksTableHandle handle = (StarRocksTableHandle) table;
        if (handle.limit().isPresent() && handle.limit().getAsLong() <= limit) {
            return Optional.empty();
        }
        return Optional.of(new LimitApplicationResult<>(handle.withLimit(limit), true, false));
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle table, Constraint constraint)
    {
        StarRocksTableHandle handle = (StarRocksTableHandle) table;
        if (handle.aggregation().isPresent() || handle.limit().isPresent() || !handle.sortOrder().isEmpty()) {
            return Optional.empty();
        }
        if (constraint.getSummary().isNone()) {
            if (handle.constraint().isNone()) {
                return Optional.empty();
            }
            return Optional.of(new ConstraintApplicationResult<>(
                    handle.withConstraint(TupleDomain.none()),
                    TupleDomain.all(),
                    constraint.getExpression(),
                    false));
        }

        Map<StarRocksColumnHandle, Domain> supported = new LinkedHashMap<>();
        Map<ColumnHandle, Domain> unsupported = new LinkedHashMap<>();
        if (constraint.getSummary().getDomains().isPresent()) {
            for (Map.Entry<ColumnHandle, Domain> entry : constraint.getSummary().getDomains().orElseThrow().entrySet()) {
                if (entry.getKey() instanceof StarRocksColumnHandle columnHandle &&
                        StarRocksQueryBuilder.buildColumnPredicate(columnHandle, entry.getValue()).isPresent()) {
                    supported.put(columnHandle, entry.getValue());
                }
                else {
                    unsupported.put(entry.getKey(), entry.getValue());
                }
            }
        }

        TupleDomain<StarRocksColumnHandle> newDomain = handle.constraint().intersect(TupleDomain.withColumnDomains(supported));
        if (handle.constraint().equals(newDomain)) {
            return Optional.empty();
        }
        return Optional.of(new ConstraintApplicationResult<>(
                handle.withConstraint(newDomain),
                TupleDomain.withColumnDomains(unsupported),
                constraint.getExpression(),
                false));
    }

    @Override
    public Optional<TopNApplicationResult<ConnectorTableHandle>> applyTopN(
            ConnectorSession session,
            ConnectorTableHandle table,
            long topNCount,
            List<SortItem> sortItems,
            Map<String, ColumnHandle> assignments)
    {
        StarRocksTableHandle handle = (StarRocksTableHandle) table;
        List<StarRocksSortItem> sortOrder = toStarRocksSortOrder(sortItems, assignments);
        if (sortOrder.isEmpty()) {
            return Optional.empty();
        }
        if (handle.sortOrder().equals(sortOrder) && handle.limit().isPresent() && handle.limit().getAsLong() <= topNCount) {
            return Optional.empty();
        }
        if (handle.limit().isPresent() || !handle.sortOrder().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new TopNApplicationResult<>(handle.withTopN(topNCount, sortOrder), true, false));
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session,
            ConnectorTableHandle table,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        StarRocksTableHandle handle = (StarRocksTableHandle) table;

        List<StarRocksColumnHandle> projectedColumns = new ArrayList<>(assignments.size());
        List<Assignment> resultAssignments = new ArrayList<>(assignments.size());
        for (Map.Entry<String, ColumnHandle> entry : assignments.entrySet()) {
            if (!(entry.getValue() instanceof StarRocksColumnHandle columnHandle)) {
                return Optional.empty();
            }
            projectedColumns.add(columnHandle);
            resultAssignments.add(new Assignment(entry.getKey(), columnHandle, columnHandle.columnType()));
        }

        if (handle.projectedColumns().isPresent() && containSameElements(handle.projectedColumns().orElseThrow(), projectedColumns)) {
            return Optional.empty();
        }

        return Optional.of(new ProjectionApplicationResult<>(
                handle.withProjectedColumns(projectedColumns),
                projections,
                resultAssignments,
                false));
    }

    @Override
    public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(
            ConnectorSession session,
            ConnectorTableHandle table,
            List<AggregateFunction> aggregates,
            Map<String, ColumnHandle> assignments,
            List<List<ColumnHandle>> groupingSets)
    {
        StarRocksTableHandle handle = (StarRocksTableHandle) table;
        if (handle.aggregation().isPresent() || handle.limit().isPresent() || !handle.sortOrder().isEmpty()) {
            return Optional.empty();
        }
        if (groupingSets.size() != 1) {
            return Optional.empty();
        }

        List<StarRocksColumnHandle> groupingColumns = new ArrayList<>();
        for (ColumnHandle groupingColumn : groupingSets.getFirst()) {
            if (!(groupingColumn instanceof StarRocksColumnHandle starRocksColumnHandle)) {
                return Optional.empty();
            }
            groupingColumns.add(starRocksColumnHandle);
        }

        List<ConnectorExpression> projections = new ArrayList<>(aggregates.size());
        List<Assignment> resultAssignments = new ArrayList<>(aggregates.size());
        List<StarRocksAggregateColumn> aggregateColumns = new ArrayList<>(aggregates.size());
        for (int index = 0; index < aggregates.size(); index++) {
            AggregateFunction aggregate = aggregates.get(index);
            Optional<String> aggregateExpression = toAggregationExpression(aggregate, assignments);
            if (aggregateExpression.isEmpty()) {
                return Optional.empty();
            }

            String columnName = "_starrocks_agg_" + index;
            StarRocksAggregateColumn aggregateColumn = new StarRocksAggregateColumn(columnName, aggregateExpression.orElseThrow(), aggregate.getOutputType());
            StarRocksColumnHandle columnHandle = new StarRocksColumnHandle(columnName, columnName, aggregate.getOutputType(), groupingColumns.size() + index);
            aggregateColumns.add(aggregateColumn);
            projections.add(new Variable(columnName, aggregate.getOutputType()));
            resultAssignments.add(new Assignment(columnName, columnHandle, aggregate.getOutputType()));
        }

        return Optional.of(new AggregationApplicationResult<>(
                handle.withAggregation(new StarRocksAggregation(groupingColumns, aggregateColumns)),
                projections,
                resultAssignments,
                Map.of(),
                false));
    }

    private Optional<StarRocksRemoteTable> getRemoteTable(ConnectorSession session, StarRocksTableHandle tableHandle)
    {
        return getRemoteTable(session, tableHandle.toSchemaTableName());
    }

    private Optional<StarRocksRemoteTable> getRemoteTable(ConnectorSession session, SchemaTableName tableName)
    {
        return metadataClient.getTable(session, tableName);
    }

    private ConnectorTableMetadata toTableMetadata(StarRocksRemoteTable remoteTable)
    {
        return new ConnectorTableMetadata(remoteTable.schemaTableName(), toColumnMetadata(remoteTable.columns()));
    }

    private List<ColumnMetadata> toColumnMetadata(List<StarRocksRemoteColumn> columns)
    {
        List<ColumnMetadata> columnMetadata = new ArrayList<>(columns.size());
        for (StarRocksRemoteColumn column : columns) {
            if (!isReadableColumn(column)) {
                continue;
            }
            columnMetadata.add(new ColumnMetadata(column.columnName(), typeMapper.toTrinoType(column)));
        }
        return List.copyOf(columnMetadata);
    }

    private List<StarRocksColumnHandle> toColumnHandles(List<StarRocksRemoteColumn> columns)
    {
        List<StarRocksColumnHandle> columnHandles = new ArrayList<>(columns.size());
        for (StarRocksRemoteColumn column : columns) {
            if (!isReadableColumn(column)) {
                continue;
            }
            columnHandles.add(new StarRocksColumnHandle(
                    column.columnName(),
                    column.remoteColumnName(),
                    typeMapper.toTrinoType(column),
                    column.ordinalPosition() - 1));
        }
        return List.copyOf(columnHandles);
    }

    private static boolean isReadableColumn(StarRocksRemoteColumn column)
    {
        return !remoteBaseType(column).equals("HLL");
    }

    private static String remoteBaseType(StarRocksRemoteColumn column)
    {
        String typeDeclaration = column.typeDefinition()
                .orElse(column.typeName())
                .trim();
        int parenthesisStart = typeDeclaration.indexOf('(');
        int angleBracketStart = typeDeclaration.indexOf('<');
        int typeParameterStart = parenthesisStart < 0 ? angleBracketStart : (angleBracketStart < 0 ? parenthesisStart : Math.min(parenthesisStart, angleBracketStart));
        if (typeParameterStart >= 0) {
            typeDeclaration = typeDeclaration.substring(0, typeParameterStart);
        }
        return typeDeclaration.trim()
                .toUpperCase(Locale.ENGLISH)
                .replace(' ', '_');
    }

    private static List<StarRocksSortItem> toStarRocksSortOrder(List<SortItem> sortItems, Map<String, ColumnHandle> assignments)
    {
        List<StarRocksSortItem> sortOrder = new ArrayList<>(sortItems.size());
        for (SortItem sortItem : sortItems) {
            ColumnHandle columnHandle = assignments.get(sortItem.getName());
            if (!(columnHandle instanceof StarRocksColumnHandle starRocksColumnHandle) || !isTopNPushdownSupported(starRocksColumnHandle.columnType())) {
                return List.of();
            }
            sortOrder.add(new StarRocksSortItem(starRocksColumnHandle.columnName(), starRocksColumnHandle.remoteColumnName(), sortItem.getSortOrder()));
        }
        return List.copyOf(sortOrder);
    }

    private static Optional<String> toAggregationExpression(AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        if (aggregate.isDistinct() ||
                aggregate.getFilter().isPresent() ||
                !aggregate.getSortItems().isEmpty()) {
            return Optional.empty();
        }
        String functionName = aggregate.getFunctionName().toLowerCase(Locale.ENGLISH);
        if (functionName.equals("count")) {
            return toCountAggregationExpression(aggregate, assignments);
        }
        if (!List.of("avg", "max", "min", "sum").contains(functionName)) {
            return Optional.empty();
        }
        if (aggregate.getArguments().size() != 1) {
            return Optional.empty();
        }

        ConnectorExpression argument = aggregate.getArguments().getFirst();
        if (!(argument instanceof Variable variable)) {
            return Optional.empty();
        }
        ColumnHandle columnHandle = assignments.get(variable.getName());
        if (!(columnHandle instanceof StarRocksColumnHandle starRocksColumnHandle)) {
            return Optional.empty();
        }
        if (functionName.equals("avg") && !isAverageAggregationSupported(starRocksColumnHandle.columnType())) {
            return Optional.empty();
        }
        if (functionName.equals("sum") && !isSumAggregationSupported(starRocksColumnHandle.columnType())) {
            return Optional.empty();
        }
        if (List.of("max", "min").contains(functionName) && !isMinMaxAggregationSupported(starRocksColumnHandle.columnType())) {
            return Optional.empty();
        }

        return Optional.of(functionName + "(" + StarRocksQueryBuilder.quoteIdentifier(starRocksColumnHandle.remoteColumnName()) + ")");
    }

    private static Optional<String> toCountAggregationExpression(AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        if (aggregate.getArguments().isEmpty()) {
            return Optional.of("count(*)");
        }
        if (aggregate.getArguments().size() != 1) {
            return Optional.empty();
        }

        ConnectorExpression argument = aggregate.getArguments().getFirst();
        if (argument instanceof Variable variable) {
            ColumnHandle columnHandle = assignments.get(variable.getName());
            if (columnHandle instanceof StarRocksColumnHandle starRocksColumnHandle) {
                return Optional.of("count(" + StarRocksQueryBuilder.quoteIdentifier(starRocksColumnHandle.remoteColumnName()) + ")");
            }
            return Optional.empty();
        }
        if (argument instanceof Constant constant) {
            if (constant.getValue() == null) {
                return Optional.of("count(NULL)");
            }
            return Optional.of("count(*)");
        }
        return Optional.empty();
    }

    private static boolean isTopNPushdownSupported(Type type)
    {
        return isNumericType(type) ||
                type == DATE ||
                type instanceof TimestampType;
    }

    private static boolean isAverageAggregationSupported(Type type)
    {
        return isIntegerType(type) ||
                type == REAL ||
                type == DOUBLE;
    }

    private static boolean isSumAggregationSupported(Type type)
    {
        return isIntegerType(type) ||
                type == REAL ||
                type == DOUBLE;
    }

    private static boolean isMinMaxAggregationSupported(Type type)
    {
        return isNumericType(type) ||
                type == DATE ||
                type instanceof TimestampType;
    }

    private static boolean isNumericType(Type type)
    {
        return isIntegerType(type) ||
                type == REAL ||
                type == DOUBLE ||
                type instanceof DecimalType;
    }

    private static boolean isIntegerType(Type type)
    {
        return type == TINYINT ||
                type == SMALLINT ||
                type == INTEGER ||
                type == BIGINT;
    }

    private static boolean containSameElements(List<StarRocksColumnHandle> left, List<StarRocksColumnHandle> right)
    {
        return left.size() == right.size() && new HashSet<>(left).equals(new HashSet<>(right));
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getTable().isPresent()) {
            return List.of(prefix.toSchemaTableName());
        }
        return listTables(session, prefix.getSchema());
    }
}
