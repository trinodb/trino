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
package io.trino.plugin.mongodb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import com.google.common.io.Closer;
import com.mongodb.client.MongoCollection;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.plugin.base.projection.ApplyProjectionUtil;
import io.trino.plugin.mongodb.MongoIndex.MongodbIndexKey;
import io.trino.plugin.mongodb.ptf.Query.QueryFunctionHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.LocalProperty;
import io.trino.spi.connector.NotFoundException;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SortingProperty;
import io.trino.spi.connector.TableFunctionApplicationResult;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.FieldDereference;
import io.trino.spi.expression.Variable;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.RowType.Field;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.bson.Document;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static com.mongodb.client.model.Aggregates.lookup;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.merge;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Filters.ne;
import static com.mongodb.client.model.Projections.exclude;
import static io.trino.plugin.base.TemporaryTables.generateTemporaryTableName;
import static io.trino.plugin.base.projection.ApplyProjectionUtil.ProjectedColumnRepresentation;
import static io.trino.plugin.base.projection.ApplyProjectionUtil.extractSupportedProjectedColumns;
import static io.trino.plugin.base.projection.ApplyProjectionUtil.replaceWithNewVariables;
import static io.trino.plugin.mongodb.MongoSession.COLLECTION_NAME;
import static io.trino.plugin.mongodb.MongoSession.DATABASE_NAME;
import static io.trino.plugin.mongodb.MongoSession.ID;
import static io.trino.plugin.mongodb.MongoSessionProperties.isProjectionPushdownEnabled;
import static io.trino.plugin.mongodb.TypeUtils.isPushdownSupportedType;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;

public class MongoMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(MongoMetadata.class);
    private static final Type TRINO_PAGE_SINK_ID_COLUMN_TYPE = BigintType.BIGINT;

    private static final int MAX_QUALIFIED_IDENTIFIER_BYTE_LENGTH = 120;

    private final MongoSession mongoSession;

    private final AtomicReference<Runnable> rollbackAction = new AtomicReference<>();

    public MongoMetadata(MongoSession mongoSession)
    {
        this.mongoSession = requireNonNull(mongoSession, "mongoSession is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return mongoSession.getAllSchemas();
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        checkArgument(properties.isEmpty(), "Can't have properties for schema creation");
        mongoSession.createSchema(schemaName);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        mongoSession.dropSchema(schemaName, cascade);
    }

    @Override
    public MongoTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        try {
            return mongoSession.getTable(tableName).getTableHandle();
        }
        catch (TableNotFoundException e) {
            log.debug(e, "Table(%s) not found", tableName);
            return null;
        }
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        SchemaTableName tableName = getTableName(tableHandle);
        return getTableMetadata(tableName);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> optionalSchemaName)
    {
        List<String> schemaNames = optionalSchemaName.map(ImmutableList::of)
                .orElseGet(() -> (ImmutableList<String>) listSchemaNames(session));
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (String schemaName : schemaNames) {
            for (String tableName : mongoSession.getAllTables(schemaName)) {
                tableNames.add(new SchemaTableName(schemaName, tableName.toLowerCase(ENGLISH)));
            }
        }
        return tableNames.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        MongoTableHandle table = (MongoTableHandle) tableHandle;
        List<MongoColumnHandle> columns = mongoSession.getTable(table.getSchemaTableName()).getColumns();

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (MongoColumnHandle columnHandle : columns) {
            columnHandles.put(columnHandle.getBaseName().toLowerCase(ENGLISH), columnHandle);
        }
        return columnHandles.buildOrThrow();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            try {
                columns.put(tableName, getTableMetadata(tableName).getColumns());
            }
            catch (NotFoundException e) {
                // table disappeared during listing operation
            }
        }
        return columns.buildOrThrow();
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getTable().isEmpty()) {
            return listTables(session, prefix.getSchema());
        }
        return ImmutableList.of(prefix.toSchemaTableName());
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((MongoColumnHandle) columnHandle).toColumnMetadata();
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        RemoteTableName remoteTableName = mongoSession.toRemoteSchemaTableName(tableMetadata.getTable());
        mongoSession.createTable(remoteTableName, buildColumnHandles(tableMetadata), tableMetadata.getComment());
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        MongoTableHandle table = (MongoTableHandle) tableHandle;

        mongoSession.dropTable(table.getRemoteTableName());
    }

    @Override
    public void setTableComment(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> comment)
    {
        MongoTableHandle table = (MongoTableHandle) tableHandle;
        mongoSession.setTableComment(table, comment);
    }

    @Override
    public void setColumnComment(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle, Optional<String> comment)
    {
        MongoTableHandle table = (MongoTableHandle) tableHandle;
        MongoColumnHandle column = (MongoColumnHandle) columnHandle;
        mongoSession.setColumnComment(table, column.getBaseName(), comment);
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        if (newTableName.toString().getBytes(UTF_8).length > MAX_QUALIFIED_IDENTIFIER_BYTE_LENGTH) {
            throw new TrinoException(NOT_SUPPORTED, format("Qualified identifier name must be shorter than or equal to '%s' bytes: '%s'", MAX_QUALIFIED_IDENTIFIER_BYTE_LENGTH, newTableName));
        }
        MongoTableHandle table = (MongoTableHandle) tableHandle;
        mongoSession.renameTable(table, newTableName);
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        mongoSession.addColumn(((MongoTableHandle) tableHandle), column);
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        mongoSession.renameColumn(((MongoTableHandle) tableHandle), ((MongoColumnHandle) source).getBaseName(), target);
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        mongoSession.dropColumn(((MongoTableHandle) tableHandle), ((MongoColumnHandle) column).getBaseName());
    }

    @Override
    public void setColumnType(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle, Type type)
    {
        MongoTableHandle table = (MongoTableHandle) tableHandle;
        MongoColumnHandle column = (MongoColumnHandle) columnHandle;
        if (!canChangeColumnType(column.getType(), type)) {
            throw new TrinoException(NOT_SUPPORTED, "Cannot change type from %s to %s".formatted(column.getType(), type));
        }
        mongoSession.setColumnType(table, column.getBaseName(), type);
    }

    private static boolean canChangeColumnType(Type sourceType, Type newType)
    {
        if (sourceType.equals(newType)) {
            return true;
        }
        if (sourceType == TINYINT) {
            return newType == SMALLINT || newType == INTEGER || newType == BIGINT;
        }
        if (sourceType == SMALLINT) {
            return newType == INTEGER || newType == BIGINT;
        }
        if (sourceType == INTEGER) {
            return newType == BIGINT;
        }
        if (sourceType == REAL) {
            return newType == DOUBLE;
        }
        if (sourceType instanceof VarcharType || sourceType instanceof CharType) {
            return newType instanceof VarcharType || newType instanceof CharType;
        }
        if (sourceType instanceof DecimalType sourceDecimal && newType instanceof DecimalType newDecimal) {
            return sourceDecimal.getScale() == newDecimal.getScale()
                    && sourceDecimal.getPrecision() <= newDecimal.getPrecision();
        }
        if (sourceType instanceof ArrayType sourceArrayType && newType instanceof ArrayType newArrayType) {
            return canChangeColumnType(sourceArrayType.getElementType(), newArrayType.getElementType());
        }
        if (sourceType instanceof RowType sourceRowType && newType instanceof RowType newRowType) {
            List<Field> fields = Streams.concat(sourceRowType.getFields().stream(), newRowType.getFields().stream())
                    .distinct()
                    .collect(toImmutableList());
            for (Field field : fields) {
                String fieldName = field.getName().orElseThrow();
                if (fieldExists(sourceRowType, fieldName) && fieldExists(newRowType, fieldName)) {
                    if (!canChangeColumnType(
                            findFieldByName(sourceRowType.getFields(), fieldName).getType(),
                            findFieldByName(newRowType.getFields(), fieldName).getType())) {
                        return false;
                    }
                }
            }
            return true;
        }
        return false;
    }

    private static Field findFieldByName(List<Field> fields, String fieldName)
    {
        return fields.stream()
                .filter(field -> field.getName().orElseThrow().equals(fieldName))
                .collect(onlyElement());
    }

    private static boolean fieldExists(RowType structType, String fieldName)
    {
        for (Field field : structType.getFields()) {
            if (field.getName().orElseThrow().equals(fieldName)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode)
    {
        RemoteTableName remoteTableName = mongoSession.toRemoteSchemaTableName(tableMetadata.getTable());

        List<MongoColumnHandle> columns = buildColumnHandles(tableMetadata);

        mongoSession.createTable(remoteTableName, columns, tableMetadata.getComment());

        List<MongoColumnHandle> handleColumns = columns.stream().filter(column -> !column.isHidden()).collect(toImmutableList());

        Closer closer = Closer.create();
        closer.register(() -> mongoSession.dropTable(remoteTableName));
        setRollback(() -> {
            try {
                closer.close();
            }
            catch (IOException e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
            }
        });

        if (retryMode == RetryMode.NO_RETRIES) {
            return new MongoOutputTableHandle(
                    remoteTableName,
                    handleColumns,
                    Optional.empty(),
                    Optional.empty());
        }

        MongoColumnHandle pageSinkIdColumn = buildPageSinkIdColumn(columns.stream().map(MongoColumnHandle::getBaseName).collect(toImmutableSet()));
        List<MongoColumnHandle> allTemporaryTableColumns = ImmutableList.<MongoColumnHandle>builderWithExpectedSize(columns.size() + 1)
                .addAll(columns)
                .add(pageSinkIdColumn)
                .build();
        RemoteTableName temporaryTable = new RemoteTableName(remoteTableName.getDatabaseName(), generateTemporaryTableName(session));
        mongoSession.createTable(temporaryTable, allTemporaryTableColumns, Optional.empty());
        closer.register(() -> mongoSession.dropTable(temporaryTable));

        return new MongoOutputTableHandle(
                remoteTableName,
                handleColumns,
                Optional.of(temporaryTable.getCollectionName()),
                Optional.of(pageSinkIdColumn.getBaseName()));
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        MongoOutputTableHandle handle = (MongoOutputTableHandle) tableHandle;
        if (handle.getTemporaryTableName().isPresent()) {
            finishInsert(session, handle.getRemoteTableName(), handle.getTemporaryRemoteTableName().get(), handle.getPageSinkIdColumnName().get(), fragments);
        }
        clearRollback();
        return Optional.empty();
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> insertedColumns, RetryMode retryMode)
    {
        MongoTable table = mongoSession.getTable(((MongoTableHandle) tableHandle).getSchemaTableName());
        MongoTableHandle handle = table.getTableHandle();
        List<MongoColumnHandle> columns = table.getColumns();
        List<MongoColumnHandle> handleColumns = columns.stream()
                .filter(column -> !column.isHidden())
                .peek(column -> validateColumnNameForInsert(column.getBaseName()))
                .collect(toImmutableList());

        if (retryMode == RetryMode.NO_RETRIES) {
            return new MongoInsertTableHandle(
                    handle.getRemoteTableName(),
                    handleColumns,
                    Optional.empty(),
                    Optional.empty());
        }
        MongoColumnHandle pageSinkIdColumn = buildPageSinkIdColumn(columns.stream().map(MongoColumnHandle::getBaseName).collect(toImmutableSet()));
        List<MongoColumnHandle> allColumns = ImmutableList.<MongoColumnHandle>builderWithExpectedSize(columns.size() + 1)
                .addAll(columns)
                .add(pageSinkIdColumn)
                .build();

        RemoteTableName temporaryTable = new RemoteTableName(handle.getSchemaTableName().getSchemaName(), generateTemporaryTableName(session));
        mongoSession.createTable(temporaryTable, allColumns, Optional.empty());

        setRollback(() -> mongoSession.dropTable(temporaryTable));

        return new MongoInsertTableHandle(
                handle.getRemoteTableName(),
                handleColumns,
                Optional.of(temporaryTable.getCollectionName()),
                Optional.of(pageSinkIdColumn.getBaseName()));
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        MongoInsertTableHandle handle = (MongoInsertTableHandle) insertHandle;
        if (handle.getTemporaryTableName().isPresent()) {
            finishInsert(session, handle.getRemoteTableName(), handle.getTemporaryRemoteTableName().get(), handle.getPageSinkIdColumnName().get(), fragments);
        }
        clearRollback();
        return Optional.empty();
    }

    private void finishInsert(
            ConnectorSession session,
            RemoteTableName targetTable,
            RemoteTableName temporaryTable,
            String pageSinkIdColumnName,
            Collection<Slice> fragments)
    {
        Closer closer = Closer.create();
        closer.register(() -> mongoSession.dropTable(temporaryTable));

        try {
            // Create the temporary page sink ID table
            RemoteTableName pageSinkIdsTable = new RemoteTableName(temporaryTable.getDatabaseName(), generateTemporaryTableName(session));
            MongoColumnHandle pageSinkIdColumn = new MongoColumnHandle(pageSinkIdColumnName, ImmutableList.of(), TRINO_PAGE_SINK_ID_COLUMN_TYPE, false, false, Optional.empty());
            mongoSession.createTable(pageSinkIdsTable, ImmutableList.of(pageSinkIdColumn), Optional.empty());
            closer.register(() -> mongoSession.dropTable(pageSinkIdsTable));

            // Insert all the page sink IDs into the page sink ID table
            MongoCollection<Document> pageSinkIdsCollection = mongoSession.getCollection(pageSinkIdsTable);
            List<Document> pageSinkIds = fragments.stream()
                    .map(slice -> new Document(pageSinkIdColumnName, slice.getLong(0)))
                    .collect(toImmutableList());
            pageSinkIdsCollection.insertMany(pageSinkIds);

            MongoCollection<Document> temporaryCollection = mongoSession.getCollection(temporaryTable);
            temporaryCollection.aggregate(ImmutableList.of(
                    lookup(pageSinkIdsTable.getCollectionName(), pageSinkIdColumnName, pageSinkIdColumnName, "page_sink_id"),
                    match(ne("page_sink_id", ImmutableList.of())),
                    project(exclude("page_sink_id")),
                    merge(targetTable.getCollectionName())))
                    .toCollection();
        }
        finally {
            try {
                closer.close();
            }
            catch (IOException e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
            }
        }
    }

    @Override
    public ColumnHandle getMergeRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return new MongoColumnHandle("$merge_row_id", ImmutableList.of(), BIGINT, true, false, Optional.empty());
    }

    @Override
    public Optional<ConnectorTableHandle> applyDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        return Optional.of(handle);
    }

    @Override
    public OptionalLong executeDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        MongoTableHandle table = (MongoTableHandle) handle;
        return OptionalLong.of(mongoSession.deleteDocuments(table.getRemoteTableName(), table.getConstraint()));
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        MongoTableHandle tableHandle = (MongoTableHandle) table;

        ImmutableList.Builder<LocalProperty<ColumnHandle>> localProperties = ImmutableList.builder();

        MongoTable tableInfo = mongoSession.getTable(tableHandle.getSchemaTableName());
        Map<String, ColumnHandle> columns = getColumnHandles(session, tableHandle);

        for (MongoIndex index : tableInfo.getIndexes()) {
            for (MongodbIndexKey key : index.getKeys()) {
                if (key.getSortOrder().isEmpty()) {
                    continue;
                }
                if (columns.get(key.getName()) != null) {
                    localProperties.add(new SortingProperty<>(columns.get(key.getName()), key.getSortOrder().get()));
                }
            }
        }

        return new ConnectorTableProperties(
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                localProperties.build());
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle table, long limit)
    {
        MongoTableHandle handle = (MongoTableHandle) table;

        // MongoDB cursor.limit(0) is equivalent to setting no limit
        if (limit == 0) {
            return Optional.empty();
        }

        // MongoDB doesn't support limit number greater than integer max
        if (limit > Integer.MAX_VALUE) {
            return Optional.empty();
        }

        if (handle.getLimit().isPresent() && handle.getLimit().getAsInt() <= limit) {
            return Optional.empty();
        }

        return Optional.of(new LimitApplicationResult<>(
                new MongoTableHandle(
                        handle.getSchemaTableName(),
                        handle.getRemoteTableName(),
                        handle.getFilter(),
                        handle.getConstraint(),
                        handle.getProjectedColumns(),
                        OptionalInt.of(toIntExact(limit))),
                true,
                false));
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle table, Constraint constraint)
    {
        MongoTableHandle handle = (MongoTableHandle) table;

        TupleDomain<ColumnHandle> oldDomain = handle.getConstraint();
        TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(constraint.getSummary());
        TupleDomain<ColumnHandle> remainingFilter;
        if (newDomain.isNone()) {
            remainingFilter = TupleDomain.all();
        }
        else {
            Map<ColumnHandle, Domain> domains = newDomain.getDomains().orElseThrow();

            Map<ColumnHandle, Domain> supported = new HashMap<>();
            Map<ColumnHandle, Domain> unsupported = new HashMap<>();

            for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
                MongoColumnHandle columnHandle = (MongoColumnHandle) entry.getKey();
                Domain domain = entry.getValue();
                Type columnType = columnHandle.getType();
                // TODO: Support predicate pushdown on more types including JSON
                if (isPushdownSupportedType(columnType)) {
                    supported.put(entry.getKey(), entry.getValue());
                }
                else {
                    unsupported.put(columnHandle, domain);
                }
            }
            newDomain = TupleDomain.withColumnDomains(supported);
            remainingFilter = TupleDomain.withColumnDomains(unsupported);
        }

        if (oldDomain.equals(newDomain)) {
            return Optional.empty();
        }

        handle = new MongoTableHandle(
                handle.getSchemaTableName(),
                handle.getRemoteTableName(),
                handle.getFilter(),
                newDomain,
                handle.getProjectedColumns(),
                handle.getLimit());

        return Optional.of(new ConstraintApplicationResult<>(handle, remainingFilter, false));
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        if (!isProjectionPushdownEnabled(session)) {
            return Optional.empty();
        }
        // Create projected column representations for supported sub expressions. Simple column references and chain of
        // dereferences on a variable are supported right now.
        Set<ConnectorExpression> projectedExpressions = projections.stream()
                .flatMap(expression -> extractSupportedProjectedColumns(expression, MongoMetadata::isSupportedForPushdown).stream())
                .collect(toImmutableSet());

        Map<ConnectorExpression, ProjectedColumnRepresentation> columnProjections = projectedExpressions.stream()
                .collect(toImmutableMap(identity(), ApplyProjectionUtil::createProjectedColumnRepresentation));

        MongoTableHandle mongoTableHandle = (MongoTableHandle) handle;

        // all references are simple variables
        if (columnProjections.values().stream().allMatch(ProjectedColumnRepresentation::isVariable)) {
            Set<MongoColumnHandle> projectedColumns = assignments.values().stream()
                    .map(MongoColumnHandle.class::cast)
                    .collect(toImmutableSet());
            if (mongoTableHandle.getProjectedColumns().equals(projectedColumns)) {
                return Optional.empty();
            }
            List<Assignment> assignmentsList = assignments.entrySet().stream()
                    .map(assignment -> new Assignment(
                            assignment.getKey(),
                            assignment.getValue(),
                            ((MongoColumnHandle) assignment.getValue()).getType()))
                    .collect(toImmutableList());

            return Optional.of(new ProjectionApplicationResult<>(
                    mongoTableHandle.withProjectedColumns(projectedColumns),
                    projections,
                    assignmentsList,
                    false));
        }

        Map<String, Assignment> newAssignments = new HashMap<>();
        ImmutableMap.Builder<ConnectorExpression, Variable> newVariablesBuilder = ImmutableMap.builder();
        ImmutableSet.Builder<MongoColumnHandle> projectedColumnsBuilder = ImmutableSet.builder();

        for (Map.Entry<ConnectorExpression, ProjectedColumnRepresentation> entry : columnProjections.entrySet()) {
            ConnectorExpression expression = entry.getKey();
            ProjectedColumnRepresentation projectedColumn = entry.getValue();

            MongoColumnHandle baseColumnHandle = (MongoColumnHandle) assignments.get(projectedColumn.getVariable().getName());
            MongoColumnHandle projectedColumnHandle = projectColumn(baseColumnHandle, projectedColumn.getDereferenceIndices(), expression.getType());
            String projectedColumnName = projectedColumnHandle.getQualifiedName();

            Variable projectedColumnVariable = new Variable(projectedColumnName, expression.getType());
            Assignment newAssignment = new Assignment(projectedColumnName, projectedColumnHandle, expression.getType());
            newAssignments.putIfAbsent(projectedColumnName, newAssignment);

            newVariablesBuilder.put(expression, projectedColumnVariable);
            projectedColumnsBuilder.add(projectedColumnHandle);
        }

        // Modify projections to refer to new variables
        Map<ConnectorExpression, Variable> newVariables = newVariablesBuilder.buildOrThrow();
        List<ConnectorExpression> newProjections = projections.stream()
                .map(expression -> replaceWithNewVariables(expression, newVariables))
                .collect(toImmutableList());

        List<Assignment> outputAssignments = newAssignments.values().stream().collect(toImmutableList());
        return Optional.of(new ProjectionApplicationResult<>(
                mongoTableHandle.withProjectedColumns(projectedColumnsBuilder.build()),
                newProjections,
                outputAssignments,
                false));
    }

    private static boolean isSupportedForPushdown(ConnectorExpression connectorExpression)
    {
        if (connectorExpression instanceof Variable) {
            return true;
        }
        if (connectorExpression instanceof FieldDereference fieldDereference) {
            RowType rowType = (RowType) fieldDereference.getTarget().getType();
            if (isDBRefField(rowType)) {
                return false;
            }
            Field field = rowType.getFields().get(fieldDereference.getField());
            if (field.getName().isEmpty()) {
                return false;
            }
            String fieldName = field.getName().get();
            if (fieldName.contains(".") || fieldName.contains("$")) {
                return false;
            }
            return true;
        }
        return false;
    }

    private static MongoColumnHandle projectColumn(MongoColumnHandle baseColumn, List<Integer> indices, Type projectedColumnType)
    {
        if (indices.isEmpty()) {
            return baseColumn;
        }
        ImmutableList.Builder<String> dereferenceNamesBuilder = ImmutableList.builder();
        dereferenceNamesBuilder.addAll(baseColumn.getDereferenceNames());

        Type type = baseColumn.getType();
        RowType parentType = null;
        for (int index : indices) {
            checkArgument(type instanceof RowType, "type should be Row type");
            RowType rowType = (RowType) type;
            Field field = rowType.getFields().get(index);
            dereferenceNamesBuilder.add(field.getName()
                    .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "ROW type does not have field names declared: " + rowType)));
            parentType = rowType;
            type = field.getType();
        }
        return new MongoColumnHandle(
                baseColumn.getBaseName(),
                dereferenceNamesBuilder.build(),
                projectedColumnType,
                baseColumn.isHidden(),
                isDBRefField(parentType),
                baseColumn.getComment());
    }

    /**
     * This method may return a wrong flag when row type use the same field names and types as dbref.
     */
    private static boolean isDBRefField(Type type)
    {
        if (!(type instanceof RowType rowType)) {
            return false;
        }
        requireNonNull(type, "type is null");
        // When projected field is inside DBRef type field
        List<RowType.Field> fields = rowType.getFields();
        if (fields.size() != 3) {
            return false;
        }
        return fields.get(0).getName().orElseThrow().equals(DATABASE_NAME)
                && fields.get(0).getType().equals(VARCHAR)
                && fields.get(1).getName().orElseThrow().equals(COLLECTION_NAME)
                && fields.get(1).getType().equals(VARCHAR)
                && fields.get(2).getName().orElseThrow().equals(ID);
               // Id type can be of any type
    }

    @Override
    public Optional<TableFunctionApplicationResult<ConnectorTableHandle>> applyTableFunction(ConnectorSession session, ConnectorTableFunctionHandle handle)
    {
        if (!(handle instanceof QueryFunctionHandle)) {
            return Optional.empty();
        }

        ConnectorTableHandle tableHandle = ((QueryFunctionHandle) handle).getTableHandle();
        List<ColumnHandle> columnHandles = getColumnHandles(session, tableHandle).values().stream()
                .filter(column -> !((MongoColumnHandle) column).isHidden())
                .collect(toImmutableList());
        return Optional.of(new TableFunctionApplicationResult<>(tableHandle, columnHandles));
    }

    private void setRollback(Runnable action)
    {
        checkState(rollbackAction.compareAndSet(null, action), "rollback action is already set");
    }

    private void clearRollback()
    {
        rollbackAction.set(null);
    }

    public void rollback()
    {
        Optional.ofNullable(rollbackAction.getAndSet(null)).ifPresent(Runnable::run);
    }

    private static SchemaTableName getTableName(ConnectorTableHandle tableHandle)
    {
        return ((MongoTableHandle) tableHandle).getSchemaTableName();
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        MongoTable mongoTable = mongoSession.getTable(tableName);

        List<ColumnMetadata> columns = mongoTable.getColumns().stream()
                .map(MongoColumnHandle::toColumnMetadata)
                .collect(toImmutableList());

        return new ConnectorTableMetadata(tableName, columns, ImmutableMap.of(), mongoTable.getComment());
    }

    private static List<MongoColumnHandle> buildColumnHandles(ConnectorTableMetadata tableMetadata)
    {
        return tableMetadata.getColumns().stream()
                .map(m -> new MongoColumnHandle(m.getName(), ImmutableList.of(), m.getType(), m.isHidden(), false, Optional.ofNullable(m.getComment())))
                .collect(toList());
    }

    private static void validateColumnNameForInsert(String columnName)
    {
        if (columnName.contains("$") || columnName.contains(".")) {
            throw new IllegalArgumentException("Column name must not contain '$' or '.' for INSERT: " + columnName);
        }
    }

    private static MongoColumnHandle buildPageSinkIdColumn(Set<String> otherColumnNames)
    {
        // While it's unlikely this column name will collide with client table columns,
        // guarantee it will not by appending a deterministic suffix to it.
        String baseColumnName = "trino_page_sink_id";
        String columnName = baseColumnName;
        int suffix = 1;
        while (otherColumnNames.contains(columnName)) {
            columnName = baseColumnName + "_" + suffix;
            suffix++;
        }
        return new MongoColumnHandle(columnName, ImmutableList.of(), TRINO_PAGE_SINK_ID_COLUMN_TYPE, false, false, Optional.empty());
    }
}
