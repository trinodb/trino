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
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.plugin.mongodb.MongoIndex.MongodbIndexKey;
import io.trino.plugin.mongodb.ptf.Query.QueryFunctionHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnSchema;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorTableSchema;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.LocalProperty;
import io.trino.spi.connector.NotFoundException;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SortingProperty;
import io.trino.spi.connector.TableFunctionApplicationResult;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.ptf.ConnectorTableFunctionHandle;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.type.Type;

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
import static io.trino.plugin.mongodb.TypeUtils.isPushdownSupportedType;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class MongoMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(MongoMetadata.class);

    private static final int MAX_QUALIFIED_IDENTIFIER_BYTE_LENGTH = 120;
    private static final String DELETE_ROW_ID = "_trino_artificial_column_handle_for_delete_row_id_";

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
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        mongoSession.dropSchema(schemaName);
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
        return getTableMetadata(session, tableName);
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
            columnHandles.put(columnHandle.getName(), columnHandle);
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
                columns.put(tableName, getTableMetadata(session, tableName).getColumns());
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
        mongoSession.createTable(tableMetadata.getTable(), buildColumnHandles(tableMetadata), tableMetadata.getComment());
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        MongoTableHandle table = (MongoTableHandle) tableHandle;

        mongoSession.dropTable(table.getSchemaTableName());
    }

    @Override
    public void setTableComment(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> comment)
    {
        MongoTableHandle table = (MongoTableHandle) tableHandle;
        mongoSession.setTableComment(table.getSchemaTableName(), comment);
    }

    @Override
    public void setColumnComment(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle, Optional<String> comment)
    {
        MongoTableHandle table = (MongoTableHandle) tableHandle;
        MongoColumnHandle column = (MongoColumnHandle) columnHandle;
        mongoSession.setColumnComment(table.getSchemaTableName(), column.getName(), comment);
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        if (newTableName.toString().getBytes(UTF_8).length > MAX_QUALIFIED_IDENTIFIER_BYTE_LENGTH) {
            throw new TrinoException(NOT_SUPPORTED, format("Qualified identifier name must be shorter than or equal to '%s' bytes: '%s'", MAX_QUALIFIED_IDENTIFIER_BYTE_LENGTH, newTableName));
        }
        MongoTableHandle table = (MongoTableHandle) tableHandle;
        mongoSession.renameTable(table.getSchemaTableName(), newTableName);
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        mongoSession.addColumn(((MongoTableHandle) tableHandle).getSchemaTableName(), column);
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        mongoSession.dropColumn(((MongoTableHandle) tableHandle).getSchemaTableName(), ((MongoColumnHandle) column).getName());
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode)
    {
        if (retryMode != NO_RETRIES) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support query retries");
        }

        List<MongoColumnHandle> columns = buildColumnHandles(tableMetadata);

        mongoSession.createTable(tableMetadata.getTable(), columns, tableMetadata.getComment());

        setRollback(() -> mongoSession.dropTable(tableMetadata.getTable()));

        return new MongoOutputTableHandle(
                tableMetadata.getTable(),
                columns.stream().filter(c -> !c.isHidden()).collect(toList()));
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        clearRollback();
        return Optional.empty();
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> insertedColumns, RetryMode retryMode)
    {
        if (retryMode != NO_RETRIES) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support query retries");
        }

        MongoTableHandle table = (MongoTableHandle) tableHandle;
        List<MongoColumnHandle> columns = mongoSession.getTable(table.getSchemaTableName()).getColumns();

        return new MongoInsertTableHandle(
                table.getSchemaTableName(),
                columns.stream()
                        .filter(column -> !column.isHidden())
                        .peek(column -> validateColumnNameForInsert(column.getName()))
                        .collect(toImmutableList()));
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    @Override
    public ColumnHandle getDeleteRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        // The column is used for row-level delete, which is not supported, but it's required during analysis anyway.
        return new MongoColumnHandle(DELETE_ROW_ID, BIGINT, true, Optional.empty());
    }

    @Override
    public ConnectorTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle, RetryMode retryMode)
    {
        throw new TrinoException(NOT_SUPPORTED, "Unsupported delete");
    }

    @Override
    public ColumnHandle getMergeRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return new MongoColumnHandle("$merge_row_id", BIGINT, true, Optional.empty());
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
        return OptionalLong.of(mongoSession.deleteDocuments(table.getSchemaTableName(), table.getConstraint()));
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        MongoTableHandle tableHandle = (MongoTableHandle) table;

        Optional<Set<ColumnHandle>> partitioningColumns = Optional.empty(); //TODO: sharding key
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
                partitioningColumns,
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
                new MongoTableHandle(handle.getSchemaTableName(), handle.getFilter(), handle.getConstraint(), OptionalInt.of(toIntExact(limit))),
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
                handle.getFilter(),
                newDomain,
                handle.getLimit());

        return Optional.of(new ConstraintApplicationResult<>(handle, remainingFilter, false));
    }

    @Override
    public Optional<TableFunctionApplicationResult<ConnectorTableHandle>> applyTableFunction(ConnectorSession session, ConnectorTableFunctionHandle handle)
    {
        if (!(handle instanceof QueryFunctionHandle)) {
            return Optional.empty();
        }

        ConnectorTableHandle tableHandle = ((QueryFunctionHandle) handle).getTableHandle();
        ConnectorTableSchema tableSchema = getTableSchema(session, tableHandle);
        Map<String, ColumnHandle> columnHandlesByName = getColumnHandles(session, tableHandle);
        List<ColumnHandle> columnHandles = tableSchema.getColumns().stream()
                .filter(column -> !column.isHidden())
                .map(ColumnSchema::getName)
                .map(columnHandlesByName::get)
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

    private ConnectorTableMetadata getTableMetadata(ConnectorSession session, SchemaTableName tableName)
    {
        MongoTable mongoTable = mongoSession.getTable(tableName);
        MongoTableHandle tableHandle = mongoTable.getTableHandle();

        List<ColumnMetadata> columns =
                getColumnHandles(session, tableHandle).values().stream()
                        .map(MongoColumnHandle.class::cast)
                        .map(MongoColumnHandle::toColumnMetadata)
                .collect(toImmutableList());

        return new ConnectorTableMetadata(tableName, columns, ImmutableMap.of(), mongoTable.getComment());
    }

    private static List<MongoColumnHandle> buildColumnHandles(ConnectorTableMetadata tableMetadata)
    {
        return tableMetadata.getColumns().stream()
                .map(m -> new MongoColumnHandle(m.getName(), m.getType(), m.isHidden(), Optional.ofNullable(m.getComment())))
                .collect(toList());
    }

    private static void validateColumnNameForInsert(String columnName)
    {
        if (columnName.contains("$") || columnName.contains(".")) {
            throw new IllegalArgumentException("Column name must not contain '$' or '.' for INSERT: " + columnName);
        }
    }
}
