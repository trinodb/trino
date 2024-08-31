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
package io.trino.plugin.accumulo;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slice;
import io.trino.plugin.accumulo.metadata.AccumuloTable;
import io.trino.plugin.accumulo.model.AccumuloColumnHandle;
import io.trino.plugin.accumulo.model.AccumuloTableHandle;
import io.trino.spi.TrinoException;
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
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.accumulo.AccumuloErrorCode.ACCUMULO_TABLE_EXISTS;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static io.trino.spi.connector.SaveMode.REPLACE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Trino metadata provider for Accumulo.
 * Responsible for creating/dropping/listing tables, schemas, columns, all sorts of goodness. Heavily leverages {@link AccumuloMetadataManager}.
 */
public class AccumuloMetadata
        implements ConnectorMetadata
{
    private static final JsonCodec<ConnectorViewDefinition> VIEW_CODEC =
            new JsonCodecFactory(new ObjectMapperProvider()).jsonCodec(ConnectorViewDefinition.class);

    private final AccumuloMetadataManager metadataManager;
    private final AtomicReference<Runnable> rollbackAction = new AtomicReference<>();

    @Inject
    public AccumuloMetadata(AccumuloMetadataManager metadataManager)
    {
        this.metadataManager = requireNonNull(metadataManager, "metadataManager is null");
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        checkArgument(properties.isEmpty(), "Can't have properties for schema creation");
        metadataManager.createSchema(schemaName);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        if (cascade) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping schemas with CASCADE option");
        }
        metadataManager.dropSchema(schemaName);
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode, boolean replace)
    {
        if (retryMode != NO_RETRIES) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support query retries");
        }
        if (replace) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support replacing tables");
        }
        if (tableMetadata.getComment().isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with table comment");
        }

        checkNoRollback();

        SchemaTableName tableName = tableMetadata.getTable();
        AccumuloTable table = metadataManager.createTable(tableMetadata);

        AccumuloTableHandle handle = new AccumuloTableHandle(
                tableName.getSchemaName(),
                tableName.getTableName(),
                table.getRowId(),
                table.isExternal(),
                table.getSerializerClassName(),
                table.getScanAuthorizations());

        setRollback(() -> rollbackCreateTable(table));

        return handle;
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        clearRollback();
        return Optional.empty();
    }

    private void rollbackCreateTable(AccumuloTable table)
    {
        metadataManager.dropTable(table);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode)
    {
        if (saveMode == REPLACE) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support replacing tables");
        }
        if (tableMetadata.getComment().isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with table comment");
        }
        metadataManager.createTable(tableMetadata);
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        AccumuloTableHandle handle = (AccumuloTableHandle) tableHandle;
        AccumuloTable table = metadataManager.getTable(handle.toSchemaTableName());
        if (table != null) {
            metadataManager.dropTable(table);
        }
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle,
            SchemaTableName newTableName)
    {
        if (metadataManager.getTable(newTableName) != null) {
            throw new TrinoException(ACCUMULO_TABLE_EXISTS, "Table " + newTableName + " already exists");
        }

        AccumuloTableHandle handle = (AccumuloTableHandle) tableHandle;
        metadataManager.renameTable(handle.toSchemaTableName(), newTableName);
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition, Map<String, Object> viewProperties, boolean replace)
    {
        checkArgument(viewProperties.isEmpty(), "This connector does not support creating views with properties");
        String viewData = VIEW_CODEC.toJson(definition);
        if (replace) {
            metadataManager.createOrReplaceView(viewName, viewData);
        }
        else {
            metadataManager.createView(viewName, viewData);
        }
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        metadataManager.dropView(viewName);
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        return Optional.ofNullable(metadataManager.getView(viewName))
                .map(view -> VIEW_CODEC.fromJson(view.data()));
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        return listViews(schemaName);
    }

    /**
     * Gets all views in the given schema, or all schemas if null.
     *
     * @param filterSchema Schema to filter the views, or absent to list all schemas
     * @return List of views
     */
    private List<SchemaTableName> listViews(Optional<String> filterSchema)
    {
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        if (filterSchema.isPresent()) {
            for (String view : metadataManager.getViewNames(filterSchema.get())) {
                builder.add(new SchemaTableName(filterSchema.get(), view));
            }
        }
        else {
            for (String schemaName : metadataManager.getSchemaNames()) {
                for (String view : metadataManager.getViewNames(schemaName)) {
                    builder.add(new SchemaTableName(schemaName, view));
                }
            }
        }

        return builder.build();
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode)
    {
        if (retryMode != NO_RETRIES) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support query retries");
        }

        checkNoRollback();
        AccumuloTableHandle handle = (AccumuloTableHandle) tableHandle;
        setRollback(() -> rollbackInsert(handle));
        return handle;
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(
            ConnectorSession session,
            ConnectorInsertTableHandle insertHandle,
            List<ConnectorTableHandle> sourceTableHandles,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        clearRollback();
        return Optional.empty();
    }

    private static void rollbackInsert(ConnectorInsertTableHandle insertHandle)
    {
        // Rollbacks for inserts are off the table when it comes to data in Accumulo.
        // When a batch of Mutations fails to be inserted, the general strategy
        // is to run the insert operation again until it is successful
        // Any mutations that were successfully written will be overwritten
        // with the same values, so that isn't a problem.
        AccumuloTableHandle handle = (AccumuloTableHandle) insertHandle;
        throw new TrinoException(NOT_SUPPORTED, format("Unable to rollback insert for table %s.%s. Some rows may have been written. Please run your insert again.", handle.getSchema(), handle.getTable()));
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }

        if (!listSchemaNames(session).contains(tableName.getSchemaName().toLowerCase(Locale.ENGLISH))) {
            return null;
        }

        // Need to validate that SchemaTableName is a table
        if (!this.listViews(session, Optional.of(tableName.getSchemaName())).contains(tableName)) {
            AccumuloTable table = metadataManager.getTable(tableName);
            if (table == null) {
                return null;
            }

            return new AccumuloTableHandle(
                    table.getSchema(),
                    table.getTable(),
                    table.getRowId(),
                    table.isExternal(),
                    table.getSerializerClassName(),
                    table.getScanAuthorizations());
        }

        return null;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        AccumuloTableHandle handle = (AccumuloTableHandle) table;
        SchemaTableName tableName = new SchemaTableName(handle.getSchema(), handle.getTable());
        ConnectorTableMetadata metadata = getTableMetadata(tableName);
        if (metadata == null) {
            throw new TableNotFoundException(tableName);
        }
        return metadata;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        AccumuloTableHandle handle = (AccumuloTableHandle) tableHandle;

        AccumuloTable table = metadataManager.getTable(handle.toSchemaTableName());
        if (table == null) {
            throw new TableNotFoundException(handle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (AccumuloColumnHandle column : table.getColumns()) {
            columnHandles.put(column.name(), column);
        }
        return columnHandles.buildOrThrow();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((AccumuloColumnHandle) columnHandle).columnMetadata();
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        AccumuloTableHandle handle = (AccumuloTableHandle) tableHandle;
        AccumuloColumnHandle columnHandle = (AccumuloColumnHandle) source;
        AccumuloTable table = metadataManager.getTable(handle.toSchemaTableName());
        if (table == null) {
            throw new TableNotFoundException(new SchemaTableName(handle.getSchema(), handle.getTable()));
        }

        metadataManager.renameColumn(table, columnHandle.name(), target);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.copyOf(metadataManager.getSchemaNames());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> filterSchema)
    {
        Set<String> schemaNames = filterSchema.<Set<String>>map(ImmutableSet::of)
                .orElseGet(metadataManager::getSchemaNames);

        ImmutableSet.Builder<SchemaTableName> builder = ImmutableSet.builder();
        for (String schemaName : schemaNames) {
            for (String tableName : metadataManager.getTableNames(schemaName)) {
                builder.add(new SchemaTableName(schemaName, tableName));
            }
        }
        builder.addAll(listViews(session, filterSchema));
        // Deduplicate with set because state may change concurrently
        return builder.build().asList();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.buildOrThrow();
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle table, Constraint constraint)
    {
        AccumuloTableHandle handle = (AccumuloTableHandle) table;

        TupleDomain<ColumnHandle> oldDomain = handle.getConstraint();
        TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(constraint.getSummary());
        if (oldDomain.equals(newDomain)) {
            return Optional.empty();
        }

        handle = new AccumuloTableHandle(
                handle.getSchema(),
                handle.getTable(),
                handle.getRowId(),
                newDomain,
                handle.isExternal(),
                handle.getSerializerClassName(),
                handle.getScanAuthorizations());

        return Optional.of(new ConstraintApplicationResult<>(handle, constraint.getSummary(), constraint.getExpression(), false));
    }

    private void checkNoRollback()
    {
        checkState(rollbackAction.get() == null, "Cannot begin a new write while in an existing one");
    }

    private void setRollback(Runnable action)
    {
        checkState(rollbackAction.compareAndSet(null, action), "Should not have to override existing rollback action");
    }

    private void clearRollback()
    {
        rollbackAction.set(null);
    }

    public void rollback()
    {
        Runnable rollbackAction = this.rollbackAction.getAndSet(null);
        if (rollbackAction != null) {
            rollbackAction.run();
        }
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        if (!metadataManager.getSchemaNames().contains(tableName.getSchemaName())) {
            return null;
        }

        // Need to validate that SchemaTableName is a table
        if (!this.listViews(Optional.ofNullable(tableName.getSchemaName())).contains(tableName)) {
            AccumuloTable table = metadataManager.getTable(tableName);
            if (table == null) {
                return null;
            }

            return new ConnectorTableMetadata(tableName, table.getColumnsMetadata());
        }

        return null;
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        // List all tables if schema or table is null
        if (prefix.getTable().isEmpty()) {
            return listTables(session, prefix.getSchema());
        }

        // Make sure requested table exists, returning the single table of it does
        SchemaTableName table = prefix.toSchemaTableName();
        if (getTableHandle(session, table, Optional.empty(), Optional.empty()) != null) {
            return ImmutableList.of(table);
        }

        // Else, return empty list
        return ImmutableList.of();
    }
}
