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
package io.trino.plugin.bigquery;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.ViewDefinition;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import io.airlift.log.Logger;
import io.trino.plugin.bigquery.BigQueryClient.RemoteDatabaseObject;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.cloud.bigquery.TableDefinition.Type.TABLE;
import static com.google.cloud.bigquery.TableDefinition.Type.VIEW;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.bigquery.BigQueryType.toField;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class BigQueryMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(BigQueryMetadata.class);

    static final int NUMERIC_DATA_TYPE_PRECISION = 38;
    static final int NUMERIC_DATA_TYPE_SCALE = 9;
    static final String INFORMATION_SCHEMA = "information_schema";
    private static final String VIEW_DEFINITION_SYSTEM_TABLE_SUFFIX = "$view_definition";

    private final BigQueryClient bigQueryClient;
    private final String projectId;

    @Inject
    public BigQueryMetadata(BigQueryClient bigQueryClient, BigQueryConfig config)
    {
        this.bigQueryClient = requireNonNull(bigQueryClient, "bigQueryClient is null");
        this.projectId = requireNonNull(config, "config is null").getProjectId().orElse(bigQueryClient.getProjectId());
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        log.debug("listSchemaNames(session=%s)", session);
        return listRemoteSchemaNames().stream()
                .map(schema -> schema.toLowerCase(ENGLISH))
                .collect(toImmutableList());
    }

    private List<String> listRemoteSchemaNames()
    {
        Stream<String> remoteSchemaNames = Streams.stream(bigQueryClient.listDatasets(projectId))
                .map(dataset -> dataset.getDatasetId().getDataset())
                .filter(schemaName -> !schemaName.equalsIgnoreCase(INFORMATION_SCHEMA))
                .distinct();

        // filter out all the ambiguous schemas to prevent failures if anyone tries to access the listed schemas
        return remoteSchemaNames.map(remoteSchema -> bigQueryClient.toRemoteDataset(projectId, remoteSchema.toLowerCase(ENGLISH)))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .filter(dataset -> !dataset.isAmbiguous())
                .map(RemoteDatabaseObject::getOnlyRemoteName)
                .collect(toImmutableList());
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        // Overridden to make sure an error message is returned in case of an ambiguous schema name
        log.debug("schemaExists(session=%s)", session);
        return bigQueryClient.toRemoteDataset(projectId, schemaName)
                .map(RemoteDatabaseObject::getOnlyRemoteName)
                .filter(remoteSchema -> bigQueryClient.getDataset(DatasetId.of(projectId, remoteSchema)) != null)
                .isPresent();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        log.debug("listTables(session=%s, schemaName=%s)", session, schemaName);
        // filter ambiguous schemas
        Optional<String> remoteSchema = schemaName.flatMap(schema -> bigQueryClient.toRemoteDataset(projectId, schema)
                .filter(dataset -> !dataset.isAmbiguous())
                .map(RemoteDatabaseObject::getOnlyRemoteName));
        if (remoteSchema.isPresent() && remoteSchema.get().equalsIgnoreCase(INFORMATION_SCHEMA)) {
            return ImmutableList.of();
        }

        Set<String> remoteSchemaNames = remoteSchema.map(ImmutableSet::of)
                .orElseGet(() -> ImmutableSet.copyOf(listRemoteSchemaNames()));

        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (String remoteSchemaName : remoteSchemaNames) {
            Iterable<Table> tables = bigQueryClient.listTables(DatasetId.of(projectId, remoteSchemaName), TABLE, VIEW);
            for (Table table : tables) {
                // filter ambiguous tables
                boolean isAmbiguous = bigQueryClient.toRemoteTable(projectId, remoteSchemaName, table.getTableId().getTable().toLowerCase(ENGLISH), tables)
                        .filter(RemoteDatabaseObject::isAmbiguous)
                        .isPresent();
                if (!isAmbiguous) {
                    tableNames.add(new SchemaTableName(table.getTableId().getDataset(), table.getTableId().getTable()));
                }
                else {
                    log.debug("Filtered out [%s.%s] from list of tables due to ambiguous name", remoteSchemaName, table.getTableId().getTable());
                }
            }
        }
        return tableNames.build();
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        log.debug("getTableHandle(session=%s, schemaTableName=%s)", session, schemaTableName);
        String remoteSchemaName = bigQueryClient.toRemoteDataset(projectId, schemaTableName.getSchemaName())
                .map(RemoteDatabaseObject::getOnlyRemoteName)
                .orElse(schemaTableName.getSchemaName());
        String remoteTableName = bigQueryClient.toRemoteTable(projectId, remoteSchemaName, schemaTableName.getTableName())
                .map(RemoteDatabaseObject::getOnlyRemoteName)
                .orElse(schemaTableName.getTableName());
        TableInfo tableInfo = bigQueryClient.getTable(TableId.of(projectId, remoteSchemaName, remoteTableName));
        if (tableInfo == null) {
            log.debug("Table [%s.%s] was not found", schemaTableName.getSchemaName(), schemaTableName.getTableName());
            return null;
        }

        return new BigQueryTableHandle(schemaTableName, new RemoteTableName(tableInfo.getTableId()), tableInfo);
    }

    private ConnectorTableHandle getTableHandleIgnoringConflicts(SchemaTableName schemaTableName)
    {
        String remoteSchemaName = bigQueryClient.toRemoteDataset(projectId, schemaTableName.getSchemaName())
                .map(RemoteDatabaseObject::getAnyRemoteName)
                .orElse(schemaTableName.getSchemaName());
        String remoteTableName = bigQueryClient.toRemoteTable(projectId, remoteSchemaName, schemaTableName.getTableName())
                .map(RemoteDatabaseObject::getAnyRemoteName)
                .orElse(schemaTableName.getTableName());
        TableInfo tableInfo = bigQueryClient.getTable(TableId.of(projectId, remoteSchemaName, remoteTableName));
        if (tableInfo == null) {
            log.debug("Table [%s.%s] was not found", schemaTableName.getSchemaName(), schemaTableName.getTableName());
            return null;
        }

        return new BigQueryTableHandle(schemaTableName, new RemoteTableName(tableInfo.getTableId()), tableInfo);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        log.debug("getTableMetadata(session=%s, tableHandle=%s)", session, tableHandle);
        BigQueryTableHandle handle = ((BigQueryTableHandle) tableHandle);

        ImmutableList.Builder<ColumnMetadata> columnMetadata = ImmutableList.builder();
        for (BigQueryColumnHandle column : bigQueryClient.getColumns(handle)) {
            columnMetadata.add(column.getColumnMetadata());
        }
        return new ConnectorTableMetadata(handle.getSchemaTableName(), columnMetadata.build());
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        if (isViewDefinitionSystemTable(tableName)) {
            return getViewDefinitionSystemTable(tableName, getViewDefinitionSourceTableName(tableName));
        }
        return Optional.empty();
    }

    private Optional<SystemTable> getViewDefinitionSystemTable(SchemaTableName viewDefinitionTableName, SchemaTableName sourceTableName)
    {
        String remoteSchemaName = bigQueryClient.toRemoteDataset(projectId, sourceTableName.getSchemaName())
                .map(RemoteDatabaseObject::getOnlyRemoteName)
                .orElseThrow(() -> new TableNotFoundException(viewDefinitionTableName));
        String remoteTableName = bigQueryClient.toRemoteTable(projectId, remoteSchemaName, sourceTableName.getTableName())
                .map(RemoteDatabaseObject::getOnlyRemoteName)
                .orElseThrow(() -> new TableNotFoundException(viewDefinitionTableName));
        TableInfo tableInfo = bigQueryClient.getTable(TableId.of(projectId, remoteSchemaName, remoteTableName));
        if (tableInfo == null || !(tableInfo.getDefinition() instanceof ViewDefinition)) {
            throw new TableNotFoundException(viewDefinitionTableName);
        }

        List<ColumnMetadata> columns = ImmutableList.of(new ColumnMetadata("query", VarcharType.VARCHAR));
        List<Type> types = columns.stream()
                .map(ColumnMetadata::getType)
                .collect(toImmutableList());
        Optional<String> query = Optional.ofNullable(((ViewDefinition) tableInfo.getDefinition()).getQuery());
        Iterable<List<Object>> propertyValues = ImmutableList.of(ImmutableList.of(query.orElse("NULL")));

        return Optional.of(createSystemTable(new ConnectorTableMetadata(sourceTableName, columns), constraint -> new InMemoryRecordSet(types, propertyValues).cursor()));
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        log.debug("getColumnHandles(session=%s, tableHandle=%s)", session, tableHandle);
        return bigQueryClient.getColumns((BigQueryTableHandle) tableHandle).stream()
                .collect(toImmutableMap(columnHandle -> columnHandle.getColumnMetadata().getName(), identity()));
    }

    @Override
    public ColumnMetadata getColumnMetadata(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ColumnHandle columnHandle)
    {
        log.debug("getColumnMetadata(session=%s, tableHandle=%s, columnHandle=%s)", session, columnHandle, columnHandle);
        return ((BigQueryColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        log.debug("listTableColumns(session=%s, prefix=%s)", session, prefix);
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        List<SchemaTableName> tables = prefix.toOptionalSchemaTableName()
                .<List<SchemaTableName>>map(ImmutableList::of)
                .orElseGet(() -> listTables(session, prefix.getSchema()));
        for (SchemaTableName tableName : tables) {
            try {
                Optional.ofNullable(getTableHandleIgnoringConflicts(tableName))
                        .ifPresent(tableHandle -> columns.put(tableName, getTableMetadata(session, tableHandle).getColumns()));
            }
            catch (TableNotFoundException e) {
                // table disappeared during listing operation
            }
        }
        return columns.build();
    }

    @Override
    public boolean usesLegacyTableLayouts()
    {
        return false;
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        log.debug("getTableProperties(session=%s, prefix=%s)", session, table);
        return new ConnectorTableProperties();
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        checkArgument(properties.isEmpty(), "Can't have properties for schema creation");
        DatasetInfo datasetInfo = DatasetInfo.newBuilder(schemaName).build();
        bigQueryClient.createSchema(datasetInfo);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        String remoteSchemaName = bigQueryClient.toRemoteDataset(projectId, schemaName)
                .map(RemoteDatabaseObject::getOnlyRemoteName)
                .orElseThrow(() -> new SchemaNotFoundException(schemaName));
        bigQueryClient.dropSchema(DatasetId.of(remoteSchemaName));
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        try {
            createTable(tableMetadata);
        }
        catch (BigQueryException e) {
            if (ignoreExisting && e.getCode() == 409) {
                return;
            }
            throw e;
        }
    }

    private void createTable(ConnectorTableMetadata tableMetadata)
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();
        List<Field> fields = tableMetadata.getColumns().stream()
                .map(column -> toField(column.getName(), column.getType()))
                .collect(toImmutableList());

        TableId tableId = TableId.of(schemaName, tableName);
        TableDefinition tableDefinition = StandardTableDefinition.of(Schema.of(fields));
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

        bigQueryClient.createTable(tableInfo);
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        BigQueryTableHandle bigQueryTable = (BigQueryTableHandle) tableHandle;
        TableId tableId = bigQueryTable.getRemoteTableName().toTableId();
        bigQueryClient.dropTable(tableId);
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(
            ConnectorSession session,
            ConnectorTableHandle handle,
            long limit)
    {
        log.debug("applyLimit(session=%s, handle=%s, limit=%s)", session, handle, limit);
        BigQueryTableHandle bigQueryTableHandle = (BigQueryTableHandle) handle;

        if (bigQueryTableHandle.getLimit().isPresent() && bigQueryTableHandle.getLimit().getAsLong() <= limit) {
            return Optional.empty();
        }

        bigQueryTableHandle = bigQueryTableHandle.withLimit(limit);

        return Optional.of(new LimitApplicationResult<>(bigQueryTableHandle, false, false));
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        log.debug("applyProjection(session=%s, handle=%s, projections=%s, assignments=%s)",
                session, handle, projections, assignments);
        BigQueryTableHandle bigQueryTableHandle = (BigQueryTableHandle) handle;

        List<ColumnHandle> newColumns = assignments.values().stream()
                .collect(toImmutableList());

        if (bigQueryTableHandle.getProjectedColumns().isPresent() && containSameElements(newColumns, bigQueryTableHandle.getProjectedColumns().get())) {
            return Optional.empty();
        }

        ImmutableList.Builder<ColumnHandle> projectedColumns = ImmutableList.builder();
        ImmutableList.Builder<Assignment> assignmentList = ImmutableList.builder();
        assignments.forEach((name, column) -> {
            projectedColumns.add(column);
            assignmentList.add(new Assignment(name, column, ((BigQueryColumnHandle) column).getTrinoType()));
        });

        bigQueryTableHandle = bigQueryTableHandle.withProjectedColumns(projectedColumns.build());

        return Optional.of(new ProjectionApplicationResult<>(bigQueryTableHandle, projections, assignmentList.build(), false));
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session,
            ConnectorTableHandle handle,
            Constraint constraint)
    {
        log.debug("applyFilter(session=%s, handle=%s, summary=%s, predicate=%s, columns=%s)",
                session, handle, constraint.getSummary(), constraint.predicate(), constraint.getPredicateColumns());
        BigQueryTableHandle bigQueryTableHandle = (BigQueryTableHandle) handle;

        TupleDomain<ColumnHandle> oldDomain = bigQueryTableHandle.getConstraint();
        TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(constraint.getSummary());
        if (oldDomain.equals(newDomain)) {
            return Optional.empty();
        }

        BigQueryTableHandle updatedHandle = bigQueryTableHandle.withConstraint(newDomain);

        return Optional.of(new ConstraintApplicationResult<>(updatedHandle, constraint.getSummary(), false));
    }

    private static boolean containSameElements(Iterable<? extends ColumnHandle> first, Iterable<? extends ColumnHandle> second)
    {
        return ImmutableSet.copyOf(first).equals(ImmutableSet.copyOf(second));
    }

    private static boolean isViewDefinitionSystemTable(SchemaTableName table)
    {
        return table.getTableName().endsWith(VIEW_DEFINITION_SYSTEM_TABLE_SUFFIX) &&
                (table.getTableName().length() > VIEW_DEFINITION_SYSTEM_TABLE_SUFFIX.length());
    }

    private static SchemaTableName getViewDefinitionSourceTableName(SchemaTableName table)
    {
        return new SchemaTableName(
                table.getSchemaName(),
                table.getTableName().substring(0, table.getTableName().length() - VIEW_DEFINITION_SYSTEM_TABLE_SUFFIX.length()));
    }

    private static SystemTable createSystemTable(ConnectorTableMetadata metadata, Function<TupleDomain<Integer>, RecordCursor> cursor)
    {
        return new SystemTable()
        {
            @Override
            public Distribution getDistribution()
            {
                return Distribution.SINGLE_COORDINATOR;
            }

            @Override
            public ConnectorTableMetadata getTableMetadata()
            {
                return metadata;
            }

            @Override
            public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
            {
                return cursor.apply(constraint);
            }
        };
    }
}
