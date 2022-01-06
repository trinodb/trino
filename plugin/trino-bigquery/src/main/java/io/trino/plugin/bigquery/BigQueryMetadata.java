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
import io.trino.spi.TrinoException;
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
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.bigquery.BigQueryErrorCode.BIGQUERY_LISTING_DATASET_ERROR;
import static io.trino.plugin.bigquery.BigQueryType.toField;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class BigQueryMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(BigQueryMetadata.class);

    static final int DEFAULT_NUMERIC_TYPE_PRECISION = 38;
    static final int DEFAULT_NUMERIC_TYPE_SCALE = 9;
    static final String INFORMATION_SCHEMA = "information_schema";
    private static final String VIEW_DEFINITION_SYSTEM_TABLE_SUFFIX = "$view_definition";

    private final BigQueryClientFactory bigQueryClientFactory;
    private final Optional<String> configProjectId;

    @Inject
    public BigQueryMetadata(BigQueryClientFactory bigQueryClientFactory, BigQueryConfig config)
    {
        this.bigQueryClientFactory = requireNonNull(bigQueryClientFactory, "bigQueryClientFactory is null");
        this.configProjectId = requireNonNull(config, "config is null").getProjectId();
    }

    protected String getProjectId(BigQueryClient client)
    {
        String projectId = configProjectId.orElse(client.getProjectId());
        checkState(projectId.toLowerCase(ENGLISH).equals(projectId), "projectId must be lowercase but it's " + projectId);
        return projectId;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        log.debug("listSchemaNames(session=%s)", session);
        return listRemoteSchemaNames(session).stream()
                .map(schema -> schema.toLowerCase(ENGLISH))
                .collect(toImmutableList());
    }

    private List<String> listRemoteSchemaNames(ConnectorSession session)
    {
        BigQueryClient client = bigQueryClientFactory.create(session);
        String projectId = getProjectId(client);

        Stream<String> remoteSchemaNames = Streams.stream(client.listDatasets(projectId))
                .map(dataset -> dataset.getDatasetId().getDataset())
                .filter(schemaName -> !schemaName.equalsIgnoreCase(INFORMATION_SCHEMA))
                .distinct();

        // filter out all the ambiguous schemas to prevent failures if anyone tries to access the listed schemas

        return remoteSchemaNames.map(remoteSchema -> client.toRemoteDataset(projectId, remoteSchema.toLowerCase(ENGLISH)))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .filter(dataset -> !dataset.isAmbiguous())
                .map(RemoteDatabaseObject::getOnlyRemoteName)
                .collect(toImmutableList());
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        BigQueryClient client = bigQueryClientFactory.create(session);

        // Overridden to make sure an error message is returned in case of an ambiguous schema name
        log.debug("schemaExists(session=%s)", session);
        String projectId = getProjectId(client);
        return client.toRemoteDataset(projectId, schemaName)
                .map(RemoteDatabaseObject::getOnlyRemoteName)
                .filter(remoteSchema -> client.getDataset(DatasetId.of(projectId, remoteSchema)) != null)
                .isPresent();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        BigQueryClient client = bigQueryClientFactory.create(session);

        log.debug("listTables(session=%s, schemaName=%s)", session, schemaName);
        if (schemaName.isPresent() && schemaName.get().equalsIgnoreCase(INFORMATION_SCHEMA)) {
            return ImmutableList.of();
        }

        String projectId = getProjectId(client);

        // filter ambiguous schemas
        Optional<String> remoteSchema = schemaName.flatMap(schema -> client.toRemoteDataset(projectId, schema)
                .filter(dataset -> !dataset.isAmbiguous())
                .map(RemoteDatabaseObject::getOnlyRemoteName));
        if (remoteSchema.isPresent() && remoteSchema.get().equalsIgnoreCase(INFORMATION_SCHEMA)) {
            return ImmutableList.of();
        }

        Set<String> remoteSchemaNames = remoteSchema.map(ImmutableSet::of)
                .orElseGet(() -> ImmutableSet.copyOf(listRemoteSchemaNames(session)));

        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (String remoteSchemaName : remoteSchemaNames) {
            try {
                Iterable<Table> tables = client.listTables(DatasetId.of(projectId, remoteSchemaName), TABLE, VIEW);
                for (Table table : tables) {
                    // filter ambiguous tables
                    client.toRemoteTable(projectId, remoteSchemaName, table.getTableId().getTable().toLowerCase(ENGLISH), tables)
                            .filter(RemoteDatabaseObject::isAmbiguous)
                            .ifPresentOrElse(
                                    remoteTable -> log.debug("Filtered out [%s.%s] from list of tables due to ambiguous name", remoteSchemaName, table.getTableId().getTable()),
                                    () -> tableNames.add(new SchemaTableName(table.getTableId().getDataset(), table.getTableId().getTable())));
                }
            }
            catch (BigQueryException e) {
                if (e.getCode() == 404 && e.getMessage().contains("Not found: Dataset")) {
                    // Dataset not found error is ignored because listTables is used for metadata queries (SELECT FROM information_schema)
                    log.debug("Dataset disappeared during listing operation: %s", remoteSchemaName);
                }
                else {
                    throw new TrinoException(BIGQUERY_LISTING_DATASET_ERROR, "Exception happened during listing BigQuery dataset: " + remoteSchemaName, e);
                }
            }
        }
        return tableNames.build();
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        BigQueryClient client = bigQueryClientFactory.create(session);
        String projectId = getProjectId(client);
        log.debug("getTableHandle(session=%s, schemaTableName=%s)", session, schemaTableName);
        String remoteSchemaName = client.toRemoteDataset(projectId, schemaTableName.getSchemaName())
                .map(RemoteDatabaseObject::getOnlyRemoteName)
                .orElse(schemaTableName.getSchemaName());
        String remoteTableName = client.toRemoteTable(projectId, remoteSchemaName, schemaTableName.getTableName())
                .map(RemoteDatabaseObject::getOnlyRemoteName)
                .orElse(schemaTableName.getTableName());
        Optional<TableInfo> tableInfo = client.getTable(TableId.of(projectId, remoteSchemaName, remoteTableName));
        if (tableInfo.isEmpty()) {
            log.debug("Table [%s.%s] was not found", schemaTableName.getSchemaName(), schemaTableName.getTableName());
            return null;
        }

        return new BigQueryTableHandle(schemaTableName, new RemoteTableName(tableInfo.get().getTableId()), tableInfo.get());
    }

    private ConnectorTableHandle getTableHandleIgnoringConflicts(ConnectorSession session, SchemaTableName schemaTableName)
    {
        BigQueryClient client = bigQueryClientFactory.create(session);
        String projectId = getProjectId(client);
        String remoteSchemaName = client.toRemoteDataset(projectId, schemaTableName.getSchemaName())
                .map(RemoteDatabaseObject::getAnyRemoteName)
                .orElse(schemaTableName.getSchemaName());
        String remoteTableName = client.toRemoteTable(projectId, remoteSchemaName, schemaTableName.getTableName())
                .map(RemoteDatabaseObject::getAnyRemoteName)
                .orElse(schemaTableName.getTableName());
        Optional<TableInfo> tableInfo = client.getTable(TableId.of(projectId, remoteSchemaName, remoteTableName));
        if (tableInfo.isEmpty()) {
            log.debug("Table [%s.%s] was not found", schemaTableName.getSchemaName(), schemaTableName.getTableName());
            return null;
        }

        return new BigQueryTableHandle(schemaTableName, new RemoteTableName(tableInfo.get().getTableId()), tableInfo.get());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        BigQueryClient client = bigQueryClientFactory.create(session);
        log.debug("getTableMetadata(session=%s, tableHandle=%s)", session, tableHandle);
        BigQueryTableHandle handle = ((BigQueryTableHandle) tableHandle);

        ImmutableList.Builder<ColumnMetadata> columnMetadata = ImmutableList.builder();
        for (BigQueryColumnHandle column : client.getColumns(handle)) {
            columnMetadata.add(column.getColumnMetadata());
        }
        return new ConnectorTableMetadata(handle.getSchemaTableName(), columnMetadata.build());
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        if (isViewDefinitionSystemTable(tableName)) {
            return getViewDefinitionSystemTable(session, tableName, getViewDefinitionSourceTableName(tableName));
        }
        return Optional.empty();
    }

    private Optional<SystemTable> getViewDefinitionSystemTable(ConnectorSession session, SchemaTableName viewDefinitionTableName, SchemaTableName sourceTableName)
    {
        BigQueryClient client = bigQueryClientFactory.create(session);
        String projectId = getProjectId(client);
        String remoteSchemaName = client.toRemoteDataset(projectId, sourceTableName.getSchemaName())
                .map(RemoteDatabaseObject::getOnlyRemoteName)
                .orElseThrow(() -> new TableNotFoundException(viewDefinitionTableName));
        String remoteTableName = client.toRemoteTable(projectId, remoteSchemaName, sourceTableName.getTableName())
                .map(RemoteDatabaseObject::getOnlyRemoteName)
                .orElseThrow(() -> new TableNotFoundException(viewDefinitionTableName));
        TableInfo tableInfo = client.getTable(TableId.of(projectId, remoteSchemaName, remoteTableName))
                .orElseThrow(() -> new TableNotFoundException(viewDefinitionTableName));
        if (!(tableInfo.getDefinition() instanceof ViewDefinition)) {
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
        BigQueryClient client = bigQueryClientFactory.create(session);
        log.debug("getColumnHandles(session=%s, tableHandle=%s)", session, tableHandle);
        return client.getColumns((BigQueryTableHandle) tableHandle).stream()
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
                Optional.ofNullable(getTableHandleIgnoringConflicts(session, tableName))
                        .ifPresent(tableHandle -> columns.put(tableName, getTableMetadata(session, tableHandle).getColumns()));
            }
            catch (TableNotFoundException e) {
                // table disappeared during listing operation
            }
        }
        return columns.build();
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
        BigQueryClient client = bigQueryClientFactory.create(session);
        checkArgument(properties.isEmpty(), "Can't have properties for schema creation");
        DatasetInfo datasetInfo = DatasetInfo.newBuilder(schemaName).build();
        client.createSchema(datasetInfo);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        BigQueryClient client = bigQueryClientFactory.create(session);
        String remoteSchemaName = client.toRemoteDataset(getProjectId(client), schemaName)
                .map(RemoteDatabaseObject::getOnlyRemoteName)
                .orElseThrow(() -> new SchemaNotFoundException(schemaName));
        client.dropSchema(DatasetId.of(remoteSchemaName));
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        try {
            createTable(session, tableMetadata);
        }
        catch (BigQueryException e) {
            if (ignoreExisting && e.getCode() == 409) {
                return;
            }
            throw e;
        }
    }

    private void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
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

        bigQueryClientFactory.create(session).createTable(tableInfo);
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        BigQueryClient client = bigQueryClientFactory.create(session);
        BigQueryTableHandle bigQueryTable = (BigQueryTableHandle) tableHandle;
        TableId tableId = bigQueryTable.getRemoteTableName().toTableId();
        client.dropTable(tableId);
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
