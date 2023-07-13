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
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
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
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.plugin.bigquery.BigQueryClient.RemoteDatabaseObject;
import io.trino.plugin.bigquery.BigQueryTableHandle.BigQueryPartitionType;
import io.trino.plugin.bigquery.ptf.Query.QueryHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorAnalyzeMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableFunctionApplicationResult;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.util.concurrent.Futures.allAsList;
import static io.trino.plugin.base.TemporaryTables.generateTemporaryTableName;
import static io.trino.plugin.bigquery.BigQueryClient.buildColumnHandles;
import static io.trino.plugin.bigquery.BigQueryErrorCode.BIGQUERY_FAILED_TO_EXECUTE_QUERY;
import static io.trino.plugin.bigquery.BigQueryErrorCode.BIGQUERY_LISTING_DATASET_ERROR;
import static io.trino.plugin.bigquery.BigQueryErrorCode.BIGQUERY_UNSUPPORTED_OPERATION;
import static io.trino.plugin.bigquery.BigQueryPseudoColumn.PARTITION_DATE;
import static io.trino.plugin.bigquery.BigQueryPseudoColumn.PARTITION_TIME;
import static io.trino.plugin.bigquery.BigQueryTableHandle.BigQueryPartitionType.INGESTION;
import static io.trino.plugin.bigquery.BigQueryTableHandle.getPartitionType;
import static io.trino.plugin.bigquery.BigQueryType.toField;
import static io.trino.plugin.bigquery.BigQueryUtil.isWildcardTable;
import static io.trino.plugin.bigquery.BigQueryUtil.quote;
import static io.trino.plugin.bigquery.BigQueryUtil.quoted;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class BigQueryMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(BigQueryMetadata.class);
    private static final Type TRINO_PAGE_SINK_ID_COLUMN_TYPE = BigintType.BIGINT;

    static final int DEFAULT_NUMERIC_TYPE_PRECISION = 38;
    static final int DEFAULT_NUMERIC_TYPE_SCALE = 9;
    private static final String VIEW_DEFINITION_SYSTEM_TABLE_SUFFIX = "$view_definition";

    private final BigQueryClientFactory bigQueryClientFactory;
    private final AtomicReference<Runnable> rollbackAction = new AtomicReference<>();
    private final ListeningExecutorService executorService;

    public BigQueryMetadata(BigQueryClientFactory bigQueryClientFactory, ListeningExecutorService executorService)
    {
        this.bigQueryClientFactory = requireNonNull(bigQueryClientFactory, "bigQueryClientFactory is null");
        this.executorService = requireNonNull(executorService, "executorService is null");
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
        String projectId = client.getProjectId();

        Stream<String> remoteSchemaNames = Streams.stream(client.listDatasets(projectId))
                .map(dataset -> dataset.getDatasetId().getDataset())
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
        String projectId = client.getProjectId();
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
        String projectId = client.getProjectId();

        // filter ambiguous schemas
        Optional<String> remoteSchema = schemaName.flatMap(schema -> client.toRemoteDataset(projectId, schema)
                .filter(dataset -> !dataset.isAmbiguous())
                .map(RemoteDatabaseObject::getOnlyRemoteName));

        Set<String> remoteSchemaNames = remoteSchema.map(ImmutableSet::of)
                .orElseGet(() -> ImmutableSet.copyOf(listRemoteSchemaNames(session)));

        return processInParallel(remoteSchemaNames.stream().toList(), remoteSchemaName -> listTablesInRemoteSchema(client, projectId, remoteSchemaName))
                .flatMap(Collection::stream)
                .collect(toImmutableList());
    }

    private List<SchemaTableName> listTablesInRemoteSchema(BigQueryClient client, String projectId, String remoteSchemaName)
    {
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        try {
            Iterable<Table> tables = client.listTables(DatasetId.of(projectId, remoteSchemaName));
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
        return tableNames.build();
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        BigQueryClient client = bigQueryClientFactory.create(session);
        String projectId = client.getProjectId();
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

        ImmutableList.Builder<BigQueryColumnHandle> columns = ImmutableList.builder();
        columns.addAll(buildColumnHandles(tableInfo.get()));
        Optional<BigQueryPartitionType> partitionType = getPartitionType(tableInfo.get().getDefinition());
        if (partitionType.isPresent() && partitionType.get() == INGESTION) {
            columns.add(PARTITION_DATE.getColumnHandle());
            columns.add(PARTITION_TIME.getColumnHandle());
        }
        return new BigQueryTableHandle(new BigQueryNamedRelationHandle(
                schemaTableName,
                new RemoteTableName(tableInfo.get().getTableId()),
                tableInfo.get().getDefinition().getType().toString(),
                partitionType,
                Optional.ofNullable(tableInfo.get().getDescription())))
                .withProjectedColumns(columns.build());
    }

    private ConnectorTableHandle getTableHandleIgnoringConflicts(ConnectorSession session, SchemaTableName schemaTableName)
    {
        BigQueryClient client = bigQueryClientFactory.create(session);
        String projectId = client.getProjectId();
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

        return new BigQueryTableHandle(new BigQueryNamedRelationHandle(
                schemaTableName,
                new RemoteTableName(tableInfo.get().getTableId()),
                tableInfo.get().getDefinition().getType().toString(),
                getPartitionType(tableInfo.get().getDefinition()),
                Optional.ofNullable(tableInfo.get().getDescription())));
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        BigQueryClient client = bigQueryClientFactory.create(session);
        log.debug("getTableMetadata(session=%s, tableHandle=%s)", session, tableHandle);
        BigQueryTableHandle handle = ((BigQueryTableHandle) tableHandle);

        List<ColumnMetadata> columns = client.getColumns(handle).stream()
                .map(BigQueryColumnHandle::getColumnMetadata)
                .collect(toImmutableList());
        return new ConnectorTableMetadata(getSchemaTableName(handle), columns, ImmutableMap.of(), getTableComment(handle));
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
        String projectId = client.getProjectId();
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

        BigQueryTableHandle table = (BigQueryTableHandle) tableHandle;
        if (table.getProjectedColumns().isPresent()) {
            return table.getProjectedColumns().get().stream()
                    .collect(toImmutableMap(columnHandle -> columnHandle.getColumnMetadata().getName(), identity()));
        }

        checkArgument(table.isNamedRelation(), "Cannot get columns for %s", tableHandle);

        ImmutableList.Builder<BigQueryColumnHandle> columns = ImmutableList.builder();
        columns.addAll(client.getColumns(table));
        if (table.asPlainTable().getPartitionType().isPresent() && table.asPlainTable().getPartitionType().get() == INGESTION) {
            columns.add(PARTITION_DATE.getColumnHandle());
            columns.add(PARTITION_TIME.getColumnHandle());
        }
        return columns.build().stream()
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
        List<SchemaTableName> tables = prefix.toOptionalSchemaTableName()
                .<List<SchemaTableName>>map(ImmutableList::of)
                .orElseGet(() -> listTables(session, prefix.getSchema()));

        List<BigQueryTableHandle> tableHandles = processInParallel(tables, table -> getTableHandleIgnoringConflicts(session, table))
                .filter(Objects::nonNull)
                .map(BigQueryTableHandle.class::cast)
                .collect(toImmutableList());

        return processInParallel(tableHandles, tableHandle -> safeGetTableMetadata(session, tableHandle))
                .filter(Objects::nonNull)
                .collect(toImmutableMap(ConnectorTableMetadata::getTable, ConnectorTableMetadata::getColumns));
    }

    @Nullable
    private ConnectorTableMetadata safeGetTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        try {
            return getTableMetadata(session, tableHandle);
        }
        catch (TableNotFoundException e) {
            // table disappeared during listing operation
            return null;
        }
    }

    protected <T, R> Stream<R> processInParallel(List<T> list, Function<T, R> function)
    {
        if (list.size() == 1) {
            return Stream.of(function.apply(list.get(0)));
        }

        List<ListenableFuture<R>> futures = list.stream()
                .map(element -> executorService.submit(() -> function.apply(element)))
                .collect(toImmutableList());
        try {
            return allAsList(futures).get().stream();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        BigQueryClient client = bigQueryClientFactory.create(session);
        checkArgument(properties.isEmpty(), "Can't have properties for schema creation");
        DatasetInfo datasetInfo = DatasetInfo.newBuilder(client.getProjectId(), schemaName).build();
        client.createSchema(datasetInfo);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        BigQueryClient client = bigQueryClientFactory.create(session);
        String projectId = client.getProjectId();
        String remoteSchemaName = getRemoteSchemaName(client, projectId, schemaName);
        client.dropSchema(DatasetId.of(projectId, remoteSchemaName));
    }

    private void setRollback(Runnable action)
    {
        checkState(rollbackAction.compareAndSet(null, action), "rollback action is already set");
    }

    public void rollback()
    {
        Optional.ofNullable(rollbackAction.getAndSet(null)).ifPresent(Runnable::run);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        try {
            createTable(session, tableMetadata, Optional.empty());
        }
        catch (BigQueryException e) {
            if (ignoreExisting && e.getCode() == 409) {
                return;
            }
            throw e;
        }
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode)
    {
        ColumnMetadata pageSinkIdColumn = buildPageSinkIdColumn(tableMetadata.getColumns().stream()
                .map(ColumnMetadata::getName)
                .collect(toImmutableList()));
        return createTable(session, tableMetadata, Optional.of(pageSinkIdColumn));
    }

    private BigQueryOutputTableHandle createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ColumnMetadata> pageSinkIdColumn)
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        if (!schemaExists(session, schemaName)) {
            throw new SchemaNotFoundException(schemaName);
        }

        int columnSize = tableMetadata.getColumns().size();
        ImmutableList.Builder<Field> fields = ImmutableList.builderWithExpectedSize(columnSize);
        // Note: this list is only actually used when pageSinkIdColumn isPresent
        ImmutableList.Builder<Field> tempFields = ImmutableList.builderWithExpectedSize(columnSize + 1);
        ImmutableList.Builder<String> columnsNames = ImmutableList.builderWithExpectedSize(columnSize);
        ImmutableList.Builder<Type> columnsTypes = ImmutableList.builderWithExpectedSize(columnSize);
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            fields.add(toField(column.getName(), column.getType(), column.getComment()));
            tempFields.add(toField(column.getName(), column.getType(), column.getComment()));
            columnsNames.add(column.getName());
            columnsTypes.add(column.getType());
        }

        BigQueryClient client = bigQueryClientFactory.create(session);
        String projectId = client.getProjectId();
        String remoteSchemaName = getRemoteSchemaName(client, projectId, schemaName);

        Closer closer = Closer.create();
        setRollback(() -> {
            try {
                closer.close();
            }
            catch (IOException e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
            }
        });

        TableId tableId = createTable(client, projectId, remoteSchemaName, tableName, fields.build(), tableMetadata.getComment());
        closer.register(() -> bigQueryClientFactory.create(session).dropTable(tableId));

        Optional<String> temporaryTableName = pageSinkIdColumn.map(column -> {
            tempFields.add(toField(column.getName(), column.getType(), column.getComment()));
            String tempTableName = generateTemporaryTableName(session);
            TableId tempTableId = createTable(client, projectId, remoteSchemaName, tempTableName, tempFields.build(), tableMetadata.getComment());
            closer.register(() -> bigQueryClientFactory.create(session).dropTable(tempTableId));
            return tempTableName;
        });

        return new BigQueryOutputTableHandle(
                new RemoteTableName(tableId),
                columnsNames.build(),
                columnsTypes.build(),
                temporaryTableName,
                pageSinkIdColumn.map(ColumnMetadata::getName));
    }

    private TableId createTable(BigQueryClient client, String projectId, String datasetName, String tableName, List<Field> fields, Optional<String> tableComment)
    {
        TableId tableId = TableId.of(projectId, datasetName, tableName);
        TableDefinition tableDefinition = StandardTableDefinition.of(Schema.of(fields));
        TableInfo.Builder tableInfo = TableInfo.newBuilder(tableId, tableDefinition);
        tableComment.ifPresent(tableInfo::setDescription);

        client.createTable(tableInfo.build());

        return tableId;
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        BigQueryOutputTableHandle handle = (BigQueryOutputTableHandle) tableHandle;
        checkState(handle.getTemporaryTableName().isPresent(), "Unexpected use of finishCreateTable without a temporaryTableName present");
        return finishInsert(session, handle.getRemoteTableName(), handle.getTemporaryRemoteTableName().orElseThrow(), handle.getPageSinkIdColumnName().orElseThrow(), handle.getColumnNames(), fragments);
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        BigQueryClient client = bigQueryClientFactory.create(session);
        BigQueryTableHandle bigQueryTable = (BigQueryTableHandle) tableHandle;
        if (isWildcardTable(TableDefinition.Type.valueOf(bigQueryTable.asPlainTable().getType()), bigQueryTable.asPlainTable().getRemoteTableName().getTableName())) {
            throw new TrinoException(BIGQUERY_UNSUPPORTED_OPERATION, "This connector does not support dropping wildcard tables");
        }
        TableId tableId = bigQueryTable.asPlainTable().getRemoteTableName().toTableId();
        client.dropTable(tableId);
    }

    @Override
    public void truncateTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        BigQueryTableHandle table = (BigQueryTableHandle) tableHandle;
        BigQueryClient client = bigQueryClientFactory.createBigQueryClient(session);

        RemoteTableName remoteTableName = table.asPlainTable().getRemoteTableName();
        String sql = format(
                "TRUNCATE TABLE %s.%s.%s",
                quote(remoteTableName.getProjectId()),
                quote(remoteTableName.getDatasetName()),
                quote(remoteTableName.getTableName()));
        client.executeUpdate(session, QueryJobConfiguration.of(sql));
    }

    @Override
    public boolean supportsMissingColumnsOnInsert()
    {
        return true;
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode)
    {
        BigQueryTableHandle table = (BigQueryTableHandle) tableHandle;
        if (isWildcardTable(TableDefinition.Type.valueOf(table.asPlainTable().getType()), table.asPlainTable().getRemoteTableName().getTableName())) {
            throw new TrinoException(BIGQUERY_UNSUPPORTED_OPERATION, "This connector does not support inserting into wildcard tables");
        }
        ImmutableList.Builder<String> columnNames = ImmutableList.builderWithExpectedSize(columns.size());
        ImmutableList.Builder<Type> columnTypes = ImmutableList.builderWithExpectedSize(columns.size());
        ImmutableList.Builder<Field> tempFields = ImmutableList.builderWithExpectedSize(columns.size() + 1);

        for (ColumnHandle columnHandle : columns) {
            BigQueryColumnHandle column = (BigQueryColumnHandle) columnHandle;
            tempFields.add(toField(column.getName(), column.getTrinoType(), column.getColumnMetadata().getComment()));
            columnNames.add(column.getName());
            columnTypes.add(column.getTrinoType());
        }
        ColumnMetadata pageSinkIdColumn = buildPageSinkIdColumn(columnNames.build());
        tempFields.add(toField(pageSinkIdColumn.getName(), pageSinkIdColumn.getType(), pageSinkIdColumn.getComment()));

        BigQueryClient client = bigQueryClientFactory.create(session);
        String projectId = table.asPlainTable().getRemoteTableName().getProjectId();
        String schemaName = table.asPlainTable().getRemoteTableName().getDatasetName();

        String temporaryTableName = generateTemporaryTableName(session);
        TableId temporaryTableId = createTable(client, projectId, schemaName, temporaryTableName, tempFields.build(), Optional.empty());
        setRollback(() -> bigQueryClientFactory.create(session).dropTable(temporaryTableId));

        return new BigQueryInsertTableHandle(
                table.asPlainTable().getRemoteTableName(),
                columnNames.build(),
                columnTypes.build(),
                temporaryTableName,
                pageSinkIdColumn.getName());
    }

    private Optional<ConnectorOutputMetadata> finishInsert(
            ConnectorSession session,
            RemoteTableName targetTable,
            RemoteTableName tempTable,
            String pageSinkIdColumnName,
            List<String> columnNames,
            Collection<Slice> fragments)
    {
        Closer closer = Closer.create();
        closer.register(() -> bigQueryClientFactory.create(session).dropTable(tempTable.toTableId()));

        try {
            BigQueryClient client = bigQueryClientFactory.create(session);

            RemoteTableName pageSinkTable = new RemoteTableName(
                    targetTable.getProjectId(),
                    targetTable.getDatasetName(),
                    generateTemporaryTableName(session));
            createTable(client, pageSinkTable.getProjectId(), pageSinkTable.getDatasetName(), pageSinkTable.getTableName(), ImmutableList.of(toField(pageSinkIdColumnName, TRINO_PAGE_SINK_ID_COLUMN_TYPE, null)), Optional.empty());
            closer.register(() -> bigQueryClientFactory.create(session).dropTable(pageSinkTable.toTableId()));

            InsertAllRequest.Builder batch = InsertAllRequest.newBuilder(pageSinkTable.toTableId());
            fragments.forEach(slice -> batch.addRow(ImmutableMap.of(pageSinkIdColumnName, slice.getLong(0))));
            client.insert(batch.build());

            String columns = columnNames.stream().map(BigQueryUtil::quote).collect(Collectors.joining(", "));

            String insertSql = format("INSERT INTO %s (%s) SELECT %s FROM %s temp_table " +
                            "WHERE EXISTS (SELECT 1 FROM %s page_sink_table WHERE page_sink_table.%s = temp_table.%s)",
                    quoted(targetTable),
                    columns,
                    columns,
                    quoted(tempTable),
                    quoted(pageSinkTable),
                    quote(pageSinkIdColumnName),
                    quote(pageSinkIdColumnName));

            client.executeUpdate(session, QueryJobConfiguration.of(insertSql));
        }
        finally {
            try {
                closer.close();
            }
            catch (IOException e) {
                throw new TrinoException(BIGQUERY_FAILED_TO_EXECUTE_QUERY, e);
            }
        }

        return Optional.empty();
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        BigQueryInsertTableHandle handle = (BigQueryInsertTableHandle) insertHandle;
        return finishInsert(session, handle.getRemoteTableName(), handle.getTemporaryRemoteTableName(), handle.getPageSinkIdColumnName(), handle.getColumnNames(), fragments);
    }

    @Override
    public Optional<ConnectorTableHandle> applyDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        // TODO Fix BaseBigQueryFailureRecoveryTest when implementing this method
        return ConnectorMetadata.super.applyDelete(session, handle);
    }

    @Override
    public OptionalLong executeDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        // TODO Fix BaseBigQueryFailureRecoveryTest when implementing this method
        return ConnectorMetadata.super.executeDelete(session, handle);
    }

    @Override
    public ConnectorMergeTableHandle beginMerge(ConnectorSession session, ConnectorTableHandle tableHandle, RetryMode retryMode)
    {
        // TODO Fix BaseBigQueryFailureRecoveryTest when implementing this method
        return ConnectorMetadata.super.beginMerge(session, tableHandle, retryMode);
    }

    @Override
    public void createMaterializedView(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition, boolean replace, boolean ignoreExisting)
    {
        // TODO Fix BaseBigQueryFailureRecoveryTest when implementing this method
        ConnectorMetadata.super.createMaterializedView(session, viewName, definition, replace, ignoreExisting);
    }

    @Override
    public ConnectorAnalyzeMetadata getStatisticsCollectionMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Object> analyzeProperties)
    {
        // TODO Fix BaseBigQueryFailureRecoveryTest when implementing this method
        return ConnectorMetadata.super.getStatisticsCollectionMetadata(session, tableHandle, analyzeProperties);
    }

    @Override
    public void setTableComment(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> newComment)
    {
        BigQueryTableHandle table = (BigQueryTableHandle) tableHandle;
        BigQueryClient client = bigQueryClientFactory.createBigQueryClient(session);

        RemoteTableName remoteTableName = table.asPlainTable().getRemoteTableName();
        String sql = format(
                "ALTER TABLE %s.%s.%s SET OPTIONS (description = ?)",
                quote(remoteTableName.getProjectId()),
                quote(remoteTableName.getDatasetName()),
                quote(remoteTableName.getTableName()));
        client.executeUpdate(session, QueryJobConfiguration.newBuilder(sql)
                .addPositionalParameter(QueryParameterValue.string(newComment.orElse(null)))
                .build());
    }

    @Override
    public void setColumnComment(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle, Optional<String> newComment)
    {
        BigQueryTableHandle table = (BigQueryTableHandle) tableHandle;
        BigQueryColumnHandle column = (BigQueryColumnHandle) columnHandle;
        BigQueryClient client = bigQueryClientFactory.createBigQueryClient(session);

        RemoteTableName remoteTableName = table.asPlainTable().getRemoteTableName();
        String sql = format(
                "ALTER TABLE %s.%s.%s ALTER COLUMN %s SET OPTIONS (description = ?)",
                quote(remoteTableName.getProjectId()),
                quote(remoteTableName.getDatasetName()),
                quote(remoteTableName.getTableName()),
                quote(column.getName()));
        client.executeUpdate(session, QueryJobConfiguration.newBuilder(sql)
                .addPositionalParameter(QueryParameterValue.string(newComment.orElse(null)))
                .build());
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

        List<ColumnHandle> newColumns = ImmutableList.copyOf(assignments.values());

        if (bigQueryTableHandle.getProjectedColumns().isPresent() && containSameElements(newColumns, bigQueryTableHandle.getProjectedColumns().get())) {
            return Optional.empty();
        }

        ImmutableList.Builder<BigQueryColumnHandle> projectedColumns = ImmutableList.builder();
        ImmutableList.Builder<Assignment> assignmentList = ImmutableList.builder();
        assignments.forEach((name, column) -> {
            BigQueryColumnHandle columnHandle = (BigQueryColumnHandle) column;
            projectedColumns.add(columnHandle);
            assignmentList.add(new Assignment(name, column, columnHandle.getTrinoType()));
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

    @Override
    public Optional<TableFunctionApplicationResult<ConnectorTableHandle>> applyTableFunction(ConnectorSession session, ConnectorTableFunctionHandle handle)
    {
        if (!(handle instanceof QueryHandle)) {
            return Optional.empty();
        }

        ConnectorTableHandle tableHandle = ((QueryHandle) handle).getTableHandle();
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(getColumnHandles(session, tableHandle).values());
        return Optional.of(new TableFunctionApplicationResult<>(tableHandle, columnHandles));
    }

    private String getRemoteSchemaName(BigQueryClient client, String projectId, String datasetName)
    {
        return client.toRemoteDataset(projectId, datasetName)
                .map(RemoteDatabaseObject::getOnlyRemoteName)
                .orElseThrow(() -> new SchemaNotFoundException(datasetName));
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

    private static SchemaTableName getSchemaTableName(BigQueryTableHandle handle)
    {
        return handle.isNamedRelation()
                ? handle.getRequiredNamedRelation().getSchemaTableName()
                // TODO (https://github.com/trinodb/trino/issues/6694) SchemaTableName should not be required for synthetic ConnectorTableHandle
                : new SchemaTableName("_generated", "_generated_query");
    }

    private static Optional<String> getTableComment(BigQueryTableHandle handle)
    {
        return handle.isNamedRelation()
                ? handle.getRequiredNamedRelation().getComment()
                : Optional.empty();
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

    private static ColumnMetadata buildPageSinkIdColumn(List<String> otherColumnNames)
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
        return new ColumnMetadata(columnName, TRINO_PAGE_SINK_ID_COLUMN_TYPE);
    }
}
