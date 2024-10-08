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

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.ViewDefinition;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.CreateWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.plugin.base.projection.ApplyProjectionUtil;
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
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RelationCommentMetadata;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SampleApplicationResult;
import io.trino.spi.connector.SampleType;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableFunctionApplicationResult;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.json.JSONArray;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.cloud.bigquery.StandardSQLTypeName.INT64;
import static com.google.cloud.bigquery.storage.v1.WriteStream.Type.COMMITTED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.util.concurrent.Futures.allAsList;
import static io.trino.plugin.base.TemporaryTables.generateTemporaryTableName;
import static io.trino.plugin.base.projection.ApplyProjectionUtil.ProjectedColumnRepresentation;
import static io.trino.plugin.base.projection.ApplyProjectionUtil.extractSupportedProjectedColumns;
import static io.trino.plugin.base.projection.ApplyProjectionUtil.replaceWithNewVariables;
import static io.trino.plugin.bigquery.BigQueryErrorCode.BIGQUERY_BAD_WRITE;
import static io.trino.plugin.bigquery.BigQueryErrorCode.BIGQUERY_FAILED_TO_EXECUTE_QUERY;
import static io.trino.plugin.bigquery.BigQueryErrorCode.BIGQUERY_LISTING_TABLE_ERROR;
import static io.trino.plugin.bigquery.BigQueryErrorCode.BIGQUERY_UNSUPPORTED_OPERATION;
import static io.trino.plugin.bigquery.BigQueryPseudoColumn.PARTITION_DATE;
import static io.trino.plugin.bigquery.BigQueryPseudoColumn.PARTITION_TIME;
import static io.trino.plugin.bigquery.BigQuerySessionProperties.isProjectionPushdownEnabled;
import static io.trino.plugin.bigquery.BigQueryTableHandle.BigQueryPartitionType.INGESTION;
import static io.trino.plugin.bigquery.BigQueryTableHandle.getPartitionType;
import static io.trino.plugin.bigquery.BigQueryUtil.isWildcardTable;
import static io.trino.plugin.bigquery.BigQueryUtil.quote;
import static io.trino.plugin.bigquery.BigQueryUtil.quoted;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.SaveMode.IGNORE;
import static io.trino.spi.connector.SaveMode.REPLACE;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class BigQueryMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(BigQueryMetadata.class);
    private static final Type TRINO_PAGE_SINK_ID_COLUMN_TYPE = BigintType.BIGINT;
    private static final Ordering<BigQueryColumnHandle> COLUMN_HANDLE_ORDERING = Ordering
            .from(Comparator.comparingInt(columnHandle -> columnHandle.dereferenceNames().size()));

    static final int DEFAULT_NUMERIC_TYPE_PRECISION = 38;
    static final int DEFAULT_NUMERIC_TYPE_SCALE = 9;
    private static final String VIEW_DEFINITION_SYSTEM_TABLE_SUFFIX = "$view_definition";

    private final BigQueryClientFactory bigQueryClientFactory;
    private final BigQueryWriteClientFactory writeClientFactory;
    private final BigQueryTypeManager typeManager;
    private final AtomicReference<Runnable> rollbackAction = new AtomicReference<>();
    private final ListeningExecutorService executorService;
    private final boolean isLegacyMetadataListing;

    public BigQueryMetadata(
            BigQueryClientFactory bigQueryClientFactory,
            BigQueryWriteClientFactory writeClientFactory,
            BigQueryTypeManager typeManager,
            ListeningExecutorService executorService,
            boolean isLegacyMetadataListing)
    {
        this.bigQueryClientFactory = requireNonNull(bigQueryClientFactory, "bigQueryClientFactory is null");
        this.writeClientFactory = requireNonNull(writeClientFactory, "writeClientFactory is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.executorService = requireNonNull(executorService, "executorService is null");
        this.isLegacyMetadataListing = isLegacyMetadataListing;
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

        List<DatasetId> datasetIds = client.listDatasetIds(projectId);
        Stream<String> remoteSchemaNames = datasetIds.stream()
                .map(DatasetId::getDataset)
                .distinct();

        // filter out all the ambiguous schemas to prevent failures if anyone tries to access the listed schemas

        return remoteSchemaNames.map(remoteSchema -> client.toRemoteDataset(projectId, remoteSchema.toLowerCase(ENGLISH), () -> datasetIds))
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
        DatasetId localDatasetId = client.toDatasetId(schemaName);

        // Overridden to make sure an error message is returned in case of an ambiguous schema name
        log.debug("schemaExists(session=%s)", session);
        return client.toRemoteDataset(localDatasetId)
                .map(RemoteDatabaseObject::getOnlyRemoteName)
                .filter(remoteSchema -> client.getDataset(DatasetId.of(localDatasetId.getProject(), remoteSchema)) != null)
                .isPresent();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        BigQueryClient client = bigQueryClientFactory.create(session);

        log.debug("listTables(session=%s, schemaName=%s)", session, schemaName);
        String projectId;

        Set<String> remoteSchemaNames;
        if (schemaName.isPresent()) {
            DatasetId localDatasetId = client.toDatasetId(schemaName.get());
            projectId = localDatasetId.getProject();
            // filter ambiguous schemas
            Optional<String> remoteSchema = client.toRemoteDataset(localDatasetId)
                    .filter(dataset -> !dataset.isAmbiguous())
                    .map(RemoteDatabaseObject::getOnlyRemoteName);

            remoteSchemaNames = remoteSchema.map(ImmutableSet::of).orElse(ImmutableSet.of());
        }
        else {
            projectId = client.getProjectId();
            remoteSchemaNames = ImmutableSet.copyOf(listRemoteSchemaNames(session));
        }

        return processInParallel(remoteSchemaNames.stream().toList(), remoteSchemaName -> listTablesInRemoteSchema(client, projectId, remoteSchemaName))
                .flatMap(Collection::stream)
                .collect(toImmutableList());
    }

    private List<SchemaTableName> listTablesInRemoteSchema(BigQueryClient client, String projectId, String remoteSchemaName)
    {
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        try {
            Iterable<TableId> tableIds = client.listTableIds(DatasetId.of(projectId, remoteSchemaName));
            for (TableId tableId : tableIds) {
                // filter ambiguous tables
                client.toRemoteTable(projectId, remoteSchemaName, tableId.getTable().toLowerCase(ENGLISH), tableIds)
                        .filter(RemoteDatabaseObject::isAmbiguous)
                        .ifPresentOrElse(
                                remoteTable -> log.debug("Filtered out [%s.%s] from list of tables due to ambiguous name", remoteSchemaName, tableId.getTable()),
                                () -> tableNames.add(new SchemaTableName(client.toSchemaName(DatasetId.of(projectId, tableId.getDataset())), tableId.getTable())));
            }
        }
        catch (TrinoException e) {
            if (e.getErrorCode() == BIGQUERY_LISTING_TABLE_ERROR.toErrorCode() &&
                    e.getCause() instanceof BigQueryException bigQueryException &&
                    bigQueryException.getCode() == 404 &&
                    bigQueryException.getMessage().contains("Not found: Dataset")) {
                // Dataset not found error is ignored because listTables is used for metadata queries (SELECT FROM information_schema)
                log.debug("Dataset disappeared during listing operation: %s", remoteSchemaName);
            }
            else {
                throw e;
            }
        }
        return tableNames.build();
    }

    @Override
    public Iterator<RelationCommentMetadata> streamRelationComments(ConnectorSession session, Optional<String> schemaName, UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        if (isLegacyMetadataListing) {
            return ConnectorMetadata.super.streamRelationComments(session, schemaName, relationFilter);
        }

        BigQueryClient client = bigQueryClientFactory.create(session);
        List<String> schemaNames = schemaName.map(name -> {
            DatasetId localDatasetId = client.toDatasetId(name);
            String remoteSchemaName = getRemoteSchemaName(client, localDatasetId.getProject(), localDatasetId.getDataset());
            return List.of(remoteSchemaName);
        }).orElseGet(() -> listSchemaNames(session));
        Map<SchemaTableName, RelationCommentMetadata> resultsByName = schemaNames.stream()
                .flatMap(schema -> listRelationCommentMetadata(session, client, schema))
                .collect(toImmutableMap(RelationCommentMetadata::name, Functions.identity(), (first, second) -> {
                    log.debug("Filtered out [%s] from list of tables due to ambiguous name", first.name());
                    return null;
                }));
        return relationFilter.apply(resultsByName.keySet()).stream()
                .map(resultsByName::get)
                .iterator();
    }

    private static Stream<RelationCommentMetadata> listRelationCommentMetadata(ConnectorSession session, BigQueryClient client, String schema)
    {
        try {
            return client.listRelationCommentMetadata(session, client, schema);
        }
        catch (BigQueryException e) {
            if (e.getCode() == 404) {
                log.debug("Dataset disappeared during listing operation: %s", schema);
                return Stream.empty();
            }
            throw new TrinoException(BIGQUERY_LISTING_TABLE_ERROR, "Failed to retrieve tables from BigQuery", e);
        }
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }

        BigQueryClient client = bigQueryClientFactory.create(session);
        log.debug("getTableHandle(session=%s, schemaTableName=%s)", session, schemaTableName);
        DatasetId localDatasetId = client.toDatasetId(schemaTableName.getSchemaName());
        String remoteSchemaName = client.toRemoteDataset(localDatasetId)
                .map(RemoteDatabaseObject::getOnlyRemoteName)
                .orElse(localDatasetId.getDataset());
        String remoteTableName = client.toRemoteTable(session, localDatasetId.getProject(), remoteSchemaName, schemaTableName.getTableName())
                .map(RemoteDatabaseObject::getOnlyRemoteName)
                .orElse(schemaTableName.getTableName());
        Optional<TableInfo> tableInfo = client.getTable(TableId.of(localDatasetId.getProject(), remoteSchemaName, remoteTableName));
        if (tableInfo.isEmpty()) {
            log.debug("Table [%s.%s] was not found", schemaTableName.getSchemaName(), schemaTableName.getTableName());
            return null;
        }

        ImmutableList.Builder<BigQueryColumnHandle> columns = ImmutableList.builder();
        columns.addAll(client.buildColumnHandles(tableInfo.get()));
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
                Optional.ofNullable(tableInfo.get().getDescription())),
                TupleDomain.all(),
                Optional.empty())
                .withProjectedColumns(columns.build());
    }

    private Optional<TableInfo> getTableInfoIgnoringConflicts(ConnectorSession session, SchemaTableName schemaTableName)
    {
        BigQueryClient client = bigQueryClientFactory.create(session);
        DatasetId localDatasetId = client.toDatasetId(schemaTableName.getSchemaName());
        String remoteSchemaName = client.toRemoteDataset(localDatasetId)
                .map(RemoteDatabaseObject::getAnyRemoteName)
                .orElse(localDatasetId.getDataset());
        String remoteTableName = client.toRemoteTable(session, localDatasetId.getProject(), remoteSchemaName, schemaTableName.getTableName())
                .map(RemoteDatabaseObject::getAnyRemoteName)
                .orElse(schemaTableName.getTableName());
        return client.getTable(TableId.of(localDatasetId.getProject(), remoteSchemaName, remoteTableName));
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        BigQueryClient client = bigQueryClientFactory.create(session);
        log.debug("getTableMetadata(session=%s, tableHandle=%s)", session, tableHandle);
        BigQueryTableHandle handle = (BigQueryTableHandle) tableHandle;

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
        DatasetId localDatasetId = client.toDatasetId(sourceTableName.getSchemaName());
        String remoteSchemaName = client.toRemoteDataset(localDatasetId)
                .map(RemoteDatabaseObject::getOnlyRemoteName)
                .orElseThrow(() -> new TableNotFoundException(viewDefinitionTableName));
        String remoteTableName = client.toRemoteTable(session, localDatasetId.getProject(), remoteSchemaName, sourceTableName.getTableName())
                .map(RemoteDatabaseObject::getOnlyRemoteName)
                .orElseThrow(() -> new TableNotFoundException(viewDefinitionTableName));
        TableInfo tableInfo = client.getTable(TableId.of(localDatasetId.getProject(), remoteSchemaName, remoteTableName))
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
        if (table.projectedColumns().isPresent()) {
            return table.projectedColumns().get().stream()
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

    @SuppressWarnings("deprecation") // TODO Remove this method when https://github.com/trinodb/trino/pull/21920 is merged
    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        BigQueryClient client = bigQueryClientFactory.create(session);
        log.debug("listTableColumns(session=%s, prefix=%s)", session, prefix);
        List<SchemaTableName> tables = prefix.toOptionalSchemaTableName()
                .<List<SchemaTableName>>map(ImmutableList::of)
                .orElseGet(() -> listTables(session, prefix.getSchema()));

        List<TableInfo> tableInfos = processInParallel(tables, table -> getTableInfoIgnoringConflicts(session, table))
                .flatMap(Optional::stream)
                .collect(toImmutableList());

        return tableInfos.stream()
                .collect(toImmutableMap(
                        table -> new SchemaTableName(table.getTableId().getDataset(), table.getTableId().getTable()),
                        table -> client.buildColumnHandles(table).stream()
                                .map(BigQueryColumnHandle::getColumnMetadata)
                                .collect(toImmutableList())));
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
        DatasetInfo datasetInfo = DatasetInfo.newBuilder(client.toDatasetId(schemaName)).build();
        client.createSchema(datasetInfo);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        BigQueryClient client = bigQueryClientFactory.create(session);
        DatasetId localDatasetId = client.toDatasetId(schemaName);
        String remoteSchemaName = getRemoteSchemaName(client, localDatasetId.getProject(), localDatasetId.getDataset());
        client.dropSchema(DatasetId.of(localDatasetId.getProject(), remoteSchemaName), cascade);
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
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode)
    {
        if (saveMode == REPLACE) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support replacing tables");
        }
        try {
            createTable(session, tableMetadata, Optional.empty());
        }
        catch (BigQueryException e) {
            if (saveMode == IGNORE && e.getCode() == 409) {
                return;
            }
            throw e;
        }
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode, boolean replace)
    {
        if (replace) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support replacing tables");
        }

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
            fields.add(typeManager.toField(column.getName(), column.getType(), column.getComment()));
            tempFields.add(typeManager.toField(column.getName(), column.getType(), column.getComment()));
            columnsNames.add(column.getName());
            columnsTypes.add(column.getType());
        }

        BigQueryClient client = bigQueryClientFactory.create(session);
        DatasetId localDatasetId = client.toDatasetId(schemaName);
        String remoteSchemaName = getRemoteSchemaName(client, localDatasetId.getProject(), localDatasetId.getDataset());

        Closer closer = Closer.create();
        setRollback(() -> {
            try {
                closer.close();
            }
            catch (IOException e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
            }
        });

        TableId tableId = createTable(client, localDatasetId.getProject(), remoteSchemaName, tableName, fields.build(), tableMetadata.getComment());
        closer.register(() -> bigQueryClientFactory.create(session).dropTable(tableId));

        Optional<String> temporaryTableName = pageSinkIdColumn.map(column -> {
            tempFields.add(typeManager.toField(column.getName(), column.getType(), column.getComment()));
            String tempTableName = generateTemporaryTableName(session);
            TableId tempTableId = createTable(client, localDatasetId.getProject(), remoteSchemaName, tempTableName, tempFields.build(), tableMetadata.getComment());
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
        checkState(handle.temporaryTableName().isPresent(), "Unexpected use of finishCreateTable without a temporaryTableName present");
        return finishInsert(session, handle.remoteTableName(), handle.getTemporaryRemoteTableName().orElseThrow(), handle.pageSinkIdColumnName().orElseThrow(), handle.columnNames(), fragments);
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        BigQueryClient client = bigQueryClientFactory.create(session);
        BigQueryTableHandle bigQueryTable = (BigQueryTableHandle) tableHandle;
        if (isWildcardTable(TableDefinition.Type.valueOf(bigQueryTable.asPlainTable().getType()), bigQueryTable.asPlainTable().getRemoteTableName().tableName())) {
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
                quote(remoteTableName.projectId()),
                quote(remoteTableName.datasetName()),
                quote(remoteTableName.tableName()));
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
        if (isWildcardTable(TableDefinition.Type.valueOf(table.asPlainTable().getType()), table.asPlainTable().getRemoteTableName().tableName())) {
            throw new TrinoException(BIGQUERY_UNSUPPORTED_OPERATION, "This connector does not support inserting into wildcard tables");
        }
        ImmutableList.Builder<String> columnNames = ImmutableList.builderWithExpectedSize(columns.size());
        ImmutableList.Builder<Type> columnTypes = ImmutableList.builderWithExpectedSize(columns.size());
        ImmutableList.Builder<Field> tempFields = ImmutableList.builderWithExpectedSize(columns.size() + 1);

        for (ColumnHandle columnHandle : columns) {
            BigQueryColumnHandle column = (BigQueryColumnHandle) columnHandle;
            tempFields.add(typeManager.toField(column.name(), column.trinoType(), column.getColumnMetadata().getComment()));
            columnNames.add(column.name());
            columnTypes.add(column.trinoType());
        }
        ColumnMetadata pageSinkIdColumn = buildPageSinkIdColumn(columnNames.build());
        tempFields.add(typeManager.toField(pageSinkIdColumn.getName(), pageSinkIdColumn.getType(), pageSinkIdColumn.getComment()));

        BigQueryClient client = bigQueryClientFactory.create(session);
        String projectId = table.asPlainTable().getRemoteTableName().projectId();
        String schemaName = table.asPlainTable().getRemoteTableName().datasetName();

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
                    targetTable.projectId(),
                    targetTable.datasetName(),
                    generateTemporaryTableName(session));
            createTable(client, pageSinkTable.projectId(), pageSinkTable.datasetName(), pageSinkTable.tableName(), ImmutableList.of(typeManager.toField(pageSinkIdColumnName, TRINO_PAGE_SINK_ID_COLUMN_TYPE, null)), Optional.empty());
            closer.register(() -> bigQueryClientFactory.create(session).dropTable(pageSinkTable.toTableId()));

            insertIntoSinkTable(session, pageSinkTable, pageSinkIdColumnName, fragments);

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

    private void insertIntoSinkTable(ConnectorSession session, RemoteTableName pageSinkTable, String pageSinkIdColumnName, Collection<Slice> fragments)
    {
        try (BigQueryWriteClient writeClient = writeClientFactory.create(session)) {
            CreateWriteStreamRequest createWriteStreamRequest = CreateWriteStreamRequest.newBuilder()
                    .setParent(TableName.of(pageSinkTable.projectId(), pageSinkTable.datasetName(), pageSinkTable.tableName()).toString())
                    .setWriteStream(WriteStream.newBuilder().setType(COMMITTED).build())
                    .build();
            WriteStream stream = writeClient.createWriteStream(createWriteStreamRequest);
            JSONArray batch = new JSONArray();
            fragments.forEach(slice -> batch.put(ImmutableMap.of(pageSinkIdColumnName, slice.getLong(0))));
            try (JsonStreamWriter writer = JsonStreamWriter.newBuilder(stream.getName(), stream.getTableSchema(), writeClient).build()) {
                ApiFuture<AppendRowsResponse> future = writer.append(batch);
                AppendRowsResponse response = future.get();
                if (response.hasError()) {
                    throw new TrinoException(BIGQUERY_BAD_WRITE, format("Response has error: %s", response.getError().getMessage()));
                }
            }
            catch (Exception e) {
                throw new TrinoException(BIGQUERY_BAD_WRITE, "Failed to insert rows", e);
            }
        }
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(
            ConnectorSession session,
            ConnectorInsertTableHandle insertHandle,
            List<ConnectorTableHandle> sourceTableHandles,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        BigQueryInsertTableHandle handle = (BigQueryInsertTableHandle) insertHandle;
        return finishInsert(session, handle.remoteTableName(), handle.getTemporaryRemoteTableName(), handle.pageSinkIdColumnName(), handle.columnNames(), fragments);
    }

    @Override
    public ColumnHandle getMergeRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return new BigQueryColumnHandle("$merge_row_id", ImmutableList.of(), BIGINT, INT64, true, Field.Mode.REQUIRED, ImmutableList.of(), null, true);
    }

    @Override
    public Optional<ConnectorTableHandle> applyDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        return Optional.of(handle);
    }

    @Override
    public OptionalLong executeDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        BigQueryTableHandle tableHandle = ((BigQueryTableHandle) handle);
        checkArgument(tableHandle.isNamedRelation(), "Unable to delete from synthetic table: %s", tableHandle);
        TupleDomain<ColumnHandle> tableConstraint = tableHandle.constraint();
        Optional<String> filter = BigQueryFilterQueryBuilder.buildFilter(tableConstraint);

        RemoteTableName remoteTableName = tableHandle.asPlainTable().getRemoteTableName();
        String sql = format(
                "DELETE FROM %s.%s.%s WHERE %s",
                quote(remoteTableName.projectId()),
                quote(remoteTableName.datasetName()),
                quote(remoteTableName.tableName()),
                filter.orElse("true"));
        BigQueryClient client = bigQueryClientFactory.create(session);
        long rows = client.executeUpdate(session, QueryJobConfiguration.newBuilder(sql)
                .setQuery(sql)
                .build());
        return OptionalLong.of(rows);
    }

    @Override
    public ConnectorMergeTableHandle beginMerge(ConnectorSession session, ConnectorTableHandle tableHandle, RetryMode retryMode)
    {
        // TODO Fix BaseBigQueryFailureRecoveryTest when implementing this method
        return ConnectorMetadata.super.beginMerge(session, tableHandle, retryMode);
    }

    @Override
    public void createMaterializedView(
            ConnectorSession session,
            SchemaTableName viewName,
            ConnectorMaterializedViewDefinition definition,
            Map<String, Object> properties,
            boolean replace,
            boolean ignoreExisting)
    {
        // TODO Fix BaseBigQueryFailureRecoveryTest when implementing this method
        ConnectorMetadata.super.createMaterializedView(session, viewName, definition, properties, replace, ignoreExisting);
    }

    @Override
    public ConnectorAnalyzeMetadata getStatisticsCollectionMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Object> analyzeProperties)
    {
        // TODO Fix BaseBigQueryFailureRecoveryTest when implementing this method
        return ConnectorMetadata.super.getStatisticsCollectionMetadata(session, tableHandle, analyzeProperties);
    }

    @Override
    public Optional<SampleApplicationResult<ConnectorTableHandle>> applySample(ConnectorSession session, ConnectorTableHandle handle, SampleType sampleType, double sampleRatio)
    {
        // TODO Enable BaseBigQueryConnectorTest.testTableSampleBernoulli when supporting this pushdown
        return ConnectorMetadata.super.applySample(session, handle, sampleType, sampleRatio);
    }

    @Override
    public void setTableComment(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> newComment)
    {
        BigQueryTableHandle table = (BigQueryTableHandle) tableHandle;
        BigQueryClient client = bigQueryClientFactory.createBigQueryClient(session);

        RemoteTableName remoteTableName = table.asPlainTable().getRemoteTableName();
        String sql = format(
                "ALTER TABLE %s.%s.%s SET OPTIONS (description = ?)",
                quote(remoteTableName.projectId()),
                quote(remoteTableName.datasetName()),
                quote(remoteTableName.tableName()));
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
                quote(remoteTableName.projectId()),
                quote(remoteTableName.datasetName()),
                quote(remoteTableName.tableName()),
                quote(column.name()));
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
        if (!isProjectionPushdownEnabled(session)) {
            List<ColumnHandle> newColumns = ImmutableList.copyOf(assignments.values());
            if (bigQueryTableHandle.projectedColumns().isPresent() && containSameElements(newColumns, bigQueryTableHandle.projectedColumns().get())) {
                return Optional.empty();
            }

            ImmutableList.Builder<BigQueryColumnHandle> projectedColumns = ImmutableList.builder();
            ImmutableList.Builder<Assignment> assignmentList = ImmutableList.builder();
            assignments.forEach((name, column) -> {
                BigQueryColumnHandle columnHandle = (BigQueryColumnHandle) column;
                projectedColumns.add(columnHandle);
                assignmentList.add(new Assignment(name, column, columnHandle.trinoType()));
            });

            bigQueryTableHandle = bigQueryTableHandle.withProjectedColumns(projectedColumns.build());

            return Optional.of(new ProjectionApplicationResult<>(bigQueryTableHandle, projections, assignmentList.build(), false));
        }

        // Create projected column representations for supported sub expressions. Simple column references and chain of
        // dereferences on a variable are supported right now.
        Set<ConnectorExpression> projectedExpressions = projections.stream()
                .flatMap(expression -> extractSupportedProjectedColumns(expression).stream())
                .collect(toImmutableSet());

        Map<ConnectorExpression, ProjectedColumnRepresentation> columnProjections = projectedExpressions.stream()
                .collect(toImmutableMap(identity(), ApplyProjectionUtil::createProjectedColumnRepresentation));

        // all references are simple variables
        if (columnProjections.values().stream().allMatch(ProjectedColumnRepresentation::isVariable)) {
            Set<BigQueryColumnHandle> projectedColumns = ImmutableSet.copyOf(projectParentColumns(assignments.values().stream()
                    .map(BigQueryColumnHandle.class::cast)
                    .collect(toImmutableList())));
            if (bigQueryTableHandle.projectedColumns().isPresent() && containSameElements(projectedColumns, bigQueryTableHandle.projectedColumns().get())) {
                return Optional.empty();
            }
            List<Assignment> assignmentsList = assignments.entrySet().stream()
                    .map(assignment -> new Assignment(
                            assignment.getKey(),
                            assignment.getValue(),
                            ((BigQueryColumnHandle) assignment.getValue()).trinoType()))
                    .collect(toImmutableList());

            return Optional.of(new ProjectionApplicationResult<>(
                    bigQueryTableHandle.withProjectedColumns(ImmutableList.copyOf(projectedColumns)),
                    projections,
                    assignmentsList,
                    false));
        }

        Map<String, Assignment> newAssignments = new HashMap<>();
        ImmutableMap.Builder<ConnectorExpression, Variable> newVariablesBuilder = ImmutableMap.builder();
        ImmutableSet.Builder<BigQueryColumnHandle> projectedColumnsBuilder = ImmutableSet.builder();

        for (Map.Entry<ConnectorExpression, ProjectedColumnRepresentation> entry : columnProjections.entrySet()) {
            ConnectorExpression expression = entry.getKey();
            ProjectedColumnRepresentation projectedColumn = entry.getValue();

            BigQueryColumnHandle baseColumnHandle = (BigQueryColumnHandle) assignments.get(projectedColumn.getVariable().getName());
            BigQueryColumnHandle projectedColumnHandle = createProjectedColumnHandle(baseColumnHandle, projectedColumn.getDereferenceIndices(), expression.getType());
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
                bigQueryTableHandle.withProjectedColumns(projectParentColumns(ImmutableList.copyOf(projectedColumnsBuilder.build()))),
                newProjections,
                outputAssignments,
                false));
    }

    /**
     * Creates a set of parent columns for the input projected columns. For example,
     * if input {@param columns} include columns "a.b" and "a.b.c", then they will be projected from a single column "a.b".
     */
    @VisibleForTesting
    static List<BigQueryColumnHandle> projectParentColumns(List<BigQueryColumnHandle> columnHandles)
    {
        List<BigQueryColumnHandle> sortedColumnHandles = COLUMN_HANDLE_ORDERING.sortedCopy(columnHandles);
        List<BigQueryColumnHandle> parentColumns = new ArrayList<>();
        for (BigQueryColumnHandle column : sortedColumnHandles) {
            if (!parentColumnExists(parentColumns, column)) {
                parentColumns.add(column);
            }
        }
        return parentColumns;
    }

    private static boolean parentColumnExists(List<BigQueryColumnHandle> existingColumns, BigQueryColumnHandle column)
    {
        for (BigQueryColumnHandle existingColumn : existingColumns) {
            List<String> existingColumnDereferenceNames = existingColumn.dereferenceNames();
            verify(
                    column.dereferenceNames().size() >= existingColumnDereferenceNames.size(),
                    "Selected column's dereference size must be greater than or equal to the existing column's dereference size");
            if (existingColumn.name().equals(column.name())
                    && column.dereferenceNames().subList(0, existingColumnDereferenceNames.size()).equals(existingColumnDereferenceNames)) {
                return true;
            }
        }
        return false;
    }

    private BigQueryColumnHandle createProjectedColumnHandle(BigQueryColumnHandle baseColumn, List<Integer> indices, Type projectedColumnType)
    {
        if (indices.isEmpty()) {
            return baseColumn;
        }

        ImmutableList.Builder<String> dereferenceNamesBuilder = ImmutableList.builder();
        dereferenceNamesBuilder.addAll(baseColumn.dereferenceNames());

        Type type = baseColumn.trinoType();
        for (int index : indices) {
            checkArgument(type instanceof RowType, "type should be Row type");
            RowType rowType = (RowType) type;
            RowType.Field field = rowType.getFields().get(index);
            dereferenceNamesBuilder.add(field.getName()
                    .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "ROW type does not have field names declared: " + rowType)));
            type = field.getType();
        }
        return new BigQueryColumnHandle(
                baseColumn.name(),
                dereferenceNamesBuilder.build(),
                projectedColumnType,
                typeManager.toStandardSqlTypeName(projectedColumnType),
                baseColumn.isPushdownSupported(),
                baseColumn.mode(),
                baseColumn.subColumns(),
                baseColumn.description(),
                baseColumn.hidden());
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

        TupleDomain<ColumnHandle> oldDomain = bigQueryTableHandle.constraint();
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
                BigQueryColumnHandle columnHandle = (BigQueryColumnHandle) entry.getKey();
                Domain domain = entry.getValue();
                if (columnHandle.isPushdownSupported()) {
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

        BigQueryTableHandle updatedHandle = bigQueryTableHandle.withConstraint(newDomain);

        return Optional.of(new ConstraintApplicationResult<>(updatedHandle, remainingFilter, constraint.getExpression(), false));
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
