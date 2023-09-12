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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQuery.DatasetDeleteOption;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.JobStatistics.QueryStatistics;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.http.BaseHttpServiceException;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import static com.google.cloud.bigquery.JobStatistics.QueryStatistics.StatementType.SELECT;
import static com.google.cloud.bigquery.TableDefinition.Type.EXTERNAL;
import static com.google.cloud.bigquery.TableDefinition.Type.MATERIALIZED_VIEW;
import static com.google.cloud.bigquery.TableDefinition.Type.SNAPSHOT;
import static com.google.cloud.bigquery.TableDefinition.Type.TABLE;
import static com.google.cloud.bigquery.TableDefinition.Type.VIEW;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Streams.stream;
import static io.trino.plugin.bigquery.BigQueryErrorCode.BIGQUERY_AMBIGUOUS_OBJECT_NAME;
import static io.trino.plugin.bigquery.BigQueryErrorCode.BIGQUERY_FAILED_TO_EXECUTE_QUERY;
import static io.trino.plugin.bigquery.BigQueryErrorCode.BIGQUERY_INVALID_STATEMENT;
import static io.trino.plugin.bigquery.BigQueryErrorCode.BIGQUERY_LISTING_DATASET_ERROR;
import static io.trino.plugin.bigquery.BigQuerySessionProperties.createDisposition;
import static io.trino.plugin.bigquery.BigQuerySessionProperties.isQueryResultsCacheEnabled;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.joining;

public class BigQueryClient
{
    private static final Logger log = Logger.get(BigQueryClient.class);

    static final Set<TableDefinition.Type> TABLE_TYPES = ImmutableSet.of(TABLE, VIEW, MATERIALIZED_VIEW, EXTERNAL, SNAPSHOT);

    private final BigQuery bigQuery;
    private final BigQueryLabelFactory labelFactory;
    private final ViewMaterializationCache materializationCache;
    private final boolean caseInsensitiveNameMatching;
    private final LoadingCache<String, List<Dataset>> remoteDatasetCache;
    private final Optional<String> configProjectId;

    public BigQueryClient(
            BigQuery bigQuery,
            BigQueryLabelFactory labelFactory,
            boolean caseInsensitiveNameMatching,
            ViewMaterializationCache materializationCache,
            Duration metadataCacheTtl,
            Optional<String> configProjectId)
    {
        this.bigQuery = requireNonNull(bigQuery, "bigQuery is null");
        this.labelFactory = requireNonNull(labelFactory, "labelFactory is null");
        this.materializationCache = requireNonNull(materializationCache, "materializationCache is null");
        this.caseInsensitiveNameMatching = caseInsensitiveNameMatching;
        this.remoteDatasetCache = EvictableCacheBuilder.newBuilder()
                .expireAfterWrite(metadataCacheTtl.toMillis(), MILLISECONDS)
                .shareNothingWhenDisabled()
                .build(CacheLoader.from(this::listDatasetsFromBigQuery));
        this.configProjectId = requireNonNull(configProjectId, "projectId is null");
    }

    public Optional<RemoteDatabaseObject> toRemoteDataset(String projectId, String datasetName)
    {
        requireNonNull(projectId, "projectId is null");
        requireNonNull(datasetName, "datasetName is null");
        verify(datasetName.codePoints().noneMatch(Character::isUpperCase), "Expected schema name from internal metadata to be lowercase: %s", datasetName);
        if (!caseInsensitiveNameMatching) {
            return Optional.of(RemoteDatabaseObject.of(datasetName));
        }

        Map<String, Optional<RemoteDatabaseObject>> mapping = new HashMap<>();
        for (Dataset dataset : listDatasets(projectId)) {
            mapping.merge(
                    dataset.getDatasetId().getDataset().toLowerCase(ENGLISH),
                    Optional.of(RemoteDatabaseObject.of(dataset.getDatasetId().getDataset())),
                    (currentValue, collision) -> currentValue.map(current -> current.registerCollision(collision.get().getOnlyRemoteName())));
        }

        if (!mapping.containsKey(datasetName)) {
            // dataset doesn't exist
            mapping.put(datasetName, Optional.empty());
        }

        verify(mapping.containsKey(datasetName));
        return mapping.get(datasetName);
    }

    public Optional<RemoteDatabaseObject> toRemoteTable(String projectId, String remoteDatasetName, String tableName)
    {
        return toRemoteTable(projectId, remoteDatasetName, tableName, () -> listTables(DatasetId.of(projectId, remoteDatasetName)));
    }

    public Optional<RemoteDatabaseObject> toRemoteTable(String projectId, String remoteDatasetName, String tableName, Iterable<Table> tables)
    {
        return toRemoteTable(projectId, remoteDatasetName, tableName, () -> tables);
    }

    private Optional<RemoteDatabaseObject> toRemoteTable(String projectId, String remoteDatasetName, String tableName, Supplier<Iterable<Table>> tables)
    {
        requireNonNull(projectId, "projectId is null");
        requireNonNull(remoteDatasetName, "remoteDatasetName is null");
        requireNonNull(tableName, "tableName is null");
        verify(tableName.codePoints().noneMatch(Character::isUpperCase), "Expected table name from internal metadata to be lowercase: %s", tableName);
        if (!caseInsensitiveNameMatching) {
            return Optional.of(RemoteDatabaseObject.of(tableName));
        }

        TableId cacheKey = TableId.of(projectId, remoteDatasetName, tableName);

        Map<TableId, Optional<RemoteDatabaseObject>> mapping = new HashMap<>();
        for (Table table : tables.get()) {
            mapping.merge(
                    tableIdToLowerCase(table.getTableId()),
                    Optional.of(RemoteDatabaseObject.of(table.getTableId().getTable())),
                    (currentValue, collision) -> currentValue.map(current -> current.registerCollision(collision.get().getOnlyRemoteName())));
        }

        if (!mapping.containsKey(cacheKey)) {
            // table doesn't exist
            mapping.put(cacheKey, Optional.empty());
        }

        verify(mapping.containsKey(cacheKey));
        return mapping.get(cacheKey);
    }

    private static TableId tableIdToLowerCase(TableId tableId)
    {
        return TableId.of(
                tableId.getProject(),
                tableId.getDataset(),
                tableId.getTable().toLowerCase(ENGLISH));
    }

    public DatasetInfo getDataset(DatasetId datasetId)
    {
        return bigQuery.getDataset(datasetId);
    }

    public Optional<TableInfo> getTable(TableId remoteTableId)
    {
        return Optional.ofNullable(bigQuery.getTable(remoteTableId));
    }

    public TableInfo getCachedTable(Duration viewExpiration, TableInfo remoteTableId, List<String> requiredColumns)
    {
        String query = selectSql(remoteTableId, requiredColumns);
        log.debug("query is %s", query);
        return materializationCache.getCachedTable(this, query, viewExpiration, remoteTableId);
    }

    /**
     * The Google Cloud Project ID that will be used to create the underlying BigQuery read session.
     * Effectively, this is the project that will be used for billing attribution.
     */
    public String getParentProjectId()
    {
        return bigQuery.getOptions().getProjectId();
    }

    /**
     * The Google Cloud Project ID where the data resides.
     */
    public String getProjectId()
    {
        String projectId = configProjectId.orElseGet(() -> bigQuery.getOptions().getProjectId());
        checkState(projectId.toLowerCase(ENGLISH).equals(projectId), "projectId must be lowercase but it's " + projectId);
        return projectId;
    }

    public Iterable<Dataset> listDatasets(String projectId)
    {
        try {
            return remoteDatasetCache.get(projectId);
        }
        catch (ExecutionException e) {
            throw new TrinoException(BIGQUERY_LISTING_DATASET_ERROR, "Failed to retrieve datasets from BigQuery", e);
        }
    }

    private List<Dataset> listDatasetsFromBigQuery(String projectId)
    {
        return stream(bigQuery.listDatasets(projectId).iterateAll())
                .collect(toImmutableList());
    }

    public Iterable<Table> listTables(DatasetId remoteDatasetId)
    {
        Iterable<Table> allTables = bigQuery.listTables(remoteDatasetId).iterateAll();
        return stream(allTables)
                .filter(table -> TABLE_TYPES.contains(table.getDefinition().getType()))
                .collect(toImmutableList());
    }

    Table update(TableInfo table)
    {
        return bigQuery.update(table);
    }

    public void createSchema(DatasetInfo datasetInfo)
    {
        bigQuery.create(datasetInfo);
    }

    public void dropSchema(DatasetId datasetId, boolean cascade)
    {
        if (cascade) {
            bigQuery.delete(datasetId, DatasetDeleteOption.deleteContents());
        }
        else {
            bigQuery.delete(datasetId);
        }
    }

    public void createTable(TableInfo tableInfo)
    {
        bigQuery.create(tableInfo);
    }

    public void dropTable(TableId tableId)
    {
        bigQuery.delete(tableId);
    }

    Job create(JobInfo jobInfo)
    {
        return bigQuery.create(jobInfo);
    }

    public void executeUpdate(ConnectorSession session, QueryJobConfiguration job)
    {
        log.debug("Execute query: %s", job.getQuery());
        execute(session, job);
    }

    public TableResult executeQuery(ConnectorSession session, String sql)
    {
        log.debug("Execute query: %s", sql);
        QueryJobConfiguration job = QueryJobConfiguration.newBuilder(sql)
                .setUseQueryCache(isQueryResultsCacheEnabled(session))
                .setCreateDisposition(createDisposition(session))
                .build();
        return execute(session, job);
    }

    private TableResult execute(ConnectorSession session, QueryJobConfiguration job)
    {
        QueryJobConfiguration jobWithQueryLabel = job.toBuilder()
                .setLabels(labelFactory.getLabels(session))
                .build();
        try {
            return bigQuery.query(jobWithQueryLabel);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BigQueryException(BaseHttpServiceException.UNKNOWN_CODE, format("Failed to run the query [%s]", job.getQuery()), e);
        }
    }

    public Schema getSchema(String sql)
    {
        log.debug("Get schema from query: %s", sql);
        JobInfo jobInfo = JobInfo.of(QueryJobConfiguration.newBuilder(sql).setDryRun(true).build());

        JobStatistics statistics;
        try {
            statistics = bigQuery.create(jobInfo).getStatistics();
        }
        catch (BigQueryException e) {
            throw new TrinoException(BIGQUERY_INVALID_STATEMENT, "Failed to get schema for query: " + sql, e);
        }

        QueryStatistics queryStatistics = (QueryStatistics) statistics;
        if (!queryStatistics.getStatementType().equals(SELECT)) {
            throw new TrinoException(BIGQUERY_INVALID_STATEMENT, "Unsupported statement type: " + queryStatistics.getStatementType());
        }

        return requireNonNull(queryStatistics.getSchema(), "Cannot determine schema for query");
    }

    public static String selectSql(TableId table, List<String> requiredColumns, Optional<String> filter)
    {
        String columns = requiredColumns.stream().map(column -> format("`%s`", column)).collect(joining(","));
        return selectSql(table, columns, filter);
    }

    private static String selectSql(TableId table, String formattedColumns, Optional<String> filter)
    {
        String tableName = fullTableName(table);
        String query = format("SELECT %s FROM `%s`", formattedColumns, tableName);
        if (filter.isEmpty()) {
            return query;
        }
        return query + " WHERE " + filter.get();
    }

    private String selectSql(TableInfo remoteTable, List<String> requiredColumns)
    {
        String columns = requiredColumns.isEmpty() ? "*" :
                requiredColumns.stream().map(column -> format("`%s`", column)).collect(joining(","));

        return selectSql(remoteTable.getTableId(), columns);
    }

    // assuming the SELECT part is properly formatted, can be used to call functions such as COUNT and SUM
    public String selectSql(TableId table, String formattedColumns)
    {
        String tableName = fullTableName(table);
        return format("SELECT %s FROM `%s`", formattedColumns, tableName);
    }

    public void insert(InsertAllRequest insertAllRequest)
    {
        InsertAllResponse response = bigQuery.insertAll(insertAllRequest);
        if (response.hasErrors()) {
            // Note that BigQuery doesn't rollback inserted rows
            throw new TrinoException(BIGQUERY_FAILED_TO_EXECUTE_QUERY, format("Failed to insert rows: %s", response.getInsertErrors()));
        }
    }

    private static String fullTableName(TableId remoteTableId)
    {
        String remoteSchemaName = remoteTableId.getDataset();
        String remoteTableName = remoteTableId.getTable();
        remoteTableId = TableId.of(remoteTableId.getProject(), remoteSchemaName, remoteTableName);
        return format("%s.%s.%s", remoteTableId.getProject(), remoteTableId.getDataset(), remoteTableId.getTable());
    }

    public List<BigQueryColumnHandle> getColumns(BigQueryTableHandle tableHandle)
    {
        if (tableHandle.getProjectedColumns().isPresent()) {
            return tableHandle.getProjectedColumns().get();
        }
        checkArgument(tableHandle.isNamedRelation(), "Cannot get columns for %s", tableHandle);

        TableInfo tableInfo = getTable(tableHandle.asPlainTable().getRemoteTableName().toTableId())
                .orElseThrow(() -> new TableNotFoundException(tableHandle.asPlainTable().getSchemaTableName()));
        return buildColumnHandles(tableInfo);
    }

    public static List<BigQueryColumnHandle> buildColumnHandles(TableInfo tableInfo)
    {
        Schema schema = tableInfo.getDefinition().getSchema();
        if (schema == null) {
            SchemaTableName schemaTableName = new SchemaTableName(tableInfo.getTableId().getDataset(), tableInfo.getTableId().getTable());
            throw new TableNotFoundException(schemaTableName, format("Table '%s' has no schema", schemaTableName));
        }
        return schema.getFields()
                .stream()
                .filter(Conversions::isSupportedType)
                .map(Conversions::toColumnHandle)
                .collect(toImmutableList());
    }

    public static final class RemoteDatabaseObject
    {
        private final Set<String> remoteNames;

        private RemoteDatabaseObject(Set<String> remoteNames)
        {
            this.remoteNames = ImmutableSet.copyOf(remoteNames);
        }

        public static RemoteDatabaseObject of(String remoteName)
        {
            return new RemoteDatabaseObject(ImmutableSet.of(remoteName));
        }

        public RemoteDatabaseObject registerCollision(String ambiguousName)
        {
            return new RemoteDatabaseObject(ImmutableSet.<String>builderWithExpectedSize(remoteNames.size() + 1)
                    .addAll(remoteNames)
                    .add(ambiguousName)
                    .build());
        }

        public String getAnyRemoteName()
        {
            return Collections.min(remoteNames);
        }

        public String getOnlyRemoteName()
        {
            if (!isAmbiguous()) {
                return getOnlyElement(remoteNames);
            }

            throw new TrinoException(BIGQUERY_AMBIGUOUS_OBJECT_NAME, "Found ambiguous names in BigQuery when looking up '" + getAnyRemoteName().toLowerCase(ENGLISH) + "': " + remoteNames);
        }

        public boolean isAmbiguous()
        {
            return remoteNames.size() > 1;
        }
    }
}
