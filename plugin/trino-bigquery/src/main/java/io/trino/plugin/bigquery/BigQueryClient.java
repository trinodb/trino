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
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobConfiguration;
import com.google.cloud.bigquery.JobException;
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
import com.google.common.base.Joiner;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.RelationCommentMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.cloud.bigquery.JobStatistics.QueryStatistics.StatementType.SELECT;
import static com.google.cloud.bigquery.TableDefinition.Type.EXTERNAL;
import static com.google.cloud.bigquery.TableDefinition.Type.MATERIALIZED_VIEW;
import static com.google.cloud.bigquery.TableDefinition.Type.SNAPSHOT;
import static com.google.cloud.bigquery.TableDefinition.Type.TABLE;
import static com.google.cloud.bigquery.TableDefinition.Type.VIEW;
import static com.google.common.base.MoreObjects.firstNonNull;
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
import static io.trino.plugin.bigquery.BigQueryErrorCode.BIGQUERY_LISTING_TABLE_ERROR;
import static io.trino.plugin.bigquery.BigQuerySessionProperties.createDisposition;
import static io.trino.plugin.bigquery.BigQuerySessionProperties.isQueryResultsCacheEnabled;
import static io.trino.plugin.bigquery.BigQueryUtil.quote;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.joining;

public class BigQueryClient
{
    private static final Logger log = Logger.get(BigQueryClient.class);
    private static final int PAGE_SIZE = 100;

    // BigQuery has different table_type in `INFORMATION_SCHEMA` than API responses that returns TableDefinition.Type
    // see https://cloud.google.com/bigquery/docs/information-schema-tables#schema
    static final Map<TableDefinition.Type, String> TABLE_TYPES = ImmutableMap.<TableDefinition.Type, String>builder()
            .put(TABLE, "BASE TABLE")
            .put(VIEW, "VIEW")
            .put(MATERIALIZED_VIEW, "MATERIALIZED VIEW")
            .put(EXTERNAL, "EXTERNAL")
            .put(SNAPSHOT, "SNAPSHOT")
            .buildOrThrow();

    private final BigQuery bigQuery;
    private final BigQueryLabelFactory labelFactory;
    private final BigQueryTypeManager typeManager;
    private final ViewMaterializationCache materializationCache;
    private final boolean caseInsensitiveNameMatching;
    private final LoadingCache<String, List<DatasetId>> remoteDatasetIdCache;
    private final Cache<DatasetId, RemoteDatabaseObject> remoteDatasetCaseInsensitiveCache;
    private final Cache<TableId, RemoteDatabaseObject> remoteTableCaseInsensitiveCache;
    private final Optional<String> configProjectId;

    public BigQueryClient(
            BigQuery bigQuery,
            BigQueryLabelFactory labelFactory,
            BigQueryTypeManager typeManager,
            boolean caseInsensitiveNameMatching,
            Duration caseInsensitiveNameMatchingCacheTtl,
            ViewMaterializationCache materializationCache,
            Duration metadataCacheTtl,
            Optional<String> configProjectId)
    {
        this.bigQuery = requireNonNull(bigQuery, "bigQuery is null");
        this.labelFactory = requireNonNull(labelFactory, "labelFactory is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.materializationCache = requireNonNull(materializationCache, "materializationCache is null");
        this.caseInsensitiveNameMatching = caseInsensitiveNameMatching;
        this.remoteDatasetIdCache = EvictableCacheBuilder.newBuilder()
                .expireAfterWrite(metadataCacheTtl.toMillis(), MILLISECONDS)
                .shareNothingWhenDisabled()
                .build(CacheLoader.from(this::listDatasetIdsFromBigQuery));
        this.remoteDatasetCaseInsensitiveCache = buildCache(caseInsensitiveNameMatchingCacheTtl);
        this.remoteTableCaseInsensitiveCache = buildCache(caseInsensitiveNameMatchingCacheTtl);
        this.configProjectId = requireNonNull(configProjectId, "projectId is null");
    }

    private static <K, V> Cache<K, V> buildCache(Duration cachingTtl)
    {
        return EvictableCacheBuilder.newBuilder()
                .expireAfterWrite(cachingTtl.toMillis(), MILLISECONDS)
                .shareNothingWhenDisabled()
                .build();
    }

    public Optional<RemoteDatabaseObject> toRemoteDataset(DatasetId datasetId)
    {
        return toRemoteDataset(datasetId.getProject(), datasetId.getDataset());
    }

    public Optional<RemoteDatabaseObject> toRemoteDataset(String projectId, String datasetName)
    {
        return toRemoteDataset(projectId, datasetName, () -> listDatasetIds(projectId));
    }

    public Optional<RemoteDatabaseObject> toRemoteDataset(String projectId, String datasetName, Supplier<List<DatasetId>> datasetIds)
    {
        requireNonNull(projectId, "projectId is null");
        requireNonNull(datasetName, "datasetName is null");
        verify(datasetName.codePoints().noneMatch(Character::isUpperCase), "Expected schema name from internal metadata to be lowercase: %s", datasetName);
        if (!caseInsensitiveNameMatching) {
            return Optional.of(RemoteDatabaseObject.of(datasetName));
        }

        DatasetId cacheKey = DatasetId.of(projectId, datasetName);

        Optional<RemoteDatabaseObject> remoteDataSetFromCache = Optional.ofNullable(remoteDatasetCaseInsensitiveCache.getIfPresent(cacheKey));
        if (remoteDataSetFromCache.isPresent()) {
            return remoteDataSetFromCache;
        }

        // Get all information from BigQuery and update cache from all fetched information
        for (DatasetId datasetId : datasetIds.get()) {
            DatasetId newCacheKey = datasetIdToLowerCase(datasetId);
            RemoteDatabaseObject newValue = RemoteDatabaseObject.of(datasetId.getDataset());
            updateCache(remoteDatasetCaseInsensitiveCache, newCacheKey, newValue);
        }
        return Optional.ofNullable(remoteDatasetCaseInsensitiveCache.getIfPresent(cacheKey));
    }

    public Optional<RemoteDatabaseObject> toRemoteTable(ConnectorSession session, String projectId, String remoteDatasetName, String tableName)
    {
        return toRemoteTable(projectId, remoteDatasetName, tableName, () -> findTableIdsIgnoreCase(session, DatasetId.of(projectId, remoteDatasetName), tableName));
    }

    public Optional<RemoteDatabaseObject> toRemoteTable(String projectId, String remoteDatasetName, String tableName, Iterable<TableId> tableIds)
    {
        return toRemoteTable(projectId, remoteDatasetName, tableName, () -> tableIds);
    }

    private Optional<RemoteDatabaseObject> toRemoteTable(String projectId, String remoteDatasetName, String tableName, Supplier<Iterable<TableId>> tableIds)
    {
        requireNonNull(projectId, "projectId is null");
        requireNonNull(remoteDatasetName, "remoteDatasetName is null");
        requireNonNull(tableName, "tableName is null");
        verify(tableName.codePoints().noneMatch(Character::isUpperCase), "Expected table name from internal metadata to be lowercase: %s", tableName);
        if (!caseInsensitiveNameMatching) {
            return Optional.of(RemoteDatabaseObject.of(tableName));
        }

        TableId cacheKey = TableId.of(projectId, remoteDatasetName, tableName);

        Optional<RemoteDatabaseObject> remoteTableFromCache = Optional.ofNullable(remoteTableCaseInsensitiveCache.getIfPresent(cacheKey));
        if (remoteTableFromCache.isPresent()) {
            return remoteTableFromCache;
        }

        // Get all information from BigQuery and update cache from all fetched information
        for (TableId table : tableIds.get()) {
            TableId newCacheKey = tableIdToLowerCase(table);
            RemoteDatabaseObject newValue = RemoteDatabaseObject.of(table.getTable());
            updateCache(remoteTableCaseInsensitiveCache, newCacheKey, newValue);
        }

        return Optional.ofNullable(remoteTableCaseInsensitiveCache.getIfPresent(cacheKey));
    }

    private static <T> void updateCache(Cache<T, RemoteDatabaseObject> caseInsensitiveCache, T newCacheKey, RemoteDatabaseObject newValue)
    {
        try {
            RemoteDatabaseObject currentCacheValue = caseInsensitiveCache.getIfPresent(newCacheKey);
            if (currentCacheValue == null) {
                caseInsensitiveCache.get(newCacheKey, () -> newValue);
            }
            else if (!currentCacheValue.remoteNames.contains(newValue.getOnlyRemoteName())) {
                // Cache already has key, check if new value is already registered and update with collision if it's not
                RemoteDatabaseObject mergedValue = currentCacheValue.registerCollision(newValue.getOnlyRemoteName());
                caseInsensitiveCache.invalidate(newCacheKey);
                caseInsensitiveCache.get(newCacheKey, () -> mergedValue);
            }
        }
        catch (ExecutionException e) {
            // Loading cache value should never throw as it's only storing precomputed value
            throw new UncheckedExecutionException(e);
        }
    }

    private static DatasetId datasetIdToLowerCase(DatasetId datasetId)
    {
        return DatasetId.of(
                datasetId.getProject(),
                datasetId.getDataset().toLowerCase(ENGLISH));
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
        try {
            return Optional.ofNullable(bigQuery.getTable(remoteTableId));
        }
        catch (BigQueryException e) {
            // getTable method throws an exception in some situations, e.g. wild card tables
            return Optional.empty();
        }
    }

    public TableInfo getCachedTable(Duration viewExpiration, TableInfo remoteTableId, List<BigQueryColumnHandle> requiredColumns, Optional<String> filter)
    {
        String query = selectSql(remoteTableId.getTableId(), requiredColumns, filter);
        log.debug("query is %s", query);
        return materializationCache.getCachedTable(this, query, viewExpiration, remoteTableId);
    }

    /**
     * The Google Cloud Project ID that will be used to create the underlying BigQuery read session.
     * Effectively, this is the project that will be used for billing attribution.
     */
    public String getParentProjectId()
    {
        return Optional.ofNullable(bigQuery.getOptions().getQuotaProjectId()).orElse(bigQuery.getOptions().getProjectId());
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

    protected DatasetId toDatasetId(String schemaName)
    {
        return DatasetId.of(getProjectId(), schemaName);
    }

    protected String toSchemaName(DatasetId datasetId)
    {
        return datasetId.getDataset();
    }

    public List<DatasetId> listDatasetIds(String projectId)
    {
        try {
            return remoteDatasetIdCache.get(projectId);
        }
        catch (ExecutionException e) {
            throw new TrinoException(BIGQUERY_LISTING_DATASET_ERROR, "Failed to retrieve datasets from BigQuery", e);
        }
    }

    private List<DatasetId> listDatasetIdsFromBigQuery(String projectId)
    {
        // BigQuery.listDatasets returns partial information on each dataset. See javadoc for more details.
        return stream(bigQuery.listDatasets(projectId, BigQuery.DatasetListOption.pageSize(PAGE_SIZE)).iterateAll())
                .map(Dataset::getDatasetId)
                .collect(toImmutableList());
    }

    public Iterable<TableId> listTableIds(DatasetId remoteDatasetId)
    {
        // BigQuery.listTables returns partial information on each table. See javadoc for more details.
        Iterable<Table> allTables;
        try {
            allTables = bigQuery.listTables(remoteDatasetId, BigQuery.TableListOption.pageSize(PAGE_SIZE)).iterateAll();
        }
        catch (BigQueryException e) {
            throw new TrinoException(BIGQUERY_LISTING_TABLE_ERROR, "Failed to retrieve tables from BigQuery", e);
        }
        return stream(allTables)
                .filter(table -> TABLE_TYPES.containsKey(table.getDefinition().getType()))
                .map(TableInfo::getTableId)
                .collect(toImmutableList());
    }

    public Iterable<TableId> findTableIdsIgnoreCase(ConnectorSession session, DatasetId remoteDatasetId, String tableName)
    {
        try {
            TableResult tableNamesMatchingResults = executeQuery(session, """
                    SELECT table_name
                    FROM %s.%s.INFORMATION_SCHEMA.TABLES
                    WHERE LOWER(table_name) = '%s' AND table_type IN (%s)""".formatted(
                    quote(remoteDatasetId.getProject()),
                    quote(remoteDatasetId.getDataset()),
                    tableName.toLowerCase(ENGLISH),
                    TABLE_TYPES.values().stream().map(value -> "'" + value + "'").collect(Collectors.joining(","))));

            return tableNamesMatchingResults.streamAll()
                    .map(row -> TableId.of(remoteDatasetId.getProject(), remoteDatasetId.getDataset(), row.getFirst().getStringValue()))
                    .collect(toImmutableList());
        }
        catch (TrinoException e) {
            if (e.getMessage().contains("Dataset %s:%s was not found".formatted(remoteDatasetId.getProject(), remoteDatasetId.getDataset()))) {
                // means that schema is missing, let MetadataManager handle it gracefully
                return ImmutableList.of();
            }
            throw e;
        }
    }

    Table update(TableInfo table)
    {
        return bigQuery.update(table);
    }

    public void createSchema(DatasetInfo datasetInfo)
    {
        bigQuery.create(datasetInfo);
        remoteDatasetIdCache.invalidate(datasetInfo.getDatasetId().getProject());
        remoteDatasetCaseInsensitiveCache.invalidate(datasetIdToLowerCase(datasetInfo.getDatasetId()));
    }

    public void dropSchema(DatasetId datasetId, boolean cascade)
    {
        if (cascade) {
            bigQuery.delete(datasetId, DatasetDeleteOption.deleteContents());
        }
        else {
            bigQuery.delete(datasetId);
        }
        remoteDatasetIdCache.invalidate(datasetId.getProject());
        remoteDatasetCaseInsensitiveCache.invalidate(datasetIdToLowerCase(datasetId));
    }

    public void createTable(TableInfo tableInfo)
    {
        bigQuery.create(tableInfo);
        remoteTableCaseInsensitiveCache.invalidate(tableIdToLowerCase(tableInfo.getTableId()));
    }

    public void dropTable(TableId tableId)
    {
        bigQuery.delete(tableId);
        remoteTableCaseInsensitiveCache.invalidate(tableIdToLowerCase(tableId));
    }

    Job create(JobInfo jobInfo)
    {
        return bigQuery.create(jobInfo);
    }

    public long executeUpdate(ConnectorSession session, QueryJobConfiguration job)
    {
        log.debug("Execute query: %s", job.getQuery());
        return execute(session, job).getTotalRows();
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
        catch (BigQueryException | JobException e) {
            throw new TrinoException(BIGQUERY_FAILED_TO_EXECUTE_QUERY, "Failed to run the query: " + firstNonNull(e.getMessage(), e), e);
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

    public boolean useStorageApi(String sql, TableId destinationTable)
    {
        JobInfo jobInfo = JobInfo.of(QueryJobConfiguration.newBuilder(sql).setDryRun(true).setDestinationTable(destinationTable).build());
        try {
            bigQuery.create(jobInfo);
        }
        catch (BigQueryException e) {
            if (e.getMessage().startsWith("Duplicate column names in the result are not supported when a destination table is present.")) {
                return false;
            }
        }
        return true;
    }

    public TableId getDestinationTable(String sql)
    {
        log.debug("Get destination table from query: %s", sql);
        JobInfo jobInfo = JobInfo.of(QueryJobConfiguration.newBuilder(sql).setDryRun(true).build());

        JobConfiguration jobConfiguration;
        try {
            jobConfiguration = bigQuery.create(jobInfo).getConfiguration();
        }
        catch (BigQueryException e) {
            throw new TrinoException(BIGQUERY_INVALID_STATEMENT, "Failed to get destination table for query. " + firstNonNull(e.getMessage(), e), e);
        }

        return requireNonNull(((QueryJobConfiguration) jobConfiguration).getDestinationTable(), "Cannot determine destination table for query");
    }

    public static String selectSql(TableId table, List<BigQueryColumnHandle> requiredColumns, Optional<String> filter)
    {
        return selectSql(table,
                requiredColumns.stream()
                        .map(column -> Joiner.on('.')
                                .join(ImmutableList.<String>builder()
                                        .add(format("`%s`", column.name()))
                                        .addAll(column.dereferenceNames().stream()
                                                .map(dereferenceName -> format("`%s`", dereferenceName))
                                                .collect(toImmutableList()))
                                        .build()))
                        .collect(joining(",")),
                filter);
    }

    public static String selectSql(TableId table, String formattedColumns, Optional<String> filter)
    {
        String tableName = fullTableName(table);
        String query = format("SELECT %s FROM `%s`", formattedColumns, tableName);
        if (filter.isEmpty()) {
            return query;
        }
        return query + " WHERE " + filter.get();
    }

    private static String fullTableName(TableId remoteTableId)
    {
        return format("%s.%s.%s", remoteTableId.getProject(), remoteTableId.getDataset(), remoteTableId.getTable());
    }

    public Stream<RelationCommentMetadata> listRelationCommentMetadata(ConnectorSession session, BigQueryClient client, String schemaName)
    {
        TableResult result = client.executeQuery(session, """
                SELECT tbls.table_name, options.option_value
                FROM %1$s.`INFORMATION_SCHEMA`.`TABLES` tbls
                LEFT JOIN %1$s.`INFORMATION_SCHEMA`.`TABLE_OPTIONS` options
                ON tbls.table_schema = options.table_schema AND tbls.table_name = options.table_name AND options.option_name = 'description'
                """.formatted(quote(schemaName)));
        return result.streamValues()
                .map(row -> {
                    Optional<String> comment = row.get(1).isNull() ? Optional.empty() : Optional.of(unquoteOptionValue(row.get(1).getStringValue()));
                    return new RelationCommentMetadata(new SchemaTableName(schemaName, row.get(0).getStringValue()), false, comment);
                });
    }

    private static String unquoteOptionValue(String quoted)
    {
        // option_value returns quoted string, e.g. "test data"
        return quoted.substring(1, quoted.length() - 1)
                .replace("\"\"", "\"")
                .replace("\\\\", "\\")
                .replace("\\\"", "\"");
    }

    public List<BigQueryColumnHandle> getColumns(BigQueryTableHandle tableHandle)
    {
        if (tableHandle.projectedColumns().isPresent()) {
            return tableHandle.projectedColumns().get();
        }
        checkArgument(tableHandle.isNamedRelation(), "Cannot get columns for %s", tableHandle);

        TableInfo tableInfo = getTable(tableHandle.asPlainTable().getRemoteTableName().toTableId())
                .orElseThrow(() -> new TableNotFoundException(tableHandle.asPlainTable().getSchemaTableName()));
        return buildColumnHandles(tableInfo);
    }

    public List<BigQueryColumnHandle> buildColumnHandles(TableInfo tableInfo)
    {
        Schema schema = tableInfo.getDefinition().getSchema();
        if (schema == null) {
            SchemaTableName schemaTableName = new SchemaTableName(tableInfo.getTableId().getDataset(), tableInfo.getTableId().getTable());
            throw new TableNotFoundException(schemaTableName, format("Table '%s' has no schema", schemaTableName));
        }
        return schema.getFields()
                .stream()
                .filter(typeManager::isSupportedType)
                .map(typeManager::toColumnHandle)
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
