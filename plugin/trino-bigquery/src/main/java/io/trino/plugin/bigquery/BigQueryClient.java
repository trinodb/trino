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
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.http.BaseHttpServiceException;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.TableNotFoundException;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.cloud.bigquery.TableDefinition.Type.TABLE;
import static com.google.cloud.bigquery.TableDefinition.Type.VIEW;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Streams.stream;
import static io.trino.plugin.bigquery.BigQueryErrorCode.BIGQUERY_AMBIGUOUS_OBJECT_NAME;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.joining;

class BigQueryClient
{
    private final BigQuery bigQuery;
    private final Optional<String> viewMaterializationProject;
    private final Optional<String> viewMaterializationDataset;
    private final boolean caseInsensitiveNameMatching;
    private final Cache<String, Optional<RemoteDatabaseObject>> remoteDatasets;
    private final Cache<TableId, Optional<RemoteDatabaseObject>> remoteTables;

    BigQueryClient(BigQuery bigQuery, BigQueryConfig config)
    {
        this.bigQuery = bigQuery;
        this.viewMaterializationProject = config.getViewMaterializationProject();
        this.viewMaterializationDataset = config.getViewMaterializationDataset();
        Duration caseInsensitiveNameMatchingCacheTtl = requireNonNull(config.getCaseInsensitiveNameMatchingCacheTtl(), "caseInsensitiveNameMatchingCacheTtl is null");

        this.caseInsensitiveNameMatching = config.isCaseInsensitiveNameMatching();
        CacheBuilder<Object, Object> remoteNamesCacheBuilder = CacheBuilder.newBuilder()
                .expireAfterWrite(caseInsensitiveNameMatchingCacheTtl.toMillis(), MILLISECONDS);
        this.remoteDatasets = remoteNamesCacheBuilder.build();
        this.remoteTables = remoteNamesCacheBuilder.build();
    }

    Optional<RemoteDatabaseObject> toRemoteDataset(String projectId, String datasetName)
    {
        requireNonNull(projectId, "projectId is null");
        requireNonNull(datasetName, "datasetName is null");
        verify(datasetName.codePoints().noneMatch(Character::isUpperCase), "Expected schema name from internal metadata to be lowercase: %s", datasetName);
        if (!caseInsensitiveNameMatching) {
            return Optional.of(RemoteDatabaseObject.of(datasetName));
        }

        Optional<RemoteDatabaseObject> remoteDataset = remoteDatasets.getIfPresent(datasetName);
        if (remoteDataset != null) {
            return remoteDataset;
        }

        // cache miss, reload the cache
        Map<String, Optional<RemoteDatabaseObject>> mapping = new HashMap<>();
        for (Dataset dataset : listDatasets(projectId)) {
            mapping.merge(
                    dataset.getDatasetId().getDataset().toLowerCase(ENGLISH),
                    Optional.of(RemoteDatabaseObject.of(dataset.getDatasetId().getDataset())),
                    (currentValue, collision) -> currentValue.map(current -> current.registerCollision(collision.get().getOnlyRemoteName())));
        }

        // explicitly cache the information if the requested dataset doesn't exist
        if (!mapping.containsKey(datasetName)) {
            mapping.put(datasetName, Optional.empty());
        }

        verify(mapping.containsKey(datasetName));
        return mapping.get(datasetName);
    }

    Optional<RemoteDatabaseObject> toRemoteTable(String projectId, String remoteDatasetName, String tableName)
    {
        return toRemoteTable(projectId, remoteDatasetName, tableName, () -> listTables(DatasetId.of(projectId, remoteDatasetName), TABLE, VIEW));
    }

    Optional<RemoteDatabaseObject> toRemoteTable(String projectId, String remoteDatasetName, String tableName, Iterable<Table> tables)
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
        Optional<RemoteDatabaseObject> remoteTable = remoteTables.getIfPresent(cacheKey);
        if (remoteTable != null) {
            return remoteTable;
        }

        // cache miss, reload the cache
        Map<TableId, Optional<RemoteDatabaseObject>> mapping = new HashMap<>();
        for (Table table : tables.get()) {
            mapping.merge(
                    tableIdToLowerCase(table.getTableId()),
                    Optional.of(RemoteDatabaseObject.of(table.getTableId().getTable())),
                    (currentValue, collision) -> currentValue.map(current -> current.registerCollision(collision.get().getOnlyRemoteName())));
        }

        // explicitly cache the information if the requested table doesn't exist
        if (!mapping.containsKey(cacheKey)) {
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

    DatasetInfo getDataset(DatasetId datasetId)
    {
        return bigQuery.getDataset(datasetId);
    }

    TableInfo getTable(TableId remoteTableId)
    {
        // TODO: Return Optional and make callers handle missing value
        return bigQuery.getTable(remoteTableId);
    }

    String getProjectId()
    {
        return bigQuery.getOptions().getProjectId();
    }

    Iterable<Dataset> listDatasets(String projectId)
    {
        return bigQuery.listDatasets(projectId).iterateAll();
    }

    Iterable<Table> listTables(DatasetId remoteDatasetId, TableDefinition.Type... types)
    {
        Set<TableDefinition.Type> allowedTypes = ImmutableSet.copyOf(types);
        Iterable<Table> allTables = bigQuery.listTables(remoteDatasetId).iterateAll();
        return stream(allTables)
                .filter(table -> allowedTypes.contains(table.getDefinition().getType()))
                .collect(toImmutableList());
    }

    TableId createDestinationTable(TableId tableId)
    {
        String project = viewMaterializationProject.orElse(tableId.getProject());
        String dataset = viewMaterializationDataset.orElse(tableId.getDataset());

        String remoteDatasetName = toRemoteDataset(project, dataset)
                .map(RemoteDatabaseObject::getOnlyRemoteName)
                .orElse(dataset);

        DatasetId datasetId = DatasetId.of(project, remoteDatasetName);
        String name = format("_pbc_%s", randomUUID().toString().toLowerCase(ENGLISH).replace("-", ""));
        return TableId.of(datasetId.getProject(), datasetId.getDataset(), name);
    }

    Table update(TableInfo table)
    {
        return bigQuery.update(table);
    }

    public void createSchema(DatasetInfo datasetInfo)
    {
        bigQuery.create(datasetInfo);
    }

    public void dropSchema(DatasetId datasetId)
    {
        bigQuery.delete(datasetId);
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

    TableResult query(String sql)
    {
        try {
            return bigQuery.query(QueryJobConfiguration.of(sql));
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BigQueryException(BaseHttpServiceException.UNKNOWN_CODE, format("Failed to run the query [%s]", sql), e);
        }
    }

    String selectSql(TableId table, List<String> requiredColumns)
    {
        String columns = requiredColumns.isEmpty() ? "*" :
                requiredColumns.stream().map(column -> format("`%s`", column)).collect(joining(","));

        return selectSql(table, columns);
    }

    // assuming the SELECT part is properly formatted, can be used to call functions such as COUNT and SUM
    String selectSql(TableId table, String formattedColumns)
    {
        String tableName = fullTableName(table);
        return format("SELECT %s FROM `%s`", formattedColumns, tableName);
    }

    private String fullTableName(TableId tableId)
    {
        String remoteSchemaName = toRemoteDataset(tableId.getProject(), tableId.getDataset())
                .map(RemoteDatabaseObject::getOnlyRemoteName)
                .orElse(tableId.getDataset());
        String remoteTableName = toRemoteTable(tableId.getProject(), remoteSchemaName, tableId.getTable())
                .map(RemoteDatabaseObject::getOnlyRemoteName)
                .orElse(tableId.getTable());
        tableId = TableId.of(tableId.getProject(), remoteSchemaName, remoteTableName);
        return format("%s.%s.%s", tableId.getProject(), tableId.getDataset(), tableId.getTable());
    }

    List<BigQueryColumnHandle> getColumns(BigQueryTableHandle tableHandle)
    {
        TableInfo tableInfo = getTable(tableHandle.getRemoteTableName().toTableId());
        if (tableInfo == null) {
            throw new TableNotFoundException(
                    tableHandle.getSchemaTableName(),
                    format("Table '%s' not found", tableHandle.getSchemaTableName()));
        }
        Schema schema = tableInfo.getDefinition().getSchema();
        if (schema == null) {
            throw new TableNotFoundException(
                    tableHandle.getSchemaTableName(),
                    format("Table '%s' has no schema", tableHandle.getSchemaTableName()));
        }
        return schema.getFields()
                .stream()
                .map(Conversions::toColumnHandle)
                .collect(toImmutableList());
    }

    static final class RemoteDatabaseObject
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
