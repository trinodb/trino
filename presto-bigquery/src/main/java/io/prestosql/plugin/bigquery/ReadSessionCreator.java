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
package io.prestosql.plugin.bigquery;

import com.google.cloud.BaseServiceException;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.storage.v1beta1.BigQueryStorageClient;
import com.google.cloud.bigquery.storage.v1beta1.ReadOptions;
import com.google.cloud.bigquery.storage.v1beta1.Storage;
import com.google.cloud.bigquery.storage.v1beta1.TableReferenceProto;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.prestosql.spi.PrestoException;

import javax.annotation.Nonnull;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.prestosql.plugin.bigquery.BigQueryErrorCode.VIEW_DESTINATION_TABLE_CREATION_FAILED;
import static java.lang.String.format;
import static java.util.UUID.randomUUID;

// A helper class, also handles view materialization
public class ReadSessionCreator
{
    private static final Logger log = Logger.get(ReadSessionCreator.class);

    private static Cache<String, TableInfo> destinationTableCache =
            CacheBuilder.newBuilder()
                    .expireAfterWrite(15, TimeUnit.MINUTES)
                    .maximumSize(1000)
                    .build();

    private final BigQueryConfig config;
    private final BigQuery bigquery;
    private final BigQueryStorageClientFactory bigQueryStorageClientFactory;

    public ReadSessionCreator(
            @Nonnull BigQueryConfig config,
            @Nonnull BigQuery bigquery,
            @Nonnull BigQueryStorageClientFactory bigQueryStorageClientFactory)
    {
        this.config = config;
        this.bigquery = bigquery;
        this.bigQueryStorageClientFactory = bigQueryStorageClientFactory;
    }

    public Storage.ReadSession create(TableId table, ImmutableList<String> selectedFields, String filter, int parallelism)
    {
        Table tableDetails = bigquery.getTable(table);

        TableInfo actualTable = getActualTable(tableDetails, selectedFields, new String[] {});

        try (BigQueryStorageClient bigQueryStorageClient = bigQueryStorageClientFactory.createBigQueryStorageClient()) {
            ReadOptions.TableReadOptions readOptions = ReadOptions.TableReadOptions.newBuilder()
                    .addAllSelectedFields(selectedFields)
                    //.setRowRestriction(filter)
                    .build();
            TableReferenceProto.TableReference tableReference = toTableReference(actualTable.getTableId());

            final Storage.ReadSession readSession = bigQueryStorageClient.createReadSession(
                    Storage.CreateReadSessionRequest.newBuilder()
                            .setParent("projects/" + config.getParentProject())
                            .setFormat(Storage.DataFormat.AVRO)
                            .setRequestedStreams(parallelism)
                            .setReadOptions(readOptions)
                            .setTableReference(tableReference)
                            // The BALANCED sharding strategy causes the server to assign roughly the same
                            // number of rows to each stream.
                            .setShardingStrategy(Storage.ShardingStrategy.BALANCED)
                            .build());

            return readSession;
        }
    }

    TableReferenceProto.TableReference toTableReference(TableId tableId)
    {
        return TableReferenceProto.TableReference.newBuilder()
                .setProjectId(tableId.getProject())
                .setDatasetId(tableId.getDataset())
                .setTableId(tableId.getTable())
                .build();
    }

    TableInfo getActualTable(
            TableInfo table,
            ImmutableList<String> requiredColumns,
            String[] filters)
    {
        TableDefinition tableDefinition = table.getDefinition();
        TableDefinition.Type tableType = tableDefinition.getType();
        if (TableDefinition.Type.TABLE == tableType) {
            return table;
        }
        else if (TableDefinition.Type.VIEW == tableType) {
            if (config.isViewsEnabled()) {
                // get it from the view
                String querySql = createSql(table.getTableId(), requiredColumns, filters);
                log.debug("querySql is %s", querySql);
                try {
                    return destinationTableCache.get(querySql, new DestinationTableBuilder(bigquery, config, querySql, table.getTableId()));
                }
                catch (ExecutionException e) {
                    throw new PrestoException(VIEW_DESTINATION_TABLE_CREATION_FAILED, "Error creating destination table", e);
                }
            }
            else {
                throw new IllegalArgumentException(format(
                        "Views were not enabled. You can enable views by setting '%s' to true. Notice additional cost may occur.",
                        BigQueryConfig.VIEWS_ENABLED));
            }
        }
        else {
            // not regular table or a view
            throw new IllegalArgumentException(format("Table type '%s' of table '%s.%s' is not supported",
                    tableType, table.getTableId().getDataset(), table.getTableId().getTable()));
        }
    }

    String createSql(TableId table, ImmutableList<String> requiredColumns, String[] filters)
    {
        String tableName = format("%s.%s.%s", table.getProject(), table.getDataset(), table.getTable());
        String columns = (requiredColumns.isEmpty()) ? "*" :
                requiredColumns.stream().map(column -> format("`%s`", column)).collect(Collectors.joining(","));

        String whereClause = createWhereClause(filters)
                .map(clause -> "WHERE " + clause)
                .orElse("");

        return format("SELECT %s FROM `%s` %s", columns, tableName, whereClause);
    }

    // return empty if no filters are used
    Optional<String> createWhereClause(String[] filters)
    {
        return Optional.empty();
    }

    static class DestinationTableBuilder
            implements Callable<TableInfo>
    {
        final BigQuery bigquery;
        final BigQueryConfig config;
        final String querySql;
        final TableId table;

        DestinationTableBuilder(BigQuery bigquery, BigQueryConfig config, String querySql, TableId table)
        {
            this.bigquery = bigquery;
            this.config = config;
            this.querySql = querySql;
            this.table = table;
        }

        @Override
        public TableInfo call()
        {
            return createTableFromQuery();
        }

        TableInfo createTableFromQuery()
        {
            TableId destinationTable = createDestinationTable();
            log.debug("destinationTable is %s", destinationTable);
            JobInfo jobInfo = JobInfo.of(
                    QueryJobConfiguration
                            .newBuilder(querySql)
                            .setDestinationTable(destinationTable)
                            .build());
            log.debug("running query %s", jobInfo);
            Job job = waitForJob(bigquery.create(jobInfo));
            log.debug("job has finished. %s", job);
            if (job.getStatus().getError() != null) {
                BigQueryUtil.convertAndThrow(job.getStatus().getError());
            }
            // add expiration time to the table
            Table createdTable = bigquery.getTable(destinationTable);
            long expirationTime = createdTable.getCreationTime() +
                    TimeUnit.HOURS.toMillis(config.getViewExpirationTimeInHours());
            Table updatedTable = bigquery.update(createdTable.toBuilder()
                    .setExpirationTime(expirationTime)
                    .build());
            return updatedTable;
        }

        Job waitForJob(Job job)
        {
            try {
                return job.waitFor();
            }
            catch (InterruptedException e) {
                throw new BigQueryException(BaseServiceException.UNKNOWN_CODE, format("Job %s has been interrupted", job.getJobId()), e);
            }
        }

        TableId createDestinationTable()
        {
            String project = config.getViewMaterializationProject().orElse(table.getProject());
            String dataset = config.getViewMaterializationDataset().orElse(table.getDataset());
            UUID uuid = randomUUID();
            String name = format("_sbc_%s%s", Long.toHexString(uuid.getMostSignificantBits()), Long.toHexString(uuid.getLeastSignificantBits()));
            return TableId.of(project, dataset, name);
        }
    }
}
