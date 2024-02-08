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
package io.trino.plugin.hive.metastore.glue.v1;

import com.amazonaws.services.glue.AWSGlueAsync;
import com.amazonaws.services.glue.model.ColumnStatistics;
import com.amazonaws.services.glue.model.ColumnStatisticsData;
import com.amazonaws.services.glue.model.ColumnStatisticsType;
import com.amazonaws.services.glue.model.DateColumnStatisticsData;
import com.amazonaws.services.glue.model.DecimalColumnStatisticsData;
import com.amazonaws.services.glue.model.DeleteColumnStatisticsForPartitionRequest;
import com.amazonaws.services.glue.model.DeleteColumnStatisticsForTableRequest;
import com.amazonaws.services.glue.model.DoubleColumnStatisticsData;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.GetColumnStatisticsForPartitionRequest;
import com.amazonaws.services.glue.model.GetColumnStatisticsForPartitionResult;
import com.amazonaws.services.glue.model.GetColumnStatisticsForTableRequest;
import com.amazonaws.services.glue.model.GetColumnStatisticsForTableResult;
import com.amazonaws.services.glue.model.LongColumnStatisticsData;
import com.amazonaws.services.glue.model.UpdateColumnStatisticsForPartitionRequest;
import com.amazonaws.services.glue.model.UpdateColumnStatisticsForTableRequest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.HiveColumnStatistics;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.trino.spi.TrinoException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.difference;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_PARTITION_NOT_FOUND;
import static io.trino.plugin.hive.metastore.glue.v1.converter.GlueStatConverter.fromGlueColumnStatistics;
import static io.trino.plugin.hive.metastore.glue.v1.converter.GlueStatConverter.toGlueColumnStatistics;
import static io.trino.plugin.hive.util.HiveUtil.toPartitionValues;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.stream.Collectors.toUnmodifiableList;

public class DefaultGlueColumnStatisticsProvider
        implements GlueColumnStatisticsProvider
{
    // Read limit for AWS Glue API GetColumnStatisticsForPartition
    // https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-partitions.html#aws-glue-api-catalog-partitions-GetColumnStatisticsForPartition
    private static final int GLUE_COLUMN_READ_STAT_PAGE_SIZE = 100;

    // Write limit for AWS Glue API UpdateColumnStatisticsForPartition
    // https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-partitions.html#aws-glue-api-catalog-partitions-UpdateColumnStatisticsForPartition
    private static final int GLUE_COLUMN_WRITE_STAT_PAGE_SIZE = 25;

    private final GlueMetastoreStats stats;
    private final AWSGlueAsync glueClient;
    private final Executor readExecutor;
    private final Executor writeExecutor;

    public DefaultGlueColumnStatisticsProvider(AWSGlueAsync glueClient, Executor readExecutor, Executor writeExecutor, GlueMetastoreStats stats)
    {
        this.glueClient = glueClient;
        this.readExecutor = readExecutor;
        this.writeExecutor = writeExecutor;
        this.stats = stats;
    }

    @Override
    public Map<String, HiveColumnStatistics> getTableColumnStatistics(String databaseName, String tableName, Set<String> columnNames)
    {
        try {
            List<List<String>> columnChunks = Lists.partition(ImmutableList.copyOf(columnNames), GLUE_COLUMN_READ_STAT_PAGE_SIZE);
            List<CompletableFuture<GetColumnStatisticsForTableResult>> getStatsFutures = columnChunks.stream()
                    .map(partialColumns -> supplyAsync(() -> {
                        GetColumnStatisticsForTableRequest request = new GetColumnStatisticsForTableRequest()
                                .withDatabaseName(databaseName)
                                .withTableName(tableName)
                                .withColumnNames(partialColumns);
                        return stats.getGetColumnStatisticsForTable().call(() -> glueClient.getColumnStatisticsForTable(request));
                    }, readExecutor)).collect(toImmutableList());

            ImmutableMap.Builder<String, HiveColumnStatistics> columnStatsMapBuilder = ImmutableMap.builder();
            for (CompletableFuture<GetColumnStatisticsForTableResult> future : getStatsFutures) {
                GetColumnStatisticsForTableResult tableColumnsStats = getFutureValue(future, TrinoException.class);
                for (ColumnStatistics columnStatistics : tableColumnsStats.getColumnStatisticsList()) {
                    columnStatsMapBuilder.put(
                            columnStatistics.getColumnName(),
                            fromGlueColumnStatistics(columnStatistics.getStatisticsData()));
                }
            }
            return columnStatsMapBuilder.buildOrThrow();
        }
        catch (RuntimeException ex) {
            throw new TrinoException(HIVE_METASTORE_ERROR, ex);
        }
    }

    @Override
    public Map<String, Map<String, HiveColumnStatistics>> getPartitionColumnStatistics(
            String databaseName,
            String tableName,
            Set<String> partitionNames,
            Set<String> columnNames)
    {
        Map<String, List<CompletableFuture<GetColumnStatisticsForPartitionResult>>> resultsForPartition = new HashMap<>();
        for (String partitionName : partitionNames) {
            ImmutableList.Builder<CompletableFuture<GetColumnStatisticsForPartitionResult>> futures = ImmutableList.builder();
            for (List<String> columnBatch : Lists.partition(ImmutableList.copyOf(columnNames), GLUE_COLUMN_READ_STAT_PAGE_SIZE)) {
                GetColumnStatisticsForPartitionRequest request = new GetColumnStatisticsForPartitionRequest()
                        .withDatabaseName(databaseName)
                        .withTableName(tableName)
                        .withColumnNames(columnBatch)
                        .withPartitionValues(toPartitionValues(partitionName));
                futures.add(supplyAsync(() -> stats.getGetColumnStatisticsForPartition().call(() -> glueClient.getColumnStatisticsForPartition(request)), readExecutor));
            }
            resultsForPartition.put(partitionName, futures.build());
        }

        try {
            ImmutableMap.Builder<String, Map<String, HiveColumnStatistics>> partitionStatistics = ImmutableMap.builder();
            resultsForPartition.forEach((partitionName, futures) -> {
                ImmutableMap.Builder<String, HiveColumnStatistics> columnStatsMapBuilder = ImmutableMap.builder();
                for (CompletableFuture<GetColumnStatisticsForPartitionResult> getColumnStatisticsResultFuture : futures) {
                    GetColumnStatisticsForPartitionResult getColumnStatisticsResult = getFutureValue(getColumnStatisticsResultFuture);
                    getColumnStatisticsResult.getColumnStatisticsList().forEach(columnStatistics ->
                            columnStatsMapBuilder.put(
                                    columnStatistics.getColumnName(),
                                    fromGlueColumnStatistics(columnStatistics.getStatisticsData())));
                }

                partitionStatistics.put(partitionName, columnStatsMapBuilder.buildOrThrow());
            });

            return partitionStatistics.buildOrThrow();
        }
        catch (RuntimeException ex) {
            if (ex.getCause() != null && ex.getCause() instanceof EntityNotFoundException) {
                throw new TrinoException(HIVE_PARTITION_NOT_FOUND, ex.getCause());
            }
            throw new TrinoException(HIVE_METASTORE_ERROR, ex);
        }
    }

    // Glue will accept null as min/max values but return 0 when reading
    // to avoid incorrect stats we skip writes for column statistics that have min/max null
    // this can be removed once glue fix this behaviour
    private boolean isGlueWritable(ColumnStatistics stats)
    {
        ColumnStatisticsData statisticsData = stats.getStatisticsData();
        String columnType = stats.getStatisticsData().getType();
        if (columnType.equals(ColumnStatisticsType.DATE.toString())) {
            DateColumnStatisticsData data = statisticsData.getDateColumnStatisticsData();
            return data.getMaximumValue() != null && data.getMinimumValue() != null;
        }
        if (columnType.equals(ColumnStatisticsType.DECIMAL.toString())) {
            DecimalColumnStatisticsData data = statisticsData.getDecimalColumnStatisticsData();
            return data.getMaximumValue() != null && data.getMinimumValue() != null;
        }
        if (columnType.equals(ColumnStatisticsType.DOUBLE.toString())) {
            DoubleColumnStatisticsData data = statisticsData.getDoubleColumnStatisticsData();
            return data.getMaximumValue() != null && data.getMinimumValue() != null;
        }
        if (columnType.equals(ColumnStatisticsType.LONG.toString())) {
            LongColumnStatisticsData data = statisticsData.getLongColumnStatisticsData();
            return data.getMaximumValue() != null && data.getMinimumValue() != null;
        }
        return true;
    }

    @Override
    public void updateTableColumnStatistics(Table table, Map<String, HiveColumnStatistics> updatedTableColumnStatistics)
    {
        try {
            List<ColumnStatistics> columnStats = toGlueColumnStatistics(table, updatedTableColumnStatistics).stream()
                    .filter(this::isGlueWritable)
                    .collect(toUnmodifiableList());

            List<List<ColumnStatistics>> columnChunks = Lists.partition(columnStats, GLUE_COLUMN_WRITE_STAT_PAGE_SIZE);

            List<CompletableFuture<Void>> updateFutures = columnChunks.stream().map(columnChunk -> runAsync(
                            () -> stats.getUpdateColumnStatisticsForTable().call(() -> glueClient.updateColumnStatisticsForTable(
                                    new UpdateColumnStatisticsForTableRequest()
                                            .withDatabaseName(table.getDatabaseName())
                                            .withTableName(table.getTableName())
                                            .withColumnStatisticsList(columnChunk))), this.writeExecutor))
                    .collect(toUnmodifiableList());

            Set<String> removedStatistics = difference(ImmutableSet.copyOf(getAllColumns(table)), updatedTableColumnStatistics.keySet());
            List<CompletableFuture<Void>> deleteFutures = removedStatistics.stream()
                    .map(column -> runAsync(() -> stats.getDeleteColumnStatisticsForTable().call(() -> {
                        try {
                            glueClient.deleteColumnStatisticsForTable(
                                    new DeleteColumnStatisticsForTableRequest()
                                            .withDatabaseName(table.getDatabaseName())
                                            .withTableName(table.getTableName())
                                            .withColumnName(column));
                        }
                        catch (EntityNotFoundException ignored) {
                        }
                        return null;
                    }), this.writeExecutor))
                    .collect(toUnmodifiableList());

            ImmutableList<CompletableFuture<Void>> updateOperationsFutures = ImmutableList.<CompletableFuture<Void>>builder()
                    .addAll(updateFutures)
                    .addAll(deleteFutures)
                    .build();

            getFutureValue(allOf(updateOperationsFutures.toArray(CompletableFuture[]::new)));
        }
        catch (RuntimeException ex) {
            throw new TrinoException(HIVE_METASTORE_ERROR, ex);
        }
    }

    @Override
    public void updatePartitionStatistics(Set<PartitionStatisticsUpdate> partitionStatisticsUpdates)
    {
        List<CompletableFuture<Void>> updateFutures = new ArrayList<>();
        for (PartitionStatisticsUpdate update : partitionStatisticsUpdates) {
            Partition partition = update.getPartition();
            Map<String, HiveColumnStatistics> updatedColumnStatistics = update.getColumnStatistics();

            List<ColumnStatistics> columnStats = toGlueColumnStatistics(partition, updatedColumnStatistics).stream()
                    .filter(this::isGlueWritable)
                    .collect(toUnmodifiableList());

            List<List<ColumnStatistics>> columnChunks = Lists.partition(columnStats, GLUE_COLUMN_WRITE_STAT_PAGE_SIZE);
            columnChunks.forEach(columnChunk ->
                    updateFutures.add(runAsync(() -> stats.getUpdateColumnStatisticsForPartition().call(() ->
                                    glueClient.updateColumnStatisticsForPartition(
                                            new UpdateColumnStatisticsForPartitionRequest()
                                                    .withDatabaseName(partition.getDatabaseName())
                                                    .withTableName(partition.getTableName())
                                                    .withPartitionValues(partition.getValues())
                                                    .withColumnStatisticsList(columnChunk))),
                            writeExecutor)));

            Set<String> removedStatistics = difference(partition.getColumns().stream().map(Column::getName).collect(toImmutableSet()), updatedColumnStatistics.keySet());
            removedStatistics.forEach(column ->
                    updateFutures.add(runAsync(() -> stats.getDeleteColumnStatisticsForPartition().call(() ->
                                    glueClient.deleteColumnStatisticsForPartition(
                                            new DeleteColumnStatisticsForPartitionRequest()
                                                    .withDatabaseName(partition.getDatabaseName())
                                                    .withTableName(partition.getTableName())
                                                    .withPartitionValues(partition.getValues())
                                                    .withColumnName(column))),
                            writeExecutor)));
        }
        try {
            getFutureValue(allOf(updateFutures.toArray(CompletableFuture[]::new)));
        }
        catch (RuntimeException ex) {
            if (ex.getCause() != null && ex.getCause() instanceof EntityNotFoundException) {
                throw new TrinoException(HIVE_PARTITION_NOT_FOUND, ex.getCause());
            }
            throw new TrinoException(HIVE_METASTORE_ERROR, ex);
        }
    }

    private Set<String> getAllColumns(Table table)
    {
        ImmutableSet.Builder<String> allColumns = ImmutableSet.builderWithExpectedSize(table.getDataColumns().size() + table.getPartitionColumns().size());
        table.getDataColumns().stream().map(Column::getName).forEach(allColumns::add);
        table.getPartitionColumns().stream().map(Column::getName).forEach(allColumns::add);
        return allColumns.build();
    }
}
