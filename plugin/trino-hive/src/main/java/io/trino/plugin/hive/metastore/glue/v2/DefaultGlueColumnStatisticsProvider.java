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
package io.trino.plugin.hive.metastore.glue.v2;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.trino.metastore.Column;
import io.trino.metastore.HiveColumnStatistics;
import io.trino.metastore.Partition;
import io.trino.metastore.Table;
import io.trino.spi.TrinoException;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.ColumnStatistics;
import software.amazon.awssdk.services.glue.model.ColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.ColumnStatisticsType;
import software.amazon.awssdk.services.glue.model.DateColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.DecimalColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.DeleteColumnStatisticsForPartitionRequest;
import software.amazon.awssdk.services.glue.model.DeleteColumnStatisticsForTableRequest;
import software.amazon.awssdk.services.glue.model.DoubleColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetColumnStatisticsForPartitionRequest;
import software.amazon.awssdk.services.glue.model.GetColumnStatisticsForPartitionResponse;
import software.amazon.awssdk.services.glue.model.GetColumnStatisticsForTableRequest;
import software.amazon.awssdk.services.glue.model.GetColumnStatisticsForTableResponse;
import software.amazon.awssdk.services.glue.model.LongColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.UpdateColumnStatisticsForPartitionRequest;
import software.amazon.awssdk.services.glue.model.UpdateColumnStatisticsForTableRequest;

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
import static io.trino.metastore.Partition.toPartitionValues;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_PARTITION_NOT_FOUND;
import static io.trino.plugin.hive.metastore.glue.v2.converter.GlueStatConverter.fromGlueColumnStatistics;
import static io.trino.plugin.hive.metastore.glue.v2.converter.GlueStatConverter.toGlueColumnStatistics;
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
    private final GlueClient glueClient;
    private final Executor readExecutor;
    private final Executor writeExecutor;

    public DefaultGlueColumnStatisticsProvider(GlueClient glueClient, Executor readExecutor, Executor writeExecutor)
    {
        this.glueClient = glueClient;
        this.readExecutor = readExecutor;
        this.writeExecutor = writeExecutor;
    }

    @Override
    public Map<String, HiveColumnStatistics> getTableColumnStatistics(String databaseName, String tableName, Set<String> columnNames)
    {
        try {
            List<List<String>> columnChunks = Lists.partition(ImmutableList.copyOf(columnNames), GLUE_COLUMN_READ_STAT_PAGE_SIZE);
            List<CompletableFuture<GetColumnStatisticsForTableResponse>> getStatsFutures = columnChunks.stream()
                    .map(partialColumns -> supplyAsync(() -> {
                        GetColumnStatisticsForTableRequest request = GetColumnStatisticsForTableRequest.builder()
                                .databaseName(databaseName)
                                .tableName(tableName)
                                .columnNames(partialColumns)
                                .build();
                        return glueClient.getColumnStatisticsForTable(request);
                    }, readExecutor)).collect(toImmutableList());

            ImmutableMap.Builder<String, HiveColumnStatistics> columnStatsMapBuilder = ImmutableMap.builder();
            for (CompletableFuture<GetColumnStatisticsForTableResponse> future : getStatsFutures) {
                GetColumnStatisticsForTableResponse tableColumnsStats = getFutureValue(future, TrinoException.class);
                for (ColumnStatistics columnStatistics : tableColumnsStats.columnStatisticsList()) {
                    columnStatsMapBuilder.put(
                            columnStatistics.columnName(),
                            fromGlueColumnStatistics(columnStatistics.statisticsData()));
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
        Map<String, List<CompletableFuture<GetColumnStatisticsForPartitionResponse>>> resultsForPartition = new HashMap<>();
        for (String partitionName : partitionNames) {
            ImmutableList.Builder<CompletableFuture<GetColumnStatisticsForPartitionResponse>> futures = ImmutableList.builder();
            for (List<String> columnBatch : Lists.partition(ImmutableList.copyOf(columnNames), GLUE_COLUMN_READ_STAT_PAGE_SIZE)) {
                GetColumnStatisticsForPartitionRequest request = GetColumnStatisticsForPartitionRequest.builder()
                        .databaseName(databaseName)
                        .tableName(tableName)
                        .columnNames(columnBatch)
                        .partitionValues(toPartitionValues(partitionName))
                        .build();
                futures.add(supplyAsync(() -> glueClient.getColumnStatisticsForPartition(request), readExecutor));
            }
            resultsForPartition.put(partitionName, futures.build());
        }

        try {
            ImmutableMap.Builder<String, Map<String, HiveColumnStatistics>> partitionStatistics = ImmutableMap.builder();
            resultsForPartition.forEach((partitionName, futures) -> {
                ImmutableMap.Builder<String, HiveColumnStatistics> columnStatsMapBuilder = ImmutableMap.builder();
                for (CompletableFuture<GetColumnStatisticsForPartitionResponse> getColumnStatisticsResultFuture : futures) {
                    GetColumnStatisticsForPartitionResponse getColumnStatisticsResult = getFutureValue(getColumnStatisticsResultFuture);
                    getColumnStatisticsResult.columnStatisticsList().forEach(columnStatistics ->
                            columnStatsMapBuilder.put(
                                    columnStatistics.columnName(),
                                    fromGlueColumnStatistics(columnStatistics.statisticsData())));
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
        ColumnStatisticsData statisticsData = stats.statisticsData();
        ColumnStatisticsType columnType = stats.statisticsData().type();

        return switch (columnType) {
            case DATE -> {
                DateColumnStatisticsData data = statisticsData.dateColumnStatisticsData();
                yield data.maximumValue() != null && data.minimumValue() != null;
            }
            case DECIMAL -> {
                DecimalColumnStatisticsData data = statisticsData.decimalColumnStatisticsData();
                yield data.maximumValue() != null && data.minimumValue() != null;
            }
            case DOUBLE -> {
                DoubleColumnStatisticsData data = statisticsData.doubleColumnStatisticsData();
                yield data.maximumValue() != null && data.minimumValue() != null;
            }

            case LONG -> {
                LongColumnStatisticsData data = statisticsData.longColumnStatisticsData();
                yield data.maximumValue() != null && data.minimumValue() != null;
            }
            case BINARY, BOOLEAN, STRING -> true;
            case UNKNOWN_TO_SDK_VERSION -> false;
        };
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
                            () -> glueClient.updateColumnStatisticsForTable(
                                    UpdateColumnStatisticsForTableRequest.builder()
                                            .databaseName(table.getDatabaseName())
                                            .tableName(table.getTableName())
                                            .columnStatisticsList(columnChunk)
                                            .build()), this.writeExecutor))
                    .collect(toUnmodifiableList());

            Set<String> removedStatistics = difference(ImmutableSet.copyOf(getAllColumns(table)), updatedTableColumnStatistics.keySet());
            List<CompletableFuture<Void>> deleteFutures = removedStatistics.stream()
                    .map(column -> runAsync(() -> {
                        try {
                            glueClient.deleteColumnStatisticsForTable(
                                    DeleteColumnStatisticsForTableRequest.builder()
                                            .databaseName(table.getDatabaseName())
                                            .tableName(table.getTableName())
                                            .columnName(column)
                                            .build());
                        }
                        catch (EntityNotFoundException _) {
                        }
                    }, this.writeExecutor))
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
                    updateFutures.add(runAsync(() -> glueClient.updateColumnStatisticsForPartition(
                                            UpdateColumnStatisticsForPartitionRequest.builder()
                                                    .databaseName(partition.getDatabaseName())
                                                    .tableName(partition.getTableName())
                                                    .partitionValues(partition.getValues())
                                                    .columnStatisticsList(columnChunk)
                                                    .build()),
                            writeExecutor)));

            Set<String> removedStatistics = difference(partition.getColumns().stream().map(Column::getName).collect(toImmutableSet()), updatedColumnStatistics.keySet());
            removedStatistics.forEach(column ->
                    updateFutures.add(runAsync(() -> glueClient.deleteColumnStatisticsForPartition(
                                            DeleteColumnStatisticsForPartitionRequest.builder()
                                                    .databaseName(partition.getDatabaseName())
                                                    .tableName(partition.getTableName())
                                                    .partitionValues(partition.getValues())
                                                    .columnName(column)
                                                    .build()),
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
