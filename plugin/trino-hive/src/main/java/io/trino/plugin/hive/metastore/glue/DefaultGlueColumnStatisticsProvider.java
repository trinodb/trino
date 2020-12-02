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
package io.trino.plugin.hive.metastore.glue;

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
import com.google.common.collect.Lists;
import io.trino.plugin.hive.HiveBasicStatistics;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.HiveColumnStatistics;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil;
import io.trino.spi.TrinoException;
import io.trino.spi.statistics.ColumnStatisticType;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Sets.difference;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_PARTITION_NOT_FOUND;
import static io.trino.plugin.hive.metastore.glue.converter.GlueStatConverter.fromGlueColumnStatistics;
import static io.trino.plugin.hive.metastore.glue.converter.GlueStatConverter.toGlueColumnStatistics;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.getHiveBasicStatistics;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.runAsync;
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

    private final AWSGlueAsync glueClient;
    private final String catalogId;
    private final Executor readExecutor;
    private final Executor writeExecutor;

    public DefaultGlueColumnStatisticsProvider(AWSGlueAsync glueClient, String catalogId, Executor readExecutor, Executor writeExecutor)
    {
        this.glueClient = glueClient;
        this.catalogId = catalogId;
        this.readExecutor = readExecutor;
        this.writeExecutor = writeExecutor;
    }

    @Override
    public Set<ColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        return ThriftMetastoreUtil.getSupportedColumnStatistics(type);
    }

    @Override
    public Map<String, HiveColumnStatistics> getTableColumnStatistics(Table table)
    {
        try {
            List<String> columnNames = getAllColumns(table);
            List<List<String>> columnChunks = Lists.partition(columnNames, GLUE_COLUMN_READ_STAT_PAGE_SIZE);
            List<CompletableFuture<GetColumnStatisticsForTableResult>> getStatsFutures = columnChunks.stream()
                    .map(partialColumns -> CompletableFuture.supplyAsync(() -> {
                        GetColumnStatisticsForTableRequest request = new GetColumnStatisticsForTableRequest()
                                .withCatalogId(catalogId)
                                .withDatabaseName(table.getDatabaseName())
                                .withTableName(table.getTableName())
                                .withColumnNames(partialColumns);
                        return glueClient.getColumnStatisticsForTable(request);
                    }, readExecutor)).collect(toImmutableList());

            HiveBasicStatistics tableStatistics = getHiveBasicStatistics(table.getParameters());
            ImmutableMap.Builder<String, HiveColumnStatistics> columnStatsMapBuilder = ImmutableMap.builder();
            for (CompletableFuture<GetColumnStatisticsForTableResult> future : getStatsFutures) {
                GetColumnStatisticsForTableResult tableColumnsStats = getFutureValue(future, TrinoException.class);
                for (ColumnStatistics columnStatistics : tableColumnsStats.getColumnStatisticsList()) {
                    columnStatsMapBuilder.put(
                            columnStatistics.getColumnName(),
                            fromGlueColumnStatistics(columnStatistics.getStatisticsData(), tableStatistics.getRowCount()));
                }
            }
            return columnStatsMapBuilder.build();
        }
        catch (RuntimeException ex) {
            throw new TrinoException(HIVE_METASTORE_ERROR, ex);
        }
    }

    private Optional<Map<String, HiveColumnStatistics>> getPartitionColumnStatisticsIfPresent(Partition partition)
    {
        try {
            return Optional.of(getPartitionColumnStatistics(partition));
        }
        catch (TrinoException ex) {
            if (ex.getErrorCode() == HIVE_PARTITION_NOT_FOUND.toErrorCode()) {
                return Optional.empty();
            }
            throw ex;
        }
    }

    @Override
    public Map<String, HiveColumnStatistics> getPartitionColumnStatistics(Partition partition)
    {
        try {
            List<List<Column>> columnChunks = Lists.partition(partition.getColumns(), GLUE_COLUMN_READ_STAT_PAGE_SIZE);
            List<CompletableFuture<GetColumnStatisticsForPartitionResult>> getStatsFutures = columnChunks.stream()
                    .map(partialColumns -> CompletableFuture.supplyAsync(() -> {
                        List<String> columnsNames = partialColumns.stream()
                                .map(Column::getName)
                                .collect(toImmutableList());
                        GetColumnStatisticsForPartitionRequest request = new GetColumnStatisticsForPartitionRequest()
                                .withCatalogId(catalogId)
                                .withDatabaseName(partition.getDatabaseName())
                                .withTableName(partition.getTableName())
                                .withColumnNames(columnsNames)
                                .withPartitionValues(partition.getValues());
                        return glueClient.getColumnStatisticsForPartition(request);
                    }, readExecutor)).collect(toImmutableList());

            HiveBasicStatistics tableStatistics = getHiveBasicStatistics(partition.getParameters());
            ImmutableMap.Builder<String, HiveColumnStatistics> columnStatsMapBuilder = ImmutableMap.builder();
            for (CompletableFuture<GetColumnStatisticsForPartitionResult> future : getStatsFutures) {
                GetColumnStatisticsForPartitionResult partitionColumnStats = getFutureValue(future, TrinoException.class);
                for (ColumnStatistics columnStatistics : partitionColumnStats.getColumnStatisticsList()) {
                    columnStatsMapBuilder.put(
                            columnStatistics.getColumnName(),
                            fromGlueColumnStatistics(columnStatistics.getStatisticsData(), tableStatistics.getRowCount()));
                }
            }
            return columnStatsMapBuilder.build();
        }
        catch (EntityNotFoundException ex) {
            throw new TrinoException(HIVE_PARTITION_NOT_FOUND, ex);
        }
        catch (RuntimeException ex) {
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
        else if (columnType.equals(ColumnStatisticsType.DECIMAL.toString())) {
            DecimalColumnStatisticsData data = statisticsData.getDecimalColumnStatisticsData();
            return data.getMaximumValue() != null && data.getMinimumValue() != null;
        }
        else if (columnType.equals(ColumnStatisticsType.DOUBLE.toString())) {
            DoubleColumnStatisticsData data = statisticsData.getDoubleColumnStatisticsData();
            return data.getMaximumValue() != null && data.getMinimumValue() != null;
        }
        else if (columnType.equals(ColumnStatisticsType.LONG.toString())) {
            LongColumnStatisticsData data = statisticsData.getLongColumnStatisticsData();
            return data.getMaximumValue() != null && data.getMinimumValue() != null;
        }
        return true;
    }

    @Override
    public void updateTableColumnStatistics(Table table, Map<String, HiveColumnStatistics> updatedTableColumnStatistics)
    {
        try {
            HiveBasicStatistics tableStats = getHiveBasicStatistics(table.getParameters());
            List<ColumnStatistics> columnStats = toGlueColumnStatistics(table, updatedTableColumnStatistics, tableStats.getRowCount()).stream()
                    .filter(this::isGlueWritable)
                    .collect(toUnmodifiableList());

            List<List<ColumnStatistics>> columnChunks = Lists.partition(columnStats, GLUE_COLUMN_WRITE_STAT_PAGE_SIZE);

            List<CompletableFuture<Void>> updateFutures = columnChunks.stream().map(columnChunk -> runAsync(
                    () -> glueClient.updateColumnStatisticsForTable(
                            new UpdateColumnStatisticsForTableRequest()
                                    .withCatalogId(catalogId)
                                    .withDatabaseName(table.getDatabaseName())
                                    .withTableName(table.getTableName())
                                    .withColumnStatisticsList(columnChunk)), this.writeExecutor))
                    .collect(toUnmodifiableList());

            Map<String, HiveColumnStatistics> currentTableColumnStatistics = this.getTableColumnStatistics(table);
            Set<String> removedStatistics = difference(currentTableColumnStatistics.keySet(), updatedTableColumnStatistics.keySet());
            List<CompletableFuture<Void>> deleteFutures = removedStatistics.stream()
                    .map(column -> runAsync(() ->
                            glueClient.deleteColumnStatisticsForTable(
                                    new DeleteColumnStatisticsForTableRequest()
                                            .withCatalogId(catalogId)
                                            .withDatabaseName(table.getDatabaseName())
                                            .withTableName(table.getTableName())
                                            .withColumnName(column)), this.writeExecutor))
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
    public void updatePartitionStatistics(Partition partition, Map<String, HiveColumnStatistics> updatedColumnStatistics)
    {
        try {
            HiveBasicStatistics partitionStats = getHiveBasicStatistics(partition.getParameters());
            List<ColumnStatistics> columnStats = toGlueColumnStatistics(partition, updatedColumnStatistics, partitionStats.getRowCount()).stream()
                    .filter(this::isGlueWritable)
                    .collect(toUnmodifiableList());

            List<List<ColumnStatistics>> columnChunks = Lists.partition(columnStats, GLUE_COLUMN_WRITE_STAT_PAGE_SIZE);

            List<CompletableFuture<Void>> writePartitionStatsFutures = columnChunks.stream()
                    .map(columnChunk ->
                            runAsync(() -> glueClient.updateColumnStatisticsForPartition(new UpdateColumnStatisticsForPartitionRequest()
                                    .withCatalogId(catalogId)
                                    .withDatabaseName(partition.getDatabaseName())
                                    .withTableName(partition.getTableName())
                                    .withPartitionValues(partition.getValues())
                                    .withColumnStatisticsList(columnChunk)), writeExecutor))
                    .collect(toUnmodifiableList());

            Map<String, HiveColumnStatistics> currentColumnStatistics = this.getPartitionColumnStatisticsIfPresent(partition).orElse(ImmutableMap.of());
            Set<String> removedStatistics = difference(currentColumnStatistics.keySet(), updatedColumnStatistics.keySet());
            List<CompletableFuture<Void>> deleteStatsFutures = removedStatistics.stream()
                    .map(column -> runAsync(() ->
                            glueClient.deleteColumnStatisticsForPartition(new DeleteColumnStatisticsForPartitionRequest()
                                    .withCatalogId(catalogId)
                                    .withDatabaseName(partition.getDatabaseName())
                                    .withTableName(partition.getTableName())
                                    .withPartitionValues(partition.getValues())
                                    .withColumnName(column)), writeExecutor))
                    .collect(toUnmodifiableList());

            ImmutableList<CompletableFuture<Void>> updateOperationsFutures = ImmutableList.<CompletableFuture<Void>>builder()
                    .addAll(writePartitionStatsFutures)
                    .addAll(deleteStatsFutures)
                    .build();

            getFutureValue(allOf(updateOperationsFutures.toArray(CompletableFuture[]::new)));
        }
        catch (RuntimeException ex) {
            throw new TrinoException(HIVE_METASTORE_ERROR, ex);
        }
    }

    private List<String> getAllColumns(Table table)
    {
        ImmutableList.Builder<String> allColumns = ImmutableList.builderWithExpectedSize(table.getDataColumns().size() + table.getPartitionColumns().size());
        table.getDataColumns().stream().map(Column::getName).forEach(allColumns::add);
        table.getPartitionColumns().stream().map(Column::getName).forEach(allColumns::add);
        return allColumns.build();
    }
}
