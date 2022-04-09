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
package io.trino.plugin.hive;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.stats.CounterStat;
import io.airlift.units.DataSize;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.trino.plugin.hive.metastore.SortingColumn;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.util.HiveBucketing.HiveBucketFilter;
import io.trino.plugin.hive.util.HiveUtil;
import io.trino.spi.TrinoException;
import io.trino.spi.VersionEmbedder;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.type.TypeManager;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Iterables.transform;
import static io.trino.plugin.hive.BackgroundHiveSplitLoader.BucketSplitInfo.createBucketSplitInfo;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_PARTITION_DROPPED_DURING_QUERY;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_PARTITION_SCHEMA_MISMATCH;
import static io.trino.plugin.hive.HivePartition.UNPARTITIONED_ID;
import static io.trino.plugin.hive.HiveSessionProperties.getDynamicFilteringWaitTimeout;
import static io.trino.plugin.hive.HiveSessionProperties.isIgnoreAbsentPartitions;
import static io.trino.plugin.hive.HiveSessionProperties.isOptimizeSymlinkListing;
import static io.trino.plugin.hive.HiveSessionProperties.isPropagateTableScanSortingProperties;
import static io.trino.plugin.hive.HiveSessionProperties.isUseOrcColumnNames;
import static io.trino.plugin.hive.HiveSessionProperties.isUseParquetColumnNames;
import static io.trino.plugin.hive.HiveStorageFormat.getHiveStorageFormat;
import static io.trino.plugin.hive.TableToPartitionMapping.mapColumnsByIndex;
import static io.trino.plugin.hive.metastore.MetastoreUtil.getProtectMode;
import static io.trino.plugin.hive.metastore.MetastoreUtil.makePartitionName;
import static io.trino.plugin.hive.metastore.MetastoreUtil.verifyOnline;
import static io.trino.plugin.hive.util.HiveCoercionPolicy.canCoerce;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.SERVER_SHUTTING_DOWN;
import static io.trino.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy.GROUPED_SCHEDULING;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hive.ql.io.AcidUtils.isTransactionalTable;

public class HiveSplitManager
        implements ConnectorSplitManager
{
    public static final String PRESTO_OFFLINE = "presto_offline";
    public static final String OBJECT_NOT_READABLE = "object_not_readable";

    private final HiveTransactionManager transactionManager;
    private final HivePartitionManager partitionManager;
    private final NamenodeStats namenodeStats;
    private final HdfsEnvironment hdfsEnvironment;
    private final Executor executor;
    private final int maxOutstandingSplits;
    private final DataSize maxOutstandingSplitsSize;
    private final int minPartitionBatchSize;
    private final int maxPartitionBatchSize;
    private final int maxInitialSplits;
    private final int splitLoaderConcurrency;
    private final int maxSplitsPerSecond;
    private final boolean recursiveDfsWalkerEnabled;
    private final CounterStat highMemorySplitSourceCounter;
    private final TypeManager typeManager;

    @Inject
    public HiveSplitManager(
            HiveConfig hiveConfig,
            HiveTransactionManager transactionManager,
            HivePartitionManager partitionManager,
            NamenodeStats namenodeStats,
            HdfsEnvironment hdfsEnvironment,
            ExecutorService executorService,
            VersionEmbedder versionEmbedder,
            TypeManager typeManager)
    {
        this(
                transactionManager,
                partitionManager,
                namenodeStats,
                hdfsEnvironment,
                versionEmbedder.embedVersion(new BoundedExecutor(executorService, hiveConfig.getMaxSplitIteratorThreads())),
                new CounterStat(),
                hiveConfig.getMaxOutstandingSplits(),
                hiveConfig.getMaxOutstandingSplitsSize(),
                hiveConfig.getMinPartitionBatchSize(),
                hiveConfig.getMaxPartitionBatchSize(),
                hiveConfig.getMaxInitialSplits(),
                hiveConfig.getSplitLoaderConcurrency(),
                hiveConfig.getMaxSplitsPerSecond(),
                hiveConfig.getRecursiveDirWalkerEnabled(),
                typeManager);
    }

    public HiveSplitManager(
            HiveTransactionManager transactionManager,
            HivePartitionManager partitionManager,
            NamenodeStats namenodeStats,
            HdfsEnvironment hdfsEnvironment,
            Executor executor,
            CounterStat highMemorySplitSourceCounter,
            int maxOutstandingSplits,
            DataSize maxOutstandingSplitsSize,
            int minPartitionBatchSize,
            int maxPartitionBatchSize,
            int maxInitialSplits,
            int splitLoaderConcurrency,
            @Nullable Integer maxSplitsPerSecond,
            boolean recursiveDfsWalkerEnabled,
            TypeManager typeManager)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.partitionManager = requireNonNull(partitionManager, "partitionManager is null");
        this.namenodeStats = requireNonNull(namenodeStats, "namenodeStats is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.executor = new ErrorCodedExecutor(executor);
        this.highMemorySplitSourceCounter = requireNonNull(highMemorySplitSourceCounter, "highMemorySplitSourceCounter is null");
        checkArgument(maxOutstandingSplits >= 1, "maxOutstandingSplits must be at least 1");
        this.maxOutstandingSplits = maxOutstandingSplits;
        this.maxOutstandingSplitsSize = maxOutstandingSplitsSize;
        this.minPartitionBatchSize = minPartitionBatchSize;
        this.maxPartitionBatchSize = maxPartitionBatchSize;
        this.maxInitialSplits = maxInitialSplits;
        this.splitLoaderConcurrency = splitLoaderConcurrency;
        this.maxSplitsPerSecond = firstNonNull(maxSplitsPerSecond, Integer.MAX_VALUE);
        this.recursiveDfsWalkerEnabled = recursiveDfsWalkerEnabled;
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            SplitSchedulingStrategy splitSchedulingStrategy,
            DynamicFilter dynamicFilter)
    {
        HiveTableHandle hiveTable = (HiveTableHandle) tableHandle;
        SchemaTableName tableName = hiveTable.getSchemaTableName();

        // get table metadata
        TransactionalMetadata transactionalMetadata = transactionManager.get(transaction, session.getIdentity());
        SemiTransactionalHiveMetastore metastore = transactionalMetadata.getMetastore();
        Table table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));

        // verify table is not marked as non-readable
        String tableNotReadable = table.getParameters().get(OBJECT_NOT_READABLE);
        if (!isNullOrEmpty(tableNotReadable)) {
            throw new HiveNotReadableException(tableName, Optional.empty(), tableNotReadable);
        }

        // get partitions
        List<HivePartition> partitions = partitionManager.getOrLoadPartitions(metastore, hiveTable);

        // short circuit if we don't have any partitions
        if (partitions.isEmpty()) {
            if (hiveTable.isRecordScannedFiles()) {
                return new FixedSplitSource(ImmutableList.of(), ImmutableList.of());
            }
            return new FixedSplitSource(ImmutableList.of());
        }

        // get buckets from first partition (arbitrary)
        Optional<HiveBucketFilter> bucketFilter = hiveTable.getBucketFilter();

        // validate bucket bucketed execution
        Optional<HiveBucketHandle> bucketHandle = hiveTable.getBucketHandle();
        if ((splitSchedulingStrategy == GROUPED_SCHEDULING) && bucketHandle.isEmpty()) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "SchedulingPolicy is bucketed, but BucketHandle is not present");
        }

        // sort partitions
        partitions = Ordering.natural().onResultOf(HivePartition::getPartitionId).reverse().sortedCopy(partitions);

        Iterable<HivePartitionMetadata> hivePartitions = getPartitionMetadata(session, metastore, table, tableName, partitions, bucketHandle.map(HiveBucketHandle::toTableBucketProperty));

        // Only one thread per partition is usable when a table is not transactional
        int concurrency = isTransactionalTable(table.getParameters()) ? splitLoaderConcurrency : min(splitLoaderConcurrency, partitions.size());
        HiveSplitLoader hiveSplitLoader = new BackgroundHiveSplitLoader(
                table,
                hiveTable.getTransaction(),
                hivePartitions,
                hiveTable.getCompactEffectivePredicate(),
                dynamicFilter,
                getDynamicFilteringWaitTimeout(session),
                typeManager,
                createBucketSplitInfo(bucketHandle, bucketFilter),
                session,
                hdfsEnvironment,
                namenodeStats,
                transactionalMetadata.getDirectoryLister(),
                executor,
                concurrency,
                recursiveDfsWalkerEnabled,
                !hiveTable.getPartitionColumns().isEmpty() && isIgnoreAbsentPartitions(session),
                isOptimizeSymlinkListing(session),
                metastore.getValidWriteIds(session, hiveTable)
                        .map(validTxnWriteIdList -> validTxnWriteIdList.getTableValidWriteIdList(table.getDatabaseName() + "." + table.getTableName())),
                hiveTable.getMaxScannedFileSize());

        HiveSplitSource splitSource;
        switch (splitSchedulingStrategy) {
            case UNGROUPED_SCHEDULING:
                splitSource = HiveSplitSource.allAtOnce(
                        session,
                        table.getDatabaseName(),
                        table.getTableName(),
                        maxInitialSplits,
                        maxOutstandingSplits,
                        maxOutstandingSplitsSize,
                        maxSplitsPerSecond,
                        hiveSplitLoader,
                        executor,
                        highMemorySplitSourceCounter,
                        hiveTable.isRecordScannedFiles());
                break;
            case GROUPED_SCHEDULING:
                splitSource = HiveSplitSource.bucketed(
                        session,
                        table.getDatabaseName(),
                        table.getTableName(),
                        maxInitialSplits,
                        maxOutstandingSplits,
                        maxOutstandingSplitsSize,
                        maxSplitsPerSecond,
                        hiveSplitLoader,
                        executor,
                        highMemorySplitSourceCounter,
                        hiveTable.isRecordScannedFiles());
                break;
            default:
                throw new IllegalArgumentException("Unknown splitSchedulingStrategy: " + splitSchedulingStrategy);
        }
        hiveSplitLoader.start(splitSource);

        return splitSource;
    }

    @Managed
    @Nested
    public CounterStat getHighMemorySplitSource()
    {
        return highMemorySplitSourceCounter;
    }

    private Iterable<HivePartitionMetadata> getPartitionMetadata(ConnectorSession session, SemiTransactionalHiveMetastore metastore, Table table, SchemaTableName tableName, List<HivePartition> hivePartitions, Optional<HiveBucketProperty> bucketProperty)
    {
        if (hivePartitions.isEmpty()) {
            return ImmutableList.of();
        }

        if (hivePartitions.size() == 1) {
            HivePartition firstPartition = getOnlyElement(hivePartitions);
            if (firstPartition.getPartitionId().equals(UNPARTITIONED_ID)) {
                return ImmutableList.of(new HivePartitionMetadata(firstPartition, Optional.empty(), TableToPartitionMapping.empty()));
            }
        }

        Optional<HiveStorageFormat> storageFormat = getHiveStorageFormat(table.getStorage().getStorageFormat());

        Iterable<List<HivePartition>> partitionNameBatches = partitionExponentially(hivePartitions, minPartitionBatchSize, maxPartitionBatchSize);
        Iterable<List<HivePartitionMetadata>> partitionBatches = transform(partitionNameBatches, partitionBatch -> {
            Map<String, Optional<Partition>> batch = metastore.getPartitionsByNames(
                    tableName.getSchemaName(),
                    tableName.getTableName(),
                    Lists.transform(partitionBatch, HivePartition::getPartitionId));
            ImmutableMap.Builder<String, Partition> partitionBuilder = ImmutableMap.builder();
            for (Map.Entry<String, Optional<Partition>> entry : batch.entrySet()) {
                if (entry.getValue().isEmpty()) {
                    throw new TrinoException(HIVE_PARTITION_DROPPED_DURING_QUERY, "Partition no longer exists: " + entry.getKey());
                }
                partitionBuilder.put(entry.getKey(), entry.getValue().get());
            }
            Map<String, Partition> partitions = partitionBuilder.buildOrThrow();
            if (partitionBatch.size() != partitions.size()) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Expected %s partitions but found %s", partitionBatch.size(), partitions.size()));
            }

            ImmutableList.Builder<HivePartitionMetadata> results = ImmutableList.builder();
            for (HivePartition hivePartition : partitionBatch) {
                Partition partition = partitions.get(hivePartition.getPartitionId());
                if (partition == null) {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, "Partition not loaded: " + hivePartition);
                }
                String partName = makePartitionName(table, partition);

                // verify partition is online
                verifyOnline(tableName, Optional.of(partName), getProtectMode(partition), partition.getParameters());

                // verify partition is not marked as non-readable
                String partitionNotReadable = partition.getParameters().get(OBJECT_NOT_READABLE);
                if (!isNullOrEmpty(partitionNotReadable)) {
                    throw new HiveNotReadableException(tableName, Optional.of(partName), partitionNotReadable);
                }

                // Verify that the partition schema matches the table schema.
                // Either adding or dropping columns from the end of the table
                // without modifying existing partitions is allowed, but every
                // column that exists in both the table and partition must have
                // the same type.
                List<Column> tableColumns = table.getDataColumns();
                List<Column> partitionColumns = partition.getColumns();
                if ((tableColumns == null) || (partitionColumns == null)) {
                    throw new TrinoException(HIVE_INVALID_METADATA, format("Table '%s' or partition '%s' has null columns", tableName, partName));
                }
                TableToPartitionMapping tableToPartitionMapping = getTableToPartitionMapping(session, storageFormat, tableName, partName, tableColumns, partitionColumns);

                if (bucketProperty.isPresent()) {
                    Optional<HiveBucketProperty> partitionBucketProperty = partition.getStorage().getBucketProperty();
                    if (partitionBucketProperty.isEmpty()) {
                        throw new TrinoException(HIVE_PARTITION_SCHEMA_MISMATCH, format(
                                "Hive table (%s) is bucketed but partition (%s) is not bucketed",
                                hivePartition.getTableName(),
                                hivePartition.getPartitionId()));
                    }
                    int tableBucketCount = bucketProperty.get().getBucketCount();
                    int partitionBucketCount = partitionBucketProperty.get().getBucketCount();
                    List<String> tableBucketColumns = bucketProperty.get().getBucketedBy();
                    List<String> partitionBucketColumns = partitionBucketProperty.get().getBucketedBy();
                    if (!tableBucketColumns.equals(partitionBucketColumns) || !isBucketCountCompatible(tableBucketCount, partitionBucketCount)) {
                        throw new TrinoException(HIVE_PARTITION_SCHEMA_MISMATCH, format(
                                "Hive table (%s) bucketing (columns=%s, buckets=%s) is not compatible with partition (%s) bucketing (columns=%s, buckets=%s)",
                                hivePartition.getTableName(),
                                tableBucketColumns,
                                tableBucketCount,
                                hivePartition.getPartitionId(),
                                partitionBucketColumns,
                                partitionBucketCount));
                    }
                    if (isPropagateTableScanSortingProperties(session)) {
                        List<SortingColumn> tableSortedColumns = bucketProperty.get().getSortedBy();
                        List<SortingColumn> partitionSortedColumns = partitionBucketProperty.get().getSortedBy();
                        if (!isSortingCompatible(tableSortedColumns, partitionSortedColumns)) {
                            throw new TrinoException(HIVE_PARTITION_SCHEMA_MISMATCH, format(
                                    "Hive table (%s) sorting by %s is not compatible with partition (%s) sorting by %s. This restriction can be avoided by disabling propagate_table_scan_sorting_properties.",
                                    hivePartition.getTableName(),
                                    tableSortedColumns.stream().map(HiveUtil::sortingColumnToString).collect(toImmutableList()),
                                    hivePartition.getPartitionId(),
                                    partitionSortedColumns.stream().map(HiveUtil::sortingColumnToString).collect(toImmutableList())));
                        }
                    }
                }

                results.add(new HivePartitionMetadata(hivePartition, Optional.of(partition), tableToPartitionMapping));
            }

            return results.build();
        });
        return concat(partitionBatches);
    }

    private TableToPartitionMapping getTableToPartitionMapping(ConnectorSession session, Optional<HiveStorageFormat> storageFormat, SchemaTableName tableName, String partName, List<Column> tableColumns, List<Column> partitionColumns)
    {
        if (storageFormat.isPresent() && isPartitionUsesColumnNames(session, storageFormat.get())) {
            return getTableToPartitionMappingByColumnNames(tableName, partName, tableColumns, partitionColumns);
        }
        ImmutableMap.Builder<Integer, HiveTypeName> columnCoercions = ImmutableMap.builder();
        for (int i = 0; i < min(partitionColumns.size(), tableColumns.size()); i++) {
            HiveType tableType = tableColumns.get(i).getType();
            HiveType partitionType = partitionColumns.get(i).getType();
            if (!tableType.equals(partitionType)) {
                if (!canCoerce(typeManager, partitionType, tableType)) {
                    throw tablePartitionColumnMismatchException(tableName, partName, tableColumns.get(i).getName(), tableType, partitionColumns.get(i).getName(), partitionType);
                }
                columnCoercions.put(i, partitionType.getHiveTypeName());
            }
        }
        return mapColumnsByIndex(columnCoercions.buildOrThrow());
    }

    private static boolean isPartitionUsesColumnNames(ConnectorSession session, HiveStorageFormat storageFormat)
    {
        switch (storageFormat) {
            case AVRO:
                return true;
            case JSON:
                return true;
            case ORC:
                return isUseOrcColumnNames(session);
            case PARQUET:
                return isUseParquetColumnNames(session);
            default:
                return false;
        }
    }

    private TableToPartitionMapping getTableToPartitionMappingByColumnNames(SchemaTableName tableName, String partName, List<Column> tableColumns, List<Column> partitionColumns)
    {
        ImmutableMap.Builder<String, Integer> partitionColumnIndexesBuilder = ImmutableMap.builder();
        for (int i = 0; i < partitionColumns.size(); i++) {
            partitionColumnIndexesBuilder.put(partitionColumns.get(i).getName().toLowerCase(ENGLISH), i);
        }
        Map<String, Integer> partitionColumnsByIndex = partitionColumnIndexesBuilder.buildOrThrow();

        ImmutableMap.Builder<Integer, HiveTypeName> columnCoercions = ImmutableMap.builder();
        ImmutableMap.Builder<Integer, Integer> tableToPartitionColumns = ImmutableMap.builder();
        for (int tableColumnIndex = 0; tableColumnIndex < tableColumns.size(); tableColumnIndex++) {
            Column tableColumn = tableColumns.get(tableColumnIndex);
            HiveType tableType = tableColumn.getType();
            Integer partitionColumnIndex = partitionColumnsByIndex.get(tableColumn.getName().toLowerCase(ENGLISH));
            if (partitionColumnIndex == null) {
                continue;
            }
            tableToPartitionColumns.put(tableColumnIndex, partitionColumnIndex);
            Column partitionColumn = partitionColumns.get(partitionColumnIndex);
            HiveType partitionType = partitionColumn.getType();
            if (!tableType.equals(partitionType)) {
                if (!canCoerce(typeManager, partitionType, tableType)) {
                    throw tablePartitionColumnMismatchException(tableName, partName, tableColumn.getName(), tableType, partitionColumn.getName(), partitionType);
                }
                columnCoercions.put(partitionColumnIndex, partitionType.getHiveTypeName());
            }
        }

        return new TableToPartitionMapping(Optional.of(tableToPartitionColumns.buildOrThrow()), columnCoercions.buildOrThrow());
    }

    private TrinoException tablePartitionColumnMismatchException(SchemaTableName tableName, String partName, String tableColumnName, HiveType tableType, String partitionColumnName, HiveType partitionType)
    {
        return new TrinoException(HIVE_PARTITION_SCHEMA_MISMATCH, format("" +
                        "There is a mismatch between the table and partition schemas. " +
                        "The types are incompatible and cannot be coerced. " +
                        "The column '%s' in table '%s' is declared as type '%s', " +
                        "but partition '%s' declared column '%s' as type '%s'.",
                tableColumnName,
                tableName,
                tableType,
                partName,
                partitionColumnName,
                partitionType));
    }

    static boolean isBucketCountCompatible(int tableBucketCount, int partitionBucketCount)
    {
        checkArgument(tableBucketCount > 0 && partitionBucketCount > 0);
        int larger = Math.max(tableBucketCount, partitionBucketCount);
        int smaller = min(tableBucketCount, partitionBucketCount);
        if (larger % smaller != 0) {
            // must be evenly divisible
            return false;
        }
        // ratio must be power of two
        return Integer.bitCount(larger / smaller) == 1;
    }

    private static boolean isSortingCompatible(List<SortingColumn> tableSortedColumns, List<SortingColumn> partitionSortedColumns)
    {
        // When propagate_table_scan_sorting_properties is enabled, all files are assumed to be sorted by tableSortedColumns
        // Therefore, sorting of each partition must satisfy the sorting criteria of the table
        if (tableSortedColumns.size() > partitionSortedColumns.size()) {
            return false;
        }
        for (int i = 0; i < tableSortedColumns.size(); i++) {
            SortingColumn tableSortingColumn = tableSortedColumns.get(i);
            SortingColumn partitionSortingColumn = partitionSortedColumns.get(i);
            if (!tableSortingColumn.equals(partitionSortingColumn)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Partition the given list in exponentially (power of 2) increasing batch sizes starting at 1 up to maxBatchSize
     */
    private static <T> Iterable<List<T>> partitionExponentially(List<T> values, int minBatchSize, int maxBatchSize)
    {
        return () -> new AbstractIterator<>()
        {
            private int currentSize = minBatchSize;
            private final Iterator<T> iterator = values.iterator();

            @Override
            protected List<T> computeNext()
            {
                if (!iterator.hasNext()) {
                    return endOfData();
                }

                int count = 0;
                ImmutableList.Builder<T> builder = ImmutableList.builder();
                while (iterator.hasNext() && count < currentSize) {
                    builder.add(iterator.next());
                    ++count;
                }

                currentSize = min(maxBatchSize, currentSize * 2);
                return builder.build();
            }
        };
    }

    public static class ErrorCodedExecutor
            implements Executor
    {
        private final Executor delegate;

        public ErrorCodedExecutor(Executor delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public void execute(Runnable command)
        {
            try {
                delegate.execute(command);
            }
            catch (RejectedExecutionException e) {
                throw new TrinoException(SERVER_SHUTTING_DOWN, "Server is shutting down", e);
            }
        }
    }
}
