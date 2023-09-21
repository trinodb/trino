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
import com.google.common.collect.PeekingIterator;
import com.google.inject.Inject;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.stats.CounterStat;
import io.airlift.units.DataSize;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.HdfsNamenodeStats;
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
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.type.TypeManager;
import jakarta.annotation.Nullable;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.Collection;
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
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterators.peekingIterator;
import static com.google.common.collect.Iterators.singletonIterator;
import static com.google.common.collect.Iterators.transform;
import static com.google.common.collect.Streams.stream;
import static io.trino.plugin.hive.BackgroundHiveSplitLoader.BucketSplitInfo.createBucketSplitInfo;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_PARTITION_DROPPED_DURING_QUERY;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_PARTITION_SCHEMA_MISMATCH;
import static io.trino.plugin.hive.HivePartition.UNPARTITIONED_ID;
import static io.trino.plugin.hive.HiveSessionProperties.getDynamicFilteringWaitTimeout;
import static io.trino.plugin.hive.HiveSessionProperties.getTimestampPrecision;
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
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SERVER_SHUTTING_DOWN;
import static io.trino.spi.connector.FixedSplitSource.emptySplitSource;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Collections.emptyIterator;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class HiveSplitManager
        implements ConnectorSplitManager
{
    public static final String PRESTO_OFFLINE = "presto_offline";
    public static final String OBJECT_NOT_READABLE = "object_not_readable";

    private final HiveTransactionManager transactionManager;
    private final HivePartitionManager partitionManager;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final HdfsNamenodeStats hdfsNamenodeStats;
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
    private final int maxPartitionsPerScan;

    @Inject
    public HiveSplitManager(
            HiveConfig hiveConfig,
            HiveTransactionManager transactionManager,
            HivePartitionManager partitionManager,
            TrinoFileSystemFactory fileSystemFactory,
            HdfsNamenodeStats hdfsNamenodeStats,
            HdfsEnvironment hdfsEnvironment,
            ExecutorService executorService,
            VersionEmbedder versionEmbedder,
            TypeManager typeManager)
    {
        this(
                transactionManager,
                partitionManager,
                fileSystemFactory,
                hdfsNamenodeStats,
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
                typeManager,
                hiveConfig.getMaxPartitionsPerScan());
    }

    public HiveSplitManager(
            HiveTransactionManager transactionManager,
            HivePartitionManager partitionManager,
            TrinoFileSystemFactory fileSystemFactory,
            HdfsNamenodeStats hdfsNamenodeStats,
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
            TypeManager typeManager,
            int maxPartitionsPerScan)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.partitionManager = requireNonNull(partitionManager, "partitionManager is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.hdfsNamenodeStats = requireNonNull(hdfsNamenodeStats, "hdfsNamenodeStats is null");
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
        this.maxPartitionsPerScan = maxPartitionsPerScan;
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        HiveTableHandle hiveTable = (HiveTableHandle) tableHandle;
        SchemaTableName tableName = hiveTable.getSchemaTableName();

        // get table metadata
        TransactionalMetadata transactionalMetadata = transactionManager.get(transaction, session.getIdentity());
        SemiTransactionalHiveMetastore metastore = transactionalMetadata.getMetastore();
        if (!metastore.isReadableWithinTransaction(tableName.getSchemaName(), tableName.getTableName())) {
            // Until transaction is committed, the table data may or may not be visible.
            throw new TrinoException(NOT_SUPPORTED, format(
                    "Cannot read from a table %s that was modified within transaction, you need to commit the transaction first",
                    tableName));
        }
        Table table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));

        // verify table is not marked as non-readable
        String tableNotReadable = table.getParameters().get(OBJECT_NOT_READABLE);
        if (!isNullOrEmpty(tableNotReadable)) {
            throw new HiveNotReadableException(tableName, Optional.empty(), tableNotReadable);
        }

        // get buckets from first partition (arbitrary)
        Optional<HiveBucketFilter> bucketFilter = hiveTable.getBucketFilter();

        // validate bucket bucketed execution
        Optional<HiveBucketHandle> bucketHandle = hiveTable.getBucketHandle();

        bucketHandle.ifPresent(bucketing ->
                verify(bucketing.getReadBucketCount() <= bucketing.getTableBucketCount(),
                        "readBucketCount (%s) is greater than the tableBucketCount (%s) which generally points to an issue in plan generation",
                        bucketing.getReadBucketCount(),
                        bucketing.getTableBucketCount()));

        // get partitions
        Iterator<HivePartition> partitions = partitionManager.getPartitions(metastore, hiveTable);

        // short circuit if we don't have any partitions
        if (!partitions.hasNext()) {
            if (hiveTable.isRecordScannedFiles()) {
                return new FixedSplitSource(ImmutableList.of(), ImmutableList.of());
            }
            return emptySplitSource();
        }

        Iterator<HivePartitionMetadata> hivePartitions = getPartitionMetadata(
                session,
                metastore,
                table,
                peekingIterator(partitions),
                bucketHandle.map(HiveBucketHandle::toTableBucketProperty));

        HiveSplitLoader hiveSplitLoader = new BackgroundHiveSplitLoader(
                table,
                hivePartitions,
                hiveTable.getCompactEffectivePredicate(),
                dynamicFilter,
                getDynamicFilteringWaitTimeout(session),
                typeManager,
                createBucketSplitInfo(bucketHandle, bucketFilter),
                session,
                fileSystemFactory,
                hdfsEnvironment,
                hdfsNamenodeStats,
                transactionalMetadata.getDirectoryLister(),
                executor,
                splitLoaderConcurrency,
                recursiveDfsWalkerEnabled,
                !hiveTable.getPartitionColumns().isEmpty() && isIgnoreAbsentPartitions(session),
                isOptimizeSymlinkListing(session),
                metastore.getValidWriteIds(session, hiveTable)
                        .map(value -> value.getTableValidWriteIdList(table.getDatabaseName() + "." + table.getTableName())),
                hiveTable.getMaxScannedFileSize(),
                maxPartitionsPerScan);

        HiveSplitSource splitSource = HiveSplitSource.allAtOnce(
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
        hiveSplitLoader.start(splitSource);

        return splitSource;
    }

    @Managed
    @Nested
    public CounterStat getHighMemorySplitSource()
    {
        return highMemorySplitSourceCounter;
    }

    private Iterator<HivePartitionMetadata> getPartitionMetadata(
            ConnectorSession session,
            SemiTransactionalHiveMetastore metastore,
            Table table,
            PeekingIterator<HivePartition> hivePartitions,
            Optional<HiveBucketProperty> bucketProperty)
    {
        if (!hivePartitions.hasNext()) {
            return emptyIterator();
        }

        HivePartition firstPartition = hivePartitions.peek();
        if (firstPartition.getPartitionId().equals(UNPARTITIONED_ID)) {
            hivePartitions.next();
            checkArgument(!hivePartitions.hasNext(), "single partition is expected for unpartitioned table");
            return singletonIterator(new HivePartitionMetadata(firstPartition, Optional.empty(), TableToPartitionMapping.empty()));
        }

        HiveTimestampPrecision hiveTimestampPrecision = getTimestampPrecision(session);
        boolean propagateTableScanSortingProperties = isPropagateTableScanSortingProperties(session);
        boolean usePartitionColumnNames = isPartitionUsesColumnNames(session, getHiveStorageFormat(table.getStorage().getStorageFormat()));

        Iterator<List<HivePartition>> partitionNameBatches = partitionExponentially(hivePartitions, minPartitionBatchSize, maxPartitionBatchSize);
        Iterator<List<HivePartitionMetadata>> partitionBatches = transform(partitionNameBatches, partitionBatch -> {
            SchemaTableName tableName = table.getSchemaTableName();
            Map<String, Optional<Partition>> partitions = metastore.getPartitionsByNames(
                    tableName.getSchemaName(),
                    tableName.getTableName(),
                    Lists.transform(partitionBatch, HivePartition::getPartitionId));

            if (partitionBatch.size() != partitions.size()) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Expected %s partitions but found %s", partitionBatch.size(), partitions.size()));
            }

            ImmutableList.Builder<HivePartitionMetadata> results = ImmutableList.builderWithExpectedSize(partitionBatch.size());
            for (HivePartition hivePartition : partitionBatch) {
                Optional<Partition> partition = partitions.get(hivePartition.getPartitionId());
                if (partition == null) {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, "Partition not loaded: " + hivePartition);
                }
                if (partition.isEmpty()) {
                    throw new TrinoException(HIVE_PARTITION_DROPPED_DURING_QUERY, "Partition no longer exists: " + hivePartition.getPartitionId());
                }
                results.add(toPartitionMetadata(
                        typeManager,
                        hiveTimestampPrecision,
                        propagateTableScanSortingProperties,
                        usePartitionColumnNames,
                        table,
                        bucketProperty,
                        hivePartition,
                        partition.get()));
            }

            return results.build();
        });
        return stream(partitionBatches)
                .flatMap(Collection::stream)
                .iterator();
    }

    private static HivePartitionMetadata toPartitionMetadata(
            TypeManager typeManager,
            HiveTimestampPrecision hiveTimestampPrecision,
            boolean propagateTableScanSortingProperties,
            boolean usePartitionColumnNames,
            Table table,
            Optional<HiveBucketProperty> bucketProperty,
            HivePartition hivePartition,
            Partition partition)
    {
        SchemaTableName tableName = table.getSchemaTableName();
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
        TableToPartitionMapping tableToPartitionMapping = getTableToPartitionMapping(usePartitionColumnNames, typeManager, hiveTimestampPrecision, tableName, partName, tableColumns, partitionColumns);

        if (bucketProperty.isPresent()) {
            HiveBucketProperty partitionBucketProperty = partition.getStorage().getBucketProperty()
                    .orElseThrow(() -> new TrinoException(HIVE_PARTITION_SCHEMA_MISMATCH, format(
                            "Hive table (%s) is bucketed but partition (%s) is not bucketed",
                            tableName,
                            partName)));
            int tableBucketCount = bucketProperty.get().getBucketCount();
            int partitionBucketCount = partitionBucketProperty.getBucketCount();
            List<String> tableBucketColumns = bucketProperty.get().getBucketedBy();
            List<String> partitionBucketColumns = partitionBucketProperty.getBucketedBy();
            if (!tableBucketColumns.equals(partitionBucketColumns) || !isBucketCountCompatible(tableBucketCount, partitionBucketCount)) {
                throw new TrinoException(HIVE_PARTITION_SCHEMA_MISMATCH, format(
                        "Hive table (%s) bucketing (columns=%s, buckets=%s) is not compatible with partition (%s) bucketing (columns=%s, buckets=%s)",
                        tableName,
                        tableBucketColumns,
                        tableBucketCount,
                        partName,
                        partitionBucketColumns,
                        partitionBucketCount));
            }
            if (propagateTableScanSortingProperties) {
                List<SortingColumn> tableSortedColumns = bucketProperty.get().getSortedBy();
                List<SortingColumn> partitionSortedColumns = partitionBucketProperty.getSortedBy();
                if (!isSortingCompatible(tableSortedColumns, partitionSortedColumns)) {
                    throw new TrinoException(HIVE_PARTITION_SCHEMA_MISMATCH, format(
                            "Hive table (%s) sorting by %s is not compatible with partition (%s) sorting by %s. This restriction can be avoided by disabling propagate_table_scan_sorting_properties.",
                            tableName,
                            tableSortedColumns.stream().map(HiveUtil::sortingColumnToString).collect(toImmutableList()),
                            partName,
                            partitionSortedColumns.stream().map(HiveUtil::sortingColumnToString).collect(toImmutableList())));
                }
            }
        }
        return new HivePartitionMetadata(hivePartition, Optional.of(partition), tableToPartitionMapping);
    }

    private static TableToPartitionMapping getTableToPartitionMapping(boolean usePartitionColumnNames, TypeManager typeManager, HiveTimestampPrecision hiveTimestampPrecision, SchemaTableName tableName, String partName, List<Column> tableColumns, List<Column> partitionColumns)
    {
        if (usePartitionColumnNames) {
            return getTableToPartitionMappingByColumnNames(typeManager, tableName, partName, tableColumns, partitionColumns, hiveTimestampPrecision);
        }
        ImmutableMap.Builder<Integer, HiveTypeName> columnCoercions = ImmutableMap.builder();
        for (int i = 0; i < min(partitionColumns.size(), tableColumns.size()); i++) {
            HiveType tableType = tableColumns.get(i).getType();
            HiveType partitionType = partitionColumns.get(i).getType();
            if (!tableType.equals(partitionType)) {
                if (!canCoerce(typeManager, partitionType, tableType, hiveTimestampPrecision)) {
                    throw tablePartitionColumnMismatchException(tableName, partName, tableColumns.get(i).getName(), tableType, partitionColumns.get(i).getName(), partitionType);
                }
                columnCoercions.put(i, partitionType.getHiveTypeName());
            }
        }
        return mapColumnsByIndex(columnCoercions.buildOrThrow());
    }

    private static boolean isPartitionUsesColumnNames(ConnectorSession session, Optional<HiveStorageFormat> storageFormat)
    {
        if (storageFormat.isEmpty()) {
            return false;
        }
        return switch (storageFormat.get()) {
            case AVRO, JSON -> true;
            case ORC -> isUseOrcColumnNames(session);
            case PARQUET -> isUseParquetColumnNames(session);
            default -> false;
        };
    }

    private static TableToPartitionMapping getTableToPartitionMappingByColumnNames(TypeManager typeManager, SchemaTableName tableName, String partName, List<Column> tableColumns, List<Column> partitionColumns, HiveTimestampPrecision hiveTimestampPrecision)
    {
        ImmutableMap.Builder<String, Integer> partitionColumnIndexesBuilder = ImmutableMap.builderWithExpectedSize(partitionColumns.size());
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
                if (!canCoerce(typeManager, partitionType, tableType, hiveTimestampPrecision)) {
                    throw tablePartitionColumnMismatchException(tableName, partName, tableColumn.getName(), tableType, partitionColumn.getName(), partitionType);
                }
                columnCoercions.put(partitionColumnIndex, partitionType.getHiveTypeName());
            }
        }

        return new TableToPartitionMapping(Optional.of(tableToPartitionColumns.buildOrThrow()), columnCoercions.buildOrThrow());
    }

    private static TrinoException tablePartitionColumnMismatchException(SchemaTableName tableName, String partName, String tableColumnName, HiveType tableType, String partitionColumnName, HiveType partitionType)
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
    private static <T> Iterator<List<T>> partitionExponentially(Iterator<T> values, int minBatchSize, int maxBatchSize)
    {
        return new AbstractIterator<>()
        {
            private int currentSize = minBatchSize;

            @Override
            protected List<T> computeNext()
            {
                if (!values.hasNext()) {
                    return endOfData();
                }

                int count = 0;
                ImmutableList.Builder<T> builder = ImmutableList.builderWithExpectedSize(currentSize);
                while (values.hasNext() && count < currentSize) {
                    builder.add(values.next());
                    ++count;
                }

                currentSize = min(maxBatchSize, currentSize * 2);
                return builder.build();
            }
        };
    }

    private static class ErrorCodedExecutor
            implements Executor
    {
        private final Executor delegate;

        private ErrorCodedExecutor(Executor delegate)
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
