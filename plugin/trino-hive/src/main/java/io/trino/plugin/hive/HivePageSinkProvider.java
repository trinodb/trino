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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.units.DataSize;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.SortingColumn;
import io.trino.metastore.cache.CachingHiveMetastore;
import io.trino.plugin.hive.metastore.HivePageSinkMetadataProvider;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.PageSorter;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.type.TypeManager;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.metastore.cache.CachingHiveMetastore.createPerTransactionCache;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class HivePageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final Set<HiveFileWriterFactory> fileWriterFactories;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final PageSorter pageSorter;
    private final HiveMetastoreFactory metastoreFactory;
    private final PageIndexerFactory pageIndexerFactory;
    private final TypeManager typeManager;
    private final int maxOpenPartitions;
    private final int maxOpenSortFiles;
    private final DataSize writerSortBufferSize;
    private final LocationService locationService;
    private final ListeningExecutorService writeVerificationExecutor;
    private final JsonCodec<PartitionUpdate> partitionUpdateCodec;
    private final HiveWriterStats hiveWriterStats;
    private final long perTransactionMetastoreCacheMaximumSize;
    private final boolean temporaryStagingDirectoryEnabled;
    private final String temporaryStagingDirectoryPath;

    @Inject
    public HivePageSinkProvider(
            Set<HiveFileWriterFactory> fileWriterFactories,
            TrinoFileSystemFactory fileSystemFactory,
            PageSorter pageSorter,
            HiveMetastoreFactory metastoreFactory,
            PageIndexerFactory pageIndexerFactory,
            TypeManager typeManager,
            HiveConfig config,
            SortingFileWriterConfig sortingFileWriterConfig,
            LocationService locationService,
            JsonCodec<PartitionUpdate> partitionUpdateCodec,
            HiveWriterStats hiveWriterStats)
    {
        this.fileWriterFactories = ImmutableSet.copyOf(requireNonNull(fileWriterFactories, "fileWriterFactories is null"));
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.pageSorter = requireNonNull(pageSorter, "pageSorter is null");
        this.metastoreFactory = requireNonNull(metastoreFactory, "metastoreFactory is null");
        this.pageIndexerFactory = requireNonNull(pageIndexerFactory, "pageIndexerFactory is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.maxOpenPartitions = config.getMaxPartitionsPerWriter();
        this.maxOpenSortFiles = sortingFileWriterConfig.getMaxOpenSortFiles();
        this.writerSortBufferSize = requireNonNull(sortingFileWriterConfig.getWriterSortBufferSize(), "writerSortBufferSize is null");
        this.locationService = requireNonNull(locationService, "locationService is null");
        this.writeVerificationExecutor = listeningDecorator(newFixedThreadPool(config.getWriteValidationThreads(), daemonThreadsNamed("hive-write-validation-%s")));
        this.partitionUpdateCodec = requireNonNull(partitionUpdateCodec, "partitionUpdateCodec is null");
        this.hiveWriterStats = requireNonNull(hiveWriterStats, "hiveWriterStats is null");
        this.perTransactionMetastoreCacheMaximumSize = config.getPerTransactionMetastoreCacheMaximumSize();
        this.temporaryStagingDirectoryEnabled = config.isTemporaryStagingDirectoryEnabled();
        this.temporaryStagingDirectoryPath = config.getTemporaryStagingDirectoryPath();
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorOutputTableHandle tableHandle, ConnectorPageSinkId pageSinkId)
    {
        HiveOutputTableHandle handle = (HiveOutputTableHandle) tableHandle;
        return createPageSink(handle, true, session, handle.getAdditionalTableParameters());
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorInsertTableHandle tableHandle, ConnectorPageSinkId pageSinkId)
    {
        HiveWritableTableHandle handle = (HiveInsertTableHandle) tableHandle;
        return createPageSink(handle, false, session, ImmutableMap.of() /* for insert properties are taken from metastore */);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle, ConnectorPageSinkId pageSinkId)
    {
        HiveTableExecuteHandle handle = (HiveTableExecuteHandle) tableExecuteHandle;
        return createPageSink(handle, false, session, ImmutableMap.of());
    }

    @Override
    public ConnectorMergeSink createMergeSink(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorMergeTableHandle mergeHandle, ConnectorPageSinkId pageSinkId)
    {
        HiveMergeTableHandle hiveMergeHandle = (HiveMergeTableHandle) mergeHandle;
        HiveInsertTableHandle insertHandle = hiveMergeHandle.getInsertHandle();
        checkArgument(insertHandle.getTransaction().isMerge(), "handle isn't an ACID MERGE");
        return createPageSink(insertHandle, false, session, ImmutableMap.of());
    }

    private HivePageSink createPageSink(HiveWritableTableHandle handle, boolean isCreateTable, ConnectorSession session, Map<String, String> additionalTableParameters)
    {
        OptionalInt bucketCount = OptionalInt.empty();
        List<SortingColumn> sortedBy = ImmutableList.of();

        if (handle.getBucketInfo().isPresent()) {
            bucketCount = OptionalInt.of(handle.getBucketInfo().get().bucketCount());
            sortedBy = handle.getBucketInfo().get().sortedBy();
        }

        CachingHiveMetastore cachingHiveMetastore = createPerTransactionCache(metastoreFactory.createMetastore(Optional.of(session.getIdentity())), perTransactionMetastoreCacheMaximumSize);
        HiveWriterFactory writerFactory = new HiveWriterFactory(
                fileWriterFactories,
                fileSystemFactory,
                handle.getSchemaName(),
                handle.getTableName(),
                isCreateTable,
                handle.getTransaction(),
                handle.getInputColumns(),
                handle.getTableStorageFormat(),
                handle.getPartitionStorageFormat(),
                additionalTableParameters,
                bucketCount,
                sortedBy,
                handle.getLocationHandle(),
                locationService,
                session.getQueryId(),
                new HivePageSinkMetadataProvider(handle.getPageSinkMetadata(), cachingHiveMetastore),
                typeManager,
                pageSorter,
                writerSortBufferSize,
                maxOpenSortFiles,
                session,
                hiveWriterStats,
                temporaryStagingDirectoryEnabled,
                temporaryStagingDirectoryPath);

        return new HivePageSink(
                writerFactory,
                handle.getInputColumns(),
                handle.getTransaction(),
                handle.getBucketInfo(),
                pageIndexerFactory,
                maxOpenPartitions,
                writeVerificationExecutor,
                partitionUpdateCodec,
                session);
    }
}
