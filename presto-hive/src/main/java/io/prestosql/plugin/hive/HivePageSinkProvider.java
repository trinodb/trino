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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.event.client.EventClient;
import io.airlift.json.JsonCodec;
import io.airlift.units.DataSize;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.metastore.HivePageSinkMetadataProvider;
import io.prestosql.plugin.hive.metastore.SortingColumn;
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.PageIndexerFactory;
import io.prestosql.spi.PageSorter;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorPageSink;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.type.TypeManager;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.plugin.hive.metastore.cache.CachingHiveMetastore.memoizeMetastore;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class HivePageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final Set<HiveFileWriterFactory> fileWriterFactories;
    private final HdfsEnvironment hdfsEnvironment;
    private final PageSorter pageSorter;
    private final HiveMetastore metastore;
    private final PageIndexerFactory pageIndexerFactory;
    private final TypeManager typeManager;
    private final int maxOpenPartitions;
    private final int maxOpenSortFiles;
    private final DataSize writerSortBufferSize;
    private final boolean immutablePartitions;
    private final LocationService locationService;
    private final ListeningExecutorService writeVerificationExecutor;
    private final JsonCodec<PartitionUpdate> partitionUpdateCodec;
    private final NodeManager nodeManager;
    private final EventClient eventClient;
    private final HiveSessionProperties hiveSessionProperties;
    private final HiveWriterStats hiveWriterStats;
    private final long perTransactionMetastoreCacheMaximumSize;

    @Inject
    public HivePageSinkProvider(
            Set<HiveFileWriterFactory> fileWriterFactories,
            HdfsEnvironment hdfsEnvironment,
            PageSorter pageSorter,
            HiveMetastore metastore,
            PageIndexerFactory pageIndexerFactory,
            TypeManager typeManager,
            HiveConfig config,
            LocationService locationService,
            JsonCodec<PartitionUpdate> partitionUpdateCodec,
            NodeManager nodeManager,
            EventClient eventClient,
            HiveSessionProperties hiveSessionProperties,
            HiveWriterStats hiveWriterStats)
    {
        this.fileWriterFactories = ImmutableSet.copyOf(requireNonNull(fileWriterFactories, "fileWriterFactories is null"));
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.pageSorter = requireNonNull(pageSorter, "pageSorter is null");
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.pageIndexerFactory = requireNonNull(pageIndexerFactory, "pageIndexerFactory is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.maxOpenPartitions = config.getMaxPartitionsPerWriter();
        this.maxOpenSortFiles = config.getMaxOpenSortFiles();
        this.writerSortBufferSize = requireNonNull(config.getWriterSortBufferSize(), "writerSortBufferSize is null");
        this.immutablePartitions = config.isImmutablePartitions();
        this.locationService = requireNonNull(locationService, "locationService is null");
        this.writeVerificationExecutor = listeningDecorator(newFixedThreadPool(config.getWriteValidationThreads(), daemonThreadsNamed("hive-write-validation-%s")));
        this.partitionUpdateCodec = requireNonNull(partitionUpdateCodec, "partitionUpdateCodec is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.eventClient = requireNonNull(eventClient, "eventClient is null");
        this.hiveSessionProperties = requireNonNull(hiveSessionProperties, "hiveSessionProperties is null");
        this.hiveWriterStats = requireNonNull(hiveWriterStats, "stats is null");
        this.perTransactionMetastoreCacheMaximumSize = config.getPerTransactionMetastoreCacheMaximumSize();
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorOutputTableHandle tableHandle)
    {
        HiveOutputTableHandle handle = (HiveOutputTableHandle) tableHandle;
        return createPageSink(handle, true, session, handle.getAdditionalTableParameters());
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorInsertTableHandle tableHandle)
    {
        HiveWritableTableHandle handle = (HiveInsertTableHandle) tableHandle;
        return createPageSink(handle, false, session, ImmutableMap.of() /* for insert properties are taken from metastore */);
    }

    private ConnectorPageSink createPageSink(HiveWritableTableHandle handle, boolean isCreateTable, ConnectorSession session, Map<String, String> additionalTableParameters)
    {
        OptionalInt bucketCount = OptionalInt.empty();
        List<SortingColumn> sortedBy = ImmutableList.of();

        if (handle.getBucketProperty().isPresent()) {
            bucketCount = OptionalInt.of(handle.getBucketProperty().get().getBucketCount());
            sortedBy = handle.getBucketProperty().get().getSortedBy();
        }

        HiveWriterFactory writerFactory = new HiveWriterFactory(
                fileWriterFactories,
                handle.getSchemaName(),
                handle.getTableName(),
                isCreateTable,
                handle.getInputColumns(),
                handle.getTableStorageFormat(),
                handle.getPartitionStorageFormat(),
                additionalTableParameters,
                bucketCount,
                sortedBy,
                handle.getLocationHandle(),
                locationService,
                session.getQueryId(),
                new HivePageSinkMetadataProvider(handle.getPageSinkMetadata(), memoizeMetastore(metastore, perTransactionMetastoreCacheMaximumSize), new HiveIdentity(session)),
                typeManager,
                hdfsEnvironment,
                pageSorter,
                writerSortBufferSize,
                maxOpenSortFiles,
                immutablePartitions,
                session,
                nodeManager,
                eventClient,
                hiveSessionProperties,
                hiveWriterStats);

        return new HivePageSink(
                writerFactory,
                handle.getInputColumns(),
                handle.getBucketProperty(),
                pageIndexerFactory,
                hdfsEnvironment,
                maxOpenPartitions,
                writeVerificationExecutor,
                partitionUpdateCodec,
                session);
    }
}
