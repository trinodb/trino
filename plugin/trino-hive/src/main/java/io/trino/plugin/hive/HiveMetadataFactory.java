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

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.fs.DirectoryLister;
import io.trino.plugin.hive.fs.TransactionScopeCachingDirectoryListerFactory;
import io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.trino.plugin.hive.security.AccessControlMetadataFactory;
import io.trino.plugin.hive.security.UsingSystemSecurity;
import io.trino.plugin.hive.statistics.MetastoreHiveStatisticsProvider;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.MetadataProvider;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeManager;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.metastore.cache.CachingHiveMetastore.createPerTransactionCache;
import static java.util.Objects.requireNonNull;

public class HiveMetadataFactory
        implements TransactionalMetadataFactory
{
    private final CatalogName catalogName;
    private final boolean skipDeletionForAlter;
    private final boolean skipTargetCleanupOnRollback;
    private final boolean writesToNonManagedTablesEnabled;
    private final boolean createsOfNonManagedTablesEnabled;
    private final boolean deleteSchemaLocationsFallback;
    private final boolean translateHiveViews;
    private final boolean hiveViewsRunAsInvoker;
    private final boolean hideDeltaLakeTables;
    private final long perTransactionCacheMaximumSize;
    private final HiveMetastoreFactory metastoreFactory;
    private final Set<HiveFileWriterFactory> fileWriterFactories;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final HivePartitionManager partitionManager;
    private final TypeManager typeManager;
    private final MetadataProvider metadataProvider;
    private final LocationService locationService;
    private final JsonCodec<PartitionUpdate> partitionUpdateCodec;
    private final BoundedExecutor fileSystemExecutor;
    private final BoundedExecutor dropExecutor;
    private final Executor updateExecutor;
    private final long maxPartitionDropsPerQuery;
    private final String trinoVersion;
    private final Set<SystemTableProvider> systemTableProviders;
    private final AccessControlMetadataFactory accessControlMetadataFactory;
    private final Optional<Duration> hiveTransactionHeartbeatInterval;
    private final ScheduledExecutorService heartbeatService;
    private final DirectoryLister directoryLister;
    private final TransactionScopeCachingDirectoryListerFactory transactionScopeCachingDirectoryListerFactory;
    private final boolean usingSystemSecurity;
    private final boolean partitionProjectionEnabled;
    private final boolean allowTableRename;
    private final HiveTimestampPrecision hiveViewsTimestampPrecision;
    private final Executor metadataFetchingExecutor;

    @Inject
    public HiveMetadataFactory(
            CatalogName catalogName,
            HiveConfig hiveConfig,
            @HideDeltaLakeTables boolean hideDeltaLakeTables,
            HiveMetastoreFactory metastoreFactory,
            Set<HiveFileWriterFactory> fileWriterFactories,
            TrinoFileSystemFactory fileSystemFactory,
            HivePartitionManager partitionManager,
            ExecutorService executorService,
            @ForHiveTransactionHeartbeats ScheduledExecutorService heartbeatService,
            TypeManager typeManager,
            MetadataProvider metadataProvider,
            LocationService locationService,
            JsonCodec<PartitionUpdate> partitionUpdateCodec,
            NodeVersion nodeVersion,
            Set<SystemTableProvider> systemTableProviders,
            AccessControlMetadataFactory accessControlMetadataFactory,
            DirectoryLister directoryLister,
            TransactionScopeCachingDirectoryListerFactory transactionScopeCachingDirectoryListerFactory,
            @UsingSystemSecurity boolean usingSystemSecurity,
            @AllowHiveTableRename boolean allowTableRename)
    {
        this(
                catalogName,
                metastoreFactory,
                fileWriterFactories,
                fileSystemFactory,
                partitionManager,
                hiveConfig.getMaxConcurrentFileSystemOperations(),
                hiveConfig.getMaxConcurrentMetastoreDrops(),
                hiveConfig.getMaxConcurrentMetastoreUpdates(),
                hiveConfig.getMaxPartitionDropsPerQuery(),
                hiveConfig.isSkipDeletionForAlter(),
                hiveConfig.isSkipTargetCleanupOnRollback(),
                hiveConfig.getWritesToNonManagedTablesEnabled(),
                hiveConfig.getCreatesOfNonManagedTablesEnabled(),
                hiveConfig.isDeleteSchemaLocationsFallback(),
                hiveConfig.isTranslateHiveViews(),
                hiveConfig.isHiveViewsRunAsInvoker(),
                hiveConfig.getPerTransactionMetastoreCacheMaximumSize(),
                hiveConfig.getHiveTransactionHeartbeatInterval(),
                hideDeltaLakeTables,
                typeManager,
                metadataProvider,
                locationService,
                partitionUpdateCodec,
                executorService,
                heartbeatService,
                nodeVersion.toString(),
                systemTableProviders,
                accessControlMetadataFactory,
                directoryLister,
                transactionScopeCachingDirectoryListerFactory,
                usingSystemSecurity,
                hiveConfig.isPartitionProjectionEnabled(),
                allowTableRename,
                hiveConfig.getTimestampPrecision(),
                hiveConfig.getMetadataParallelism());
    }

    public HiveMetadataFactory(
            CatalogName catalogName,
            HiveMetastoreFactory metastoreFactory,
            Set<HiveFileWriterFactory> fileWriterFactories,
            TrinoFileSystemFactory fileSystemFactory,
            HivePartitionManager partitionManager,
            int maxConcurrentFileSystemOperations,
            int maxConcurrentMetastoreDrops,
            int maxConcurrentMetastoreUpdates,
            long maxPartitionDropsPerQuery,
            boolean skipDeletionForAlter,
            boolean skipTargetCleanupOnRollback,
            boolean writesToNonManagedTablesEnabled,
            boolean createsOfNonManagedTablesEnabled,
            boolean deleteSchemaLocationsFallback,
            boolean translateHiveViews,
            boolean hiveViewsRunAsInvoker,
            long perTransactionCacheMaximumSize,
            Optional<Duration> hiveTransactionHeartbeatInterval,
            boolean hideDeltaLakeTables,
            TypeManager typeManager,
            MetadataProvider metadataProvider,
            LocationService locationService,
            JsonCodec<PartitionUpdate> partitionUpdateCodec,
            ExecutorService executorService,
            ScheduledExecutorService heartbeatService,
            String trinoVersion,
            Set<SystemTableProvider> systemTableProviders,
            AccessControlMetadataFactory accessControlMetadataFactory,
            DirectoryLister directoryLister,
            TransactionScopeCachingDirectoryListerFactory transactionScopeCachingDirectoryListerFactory,
            boolean usingSystemSecurity,
            boolean partitionProjectionEnabled,
            boolean allowTableRename,
            HiveTimestampPrecision hiveViewsTimestampPrecision,
            int metadataParallelism)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.skipDeletionForAlter = skipDeletionForAlter;
        this.skipTargetCleanupOnRollback = skipTargetCleanupOnRollback;
        this.writesToNonManagedTablesEnabled = writesToNonManagedTablesEnabled;
        this.createsOfNonManagedTablesEnabled = createsOfNonManagedTablesEnabled;
        this.deleteSchemaLocationsFallback = deleteSchemaLocationsFallback;
        this.translateHiveViews = translateHiveViews;
        this.hiveViewsRunAsInvoker = hiveViewsRunAsInvoker;
        this.hideDeltaLakeTables = hideDeltaLakeTables;
        this.perTransactionCacheMaximumSize = perTransactionCacheMaximumSize;

        this.metastoreFactory = requireNonNull(metastoreFactory, "metastoreFactory is null");
        this.fileWriterFactories = ImmutableSet.copyOf(requireNonNull(fileWriterFactories, "fileWriterFactories is null"));
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.partitionManager = requireNonNull(partitionManager, "partitionManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.metadataProvider = requireNonNull(metadataProvider, "metadataProvider is null");
        this.locationService = requireNonNull(locationService, "locationService is null");
        this.partitionUpdateCodec = requireNonNull(partitionUpdateCodec, "partitionUpdateCodec is null");
        this.trinoVersion = requireNonNull(trinoVersion, "trinoVersion is null");
        this.systemTableProviders = requireNonNull(systemTableProviders, "systemTableProviders is null");
        this.accessControlMetadataFactory = requireNonNull(accessControlMetadataFactory, "accessControlMetadataFactory is null");
        this.hiveTransactionHeartbeatInterval = requireNonNull(hiveTransactionHeartbeatInterval, "hiveTransactionHeartbeatInterval is null");

        fileSystemExecutor = new BoundedExecutor(executorService, maxConcurrentFileSystemOperations);
        dropExecutor = new BoundedExecutor(executorService, maxConcurrentMetastoreDrops);
        if (maxConcurrentMetastoreUpdates == 1) {
            // this will serve as a kill switch in case we observe that parallel updates causes conflicts in metastore's DB side
            updateExecutor = directExecutor();
        }
        else {
            updateExecutor = new BoundedExecutor(executorService, maxConcurrentMetastoreUpdates);
        }
        this.maxPartitionDropsPerQuery = maxPartitionDropsPerQuery;
        this.heartbeatService = requireNonNull(heartbeatService, "heartbeatService is null");
        this.directoryLister = requireNonNull(directoryLister, "directoryLister is null");
        this.transactionScopeCachingDirectoryListerFactory = requireNonNull(transactionScopeCachingDirectoryListerFactory, "transactionScopeCachingDirectoryListerFactory is null");
        this.usingSystemSecurity = usingSystemSecurity;
        this.partitionProjectionEnabled = partitionProjectionEnabled;
        this.allowTableRename = allowTableRename;
        this.hiveViewsTimestampPrecision = requireNonNull(hiveViewsTimestampPrecision, "hiveViewsTimestampPrecision is null");
        if (metadataParallelism == 1) {
            this.metadataFetchingExecutor = directExecutor();
        }
        else {
            this.metadataFetchingExecutor = new BoundedExecutor(executorService, metadataParallelism);
        }
    }

    @Override
    public TransactionalMetadata create(ConnectorIdentity identity, boolean autoCommit)
    {
        HiveMetastore hiveMetastore = createPerTransactionCache(metastoreFactory.createMetastore(Optional.of(identity)), perTransactionCacheMaximumSize);

        DirectoryLister directoryLister = transactionScopeCachingDirectoryListerFactory.get(this.directoryLister);
        SemiTransactionalHiveMetastore metastore = new SemiTransactionalHiveMetastore(
                typeManager,
                partitionProjectionEnabled,
                fileSystemFactory,
                hiveMetastore,
                fileSystemExecutor,
                dropExecutor,
                updateExecutor,
                skipDeletionForAlter,
                skipTargetCleanupOnRollback,
                deleteSchemaLocationsFallback,
                hiveTransactionHeartbeatInterval,
                heartbeatService,
                directoryLister);

        return new HiveMetadata(
                catalogName,
                metastore,
                autoCommit,
                fileWriterFactories,
                fileSystemFactory,
                partitionManager,
                writesToNonManagedTablesEnabled,
                createsOfNonManagedTablesEnabled,
                translateHiveViews,
                hiveViewsRunAsInvoker,
                hideDeltaLakeTables,
                typeManager,
                metadataProvider,
                locationService,
                partitionUpdateCodec,
                trinoVersion,
                new MetastoreHiveStatisticsProvider(metastore),
                systemTableProviders,
                accessControlMetadataFactory.create(metastore),
                directoryLister,
                usingSystemSecurity,
                partitionProjectionEnabled,
                allowTableRename,
                maxPartitionDropsPerQuery,
                hiveViewsTimestampPrecision,
                metadataFetchingExecutor);
    }
}
