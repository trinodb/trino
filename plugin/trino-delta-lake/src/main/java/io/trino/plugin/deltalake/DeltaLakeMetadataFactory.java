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
package io.trino.plugin.deltalake;

import com.google.inject.Inject;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.json.JsonCodec;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.cache.CachingHiveMetastore;
import io.trino.plugin.deltalake.metastore.DeltaLakeTableMetadataScheduler;
import io.trino.plugin.deltalake.metastore.HiveMetastoreBackedDeltaLakeMetastore;
import io.trino.plugin.deltalake.statistics.CachingExtendedStatisticsAccess;
import io.trino.plugin.deltalake.statistics.FileBasedTableStatisticsProvider;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointWriterManager;
import io.trino.plugin.deltalake.transactionlog.writer.TransactionLogWriterFactory;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.TrinoViewHiveMetastore;
import io.trino.plugin.hive.security.UsingSystemSecurity;
import io.trino.spi.NodeManager;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeManager;

import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.metastore.cache.CachingHiveMetastore.createPerTransactionCache;
import static java.util.Objects.requireNonNull;

public class DeltaLakeMetadataFactory
{
    private final HiveMetastoreFactory hiveMetastoreFactory;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final TransactionLogAccess transactionLogAccess;
    private final TypeManager typeManager;
    private final JsonCodec<DataFileInfo> dataFileInfoCodec;
    private final JsonCodec<DeltaLakeMergeResult> mergeResultJsonCodec;
    private final TransactionLogWriterFactory transactionLogWriterFactory;
    private final NodeManager nodeManager;
    private final CheckpointWriterManager checkpointWriterManager;
    private final CachingExtendedStatisticsAccess statisticsAccess;
    private final int domainCompactionThreshold;
    private final boolean unsafeWritesEnabled;
    private final long checkpointWritingInterval;
    private final long perTransactionMetastoreCacheMaximumSize;
    private final boolean deleteSchemaLocationsFallback;
    private final boolean useUniqueTableLocation;
    private final DeltaLakeTableMetadataScheduler metadataScheduler;
    private final Executor metadataFetchingExecutor;
    private final boolean allowManagedTableRename;
    private final boolean usingSystemSecurity;
    private final String trinoVersion;

    @Inject
    public DeltaLakeMetadataFactory(
            HiveMetastoreFactory hiveMetastoreFactory,
            TrinoFileSystemFactory fileSystemFactory,
            TransactionLogAccess transactionLogAccess,
            TypeManager typeManager,
            DeltaLakeConfig deltaLakeConfig,
            JsonCodec<DataFileInfo> dataFileInfoCodec,
            JsonCodec<DeltaLakeMergeResult> mergeResultJsonCodec,
            TransactionLogWriterFactory transactionLogWriterFactory,
            NodeManager nodeManager,
            CheckpointWriterManager checkpointWriterManager,
            CachingExtendedStatisticsAccess statisticsAccess,
            @AllowDeltaLakeManagedTableRename boolean allowManagedTableRename,
            @UsingSystemSecurity boolean useSystemSecurity,
            NodeVersion nodeVersion,
            DeltaLakeTableMetadataScheduler metadataScheduler,
            ExecutorService executorService)
    {
        this.hiveMetastoreFactory = requireNonNull(hiveMetastoreFactory, "hiveMetastore is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.transactionLogAccess = requireNonNull(transactionLogAccess, "transactionLogAccess is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.dataFileInfoCodec = requireNonNull(dataFileInfoCodec, "dataFileInfoCodec is null");
        this.mergeResultJsonCodec = requireNonNull(mergeResultJsonCodec, "mergeResultJsonCodec is null");
        this.transactionLogWriterFactory = requireNonNull(transactionLogWriterFactory, "transactionLogWriterFactory is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.checkpointWriterManager = requireNonNull(checkpointWriterManager, "checkpointWriterManager is null");
        this.statisticsAccess = requireNonNull(statisticsAccess, "statisticsAccess is null");
        this.domainCompactionThreshold = deltaLakeConfig.getDomainCompactionThreshold();
        this.unsafeWritesEnabled = deltaLakeConfig.getUnsafeWritesEnabled();
        this.checkpointWritingInterval = deltaLakeConfig.getDefaultCheckpointWritingInterval();
        this.perTransactionMetastoreCacheMaximumSize = deltaLakeConfig.getPerTransactionMetastoreCacheMaximumSize();
        this.deleteSchemaLocationsFallback = deltaLakeConfig.isDeleteSchemaLocationsFallback();
        this.useUniqueTableLocation = deltaLakeConfig.isUniqueTableLocation();
        this.allowManagedTableRename = allowManagedTableRename;
        this.usingSystemSecurity = useSystemSecurity;
        this.trinoVersion = requireNonNull(nodeVersion, "nodeVersion is null").toString();
        this.metadataScheduler = requireNonNull(metadataScheduler, "metadataScheduler is null");
        if (deltaLakeConfig.getMetadataParallelism() == 1) {
            this.metadataFetchingExecutor = directExecutor();
        }
        else {
            this.metadataFetchingExecutor = new BoundedExecutor(executorService, deltaLakeConfig.getMetadataParallelism());
        }
    }

    public DeltaLakeMetadata create(ConnectorIdentity identity)
    {
        CachingHiveMetastore cachingHiveMetastore = createPerTransactionCache(
                hiveMetastoreFactory.createMetastore(Optional.of(identity)),
                perTransactionMetastoreCacheMaximumSize);
        HiveMetastoreBackedDeltaLakeMetastore deltaLakeMetastore = new HiveMetastoreBackedDeltaLakeMetastore(cachingHiveMetastore);
        FileBasedTableStatisticsProvider tableStatisticsProvider = new FileBasedTableStatisticsProvider(
                typeManager,
                transactionLogAccess,
                statisticsAccess);
        TrinoViewHiveMetastore trinoViewHiveMetastore = new TrinoViewHiveMetastore(
                cachingHiveMetastore,
                usingSystemSecurity,
                trinoVersion,
                "Trino Delta Lake connector");
        return new DeltaLakeMetadata(
                deltaLakeMetastore,
                transactionLogAccess,
                tableStatisticsProvider,
                fileSystemFactory,
                typeManager,
                trinoViewHiveMetastore,
                domainCompactionThreshold,
                unsafeWritesEnabled,
                dataFileInfoCodec,
                mergeResultJsonCodec,
                transactionLogWriterFactory,
                nodeManager,
                checkpointWriterManager,
                checkpointWritingInterval,
                deleteSchemaLocationsFallback,
                statisticsAccess,
                metadataScheduler,
                useUniqueTableLocation,
                allowManagedTableRename,
                metadataFetchingExecutor);
    }
}
