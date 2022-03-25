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

import io.airlift.json.JsonCodec;
import io.trino.plugin.deltalake.metastore.HiveMetastoreBackedDeltaLakeMetastore;
import io.trino.plugin.deltalake.statistics.CachingExtendedStatisticsAccess;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointWriterManager;
import io.trino.plugin.deltalake.transactionlog.writer.TransactionLogWriterFactory;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.cache.CachingHiveMetastore;
import io.trino.spi.NodeManager;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeManager;

import javax.inject.Inject;

import java.util.Optional;

import static io.trino.plugin.hive.metastore.cache.CachingHiveMetastore.memoizeMetastore;
import static java.util.Objects.requireNonNull;

public class DeltaLakeMetadataFactory
{
    private final HiveMetastoreFactory hiveMetastoreFactory;
    private final HdfsEnvironment hdfsEnvironment;
    private final TransactionLogAccess transactionLogAccess;
    private final TypeManager typeManager;
    private final JsonCodec<DataFileInfo> dataFileInfoCodec;
    private final JsonCodec<DeltaLakeUpdateResult> updateResultJsonCodec;
    private final TransactionLogWriterFactory transactionLogWriterFactory;
    private final NodeManager nodeManager;
    private final CheckpointWriterManager checkpointWriterManager;
    private final CachingExtendedStatisticsAccess statisticsAccess;
    private final int domainCompactionThreshold;
    private final boolean hideNonDeltaLakeTables;
    private final boolean unsafeWritesEnabled;
    private final long checkpointWritingInterval;
    private final boolean ignoreCheckpointWriteFailures;
    private final long perTransactionMetastoreCacheMaximumSize;
    private final boolean deleteSchemaLocationsFallback;

    @Inject
    public DeltaLakeMetadataFactory(
            HiveMetastoreFactory hiveMetastoreFactory,
            HdfsEnvironment hdfsEnvironment,
            TransactionLogAccess transactionLogAccess,
            TypeManager typeManager,
            DeltaLakeConfig deltaLakeConfig,
            @HideNonDeltaLakeTables boolean hideNonDeltaLakeTables,
            JsonCodec<DataFileInfo> dataFileInfoCodec,
            JsonCodec<DeltaLakeUpdateResult> updateResultJsonCodec,
            TransactionLogWriterFactory transactionLogWriterFactory,
            NodeManager nodeManager,
            CheckpointWriterManager checkpointWriterManager,
            CachingExtendedStatisticsAccess statisticsAccess,
            HiveConfig hiveConfig)
    {
        this.hiveMetastoreFactory = requireNonNull(hiveMetastoreFactory, "hiveMetastore is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.transactionLogAccess = requireNonNull(transactionLogAccess, "transactionLogAccess is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.dataFileInfoCodec = requireNonNull(dataFileInfoCodec, "dataFileInfoCodec is null");
        this.updateResultJsonCodec = requireNonNull(updateResultJsonCodec, "updateResultJsonCodec is null");
        this.transactionLogWriterFactory = requireNonNull(transactionLogWriterFactory, "transactionLogWriterFactory is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.checkpointWriterManager = requireNonNull(checkpointWriterManager, "checkpointWriterManager is null");
        this.statisticsAccess = requireNonNull(statisticsAccess, "statisticsAccess is null");
        requireNonNull(deltaLakeConfig, "deltaLakeConfig is null");
        this.domainCompactionThreshold = deltaLakeConfig.getDomainCompactionThreshold();
        this.hideNonDeltaLakeTables = hideNonDeltaLakeTables;
        this.unsafeWritesEnabled = deltaLakeConfig.getUnsafeWritesEnabled();
        this.checkpointWritingInterval = deltaLakeConfig.getDefaultCheckpointWritingInterval();
        this.ignoreCheckpointWriteFailures = deltaLakeConfig.isIgnoreCheckpointWriteFailures();
        this.perTransactionMetastoreCacheMaximumSize = hiveConfig.getPerTransactionMetastoreCacheMaximumSize();
        this.deleteSchemaLocationsFallback = hiveConfig.isDeleteSchemaLocationsFallback();
    }

    public DeltaLakeMetadata create(ConnectorIdentity identity)
    {
        // create per-transaction cache over hive metastore interface
        CachingHiveMetastore cachingHiveMetastore = memoizeMetastore(
                hiveMetastoreFactory.createMetastore(Optional.of(identity)),
                perTransactionMetastoreCacheMaximumSize);
        HiveMetastoreBackedDeltaLakeMetastore deltaLakeMetastore = new HiveMetastoreBackedDeltaLakeMetastore(
                cachingHiveMetastore,
                transactionLogAccess,
                typeManager,
                statisticsAccess);
        return new DeltaLakeMetadata(
                deltaLakeMetastore,
                hdfsEnvironment,
                typeManager,
                domainCompactionThreshold,
                hideNonDeltaLakeTables,
                unsafeWritesEnabled,
                dataFileInfoCodec,
                updateResultJsonCodec,
                transactionLogWriterFactory,
                nodeManager,
                checkpointWriterManager,
                checkpointWritingInterval,
                ignoreCheckpointWriteFailures,
                deleteSchemaLocationsFallback,
                statisticsAccess);
    }
}
