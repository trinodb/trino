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

import io.airlift.concurrent.BoundedExecutor;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import io.prestosql.plugin.base.CatalogName;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.prestosql.plugin.hive.security.AccessControlMetadataFactory;
import io.prestosql.plugin.hive.statistics.MetastoreHiveStatisticsProvider;
import io.prestosql.spi.type.TypeManager;

import javax.inject.Inject;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.prestosql.plugin.hive.metastore.cache.CachingHiveMetastore.memoizeMetastore;
import static java.util.Objects.requireNonNull;

public class HiveMetadataFactory
        implements TransactionalMetadataFactory
{
    private final CatalogName catalogName;
    private final boolean skipDeletionForAlter;
    private final boolean skipTargetCleanupOnRollback;
    private final boolean writesToNonManagedTablesEnabled;
    private final boolean createsOfNonManagedTablesEnabled;
    private final boolean translateHiveViews;
    private final long perTransactionCacheMaximumSize;
    private final HiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;
    private final HivePartitionManager partitionManager;
    private final TypeManager typeManager;
    private final LocationService locationService;
    private final JsonCodec<PartitionUpdate> partitionUpdateCodec;
    private final BoundedExecutor renameExecution;
    private final BoundedExecutor dropExecutor;
    private final String prestoVersion;
    private final AccessControlMetadataFactory accessControlMetadataFactory;
    private final Optional<Duration> hiveTransactionHeartbeatInterval;
    private final ScheduledExecutorService heartbeatService;

    @Inject
    @SuppressWarnings("deprecation")
    public HiveMetadataFactory(
            CatalogName catalogName,
            HiveConfig hiveConfig,
            HiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            HivePartitionManager partitionManager,
            ExecutorService executorService,
            @ForHiveTransactionHeartbeats ScheduledExecutorService heartbeatService,
            TypeManager typeManager,
            LocationService locationService,
            JsonCodec<PartitionUpdate> partitionUpdateCodec,
            NodeVersion nodeVersion,
            AccessControlMetadataFactory accessControlMetadataFactory)
    {
        this(
                catalogName,
                metastore,
                hdfsEnvironment,
                partitionManager,
                hiveConfig.getMaxConcurrentFileRenames(),
                hiveConfig.getMaxConcurrentMetastoreDrops(),
                hiveConfig.isSkipDeletionForAlter(),
                hiveConfig.isSkipTargetCleanupOnRollback(),
                hiveConfig.getWritesToNonManagedTablesEnabled(),
                hiveConfig.getCreatesOfNonManagedTablesEnabled(),
                hiveConfig.isTranslateHiveViews(),
                hiveConfig.getPerTransactionMetastoreCacheMaximumSize(),
                hiveConfig.getHiveTransactionHeartbeatInterval(),
                typeManager,
                locationService,
                partitionUpdateCodec,
                executorService,
                heartbeatService,
                nodeVersion.toString(),
                accessControlMetadataFactory);
    }

    public HiveMetadataFactory(
            CatalogName catalogName,
            HiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            HivePartitionManager partitionManager,
            int maxConcurrentFileRenames,
            int maxConcurrentMetastoreDrops,
            boolean skipDeletionForAlter,
            boolean skipTargetCleanupOnRollback,
            boolean writesToNonManagedTablesEnabled,
            boolean createsOfNonManagedTablesEnabled,
            boolean translateHiveViews,
            long perTransactionCacheMaximumSize,
            Optional<Duration> hiveTransactionHeartbeatInterval,
            TypeManager typeManager,
            LocationService locationService,
            JsonCodec<PartitionUpdate> partitionUpdateCodec,
            ExecutorService executorService,
            ScheduledExecutorService heartbeatService,
            String prestoVersion,
            AccessControlMetadataFactory accessControlMetadataFactory)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.skipDeletionForAlter = skipDeletionForAlter;
        this.skipTargetCleanupOnRollback = skipTargetCleanupOnRollback;
        this.writesToNonManagedTablesEnabled = writesToNonManagedTablesEnabled;
        this.createsOfNonManagedTablesEnabled = createsOfNonManagedTablesEnabled;
        this.translateHiveViews = translateHiveViews;
        this.perTransactionCacheMaximumSize = perTransactionCacheMaximumSize;

        this.metastore = requireNonNull(metastore, "metastore is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.partitionManager = requireNonNull(partitionManager, "partitionManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.locationService = requireNonNull(locationService, "locationService is null");
        this.partitionUpdateCodec = requireNonNull(partitionUpdateCodec, "partitionUpdateCodec is null");
        this.prestoVersion = requireNonNull(prestoVersion, "prestoVersion is null");
        this.accessControlMetadataFactory = requireNonNull(accessControlMetadataFactory, "accessControlMetadataFactory is null");
        this.hiveTransactionHeartbeatInterval = requireNonNull(hiveTransactionHeartbeatInterval, "hiveTransactionHeartbeatInterval is null");

        renameExecution = new BoundedExecutor(executorService, maxConcurrentFileRenames);
        dropExecutor = new BoundedExecutor(executorService, maxConcurrentMetastoreDrops);
        this.heartbeatService = requireNonNull(heartbeatService, "heartbeatService is null");
    }

    @Override
    public TransactionalMetadata create()
    {
        SemiTransactionalHiveMetastore metastore = new SemiTransactionalHiveMetastore(
                hdfsEnvironment,
                new HiveMetastoreClosure(memoizeMetastore(this.metastore, perTransactionCacheMaximumSize)), // per-transaction cache
                renameExecution,
                dropExecutor,
                skipDeletionForAlter,
                skipTargetCleanupOnRollback,
                hiveTransactionHeartbeatInterval,
                heartbeatService);

        return new HiveMetadata(
                catalogName,
                metastore,
                hdfsEnvironment,
                partitionManager,
                writesToNonManagedTablesEnabled,
                createsOfNonManagedTablesEnabled,
                translateHiveViews,
                typeManager,
                locationService,
                partitionUpdateCodec,
                prestoVersion,
                new MetastoreHiveStatisticsProvider(metastore),
                accessControlMetadataFactory.create(metastore));
    }
}
