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
package io.trino.plugin.hidden.partitioning;

import io.airlift.concurrent.BoundedExecutor;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.ForHiveTransactionHeartbeats;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HiveMaterializedViewMetadata;
import io.trino.plugin.hive.HiveMetadataFactory;
import io.trino.plugin.hive.HiveMetastoreClosure;
import io.trino.plugin.hive.HivePartitionManager;
import io.trino.plugin.hive.HiveRedirectionsProvider;
import io.trino.plugin.hive.HiveTableRedirectionsProvider;
import io.trino.plugin.hive.LocationService;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.PartitionUpdate;
import io.trino.plugin.hive.SystemTableProvider;
import io.trino.plugin.hive.TransactionalMetadata;
import io.trino.plugin.hive.TransactionalMetadataFactory;
import io.trino.plugin.hive.fs.DirectoryLister;
import io.trino.plugin.hive.fs.TransactionScopeCachingDirectoryLister;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.MetastoreConfig;
import io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.trino.plugin.hive.security.AccessControlMetadataFactory;
import io.trino.plugin.hive.statistics.MetastoreHiveStatisticsProvider;
import io.trino.spi.connector.MetadataProvider;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeManager;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.trino.plugin.hive.metastore.cache.CachingHiveMetastore.memoizeMetastore;
import static java.util.Objects.requireNonNull;

public class HiddenPartitioningMetadataFactory
        implements TransactionalMetadataFactory
{
    private static final Logger log = Logger.get(HiveMetadataFactory.class);

    private final CatalogName catalogName;
    private final boolean skipDeletionForAlter;
    private final boolean skipTargetCleanupOnRollback;
    private final boolean writesToNonManagedTablesEnabled;
    private final boolean createsOfNonManagedTablesEnabled;
    private final boolean translateHiveViews;
    private final boolean hideDeltaLakeTables;
    private final long perTransactionCacheMaximumSize;
    private final HiveMetastoreFactory metastoreFactory;
    private final HdfsEnvironment hdfsEnvironment;
    private final HivePartitionManager partitionManager;
    private final DateTimeZone timeZone;
    private final TypeManager typeManager;
    private final LocationService locationService;
    private final JsonCodec<PartitionUpdate> partitionUpdateCodec;
    private final BoundedExecutor renameExecution;
    private final BoundedExecutor dropExecutor;
    private final BoundedExecutor updateExecutor;
    private final String prestoVersion;
    private final HiveRedirectionsProvider hiveRedirectionsProvider;
    private final HiveMaterializedViewMetadata hiveMaterializedViewMetadata;
    private final AccessControlMetadataFactory accessControlMetadataFactory;
    private final Optional<Duration> hiveTransactionHeartbeatInterval;
    private final ScheduledExecutorService heartbeatService;

    private Set<SystemTableProvider> systemTableProviders;
    private MetadataProvider metadataProvider;
    private HiveTableRedirectionsProvider hiveTableRedirectionsProvider;
    private final DirectoryLister directoryLister;
    private final long perTransactionFileStatusCacheMaximumSize;

    @Inject
    @SuppressWarnings("deprecation")
    public HiddenPartitioningMetadataFactory(
            CatalogName catalogName,
            HiveConfig hiveConfig,
            MetastoreConfig metastoreConfig,
            HiveMetastoreFactory metastoreFactory,
            HdfsEnvironment hdfsEnvironment,
            HivePartitionManager partitionManager,
            ExecutorService executorService,
            @ForHiveTransactionHeartbeats ScheduledExecutorService heartbeatService,
            TypeManager typeManager,
            MetadataProvider metadataProvider,
            LocationService locationService,
            JsonCodec<PartitionUpdate> partitionUpdateCodec,
            NodeVersion nodeVersion,
            HiveRedirectionsProvider hiveRedirectionsProvider,
            Set<SystemTableProvider> systemTableProviders,
            HiveMaterializedViewMetadata hiveMaterializedViewMetadata,
            AccessControlMetadataFactory accessControlMetadataFactory,
            HiveTableRedirectionsProvider hiveTableRedirectionsProvider,
            DirectoryLister directoryLister)
    {
        this(
                catalogName,
                metastoreFactory,
                hdfsEnvironment,
                partitionManager,
                hiveConfig.getOrcLegacyDateTimeZone(),
                hiveConfig.getMaxConcurrentFileRenames(),
                hiveConfig.getMaxConcurrentMetastoreDrops(),
                hiveConfig.getMaxConcurrentMetastoreUpdates(),
                hiveConfig.isSkipDeletionForAlter(),
                hiveConfig.isSkipTargetCleanupOnRollback(),
                hiveConfig.getWritesToNonManagedTablesEnabled(),
                hiveConfig.getCreatesOfNonManagedTablesEnabled(),
                hiveConfig.isTranslateHiveViews(),
                metastoreConfig.isHideDeltaLakeTables(),
                hiveConfig.getPerTransactionMetastoreCacheMaximumSize(),
                hiveConfig.getHiveTransactionHeartbeatInterval(),
                typeManager,
                metadataProvider,
                locationService,
                partitionUpdateCodec,
                executorService,
                heartbeatService,
                nodeVersion.toString(),
                hiveRedirectionsProvider,
                systemTableProviders,
                hiveMaterializedViewMetadata,
                accessControlMetadataFactory,
                hiveTableRedirectionsProvider,
                directoryLister,
                hiveConfig.getPerTransactionFileStatusCacheMaximumSize());
    }

    public HiddenPartitioningMetadataFactory(
            CatalogName catalogName,
            HiveMetastoreFactory metastoreFactory,
            HdfsEnvironment hdfsEnvironment,
            HivePartitionManager partitionManager,
            DateTimeZone timeZone,
            int maxConcurrentFileRenames,
            int maxConcurrentMetastoreDrops,
            int maxConcurrentMetastoreUpdates,
            boolean skipDeletionForAlter,
            boolean skipTargetCleanupOnRollback,
            boolean writesToNonManagedTablesEnabled,
            boolean createsOfNonManagedTablesEnabled,
            boolean translateHiveViews,
            boolean hideDeltaLakeTables,
            long perTransactionCacheMaximumSize,
            Optional<Duration> hiveTransactionHeartbeatInterval,
            TypeManager typeManager,
            MetadataProvider metadataProvider,
            LocationService locationService,
            JsonCodec<PartitionUpdate> partitionUpdateCodec,
            ExecutorService executorService,
            ScheduledExecutorService heartbeatService,
            String prestoVersion,
            HiveRedirectionsProvider hiveRedirectionsProvider,
            Set<SystemTableProvider> systemTableProviders,
            HiveMaterializedViewMetadata hiveMaterializedViewMetadata,
            AccessControlMetadataFactory accessControlMetadataFactory,
            HiveTableRedirectionsProvider hiveTableRedirectionsProvider,
            DirectoryLister directoryLister,
            long perTransactionFileStatusCacheMaximumSize)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.skipDeletionForAlter = skipDeletionForAlter;
        this.skipTargetCleanupOnRollback = skipTargetCleanupOnRollback;
        this.writesToNonManagedTablesEnabled = writesToNonManagedTablesEnabled;
        this.createsOfNonManagedTablesEnabled = createsOfNonManagedTablesEnabled;
        this.translateHiveViews = translateHiveViews;
        this.hideDeltaLakeTables = hideDeltaLakeTables;
        this.perTransactionCacheMaximumSize = perTransactionCacheMaximumSize;

        this.metastoreFactory = requireNonNull(metastoreFactory, "metastore is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.partitionManager = requireNonNull(partitionManager, "partitionManager is null");
        this.timeZone = requireNonNull(timeZone, "timeZone is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.metadataProvider = requireNonNull(metadataProvider, "metadata provider is null");
        this.locationService = requireNonNull(locationService, "locationService is null");
        this.partitionUpdateCodec = requireNonNull(partitionUpdateCodec, "partitionUpdateCodec is null");
        this.prestoVersion = requireNonNull(prestoVersion, "prestoVersion is null");
        this.hiveRedirectionsProvider = requireNonNull(hiveRedirectionsProvider, "hiveRedirectionsProvider is null");
        this.hiveMaterializedViewMetadata = requireNonNull(hiveMaterializedViewMetadata, "hiveMaterializedViewMetadata is null");
        this.accessControlMetadataFactory = requireNonNull(accessControlMetadataFactory, "accessControlMetadataFactory is null");
        this.hiveTransactionHeartbeatInterval = requireNonNull(hiveTransactionHeartbeatInterval, "hiveTransactionHeartbeatInterval is null");
        this.systemTableProviders = requireNonNull(systemTableProviders, "systemTableProviders is null");

        if (!timeZone.equals(DateTimeZone.getDefault())) {
            log.warn("Hive writes are disabled. " +
                            "To write data to Hive, your JVM timezone must match the Hive storage timezone. " +
                            "Add -Duser.timezone=%s to your JVM arguments",
                    timeZone.getID());
        }

        renameExecution = new BoundedExecutor(executorService, maxConcurrentFileRenames);
        dropExecutor = new BoundedExecutor(executorService, maxConcurrentMetastoreDrops);
        updateExecutor = new BoundedExecutor(executorService, maxConcurrentMetastoreUpdates);
        this.heartbeatService = requireNonNull(heartbeatService, "heartbeatService is null");
        this.hiveTableRedirectionsProvider = requireNonNull(hiveTableRedirectionsProvider, "hiveTableRedirectionsProvider is null");
        this.directoryLister = requireNonNull(directoryLister, "directoryLister is null");
        this.perTransactionFileStatusCacheMaximumSize = perTransactionFileStatusCacheMaximumSize;
    }

    @Override
    public TransactionalMetadata create(ConnectorIdentity connectorIdentity, boolean autocommit)
    {
        DirectoryLister directoryLister = new TransactionScopeCachingDirectoryLister(this.directoryLister, perTransactionFileStatusCacheMaximumSize);

        SemiTransactionalHiveMetastore metastore = new SemiTransactionalHiveMetastore(
                hdfsEnvironment,
                new HiveMetastoreClosure(memoizeMetastore(this.metastoreFactory.createMetastore(Optional.of(connectorIdentity)), perTransactionCacheMaximumSize)), // per-transaction cache
                renameExecution,
                dropExecutor,
                updateExecutor,
                skipDeletionForAlter,
                skipTargetCleanupOnRollback,
                autocommit,
                hiveTransactionHeartbeatInterval,
                heartbeatService,
                directoryLister);

        return new HiddenPartitioningHiveMetadata(
                catalogName,
                metastore,
                hdfsEnvironment,
                partitionManager,
                timeZone,
                writesToNonManagedTablesEnabled,
                createsOfNonManagedTablesEnabled,
                translateHiveViews,
                hideDeltaLakeTables,
                typeManager,
                metadataProvider,
                locationService,
                partitionUpdateCodec,
                prestoVersion,
                new MetastoreHiveStatisticsProvider(metastore),
                hiveRedirectionsProvider,
                systemTableProviders,
                hiveMaterializedViewMetadata,
                accessControlMetadataFactory.create(metastore),
                hiveTableRedirectionsProvider,
                autocommit,
                directoryLister);
    }

    public Set<SystemTableProvider> getSystemTableProviders()
    {
        return systemTableProviders;
    }
}
