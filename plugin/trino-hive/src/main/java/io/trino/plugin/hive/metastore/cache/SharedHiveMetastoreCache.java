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
package io.trino.plugin.hive.metastore.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;
import com.google.common.math.LongMath;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.units.Duration;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.spi.NodeManager;
import io.trino.spi.TrinoException;
import io.trino.spi.security.ConnectorIdentity;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.collect.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.plugin.hive.metastore.cache.CachingHiveMetastore.cachingHiveMetastore;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class SharedHiveMetastoreCache
{
    private final boolean enabled;
    private final CatalogName catalogName;
    private final Duration metastoreCacheTtl;
    private final Optional<Duration> metastoreRefreshInterval;
    private final long metastoreCacheMaximumSize;
    private final int maxMetastoreRefreshThreads;

    private final Duration userMetastoreCacheTtl;
    private final long userMetastoreCacheMaximumSize;

    private ExecutorService executorService;

    @Inject
    public SharedHiveMetastoreCache(
            CatalogName catalogName,
            NodeManager nodeManager,
            CachingHiveMetastoreConfig config,
            ImpersonationCachingConfig impersonationCachingConfig)
    {
        requireNonNull(nodeManager, "nodeManager is null");
        requireNonNull(config, "config is null");
        requireNonNull(catalogName, "catalogName is null");

        this.catalogName = catalogName;
        maxMetastoreRefreshThreads = config.getMaxMetastoreRefreshThreads();
        metastoreCacheTtl = config.getMetastoreCacheTtl();
        metastoreRefreshInterval = config.getMetastoreRefreshInterval();
        metastoreCacheMaximumSize = config.getMetastoreCacheMaximumSize();

        userMetastoreCacheTtl = impersonationCachingConfig.getUserMetastoreCacheTtl();
        userMetastoreCacheMaximumSize = impersonationCachingConfig.getUserMetastoreCacheMaximumSize();

        // Disable caching on workers, because there currently is no way to invalidate such a cache.
        // Note: while we could skip CachingHiveMetastoreModule altogether on workers, we retain it so that catalog
        // configuration can remain identical for all nodes, making cluster configuration easier.
        enabled = nodeManager.getCurrentNode().isCoordinator() &&
                metastoreCacheTtl.toMillis() > 0 &&
                metastoreCacheMaximumSize > 0;
    }

    @PostConstruct
    public void start()
    {
        if (enabled) {
            executorService = newCachedThreadPool(daemonThreadsNamed("hive-metastore-" + catalogName + "-%s"));
        }
    }

    @PreDestroy
    public void stop()
    {
        if (executorService != null) {
            executorService.shutdownNow();
            executorService = null;
        }
    }

    public boolean isEnabled()
    {
        return enabled;
    }

    public HiveMetastoreFactory createCachingHiveMetastoreFactory(HiveMetastoreFactory metastoreFactory)
    {
        if (!enabled) {
            return metastoreFactory;
        }

        if (metastoreFactory.isImpersonationEnabled()) {
            // per user cache can be disabled also
            if (userMetastoreCacheMaximumSize == 0 || userMetastoreCacheTtl.toMillis() == 0) {
                return metastoreFactory;
            }
            return new ImpersonationCachingHiveMetastoreFactory(metastoreFactory);
        }

        CachingHiveMetastore cachingHiveMetastore = cachingHiveMetastore(
                // Loading of cache entry in CachingHiveMetastore might trigger loading of another cache entry for different object type
                // In case there are no empty executor slots, such operation would deadlock. Therefore, a reentrant executor needs to be
                // used.
                metastoreFactory.createMetastore(Optional.empty()),
                new ReentrantBoundedExecutor(executorService, maxMetastoreRefreshThreads),
                metastoreCacheTtl,
                metastoreRefreshInterval,
                metastoreCacheMaximumSize);
        return new CachingHiveMetastoreFactory(cachingHiveMetastore);
    }

    public static class CachingHiveMetastoreFactory
            implements HiveMetastoreFactory
    {
        private final CachingHiveMetastore metastore;

        private CachingHiveMetastoreFactory(CachingHiveMetastore metastore)
        {
            this.metastore = requireNonNull(metastore, "metastore is null");
        }

        @Override
        public boolean isImpersonationEnabled()
        {
            return false;
        }

        @Override
        public HiveMetastore createMetastore(Optional<ConnectorIdentity> identity)
        {
            return metastore;
        }

        @Nested
        @Flatten
        public CachingHiveMetastore getMetastore()
        {
            return metastore;
        }
    }

    public class ImpersonationCachingHiveMetastoreFactory
            implements HiveMetastoreFactory
    {
        private final HiveMetastoreFactory metastoreFactory;
        private final LoadingCache<String, CachingHiveMetastore> cache;

        public ImpersonationCachingHiveMetastoreFactory(HiveMetastoreFactory metastoreFactory)
        {
            this.metastoreFactory = metastoreFactory;
            cache = buildNonEvictableCache(
                    CacheBuilder.newBuilder()
                            .expireAfterWrite(userMetastoreCacheTtl.toMillis(), MILLISECONDS)
                            .maximumSize(userMetastoreCacheMaximumSize),
                    CacheLoader.from(this::createUserCachingMetastore));
        }

        @Override
        public boolean isImpersonationEnabled()
        {
            return true;
        }

        @Override
        public HiveMetastore createMetastore(Optional<ConnectorIdentity> identity)
        {
            checkArgument(identity.isPresent(), "Identity must be present for impersonation cache");
            try {
                return cache.getUnchecked(identity.get().getUser());
            }
            catch (UncheckedExecutionException e) {
                throwIfInstanceOf(e.getCause(), TrinoException.class);
                throw e;
            }
        }

        private CachingHiveMetastore createUserCachingMetastore(String user)
        {
            ConnectorIdentity identity = ConnectorIdentity.ofUser(user);
            return cachingHiveMetastore(
                    metastoreFactory.createMetastore(Optional.of(identity)),
                    new ReentrantBoundedExecutor(executorService, maxMetastoreRefreshThreads),
                    metastoreCacheTtl,
                    metastoreRefreshInterval,
                    metastoreCacheMaximumSize);
        }

        @Managed
        public void flushCache()
        {
            cache.asMap().values().forEach(CachingHiveMetastore::flushCache);
        }

        @Managed
        @Nested
        public AggregateCacheStatsMBean getDatabaseStats()
        {
            return new AggregateCacheStatsMBean(CachingHiveMetastore::getDatabaseCache);
        }

        @Managed
        @Nested
        public AggregateCacheStatsMBean getDatabaseNamesStats()
        {
            return new AggregateCacheStatsMBean(CachingHiveMetastore::getDatabaseNamesCache);
        }

        @Managed
        @Nested
        public AggregateCacheStatsMBean getTableStats()
        {
            return new AggregateCacheStatsMBean(CachingHiveMetastore::getTableCache);
        }

        @Managed
        @Nested
        public AggregateCacheStatsMBean getTableNamesStats()
        {
            return new AggregateCacheStatsMBean(CachingHiveMetastore::getTableNamesCache);
        }

        @Managed
        @Nested
        public AggregateCacheStatsMBean getTableWithParameterStats()
        {
            return new AggregateCacheStatsMBean(CachingHiveMetastore::getTablesWithParameterCache);
        }

        @Managed
        @Nested
        public AggregateCacheStatsMBean getTableStatisticsStats()
        {
            return new AggregateCacheStatsMBean(CachingHiveMetastore::getTableStatisticsCache);
        }

        @Managed
        @Nested
        public AggregateCacheStatsMBean getPartitionStatisticsStats()
        {
            return new AggregateCacheStatsMBean(CachingHiveMetastore::getPartitionStatisticsCache);
        }

        @Managed
        @Nested
        public AggregateCacheStatsMBean getViewNamesStats()
        {
            return new AggregateCacheStatsMBean(CachingHiveMetastore::getViewNamesCache);
        }

        @Managed
        @Nested
        public AggregateCacheStatsMBean getPartitionStats()
        {
            return new AggregateCacheStatsMBean(CachingHiveMetastore::getPartitionCache);
        }

        @Managed
        @Nested
        public AggregateCacheStatsMBean getPartitionFilterStats()
        {
            return new AggregateCacheStatsMBean(CachingHiveMetastore::getPartitionFilterCache);
        }

        @Managed
        @Nested
        public AggregateCacheStatsMBean getTablePrivilegesStats()
        {
            return new AggregateCacheStatsMBean(CachingHiveMetastore::getTablePrivilegesCache);
        }

        @Managed
        @Nested
        public AggregateCacheStatsMBean getRolesStats()
        {
            return new AggregateCacheStatsMBean(CachingHiveMetastore::getRolesCache);
        }

        @Managed
        @Nested
        public AggregateCacheStatsMBean getRoleGrantsStats()
        {
            return new AggregateCacheStatsMBean(CachingHiveMetastore::getRoleGrantsCache);
        }

        @Managed
        @Nested
        public AggregateCacheStatsMBean getGrantedPrincipalsStats()
        {
            return new AggregateCacheStatsMBean(CachingHiveMetastore::getGrantedPrincipalsCache);
        }

        @Managed
        @Nested
        public AggregateCacheStatsMBean getConfigValuesStats()
        {
            return new AggregateCacheStatsMBean(CachingHiveMetastore::getConfigValuesCache);
        }

        public class AggregateCacheStatsMBean
        {
            private final Function<CachingHiveMetastore, Cache<?, ?>> cacheExtractor;

            public AggregateCacheStatsMBean(Function<CachingHiveMetastore, Cache<?, ?>> cacheExtractor)
            {
                this.cacheExtractor = requireNonNull(cacheExtractor, "cacheExtractor is null");
            }

            @Managed
            public long size()
            {
                return cache.asMap().values().stream()
                        .map(cacheExtractor)
                        .mapToLong(Cache::size)
                        .reduce(0, LongMath::saturatedAdd);
            }

            @Managed
            public Double getHitRate()
            {
                return aggregateStats().getHitRate();
            }

            @Managed
            public Double getMissRate()
            {
                return aggregateStats().getMissRate();
            }

            @Managed
            public long getRequestCount()
            {
                return aggregateStats().getRequestCount();
            }

            private CacheStatsAggregator aggregateStats()
            {
                CacheStatsAggregator aggregator = new CacheStatsAggregator();
                for (CachingHiveMetastore metastore : cache.asMap().values()) {
                    aggregator.add(cacheExtractor.apply(metastore).stats());
                }
                return aggregator;
            }
        }
    }

    private static final class CacheStatsAggregator
    {
        private long requestCount;
        private long hitCount;
        private long missCount;

        void add(CacheStats stats)
        {
            requestCount += stats.requestCount();
            hitCount += stats.hitCount();
            missCount += stats.missCount();
        }

        public long getRequestCount()
        {
            return requestCount;
        }

        public long getHitCount()
        {
            return hitCount;
        }

        public long getMissCount()
        {
            return missCount;
        }

        public double getHitRate()
        {
            return (requestCount == 0) ? 1.0 : (double) hitCount / requestCount;
        }

        public double getMissRate()
        {
            return (requestCount == 0) ? 0.0 : (double) missCount / requestCount;
        }
    }
}
