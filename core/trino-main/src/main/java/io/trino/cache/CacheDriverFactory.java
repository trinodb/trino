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
package io.trino.cache;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableBiMap;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.execution.ScheduledSplit;
import io.trino.metadata.TableHandle;
import io.trino.operator.Driver;
import io.trino.operator.DriverContext;
import io.trino.operator.DriverFactory;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheManager.SplitCache;
import io.trino.spi.cache.CacheSplitId;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.predicate.TupleDomain;
import io.trino.split.PageSourceProvider;
import jakarta.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import static io.trino.cache.CacheCommonSubqueries.LOAD_PAGES_ALTERNATIVE;
import static io.trino.cache.CacheCommonSubqueries.ORIGINAL_PLAN_ALTERNATIVE;
import static io.trino.cache.CacheCommonSubqueries.STORE_PAGES_ALTERNATIVE;
import static io.trino.plugin.base.cache.CacheUtils.normalizeTupleDomain;
import static java.util.Objects.requireNonNull;

public class CacheDriverFactory
{
    private static final Logger LOG = Logger.get(CacheDriverFactory.class);

    public static final float THRASHING_CACHE_THRESHOLD = 0.7f;
    public static final int MIN_PROCESSED_SPLITS = 16;

    private final Session session;
    private final PageSourceProvider pageSourceProvider;
    private final CacheManagerRegistry cacheManagerRegistry;
    private final TableHandle originalTableHandle;
    private final PlanSignature basePlanSignature;
    private final Map<ColumnHandle, CacheColumnId> dynamicFilterColumnMapping;
    private final Supplier<StaticDynamicFilter> commonDynamicFilterSupplier;
    private final Supplier<StaticDynamicFilter> originalDynamicFilterSupplier;
    private final List<DriverFactory> alternatives;
    private final CacheMetrics cacheMetrics = new CacheMetrics();
    private final CacheStats cacheStats;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    @GuardedBy("lock")
    @Nullable
    private SplitCache splitCache;
    @GuardedBy("lock")
    @Nullable
    @VisibleForTesting
    private PlanSignature cachePlanSignature;
    @GuardedBy("lock")
    private boolean cachePlanSignatureComplete;

    public CacheDriverFactory(
            Session session,
            PageSourceProvider pageSourceProvider,
            CacheManagerRegistry cacheManagerRegistry,
            TableHandle originalTableHandle,
            PlanSignature basePlanSignature,
            Map<CacheColumnId, ColumnHandle> dynamicFilterColumnMapping,
            Supplier<StaticDynamicFilter> commonDynamicFilterSupplier,
            Supplier<StaticDynamicFilter> originalDynamicFilterSupplier,
            List<DriverFactory> alternatives,
            CacheStats cacheStats)
    {
        this.session = requireNonNull(session, "session is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.cacheManagerRegistry = requireNonNull(cacheManagerRegistry, "cacheManagerRegistry is null");
        this.originalTableHandle = requireNonNull(originalTableHandle, "originalTableHandle is null");
        this.basePlanSignature = requireNonNull(basePlanSignature, "basePlanSignature is null");
        this.dynamicFilterColumnMapping = ImmutableBiMap.copyOf(requireNonNull(dynamicFilterColumnMapping, "dynamicFilterColumnMapping is null")).inverse();
        this.commonDynamicFilterSupplier = requireNonNull(commonDynamicFilterSupplier, "commonDynamicFilterSupplier is null");
        this.originalDynamicFilterSupplier = requireNonNull(originalDynamicFilterSupplier, "originalDynamicFilterSupplier is null");
        this.alternatives = requireNonNull(alternatives, "alternatives is null");
        this.cacheStats = requireNonNull(cacheStats, "cacheStats is null");
    }

    public Driver createDriver(DriverContext driverContext, ScheduledSplit split, Optional<CacheSplitId> cacheSplitIdOptional)
    {
        try {
            return createDriverInternal(driverContext, split, cacheSplitIdOptional);
        }
        catch (Throwable t) {
            LOG.error(t, "SUBQUERY CACHE: create driver exception");
            throw t;
        }
    }

    private Driver createDriverInternal(DriverContext driverContext, ScheduledSplit split, Optional<CacheSplitId> cacheSplitIdOptional)
    {
        if (cacheSplitIdOptional.isEmpty()) {
            // no split id, fallback to original plan
            return alternatives.get(ORIGINAL_PLAN_ALTERNATIVE).createDriver(driverContext);
        }
        CacheSplitId splitId = cacheSplitIdOptional.get();

        // simplify dynamic filter predicate to improve cache hits
        StaticDynamicFilter originalDynamicFilter = originalDynamicFilterSupplier.get();
        StaticDynamicFilter commonDynamicFilter = commonDynamicFilterSupplier.get();
        StaticDynamicFilter dynamicFilter = resolveDynamicFilter(originalDynamicFilter, commonDynamicFilter);
        TupleDomain<ColumnHandle> dynamicPredicate = pageSourceProvider.simplifyPredicate(
                        session,
                        split.getSplit(),
                        originalTableHandle,
                        dynamicFilter.getCurrentPredicate())
                // filter out DF columns which are not mapped to signature output columns
                .filter((column, domain) -> dynamicFilterColumnMapping.containsKey(column));

        if (dynamicPredicate.isNone()) {
            // skip caching of completely filtered out splits
            return alternatives.get(ORIGINAL_PLAN_ALTERNATIVE).createDriver(driverContext);
        }

        // enhance plan signature with current dynamic filter
        PlanSignature planSignature = basePlanSignature
                .withDynamicPredicate(normalizeTupleDomain(dynamicPredicate.transformKeys(dynamicFilterColumnMapping::get)));
        boolean planSignatureComplete = originalDynamicFilter.isComplete() && commonDynamicFilter.isComplete();

        // load data from cache
        Optional<ConnectorPageSource> pageSource = loadPages(splitId, planSignature, planSignatureComplete);
        if (pageSource.isPresent()) {
            cacheStats.recordCacheHit(1);
            driverContext.setCacheDriverContext(new CacheDriverContext(pageSource, Optional.empty(), dynamicFilter, cacheMetrics, cacheStats));
            return alternatives.get(LOAD_PAGES_ALTERNATIVE).createDriver(driverContext);
        }
        else {
            cacheStats.recordCacheMiss(1);
        }

        int processedSplitCount = cacheMetrics.getSplitNotCachedCount() + cacheMetrics.getSplitCachedCount();
        float cachingRatio = processedSplitCount > MIN_PROCESSED_SPLITS ? cacheMetrics.getSplitCachedCount() / (float) processedSplitCount : 1.0f;
        // try storing results instead
        // if splits are too large to be cached then do not try caching data as it adds extra computational cost
        if (cachingRatio > THRASHING_CACHE_THRESHOLD) {
            Optional<ConnectorPageSink> pageSink = storePages(splitId, planSignature, planSignatureComplete);
            if (pageSink.isPresent()) {
                driverContext.setCacheDriverContext(new CacheDriverContext(Optional.empty(), pageSink, dynamicFilter, cacheMetrics, cacheStats));
                return alternatives.get(STORE_PAGES_ALTERNATIVE).createDriver(driverContext);
            }
        }

        // fallback to original subplan
        return alternatives.get(ORIGINAL_PLAN_ALTERNATIVE).createDriver(driverContext);
    }

    public void closeSplitCache()
    {
        runWithLock(lock.writeLock(), () -> {
            try {
                if (splitCache != null) {
                    splitCache.close();
                    splitCache = null;
                    cachePlanSignature = null;
                    cachePlanSignatureComplete = false;
                }
            }
            catch (IOException exception) {
                throw new UncheckedIOException(exception);
            }
        });
    }

    @Nullable
    public PlanSignature getCachePlanSignature()
    {
        return cachePlanSignature;
    }

    private StaticDynamicFilter resolveDynamicFilter(StaticDynamicFilter originalDynamicFilter, StaticDynamicFilter commonDynamicFilter)
    {
        TupleDomain<ColumnHandle> originalPredicate = originalDynamicFilter.getCurrentPredicate();
        TupleDomain<ColumnHandle> commonPredicate = commonDynamicFilter.getCurrentPredicate();

        if (commonPredicate.isNone() || originalPredicate.isNone()) {
            // prefer original DF when common DF is absent
            return originalDynamicFilter;
        }

        if (originalPredicate.getDomains().get().size() > commonPredicate.getDomains().get().size()) {
            // prefer original DF when it contains more domains
            return originalDynamicFilter;
        }

        return commonDynamicFilter;
    }

    private Optional<ConnectorPageSource> loadPages(
            CacheSplitId splitId,
            PlanSignature planSignature,
            boolean planSignatureComplete)
    {
        return runWithLock(
                () -> needsSplitCacheUpdate(planSignature, planSignatureComplete),
                () -> updateSplitCache(planSignature, planSignatureComplete),
                () -> splitCache.loadPages(splitId));
    }

    private Optional<ConnectorPageSink> storePages(
            CacheSplitId splitId,
            PlanSignature planSignature,
            boolean planSignatureComplete)
    {
        return runWithLock(
                () -> needsSplitCacheUpdate(planSignature, planSignatureComplete),
                () -> updateSplitCache(planSignature, planSignatureComplete),
                () -> splitCache.storePages(splitId));
    }

    @VisibleForTesting
    void updateSplitCache(PlanSignature planSignature, boolean planSignatureComplete)
    {
        if (!needsSplitCacheUpdate(planSignature, planSignatureComplete)) {
            return;
        }

        closeSplitCache();
        splitCache = cacheManagerRegistry.getCacheManager().getSplitCache(planSignature);
        cachePlanSignature = planSignature;
        cachePlanSignatureComplete = planSignatureComplete;
    }

    private boolean needsSplitCacheUpdate(PlanSignature planSignature, boolean planSignatureComplete)
    {
        if (splitCache != null) {
            if (cachePlanSignatureComplete && planSignatureComplete) {
                // plan signature cannot be enhanced further, no need to compare signatures or re-create SplitCache
                return false;
            }

            // do not re-create SplitCache if dynamic predicate hasn't changed
            return !cachePlanSignature.getDynamicPredicate().equals(planSignature.getDynamicPredicate());
        }

        return true;
    }

    private <T> T runWithLock(
            BooleanSupplier updateCondition,
            Runnable updateAction,
            Supplier<T> supplier)
    {
        return runWithLock(lock.readLock(), () -> {
            if (updateCondition.getAsBoolean()) {
                return Optional.<T>empty();
            }

            return Optional.of(supplier.get());
        }).orElseGet(() -> runWithLock(lock.writeLock(), () -> {
            updateAction.run();
            return supplier.get();
        }));
    }

    private static void runWithLock(Lock lock, Runnable runnable)
    {
        lock.lock();
        try {
            runnable.run();
        }
        finally {
            lock.unlock();
        }
    }

    private static <T> T runWithLock(Lock lock, Supplier<T> supplier)
    {
        lock.lock();
        try {
            return supplier.get();
        }
        finally {
            lock.unlock();
        }
    }

    public CacheMetrics getCacheMetrics()
    {
        return cacheMetrics;
    }
}
