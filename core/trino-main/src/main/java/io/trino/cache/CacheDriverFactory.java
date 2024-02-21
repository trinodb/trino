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
import com.google.common.base.Ticker;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.cache.CommonPlanAdaptation.PlanSignatureWithPredicate;
import io.trino.execution.ScheduledSplit;
import io.trino.metadata.TableHandle;
import io.trino.operator.Driver;
import io.trino.operator.DriverContext;
import io.trino.operator.DriverFactory;
import io.trino.plugin.base.cache.CacheUtils;
import io.trino.plugin.base.metrics.TDigestHistogram;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheManager.SplitCache;
import io.trino.spi.cache.CacheSplitId;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.predicate.DiscreteValues;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Ranges;
import io.trino.spi.predicate.TupleDomain;
import io.trino.split.PageSourceProvider;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.cache.CacheCommonSubqueries.LOAD_PAGES_ALTERNATIVE;
import static io.trino.cache.CacheCommonSubqueries.ORIGINAL_PLAN_ALTERNATIVE;
import static io.trino.cache.CacheCommonSubqueries.STORE_PAGES_ALTERNATIVE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class CacheDriverFactory
{
    static final int MAX_UNENFORCED_PREDICATE_VALUE_COUNT = 1_000_000;
    private static final Logger LOG = Logger.get(CacheDriverFactory.class);

    public static final float THRASHING_CACHE_THRESHOLD = 0.7f;
    public static final int MIN_PROCESSED_SPLITS = 16;

    private final Session session;
    private final PageSourceProvider pageSourceProvider;
    private final SplitCache splitCache;
    private final JsonCodec<TupleDomain> tupleDomainCodec;
    private final TableHandle originalTableHandle;
    private final Set<CacheColumnId> projectedColumns;
    private final TupleDomain<CacheColumnId> enforcedPredicate;
    private final BiMap<ColumnHandle, CacheColumnId> commonColumnHandles;
    private final Supplier<StaticDynamicFilter> commonDynamicFilterSupplier;
    private final Supplier<StaticDynamicFilter> originalDynamicFilterSupplier;
    private final List<DriverFactory> alternatives;
    private final CacheMetrics cacheMetrics = new CacheMetrics();
    private final CacheStats cacheStats;
    private final Ticker ticker = Ticker.systemTicker();

    public CacheDriverFactory(
            Session session,
            PageSourceProvider pageSourceProvider,
            CacheManagerRegistry cacheManagerRegistry,
            JsonCodec<TupleDomain> tupleDomainCodec,
            TableHandle originalTableHandle,
            PlanSignatureWithPredicate planSignature,
            Map<CacheColumnId, ColumnHandle> commonColumnHandles,
            Supplier<StaticDynamicFilter> commonDynamicFilterSupplier,
            Supplier<StaticDynamicFilter> originalDynamicFilterSupplier,
            List<DriverFactory> alternatives,
            CacheStats cacheStats)
    {
        requireNonNull(planSignature, "planSignature is null");
        this.session = requireNonNull(session, "session is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.splitCache = requireNonNull(cacheManagerRegistry, "cacheManagerRegistry is null").getCacheManager().getSplitCache(planSignature.signature());
        this.tupleDomainCodec = requireNonNull(tupleDomainCodec, "tupleDomainCodec is null");
        this.originalTableHandle = requireNonNull(originalTableHandle, "originalTableHandle is null");
        this.projectedColumns = ImmutableSet.copyOf(planSignature.signature().getColumns());
        this.enforcedPredicate = planSignature.predicate();
        this.commonColumnHandles = ImmutableBiMap.copyOf(requireNonNull(commonColumnHandles, "commonColumnHandles is null")).inverse();
        this.commonDynamicFilterSupplier = requireNonNull(commonDynamicFilterSupplier, "commonDynamicFilterSupplier is null");
        this.originalDynamicFilterSupplier = requireNonNull(originalDynamicFilterSupplier, "originalDynamicFilterSupplier is null");
        this.alternatives = requireNonNull(alternatives, "alternatives is null");
        this.cacheStats = requireNonNull(cacheStats, "cacheStats is null");
    }

    public Driver createDriver(DriverContext driverContext, ScheduledSplit split, Optional<CacheSplitId> cacheSplitIdOptional)
    {
        DriverFactoryWithCacheContext driverFactory;
        long lookupStartNanos = ticker.read();
        try {
            driverFactory = chooseDriverFactory(split, cacheSplitIdOptional);
        }
        catch (Throwable t) {
            LOG.error(t, "SUBQUERY CACHE: create driver exception");
            throw t;
        }
        long lookupDurationNanos = ticker.read() - lookupStartNanos;
        cacheStats.getCacheLookupTime().addNanos(lookupDurationNanos);

        driverFactory.context()
                .map(context -> context.withMetrics(new Metrics(ImmutableMap.of(
                        "Cache lookup time (ms)", TDigestHistogram.fromValue(new Duration(lookupDurationNanos, NANOSECONDS).convertTo(MILLISECONDS).getValue())))))
                .ifPresent(driverContext::setCacheDriverContext);
        return driverFactory.factory().createDriver(driverContext);
    }

    private DriverFactoryWithCacheContext chooseDriverFactory(ScheduledSplit split, Optional<CacheSplitId> cacheSplitIdOptional)
    {
        if (cacheSplitIdOptional.isEmpty()) {
            // no split id, fallback to original plan
            cacheStats.recordMissingSplitId();
            return new DriverFactoryWithCacheContext(alternatives.get(ORIGINAL_PLAN_ALTERNATIVE), Optional.empty());
        }
        if (split.getSplit().getFailoverHappened()) {
            // failed to schedule split on the preferred node, fallback to original plan
            cacheStats.recordSplitFailoverHappened();
            return new DriverFactoryWithCacheContext(alternatives.get(ORIGINAL_PLAN_ALTERNATIVE), Optional.empty());
        }
        CacheSplitId splitId = cacheSplitIdOptional.get();

        StaticDynamicFilter originalDynamicFilter = originalDynamicFilterSupplier.get();
        StaticDynamicFilter commonDynamicFilter = commonDynamicFilterSupplier.get();
        StaticDynamicFilter dynamicFilter = resolveDynamicFilter(originalDynamicFilter, commonDynamicFilter);

        TupleDomain<CacheColumnId> enforcedPredicate = pruneEnforcedPredicate(split);
        TupleDomain<CacheColumnId> unenforcedPredicate = pageSourceProvider.getUnenforcedPredicate(
                        session,
                        split.getSplit(),
                        originalTableHandle,
                        dynamicFilter.getCurrentPredicate())
                .transformKeys(handle -> requireNonNull(commonColumnHandles.get(handle)));

        // skip caching of completely filtered out splits
        if (enforcedPredicate.isNone() || unenforcedPredicate.isNone()) {
            return new DriverFactoryWithCacheContext(alternatives.get(ORIGINAL_PLAN_ALTERNATIVE), Optional.empty());
        }

        // skip caching if unenforced predicate becomes too big,
        // because large predicates are not likely to be reused in other subqueries
        if (getTupleDomainValueCount(unenforcedPredicate) > MAX_UNENFORCED_PREDICATE_VALUE_COUNT) {
            cacheStats.recordPredicateTooBig();
            return new DriverFactoryWithCacheContext(alternatives.get(ORIGINAL_PLAN_ALTERNATIVE), Optional.empty());
        }

        ProjectPredicate projectedEnforcedPredicate = projectPredicate(enforcedPredicate);
        ProjectPredicate projectedUnenforcedPredicate = projectPredicate(unenforcedPredicate);
        CacheSplitId splitIdWithPredicates = appendRemainingPredicates(splitId, projectedEnforcedPredicate, projectedUnenforcedPredicate);

        // load data from cache
        Optional<ConnectorPageSource> pageSource = splitCache.loadPages(splitIdWithPredicates, projectedEnforcedPredicate.predicate(), projectedUnenforcedPredicate.predicate());
        if (pageSource.isPresent()) {
            cacheStats.recordCacheHit();
            return new DriverFactoryWithCacheContext(
                    alternatives.get(LOAD_PAGES_ALTERNATIVE),
                    Optional.of(new CacheDriverContext(pageSource, Optional.empty(), dynamicFilter, cacheMetrics, cacheStats, Metrics.EMPTY)));
        }
        else {
            cacheStats.recordCacheMiss();
        }

        int processedSplitCount = cacheMetrics.getSplitNotCachedCount() + cacheMetrics.getSplitCachedCount();
        float cachingRatio = processedSplitCount > MIN_PROCESSED_SPLITS ? cacheMetrics.getSplitCachedCount() / (float) processedSplitCount : 1.0f;
        // try storing results instead
        // if splits are too large to be cached then do not try caching data as it adds extra computational cost
        if (cachingRatio > THRASHING_CACHE_THRESHOLD) {
            Optional<ConnectorPageSink> pageSink = splitCache.storePages(splitIdWithPredicates, projectedEnforcedPredicate.predicate(), projectedUnenforcedPredicate.predicate());
            if (pageSink.isPresent()) {
                return new DriverFactoryWithCacheContext(
                        alternatives.get(STORE_PAGES_ALTERNATIVE),
                        Optional.of(new CacheDriverContext(Optional.empty(), pageSink, dynamicFilter, cacheMetrics, cacheStats, Metrics.EMPTY)));
            }
            else {
                cacheStats.recordSplitRejected();
            }
        }
        else {
            cacheStats.recordSplitsTooBig();
        }

        // fallback to original subplan
        return new DriverFactoryWithCacheContext(alternatives.get(ORIGINAL_PLAN_ALTERNATIVE), Optional.empty());
    }

    private record DriverFactoryWithCacheContext(DriverFactory factory, Optional<CacheDriverContext> context) {}

    private TupleDomain<CacheColumnId> pruneEnforcedPredicate(ScheduledSplit split)
    {
        return TupleDomain.intersect(ImmutableList.of(
                // prune scan domains of enforced predicate
                pageSourceProvider.prunePredicate(
                                session,
                                split.getSplit(),
                                originalTableHandle,
                                enforcedPredicate
                                        .filter((columnId, domain) -> commonColumnHandles.containsValue(columnId))
                                        .transformKeys(columnId -> commonColumnHandles.inverse().get(columnId)))
                        .transformKeys(commonColumnHandles::get),
                enforcedPredicate.filter((columnId, domain) -> !commonColumnHandles.containsValue(columnId))));
    }

    private ProjectPredicate projectPredicate(TupleDomain<CacheColumnId> predicate)
    {
        return new ProjectPredicate(
                predicate.filter((columnId, domain) -> projectedColumns.contains(columnId)),
                Optional.of(predicate.filter((columnId, domain) -> !projectedColumns.contains(columnId)))
                        .filter(domain -> !domain.isAll())
                        .map(CacheUtils::normalizeTupleDomain)
                        .map(tupleDomainCodec::toJson));
    }

    private record ProjectPredicate(TupleDomain<CacheColumnId> predicate, Optional<String> remainingPredicate) {}

    private static CacheSplitId appendRemainingPredicates(CacheSplitId splitId, ProjectPredicate enforcedPredicate, ProjectPredicate unenforcedPredicate)
    {
        return appendRemainingPredicates(splitId, enforcedPredicate.remainingPredicate(), unenforcedPredicate.remainingPredicate());
    }

    @VisibleForTesting
    static CacheSplitId appendRemainingPredicates(CacheSplitId splitId, Optional<String> remainingEnforcedPredicate, Optional<String> remainingUnenforcedPredicate)
    {
        if (remainingEnforcedPredicate.isEmpty() && remainingUnenforcedPredicate.isEmpty()) {
            return splitId;
        }
        return new CacheSplitId(toStringHelper("SplitId")
                .add("splitId", splitId)
                .add("enforcedPredicate", remainingEnforcedPredicate.orElse("all"))
                .add("unenforcedPredicate", remainingUnenforcedPredicate.orElse("all"))
                .toString());
    }

    public void closeSplitCache()
    {
        try {
            splitCache.close();
        }
        catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
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

    @VisibleForTesting
    public CacheMetrics getCacheMetrics()
    {
        return cacheMetrics;
    }

    private static int getTupleDomainValueCount(TupleDomain<?> tupleDomain)
    {
        return tupleDomain.getDomains()
                .map(domains -> domains.values().stream()
                        .mapToInt(CacheDriverFactory::getDomainValueCount)
                        .sum())
                .orElse(0);
    }

    private static int getDomainValueCount(Domain domain)
    {
        return domain.getValues().getValuesProcessor().transform(
                Ranges::getRangeCount,
                DiscreteValues::getValuesCount,
                ignored -> 0);
    }
}
