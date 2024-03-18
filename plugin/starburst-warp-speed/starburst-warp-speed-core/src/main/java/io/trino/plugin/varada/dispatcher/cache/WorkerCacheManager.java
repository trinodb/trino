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
package io.trino.plugin.varada.dispatcher.cache;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.Hashing;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.dispatcher.DispatcherPageSourceFactory;
import io.trino.plugin.varada.dispatcher.WarmupElementWriteMetadata;
import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.dispatcher.services.RowGroupDataService;
import io.trino.plugin.varada.dispatcher.warmup.WarpCacheTask;
import io.trino.plugin.varada.dispatcher.warmup.WorkerTaskExecutorService;
import io.trino.plugin.varada.dispatcher.warmup.warmers.CacheWarmer;
import io.trino.plugin.varada.dispatcher.warmup.warmers.StorageWarmerService;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.storage.engine.ConnectorSync;
import io.trino.plugin.varada.storage.write.WarpCacheFilesMerger;
import io.trino.plugin.varada.storage.write.WarpCachePageSink;
import io.trino.plugin.warp.gen.stats.VaradaStatsWarmingService;
import io.trino.spi.NodeManager;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheManager;
import io.trino.spi.cache.CacheSplitId;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.predicate.TupleDomain;
import io.varada.log.ShapingLogger;

import java.util.Optional;

import static io.trino.plugin.varada.dispatcher.warmup.WorkerWarmingService.WARMING_SERVICE_STAT_GROUP;
import static java.util.Objects.requireNonNull;

@Singleton
public class WorkerCacheManager
        implements CacheManager
{
    private static final Logger logger = Logger.get(WorkerCacheManager.class);
    private final ShapingLogger shapingLogger;
    private final ConnectorSync connectorSync;
    private final DispatcherPageSourceFactory dispatcherPageSourceFactory;
    private final WorkerTaskExecutorService workerTaskExecutorService;
    private final RowGroupDataService rowGroupDataService;
    private final VaradaStatsWarmingService statsWarmingService;

    private final CacheWarmer cacheWarmer;
    private final ObjectMapper objectMapper;
    private final StorageWarmerService storageWarmerService;
    private final WarpCacheFilesMerger warpCacheFilesMerger;

    @Inject
    public WorkerCacheManager(DispatcherPageSourceFactory dispatcherPageSourceFactory,
            WorkerTaskExecutorService workerTaskExecutorService,
            RowGroupDataService rowGroupDataService,
            MetricsManager metricsManager,
            CacheWarmer cacheWarmer,
            ObjectMapperProvider objectMapper,
            StorageWarmerService storageWarmerService,
            WarpCacheFilesMerger warpCacheFilesMerger,
            GlobalConfiguration globalConfiguration,
            ConnectorSync connectorSync)
    {
        this.dispatcherPageSourceFactory = requireNonNull(dispatcherPageSourceFactory);
        this.workerTaskExecutorService = requireNonNull(workerTaskExecutorService);
        this.rowGroupDataService = requireNonNull(rowGroupDataService);
        this.statsWarmingService = metricsManager.registerMetric(VaradaStatsWarmingService.create(WARMING_SERVICE_STAT_GROUP));
        this.cacheWarmer = requireNonNull(cacheWarmer);
        this.objectMapper = objectMapper.get();
        this.storageWarmerService = requireNonNull(storageWarmerService);
        this.warpCacheFilesMerger = requireNonNull(warpCacheFilesMerger);
        this.shapingLogger = ShapingLogger.getInstance(
                logger,
                globalConfiguration.getShapingLoggerThreshold(),
                globalConfiguration.getShapingLoggerDuration(),
                globalConfiguration.getShapingLoggerNumberOfSamples());
        this.connectorSync = requireNonNull(connectorSync);
    }

    @Override
    public SplitCache getSplitCache(PlanSignature signature)
    {
        return new WarpSplitCache(signature);
    }

    @SuppressWarnings("deprecation")
    @Override
    public PreferredAddressProvider getPreferredAddressProvider(PlanSignature signature, NodeManager nodeManager)
    {
        return new WarpPreferredAddressProvider(nodeManager);
    }

    @Override
    public long revokeMemory(long bytesToRevoke)
    {
        return 0;
    }

    private class WarpSplitCache
            implements SplitCache
    {
        private final PlanSignature planSignature;

        public WarpSplitCache(PlanSignature planSignature)
        {
            this.planSignature = planSignature;
        }

        @Override
        public Optional<ConnectorPageSource> loadPages(CacheSplitId splitId, TupleDomain<CacheColumnId> predicate, TupleDomain<CacheColumnId> unenforcedPredicate)
        {
            Optional<ConnectorPageSource> result = Optional.empty();
            try {
                RowGroupKey rowGroupKey = getRowGroupKey(splitId, predicate, unenforcedPredicate);
                result = dispatcherPageSourceFactory.createConnectorPageSource(rowGroupKey, planSignature);
            }
            catch (Exception e) {
                shapingLogger.error(e, "failed to load pages splitId=%s, planSignature=%s", splitId, planSignature);
            }
            return result;
        }

        @Override
        public Optional<ConnectorPageSink> storePages(CacheSplitId splitId, TupleDomain<CacheColumnId> predicate, TupleDomain<CacheColumnId> unenforcedPredicate)
        {
            Optional<ConnectorPageSink> res = Optional.empty();
            boolean txMemoryReserved = storageWarmerService.tryAllocateNativeResourceForWarmup();
            if (!txMemoryReserved) {
                logger.info("nativeResourceForWarmup is not available");
                return res;
            }
            boolean releaseLoaderThread = false;
            try {
                RowGroupKey rowGroupKey = getRowGroupKey(splitId, predicate, unenforcedPredicate);

                Optional<WarmupElementWriteMetadata> elementToWarm = cacheWarmer.getWarmupElementWriteMetadata(
                        planSignature.getColumns(), planSignature.getColumnsTypes(), rowGroupKey);

                if (elementToWarm.isEmpty()) {
                    logger.debug("nothing to warm for %s", planSignature);
                    return res;
                }
                boolean loaderAvailable = storageWarmerService.isLoaderAvailable();
                if (!loaderAvailable) {
                    return Optional.empty();
                }
                releaseLoaderThread = true;
                WarpCacheTask warpCacheTask = new WarpCacheTask(statsWarmingService, rowGroupKey);
                WorkerTaskExecutorService.SubmissionResult submissionResult = workerTaskExecutorService.submitTask(warpCacheTask, false);
                switch (submissionResult) {
                    case SCHEDULED:
                        res = Optional.of(new WarpCachePageSink(rowGroupDataService,
                                planSignature,
                                warpCacheTask,
                                cacheWarmer,
                                workerTaskExecutorService,
                                statsWarmingService,
                                storageWarmerService,
                                warpCacheFilesMerger));
                        releaseLoaderThread = false;
                        break;
                    case CONFLICT:
                        statsWarmingService.incwarm_skipped_due_key_conflict();
                        releaseLocks(warpCacheTask);
                        break;
                    case REJECTED:
                        releaseLocks(warpCacheTask);
                        break;
                }
            }
            catch (Exception e) {
                shapingLogger.error(e, "failed to store data from WarpCacheManager splitId=%s, planSignature=%s", splitId, planSignature);
                res = Optional.empty();
            }
            finally {
                if (releaseLoaderThread) {
                    storageWarmerService.releaseLoaderThread(true);
                }
            }
            return res;
        }

        @Override
        public void close()
        {
        }

        private RowGroupKey getRowGroupKey(
                CacheSplitId splitId,
                TupleDomain<CacheColumnId> predicate,
                TupleDomain<CacheColumnId> unenforcedPredicate)
                throws JsonProcessingException
        {
            String key = splitId.toString() + "_" +
                    planSignature.getKey().toString() + "_" +
                    objectMapper.writeValueAsString(predicate + "_" +
                            objectMapper.writeValueAsString(unenforcedPredicate));
            if (planSignature.getGroupByColumns().isPresent()) {
                key = key + "_" + planSignature.getGroupByColumns().get();
            }
            String uniqueKey = Hashing.sha256().hashUnencodedChars(key).toString();
            return new RowGroupKey("WarpCache",
                    uniqueKey,
                    "",
                    0,
                    0,
                    0,
                    "",
                    connectorSync.getCatalogName());
        }
    }

    private void releaseLocks(WarpCacheTask warpCacheTask)
    {
        warpCacheTask.getTaskStartedLatch().countDown();
        warpCacheTask.getPageSinkFinishLatch().countDown();
        storageWarmerService.releaseTx(true);
    }
}
