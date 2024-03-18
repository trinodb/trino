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
package io.trino.plugin.varada.storage.write;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.plugin.varada.dispatcher.WarmupElementWriteMetadata;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.services.RowGroupDataService;
import io.trino.plugin.varada.dispatcher.warmup.WarpCacheTask;
import io.trino.plugin.varada.dispatcher.warmup.WorkerTaskExecutorService;
import io.trino.plugin.varada.dispatcher.warmup.warmers.CacheWarmer;
import io.trino.plugin.varada.dispatcher.warmup.warmers.StorageWarmerService;
import io.trino.plugin.varada.dispatcher.warmup.warmers.WarmSinkResult;
import io.trino.plugin.varada.dispatcher.warmup.warmers.WarmingCacheData;
import io.trino.plugin.warp.gen.stats.VaradaStatsWarmingService;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.connector.ConnectorPageSink;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class WarpCachePageSink
        implements ConnectorPageSink
{
    private static final Logger logger = Logger.get(WarpCachePageSink.class);

    private final RowGroupDataService rowGroupDataService;
    private final PlanSignature planSignature;
    private final WarpCacheTask warpCacheTask;
    private final CacheWarmer cacheWarmer;
    private final WorkerTaskExecutorService workerTaskExecutorService;
    private final VaradaStatsWarmingService statsWarmingService;
    private final StorageWarmerService storageWarmerService;
    private final WarpCacheFilesMerger warpCacheFilesMerger;
    private boolean finished;
    private boolean firstTime;

    private boolean appendSuccess;
    private int totalRecords;

    private Optional<WarmupElementWriteMetadata> warmupElementWriteMetadata;

    private WarmingCacheData warmingCacheData;

    public WarpCachePageSink(RowGroupDataService rowGroupDataService,
            PlanSignature planSignature,
            WarpCacheTask warpCacheTask,
            CacheWarmer cacheWarmer,
            WorkerTaskExecutorService workerTaskExecutorService,
            VaradaStatsWarmingService statsWarmingService,
            StorageWarmerService storageWarmerService,
            WarpCacheFilesMerger warpCacheFilesMerger)
    {
        this.rowGroupDataService = requireNonNull(rowGroupDataService);
        this.planSignature = requireNonNull(planSignature);
        this.warpCacheTask = requireNonNull(warpCacheTask);
        this.cacheWarmer = requireNonNull(cacheWarmer);
        this.workerTaskExecutorService = requireNonNull(workerTaskExecutorService);
        this.statsWarmingService = requireNonNull(statsWarmingService);
        this.storageWarmerService = requireNonNull(storageWarmerService);
        this.warpCacheFilesMerger = requireNonNull(warpCacheFilesMerger);
        this.warmupElementWriteMetadata = Optional.empty();
        this.firstTime = true;
        this.appendSuccess = true;
        this.totalRecords = 0;
        this.finished = false;
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        try {
            if (warpCacheTask.getTaskStartedLatch().getCount() > 0) {
                logger.debug("wait warming task to get started");
                warpCacheTask.getTaskStartedLatch().await();
            }
            if (firstTime) {
                statsWarmingService.incwarm_warp_cache_started();
                warmupElementWriteMetadata = cacheWarmer.getWarmupElementWriteMetadata(planSignature.getColumns(),
                        planSignature.getColumnsTypes(),
                        warpCacheTask.getRowGroupKey());
                if (warmupElementWriteMetadata.isPresent()) {
                    warmingCacheData = cacheWarmer.initCacheWarming(warpCacheTask.getRowGroupKey(), warmupElementWriteMetadata.get());
                }
                firstTime = false;
            }
            if (warmupElementWriteMetadata.isEmpty()) {
                //nothing to warm
                return NOT_BLOCKED;
            }
            checkArgument(warmingCacheData != null, "warmingCacheData must be set");
            if (appendSuccess) {
                appendSuccess = warmingCacheData.pageSink().appendPage(page, totalRecords);
                totalRecords += page.getPositionCount();
            }
        }
        catch (Exception e) {
            logFailure(e, warpCacheTask.getRowGroupKey());
            throw new RuntimeException(e);
        }
        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        CompletableFuture<Collection<Slice>> res = CompletableFuture.completedFuture(Collections.emptyList());
        if (finished) {
            logger.info("already finished, should not happened");
            return res;
        }
        RowGroupData tmpRowGroupData = null;
        try {
            if (isEmptyPageSource()) {
                RowGroupData rowGroupData = rowGroupDataService.getOrCreateRowGroupData(warpCacheTask.getRowGroupKey(), Collections.emptyMap());
                warmupElementWriteMetadata = cacheWarmer.getWarmupElementWriteMetadata(
                        planSignature.getColumns(), planSignature.getColumnsTypes(), warpCacheTask.getRowGroupKey());
                if (warmupElementWriteMetadata.isPresent()) {
                    logger.debug("Will warm empty page source. rowGroupKey=%s", warpCacheTask.getRowGroupKey());
                    cacheWarmer.warmEmptyPageSource(rowGroupData, warmupElementWriteMetadata.get());
                    statsWarmingService.incwarm_warp_cache_started();
                    statsWarmingService.incwarm_started();
                }
                else {
                    logger.debug("Nothing to warm for empty page source. rowGroupKey=%s", warpCacheTask.getRowGroupKey());
                }
                return res;
            }
            checkArgument(warmupElementWriteMetadata.isPresent(), "warmupElementWriteMetadata must be present");
            checkArgument(warmingCacheData != null, "warmingCacheData must be set");
            checkArgument(!firstTime, "must open file before closing it");
            logger.debug("finish warm rowGroupKey=%s", warpCacheTask.getRowGroupKey());
            tmpRowGroupData = rowGroupDataService.getOrCreateRowGroupData(warmingCacheData.tmpRowGroupKey(), Collections.emptyMap());

            WarmSinkResult warmSinkResult = storageWarmerService.sinkClose(warmingCacheData.pageSink(),
                    warmupElementWriteMetadata.get(),
                    totalRecords,
                    appendSuccess,
                    warmingCacheData.fileOffset(),
                    warmingCacheData.fileCookie());
            tmpRowGroupData = rowGroupDataService.updateRowGroupData(tmpRowGroupData, warmSinkResult.warmUpElement(), warmSinkResult.offset(), totalRecords);
        }
        catch (Exception e) {
            logger.error(e, "Failed on finish warm. warmupElementWriteMetadata=%s", warmupElementWriteMetadata);
            throw e;
        }
        finally {
            storageWarmerService.releaseLoaderThread(true);
            if (!isEmptyPageSource() && tmpRowGroupData != null) {
                closeStorageLayer(tmpRowGroupData, true);
                List<RowGroupData> tmpRowGroupDataList = List.of(tmpRowGroupData);
                try {
                    warpCacheFilesMerger.mergeTmpFiles(tmpRowGroupDataList, warpCacheTask.getRowGroupKey());
                }
                catch (Exception e) {
                    logger.error(e, "Failed on finish warm. warmupElementWriteMetadata");
                }
                finally {
                    warpCacheFilesMerger.deleteTmpRowGroups(tmpRowGroupDataList);
                }
            }
            if (warmingCacheData != null) {
                cacheWarmer.finishCacheWarming(warmingCacheData);
                warmingCacheData = null;
            }
            statsWarmingService.incwarm_accomplished();
            statsWarmingService.incwarm_warp_cache_accomplished();
            releaseLocks();
        }
        return res;
    }

    @Override
    public void abort()
    {
        if (finished) {
            logger.info("already finished should not happened");
            return;
        }
        if (warmupElementWriteMetadata.isEmpty() || warmingCacheData == null) {
            logger.debug("abort, nothing to do. key=%s", warpCacheTask.getRowGroupKey());
            return;
        }
        try {
            RowGroupKey tmpRowGroupKey = warmingCacheData.tmpRowGroupKey();
            RowGroupData tmpRowGroupData = rowGroupDataService.getOrCreateRowGroupData(tmpRowGroupKey, Collections.emptyMap());
            warmingCacheData.pageSink().abort(false);
            storageWarmerService.fileTruncate(warmingCacheData.fileCookie(), tmpRowGroupData.getNextOffset());
            closeStorageLayer(tmpRowGroupData, false);
        }
        finally {
            storageWarmerService.releaseLoaderThread(true);
            cacheWarmer.finishCacheWarming(warmingCacheData);
            releaseLocks();
            statsWarmingService.incwarm_failed();
            statsWarmingService.incwarm_warp_cache_failed();
        }
    }

    private void closeStorageLayer(RowGroupData rowGroupData, boolean finished)
    {
        storageWarmerService.warmupClose(warmingCacheData.txId());
        boolean runDemote = false;
        if (finished) {
            storageWarmerService.flushRecords(warmingCacheData.fileCookie(), rowGroupData);
            runDemote = true;
        }
        storageWarmerService.fileClose(warmingCacheData.fileCookie(), rowGroupData);
        if (!finished) { //abort
            WarmUpElement failedWarmupElement = warmupElementWriteMetadata.get().warmUpElement();
            rowGroupDataService.markAsFailed(rowGroupData.getRowGroupKey(), List.of(failedWarmupElement), Collections.emptyMap());
        }
        storageWarmerService.releaseRowGroup(rowGroupData, warmingCacheData.locked());
        storageWarmerService.finishWarm(
                warmingCacheData.flowId(),
                true,
                true,
                runDemote);
    }

    private void releaseLocks()
    {
        if (!finished) {
            workerTaskExecutorService.taskFinished(warpCacheTask.getRowGroupKey());
            warpCacheTask.getPageSinkFinishLatch().countDown();
            finished = true;
            statsWarmingService.incwarm_finished();
        }
    }

    private void logFailure(Exception e, RowGroupKey rowGroupKey)
    {
        if (!(e instanceof TrinoException || e instanceof UnsupportedOperationException)) {
            logger.error(e, "warm failed %s", rowGroupKey);
        }
    }

    private boolean isEmptyPageSource()
    {
        return warmingCacheData == null && firstTime;
    }
}
