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
package io.trino.plugin.varada.dispatcher.warmup.warmers;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.dispatcher.WarmupElementWriteMetadata;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.services.RowGroupDataService;
import io.trino.plugin.varada.dispatcher.warmup.demoter.AcquireWarmupStatus;
import io.trino.plugin.varada.dispatcher.warmup.demoter.WarmupDemoterService;
import io.trino.plugin.varada.juffer.StorageEngineTxService;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.storage.engine.ConnectorSync;
import io.trino.plugin.varada.storage.engine.StorageEngine;
import io.trino.plugin.varada.storage.flows.FlowType;
import io.trino.plugin.varada.storage.flows.FlowsSequencer;
import io.trino.plugin.varada.storage.write.PageSink;
import io.trino.plugin.warp.gen.stats.VaradaStatsWarmingService;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static io.trino.plugin.varada.dispatcher.warmup.WorkerWarmingService.WARMING_SERVICE_STAT_GROUP;
import static java.util.Objects.requireNonNull;

@Singleton
public class StorageWarmerService
{
    private static final Logger logger = Logger.get(StorageWarmerService.class);
    public static final long INVALID_FILE_COOKIE = -1;
    public static final int INVALID_TX_ID = -1;
    public static final int INVALID_FLOW_ID = -1;

    private final RowGroupDataService rowGroupDataService;
    private final StorageEngine storageEngine;
    private final GlobalConfiguration globalConfiguration;
    private final ConnectorSync connectorSync;
    private final WarmupDemoterService warmupDemoterService;
    private final StorageEngineTxService storageEngineTxService;
    private final FlowsSequencer flowsSequencer;
    private final VaradaStatsWarmingService statsWarmingService;

    @Inject
    public StorageWarmerService(RowGroupDataService rowGroupDataService,
            StorageEngine storageEngine,
            GlobalConfiguration globalConfiguration,
            ConnectorSync connectorSync,
            WarmupDemoterService warmupDemoterService,
            StorageEngineTxService storageEngineTxService,
            FlowsSequencer flowsSequencer,
            MetricsManager metricsManager)
    {
        this.rowGroupDataService = requireNonNull(rowGroupDataService);
        this.storageEngine = requireNonNull(storageEngine);
        this.globalConfiguration = requireNonNull(globalConfiguration);
        this.connectorSync = requireNonNull(connectorSync);
        this.warmupDemoterService = requireNonNull(warmupDemoterService);
        this.storageEngineTxService = requireNonNull(storageEngineTxService);
        this.flowsSequencer = requireNonNull(flowsSequencer);
        this.statsWarmingService = metricsManager.registerMetric(VaradaStatsWarmingService.create(WARMING_SERVICE_STAT_GROUP));
    }

    public void createFile(RowGroupKey rowGroupKey)
            throws IOException
    {
        String rowGroupFilePath = rowGroupKey.stringFileNameRepresentation(globalConfiguration.getLocalStorePath());
        File file = new File(rowGroupFilePath);
        if (!file.exists()) {
            FileUtils.createParentDirectories(file);
        }
    }

    public long fileOpen(RowGroupKey rowGroupKey)
            throws IOException
    {
        String rowGroupFilePath = rowGroupKey.stringFileNameRepresentation(globalConfiguration.getLocalStorePath());
        // fileCookie was initialized to -1. In case fileOpen throws an exception we will not close it in the finally clause
        return storageEngine.fileOpen(rowGroupFilePath);
    }

    // in case we already have an open tx we close it and open a new one
    public int warmupOpen(int txId)
    {
        warmupClose(txId);
        txId = storageEngine.warmupOpen(connectorSync.getCatalogSequence());
        if (txId == INVALID_TX_ID) {
            throw new RuntimeException("failed to allocate tx for write");
        }
        return txId;
    }

    public void warmupClose(int txId)
    {
        if (txId != INVALID_TX_ID) {
            storageEngine.warmupClose(txId);
        }
    }

    public void flushRecords(long fileCookie, RowGroupData rowGroupData)
    {
        if (fileCookie != INVALID_FILE_COOKIE) {
            if (rowGroupData.getValidWarmUpElements().isEmpty()) {
                rowGroupDataService.deleteData(rowGroupData, false);
                logger.debug("all we failed for row group=%s", rowGroupData.getRowGroupKey());
            }
            else {
                rowGroupDataService.flush(rowGroupData.getRowGroupKey());
            }
        }
    }

    public WarmSinkResult sinkClose(PageSink pageSink,
            WarmupElementWriteMetadata currWarmUpElement,
            int rowCount,
            boolean isValidWE,
            int currentOffset,
            long fileCookie)
    {
        WarmUpElement updatedWarmupElement;
        int newOffset = currentOffset;
        if (rowCount > 0) {
            // close might fail as well, we need to check the success elements after returning
            WarmSinkResult warmSinkResult = pageSink.close(rowCount);
            updatedWarmupElement = warmSinkResult.warmUpElement();
            isValidWE &= updatedWarmupElement.isValid();
            if (isValidWE) {
                newOffset = warmSinkResult.offset();
            }
            else {
                fileTruncate(fileCookie, currentOffset);
            }
        }
        else {
            //case we never init sink it means that total rowCount is 0, and RowGroup will be mark as EmptyPageSource
            updatedWarmupElement = currWarmUpElement.warmUpElement();
        }
        return new WarmSinkResult(updatedWarmupElement, newOffset);
    }

    public void fileTruncate(long fileCookie, int currentOffset)
    {
        storageEngine.fileTruncate(fileCookie, currentOffset);
    }

    public void fileClose(long fileCookie, RowGroupData rowGroupData)
    {
        if (fileCookie != INVALID_FILE_COOKIE) {
            try {
                storageEngine.fileClose(fileCookie);
            }
            catch (Exception e) {
                rowGroupDataService.deleteData(rowGroupData, true);
                String rowGroupFilePath = rowGroupData.getRowGroupKey().stringFileNameRepresentation(globalConfiguration.getLocalStorePath());
                logger.error(e, String.format("failed to close file %s", rowGroupFilePath));
            }
        }
        else { // we failed in opening the file
            rowGroupDataService.deleteData(rowGroupData, true);
            String rowGroupFilePath = rowGroupData.getRowGroupKey().stringFileNameRepresentation(globalConfiguration.getLocalStorePath());
            logger.error(String.format("failed to open file %s", rowGroupFilePath));
        }
    }

    public void releaseRowGroup(RowGroupData rowGroupData, boolean locked)
    {
        if (rowGroupData != null && locked) {
            rowGroupData.getLock().writeUnlock();
        }
    }

    public boolean lockRowGroup(RowGroupData rowGroupData)
            throws InterruptedException
    {
        boolean locked = false;
        if (rowGroupData != null) {
            rowGroupData.getLock().writeLock();
            locked = true;
        }
        return locked;
    }

    public void finishWarm(boolean releaseTx)
    {
        finishWarm(INVALID_FLOW_ID, releaseTx, false, false);
    }

    public void releaseLoaderThread(boolean skipWait)
    {
        storageEngineTxService.doneWarming(skipWait);
    }

    public void finishWarm(long flowId,
            boolean releaseTx,
            boolean force,
            boolean runDemote)
    {
        releaseTx(releaseTx);
        if (flowId != INVALID_FILE_COOKIE) {
            flowsSequencer.flowFinished(FlowType.WARMUP, flowId, force);
        }
        if (runDemote) {
            try {
                warmupDemoterService.tryDemoteStart();
            }
            catch (Throwable ignored) {
                logger.warn("demoter failed");
            } //do nothing
        }
    }

    public void releaseTx(boolean releaseTx)
    {
        if (releaseTx) {
            warmupDemoterService.releaseTx();
        }
    }

    public void waitForLoaders()
    {
        CompletableFuture<Boolean> future = storageEngineTxService.tryToWarm();
        try {
            future.get();
        }
        catch (Exception e) {
            logger.warn(e, "failed to release waiting warm");
        }
    }

    public boolean isLoaderAvailable()
    {
        return storageEngineTxService.isLoaderAvailable();
    }

    public void tryRunningWarmFlow(long flowId, RowGroupKey rowGroupKey)
            throws ExecutionException, InterruptedException
    {
        CompletableFuture<Boolean> flowFuture = flowsSequencer.tryRunningFlow(
                FlowType.WARMUP,
                flowId,
                Optional.of(rowGroupKey.toString()));
        flowFuture.get();
    }

    public boolean tryAllocateNativeResourceForWarmup()
    {
        AcquireWarmupStatus acquireWarmupStatus = warmupDemoterService.tryAllocateNativeResourceForWarmup();
        boolean result = true;
        if (!acquireWarmupStatus.equals(AcquireWarmupStatus.SUCCESS)) {
            if (acquireWarmupStatus.equals(AcquireWarmupStatus.EXCEEDED_LOADERS)) {
                statsWarmingService.incwarm_skipped_due_loaders_exceeded();
            }
            else { //AcquireWarmupStatus.REACHED_THRESHOLD
                statsWarmingService.incwarm_skipped_due_reaching_threshold();
            }
            result = false;
        }
        return result;
    }
}
