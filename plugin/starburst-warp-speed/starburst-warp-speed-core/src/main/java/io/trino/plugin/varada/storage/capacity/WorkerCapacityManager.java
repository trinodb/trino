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
package io.trino.plugin.varada.storage.capacity;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.varada.VaradaErrorCode;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.configuration.WarmupDemoterConfiguration;
import io.trino.plugin.varada.di.VaradaInitializedServiceRegistry;
import io.trino.plugin.varada.dispatcher.warmup.demoter.WarmupDemoterService;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.varada.storage.engine.nativeimpl.NativeStorageStateHandler;
import io.trino.plugin.varada.util.VaradaInitializedServiceMarker;
import io.trino.plugin.warp.gen.stats.VaradaStatsWarmupDemoter;
import io.trino.spi.TrinoException;
import io.varada.tools.util.PathUtils;
import io.varada.tools.util.StopWatch;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

@Singleton
public class WorkerCapacityManager
        implements VaradaInitializedServiceMarker
{
    private static final Logger logger = Logger.get(WorkerCapacityManager.class);

    private final GlobalConfiguration globalConfiguration;
    private final WarmupDemoterConfiguration warmupDemoterConfiguration;
    private final StorageEngineConstants storageEngineConstants;
    private final VaradaStatsWarmupDemoter statsWarmupDemoter;
    private final NativeStorageStateHandler nativeStorageStateHandler;
    private final AtomicBoolean workerInitialized = new AtomicBoolean();
    private final AtomicInteger executingTxCount = new AtomicInteger();

    private File warpDir;
    private long totalCapacity;
    private long reservationUsageForSingleTx;

    @Inject
    WorkerCapacityManager(GlobalConfiguration globalConfiguration,
            WarmupDemoterConfiguration warmupDemoterConfiguration,
            StorageEngineConstants storageEngineConstants,
            NativeStorageStateHandler nativeStorageStateHandler,
            VaradaInitializedServiceRegistry varadaInitializedServiceRegistry,
            MetricsManager metricsManager)
    {
        this.globalConfiguration = requireNonNull(globalConfiguration);
        this.warmupDemoterConfiguration = requireNonNull(warmupDemoterConfiguration);
        this.storageEngineConstants = requireNonNull(storageEngineConstants);
        this.nativeStorageStateHandler = requireNonNull(nativeStorageStateHandler);
        statsWarmupDemoter = metricsManager.registerMetric(VaradaStatsWarmupDemoter.create(WarmupDemoterService.WARMUP_DEMOTER_STAT_GROUP));
        varadaInitializedServiceRegistry.addService(this);
    }

    @Override
    public void init()
    {
        cleanLocalStorage();
    }

    public synchronized void initWorker()
    {
        if (workerInitialized.compareAndSet(false, true)) {
            calculateReservationUsageForSingleTx();
        }
    }

    private void calculateReservationUsageForSingleTx()
    {
        int pageSize = storageEngineConstants.getPageSize();
        reservationUsageForSingleTx = globalConfiguration.getReservationUsageForSingleTxInBytes() / pageSize;
    }

    public boolean isWorkerInitialized()
    {
        return workerInitialized.get();
    }

    public Integer getExecutingTxCount()
    {
        return executingTxCount.get();
    }

    public double getFractionCurrentUsageFromTotal()
    {
        double fractionUsedUsageFromTotal = (double) getCurrentUsageGross() / (double) getTotalCapacity();
        if (fractionUsedUsageFromTotal > warmupDemoterConfiguration.getMaxUsageThresholdPercentage()) {
            logger.debug("get fraction usage=%f, executingTxCount=%d", fractionUsedUsageFromTotal, executingTxCount.get());
        }
        return fractionUsedUsageFromTotal;
    }

    public long getCurrentUsageGross()
    {
        int executingTxCount = this.executingTxCount.get();
        int actualExecutingTxCount = executingTxCount > 0 ? executingTxCount - 1 : 0;
        return getCurrentUsage() + actualExecutingTxCount * reservationUsageForSingleTx;
    }

    public long getCurrentUsage()
    {
        return totalCapacity - warpDir.getFreeSpace();
    }

    public void setCurrentUsage()
    {
        statsWarmupDemoter.setcurrentUsage(getCurrentUsage());
    }

    public long getTotalCapacity()
    {
        return totalCapacity;
    }

    public void increaseExecutingTx()
    {
        executingTxCount.incrementAndGet();
    }

    public void setExecutingTx(int executingTx)
    {
        executingTxCount.set(executingTx);
        statsWarmupDemoter.setreserved_tx(executingTx);
    }

    public void decreaseExecutingTx()
    {
        executingTxCount.decrementAndGet();
    }

    private void calculateTotalCapacity()
    {
        Path dataPath = Paths.get(globalConfiguration.getLocalStorePath());
        if (!Files.exists(dataPath)) {
            logger.error("local store directory does not exists %s setting StorageDisableState to permanently disabled", globalConfiguration.getLocalStorePath());
            nativeStorageStateHandler.setStorageDisableState(true, false);
            return;
        }

        try {
            warpDir = dataPath.toFile();
            totalCapacity = warpDir.getTotalSpace(); // As we are the sole users of the mount we can use total space
            statsWarmupDemoter.addtotalUsage(totalCapacity);
            logger.info("totalCapacity %dMB", totalCapacity >> 20);
        }
        catch (Exception e) {
            logger.error(e);
            throw new TrinoException(VaradaErrorCode.VARADA_CONTROL, "could not open local store directory");
        }
    }

    private void cleanLocalStorage()
    {
        String localStorePath = globalConfiguration.getLocalStorePath();

        File doNotRemove = new File(PathUtils.getUriPath(localStorePath, "DO-NOT-REMOVE"));
        if (doNotRemove.exists()) {
            if (doNotRemove.delete()) {
                logger.info("cleanLocalStorage exiting since DO-NOT-REMOVE file exists (and removed)");
            }
            else {
                logger.info("cleanLocalStorage exiting since DO-NOT-REMOVE file exists (was not removed)");
            }
            calculateTotalCapacity();
            return;
        }

        File localStore = new File(localStorePath);
        String[] ls = localStore.list();
        if ((ls == null) || (ls.length == 0)) {
            logger.info("cleanLocalStorage exiting since no files found");
            calculateTotalCapacity();
            return;
        }

        logger.info("cleanLocalStorage launching background delete for path %s", localStorePath);
        nativeStorageStateHandler.setStorageDisableState(true, false);
        Thread cleanLocalStorageThread = new Thread(() -> {
            try {
                logger.info("cleanLocalStorage job starting localStorePath %s", localStorePath);
                StopWatch stopWatch = new StopWatch();
                stopWatch.start();
                FileUtils.cleanDirectory(new File(localStorePath));
                stopWatch.stop();
                logger.info("cleanLocalStorage job finished. took %d nano sec", stopWatch.getNanoTime());
                // in case we hit an error, we leave total capacity as zero and storage state as permanently failed
                nativeStorageStateHandler.setStorageDisableState(false, false);
            }
            catch (IOException e) {
                logger.error(e, "cleanLocalStorage job failed to delete localStorePath %s", localStorePath);
            }
            finally {
                calculateTotalCapacity();
            }
        });
        cleanLocalStorageThread.start();
    }
}
