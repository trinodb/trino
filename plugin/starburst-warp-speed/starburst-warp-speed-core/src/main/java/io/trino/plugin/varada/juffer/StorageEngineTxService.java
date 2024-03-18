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
package io.trino.plugin.varada.juffer;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.plugin.varada.configuration.NativeConfiguration;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.warp.gen.stats.VaradaStatsTxService;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

/**
 * Responsible for managing the bundle pools - the large buffers shared between storage layer (c) and presto connector (java)
 * to pass data.
 */
@Singleton
public class StorageEngineTxService
{
    static final String STATS_GROUP_NAME = "txService";

    private final NativeConfiguration nativeConfiguration;
    private final VaradaStatsTxService statsTxService;
    private final ConcurrentLinkedQueue<CompletableFuture<Boolean>> warmingBlockingFutures;
    private final AtomicInteger runningPageSources = new AtomicInteger();
    private final AtomicInteger runningLoaders = new AtomicInteger();

    @Inject
    public StorageEngineTxService(
            NativeConfiguration nativeConfiguration,
            MetricsManager metricsManager)
    {
        this.nativeConfiguration = requireNonNull(nativeConfiguration);
        this.warmingBlockingFutures = new ConcurrentLinkedQueue<>();

        statsTxService = metricsManager.registerMetric(VaradaStatsTxService.create(STATS_GROUP_NAME));
    }

    public boolean isLoaderAvailable()
    {
        long ticket = runningLoaders.getAndIncrement();

        if (ticket < nativeConfiguration.getTaskMinWarmingThreads()) {
            return true;
        }
        else {
            runningLoaders.decrementAndGet();
            return false;
        }
    }

    public void doneWarming(boolean skipWait)
    {
        if (skipWait) {
            runningLoaders.decrementAndGet();
        }
    }

    public void updateRunningPageSourcesCount(boolean started)
    {
        if (started) {
            int currentlyRunningPageSources = runningPageSources.incrementAndGet();
            statsTxService.setrunning_page_source(currentlyRunningPageSources);
        }
        else {
            int currentlyRunningPageSources = runningPageSources.decrementAndGet();
            statsTxService.setrunning_page_source(currentlyRunningPageSources);
            if (currentlyRunningPageSources == 0) {
                releaseAllWaitingWarm();
            }
            else if (currentlyRunningPageSources < nativeConfiguration.getMaxPageSourcesWithoutWarmingLimit()) {
                releaseOneWaitingWarm();
            }
        }
    }

    private void releaseOneWaitingWarm()
    {
        CompletableFuture<Boolean> future = warmingBlockingFutures.poll();
        if (future != null) {
            statsTxService.setblocking_warmings(warmingBlockingFutures.size());
            future.complete(true);
        }
    }

    private void releaseAllWaitingWarm()
    {
        while (!warmingBlockingFutures.isEmpty()) {
            releaseOneWaitingWarm();
        }
    }

    public CompletableFuture<Boolean> tryToWarm()
    {
        CompletableFuture<Boolean> ret;
        if (runningPageSources.get() >= nativeConfiguration.getMaxPageSourcesWithoutWarmingLimit()) {
            ret = new CompletableFuture<>();
            warmingBlockingFutures.add(ret);
            statsTxService.setblocking_warmings(warmingBlockingFutures.size());
        }
        else {
            ret = CompletableFuture.completedFuture(true);
        }
        return ret;
    }

    @VisibleForTesting
    public int getRunningLoaders()
    {
        return runningLoaders.get();
    }
}
