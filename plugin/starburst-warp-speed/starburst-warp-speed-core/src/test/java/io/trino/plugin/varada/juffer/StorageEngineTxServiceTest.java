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

import io.trino.plugin.varada.configuration.NativeConfiguration;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.warp.gen.stats.VaradaStatsTxService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import static io.trino.plugin.varada.juffer.StorageEngineTxService.STATS_GROUP_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StorageEngineTxServiceTest
{
    private StorageEngineTxService storageEngineTxService;
    private NativeConfiguration nativeConfiguration;
    private MetricsManager metricsManager;

    @BeforeEach
    public void before()
    {
        nativeConfiguration = new NativeConfiguration();
        metricsManager = mock(MetricsManager.class);
        when(metricsManager.registerMetric(any())).thenReturn(VaradaStatsTxService.create(STATS_GROUP_NAME));
        this.storageEngineTxService = new StorageEngineTxService(nativeConfiguration, metricsManager);
    }

    @Test
    public void testTooManyPageSourcesBlockWarm()
    {
        int numOfPageSources = 1;
        nativeConfiguration.setMaxPageSourcesWithoutWarmingLimit(1);
        runPageSource(numOfPageSources, true);
        CompletableFuture<Boolean> future = storageEngineTxService.tryToWarm();
        assertThat(future.isDone()).isFalse();
    }

    @Test
    public void testClosingPageSourceExecuteWarm()
    {
        int numOfPageSources = 1;
        nativeConfiguration.setMaxPageSourcesWithoutWarmingLimit(numOfPageSources);
        storageEngineTxService = new StorageEngineTxService(nativeConfiguration, metricsManager);
        runPageSource(numOfPageSources, true);
        CompletableFuture<Boolean> future = storageEngineTxService.tryToWarm();
        assertThat(future.isDone()).isFalse();
        runPageSource(numOfPageSources, false);
        assertThat(future.isDone()).isTrue();
    }

    @Test
    public void testClosingPageSourceShouldExecuteOnlySingleWarm()
    {
        int numOfPageSources = 3;
        nativeConfiguration.setMaxPageSourcesWithoutWarmingLimit(numOfPageSources);
        storageEngineTxService = new StorageEngineTxService(nativeConfiguration, metricsManager);
        runPageSource(numOfPageSources + 2, true);
        CompletableFuture<Boolean> future1 = storageEngineTxService.tryToWarm();
        CompletableFuture<Boolean> future2 = storageEngineTxService.tryToWarm();
        assertThat(future1.isDone()).isFalse();
        assertThat(future2.isDone()).isFalse();
        runPageSource(3, false);
        assertThat(future1.isDone()).isTrue();
        assertThat(future2.isDone()).isFalse();
    }

    @Test
    public void testMinWarmingThreads()
    {
        int minWarmingThreads = 2;
        nativeConfiguration.setTaskMinWarmingThreads(minWarmingThreads);
        storageEngineTxService = new StorageEngineTxService(nativeConfiguration, metricsManager);
        assertThat(storageEngineTxService.isLoaderAvailable()).isTrue();
        assertThat(storageEngineTxService.isLoaderAvailable()).isTrue();
        assertThat(storageEngineTxService.isLoaderAvailable()).isFalse();
        assertThat(storageEngineTxService.isLoaderAvailable()).isFalse();
        storageEngineTxService.doneWarming(true);
        assertThat(storageEngineTxService.isLoaderAvailable()).isTrue();
        assertThat(storageEngineTxService.isLoaderAvailable()).isFalse();
        storageEngineTxService.doneWarming(false);
        assertThat(storageEngineTxService.isLoaderAvailable()).isFalse();
        storageEngineTxService.doneWarming(true);
        storageEngineTxService.doneWarming(true);
        assertThat(storageEngineTxService.isLoaderAvailable()).isTrue();
        assertThat(storageEngineTxService.isLoaderAvailable()).isTrue();
        assertThat(storageEngineTxService.isLoaderAvailable()).isFalse();
    }

    private void runPageSource(int count, boolean start)
    {
        IntStream.range(0, count).forEach((i) -> storageEngineTxService.updateRunningPageSourcesCount(start));
    }
}
