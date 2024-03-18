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
package io.trino.plugin.warp.extension.execution.demoter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.eventbus.EventBus;
import io.airlift.json.ObjectMapperProvider;
import io.trino.plugin.varada.configuration.WarmupDemoterConfiguration;
import io.trino.plugin.varada.dispatcher.warmup.demoter.WarmupDemoterService;
import io.trino.plugin.varada.dispatcher.warmup.demoter.events.WarmupDemoterFinishEvent;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.storage.capacity.WorkerCapacityManager;
import io.trino.plugin.warp.extension.execution.debugtools.WarmupDemoterData;
import io.trino.plugin.warp.extension.execution.debugtools.WarmupDemoterThreshold;
import io.trino.plugin.warp.extension.execution.debugtools.WorkerWarmupDemoterTask;
import io.trino.plugin.warp.gen.stats.VaradaStatsWarmupDemoter;
import io.varada.tools.CatalogNameProvider;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static io.trino.plugin.varada.dispatcher.warmup.demoter.WarmupDemoterService.WARMUP_DEMOTER_STAT_GROUP;
import static org.assertj.core.api.Assertions.assertThat;

public class WorkerWarmupDemoterTaskTest
{
    final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private WorkerWarmupDemoterTask workerWarmupDemoterTask;
    private WarmupDemoterService warmupDemoterService;
    private WorkerCapacityManager workerCapacityManager;
    private WarmupDemoterConfiguration warmupDemoterConfiguration;
    private EventBus eventBus;

    @BeforeEach
    public void before()
    {
        warmupDemoterConfiguration = new WarmupDemoterConfiguration();
        workerCapacityManager = Mockito.mock(WorkerCapacityManager.class);
        warmupDemoterService = Mockito.mock(WarmupDemoterService.class);
        eventBus = new EventBus();
        VaradaStatsWarmupDemoter varadaStatsWarmupDemoter = VaradaStatsWarmupDemoter.create(WARMUP_DEMOTER_STAT_GROUP);
        MetricsManager metricsManager = Mockito.mock(MetricsManager.class);
        Mockito.when(metricsManager.registerMetric(ArgumentMatchers.any())).thenReturn(varadaStatsWarmupDemoter);
        workerWarmupDemoterTask = new WorkerWarmupDemoterTask(warmupDemoterService,
                warmupDemoterConfiguration,
                workerCapacityManager,
                Mockito.mock(CatalogNameProvider.class),
                metricsManager,
                eventBus);
    }

    @Test
    public void testCalculatedThreshold()
    {
        Mockito.when(workerCapacityManager.getCurrentUsage()).thenReturn(2048L);
        Mockito.when(workerCapacityManager.getTotalCapacity()).thenReturn(4096L);
        Mockito.when(warmupDemoterService.tryDemoteStart()).thenAnswer(invocation -> {
            Future<?> unused = executorService.submit(() -> eventBus.post(new WarmupDemoterFinishEvent(1, true, new HashMap<>())));
            return 1;
        });
        WarmupDemoterData warmupDemoterData = WarmupDemoterData.builder()
                .batchSize(1)
                .executeDemoter(true)
                .modifyConfiguration(true)
                .warmupDemoterThreshold(new WarmupDemoterThreshold(0.9, 0.7))
                .build();
        workerWarmupDemoterTask.start(warmupDemoterData);
        Assertions.assertThat(warmupDemoterConfiguration.getMaxUsageThresholdPercentage()).isEqualTo(45d);
        Assertions.assertThat(warmupDemoterConfiguration.getCleanupUsageThresholdPercentage()).isEqualTo(35d);
        Assertions.assertThat(warmupDemoterConfiguration.getBatchSize()).isEqualTo(1);
    }

    @Test
    public void testDefaultValues()
            throws JsonProcessingException
    {
        ObjectMapper objectMapper = new ObjectMapperProvider().get();
        String jsonStr = "{\"@class\":\"io.trino.plugin.warp.extension.execution.debugtools.WarmupDemoterData\"}";
        WarmupDemoterData warmupDemoterData = objectMapper.readerFor(WarmupDemoterData.class).readValue(jsonStr);
        assertThat(warmupDemoterData.getBatchSize()).isEqualTo(-1);
    }
}
