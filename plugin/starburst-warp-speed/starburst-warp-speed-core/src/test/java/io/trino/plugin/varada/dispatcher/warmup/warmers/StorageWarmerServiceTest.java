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

import io.trino.plugin.varada.TestingTxService;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.dispatcher.services.RowGroupDataService;
import io.trino.plugin.varada.dispatcher.warmup.demoter.AcquireWarmupStatus;
import io.trino.plugin.varada.dispatcher.warmup.demoter.WarmupDemoterService;
import io.trino.plugin.varada.juffer.StorageEngineTxService;
import io.trino.plugin.varada.storage.engine.ConnectorSync;
import io.trino.plugin.varada.storage.engine.StubsStorageEngine;
import io.trino.plugin.varada.storage.flows.FlowsSequencer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class StorageWarmerServiceTest
{
    @Test
    public void testTryToAcquireWarmupLoader()
    {
        WarmupDemoterService warmupDemoterService = mock(WarmupDemoterService.class);
        when(warmupDemoterService.tryAllocateNativeResourceForWarmup()).thenReturn(AcquireWarmupStatus.SUCCESS,
                AcquireWarmupStatus.EXCEEDED_LOADERS,
                AcquireWarmupStatus.REACHED_THRESHOLD);
        StorageWarmerService storageWarmerService = new StorageWarmerService(mock(RowGroupDataService.class),
                new StubsStorageEngine(),
                new GlobalConfiguration(),
                mock(ConnectorSync.class),
                warmupDemoterService,
                mock(StorageEngineTxService.class),
                mock(FlowsSequencer.class),
                TestingTxService.createMetricsManager());
        assertThat(storageWarmerService.tryAllocateNativeResourceForWarmup()).isTrue();
        assertThat(storageWarmerService.tryAllocateNativeResourceForWarmup()).isFalse();
        assertThat(storageWarmerService.tryAllocateNativeResourceForWarmup()).isFalse();
    }
}
