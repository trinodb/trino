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
package io.trino.plugin.warp.extension.execution.callhome;

import com.google.common.eventbus.EventBus;
import io.trino.plugin.varada.storage.engine.ConnectorSync;
import io.trino.plugin.varada.util.NodeUtils;
import io.trino.plugin.warp.extension.configuration.CallHomeConfiguration;
import io.trino.plugin.warp.extension.configuration.WarpExtensionConfiguration;
import io.trino.spi.NodeManager;
import io.varada.cloudvendors.CloudVendorService;
import io.varada.cloudvendors.configuration.CloudVendorConfiguration;
import io.varada.tools.CatalogNameProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CallHomeServiceTest
{
    private ScheduledExecutorService scheduledExecutorService;
    @SuppressWarnings("rawtypes") private ScheduledFuture unusedScheduledFuture;
    private CallHomeService callHomeService;

    @SuppressWarnings("unchecked")
    @BeforeEach
    public void beforeEach()
    {
        ConnectorSync connectorSync = mock(ConnectorSync.class);
        NodeManager nodeManager = NodeUtils.mockNodeManager();

        CloudVendorConfiguration cloudVendorConfiguration = new CloudVendorConfiguration();
        cloudVendorConfiguration.setStorePath("s3://storePathBucket/storePathS3_1/storePathS3_2");

        WarpExtensionConfiguration warpExtensionConfiguration = new WarpExtensionConfiguration();
        warpExtensionConfiguration.setClusterUUID(UUID.randomUUID().toString());

        CatalogNameProvider catalogNameProvider = new CatalogNameProvider("warp");

        CallHomeConfiguration callHomeConfiguration = new CallHomeConfiguration();

        CloudVendorService cloudVendorService = mock(CloudVendorService.class);

        scheduledExecutorService = mock(ScheduledExecutorService.class);
        unusedScheduledFuture = mock(RunnableScheduledFuture.class);
        when(scheduledExecutorService.schedule(any(CallHomeJob.class),
                eq(Integer.valueOf(callHomeConfiguration.getIntervalInSeconds()).longValue()),
                eq(TimeUnit.SECONDS))).thenReturn(unusedScheduledFuture);

        callHomeService = new CallHomeService(connectorSync,
                nodeManager,
                catalogNameProvider,
                cloudVendorConfiguration,
                callHomeConfiguration,
                cloudVendorService,
                mock(EventBus.class),
                scheduledExecutorService);
    }

    @Test
    public void testScheduleDontWait()
            throws ExecutionException, InterruptedException, TimeoutException
    {
        Optional<Integer> optional = callHomeService.triggerCallHome(
                "s3://storePathBucket/storePathS3_1/storePathS3_2",
                true,
                false);

        assertThat(optional).isEmpty();
        verify(scheduledExecutorService, times(1))
                .schedule(any(CallHomeJob.class),
                        anyLong(),
                        any(TimeUnit.class));
        verify(unusedScheduledFuture, never()).get(anyLong(), any(TimeUnit.class));
    }

    @Test
    public void testScheduleWait()
            throws ExecutionException, InterruptedException, TimeoutException
    {
        Optional<Integer> optional = callHomeService.triggerCallHome(
                "s3://storePathBucket/storePathS3_1/storePathS3_2",
                true,
                true);

        assertThat(optional).isNotEmpty().hasValue(0);
        verify(scheduledExecutorService, never())
                .schedule(any(CallHomeJob.class),
                        eq(0L),
                        eq(TimeUnit.SECONDS));
        verify(unusedScheduledFuture, never()).get(anyLong(), any(TimeUnit.class));
    }
}
