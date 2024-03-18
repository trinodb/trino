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
package io.trino.plugin.warp.extension.execution.metrics;

import io.airlift.http.client.FullJsonResponseHandler;
import io.airlift.http.client.Request;
import io.trino.plugin.varada.WorkerNodeManager;
import io.trino.plugin.varada.api.metrics.ClusterMetricsResult;
import io.trino.plugin.varada.execution.VaradaClient;
import io.trino.plugin.varada.storage.capacity.WorkerCapacityManager;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WorkerMetricsTaskTest
{
    @Test
    public void testGet()
    {
        double cpuUsage = 1;
        long memoryCapacity = 3;
        long memoryAllocated = 2;
        long storageCapacity = 10L;
        long storageAllocated = 4L;

        WorkerCapacityManager workerCapacityManager = mock(WorkerCapacityManager.class);
        when(workerCapacityManager.getTotalCapacity()).thenReturn(storageCapacity);
        when(workerCapacityManager.getCurrentUsage()).thenReturn(storageAllocated);

        VaradaClient varadaClient = mockNodeStatus(cpuUsage, memoryCapacity, memoryAllocated);

        WorkerNodeManager workerNodeManager = mock(WorkerNodeManager.class);
        when(workerNodeManager.getCurrentNodeOrigHttpUri()).thenReturn(URI.create("http://localhost"));

        WorkerMetricsTask task = new WorkerMetricsTask(workerCapacityManager, varadaClient, workerNodeManager);

        assertThat(task.workerMetricsGet())
                .isEqualTo(new ClusterMetricsResult(cpuUsage, memoryAllocated, memoryCapacity, storageAllocated, storageCapacity));
    }

    @SuppressWarnings("unchecked")
    private VaradaClient mockNodeStatus(double cpuUsage, long memoryCapacity, long memoryAllocated)
    {
        Map<String, Object> memoryPoolInfo = new HashMap<>();
        memoryPoolInfo.put("maxBytes", memoryCapacity);
        memoryPoolInfo.put("freeBytes", (memoryCapacity - memoryAllocated));

        Map<String, Object> memoryInfo = new HashMap<>();
        memoryInfo.put("pool", memoryPoolInfo);

        Map<String, Object> nodeStatus = new HashMap<>();
        nodeStatus.put("memoryInfo", memoryInfo);
        nodeStatus.put("systemCpuLoad", cpuUsage);

        VaradaClient varadaClient = mock(VaradaClient.class);
        when(varadaClient.sendWithRetry(any(Request.class), any(FullJsonResponseHandler.class)))
                .thenReturn(nodeStatus);
        return varadaClient;
    }
}
