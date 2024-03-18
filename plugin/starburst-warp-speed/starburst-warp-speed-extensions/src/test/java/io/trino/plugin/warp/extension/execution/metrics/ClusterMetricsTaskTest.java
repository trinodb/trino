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

import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.trino.plugin.varada.CoordinatorNodeManager;
import io.trino.plugin.varada.api.metrics.ClusterMetricsResult;
import io.trino.plugin.varada.execution.VaradaClient;
import io.trino.plugin.varada.util.NodeUtils;
import io.trino.plugin.varada.util.UriUtils;
import io.trino.spi.Node;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClusterMetricsTaskTest
{
    public void testGet()
    {
        double workerCpuUsage = 1;
        long workerMemoryCapacity = 3;
        long workerMemoryAllocated = 2;
        long workerStorageCapacity = 10L;
        long workerStorageAllocated = 4L;

        CoordinatorNodeManager coordinatorNodeManager = mock(CoordinatorNodeManager.class);
        VaradaClient varadaClient = mock(VaradaClient.class);

        List<Node> workers = IntStream.range(0, 4)
                .mapToObj(i -> NodeUtils.node(Integer.toString(i), true))
                .collect(Collectors.toList());
        workers.forEach(node -> when(varadaClient.getRestEndpoint(eq(UriUtils.getHttpUri(node)))).thenReturn(HttpUriBuilder.uriBuilderFrom(UriUtils.getHttpUri(node))));
        when(coordinatorNodeManager.getWorkerNodes()).thenReturn(workers);

        List<ClusterMetricsResult> clusterMetricsResults = workers.stream()
                .map(node -> new ClusterMetricsResult(workerCpuUsage,
                        workerMemoryAllocated,
                        workerMemoryCapacity,
                        workerStorageAllocated,
                        workerStorageCapacity)).toList();

        when(varadaClient.sendWithRetry(any(Request.class), any()))
                .thenReturn(clusterMetricsResults.get(0), clusterMetricsResults.get(1), clusterMetricsResults.get(2), clusterMetricsResults.get(3));

        int workersCount = workers.size();
        ClusterMetricsResult expected = new ClusterMetricsResult(workerCpuUsage * workersCount,
                workerMemoryAllocated * workersCount,
                workerMemoryCapacity * workersCount,
                workerStorageAllocated * workersCount,
                workerStorageCapacity * workersCount);

        ClusterMetricsTask task = new ClusterMetricsTask(coordinatorNodeManager, varadaClient);
        assertThat(task.get()).isEqualTo(expected);
    }
}
