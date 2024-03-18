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

import com.google.inject.Inject;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.trino.plugin.varada.WorkerNodeManager;
import io.trino.plugin.varada.api.metrics.ClusterMetricsResult;
import io.trino.plugin.varada.execution.VaradaClient;
import io.trino.plugin.varada.storage.capacity.WorkerCapacityManager;
import io.trino.plugin.warp.extension.execution.TaskResource;
import io.trino.plugin.warp.extension.execution.TaskResourceMarker;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;

import java.util.Map;

import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static java.util.Objects.requireNonNull;

@TaskResourceMarker(coordinator = false)
@Path(WorkerMetricsTask.WORKER_METRICS_TASK)
//@Api(value = "Worker Metrics", tags = "Worker Metrics")
public class WorkerMetricsTask
        implements TaskResource
{
    public static final String WORKER_METRICS_TASK = "worker-metrics";
    private final WorkerCapacityManager workerCapacityManager;
    private final VaradaClient varadaClient;
    private final WorkerNodeManager workerNodeManager;

    @Inject
    public WorkerMetricsTask(WorkerCapacityManager workerCapacityManager,
            VaradaClient varadaClient,
            WorkerNodeManager workerNodeManager)
    {
        this.workerCapacityManager = requireNonNull(workerCapacityManager);
        this.varadaClient = requireNonNull(varadaClient);
        this.workerNodeManager = requireNonNull(workerNodeManager);
    }

    @GET
    //@ApiOperation(value = "get", extensions = {@Extension(properties = @ExtensionProperty(name = "exposing-level", value = "DEBUG"))})
    public ClusterMetricsResult workerMetricsGet()
    {
        long totalCapacity = workerCapacityManager.getTotalCapacity();
        long storageAllocated = workerCapacityManager.getCurrentUsage();
        Map nodeStatus = getNodeStatus();
        Map generalPoolInfo = (Map) ((Map) nodeStatus.get("memoryInfo")).get("pool");
        return new ClusterMetricsResult((Double) nodeStatus.get("systemCpuLoad"),
                ((long) generalPoolInfo.get("maxBytes")) - (long) generalPoolInfo.get("freeBytes"),
                (long) generalPoolInfo.get("maxBytes"),
                storageAllocated,
                totalCapacity);
    }

    private Map getNodeStatus()
    {
        Request request = prepareGet()
                .setUri(uriBuilderFrom(workerNodeManager.getCurrentNodeOrigHttpUri())
                        .appendPath("/v1/status")
                        .build())
                .build();
        return varadaClient.sendWithRetry(request, createFullJsonResponseHandler(JsonCodec.jsonCodec(Map.class)));
    }
}
