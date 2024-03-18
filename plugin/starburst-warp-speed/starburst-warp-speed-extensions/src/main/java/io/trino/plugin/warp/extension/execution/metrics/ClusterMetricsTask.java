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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.trino.plugin.varada.CoordinatorNodeManager;
import io.trino.plugin.varada.api.metrics.ClusterMetricsResult;
import io.trino.plugin.varada.execution.VaradaClient;
import io.trino.plugin.varada.util.UriUtils;
import io.trino.plugin.warp.extension.execution.TaskResource;
import io.trino.plugin.warp.extension.execution.TaskResourceMarker;
import io.trino.spi.Node;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.trino.plugin.warp.extension.execution.metrics.ClusterMetricsTask.TASK_NAME;
import static io.trino.plugin.warp.extension.execution.metrics.WorkerMetricsTask.WORKER_METRICS_TASK;
import static java.util.Objects.requireNonNull;

@TaskResourceMarker(worker = false)
@Path(TASK_NAME)
//@Api(value = "Cluster Metrics", tags = "Cluster Metrics")
public class ClusterMetricsTask
        implements TaskResource
{
    public static final String TASK_NAME = "cluster-metrics";

    private static final Logger logger = Logger.get(ClusterMetricsTask.class);

    private static final JsonCodec<ClusterMetricsResult> workerMetricsCollect = JsonCodec.jsonCodec(ClusterMetricsResult.class);

    private final CoordinatorNodeManager coordinatorNodeManager;
    private final VaradaClient varadaClient;

    private final ExecutorService executorService;

    private ClusterMetricsResult currentClusterMetricsResult;

    @Inject
    public ClusterMetricsTask(CoordinatorNodeManager coordinatorNodeManager,
            VaradaClient varadaClient)
    {
        this.coordinatorNodeManager = requireNonNull(coordinatorNodeManager);
        this.varadaClient = requireNonNull(varadaClient);
        executorService = new ThreadPoolExecutor(0,
                10,
                10L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(1000));
    }

    @GET
    //@ApiOperation(value = "get", extensions = {@Extension(properties = @ExtensionProperty(name = "exposing-level", value = "DEBUG"))})
    public ClusterMetricsResult get()
    {
        List<Node> workerNodes = coordinatorNodeManager.getWorkerNodes();
        try {
            List<ListenableFuture<ClusterMetricsResult>> workerMetricsFutures = workerNodes.stream()
                    .map(node -> {
                        HttpUriBuilder uriBuilder = varadaClient.getRestEndpoint(UriUtils.getHttpUri(node))
                                .appendPath(WORKER_METRICS_TASK);
                        Request request = prepareGet()
                                .setUri(uriBuilder.build())
                                .setHeader("Content-Type", "application/json")
                                .build();
                        return Futures.submit(() -> varadaClient.sendWithRetry(request, createFullJsonResponseHandler(workerMetricsCollect)), executorService);
                    })
                    .collect(Collectors.toList());

            currentClusterMetricsResult = handleWorkerMetricsFutures(workerMetricsFutures);
        }
        catch (Throwable e) {
            logger.error(e, "failed getting cluster metrics");
        }
        return currentClusterMetricsResult;
    }

    private ClusterMetricsResult handleWorkerMetricsFutures(List<ListenableFuture<ClusterMetricsResult>> workerMetricsFutures)
            throws ExecutionException, InterruptedException
    {
        Futures.allAsList(workerMetricsFutures).get();

        double cpuUsage = 0;
        long memoryCapacity = 0;
        long memoryAllocated = 0;
        long storageCapacity = 0;
        long storageAllocated = 0;
        for (ListenableFuture<ClusterMetricsResult> future : workerMetricsFutures) {
            ClusterMetricsResult workerMetric = future.get();
            cpuUsage += workerMetric.cpuUsage();
            memoryCapacity += workerMetric.memoryCapacity();
            memoryAllocated += workerMetric.memoryAllocated();
            storageCapacity += workerMetric.storageCapacity();
            storageAllocated += workerMetric.storageAllocated();
        }
        return new ClusterMetricsResult(cpuUsage, memoryAllocated, memoryCapacity, storageAllocated, storageCapacity);
    }
}
