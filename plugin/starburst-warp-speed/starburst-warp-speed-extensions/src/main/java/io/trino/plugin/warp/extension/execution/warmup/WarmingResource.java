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
package io.trino.plugin.warp.extension.execution.warmup;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.http.client.FullJsonResponseHandler;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.trino.plugin.varada.CoordinatorNodeManager;
import io.trino.plugin.varada.execution.VaradaClient;
import io.trino.plugin.varada.util.UriUtils;
import io.trino.plugin.warp.extension.execution.TaskResource;
import io.trino.plugin.warp.extension.execution.TaskResourceMarker;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static java.util.Objects.requireNonNull;

@TaskResourceMarker(worker = false)
@Singleton
//@Api(value = "Warmup", tags = WarmingResource.WARMING_SWAGGER_TAG)
@Path(WarmingResource.WARMING)
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class WarmingResource
        implements TaskResource
{
    public static final String WARMING_SWAGGER_TAG = "Warmup";
    public static final String WARMING = "warming";
    public static final String WARMING_STATUS = "status";
    private static final JsonCodec<WorkerWarmingStatusData> WARMING_STATUS_DATA_JSON_CODEC = JsonCodec.jsonCodec(WorkerWarmingStatusData.class);
    private final CoordinatorNodeManager coordinatorNodeManager;
    private final VaradaClient varadaClient;

    @Inject
    public WarmingResource(CoordinatorNodeManager coordinatorNodeManager,
            VaradaClient varadaClient)
    {
        this.coordinatorNodeManager = requireNonNull(coordinatorNodeManager);
        this.varadaClient = requireNonNull(varadaClient);
    }

    @GET
    @Path(WARMING_STATUS)
    //@ApiOperation(value = "get nodes warming status", extensions = {@Extension(properties = @ExtensionProperty(name = "exposing-level", value = "PRODUCTION"))})
    public WarmingStatusData getNodesStatus()
    {
        Map<String, HttpClient.HttpResponseFuture<FullJsonResponseHandler.JsonResponse<WorkerWarmingStatusData>>> allFutures = new HashMap<>();
        coordinatorNodeManager.getWorkerNodes()
                .forEach(node -> {
                    HttpUriBuilder uriBuilder = varadaClient.getRestEndpoint(UriUtils.getHttpUri(node));
                    uriBuilder.appendPath(WarmingResource.WARMING);
                    uriBuilder.appendPath(WorkerWarmingResource.STATUS);

                    Request request = prepareGet()
                            .setUri(uriBuilder.build())
                            .setHeader("Content-Type", "application/json")
                            .build();
                    allFutures.put(node.getHost(), varadaClient.executeAsync(request, createFullJsonResponseHandler(WARMING_STATUS_DATA_JSON_CODEC)));
                });

        try {
            ListenableFuture<List<FullJsonResponseHandler.JsonResponse<WorkerWarmingStatusData>>> waitingFuture = Futures.allAsList(allFutures.values());
            waitingFuture.get();
            Map<String, WorkerWarmingStatusData> nodesStatus = new HashMap<>();
            boolean warming = false;
            for (Map.Entry<String, HttpClient.HttpResponseFuture<FullJsonResponseHandler.JsonResponse<WorkerWarmingStatusData>>> entry : allFutures.entrySet()) {
                WorkerWarmingStatusData workerWarmingStatusData = entry.getValue().get().getValue();
                nodesStatus.put(entry.getKey(), workerWarmingStatusData);
                if (workerWarmingStatusData.finished() != workerWarmingStatusData.started()) {
                    warming = true;
                }
            }
            return new WarmingStatusData(nodesStatus, warming);
        }
        catch (Throwable e) {
            throw new RuntimeException("failed executing get warming status", e);
        }
    }
}
