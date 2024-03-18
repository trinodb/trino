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
package io.trino.plugin.warp.extension.execution.debugtools;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
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
import io.varada.annotation.Audit;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.trino.plugin.warp.extension.execution.debugtools.WarmupDemoterTask.WARMUP_DEMOTER_PATH;
import static java.util.Objects.requireNonNull;

@TaskResourceMarker(worker = false)
@Path(WARMUP_DEMOTER_PATH)
//@Api(value = "Demoter", tags = "Demoter")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class WarmupDemoterTask
        implements TaskResource
{
    public static final String WARMUP_DEMOTER_PATH = "demoter";
    public static final String WARMUP_DEMOTER_START_TASK_NAME = "warmup-demoter-start";
    public static final String WARMUP_DEMOTER_STATUS_TASK_NAME = "warmup-demoter-status";
    private static final JsonCodec<WarmupDemoterData> startWarmupDemoterJsonCodec = JsonCodec.jsonCodec(WarmupDemoterData.class);
    private static final JsonCodec<Map<String, Object>> workerWarmupDemoterResult = JsonCodec.mapJsonCodec(String.class, Object.class);
    private static final JsonCodec<DemoterStatus> workerWarmupDemoterStatusResult = JsonCodec.jsonCodec(DemoterStatus.class);
    private final CoordinatorNodeManager coordinatorNodeManager;
    private final VaradaClient varadaClient;

    @Inject
    public WarmupDemoterTask(CoordinatorNodeManager coordinatorNodeManager,
            VaradaClient varadaClient)
    {
        this.coordinatorNodeManager = requireNonNull(coordinatorNodeManager);
        this.varadaClient = requireNonNull(varadaClient);
    }

    @POST
    @Path(WARMUP_DEMOTER_START_TASK_NAME)
    //@ApiOperation(value = "start", extensions = {@Extension(properties = @ExtensionProperty(name = "exposing-level", value = "DEBUG"))})
    @Audit
    public Map<String, Object> start(WarmupDemoterData warmupDemoterData)
    {
        Map<String, HttpClient.HttpResponseFuture<FullJsonResponseHandler.JsonResponse<Map<String, Object>>>> allFutures = new HashMap<>();
        coordinatorNodeManager.getWorkerNodes()
                .forEach(node -> {
                    HttpUriBuilder uriBuilder = varadaClient.getRestEndpoint(UriUtils.getHttpUri(node));
                    uriBuilder.appendPath(WarmupDemoterTask.WARMUP_DEMOTER_PATH).appendPath(WorkerWarmupDemoterTask.WARMUP_DEMOTER_START_TASK_NAME);

                    Request request = preparePost()
                            .setUri(uriBuilder.build())
                            .setBodyGenerator(jsonBodyGenerator(startWarmupDemoterJsonCodec, warmupDemoterData))
                            .setHeader("Content-Type", "application/json")
                            .build();
                    allFutures.put(node.getNodeIdentifier(), varadaClient.executeAsync(request, createFullJsonResponseHandler(workerWarmupDemoterResult)));
                });

        ListenableFuture<List<FullJsonResponseHandler.JsonResponse<Map<String, Object>>>> waitingFuture = Futures.allAsList(allFutures.values());
        try {
            waitingFuture.get();
            Map<String, Map<String, Object>> allRes = new HashMap<>();
            for (Map.Entry<String, HttpClient.HttpResponseFuture<FullJsonResponseHandler.JsonResponse<Map<String, Object>>>> entry : allFutures.entrySet()) {
                allRes.put(entry.getKey(), entry.getValue().get().getValue());
            }
            return mergeResults(allRes);
        }
        catch (Throwable e) {
            throw new RuntimeException("failed executing warmup demoter task", e);
        }
    }

    @GET
    @Path(WARMUP_DEMOTER_STATUS_TASK_NAME)
    public Map<String, Object> status()
    {
        Map<String, HttpClient.HttpResponseFuture<FullJsonResponseHandler.JsonResponse<DemoterStatus>>> allFutures = new HashMap<>();
        coordinatorNodeManager.getWorkerNodes()
                .forEach(node -> {
                    HttpUriBuilder uriBuilder = varadaClient.getRestEndpoint(UriUtils.getHttpUri(node));
                    uriBuilder.appendPath(WarmupDemoterTask.WARMUP_DEMOTER_PATH).appendPath(WorkerWarmupDemoterTask.WARMUP_DEMOTER_STATUS_TASK_NAME);

                    Request request = prepareGet()
                            .setUri(uriBuilder.build())
                            .setHeader("Content-Type", "application/json")
                            .build();
                    allFutures.put(node.getNodeIdentifier(), varadaClient.executeAsync(request, createFullJsonResponseHandler(workerWarmupDemoterStatusResult)));
                });

        ListenableFuture<List<FullJsonResponseHandler.JsonResponse<DemoterStatus>>> waitingFuture = Futures.allAsList(allFutures.values());
        try {
            waitingFuture.get();
            Map<String, Object> allRes = new HashMap<>();
            for (Map.Entry<String, HttpClient.HttpResponseFuture<FullJsonResponseHandler.JsonResponse<DemoterStatus>>> entry : allFutures.entrySet()) {
                allRes.put(entry.getKey(), entry.getValue().get().getValue());
            }
            return allRes;
        }
        catch (Throwable e) {
            throw new RuntimeException("failed executing warmup demoter task", e);
        }
    }

    private Map<String, Object> mergeResults(Map<String, Map<String, Object>> allRes)
    {
        Map<String, Object> res = new HashMap<>();
        allRes.forEach((workerIp, workerResMap) ->
                workerResMap.forEach((k, v) -> res.put(String.format("%s:%s", workerIp, k), v)));
        return res;
    }
}
