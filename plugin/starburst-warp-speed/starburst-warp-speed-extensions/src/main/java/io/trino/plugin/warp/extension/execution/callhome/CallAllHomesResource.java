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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.http.client.FullJsonResponseHandler;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient.HttpResponseFuture;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.JsonBodyGenerator;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.trino.plugin.varada.CoordinatorNodeManager;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.execution.VaradaClient;
import io.trino.plugin.varada.util.UriUtils;
import io.trino.plugin.warp.extension.execution.TaskResource;
import io.trino.plugin.warp.extension.execution.TaskResourceMarker;
import io.trino.spi.Node;
import io.varada.annotation.Audit;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.trino.plugin.warp.extension.execution.callhome.CallAllHomesResource.CALL_ALL_HOMES_PATH;
import static io.trino.plugin.warp.extension.execution.callhome.CallHomeResource.CALL_HOME_PATH;
import static java.util.Objects.requireNonNull;

@Singleton
@Path(CALL_ALL_HOMES_PATH)
////@Api(value = "Call All Homes", tags = "Call Home")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@TaskResourceMarker(worker = false)
public class CallAllHomesResource
        implements TaskResource
{
    public static final String CALL_ALL_HOMES_PATH = "call-all-homes";

    private static final JsonCodec<CallHomeData> callHomeDataJsonCodec = JsonCodec.jsonCodec(CallHomeData.class);

    private final CoordinatorNodeManager coordinatorNodeManager;
    private final GlobalConfiguration globalConfiguration;
    private final VaradaClient varadaClient;

    @Inject
    public CallAllHomesResource(
            CoordinatorNodeManager coordinatorNodeManager,
            GlobalConfiguration globalConfiguration,
            VaradaClient varadaClient)
    {
        this.coordinatorNodeManager = requireNonNull(coordinatorNodeManager);
        this.globalConfiguration = requireNonNull(globalConfiguration);
        this.varadaClient = requireNonNull(varadaClient);
    }

    @POST
    @Audit
    ////@ApiOperation(value = "call-all-homes", extensions = {@Extension(properties = @ExtensionProperty(name = "exposing-level", value = "PRODUCTION"))})
    public void executeCallAllHomes(CallHomeData callHomeData)
    {
        List<Node> workerNodes = coordinatorNodeManager.getWorkerNodes();
        Map<String, HttpResponseFuture<JsonResponse<Void>>> allFutures = new HashMap<>();
        workerNodes.forEach(node -> {
            HttpUriBuilder uriBuilder = varadaClient.getRestEndpoint(UriUtils.getHttpUri(node));
            uriBuilder.appendPath(CALL_HOME_PATH);

            Request request = Request.Builder.preparePost()
                    .setUri(uriBuilder.build())
                    .setHeader("Content-Type", "application/json")
                    .setBodyGenerator(JsonBodyGenerator.jsonBodyGenerator(callHomeDataJsonCodec, callHomeData))
                    .build();
            allFutures.put(node.getNodeIdentifier(), varadaClient.executeAsync(request, FullJsonResponseHandler.createFullJsonResponseHandler(VaradaClient.VOID_RESULTS_CODEC)));
        });

        if (!globalConfiguration.getIsSingle()) {
            HttpUriBuilder uriBuilder = varadaClient.getRestEndpoint(UriUtils.getHttpUri(coordinatorNodeManager.getCoordinatorNode()));
            uriBuilder.appendPath(CALL_HOME_PATH);

            Request request = Request.Builder.preparePost()
                    .setUri(uriBuilder.build())
                    .setHeader("Content-Type", "application/json")
                    .setBodyGenerator(JsonBodyGenerator.jsonBodyGenerator(callHomeDataJsonCodec, callHomeData))
                    .build();
            allFutures.put(coordinatorNodeManager.getCoordinatorNode().getNodeIdentifier(), varadaClient.executeAsync(request, FullJsonResponseHandler.createFullJsonResponseHandler(VaradaClient.VOID_RESULTS_CODEC)));
        }

        ListenableFuture<List<JsonResponse<Void>>> waitingFuture = Futures.allAsList(allFutures.values());
        try {
            waitingFuture.get();
        }
        catch (Throwable e) {
            throw new RuntimeException("failed executing call home task", e);
        }
    }
}
