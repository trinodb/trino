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
package io.trino.plugin.warp.extension.execution.health;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.trino.plugin.varada.CoordinatorNodeManager;
import io.trino.plugin.varada.api.health.HealthNode;
import io.trino.plugin.varada.api.health.HealthResult;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.execution.VaradaClient;
import io.trino.plugin.varada.util.UriUtils;
import io.trino.plugin.warp.extension.execution.TaskResource;
import io.trino.plugin.warp.extension.execution.TaskResourceMarker;
import io.trino.spi.Node;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.trino.plugin.warp.extension.execution.health.HealthTask.HEALTH_PATH;
import static jakarta.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static java.util.Objects.requireNonNull;

@TaskResourceMarker(worker = false, shouldCheckExecutionAllowed = false)
//@Api(value = "Cluster Health", tags = HEALTH_TAG)
@Path(HEALTH_PATH)
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class ClusterHealthTask
        implements TaskResource
{
    public static final String TASK_NAME = "cluster-health";

    private static final JsonCodec<HealthResult> HEALTH_RESULT_CODEC = JsonCodec.jsonCodec(HealthResult.class);

    private final CoordinatorNodeManager coordinatorNodeManager;
    private final GlobalConfiguration globalConfiguration;
    private final VaradaClient varadaClient;

    @Inject
    public ClusterHealthTask(CoordinatorNodeManager coordinatorNodeManager,
            GlobalConfiguration globalConfiguration,
            VaradaClient varadaClient)
    {
        this.coordinatorNodeManager = requireNonNull(coordinatorNodeManager);
        this.globalConfiguration = requireNonNull(globalConfiguration);
        this.varadaClient = requireNonNull(varadaClient);
    }

    @SuppressWarnings("unused")
    @Path(TASK_NAME)
    @GET
    //@ApiOperation(value = "cluster-health", extensions = {@Extension(properties = @ExtensionProperty(name = "exposing-level", value = "PRODUCTION"))})
    public HealthResult getClusterHealthResult()
    {
        List<Node> workers = coordinatorNodeManager.getWorkerNodes();
        List<HealthNode> healthNodes = new ArrayList<>();

        long totalCapacityMB = workers.stream()
                .parallel()
                .map(node -> {
                    HttpUriBuilder uriBuilder = varadaClient.getRestEndpoint(UriUtils.getHttpUri(node));
                    uriBuilder.appendPath(HEALTH_PATH).appendPath(HealthTask.TASK_NAME);
                    Request request = prepareGet()
                            .setUri(uriBuilder.build())
                            .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                            .build();

                    long totalCapacity = 0;
                    try {
                        HealthResult healthResult = varadaClient.sendWithRetry(request, createFullJsonResponseHandler(HEALTH_RESULT_CODEC));
                        healthNodes.add(new HealthNode(healthResult.getCreateEpochTime(), node.getNodeIdentifier(), UriUtils.getHttpUri(node).toString(), "UP"));
                        totalCapacity = healthResult.getTotalCapacityMB();
                    }
                    catch (Exception e) {
                        healthNodes.add(new HealthNode(-1, node.getNodeIdentifier(), UriUtils.getHttpUri(node).toString(), "DOWN"));
                    }
                    return totalCapacity;
                })
                .mapToLong(Long::longValue)
                .sum();
        Optional<HealthNode> downNode = healthNodes.stream().filter(healthNode -> healthNode.state().equals("DOWN")).findAny();
        return new HealthResult(coordinatorNodeManager.isClusterReady() && downNode.isEmpty(),
                Objects.nonNull(coordinatorNodeManager.getCoordinatorNode()) ? UriUtils.getHttpUri(coordinatorNodeManager.getCoordinatorNode()) : null,
                globalConfiguration.getClusterUpTime(),
                ImmutableList.copyOf(healthNodes),
                totalCapacityMB);
    }
}
