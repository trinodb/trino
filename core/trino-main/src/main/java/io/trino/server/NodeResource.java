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
package io.trino.server;

import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.failuredetector.HeartbeatFailureDetector;
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;
import io.trino.metadata.NodeState;
import io.trino.server.security.ResourceSecurity;
import io.trino.server.ui.WorkerResource.JsonNodeInfo;
import io.trino.spi.Node;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Response;

import java.util.Collection;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Predicates.in;
import static io.trino.metadata.NodeState.ACTIVE;
import static io.trino.metadata.NodeState.INACTIVE;
import static io.trino.server.security.ResourceSecurity.AccessType.MANAGEMENT_READ;
import static java.util.Objects.requireNonNull;

@Path("/v1/node")
public class NodeResource
{
    private final HeartbeatFailureDetector failureDetector;
    private final InternalNodeManager nodeManager;
    private final boolean isIncludeCoordinator;

    @Inject
    public NodeResource(
            HeartbeatFailureDetector failureDetector,
            InternalNodeManager nodeManager,
            NodeSchedulerConfig nodeSchedulerConfig)
    {
        this.failureDetector = requireNonNull(failureDetector, "failureDetector is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.isIncludeCoordinator = requireNonNull(nodeSchedulerConfig, "nodeSchedulerConfig is null").isIncludeCoordinator();
    }

    @ResourceSecurity(MANAGEMENT_READ)
    @GET
    public Collection<HeartbeatFailureDetector.Stats> getNodeStats()
    {
        return failureDetector.getStats().values();
    }

    @ResourceSecurity(MANAGEMENT_READ)
    @GET
    @Path("failed")
    public Collection<HeartbeatFailureDetector.Stats> getFailed()
    {
        return Maps.filterKeys(failureDetector.getStats(), in(failureDetector.getFailed())).values();
    }

    @ResourceSecurity(MANAGEMENT_READ)
    @GET
    @Path("worker")
    public Response getWorkerList()
    {
        Set<InternalNode> activeNodes = nodeManager.getNodes(NodeState.ACTIVE).stream()
                .filter(node -> isIncludeCoordinator || !node.isCoordinator()).collect(Collectors.toSet());
        Set<InternalNode> inactiveNodes = nodeManager.getNodes(NodeState.INACTIVE).stream()
                .filter(node -> isIncludeCoordinator || !node.isCoordinator()).collect(Collectors.toSet());
        Set<JsonNodeInfo> jsonNodes = new HashSet<>();
        for (Node node : activeNodes) {
            JsonNodeInfo jsonNode = new JsonNodeInfo(node.getNodeIdentifier(), node.getHostAndPort().getHostText(), node.getVersion(), node.isCoordinator(), ACTIVE.toString().toLowerCase(Locale.ENGLISH));
            jsonNodes.add(jsonNode);
        }
        for (Node node : inactiveNodes) {
            JsonNodeInfo jsonNode = new JsonNodeInfo(node.getNodeIdentifier(), node.getHostAndPort().getHostText(), node.getVersion(), node.isCoordinator(), INACTIVE.toString().toLowerCase(Locale.ENGLISH));
            jsonNodes.add(jsonNode);
        }
        return Response.ok().entity(jsonNodes).build();
    }
}
