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

import io.airlift.node.NodeInfo;
import io.trino.client.NodeVersion;
import io.trino.client.ServerInfo;
import io.trino.metadata.NodeState;
import io.trino.server.security.ResourceSecurity;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.server.security.ResourceSecurity.AccessType.MANAGEMENT_WRITE;
import static io.trino.server.security.ResourceSecurity.AccessType.PUBLIC;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN;

@Path("/v1/info")
public class ServerInfoResource
{
    private final NodeVersion version;
    private final String environment;
    private final boolean coordinator;
    private final UpdateNodeStateHandler updateNodeStateHandler;
    private final StartupStatus startupStatus;
    private final long startTime = System.nanoTime();
    private final AtomicBoolean startupComplete = new AtomicBoolean();

    @Inject
    public ServerInfoResource(NodeVersion nodeVersion, NodeInfo nodeInfo,
            ServerConfig serverConfig, UpdateNodeStateHandler updateNodeStateHandler,
            StartupStatus startupStatus)
    {
        this.version = requireNonNull(nodeVersion, "nodeVersion is null");
        this.environment = nodeInfo.getEnvironment();
        this.coordinator = serverConfig.isCoordinator();
        this.updateNodeStateHandler = requireNonNull(updateNodeStateHandler, "updateNodeStateHandler is null");
        this.startupStatus = requireNonNull(startupStatus, "startupStatus is null");
    }

    @ResourceSecurity(PUBLIC)
    @GET
    @Produces(APPLICATION_JSON)
    public ServerInfo getInfo()
    {
        boolean starting = !startupStatus.isStartupComplete();
        return new ServerInfo(version, environment, coordinator, starting, Optional.of(nanosSince(startTime)));
    }

    @ResourceSecurity(MANAGEMENT_WRITE)
    @PUT
    @Path("state")
    @Consumes(APPLICATION_JSON)
    @Produces(TEXT_PLAIN)
    public Response updateState(NodeState state)
            throws WebApplicationException
    {
        requireNonNull(state, "state is null");
        return updateNodeStateHandler.updateState(state);
    }

    @ResourceSecurity(PUBLIC)
    @GET
    @Path("state")
    @Produces(APPLICATION_JSON)
    public NodeState getServerState()
    {
        return updateNodeStateHandler.getServerState();
    }

    @ResourceSecurity(PUBLIC)
    @GET
    @Path("coordinator")
    @Produces(TEXT_PLAIN)
    public Response getServerCoordinator()
    {
        if (coordinator) {
            return Response.ok().build();
        }
        // return 404 to allow load balancers to only send traffic to the coordinator
        return Response.status(Response.Status.NOT_FOUND).build();
    }

    public void startupComplete()
    {
        checkState(startupComplete.compareAndSet(false, true), "Server startup already marked as complete");
    }
}
