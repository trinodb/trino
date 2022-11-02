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

import io.airlift.log.Logger;
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
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.metadata.NodeState.ACTIVE;
import static io.trino.metadata.NodeState.DECOMMISSIONED;
import static io.trino.metadata.NodeState.DECOMMISSIONING;
import static io.trino.metadata.NodeState.SHUTTING_DOWN;
import static io.trino.server.security.ResourceSecurity.AccessType.MANAGEMENT_WRITE;
import static io.trino.server.security.ResourceSecurity.AccessType.PUBLIC;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;

@Path("/v1/info")
public class ServerInfoResource
{
    private static Logger log = Logger.get(ServerInfoResource.class);

    private final NodeVersion version;
    private final String environment;
    private final boolean coordinator;
    private final GracefulShutdownHandler shutdownHandler;
    private final StartupStatus startupStatus;
    private final DecommissionHandler decommissionHandler;
    private final long startTime = System.nanoTime();
    private final AtomicBoolean startupComplete = new AtomicBoolean();
    private NodeState currState = NodeState.ACTIVE;

    @Inject
    public ServerInfoResource(NodeVersion nodeVersion, NodeInfo nodeInfo,
            ServerConfig serverConfig, GracefulShutdownHandler shutdownHandler,
            StartupStatus startupStatus,
            DecommissionHandler decommissionHandler)
    {
        this.version = requireNonNull(nodeVersion, "nodeVersion is null");
        this.environment = nodeInfo.getEnvironment();
        this.coordinator = serverConfig.isCoordinator();
        this.shutdownHandler = requireNonNull(shutdownHandler, "shutdownHandler is null");
        this.startupStatus = requireNonNull(startupStatus, "startupStatus is null");
        this.decommissionHandler = requireNonNull(decommissionHandler, "decommissionHandler is null");
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
        log.info(String.format("Entre updateState %s -> %s", currState, state));

        // Supported state transitions:
        //   1. ? -> ?
        //   2. * -> SHUTTING_DOWN
        //   3. ACTIVE -> DECOMMISSIONING
        //   4. DECOMMISSIONING, DECOMMISSIONED -> ACTIVE

        if (currState == state || (state == DECOMMISSIONING && currState == DECOMMISSIONED)) {
            return Response.ok().build();
        }

        // Prefer using a switch instead of a chained if-else for enums
        switch (state) {
            case SHUTTING_DOWN:
                shutdownHandler.requestShutdown();
                currState = SHUTTING_DOWN;
                return Response.ok().build();
            case DECOMMISSIONING:
                if (currState == ACTIVE) {
                    decommissionHandler.requestDecommission(this);
                    currState = DECOMMISSIONING;
                    return Response.ok().build();
                }
                break;
            case ACTIVE:
                if (currState == DECOMMISSIONING || currState == DECOMMISSIONED) {
                    currState = ACTIVE;
                    return Response.ok().build();
                }
                break;
            default:
                return Response.status(BAD_REQUEST).type(TEXT_PLAIN)
                        .entity(format("Invalid state %s", state))
                        .build();
        }

        throw new WebApplicationException(Response
                .status(BAD_REQUEST)
                .type(MediaType.TEXT_PLAIN)
                .entity(format("Invalid state transition %s to %s", currState, state))
                .build());
    }

    @ResourceSecurity(PUBLIC)
    @GET
    @Path("state")
    @Produces(APPLICATION_JSON)
    public NodeState getServerState()
    {
        return currState;
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

    // callback used by decommissionHandler
    NodeState onDecommissioned()
    {
        log.info("onDecommissioned " + (currState == null ? "null" : currState));
        if (currState == NodeState.DECOMMISSIONING) {
            currState = NodeState.DECOMMISSIONED;
        }
        return currState;
    }
}
