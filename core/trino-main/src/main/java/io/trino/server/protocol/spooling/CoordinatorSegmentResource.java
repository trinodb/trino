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
package io.trino.server.protocol.spooling;

import com.google.inject.Inject;
import io.trino.node.InternalNode;
import io.trino.node.InternalNodeManager;
import io.trino.server.ExternalUriInfo;
import io.trino.server.protocol.spooling.SpoolingConfig.SegmentRetrievalMode;
import io.trino.server.security.ResourceSecurity;
import io.trino.spi.HostAddress;
import io.trino.spi.spool.SpooledSegmentHandle;
import io.trino.spi.spool.SpoolingManager;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.ServiceUnavailableException;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriBuilder;
import jakarta.ws.rs.core.UriInfo;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.server.security.ResourceSecurity.AccessType.PUBLIC;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

@Path("/v1/spooled")
@ResourceSecurity(PUBLIC)
public class CoordinatorSegmentResource
{
    private final SpoolingManager spoolingManager;
    private final SegmentRetrievalMode retrievalMode;
    private final InternalNodeManager nodeManager;

    @Inject
    public CoordinatorSegmentResource(SpoolingManager spoolingManager, SpoolingConfig config, InternalNodeManager nodeManager)
    {
        this.spoolingManager = requireNonNull(spoolingManager, "spoolingManager is null");
        this.retrievalMode = requireNonNull(config, "config is null").getRetrievalMode();
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    @GET
    @Path("/download/{identifier}")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response download(@Context UriInfo uriInfo, @PathParam("identifier") String identifier, @Context HttpHeaders headers)
            throws IOException
    {
        SpooledSegmentHandle handle = handle(identifier, headers);

        return switch (retrievalMode) {
            case STORAGE -> throw new ServiceUnavailableException("Retrieval mode is STORAGE but segment resource was called");
            case COORDINATOR_PROXY -> Response.ok(spoolingManager.openInputStream(handle)).build();
            case WORKER_PROXY -> {
                HostAddress hostAddress = randomActiveWorkerNode();
                yield Response.seeOther(uriInfo
                                .getRequestUriBuilder()
                                .host(hostAddress.getHostText())
                                .port(hostAddress.getPort())
                                .build())
                        .build();
            }
            case COORDINATOR_STORAGE_REDIRECT -> Response
                    .seeOther(spoolingManager
                            .directLocation(handle).orElseThrow(() -> new ServiceUnavailableException("Could not generate pre-signed URI"))
                            .directUri())
                    .build();
        };
    }

    @GET
    @Path("/ack/{identifier}")
    public Response acknowledge(@PathParam("identifier") String identifier, @Context HttpHeaders headers)
            throws IOException
    {
        try {
            spoolingManager.acknowledge(handle(identifier, headers));
            return Response.ok().build();
        }
        catch (IOException e) {
            return Response.serverError()
                    .entity(e.toString())
                    .build();
        }
    }

    public static UriBuilder spooledSegmentUriBuilder(ExternalUriInfo info)
    {
        return UriBuilder.fromUri(info.baseUriBuilder().build())
                .path(CoordinatorSegmentResource.class);
    }

    public HostAddress randomActiveWorkerNode()
    {
        List<InternalNode> internalNodes = nodeManager.getActiveNodesSnapshot().getAllNodes()
                .stream()
                .filter(node -> !node.isCoordinator())
                .collect(toImmutableList());

        verify(!internalNodes.isEmpty(), "No active worker nodes available");
        return internalNodes.get(ThreadLocalRandom.current().nextInt(internalNodes.size()))
                .getHostAndPort();
    }

    private SpooledSegmentHandle handle(String identifier, HttpHeaders headers)
    {
        return spoolingManager.handle(wrappedBuffer(identifier.getBytes(UTF_8)), headers.getRequestHeaders());
    }
}
