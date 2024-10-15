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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;
import io.trino.server.ExternalUriInfo;
import io.trino.server.security.ResourceSecurity;
import io.trino.spi.HostAddress;
import io.trino.spi.protocol.SpooledLocation.DirectLocation;
import io.trino.spi.protocol.SpooledSegmentHandle;
import io.trino.spi.protocol.SpoolingManager;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriBuilder;
import jakarta.ws.rs.core.UriInfo;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.server.security.ResourceSecurity.AccessType.PUBLIC;
import static io.trino.spi.protocol.SpooledLocation.coordinatorLocation;
import static java.lang.Math.floorDiv;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

@Path("/v1/spooled/segments/{identifier}")
@ResourceSecurity(PUBLIC)
public class SegmentResource
{
    private final SpoolingManager spoolingManager;
    private final boolean useWorkers;
    private final InternalNodeManager nodeManager;
    private final AtomicInteger nextWorkerIndex = new AtomicInteger();

    @Inject
    public SegmentResource(SpoolingManager spoolingManager, SpoolingConfig config, InternalNodeManager nodeManager)
    {
        this.spoolingManager = requireNonNull(spoolingManager, "spoolingManager is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.useWorkers = config.isUseWorkers() && nodeManager.getCurrentNode().isCoordinator();
    }

    @GET
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @ResourceSecurity(PUBLIC)
    public Response download(@Context UriInfo uriInfo, @PathParam("identifier") String identifier, @Context HttpHeaders headers)
            throws IOException
    {
        SpooledSegmentHandle handle = handle(identifier, headers);
        Optional<DirectLocation> directLocation = spoolingManager.directLocation(handle);

        // Direct access is enabled & supported - redirect user to the spooled location using pre-signed URIs
        if (directLocation.isPresent()) {
            return Response
                    .seeOther(directLocation.get().uri())
                    .build();
        }

        if (useWorkers) {
            HostAddress hostAddress = nextActiveNode();
            return Response.seeOther(uriInfo
                    .getRequestUriBuilder()
                        .host(hostAddress.getHostText())
                        .port(hostAddress.getPort())
                        .build())
                    .build();
        }
        // Either direct access is not enabled or the fallback to the coordinator access happened
        return Response.ok(spoolingManager.openInputStream(handle)).build();
    }

    @DELETE
    @ResourceSecurity(PUBLIC)
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
                .path(SegmentResource.class);
    }

    public HostAddress nextActiveNode()
    {
        List<InternalNode> internalNodes = ImmutableList.copyOf(nodeManager.getActiveNodesSnapshot().getAllNodes());
        verify(!internalNodes.isEmpty(), "No active nodes available");
        return internalNodes.get(floorDiv(nextWorkerIndex.incrementAndGet(), internalNodes.size()))
                .getHostAndPort();
    }

    private SpooledSegmentHandle handle(String identifier, HttpHeaders headers)
    {
        return spoolingManager.handle(coordinatorLocation(wrappedBuffer(identifier.getBytes(UTF_8)), headers.getRequestHeaders()));
    }
}
