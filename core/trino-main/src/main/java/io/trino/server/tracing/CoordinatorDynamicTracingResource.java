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
package io.trino.server.tracing;

import com.google.inject.Inject;
import io.trino.server.security.ResourceSecurity;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Response;

import static io.trino.server.security.ResourceSecurity.AccessType.MANAGEMENT_READ;
import static io.trino.server.security.ResourceSecurity.AccessType.MANAGEMENT_WRITE;
import static jakarta.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static java.util.Objects.requireNonNull;

@Path("/v1/tracing")
@Produces("application/json")
public class CoordinatorDynamicTracingResource
{
    private final CoordinatorDynamicTracingController controller;

    @Inject
    public CoordinatorDynamicTracingResource(CoordinatorDynamicTracingController controller)
    {
        this.controller = requireNonNull(controller, "controller is null");
    }

    @GET
    @ResourceSecurity(MANAGEMENT_READ)
    public Response getStatus()
    {
        return Response.ok(controller.getNodesStatus()).build();
    }

    @GET
    @Path("/enable")
    @ResourceSecurity(MANAGEMENT_WRITE)
    public Response enable()
    {
        TracingStatus tracingStatus = controller.enableExport();
        if (tracingStatus.error().isPresent()) {
            return Response.status(INTERNAL_SERVER_ERROR).entity(tracingStatus).build();
        }

        return Response.ok(controller.getNodesStatus()).build();
    }

    @GET
    @Path("/disable")
    @ResourceSecurity(MANAGEMENT_WRITE)
    public Response disable()
    {
        TracingStatus tracingStatus = controller.disableExport();
        if (tracingStatus.error().isPresent()) {
            return Response.status(INTERNAL_SERVER_ERROR).entity(tracingStatus).build();
        }

        return Response.ok(controller.getNodesStatus()).build();
    }
}
