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
import io.trino.server.security.ResourceSecurity;
import io.trino.spi.spool.SpooledSegmentHandle;
import io.trino.spi.spool.SpoolingManager;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.io.IOException;

import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.server.security.ResourceSecurity.AccessType.PUBLIC;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

@Path("/v1/spooled/download/{identifier}")
@ResourceSecurity(PUBLIC)
public class WorkerSegmentResource
{
    private final SpoolingManager spoolingManager;

    @Inject
    public WorkerSegmentResource(SpoolingManager spoolingManager)
    {
        this.spoolingManager = requireNonNull(spoolingManager, "spoolingManager is null");
    }

    @GET
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response download(@PathParam("identifier") String identifier, @Context HttpHeaders headers)
            throws IOException
    {
        SpooledSegmentHandle handle = handle(identifier, headers);
        return Response.ok(spoolingManager.openInputStream(handle)).build();
    }

    private SpooledSegmentHandle handle(String identifier, HttpHeaders headers)
    {
        return spoolingManager.handle(wrappedBuffer(identifier.getBytes(UTF_8)), headers.getRequestHeaders());
    }
}
