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
package io.trino.plugin.raptor.legacy.backup;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;
import io.trino.server.GoneException;
import io.trino.spi.NodeManager;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static io.trino.plugin.raptor.legacy.backup.HttpBackupStore.CONTENT_XXH64;
import static io.trino.plugin.raptor.legacy.backup.HttpBackupStore.TRINO_ENVIRONMENT;
import static jakarta.ws.rs.core.MediaType.APPLICATION_OCTET_STREAM;
import static java.lang.Long.parseUnsignedLong;
import static java.util.Objects.requireNonNull;

@Path("/")
public class TestingHttpBackupResource
{
    private final String environment;

    @GuardedBy("this")
    private final Map<UUID, byte[]> shards = new HashMap<>();

    @Inject
    public TestingHttpBackupResource(NodeManager nodeManager)
    {
        this(nodeManager.getEnvironment());
    }

    public TestingHttpBackupResource(String environment)
    {
        this.environment = requireNonNull(environment, "environment is null");
    }

    @HEAD
    @Path("{uuid}")
    public synchronized Response headRequest(
            @HeaderParam(TRINO_ENVIRONMENT) String environment,
            @PathParam("uuid") UUID uuid)
    {
        checkEnvironment(environment);
        if (!shards.containsKey(uuid)) {
            throw new NotFoundException();
        }
        if (shards.get(uuid) == null) {
            throw new GoneException();
        }
        return Response.noContent().build();
    }

    @GET
    @Path("{uuid}")
    @Produces(APPLICATION_OCTET_STREAM)
    public synchronized Response getRequest(
            @HeaderParam(TRINO_ENVIRONMENT) String environment,
            @PathParam("uuid") UUID uuid)
    {
        checkEnvironment(environment);
        if (!shards.containsKey(uuid)) {
            throw new NotFoundException();
        }
        byte[] bytes = shards.get(uuid);
        if (bytes == null) {
            throw new GoneException();
        }
        return Response.ok(bytes).build();
    }

    @PUT
    @Path("{uuid}")
    public synchronized Response putRequest(
            @HeaderParam(TRINO_ENVIRONMENT) String environment,
            @HeaderParam(CONTENT_XXH64) String hexHash,
            @Context HttpServletRequest request,
            @PathParam("uuid") UUID uuid,
            byte[] bytes)
    {
        checkEnvironment(environment);
        if ((request.getContentLength() < 0) || (bytes.length != request.getContentLength())) {
            throw new BadRequestException();
        }
        if (parseUnsignedLong(hexHash, 16) != XxHash64.hash(Slices.wrappedBuffer(bytes))) {
            throw new BadRequestException();
        }
        if (shards.containsKey(uuid)) {
            byte[] existing = shards.get(uuid);
            if ((existing == null) || !Arrays.equals(bytes, existing)) {
                throw new ForbiddenException();
            }
        }
        shards.put(uuid, bytes);
        return Response.noContent().build();
    }

    @DELETE
    @Path("{uuid}")
    public synchronized Response deleteRequest(
            @HeaderParam(TRINO_ENVIRONMENT) String environment,
            @PathParam("uuid") UUID uuid)
    {
        checkEnvironment(environment);
        if (!shards.containsKey(uuid)) {
            throw new NotFoundException();
        }
        if (shards.get(uuid) == null) {
            throw new GoneException();
        }
        shards.put(uuid, null);
        return Response.noContent().build();
    }

    private void checkEnvironment(String environment)
    {
        if (!this.environment.equals(environment)) {
            throw new ForbiddenException();
        }
    }
}
