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
package io.trino.node;

import com.google.inject.Inject;
import io.trino.server.security.ResourceSecurity;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;

import java.net.URI;
import java.util.Set;

import static io.trino.server.security.ResourceSecurity.AccessType.INTERNAL_ONLY;
import static io.trino.server.security.ResourceSecurity.AccessType.MANAGEMENT_READ;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static jakarta.ws.rs.core.MediaType.TEXT_PLAIN;
import static java.util.Objects.requireNonNull;

/**
 * Announces the existence of a node to the cluster.
 */
@Path("/v1/announce")
public class AnnounceNodeResource
{
    private final AnnounceNodeInventory announceNodeInventory;

    @Inject
    public AnnounceNodeResource(AnnounceNodeInventory announceNodeInventory)
    {
        this.announceNodeInventory = requireNonNull(announceNodeInventory, "announceNodeInventory is null");
    }

    @ResourceSecurity(MANAGEMENT_READ)
    @GET
    @Produces(APPLICATION_JSON)
    public Set<URI> getNodes()
    {
        return announceNodeInventory.getNodes();
    }

    @ResourceSecurity(INTERNAL_ONLY)
    @POST
    @Consumes(TEXT_PLAIN)
    public void announce(String uri)
    {
        announceNodeInventory.announce(URI.create(uri));
    }
}
