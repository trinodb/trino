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

import com.google.inject.Inject;
import io.trino.execution.resourcegroups.ResourceGroupInfoProvider;
import io.trino.server.security.ResourceSecurity;
import io.trino.spi.resourcegroups.ResourceGroupId;
import jakarta.ws.rs.Encoded;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.net.URLDecoder;
import java.util.Arrays;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.server.security.ResourceSecurity.AccessType.MANAGEMENT_READ;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

@Path("/v1/resourceGroupState")
@ResourceSecurity(MANAGEMENT_READ)
public class ResourceGroupStateInfoResource
{
    private final ResourceGroupInfoProvider resourceGroupInfoProvider;

    @Inject
    public ResourceGroupStateInfoResource(ResourceGroupInfoProvider resourceGroupInfoProvider)
    {
        this.resourceGroupInfoProvider = requireNonNull(resourceGroupInfoProvider, "resourceGroupInfoProvider is null");
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Encoded
    @Path("{resourceGroupId: .+}")
    public ResourceGroupInfo getQueryStateInfos(@PathParam("resourceGroupId") String resourceGroupIdString)
    {
        if (!isNullOrEmpty(resourceGroupIdString)) {
            return resourceGroupInfoProvider.tryGetResourceGroupInfo(
                    new ResourceGroupId(
                            Arrays.stream(resourceGroupIdString.split("/"))
                                    .map(ResourceGroupStateInfoResource::urlDecode)
                                    .collect(toImmutableList())))
                    .orElseThrow(NotFoundException::new);
        }
        throw new NotFoundException();
    }

    private static String urlDecode(String value)
    {
        return URLDecoder.decode(value, UTF_8);
    }
}
