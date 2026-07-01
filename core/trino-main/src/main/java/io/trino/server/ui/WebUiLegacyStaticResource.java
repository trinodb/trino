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
package io.trino.server.ui;

import com.google.inject.Inject;
import io.trino.server.ExternalUriInfo;
import io.trino.server.security.ResourceSecurity;
import jakarta.ws.rs.BeanParam;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.core.Response;

import java.io.IOException;

import static io.trino.server.security.ResourceSecurity.AccessType.PUBLIC;
import static io.trino.server.security.ResourceSecurity.AccessType.WEB_UI;
import static io.trino.web.ui.WebUiResources.webUiResource;

@Path("")
@ResourceSecurity(PUBLIC)
public class WebUiLegacyStaticResource
{
    private final WebUiConfig config;

    @Inject
    public WebUiLegacyStaticResource(WebUiConfig config)
    {
        this.config = config;
    }

    @GET
    @Path("/ui/legacy")
    public Response getUi(@BeanParam ExternalUriInfo externalUriInfo)
    {
        if (!config.isLegacyUiAvailable()) {
            throw new NotFoundException();
        }
        return Response.seeOther(externalUriInfo.absolutePath("/ui/legacy/")).build();
    }

    @ResourceSecurity(WEB_UI)
    @POST
    @Path("/ui/legacy/{path: .*}")
    public Response postFile()
    {
        // The "getFile" resource method matches all GET requests, and without a
        // resource for POST requests, a METHOD_NOT_ALLOWED error will be returned
        // instead of a NOT_FOUND error
        throw new NotFoundException();
    }

    // asset files are always visible
    @ResourceSecurity(PUBLIC)
    @GET
    @Path("/ui/legacy/assets/{path: .*}")
    public Response getAssetsFile(@PathParam("path") String path)
            throws IOException
    {
        return webUiResource("/webapp-legacy/assets/" + path);
    }

    // vendor files are always visible
    @ResourceSecurity(PUBLIC)
    @GET
    @Path("/ui/legacy/vendor/{path: .*}")
    public Response getVendorFile(@PathParam("path") String path)
            throws IOException
    {
        return webUiResource("/webapp-legacy/vendor/" + path);
    }

    @ResourceSecurity(WEB_UI)
    @GET
    @Path("/ui/legacy/{path: .*}")
    public Response getFile(@PathParam("path") String path)
            throws IOException
    {
        if (!config.isLegacyUiAvailable()) {
            throw new NotFoundException();
        }
        if (path.isEmpty()) {
            path = "index.html";
        }
        return webUiResource("/webapp-legacy/" + path);
    }
}
