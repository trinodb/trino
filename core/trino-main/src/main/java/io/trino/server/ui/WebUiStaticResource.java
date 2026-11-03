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
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.core.Response;

import java.io.InputStream;

import static io.trino.server.security.ResourceSecurity.AccessType.PUBLIC;
import static io.trino.server.security.ResourceSecurity.AccessType.WEB_UI;
import static io.trino.server.ui.FormWebUiAuthenticationFilter.UI_DISABLED;

@Path("")
public class WebUiStaticResource
{
    private final WebUiConfig config;

    @Inject
    public WebUiStaticResource(WebUiConfig config)
    {
        this.config = config;
    }

    @ResourceSecurity(PUBLIC)
    @GET
    public Response getRoot(@BeanParam ExternalUriInfo externalUriInfo)
    {
        return Response.seeOther(externalUriInfo.absolutePath("/ui/")).build();
    }

    @ResourceSecurity(PUBLIC)
    @GET
    @Path("/ui")
    public Response getUi(@BeanParam ExternalUriInfo externalUriInfo)
    {
        if (config.isLegacyEnabled()) {
            return Response.seeOther(externalUriInfo.absolutePath("/ui/legacy/")).build();
        }
        return Response.seeOther(externalUriInfo.absolutePath("/ui/")).build();
    }

    @ResourceSecurity(PUBLIC)
    @GET
    @Path("/ui/assets/{path: .*}")
    public Response getAssetsFile(@PathParam("path") String path)
    {
        if (path == null || path.contains("..") || path.startsWith("/")) {
            throw new NotFoundException("Invalid path");
        }

        String fullPath = "/webapp/dist/assets/" + path;

        InputStream resource = getClass().getResourceAsStream(fullPath);
        if (resource == null) {
            throw new NotFoundException("Resource not found");
        }

        return Response.ok(resource).build();
    }

    @ResourceSecurity(PUBLIC)
    @GET
    @Path(UI_DISABLED)
    public Response getDisabled()
    {
        InputStream resource = getClass().getResourceAsStream("/webapp/dist/static/disabled.html");
        if (resource == null) {
            throw new NotFoundException("Resource not found");
        }

        return Response.ok(resource).build();
    }

    @ResourceSecurity(PUBLIC)
    @GET
    @Path("/ui/static/{path: .*}")
    public Response getStaticFile(@PathParam("path") String path)
    {
        if (path == null || path.contains("..") || path.startsWith("/")) {
            throw new NotFoundException("Invalid path");
        }

        String fullPath = "/webapp/dist/static/" + path;

        InputStream resource = getClass().getResourceAsStream(fullPath);
        if (resource == null) {
            throw new NotFoundException("Resource not found");
        }

        return Response.ok(resource).build();
    }

    @ResourceSecurity(WEB_UI)
    @GET
    @Path("/ui/{path: .*}")
    public Response getFile(@BeanParam ExternalUriInfo externalUriInfo, @PathParam("path") String path)
    {
        if (path == null || path.contains("..") || path.startsWith("/")) {
            throw new NotFoundException("Invalid path");
        }

        if (path.isEmpty()) {
            if (config.isLegacyEnabled()) {
                return Response.seeOther(externalUriInfo.absolutePath("/ui/legacy/")).build();
            }
            path = "index.html";
        }

        String fullPath = "/webapp/dist/" + path;

        InputStream resource = getClass().getResourceAsStream(fullPath);
        if (resource == null) {
            throw new NotFoundException("Resource not found");
        }

        return Response.ok(resource).build();
    }
}
