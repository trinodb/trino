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
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.core.Response;

import java.io.IOException;

import static io.trino.server.security.ResourceSecurity.AccessType.PUBLIC;
import static io.trino.server.security.ResourceSecurity.AccessType.WEB_UI;
import static io.trino.server.ui.FormWebUiAuthenticationFilter.UI_DISABLED;
import static io.trino.web.ui.WebUiResources.webUiResource;

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
        if (!config.isPreviewEnabled()) {
            return Response.seeOther(externalUriInfo.absolutePath("/ui/legacy/")).build();
        }
        return Response.seeOther(externalUriInfo.absolutePath("/ui/")).build();
    }

    @ResourceSecurity(PUBLIC)
    @GET
    @Path("/ui/assets/{path: .*}")
    public Response getAssetsFile(@PathParam("path") String path)
            throws IOException
    {
        return webUiResource("/webapp/dist/assets/" + path);
    }

    @ResourceSecurity(PUBLIC)
    @GET
    @Path(UI_DISABLED)
    public Response getDisabled()
            throws IOException
    {
        return webUiResource("/webapp/dist/static/disabled.html");
    }

    @ResourceSecurity(PUBLIC)
    @GET
    @Path("/ui/static/{path: .*}")
    public Response getStaticFile(@PathParam("path") String path)
            throws IOException
    {
        return webUiResource("/webapp/dist/static/" + path);
    }

    @ResourceSecurity(WEB_UI)
    @GET
    @Path("/ui/{path: .*}")
    public Response getFile(@BeanParam ExternalUriInfo externalUriInfo, @PathParam("path") String path)
            throws IOException
    {
        if (path.isEmpty()) {
            if (!config.isPreviewEnabled()) {
                return Response.seeOther(externalUriInfo.absolutePath("/ui/legacy/")).build();
            }
            path = "index.html";
        }
        return webUiResource("/webapp/dist/" + path);
    }
}
