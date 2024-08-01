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

import io.trino.server.ExternalUriInfo;
import io.trino.server.security.ResourceSecurity;
import jakarta.ws.rs.BeanParam;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.core.Response;

import java.io.IOException;

import static io.trino.server.security.ResourceSecurity.AccessType.WEB_UI;
import static io.trino.web.ui.WebUiResources.webUiResource;

@Path("/ui/preview")
public class WebUiPreviewStaticResource
{
    @ResourceSecurity(WEB_UI)
    @GET
    public Response getUiPreview(@BeanParam ExternalUriInfo externalUriInfo)
    {
        return Response.seeOther(externalUriInfo.absolutePath("/ui/preview/index.html")).build();
    }

    @ResourceSecurity(WEB_UI)
    @GET
    @Path("{path: .*}")
    public Response getFile(@PathParam("path") String path)
            throws IOException
    {
        if (path.isEmpty()) {
            path = "index.html";
        }
        return webUiResource("/webapp-preview/dist/" + path);
    }
}
