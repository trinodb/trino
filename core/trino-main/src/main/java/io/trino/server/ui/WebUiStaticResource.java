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

import io.trino.server.security.ResourceSecurity;
import jakarta.servlet.ServletContext;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import static io.trino.server.security.ResourceSecurity.AccessType.PUBLIC;
import static io.trino.server.security.ResourceSecurity.AccessType.WEB_UI;
import static jakarta.ws.rs.core.Response.Status.NOT_FOUND;

@Path("")
public class WebUiStaticResource
{
    @ResourceSecurity(PUBLIC)
    @GET
    @Path("/")
    public Response getRoot()
    {
        return Response.seeOther(URI.create("/ui/")).build();
    }

    @ResourceSecurity(PUBLIC)
    @GET
    @Path("/ui")
    public Response getUi()
    {
        return Response.seeOther(URI.create("/ui/")).build();
    }

    @ResourceSecurity(WEB_UI)
    @POST
    @Path("/ui/{path: .*}")
    public Response postFile(@PathParam("path") String path)
    {
        // The "getFile" resource method matches all GET requests, and without a
        // resource for POST requests, a METHOD_NOT_ALLOWED error will be returned
        // instead of a NOT_FOUND error
        return Response.status(NOT_FOUND).build();
    }

    // asset files are always visible
    @ResourceSecurity(PUBLIC)
    @GET
    @Path("/ui/assets/{path: .*}")
    public Response getAssetsFile(@PathParam("path") String path, @Context ServletContext servletContext)
            throws IOException
    {
        return getFile("assets/" + path, servletContext);
    }

    // vendor files are always visible
    @ResourceSecurity(PUBLIC)
    @GET
    @Path("/ui/vendor/{path: .*}")
    public Response getVendorFile(@PathParam("path") String path, @Context ServletContext servletContext)
            throws IOException
    {
        return getFile("vendor/" + path, servletContext);
    }

    @ResourceSecurity(WEB_UI)
    @GET
    @Path("/ui/{path: .*}")
    public Response getFile(@PathParam("path") String path, @Context ServletContext servletContext)
            throws IOException
    {
        if (path.isEmpty()) {
            path = "index.html";
        }

        String fullPath = "/webapp/" + path;
        if (!isCanonical(fullPath)) {
            // consider redirecting to the absolute path
            return Response.status(NOT_FOUND).build();
        }

        URL resource = getClass().getResource(fullPath);
        if (resource == null) {
            return Response.status(NOT_FOUND).build();
        }

        return Response.ok(resource.openStream(), servletContext.getMimeType(resource.toString())).build();
    }

    private static boolean isCanonical(String fullPath)
    {
        try {
            return new URI(fullPath).normalize().getPath().equals(fullPath);
        }
        catch (URISyntaxException e) {
            return false;
        }
    }
}
