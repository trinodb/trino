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

import com.google.common.io.Resources;
import io.trino.server.security.ResourceSecurity;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;

import java.io.IOException;

import static io.trino.server.security.ResourceSecurity.AccessType.WEB_UI;
import static io.trino.server.ui.FormWebUiAuthenticationFilter.UI_LOGOUT;
import static java.nio.charset.StandardCharsets.UTF_8;

@Path(UI_LOGOUT)
public class OAuth2WebUiLogoutResource
{
    @ResourceSecurity(WEB_UI)
    @GET
    public Response logout(@Context HttpHeaders httpHeaders, @Context UriInfo uriInfo, @Context SecurityContext securityContext)
            throws IOException
    {
        return Response.ok(Resources.toString(Resources.getResource(getClass(), "/oauth2/logout.html"), UTF_8))
                .cookie(OAuthWebUiCookie.delete())
                .cookie(OAuthRefreshWebUiCookie.delete())
                .build();
    }
}
