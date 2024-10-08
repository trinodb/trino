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
import io.trino.server.security.ResourceSecurity;
import io.trino.server.security.oauth2.OAuth2Client;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriBuilder;
import jakarta.ws.rs.core.UriInfo;

import java.io.IOException;
import java.net.URI;
import java.util.Optional;

import static io.trino.server.security.ResourceSecurity.AccessType.PUBLIC;
import static io.trino.server.security.ResourceSecurity.AccessType.WEB_UI;
import static io.trino.server.ui.FormWebUiAuthenticationFilter.UI_LOGOUT;
import static io.trino.server.ui.OAuthWebUiCookie.delete;
import static io.trino.web.ui.WebUiResources.webUiResource;
import static java.util.Objects.requireNonNull;

@Path(UI_LOGOUT)
public class OAuth2WebUiLogoutResource
{
    private final OAuth2Client auth2Client;

    @Inject
    public OAuth2WebUiLogoutResource(OAuth2Client auth2Client)
    {
        this.auth2Client = requireNonNull(auth2Client, "auth2Client is null");
    }

    @ResourceSecurity(WEB_UI)
    @GET
    public Response logout(@Context HttpHeaders httpHeaders, @Context UriInfo uriInfo)
            throws IOException
    {
        Optional<String> idToken = OAuthIdTokenCookie.read(httpHeaders.getCookies());
        URI callBackUri = UriBuilder.fromUri(uriInfo.getAbsolutePath())
                .path("logout.html")
                .build();

        return Response.seeOther(auth2Client.getLogoutEndpoint(idToken, callBackUri).orElse(callBackUri))
                .cookie(OAuthIdTokenCookie.delete(httpHeaders.getCookies()))
                .cookie(delete(httpHeaders.getCookies()))
                .build();
    }

    @ResourceSecurity(PUBLIC)
    @GET
    @Path("/logout.html")
    public Response logoutPage()
            throws IOException
    {
        return webUiResource("/oauth2/logout.html");
    }
}
