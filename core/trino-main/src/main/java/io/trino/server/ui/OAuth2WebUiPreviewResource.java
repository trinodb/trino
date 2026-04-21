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
import io.trino.server.security.oauth2.OAuth2Client;
import io.trino.spi.security.Identity;
import jakarta.ws.rs.BeanParam;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;

import java.net.URI;
import java.util.Optional;

import static io.trino.server.ServletSecurityUtils.authenticatedIdentity;
import static io.trino.server.security.ResourceSecurity.AccessType.WEB_UI;
import static io.trino.server.ui.FormWebUiAuthenticationFilter.UI_LOGOUT;
import static io.trino.server.ui.FormWebUiAuthenticationFilter.UI_PREVIEW_AUTH_INFO;
import static io.trino.server.ui.FormWebUiAuthenticationFilter.UI_PREVIEW_LOGOUT;
import static io.trino.server.ui.FormWebUiAuthenticationFilter.getDeleteCookies;
import static io.trino.server.ui.OAuthWebUiCookie.delete;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.util.Objects.requireNonNull;

@Path("")
@ResourceSecurity(WEB_UI)
public class OAuth2WebUiPreviewResource
{
    private final OAuth2Client oAuth2Client;

    @Inject
    public OAuth2WebUiPreviewResource(OAuth2Client oAuth2Client)
    {
        this.oAuth2Client = requireNonNull(oAuth2Client, "oAuth2Client is null");
    }

    @GET
    @Path(UI_PREVIEW_AUTH_INFO)
    @Produces(APPLICATION_JSON)
    public AuthInfo getAuthInfo(ContainerRequestContext request)
    {
        Optional<String> username = authenticatedIdentity(request).map(Identity::getUser);
        return new AuthInfo("oauth2", false, username.isPresent(), username);
    }

    @GET
    @Path(UI_PREVIEW_LOGOUT)
    @Produces(APPLICATION_JSON)
    public Response logout(@Context HttpHeaders httpHeaders, @BeanParam ExternalUriInfo uriInfo, @Context SecurityContext securityContext)
    {
        Optional<String> idToken = OAuthIdTokenCookie.read(httpHeaders.getCookies());
        URI callbackUri = uriInfo.absolutePath(UI_LOGOUT + "/logout.html");
        return Response.seeOther(oAuth2Client.getLogoutEndpoint(idToken, callbackUri)
                        .orElse(callbackUri))
                .cookie(OAuthIdTokenCookie.delete(httpHeaders.getCookies()))
                .cookie(getDeleteCookies(httpHeaders.getCookies(), securityContext.isSecure()))
                .cookie(delete(httpHeaders.getCookies()))
                .build();
    }
}
