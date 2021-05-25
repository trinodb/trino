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
package io.trino.server.security.oauth2;

import io.airlift.log.Logger;
import io.trino.server.security.ResourceSecurity;
import io.trino.server.security.oauth2.OAuth2Service.OAuthResult;
import io.trino.server.ui.OAuthWebUiCookie;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;

import java.net.URI;

import static io.trino.server.security.ResourceSecurity.AccessType.PUBLIC;
import static io.trino.server.security.oauth2.OAuth2CallbackResource.CALLBACK_ENDPOINT;
import static io.trino.server.ui.FormWebUiAuthenticationFilter.UI_LOCATION;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.TEXT_HTML;

@Path(CALLBACK_ENDPOINT)
public class OAuth2CallbackResource
{
    private static final Logger LOG = Logger.get(OAuth2CallbackResource.class);

    public static final String CALLBACK_ENDPOINT = "/oauth2/callback";

    private final OAuth2Service service;

    @Inject
    public OAuth2CallbackResource(OAuth2Service service)
    {
        this.service = requireNonNull(service, "service is null");
    }

    @ResourceSecurity(PUBLIC)
    @GET
    @Produces(TEXT_HTML)
    public Response callback(
            @QueryParam("state") String state,
            @QueryParam("code") String code,
            @QueryParam("error") String error,
            @QueryParam("error_description") String errorDescription,
            @QueryParam("error_uri") String errorUri,
            @Context UriInfo uriInfo,
            @Context SecurityContext securityContext)
    {
        // Note: the Web UI may be disabled, so REST requests can not redirect to a success or error page inside of the Web UI

        if (error != null) {
            LOG.debug(
                    "OAuth server returned an error: error=%s, error_description=%s, error_uri=%s, state=%s",
                    error,
                    errorDescription,
                    errorUri,
                    state);
            return Response.ok()
                    .entity(service.getCallbackErrorHtml(error))
                    .build();
        }

        OAuthResult result;
        try {
            result = service.finishChallenge(state, code, uriInfo.getBaseUri().resolve(CALLBACK_ENDPOINT));
        }
        catch (ChallengeFailedException | RuntimeException e) {
            LOG.debug(e, "Authentication response could not be verified: state=%s", state);
            return Response.ok()
                    .entity(service.getInternalFailureHtml("Authentication response could not be verified"))
                    .build();
        }

        return Response
                .seeOther(URI.create(UI_LOCATION))
                .cookie(OAuthWebUiCookie.create(result.getAccessToken(), result.getTokenExpiration(), securityContext.isSecure()))
                .build();
    }
}
